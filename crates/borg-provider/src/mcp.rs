use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde_json::{Map, json};
#[cfg(windows)]
use uuid::Uuid;

// Persistent peer consultations wait for at most two hours. Provider MCP tool
// deadlines can be five minutes, so leave a minute of headroom for the peer
// result to cross the local bridge before the provider gives up.
pub(crate) const BORG_AGENT_MCP_TOOL_TIMEOUT_SECS: u64 = 2 * 60 * 60 + 60;
const BORG_AGENT_MCP_TOOL_TIMEOUT_MS: u64 = BORG_AGENT_MCP_TOOL_TIMEOUT_SECS * 1_000;

#[derive(Debug, Clone, Default)]
pub struct ProviderMcpSetup {
    pub claude_config_path: Option<PathBuf>,
    pub allowed_tools: String,
}

#[derive(Debug, Clone, Default)]
pub struct ExternalMcpServer {
    pub name: String,
    pub command: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub allowed_tools: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct BorgAgentMcpContext {
    pub owner_id: Option<String>,
    pub allowed_scopes: Vec<String>,
    pub user_id: Option<String>,
    pub external_servers: Vec<ExternalMcpServer>,
    pub api_token: Option<String>,
}

pub fn merge_allowed_tools(base: &str, extras: &[&str]) -> String {
    let mut seen = BTreeSet::new();
    base.split(',')
        .chain(extras.iter().copied())
        .map(str::trim)
        .filter(|tool| !tool.is_empty())
        .filter(|tool| seen.insert((*tool).to_string()))
        .collect::<Vec<_>>()
        .join(",")
}

/// Normalize a provider-neutral allowlist entry to the MCP wire tool name.
/// Entries from another server namespace are ignored rather than allowing a
/// caller to smuggle a cross-server permission into a provider config.
pub(crate) fn normalize_mcp_tool_name(server_name: &str, allowed: &str) -> Option<String> {
    let prefix = format!("mcp__{server_name}__");
    allowed
        .strip_prefix(&prefix)
        .map(str::to_string)
        .or_else(|| (!allowed.starts_with("mcp__")).then(|| allowed.to_string()))
}

fn claude_mcp_tool_name(server_name: &str, allowed: &str) -> Option<String> {
    normalize_mcp_tool_name(server_name, allowed).map(|tool| format!("mcp__{server_name}__{tool}"))
}

pub fn prepare_external_provider_mcp(
    work_dir: &Path,
    external_servers: &[ExternalMcpServer],
) -> Result<ProviderMcpSetup> {
    let claude_config_path = work_dir.join(".borg-local-mcp.json");
    let mut servers = Map::new();
    for server in external_servers {
        if server.name.trim().is_empty() || servers.contains_key(&server.name) {
            continue;
        }
        let mut config = json!({
            "command": server.command,
            "args": server.args,
            "env": server.env,
        });
        if server.name == "borg_agent" {
            config["timeout"] = json!(BORG_AGENT_MCP_TOOL_TIMEOUT_MS);
        }
        servers.insert(server.name.clone(), config);
    }
    write_private_file(
        &claude_config_path,
        &serde_json::to_vec_pretty(&json!({ "mcpServers": servers }))?,
    )
    .with_context(|| format!("failed to write {}", claude_config_path.display()))?;

    Ok(ProviderMcpSetup {
        claude_config_path: Some(claude_config_path),
        allowed_tools: external_servers
            .iter()
            .flat_map(|server| {
                server
                    .allowed_tools
                    .iter()
                    .filter_map(|tool| claude_mcp_tool_name(&server.name, tool))
            })
            .collect::<Vec<_>>()
            .join(","),
    })
}

pub(crate) fn write_private_file(path: &Path, contents: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("private file {} has no parent", path.display()))?;
    let mut temporary = tempfile::Builder::new()
        .prefix(".borg-mcp-")
        .tempfile_in(parent)
        .with_context(|| format!("create temporary private file in {}", parent.display()))?;
    temporary.write_all(contents)?;
    temporary.as_file().sync_all()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(temporary.path(), fs::Permissions::from_mode(0o600))?;
    }
    let temporary = temporary.into_temp_path();
    #[cfg(not(windows))]
    fs::rename(&temporary, path)
        .with_context(|| format!("atomically replace {}", path.display()))?;
    #[cfg(windows)]
    {
        let backup = path.with_file_name(format!(".mcp-backup-{}", Uuid::new_v4()));
        if path.exists() {
            fs::rename(path, &backup)
                .with_context(|| format!("stage existing private file {}", path.display()))?;
        }
        if let Err(error) = fs::rename(&temporary, path) {
            if backup.exists() {
                let _ = fs::rename(&backup, path);
            }
            return Err(error).with_context(|| format!("replace {}", path.display()));
        }
        if backup.exists() {
            fs::remove_file(backup)?;
        }
    }
    Ok(())
}

pub fn prepare_provider_mcp_with_scope(
    work_dir: &Path,
    _owner_id: Option<&str>,
    _allowed_owner_scopes: &[String],
    _current_user_id: Option<&str>,
    external_servers: &[ExternalMcpServer],
    _api_token: Option<&str>,
) -> Result<ProviderMcpSetup> {
    prepare_external_provider_mcp(work_dir, external_servers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_config_preserves_mcp_allowlists_and_namespaces() {
        let root = tempfile::tempdir().expect("temporary provider home");
        let servers = vec![ExternalMcpServer {
            name: "docs__search".to_string(),
            command: "docs-mcp".to_string(),
            args: vec!["serve".to_string()],
            env: BTreeMap::from([(String::from("TOKEN"), String::from("secret"))]),
            allowed_tools: vec![
                "search".to_string(),
                "mcp__docs__search__read".to_string(),
                "mcp__other__secret".to_string(),
            ],
        }];

        let setup = prepare_external_provider_mcp(root.path(), &servers).unwrap();
        assert_eq!(
            setup.allowed_tools,
            "mcp__docs__search__search,mcp__docs__search__read"
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(root.path().join(".borg-local-mcp.json"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn borg_agent_provider_config_allows_long_running_peer_tools() {
        let root = tempfile::tempdir().expect("temporary provider home");
        let servers = vec![ExternalMcpServer {
            name: "borg_agent".to_string(),
            command: "/bin/borg".to_string(),
            args: vec!["__agent-mcp".to_string()],
            env: BTreeMap::new(),
            allowed_tools: Vec::new(),
        }];

        prepare_external_provider_mcp(root.path(), &servers).unwrap();
        let config = serde_json::from_slice::<serde_json::Value>(
            &fs::read(root.path().join(".borg-local-mcp.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            config
                .get("mcpServers")
                .and_then(|servers| servers.get("borg_agent"))
                .and_then(|server| server.get("timeout")),
            Some(&json!(BORG_AGENT_MCP_TOOL_TIMEOUT_MS))
        );
    }
}
