use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde_json::{Map, json};

#[derive(Debug, Clone, Default)]
pub struct ProviderMcpSetup {
    pub claude_config_path: Option<PathBuf>,
    pub codex_home: Option<PathBuf>,
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

pub fn prepare_external_provider_mcp(
    work_dir: &Path,
    external_servers: &[ExternalMcpServer],
) -> Result<ProviderMcpSetup> {
    let claude_config_path = work_dir.join(".borg-local-mcp.json");
    let codex_home = work_dir.join(".codex-local-mcp");
    let mut servers = Map::new();
    for server in external_servers {
        if server.name.trim().is_empty() || servers.contains_key(&server.name) {
            continue;
        }
        servers.insert(
            server.name.clone(),
            json!({
                "command": server.command,
                "args": server.args,
                "env": server.env,
            }),
        );
    }
    fs::write(
        &claude_config_path,
        serde_json::to_vec_pretty(&json!({ "mcpServers": servers }))?,
    )
    .with_context(|| format!("failed to write {}", claude_config_path.display()))?;

    fs::create_dir_all(&codex_home)
        .with_context(|| format!("failed to create {}", codex_home.display()))?;
    let mut mcp_servers = BTreeMap::new();
    for server in external_servers {
        let mut config = BTreeMap::new();
        config.insert(
            "command".to_string(),
            toml::Value::String(server.command.clone()),
        );
        config.insert(
            "args".to_string(),
            toml::Value::Array(
                server
                    .args
                    .iter()
                    .cloned()
                    .map(toml::Value::String)
                    .collect(),
            ),
        );
        config.insert("env".to_string(), toml::Value::try_from(&server.env)?);
        mcp_servers.insert(server.name.clone(), toml::Value::try_from(config)?);
    }
    let config = BTreeMap::from([(
        "mcp_servers".to_string(),
        toml::Value::try_from(mcp_servers)?,
    )]);
    fs::write(codex_home.join("config.toml"), toml::to_string(&config)?)?;

    Ok(ProviderMcpSetup {
        claude_config_path: Some(claude_config_path),
        codex_home: Some(codex_home),
        allowed_tools: external_servers
            .iter()
            .flat_map(|server| server.allowed_tools.iter().cloned())
            .collect::<Vec<_>>()
            .join(","),
    })
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
