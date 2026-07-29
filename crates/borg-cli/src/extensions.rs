use crate::agent_config::CapabilityConfig;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

pub const MANIFEST_VERSION: u32 = 1;
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ExtensionCatalog {
    pub extensions: Vec<EffectiveExtension>,
}
#[derive(Debug, Clone, Serialize)]
pub(crate) struct EffectiveExtension {
    pub id: String,
    pub version: String,
    pub enabled: bool,
    pub active: bool,
    pub reason: Option<String>,
    pub skill_roots: Vec<PathBuf>,
    pub servers: Vec<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    manifest_version: u32,
    id: String,
    version: String,
    #[serde(default = "yes")]
    enabled: bool,
    #[serde(default)]
    required_capabilities: Vec<String>,
    #[serde(default)]
    skill_roots: Vec<PathBuf>,
    #[serde(default)]
    mcp: BTreeMap<String, Server>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Server {
    command: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    allowed_tools: Vec<String>,
}
fn yes() -> bool {
    true
}
pub(crate) fn discover(
    cwd: &Path,
    capabilities: &CapabilityConfig,
) -> Result<(ExtensionCatalog, Vec<borg_provider::mcp::ExternalMcpServer>)> {
    let mut dirs = vec![cwd.join(".borg/extensions")];
    if let Some(root) = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|v| PathBuf::from(v).join(".config")))
    {
        dirs.push(root.join("borg/extensions"));
    }
    let mut ids = BTreeSet::new();
    let mut list = Vec::new();
    let mut servers = Vec::new();
    for dir in dirs {
        if !dir.exists() {
            continue;
        }
        for entry in fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.extension().and_then(|x| x.to_str()) != Some("toml") {
                continue;
            }
            let m: Manifest = toml::from_str(&fs::read_to_string(&path)?)
                .with_context(|| format!("invalid extension {}", path.display()))?;
            ensure!(
                m.manifest_version == MANIFEST_VERSION,
                "unsupported extension manifest version"
            );
            ensure!(
                valid(&m.id) && ids.insert(m.id.clone()),
                "duplicate or invalid extension id `{}`",
                m.id
            );
            let missing = m
                .required_capabilities
                .iter()
                .find(|x| !cap(capabilities, x));
            let active = m.enabled && missing.is_none();
            let reason = (!m.enabled)
                .then(|| "disabled by manifest".into())
                .or_else(|| missing.map(|x| format!("requires capability `{x}`")));
            let roots = m
                .skill_roots
                .into_iter()
                .map(|r| {
                    ensure!(
                        !r.is_absolute()
                            && !r
                                .components()
                                .any(|c| matches!(c, std::path::Component::ParentDir)),
                        "invalid skill root"
                    );
                    Ok(path.parent().unwrap().join(r))
                })
                .collect::<Result<Vec<_>>>()?;
            let names = m.mcp.keys().map(|n| format!("{}__{}", m.id, n)).collect();
            if active {
                for (n, s) in m.mcp {
                    ensure!(
                        valid(&n) && !s.command.trim().is_empty() && !s.command.contains('\0'),
                        "invalid extension server"
                    );
                    servers.push(borg_provider::mcp::ExternalMcpServer {
                        name: format!("{}__{}", m.id, n),
                        command: s.command,
                        args: s.args,
                        env: s.env,
                        allowed_tools: s.allowed_tools,
                    });
                }
            }
            list.push(EffectiveExtension {
                id: m.id,
                version: m.version,
                enabled: m.enabled,
                active,
                reason,
                skill_roots: roots,
                servers: names,
            });
        }
    }
    Ok((ExtensionCatalog { extensions: list }, servers))
}
fn valid(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|x| x.is_ascii_alphanumeric() || matches!(x, b'-' | b'_'))
}
fn cap(c: &CapabilityConfig, s: &str) -> bool {
    match s {
        "multiplayer" => c.multiplayer,
        "subagents" => c.subagents,
        "autonomous_team" => c.autonomous_team,
        "shared_work" => c.shared_work,
        "presence" => c.presence,
        "cloud_sync" => c.cloud_sync,
        "web_relay" => c.web_relay,
        "telemetry" => c.telemetry,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(id: &str, capability: &str) -> String {
        format!(
            "manifest_version = 1\nid = \"{id}\"\nversion = \"1.0.0\"\nrequired_capabilities = [\"{capability}\"]\nskill_roots = [\"skills\"]\n[mcp.docs]\ncommand = \"docs-mcp\"\nargs = [\"serve\"]\n"
        )
    }

    #[test]
    fn active_manifest_is_namespaced_and_inactive_manifest_does_not_expose_servers() {
        let dir = tempfile::tempdir().unwrap();
        let extension_dir = dir.path().join(".borg/extensions");
        fs::create_dir_all(&extension_dir).unwrap();
        fs::write(
            extension_dir.join("docs.toml"),
            manifest("docs", "multiplayer"),
        )
        .unwrap();
        let (catalog, servers) = discover(dir.path(), &CapabilityConfig::default()).unwrap();
        assert!(catalog.extensions[0].active);
        assert_eq!(servers[0].name, "docs__docs");
        let mut disabled = CapabilityConfig::default();
        disabled.multiplayer = false;
        let (catalog, servers) = discover(dir.path(), &disabled).unwrap();
        assert!(!catalog.extensions[0].active);
        assert!(servers.is_empty());
    }

    #[test]
    fn duplicate_manifest_ids_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let extension_dir = dir.path().join(".borg/extensions");
        fs::create_dir_all(&extension_dir).unwrap();
        fs::write(
            extension_dir.join("one.toml"),
            manifest("same", "multiplayer"),
        )
        .unwrap();
        fs::write(
            extension_dir.join("two.toml"),
            manifest("same", "multiplayer"),
        )
        .unwrap();
        assert!(discover(dir.path(), &CapabilityConfig::default()).is_err());
    }
}
