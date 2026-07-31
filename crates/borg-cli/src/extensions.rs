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
    allow_project_mcp: bool,
) -> Result<(ExtensionCatalog, Vec<borg_provider::mcp::ExternalMcpServer>)> {
    let user_dir = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|v| PathBuf::from(v).join(".config")))
        .map(|root| root.join("borg/extensions"));
    discover_in_dirs(
        Some(cwd.join(".borg/extensions")),
        user_dir,
        capabilities,
        allow_project_mcp,
    )
}

fn discover_in_dirs(
    project_dir: Option<PathBuf>,
    user_dir: Option<PathBuf>,
    capabilities: &CapabilityConfig,
    allow_project_mcp: bool,
) -> Result<(ExtensionCatalog, Vec<borg_provider::mcp::ExternalMcpServer>)> {
    let dirs = [(project_dir, true), (user_dir, false)];
    let mut ids = BTreeSet::new();
    let mut list = Vec::new();
    let mut servers = Vec::new();
    for (dir, is_project) in dirs
        .into_iter()
        .filter_map(|(dir, is_project)| dir.map(|dir| (dir, is_project)))
    {
        if !dir.exists() {
            continue;
        }
        let mut entries = fs::read_dir(&dir)?.collect::<std::io::Result<Vec<_>>>()?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
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
            let project_untrusted = is_project && !allow_project_mcp;
            let active = m.enabled && missing.is_none() && !project_untrusted;
            let reason = (!m.enabled)
                .then(|| "disabled by manifest".into())
                .or_else(|| missing.map(|x| format!("requires capability `{x}`")))
                .or_else(|| {
                    project_untrusted.then(|| {
                        "project MCP trust is disabled; set [extensions].allow_project_mcp = true"
                            .into()
                    })
                });
            let manifest_dir =
                path.parent().unwrap().canonicalize().with_context(|| {
                    format!("canonicalize extension directory {}", path.display())
                })?;
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
                    let requested = manifest_dir.join(r);
                    if !requested.exists() {
                        return Ok(None);
                    }
                    let root = requested.canonicalize().with_context(|| {
                        format!("canonicalize extension skill root in {}", path.display())
                    })?;
                    ensure!(
                        root.starts_with(&manifest_dir),
                        "extension skill root escapes manifest directory"
                    );
                    Ok(Some(root))
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
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
        let (catalog, servers) = discover(dir.path(), &CapabilityConfig::default(), true).unwrap();
        assert!(catalog.extensions[0].active);
        assert_eq!(servers[0].name, "docs__docs");
        let disabled = CapabilityConfig {
            multiplayer: false,
            ..CapabilityConfig::default()
        };
        let (catalog, servers) = discover(dir.path(), &disabled, true).unwrap();
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
        assert!(discover(dir.path(), &CapabilityConfig::default(), true).is_err());
    }

    #[test]
    fn path_traversal_and_unknown_capabilities_are_inactive_or_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let extension_dir = dir.path().join(".borg/extensions");
        fs::create_dir_all(&extension_dir).unwrap();
        fs::write(
            extension_dir.join("bad.toml"),
            "manifest_version=1\nid=\"bad\"\nversion=\"1\"\nskill_roots=[\"../escape\"]\n",
        )
        .unwrap();
        assert!(discover(dir.path(), &CapabilityConfig::default(), true).is_err());
        fs::remove_file(extension_dir.join("bad.toml")).unwrap();
        fs::write(
            extension_dir.join("unknown.toml"),
            manifest("unknown", "not_real"),
        )
        .unwrap();
        let (catalog, servers) = discover(dir.path(), &CapabilityConfig::default(), true).unwrap();
        assert!(!catalog.extensions[0].active);
        assert!(
            catalog.extensions[0]
                .reason
                .as_deref()
                .unwrap()
                .contains("not_real")
        );
        assert!(servers.is_empty());
    }

    #[test]
    fn project_manifests_are_listed_before_user_catalog_position() {
        let dir = tempfile::tempdir().unwrap();
        let extension_dir = dir.path().join(".borg/extensions");
        fs::create_dir_all(&extension_dir).unwrap();
        fs::write(
            extension_dir.join("first.toml"),
            manifest("first", "multiplayer"),
        )
        .unwrap();
        fs::write(
            extension_dir.join("second.toml"),
            manifest("second", "multiplayer"),
        )
        .unwrap();
        let (catalog, _) = discover(dir.path(), &CapabilityConfig::default(), true).unwrap();
        let ids = catalog
            .extensions
            .into_iter()
            .map(|extension| extension.id)
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["first", "second"]);
    }

    #[test]
    fn project_mcp_is_denied_by_default_and_activates_only_when_trusted() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("docs.toml"),
            manifest("docs", "multiplayer"),
        )
        .unwrap();

        let (catalog, servers) = discover_in_dirs(
            Some(dir.path().to_owned()),
            None,
            &CapabilityConfig::default(),
            false,
        )
        .unwrap();
        assert!(!catalog.extensions[0].active);
        assert!(
            catalog.extensions[0]
                .reason
                .as_deref()
                .unwrap()
                .contains("project MCP trust is disabled")
        );
        assert!(servers.is_empty());

        let (catalog, servers) = discover_in_dirs(
            Some(dir.path().to_owned()),
            None,
            &CapabilityConfig::default(),
            true,
        )
        .unwrap();
        assert!(catalog.extensions[0].active);
        assert_eq!(servers[0].name, "docs__docs");
    }

    #[test]
    fn trusted_skill_roots_are_canonical_and_untrusted_project_roots_are_inactive() {
        let dir = tempfile::tempdir().unwrap();
        let manifest_dir = dir.path().join(".borg/extensions");
        let skills = manifest_dir.join("skills");
        fs::create_dir_all(&skills).unwrap();
        fs::write(
            manifest_dir.join("docs.toml"),
            manifest("docs", "multiplayer"),
        )
        .unwrap();
        let canonical_manifest_dir = manifest_dir.canonicalize().unwrap();
        let (untrusted, _) = discover(dir.path(), &CapabilityConfig::default(), false).unwrap();
        assert!(!untrusted.extensions[0].active);
        assert!(
            untrusted.extensions[0]
                .skill_roots
                .iter()
                .all(|root| root.starts_with(&canonical_manifest_dir))
        );
        let (trusted, _) = discover(dir.path(), &CapabilityConfig::default(), true).unwrap();
        assert!(trusted.extensions[0].active);
        assert_eq!(
            trusted.extensions[0].skill_roots,
            vec![skills.canonicalize().unwrap()]
        );
    }

    #[test]
    fn symlinked_skill_root_escape_is_rejected() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let dir = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            let manifest_dir = dir.path().join(".borg/extensions");
            fs::create_dir_all(&manifest_dir).unwrap();
            symlink(outside.path(), manifest_dir.join("skills")).unwrap();
            fs::write(
                manifest_dir.join("bad.toml"),
                manifest("bad", "multiplayer"),
            )
            .unwrap();
            assert!(discover(dir.path(), &CapabilityConfig::default(), true).is_err());
        }
    }

    #[test]
    fn user_manifest_remains_active_without_project_trust() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("docs.toml"),
            manifest("docs", "multiplayer"),
        )
        .unwrap();
        let (catalog, servers) = discover_in_dirs(
            None,
            Some(dir.path().to_owned()),
            &CapabilityConfig::default(),
            false,
        )
        .unwrap();
        assert!(catalog.extensions[0].active);
        assert_eq!(servers[0].name, "docs__docs");
    }
}
