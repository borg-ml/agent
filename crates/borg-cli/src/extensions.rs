//! Blu: Borg's live, declarative extension system.
//!
//! Blu packages stay deliberately inspectable. A package may contribute skills
//! and provider-neutral stdio MCP servers, but it cannot register opaque shell
//! hooks. Catalog reloads are transactional: callers keep the last-known-good
//! runtime when a changed package does not validate.

use crate::agent_config::{CapabilityConfig, ExtensionAccess, ExtensionConfig, NativeAccessPolicy};
use anyhow::{Context, Result, bail, ensure};
use semver::{Version, VersionReq};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    ffi::{CStr, CString, c_char, c_void},
    fs,
    path::{Component, Path, PathBuf},
    process::Command,
    sync::{Mutex, OnceLock},
};
use uuid::Uuid;

pub const MANIFEST_VERSION: u32 = 1;
const STATE_VERSION: u32 = 1;
const MAX_WORKFLOW_SOURCE: u64 = 256 * 1024;
const PACKAGE_MANIFEST_NAMES: [&str; 2] = ["blu.toml", "extension.toml"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExtensionScope {
    Project,
    User,
}

impl ExtensionScope {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Project => "project",
            Self::User => "user",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ExtensionCatalog {
    /// Content-addressed catalog revision used by the live reload loop.
    pub revision: String,
    /// Dependency-first activation order.
    pub load_order: Vec<String>,
    pub extensions: Vec<EffectiveExtension>,
    /// Invalid packages are isolated here instead of preventing Borg startup.
    pub diagnostics: Vec<ExtensionDiagnostic>,
}

impl ExtensionCatalog {
    pub(crate) fn apply_editor_customization(
        &self,
        editor: &mut crate::editor_preferences::EditorPreferences,
        agent: &mut crate::agent_config::AgentConfig,
    ) -> Result<()> {
        let mut editor_value = toml::Value::try_from(editor.clone())?;
        for id in &self.load_order {
            let Some(extension) = self.extensions.iter().find(|extension| &extension.id == id)
            else {
                continue;
            };
            if !extension.active {
                continue;
            }
            merge_toml(
                &mut editor_value,
                toml::Value::Table(toml::map::Map::from_iter(extension.api.editor.clone())),
            );
            for (action, bindings) in &extension.api.keybindings {
                agent.keybindings.replace(action, bindings.clone())?;
            }
            for (alias, target) in &extension.api.aliases {
                ensure!(
                    valid_id(alias),
                    "invalid command alias `{alias}` in extension {}",
                    extension.id
                );
                ensure!(
                    target.starts_with('/'),
                    "extension alias `{alias}` must target a slash command"
                );
                agent.commands.aliases.insert(alias.clone(), target.clone());
            }
        }
        let customized: crate::editor_preferences::EditorPreferences = editor_value.try_into()?;
        customized.validate()?;
        *editor = customized;
        Ok(())
    }

    pub(crate) fn active_skill_roots(&self) -> Vec<PathBuf> {
        self.load_order
            .iter()
            .filter_map(|id| self.extensions.iter().find(|extension| &extension.id == id))
            .flat_map(|extension| extension.skill_roots.iter().cloned())
            .collect()
    }

    pub(crate) fn active_workflows(&self) -> Vec<borg_remote::BluWorkflowDefinition> {
        self.load_order
            .iter()
            .filter_map(|id| self.extensions.iter().find(|extension| &extension.id == id))
            .flat_map(|extension| extension.workflows.iter().cloned())
            .collect()
    }

    pub(crate) fn api_snapshot(&self) -> borg_remote::ExtensionApiSnapshot {
        let mut snapshot = borg_remote::ExtensionApiSnapshot {
            api_version: borg_remote::EXTENSION_API_VERSION,
            catalog_revision: self.revision.clone(),
            ..Default::default()
        };
        for id in &self.load_order {
            let Some(extension) = self.extensions.iter().find(|extension| &extension.id == id)
            else {
                continue;
            };
            if !extension.active {
                continue;
            }
            snapshot
                .transforms
                .extend(extension.api.transforms.iter().map(|(name, transform)| {
                    borg_remote::ExtensionApiTransform {
                        extension_id: extension.id.clone(),
                        name: name.clone(),
                        scope: transform.scope,
                        append_system_prompt: transform.append_system_prompt.clone(),
                        append_context: transform.append_context.clone(),
                    }
                }));
            snapshot
                .hooks
                .extend(extension.api.hooks.iter().map(|(name, hook)| {
                    borg_remote::ExtensionApiHook {
                        extension_id: extension.id.clone(),
                        name: name.clone(),
                        scope: hook.scope,
                        event: hook.event.clone(),
                        workflow: hook.workflow.clone(),
                        effect: borg_remote::effect_class_from_name(&hook.effect)
                            .unwrap_or(borg_remote::ExtensionEffectClass::Idempotent),
                    }
                }));
            snapshot
                .tools
                .extend(extension.api.tools.iter().map(|(name, tool)| {
                    borg_remote::ExtensionApiTool {
                        extension_id: extension.id.clone(),
                        name: name.clone(),
                        wire_name: format!("ext__{}__{}", extension.id, name),
                        scope: tool.scope,
                        workflow: tool.workflow.clone(),
                        description: tool.description.clone(),
                        input_schema: toml_value_to_json(&tool.input_schema),
                        effect: borg_remote::effect_class_from_name(&tool.effect)
                            .unwrap_or(borg_remote::ExtensionEffectClass::Idempotent),
                    }
                }));
            snapshot
                .commands
                .extend(extension.api.commands.iter().map(|(name, command)| {
                    borg_remote::ExtensionApiCommand {
                        extension_id: extension.id.clone(),
                        name: name.clone(),
                        scope: command.scope,
                        workflow: command.workflow.clone(),
                        description: command.description.clone(),
                        effect: borg_remote::effect_class_from_name(&command.effect)
                            .unwrap_or(borg_remote::ExtensionEffectClass::Idempotent),
                    }
                }));
        }
        if let Err(error) = snapshot.validate() {
            tracing::warn!(%error, "invalid extension API snapshot; keeping an empty API");
            borg_remote::ExtensionApiSnapshot {
                api_version: borg_remote::EXTENSION_API_VERSION,
                catalog_revision: self.revision.clone(),
                ..Default::default()
            }
        } else {
            snapshot
        }
    }

    pub(crate) fn extension(&self, id: &str) -> Option<&EffectiveExtension> {
        self.extensions.iter().find(|extension| extension.id == id)
    }

    pub(crate) fn has_errors(&self) -> bool {
        self.error_count() > 0
    }

    pub(crate) fn error_count(&self) -> usize {
        self.diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.level == ExtensionDiagnosticLevel::Error)
            .count()
    }
}

fn merge_toml(base: &mut toml::Value, overlay: toml::Value) {
    match (base, overlay) {
        (toml::Value::Table(base), toml::Value::Table(overlay)) => {
            for (key, value) in overlay {
                if let Some(existing) = base.get_mut(&key) {
                    merge_toml(existing, value);
                } else {
                    base.insert(key, value);
                }
            }
        }
        (base, overlay) => *base = overlay,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExtensionDiagnosticLevel {
    Error,
    Warning,
}

impl ExtensionDiagnosticLevel {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warning => "warning",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ExtensionDiagnostic {
    pub level: ExtensionDiagnosticLevel,
    pub path: PathBuf,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct EffectiveExtension {
    pub id: String,
    pub name: String,
    pub version: String,
    pub description: Option<String>,
    pub scope: ExtensionScope,
    pub requested_access: ExtensionAccess,
    pub manifest_path: PathBuf,
    pub enabled: bool,
    pub active: bool,
    pub reason: Option<String>,
    pub dependencies: BTreeMap<String, String>,
    pub skill_roots: Vec<PathBuf>,
    pub servers: Vec<String>,
    pub workflow_names: Vec<String>,
    pub workflow_runtimes: BTreeMap<String, String>,
    pub api: ApiManifest,
    #[serde(skip_serializing)]
    pub workflows: Vec<borg_remote::BluWorkflowDefinition>,
    /// Secret settings are always redacted from catalog output.
    pub settings: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    manifest_version: u32,
    id: String,
    #[serde(default)]
    name: Option<String>,
    version: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default = "yes")]
    enabled: bool,
    #[serde(default = "trusted_access")]
    runtime_access: ExtensionAccess,
    #[serde(default)]
    borg_version: Option<String>,
    #[serde(default)]
    required_capabilities: Vec<String>,
    #[serde(default)]
    dependencies: BTreeMap<String, String>,
    #[serde(default)]
    skill_roots: Vec<PathBuf>,
    #[serde(default)]
    config: BTreeMap<String, ConfigField>,
    #[serde(default)]
    mcp: BTreeMap<String, Server>,
    #[serde(default)]
    workflows: BTreeMap<String, Workflow>,
    #[serde(default)]
    api: ApiManifest,
    #[serde(default)]
    native: Option<NativeManifest>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeManifest {
    library: PathBuf,
    sha256: String,
    #[serde(default = "native_abi_version")]
    abi_version: u32,
}

fn native_abi_version() -> u32 {
    1
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ApiManifest {
    pub version: u32,
    /// Partial editor.toml tree merged in catalog order for active packages.
    pub editor: BTreeMap<String, toml::Value>,
    /// Keybinding action names mapped to complete chord lists.
    pub keybindings: BTreeMap<String, Vec<String>>,
    /// Slash-command aliases contributed by this package.
    pub aliases: BTreeMap<String, String>,
    pub transforms: BTreeMap<String, ApiTransform>,
    pub hooks: BTreeMap<String, ApiHook>,
    pub tools: BTreeMap<String, ApiTool>,
    pub commands: BTreeMap<String, ApiCommand>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApiTransform {
    #[serde(default)]
    pub append_system_prompt: String,
    #[serde(default)]
    pub append_context: String,
    #[serde(default = "project_scope")]
    pub scope: borg_remote::ExtensionApiScope,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApiHook {
    pub event: String,
    pub workflow: String,
    #[serde(default = "idempotent_effect")]
    pub effect: String,
    #[serde(default = "project_scope")]
    pub scope: borg_remote::ExtensionApiScope,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApiTool {
    pub workflow: String,
    pub description: String,
    #[serde(default = "object_schema")]
    pub input_schema: toml::Value,
    #[serde(default = "idempotent_effect")]
    pub effect: String,
    #[serde(default = "project_scope")]
    pub scope: borg_remote::ExtensionApiScope,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApiCommand {
    pub workflow: String,
    pub description: String,
    #[serde(default = "idempotent_effect")]
    pub effect: String,
    #[serde(default = "project_scope")]
    pub scope: borg_remote::ExtensionApiScope,
}

fn project_scope() -> borg_remote::ExtensionApiScope {
    borg_remote::ExtensionApiScope::Project
}

fn idempotent_effect() -> String {
    "idempotent".to_string()
}

fn object_schema() -> toml::Value {
    toml::Value::Table(toml::map::Map::from_iter([(
        "type".to_string(),
        toml::Value::String("object".to_string()),
    )]))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigField {
    #[serde(rename = "type", default = "string_kind")]
    kind: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    required: bool,
    #[serde(default)]
    secret: bool,
    #[serde(default)]
    default: Option<toml::Value>,
}

#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Workflow {
    entrypoint: PathBuf,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    runtime: borg_remote::WorkflowRuntime,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    args: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct BluState {
    state_version: u32,
    extensions: BTreeMap<String, ExtensionState>,
    sources: BTreeMap<String, SourceRecord>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ExtensionState {
    enabled: Option<bool>,
    config: BTreeMap<String, toml::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceRecord {
    kind: String,
    location: String,
    #[serde(default)]
    revision: Option<String>,
}

#[derive(Debug, Clone)]
struct Candidate {
    manifest: Manifest,
    manifest_path: PathBuf,
    package_root: PathBuf,
    scope: ExtensionScope,
    state: ExtensionState,
    active: bool,
    reason: Option<String>,
    roots: Vec<PathBuf>,
    servers: Vec<borg_provider::mcp::ExternalMcpServer>,
    workflows: Vec<borg_remote::BluWorkflowDefinition>,
}

fn yes() -> bool {
    true
}

fn string_kind() -> String {
    "string".to_string()
}

fn trusted_access() -> ExtensionAccess {
    ExtensionAccess::Trusted
}

pub(crate) fn discover(
    cwd: &Path,
    capabilities: &CapabilityConfig,
    extension_config: &ExtensionConfig,
) -> Result<(
    ExtensionCatalog,
    Vec<borg_provider::mcp::ExternalMcpServer>,
    Vec<borg_remote::BluWorkflowDefinition>,
)> {
    let user_root = user_config_root()?;
    discover_in_dirs(
        Some(cwd.join(".borg/extensions")),
        Some(user_root.join("extensions")),
        Some(cwd.join(".borg/blu.toml")),
        Some(user_root.join("blu.toml")),
        capabilities,
        extension_config,
    )
}

#[allow(clippy::too_many_arguments)]
fn discover_in_dirs(
    project_dir: Option<PathBuf>,
    user_dir: Option<PathBuf>,
    project_state_path: Option<PathBuf>,
    user_state_path: Option<PathBuf>,
    capabilities: &CapabilityConfig,
    extension_config: &ExtensionConfig,
) -> Result<(
    ExtensionCatalog,
    Vec<borg_provider::mcp::ExternalMcpServer>,
    Vec<borg_remote::BluWorkflowDefinition>,
)> {
    let mut diagnostics = Vec::new();
    let mut digest = Sha256::new();
    digest.update(format!("{capabilities:?}:{extension_config:?}").as_bytes());
    let project_state =
        load_state_for_discovery(project_state_path.as_deref(), &mut diagnostics, &mut digest);
    let user_state =
        load_state_for_discovery(user_state_path.as_deref(), &mut diagnostics, &mut digest);
    let roots = [
        (project_dir, ExtensionScope::Project, &project_state),
        (user_dir, ExtensionScope::User, &user_state),
    ];
    let mut candidates = Vec::new();

    for (dir, scope, state) in roots {
        let Some(dir) = dir else { continue };
        let extension_base = match dir.canonicalize() {
            Ok(path) => path,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                diagnostics.push(ExtensionDiagnostic {
                    level: ExtensionDiagnosticLevel::Error,
                    path: dir.clone(),
                    message: format!("could not resolve extension directory: {error:#}"),
                });
                continue;
            }
        };
        let reload_signal = dir
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("blu.reload");
        if let Err(error) = hash_directory_signature(&mut digest, &extension_base) {
            diagnostics.push(ExtensionDiagnostic {
                level: ExtensionDiagnosticLevel::Error,
                path: extension_base.clone(),
                message: format!("could not inspect extension directory: {error:#}"),
            });
        }
        if reload_signal.is_file()
            && let Err(error) = hash_file(&mut digest, &reload_signal)
        {
            diagnostics.push(ExtensionDiagnostic {
                level: ExtensionDiagnosticLevel::Warning,
                path: reload_signal.clone(),
                message: format!("could not inspect Blu reload signal: {error:#}"),
            });
        }
        let paths = match manifest_paths(&extension_base) {
            Ok(paths) => paths,
            Err(error) => {
                diagnostics.push(ExtensionDiagnostic {
                    level: ExtensionDiagnosticLevel::Error,
                    path: extension_base.clone(),
                    message: format!("could not enumerate extensions: {error:#}"),
                });
                Vec::new()
            }
        };
        for path in paths {
            if let Err(error) = hash_file(&mut digest, &path) {
                diagnostics.push(ExtensionDiagnostic {
                    level: ExtensionDiagnosticLevel::Error,
                    path: path.clone(),
                    message: format!("could not fingerprint extension: {error:#}"),
                });
            }
            match load_candidate(&path, scope, Some(&extension_base)) {
                Ok(mut candidate) => {
                    if let Err(error) = hash_package_tree(&mut digest, &candidate.package_root) {
                        diagnostics.push(ExtensionDiagnostic {
                            level: ExtensionDiagnosticLevel::Warning,
                            path: candidate.package_root.clone(),
                            message: format!(
                                "could not completely fingerprint extension package: {error:#}"
                            ),
                        });
                    }
                    // The filename hint is only an optimization; state is keyed by
                    // the validated manifest id.
                    candidate.state = state
                        .extensions
                        .get(&candidate.manifest.id)
                        .cloned()
                        .unwrap_or_default();
                    match evaluate_candidate(&mut candidate, capabilities, extension_config) {
                        Ok(()) => candidates.push(candidate),
                        Err(error) => diagnostics.push(ExtensionDiagnostic {
                            level: ExtensionDiagnosticLevel::Error,
                            path,
                            message: format!("{error:#}"),
                        }),
                    }
                }
                Err(error) => diagnostics.push(ExtensionDiagnostic {
                    level: ExtensionDiagnosticLevel::Error,
                    path,
                    message: format!("{error:#}"),
                }),
            }
        }
    }

    // The closest scope wins deterministically. A project package can shadow a
    // user package, but the duplicate remains visible as a diagnostic.
    let mut seen = BTreeSet::new();
    candidates.retain(|candidate| {
        if seen.insert(candidate.manifest.id.clone()) {
            true
        } else {
            diagnostics.push(ExtensionDiagnostic {
                level: ExtensionDiagnosticLevel::Warning,
                path: candidate.manifest_path.clone(),
                message: format!(
                    "extension `{}` is shadowed by the higher-priority project/user package",
                    candidate.manifest.id
                ),
            });
            false
        }
    });

    resolve_dependencies(&mut candidates);
    let load_order = dependency_order(&mut candidates);
    let mut external_servers = Vec::new();
    let mut workflows = Vec::new();
    for id in &load_order {
        if let Some(candidate) = candidates
            .iter()
            .find(|candidate| candidate.active && &candidate.manifest.id == id)
        {
            external_servers.extend(candidate.servers.clone());
            workflows.extend(candidate.workflows.clone());
        }
    }

    let extensions = candidates.into_iter().map(effective).collect();
    let catalog = ExtensionCatalog {
        revision: hex::encode(digest.finalize()),
        load_order,
        extensions,
        diagnostics,
    };
    Ok((catalog, external_servers, workflows))
}

fn load_candidate(
    path: &Path,
    scope: ExtensionScope,
    extension_base: Option<&Path>,
) -> Result<Candidate> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("read extension manifest {}", path.display()))?;
    let manifest: Manifest =
        toml::from_str(&text).with_context(|| format!("invalid extension {}", path.display()))?;
    ensure!(
        manifest.manifest_version == MANIFEST_VERSION,
        "unsupported manifest_version {}; this Borg supports {}",
        manifest.manifest_version,
        MANIFEST_VERSION
    );
    ensure!(
        valid_id(&manifest.id),
        "invalid extension id `{}`",
        manifest.id
    );
    Version::parse(&manifest.version).with_context(|| {
        format!(
            "extension `{}` has an invalid semantic version",
            manifest.id
        )
    })?;
    let package_root = path
        .parent()
        .context("extension manifest has no parent directory")?
        .canonicalize()
        .with_context(|| format!("canonicalize package containing {}", path.display()))?;
    if let Some(extension_base) = extension_base {
        ensure!(
            package_root.starts_with(extension_base),
            "extension package escapes its configured extension directory"
        );
    }
    let canonical_manifest = path
        .canonicalize()
        .with_context(|| format!("canonicalize extension manifest {}", path.display()))?;
    ensure!(
        canonical_manifest.starts_with(&package_root),
        "extension manifest escapes its package"
    );
    let candidate = Candidate {
        manifest,
        manifest_path: path.to_path_buf(),
        package_root,
        scope,
        state: ExtensionState::default(),
        active: true,
        reason: None,
        roots: Vec::new(),
        servers: Vec::new(),
        workflows: Vec::new(),
    };
    Ok(candidate)
}

fn evaluate_candidate(
    candidate: &mut Candidate,
    capabilities: &CapabilityConfig,
    extension_config: &ExtensionConfig,
) -> Result<()> {
    candidate.active = true;
    candidate.reason = None;
    candidate.roots.clear();
    candidate.servers.clear();
    candidate.workflows.clear();
    // Keep validation inputs detached from the activation fields we mutate as
    // individual gates fail.
    let manifest = candidate.manifest.clone();
    validate_candidate_declarations(candidate, &manifest)?;
    let enabled = candidate.state.enabled.unwrap_or(manifest.enabled);
    if !enabled {
        deactivate(candidate, "disabled by Blu state");
    }
    if let Some(requirement) = &manifest.borg_version {
        let requirement = VersionReq::parse(requirement)
            .with_context(|| format!("extension `{}` has invalid borg_version", manifest.id))?;
        let running = Version::parse(env!("CARGO_PKG_VERSION"))?;
        if !requirement.matches(&running) {
            deactivate(
                candidate,
                format!("requires Borg {requirement}; running {running}"),
            );
        }
    }
    if let Some(missing) = manifest
        .required_capabilities
        .iter()
        .find(|capability| !cap(capabilities, capability))
    {
        deactivate(candidate, format!("requires capability `{missing}`"));
    }
    let allowed_access = match candidate.scope {
        ExtensionScope::User => extension_config.default_access,
        ExtensionScope::Project if extension_config.allow_project_mcp => extension_config
            .project_access
            .max(ExtensionAccess::Trusted),
        ExtensionScope::Project => extension_config.project_access,
    };
    if manifest.runtime_access > allowed_access {
        deactivate(
            candidate,
            format!(
                "requests {:?} access but user policy allows {:?}",
                manifest.runtime_access, allowed_access
            )
            .to_lowercase(),
        );
    }
    if manifest.runtime_access == ExtensionAccess::Native
        && extension_config.native_access != NativeAccessPolicy::Allow
    {
        let reason = match extension_config.native_access {
            NativeAccessPolicy::Deny => "native extension access is denied by user policy",
            NativeAccessPolicy::Prompt => {
                "native extension awaits approval; set [extensions].native_access = \"allow\" for prompt-free loading"
            }
            NativeAccessPolicy::Allow => unreachable!(),
        };
        deactivate(candidate, reason);
    }
    // Disabled, untrusted, or capability-gated packages stay inspectable but
    // do not require runtime-only settings or environment variables to exist.
    if !candidate.active {
        return Ok(());
    }
    let config = match resolved_config(&manifest, &candidate.state) {
        Ok(config) => config,
        Err(error) => {
            deactivate(candidate, format!("{error:#}"));
            return Ok(());
        }
    };
    if let Some(native) = &manifest.native
        && let Err(error) =
            load_native_extension(&candidate.package_root, native, &manifest.id, &config)
    {
        deactivate(
            candidate,
            format!("native extension failed to load: {error:#}"),
        );
        return Ok(());
    }
    let mut servers = Vec::new();
    for (name, server) in &manifest.mcp {
        let rendered = (|| -> Result<_> {
            let command = render_template(&server.command, &config, &candidate.package_root)?;
            ensure!(
                !command.trim().is_empty() && !command.contains('\0'),
                "extension `{}` has an invalid MCP command",
                manifest.id
            );
            let args = server
                .args
                .iter()
                .map(|value| render_template(value, &config, &candidate.package_root))
                .collect::<Result<Vec<_>>>()?;
            let env = server
                .env
                .iter()
                .map(|(key, value)| {
                    Ok((
                        key.clone(),
                        render_template(value, &config, &candidate.package_root)?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>>>()?;
            Ok((command, args, env))
        })();
        let (command, args, env) = match rendered {
            Ok(rendered) => rendered,
            Err(error) => {
                deactivate(candidate, format!("{error:#}"));
                return Ok(());
            }
        };
        servers.push(borg_provider::mcp::ExternalMcpServer {
            name: format!("{}__{name}", manifest.id),
            command,
            args,
            env,
            allowed_tools: server.allowed_tools.clone(),
        });
    }
    candidate.servers = servers;
    for workflow in &mut candidate.workflows {
        workflow.command = workflow
            .command
            .as_deref()
            .map(|command| render_template(command, &config, &candidate.package_root))
            .transpose()?;
        workflow.args = workflow
            .args
            .iter()
            .map(|argument| render_template(argument, &config, &candidate.package_root))
            .collect::<Result<Vec<_>>>()?;
    }
    Ok(())
}

fn validate_candidate_declarations(candidate: &mut Candidate, manifest: &Manifest) -> Result<()> {
    ensure!(
        (manifest.runtime_access == ExtensionAccess::Native) == manifest.native.is_some(),
        "extension `{}` must declare both runtime_access = \"native\" and [native]",
        manifest.id
    );
    if let Some(native) = &manifest.native {
        ensure!(
            matches!(native.abi_version, 1 | 2),
            "extension `{}` requests native ABI {}; Borg supports 1 and 2",
            manifest.id,
            native.abi_version
        );
        ensure!(
            native.sha256.len() == 64 && native.sha256.chars().all(|ch| ch.is_ascii_hexdigit()),
            "extension `{}` native sha256 must be 64 hexadecimal characters",
            manifest.id
        );
        validate_relative_path(&native.library, "native library")?;
        let library = candidate.package_root.join(&native.library);
        ensure!(
            library.is_file(),
            "native library does not exist: {}",
            library.display()
        );
        ensure!(
            library.canonicalize()?.starts_with(&candidate.package_root),
            "native library escapes its package"
        );
    }
    if !manifest.api.editor.is_empty() {
        let mut editor =
            toml::Value::try_from(crate::editor_preferences::EditorPreferences::default())?;
        merge_toml(
            &mut editor,
            toml::Value::Table(toml::map::Map::from_iter(manifest.api.editor.clone())),
        );
        let editor: crate::editor_preferences::EditorPreferences =
            editor.try_into().with_context(|| {
                format!(
                    "extension `{}` has invalid editor customization",
                    manifest.id
                )
            })?;
        editor.validate()?;
    }
    let mut keybindings = crate::agent_config::KeybindingConfig::default();
    for (action, bindings) in &manifest.api.keybindings {
        keybindings
            .replace(action, bindings.clone())
            .with_context(|| format!("extension `{}` has invalid keybindings", manifest.id))?;
    }
    for (alias, target) in &manifest.api.aliases {
        ensure!(
            valid_id(alias),
            "extension `{}` has invalid alias `{alias}`",
            manifest.id
        );
        ensure!(
            target.starts_with('/'),
            "extension `{}` alias `{alias}` must target a slash command",
            manifest.id
        );
    }
    if manifest.runtime_access == ExtensionAccess::Sandboxed {
        ensure!(
            manifest.mcp.is_empty(),
            "sandboxed extension `{}` cannot launch MCP processes",
            manifest.id
        );
        ensure!(
            manifest
                .workflows
                .values()
                .all(|workflow| workflow.runtime == borg_remote::WorkflowRuntime::Blu),
            "sandboxed extension `{}` can only run embedded Blu/Lua/Luau workflows",
            manifest.id
        );
    }
    if let Some(requirement) = &manifest.borg_version {
        VersionReq::parse(requirement)
            .with_context(|| format!("extension `{}` has invalid borg_version", manifest.id))?;
    }
    for capability in &manifest.required_capabilities {
        ensure!(
            known_capability(capability),
            "extension `{}` requires unknown capability `{capability}`",
            manifest.id
        );
    }
    for (dependency, requirement) in &manifest.dependencies {
        ensure!(valid_id(dependency), "invalid dependency id `{dependency}`");
        VersionReq::parse(requirement).with_context(|| {
            format!(
                "extension `{}` dependency `{dependency}` has invalid version requirement",
                manifest.id
            )
        })?;
    }
    validate_config_schema(manifest, &candidate.state)?;
    for root in &manifest.skill_roots {
        validate_relative_path(root, "skill root")?;
        let requested = candidate.package_root.join(root);
        ensure!(
            requested.is_dir(),
            "extension `{}` skill root does not exist: {}",
            manifest.id,
            requested.display()
        );
        let canonical = requested.canonicalize()?;
        ensure!(
            canonical.starts_with(&candidate.package_root),
            "extension skill root escapes its package"
        );
        candidate.roots.push(canonical);
    }
    for (name, workflow) in &manifest.workflows {
        ensure!(
            valid_id(name),
            "invalid workflow name {name} in extension {}",
            manifest.id
        );
        validate_relative_path(&workflow.entrypoint, "workflow entrypoint")?;
        let extension = workflow
            .entrypoint
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        let accepted_extensions = workflow
            .runtime
            .source_extensions()
            .iter()
            .map(|extension| format!(".{extension}"))
            .collect::<Vec<_>>()
            .join(", ");
        ensure!(
            workflow.runtime.accepts_source_extension(extension),
            "extension {} workflow {name} runtime {} requires one of {accepted_extensions} entrypoints",
            manifest.id,
            workflow.runtime.label()
        );
        let requested = candidate.package_root.join(&workflow.entrypoint);
        ensure!(
            requested.is_file(),
            "extension {} workflow entrypoint does not exist: {}",
            manifest.id,
            requested.display()
        );
        let canonical = requested.canonicalize()?;
        ensure!(
            canonical.starts_with(&candidate.package_root),
            "Blu workflow entrypoint escapes its package"
        );
        let metadata = fs::metadata(&canonical)?;
        ensure!(
            metadata.len() <= MAX_WORKFLOW_SOURCE,
            "extension {} workflow {name} exceeds the {} byte source limit",
            manifest.id,
            MAX_WORKFLOW_SOURCE
        );
        let source = fs::read_to_string(&canonical)
            .with_context(|| format!("read workflow {name} for extension {}", manifest.id))?;
        ensure!(
            !source.contains('\0'),
            "extension {} workflow {name} contains NUL bytes",
            manifest.id
        );
        if let Some(command) = &workflow.command {
            ensure!(
                !command.trim().is_empty() && !command.contains('\0'),
                "extension {} workflow {name} has an invalid runtime command",
                manifest.id
            );
            validate_template(command, manifest)?;
        }
        for argument in &workflow.args {
            ensure!(!argument.contains('\0'), "workflow argument contains NUL");
            validate_template(argument, manifest)?;
        }
        candidate
            .workflows
            .push(borg_remote::BluWorkflowDefinition {
                extension_id: manifest.id.clone(),
                name: name.clone(),
                description: workflow.description.clone(),
                runtime: workflow.runtime,
                source,
                entrypoint: canonical,
                working_directory: candidate.package_root.clone(),
                command: workflow.command.clone(),
                args: workflow.args.clone(),
            });
    }
    ensure!(
        manifest.api.version == 0 || manifest.api.version == borg_remote::EXTENSION_API_VERSION,
        "extension {} uses unsupported API version {}",
        manifest.id,
        manifest.api.version
    );
    for (name, transform) in &manifest.api.transforms {
        ensure!(valid_id(name), "invalid extension transform name {name}");
        ensure!(
            transform.append_system_prompt.len() <= 16 * 1024,
            "extension {} transform {name} is too large",
            manifest.id
        );
        ensure!(
            !transform.append_system_prompt.contains('\0'),
            "extension {} transform {name} contains NUL",
            manifest.id
        );
        ensure!(
            transform.append_context.len() <= 16 * 1024,
            "extension {} context transform {name} is too large",
            manifest.id
        );
        ensure!(
            !transform.append_context.contains('\0'),
            "extension {} context transform {name} contains NUL",
            manifest.id
        );
    }
    for (name, hook) in &manifest.api.hooks {
        ensure!(valid_id(name), "invalid extension hook name {name}");
        ensure!(
            valid_id(&hook.event),
            "extension {} hook {name} has an invalid event",
            manifest.id
        );
        ensure!(
            borg_remote::EXTENSION_HOOK_EVENTS.contains(&hook.event.as_str()),
            "extension {} hook {name} uses unsupported event {}",
            manifest.id,
            hook.event
        );
        ensure!(
            manifest.workflows.contains_key(&hook.workflow),
            "extension {} hook {name} references missing workflow {}",
            manifest.id,
            hook.workflow
        );
        borg_remote::effect_class_from_name(&hook.effect)?;
    }
    for (name, tool) in &manifest.api.tools {
        ensure!(valid_id(name), "invalid extension tool name {name}");
        ensure!(
            manifest.workflows.contains_key(&tool.workflow),
            "extension {} tool {name} references missing workflow {}",
            manifest.id,
            tool.workflow
        );
        ensure!(
            tool.description.len() <= 4 * 1024 && !tool.description.trim().is_empty(),
            "extension {} tool {name} needs a bounded description",
            manifest.id
        );
        ensure!(
            tool.input_schema.as_table().is_some(),
            "extension {} tool {name} input_schema must be a TOML table",
            manifest.id
        );
        borg_remote::effect_class_from_name(&tool.effect)?;
    }
    for (name, command) in &manifest.api.commands {
        ensure!(valid_id(name), "invalid extension command name {name}");
        ensure!(
            manifest.workflows.contains_key(&command.workflow),
            "extension {} command {name} references missing workflow {}",
            manifest.id,
            command.workflow
        );
        ensure!(
            command.description.len() <= 4 * 1024 && !command.description.trim().is_empty(),
            "extension {} command {name} needs a bounded description",
            manifest.id
        );
        borg_remote::effect_class_from_name(&command.effect)?;
    }
    for (name, server) in &manifest.mcp {
        ensure!(valid_id(name), "invalid MCP server name `{name}`");
        ensure!(
            !server.command.trim().is_empty() && !server.command.contains('\0'),
            "extension `{}` has an invalid MCP command",
            manifest.id
        );
        for key in server.env.keys() {
            ensure!(valid_env_name(key), "invalid MCP environment name `{key}`");
        }
        validate_template(&server.command, manifest)?;
        for argument in &server.args {
            validate_template(argument, manifest)?;
        }
        for value in server.env.values() {
            validate_template(value, manifest)?;
        }
        for tool in &server.allowed_tools {
            ensure!(
                valid_allowed_tool(tool),
                "extension `{}` has an invalid allowed MCP tool `{tool}`",
                manifest.id
            );
        }
    }
    Ok(())
}

type NativeShutdown = unsafe extern "C" fn(*mut c_void);

struct LoadedNative {
    hash: String,
    _library: libloading::Library,
    shutdown: Option<NativeShutdown>,
    handle: usize,
}

unsafe impl Send for LoadedNative {}

impl Drop for LoadedNative {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown {
            // SAFETY: callback and opaque handle came from this loaded library.
            unsafe { shutdown(self.handle as *mut c_void) };
        }
    }
}

#[repr(C)]
struct NativeHostV2 {
    abi_version: u32,
    struct_size: usize,
    extension_id: *const c_char,
    config_json: *const c_char,
    log: unsafe extern "C" fn(u32, *const c_char),
    emit_event: unsafe extern "C" fn(*const c_char) -> i32,
}

unsafe extern "C" fn native_log(level: u32, message: *const c_char) {
    if message.is_null() {
        return;
    }
    // SAFETY: ABI callers provide a NUL-terminated string for this call.
    let message = unsafe { CStr::from_ptr(message) }.to_string_lossy();
    match level {
        0 => tracing::debug!(target: "borg::native_extension", "{message}"),
        1 => tracing::info!(target: "borg::native_extension", "{message}"),
        2 => tracing::warn!(target: "borg::native_extension", "{message}"),
        _ => tracing::error!(target: "borg::native_extension", "{message}"),
    }
}

unsafe extern "C" fn native_emit_event(event_json: *const c_char) -> i32 {
    if event_json.is_null() {
        return 1;
    }
    // SAFETY: ABI callers provide a NUL-terminated string for this call.
    let event = unsafe { CStr::from_ptr(event_json) }.to_bytes();
    if event.len() > borg_remote::MAX_HOOK_ARGUMENT_BYTES {
        return 2;
    }
    match serde_json::from_slice::<serde_json::Value>(event) {
        Ok(event) => {
            tracing::info!(target: "borg::native_extension", event = %event, "native extension event");
            0
        }
        Err(_) => 3,
    }
}

static NATIVE_LIBRARIES: OnceLock<Mutex<HashMap<PathBuf, LoadedNative>>> = OnceLock::new();

fn load_native_extension(
    package_root: &Path,
    native: &NativeManifest,
    extension_id: &str,
    config: &BTreeMap<String, toml::Value>,
) -> Result<()> {
    let path = package_root.join(&native.library).canonicalize()?;
    let actual = hex::encode(Sha256::digest(fs::read(&path)?));
    ensure!(
        actual.eq_ignore_ascii_case(&native.sha256),
        "native library hash mismatch for {}",
        path.display()
    );
    let libraries = NATIVE_LIBRARIES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut libraries = libraries
        .lock()
        .map_err(|_| anyhow::anyhow!("native extension registry lock is poisoned"))?;
    if let Some(loaded) = libraries.get(&path) {
        ensure!(
            loaded.hash == actual,
            "native library changed after loading; restart Borg to load the new bytes"
        );
        return Ok(());
    }
    // SAFETY: native mode is an explicit user grant for in-process execution.
    // Hash pinning binds that grant to these exact bytes, and the versioned C
    // entrypoint avoids relying on Rust's unstable ABI.
    let library = unsafe { libloading::Library::new(&path) }
        .with_context(|| format!("open native library {}", path.display()))?;
    let mut shutdown = None;
    let mut handle = std::ptr::null_mut();
    unsafe {
        if native.abi_version == 1 {
            let initialize: libloading::Symbol<'_, unsafe extern "C" fn(u32) -> i32> = library
                .get(b"borg_extension_init\0")
                .context("missing borg_extension_init symbol")?;
            let status = initialize(1);
            ensure!(status == 0, "borg_extension_init returned status {status}");
        } else {
            ensure!(
                native.abi_version == 2,
                "unsupported native ABI {}; expected 1 or 2",
                native.abi_version
            );
            let id = CString::new(extension_id)?;
            let config_json = CString::new(serde_json::to_string(config)?)?;
            let host = NativeHostV2 {
                abi_version: 2,
                struct_size: std::mem::size_of::<NativeHostV2>(),
                extension_id: id.as_ptr(),
                config_json: config_json.as_ptr(),
                log: native_log,
                emit_event: native_emit_event,
            };
            let initialize: libloading::Symbol<
                '_,
                unsafe extern "C" fn(*const NativeHostV2, *mut *mut c_void) -> i32,
            > = library
                .get(b"borg_extension_init_v2\0")
                .context("missing borg_extension_init_v2 symbol")?;
            let status = initialize(&host, &mut handle);
            ensure!(
                status == 0,
                "borg_extension_init_v2 returned status {status}"
            );
            shutdown = library
                .get::<NativeShutdown>(b"borg_extension_shutdown_v2\0")
                .ok()
                .map(|symbol| *symbol);
        }
    }
    libraries.insert(
        path,
        LoadedNative {
            hash: actual,
            _library: library,
            shutdown,
            handle: handle as usize,
        },
    );
    Ok(())
}

fn resolved_config(
    manifest: &Manifest,
    state: &ExtensionState,
) -> Result<BTreeMap<String, toml::Value>> {
    validate_config_schema(manifest, state)?;
    let mut resolved = BTreeMap::new();
    for (key, field) in &manifest.config {
        let value = state
            .config
            .get(key)
            .cloned()
            .or_else(|| field.default.clone());
        if let Some(value) = value {
            resolved.insert(key.clone(), value);
        } else if field.required {
            let description = field
                .description
                .as_deref()
                .map(|value| format!(" ({value})"))
                .unwrap_or_default();
            bail!(
                "extension `{}` requires setting `{key}`{description}",
                manifest.id
            );
        }
    }
    Ok(resolved)
}

fn validate_config_schema(manifest: &Manifest, state: &ExtensionState) -> Result<()> {
    for key in state.config.keys() {
        ensure!(
            manifest.config.contains_key(key),
            "extension `{}` has unknown configured setting `{key}`",
            manifest.id
        );
    }
    for (key, field) in &manifest.config {
        ensure!(valid_id(key), "invalid extension setting name `{key}`");
        ensure!(
            matches!(
                field.kind.as_str(),
                "string" | "integer" | "float" | "boolean" | "array"
            ),
            "extension `{}` setting `{key}` has unsupported type `{}`",
            manifest.id,
            field.kind
        );
        if let Some(value) = state.config.get(key).or(field.default.as_ref()) {
            ensure!(
                config_type_matches(&field.kind, value),
                "extension `{}` setting `{key}` must be {}",
                manifest.id,
                field.kind
            );
        }
    }
    Ok(())
}

fn config_type_matches(kind: &str, value: &toml::Value) -> bool {
    matches!(
        (kind, value),
        ("string", toml::Value::String(_))
            | ("integer", toml::Value::Integer(_))
            | ("float", toml::Value::Float(_))
            | ("boolean", toml::Value::Boolean(_))
            | ("array", toml::Value::Array(_))
    )
}

fn validate_template(input: &str, manifest: &Manifest) -> Result<()> {
    let mut remainder = input;
    while let Some(start) = remainder.find("${") {
        let tail = &remainder[start + 2..];
        let end = tail
            .find('}')
            .context("unterminated Blu template variable")?;
        let variable = &tail[..end];
        if variable == "extension_dir" {
            // Always available.
        } else if let Some(key) = variable.strip_prefix("config.") {
            ensure!(
                manifest.config.contains_key(key),
                "extension `{}` template references unknown setting `{key}`",
                manifest.id
            );
        } else if let Some(name) = variable.strip_prefix("env.") {
            ensure!(
                valid_env_name(name),
                "invalid environment variable `{name}`"
            );
        } else {
            bail!("unknown Blu template variable `${{{variable}}}`");
        }
        remainder = &tail[end + 1..];
    }
    Ok(())
}

fn render_template(
    input: &str,
    config: &BTreeMap<String, toml::Value>,
    package_root: &Path,
) -> Result<String> {
    let mut output = String::with_capacity(input.len());
    let mut remainder = input;
    while let Some(start) = remainder.find("${") {
        output.push_str(&remainder[..start]);
        let tail = &remainder[start + 2..];
        let end = tail
            .find('}')
            .context("unterminated Blu template variable")?;
        let variable = &tail[..end];
        let value = if variable == "extension_dir" {
            package_root.display().to_string()
        } else if let Some(key) = variable.strip_prefix("config.") {
            config
                .get(key)
                .with_context(|| format!("missing extension setting `{key}`"))
                .map(config_value_string)?
        } else if let Some(name) = variable.strip_prefix("env.") {
            ensure!(
                valid_env_name(name),
                "invalid environment variable `{name}`"
            );
            std::env::var(name)
                .with_context(|| format!("environment variable `{name}` is not set"))?
        } else {
            bail!("unknown Blu template variable `${{{variable}}}`");
        };
        ensure!(!value.contains('\0'), "Blu template value contains NUL");
        output.push_str(&value);
        remainder = &tail[end + 1..];
    }
    output.push_str(remainder);
    Ok(output)
}

fn config_value_string(value: &toml::Value) -> String {
    match value {
        toml::Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

fn toml_value_to_json(value: &toml::Value) -> serde_json::Value {
    match value {
        toml::Value::String(value) => serde_json::Value::String(value.clone()),
        toml::Value::Integer(value) => serde_json::json!(value),
        toml::Value::Float(value) => serde_json::json!(value),
        toml::Value::Boolean(value) => serde_json::json!(value),
        toml::Value::Datetime(value) => serde_json::Value::String(value.to_string()),
        toml::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(toml_value_to_json).collect())
        }
        toml::Value::Table(table) => serde_json::Value::Object(
            table
                .iter()
                .map(|(key, value)| (key.clone(), toml_value_to_json(value)))
                .collect(),
        ),
    }
}

fn resolve_dependencies(candidates: &mut [Candidate]) {
    let versions = candidates
        .iter()
        .map(|candidate| {
            (
                candidate.manifest.id.clone(),
                Version::parse(&candidate.manifest.version).expect("validated version"),
            )
        })
        .collect::<HashMap<_, _>>();
    loop {
        let active = candidates
            .iter()
            .filter(|candidate| candidate.active)
            .map(|candidate| candidate.manifest.id.clone())
            .collect::<BTreeSet<_>>();
        let mut changed = false;
        for candidate in candidates.iter_mut().filter(|candidate| candidate.active) {
            for (dependency, requirement) in &candidate.manifest.dependencies {
                let requirement = match VersionReq::parse(requirement) {
                    Ok(requirement) => requirement,
                    Err(_) => {
                        deactivate(
                            candidate,
                            format!(
                                "dependency `{dependency}` has invalid version requirement `{requirement}`"
                            ),
                        );
                        changed = true;
                        break;
                    }
                };
                let Some(version) = versions.get(dependency) else {
                    deactivate(
                        candidate,
                        format!("missing dependency `{dependency}` ({requirement})"),
                    );
                    changed = true;
                    break;
                };
                if !requirement.matches(version) {
                    deactivate(
                        candidate,
                        format!("dependency `{dependency}` is {version}, requires {requirement}"),
                    );
                    changed = true;
                    break;
                }
                if !active.contains(dependency) {
                    deactivate(candidate, format!("dependency `{dependency}` is inactive"));
                    changed = true;
                    break;
                }
            }
        }
        if !changed {
            break;
        }
    }
}

fn dependency_order(candidates: &mut [Candidate]) -> Vec<String> {
    let active_ids = candidates
        .iter()
        .filter(|candidate| candidate.active)
        .map(|candidate| candidate.manifest.id.clone())
        .collect::<BTreeSet<_>>();
    let mut indegree = active_ids
        .iter()
        .map(|id| (id.clone(), 0_usize))
        .collect::<BTreeMap<_, _>>();
    let mut dependents = BTreeMap::<String, Vec<String>>::new();
    for candidate in candidates.iter().filter(|candidate| candidate.active) {
        for dependency in candidate.manifest.dependencies.keys() {
            if active_ids.contains(dependency) {
                *indegree.get_mut(&candidate.manifest.id).expect("active id") += 1;
                dependents
                    .entry(dependency.clone())
                    .or_default()
                    .push(candidate.manifest.id.clone());
            }
        }
    }
    let mut ready = indegree
        .iter()
        .filter_map(|(id, degree)| (*degree == 0).then_some(id.clone()))
        .collect::<VecDeque<_>>();
    let mut order = Vec::with_capacity(active_ids.len());
    while let Some(id) = ready.pop_front() {
        order.push(id.clone());
        if let Some(children) = dependents.get(&id) {
            for child in children {
                let degree = indegree.get_mut(child).expect("dependent id");
                *degree -= 1;
                if *degree == 0 {
                    ready.push_back(child.clone());
                }
            }
        }
    }
    if order.len() != active_ids.len() {
        let cycle_ids = active_ids
            .difference(&order.iter().cloned().collect())
            .cloned()
            .collect::<BTreeSet<_>>();
        for candidate in candidates
            .iter_mut()
            .filter(|candidate| cycle_ids.contains(&candidate.manifest.id))
        {
            deactivate(candidate, "dependency cycle detected");
        }
    }
    order
}

fn effective(candidate: Candidate) -> EffectiveExtension {
    let enabled = candidate
        .state
        .enabled
        .unwrap_or(candidate.manifest.enabled);
    let settings = candidate
        .manifest
        .config
        .iter()
        .filter_map(|(key, field)| {
            candidate
                .state
                .config
                .get(key)
                .or(field.default.as_ref())
                .map(|value| {
                    (
                        key.clone(),
                        if field.secret {
                            "<redacted>".to_string()
                        } else {
                            config_value_string(value)
                        },
                    )
                })
        })
        .collect();
    EffectiveExtension {
        id: candidate.manifest.id.clone(),
        name: candidate
            .manifest
            .name
            .clone()
            .unwrap_or_else(|| candidate.manifest.id.clone()),
        version: candidate.manifest.version,
        description: candidate.manifest.description,
        scope: candidate.scope,
        requested_access: candidate.manifest.runtime_access,
        manifest_path: candidate.manifest_path,
        enabled,
        active: candidate.active,
        reason: candidate.reason,
        dependencies: candidate.manifest.dependencies,
        skill_roots: candidate.roots,
        servers: candidate
            .manifest
            .mcp
            .keys()
            .map(|name| format!("{}__{name}", candidate.manifest.id))
            .collect(),
        workflow_names: candidate.manifest.workflows.keys().cloned().collect(),
        workflow_runtimes: candidate
            .manifest
            .workflows
            .iter()
            .map(|(name, workflow)| (name.clone(), workflow.runtime.label().to_string()))
            .collect(),
        api: candidate.manifest.api,
        workflows: candidate.workflows,
        settings,
    }
}

fn deactivate(candidate: &mut Candidate, reason: impl Into<String>) {
    if candidate.active {
        candidate.active = false;
        candidate.reason = Some(reason.into());
    }
}

fn manifest_paths(dir: &Path) -> Result<Vec<PathBuf>> {
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut paths = Vec::new();
    let mut entries = fs::read_dir(dir)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        let file_type = entry.file_type()?;
        if (file_type.is_file() || (file_type.is_symlink() && path.is_file()))
            && path.extension().and_then(|value| value.to_str()) == Some("toml")
        {
            paths.push(path);
        } else if file_type.is_dir() || (file_type.is_symlink() && path.is_dir()) {
            for name in PACKAGE_MANIFEST_NAMES {
                let manifest = path.join(name);
                if manifest.is_file() {
                    paths.push(manifest);
                    break;
                }
            }
        }
    }
    Ok(paths)
}

fn package_manifest(root: &Path) -> Result<PathBuf> {
    if root.is_file() {
        return Ok(root.to_path_buf());
    }
    for name in PACKAGE_MANIFEST_NAMES {
        let path = root.join(name);
        if path.is_file() {
            return Ok(path);
        }
    }
    let legacy = manifest_paths(root)?;
    ensure!(
        legacy.len() == 1,
        "package must contain exactly one blu.toml, extension.toml, or legacy TOML manifest"
    );
    Ok(legacy[0].clone())
}

fn load_state(path: Option<&Path>) -> Result<BluState> {
    let Some(path) = path.filter(|path| path.is_file()) else {
        return Ok(BluState {
            state_version: STATE_VERSION,
            ..BluState::default()
        });
    };
    let state: BluState = toml::from_str(&fs::read_to_string(path)?)
        .with_context(|| format!("invalid Blu state {}", path.display()))?;
    ensure!(
        state.state_version == STATE_VERSION,
        "unsupported Blu state version {} in {}",
        state.state_version,
        path.display()
    );
    Ok(state)
}

fn load_state_for_discovery(
    path: Option<&Path>,
    diagnostics: &mut Vec<ExtensionDiagnostic>,
    digest: &mut Sha256,
) -> BluState {
    let Some(path) = path.filter(|path| path.is_file()) else {
        return BluState {
            state_version: STATE_VERSION,
            ..BluState::default()
        };
    };
    if let Err(error) = hash_file(digest, path) {
        diagnostics.push(ExtensionDiagnostic {
            level: ExtensionDiagnosticLevel::Error,
            path: path.to_path_buf(),
            message: format!("could not read Blu state: {error:#}"),
        });
        return BluState {
            state_version: STATE_VERSION,
            ..BluState::default()
        };
    }
    match load_state(Some(path)) {
        Ok(state) => state,
        Err(error) => {
            diagnostics.push(ExtensionDiagnostic {
                level: ExtensionDiagnosticLevel::Error,
                path: path.to_path_buf(),
                message: format!("{error:#}"),
            });
            BluState {
                state_version: STATE_VERSION,
                ..BluState::default()
            }
        }
    }
}

fn write_state(path: &Path, state: &BluState) -> Result<()> {
    let parent = path.parent().context("Blu state path has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(".blu.{}.tmp", Uuid::new_v4()));
    fs::write(&temporary, toml::to_string_pretty(state)?)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(windows))]
    fs::rename(&temporary, path)
        .with_context(|| format!("atomically replace {}", path.display()))?;
    #[cfg(windows)]
    {
        let backup = parent.join(format!(".blu.{}.backup", Uuid::new_v4()));
        if path.exists() {
            fs::rename(path, &backup)
                .with_context(|| format!("stage existing Blu state {}", path.display()))?;
        }
        if let Err(error) = fs::rename(&temporary, path) {
            if backup.exists() {
                let _ = fs::rename(&backup, path);
            }
            return Err(error).with_context(|| format!("replace Blu state {}", path.display()));
        }
        if backup.exists() {
            fs::remove_file(backup)?;
        }
    }
    Ok(())
}

pub(crate) fn set_enabled(cwd: &Path, id: &str, project: bool, enabled: bool) -> Result<PathBuf> {
    ensure!(valid_id(id), "invalid extension id `{id}`");
    let _ = manifest_in_scope(cwd, id, project)?;
    let path = scope_state_path(cwd, project)?;
    let mut state = load_state(Some(&path))?;
    state.state_version = STATE_VERSION;
    state.extensions.entry(id.to_string()).or_default().enabled = Some(enabled);
    write_state(&path, &state)?;
    Ok(path)
}

pub(crate) fn configure(
    cwd: &Path,
    id: &str,
    key: &str,
    value: Option<toml::Value>,
    project: bool,
) -> Result<PathBuf> {
    ensure!(valid_id(id), "invalid extension id `{id}`");
    ensure!(valid_id(key), "invalid extension setting `{key}`");
    let (_, manifest) = manifest_in_scope(cwd, id, project)?;
    let field = manifest
        .config
        .get(key)
        .with_context(|| format!("extension `{id}` does not declare setting `{key}`"))?;
    if let Some(value) = &value {
        ensure!(
            config_type_matches(&field.kind, value),
            "extension `{id}` setting `{key}` must be {}",
            field.kind
        );
    } else {
        ensure!(
            !field.required || field.default.is_some(),
            "extension `{id}` setting `{key}` is required and has no default"
        );
    }
    let path = scope_state_path(cwd, project)?;
    let mut state = load_state(Some(&path))?;
    state.state_version = STATE_VERSION;
    let extension = state.extensions.entry(id.to_string()).or_default();
    match value {
        Some(value) => {
            extension.config.insert(key.to_string(), value);
        }
        None => {
            extension.config.remove(key);
        }
    }
    write_state(&path, &state)?;
    Ok(path)
}

fn manifest_in_scope(cwd: &Path, id: &str, project: bool) -> Result<(PathBuf, Manifest)> {
    let directory = scope_extensions_dir(cwd, project)?;
    for path in manifest_paths(&directory)? {
        let text = fs::read_to_string(&path)?;
        let manifest: Manifest = match toml::from_str(&text) {
            Ok(manifest) => manifest,
            Err(_) => continue,
        };
        if manifest.id == id {
            return Ok((path, manifest));
        }
    }
    bail!(
        "extension `{id}` is not installed in the {} scope",
        if project { "project" } else { "user" }
    )
}

pub(crate) fn parse_config_value(value: &str) -> toml::Value {
    #[derive(Deserialize)]
    struct Wrapper {
        value: toml::Value,
    }
    toml::from_str::<Wrapper>(&format!("value = {value}"))
        .map(|wrapper| wrapper.value)
        .unwrap_or_else(|_| toml::Value::String(value.to_string()))
}

pub(crate) fn install(cwd: &Path, source: &str, project: bool, force: bool) -> Result<String> {
    install_expected(cwd, source, project, force, None)
}

fn install_expected(
    cwd: &Path,
    source: &str,
    project: bool,
    force: bool,
    expected_id: Option<&str>,
) -> Result<String> {
    let (package, source_record, _temporary) = materialize_source(source)?;
    let manifest_path = package_manifest(&package)?;
    let scope = if project {
        ExtensionScope::Project
    } else {
        ExtensionScope::User
    };
    let mut source_candidate = load_candidate(&manifest_path, scope, None)?;
    let source_manifest = source_candidate.manifest.clone();
    validate_candidate_declarations(&mut source_candidate, &source_manifest)?;
    if let Some(expected_id) = expected_id {
        ensure!(
            source_candidate.manifest.id == expected_id,
            "updated package changed id from `{expected_id}` to `{}`",
            source_candidate.manifest.id
        );
    }
    let manifest = source_candidate.manifest;
    let extensions_dir = scope_extensions_dir(cwd, project)?;
    fs::create_dir_all(&extensions_dir)?;
    let destination = extensions_dir.join(&manifest.id);
    ensure!(
        force || !destination.exists(),
        "extension `{}` is already installed; pass --force to replace it",
        manifest.id
    );
    let staging = extensions_dir.join(format!(".{}.install-{}", manifest.id, Uuid::new_v4()));
    if let Err(error) = copy_package(&package, &staging) {
        let _ = fs::remove_dir_all(&staging);
        return Err(error).context("stage Blu package");
    }
    // Validate the staged package before replacing the live directory.
    let staged_validation = (|| -> Result<()> {
        let staged_manifest = package_manifest(&staging)?;
        let mut staged_candidate = load_candidate(&staged_manifest, scope, None)?;
        let staged_manifest = staged_candidate.manifest.clone();
        validate_candidate_declarations(&mut staged_candidate, &staged_manifest)?;
        ensure!(
            staged_candidate.manifest.id == manifest.id,
            "staged package id changed during copy"
        );
        Ok(())
    })();
    if let Err(error) = staged_validation {
        let _ = fs::remove_dir_all(&staging);
        return Err(error).context("validate staged Blu package");
    }
    let state_path = scope_state_path(cwd, project)?;
    let mut state = match load_state(Some(&state_path)) {
        Ok(state) => state,
        Err(error) => {
            let _ = fs::remove_dir_all(&staging);
            return Err(error);
        }
    };
    state.state_version = STATE_VERSION;
    state.sources.insert(manifest.id.clone(), source_record);
    let backup = extensions_dir.join(format!(".{}.backup-{}", manifest.id, Uuid::new_v4()));
    if destination.exists()
        && let Err(error) = fs::rename(&destination, &backup)
    {
        let _ = fs::remove_dir_all(&staging);
        return Err(error).context("stage previous Blu package");
    }
    if let Err(error) = fs::rename(&staging, &destination) {
        if backup.exists() {
            let _ = fs::rename(&backup, &destination);
        }
        let _ = fs::remove_dir_all(&staging);
        return Err(error).context("activate staged Blu package");
    }
    if let Err(error) = write_state(&state_path, &state) {
        let _ = fs::remove_dir_all(&destination);
        if backup.exists() {
            let _ = fs::rename(&backup, &destination);
        }
        return Err(error).context("record Blu install state; restored previous package");
    }
    if backup.exists()
        && let Err(error) = fs::remove_dir_all(&backup)
    {
        tracing::warn!(path = %backup.display(), %error, "could not remove replaced Blu package backup");
    }
    Ok(manifest.id)
}

pub(crate) fn update(cwd: &Path, id: Option<&str>, project: bool) -> Result<Vec<String>> {
    let state_path = scope_state_path(cwd, project)?;
    let state = load_state(Some(&state_path))?;
    let targets = match id {
        Some(id) => vec![(
            id.to_string(),
            state
                .sources
                .get(id)
                .cloned()
                .with_context(|| format!("extension `{id}` has no recorded install source"))?,
        )],
        None => state
            .sources
            .iter()
            .filter(|(_, source)| source.kind == "git")
            .map(|(id, source)| (id.clone(), source.clone()))
            .collect(),
    };
    let mut updated = Vec::new();
    for (id, source) in targets {
        ensure!(
            source.kind == "git",
            "extension `{id}` is local and cannot be updated automatically"
        );
        install_expected(cwd, &source.location, project, true, Some(&id))?;
        updated.push(id);
    }
    Ok(updated)
}

pub(crate) fn remove(cwd: &Path, id: &str, project: bool) -> Result<PathBuf> {
    ensure!(valid_id(id), "invalid extension id `{id}`");
    let extensions_dir = scope_extensions_dir(cwd, project)?;
    let package = extensions_dir.join(id);
    let legacy = extensions_dir.join(format!("{id}.toml"));
    let removed = if package.is_dir() {
        package
    } else if legacy.is_file() {
        legacy
    } else {
        bail!("extension `{id}` is not installed in this scope");
    };
    let state_path = scope_state_path(cwd, project)?;
    let mut state = load_state(Some(&state_path))?;
    state.extensions.remove(id);
    state.sources.remove(id);
    let staged = extensions_dir.join(format!(".{id}.remove-{}", Uuid::new_v4()));
    fs::rename(&removed, &staged)?;
    if let Err(error) = write_state(&state_path, &state) {
        let _ = fs::rename(&staged, &removed);
        return Err(error).context("record Blu removal state; restored package");
    }
    if staged.is_dir() {
        if let Err(error) = fs::remove_dir_all(&staged) {
            tracing::warn!(path = %staged.display(), %error, "could not remove staged Blu package");
        }
    } else {
        if let Err(error) = fs::remove_file(&staged) {
            tracing::warn!(path = %staged.display(), %error, "could not remove staged Blu manifest");
        }
    }
    Ok(removed)
}

pub(crate) fn scaffold(cwd: &Path, id: &str, version: &str, project: bool) -> Result<PathBuf> {
    ensure!(valid_id(id), "invalid extension id `{id}`");
    Version::parse(version).context("--version must be a semantic version")?;
    let package = scope_extensions_dir(cwd, project)?.join(id);
    ensure!(
        !package.exists(),
        "extension package already exists: {}",
        package.display()
    );
    fs::create_dir_all(package.join("skills").join(id))?;
    let manifest = format!(
        "manifest_version = 1\nid = \"{id}\"\nname = \"{id}\"\nversion = \"{version}\"\ndescription = \"Describe this Blu extension\"\nenabled = true\nskill_roots = [\"skills\"]\n"
    );
    fs::write(package.join("blu.toml"), manifest)?;
    fs::write(
        package.join("skills").join(id).join("SKILL.md"),
        format!(
            "---\nname: {id}\ndescription: Describe when this extension applies.\n---\n\n# {id}\n\nAdd focused agent instructions here.\n"
        ),
    )?;
    Ok(package)
}

pub(crate) fn touch_reload(cwd: &Path) -> Result<PathBuf> {
    let path = cwd.join(".borg/blu.reload");
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&path, format!("{}\n", chrono::Utc::now().to_rfc3339()))?;
    Ok(path)
}

fn materialize_source(source: &str) -> Result<(PathBuf, SourceRecord, Option<tempfile::TempDir>)> {
    let local = PathBuf::from(source);
    if local.exists() {
        let canonical = local.canonicalize()?;
        if canonical.is_file()
            && !PACKAGE_MANIFEST_NAMES
                .iter()
                .any(|name| canonical.file_name().and_then(|value| value.to_str()) == Some(name))
        {
            let temporary = tempfile::tempdir()?;
            let package = temporary.path().join("package");
            fs::create_dir_all(&package)?;
            fs::copy(&canonical, package.join("blu.toml"))?;
            return Ok((
                package,
                SourceRecord {
                    kind: "local".to_string(),
                    location: canonical.display().to_string(),
                    revision: None,
                },
                Some(temporary),
            ));
        }
        let package = if canonical.is_file() {
            canonical
                .parent()
                .context("manifest source has no parent")?
                .to_path_buf()
        } else {
            canonical.clone()
        };
        return Ok((
            package,
            SourceRecord {
                kind: "local".to_string(),
                location: canonical.display().to_string(),
                revision: None,
            },
            None,
        ));
    }
    ensure!(
        looks_like_git_source(source),
        "source is not a local path or Git URL"
    );
    let location = source.strip_prefix("git+").unwrap_or(source);
    let temporary = tempfile::tempdir()?;
    let package = temporary.path().join("package");
    let output = Command::new("git")
        .args(["clone", "--depth", "1", "--", location])
        .arg(&package)
        .output()
        .context("run git clone")?;
    ensure!(
        output.status.success(),
        "git clone failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    let revision = Command::new("git")
        .args(["-C"])
        .arg(&package)
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string());
    Ok((
        package,
        SourceRecord {
            kind: "git".to_string(),
            location: source.to_string(),
            revision,
        },
        Some(temporary),
    ))
}

fn copy_package(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let name = entry.file_name();
        if name == ".git" || name == "target" {
            continue;
        }
        let file_type = entry.file_type()?;
        let target = destination.join(&name);
        if file_type.is_symlink() {
            bail!(
                "Blu packages may not contain symlinks: {}",
                entry.path().display()
            );
        } else if file_type.is_dir() {
            copy_package(&entry.path(), &target)?;
        } else if file_type.is_file() {
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

fn looks_like_git_source(source: &str) -> bool {
    source.starts_with("git+")
        || source.starts_with("https://")
        || source.starts_with("http://")
        || source.starts_with("ssh://")
        || source.starts_with("git@")
}

fn scope_extensions_dir(cwd: &Path, project: bool) -> Result<PathBuf> {
    Ok(if project {
        cwd.join(".borg/extensions")
    } else {
        user_config_root()?.join("extensions")
    })
}

fn scope_state_path(cwd: &Path, project: bool) -> Result<PathBuf> {
    Ok(if project {
        cwd.join(".borg/blu.toml")
    } else {
        user_config_root()?.join("blu.toml")
    })
}

fn user_config_root() -> Result<PathBuf> {
    user_config_base(
        std::env::var_os("XDG_CONFIG_HOME").map(PathBuf::from),
        std::env::var_os("HOME").map(PathBuf::from),
        dirs::config_dir(),
    )
    .map(|root| root.join("borg"))
    .context("unable to determine a config directory for user extensions")
}

fn user_config_base(
    xdg_config_home: Option<PathBuf>,
    home: Option<PathBuf>,
    platform_config_dir: Option<PathBuf>,
) -> Option<PathBuf> {
    xdg_config_home
        .or_else(|| home.map(|home| home.join(".config")))
        .or(platform_config_dir)
}

fn validate_relative_path(path: &Path, label: &str) -> Result<()> {
    ensure!(!path.as_os_str().is_empty(), "{label} must not be empty");
    ensure!(!path.is_absolute(), "{label} must be relative");
    ensure!(
        !path.components().any(|component| matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )),
        "invalid {label}"
    );
    Ok(())
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn valid_env_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn valid_allowed_tool(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn cap(config: &CapabilityConfig, name: &str) -> bool {
    match name {
        "multiplayer" => config.multiplayer,
        "subagents" => config.subagents,
        "autonomous_team" => config.autonomous_team,
        "shared_work" => config.shared_work,
        "presence" => config.presence,
        "cloud_sync" => config.cloud_sync,
        "web_relay" => config.web_relay,
        "telemetry" => config.telemetry,
        _ => false,
    }
}

fn known_capability(name: &str) -> bool {
    matches!(
        name,
        "multiplayer"
            | "subagents"
            | "autonomous_team"
            | "shared_work"
            | "presence"
            | "cloud_sync"
            | "web_relay"
            | "telemetry"
    )
}

fn hash_file(digest: &mut Sha256, path: &Path) -> Result<()> {
    digest.update(path.to_string_lossy().as_bytes());
    digest.update(fs::read(path)?);
    Ok(())
}

fn hash_directory_signature(digest: &mut Sha256, path: &Path) -> Result<()> {
    digest.update(b"directory-signature-v1");
    digest.update(path.to_string_lossy().as_bytes());
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            digest.update(b"missing");
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    let mut entries = entries.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let metadata = entry.metadata()?;
        digest.update(entry.file_name().to_string_lossy().as_bytes());
        digest.update(metadata.len().to_le_bytes());
        digest.update([metadata.is_dir() as u8, metadata.is_file() as u8]);
    }
    Ok(())
}

fn hash_package_tree(digest: &mut Sha256, root: &Path) -> Result<()> {
    let mut entries = fs::read_dir(root)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        let name = entry.file_name();
        if name == ".git" || name == "target" {
            continue;
        }
        let metadata = fs::symlink_metadata(&path)?;
        digest.update(path.to_string_lossy().as_bytes());
        digest.update(metadata.len().to_le_bytes());
        digest.update([metadata.is_dir() as u8, metadata.is_file() as u8]);
        if metadata.is_dir() {
            hash_package_tree(digest, &path)?;
        } else if metadata.is_file() {
            digest.update(fs::read(&path)?);
        } else if metadata.file_type().is_symlink() {
            digest.update(fs::read_link(&path)?.to_string_lossy().as_bytes());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_config_base_falls_back_to_the_native_platform_directory() {
        let platform = PathBuf::from(r"C:\Users\borg\AppData\Roaming");
        assert_eq!(
            user_config_base(None, None, Some(platform.clone())),
            Some(platform)
        );
    }

    fn manifest(id: &str, extra: &str) -> String {
        format!(
            "manifest_version = 1\nid = \"{id}\"\nversion = \"1.0.0\"\nskill_roots = [\"skills\"]\n{extra}\n[mcp.docs]\ncommand = \"docs-mcp\"\nargs = [\"serve\"]\n"
        )
    }

    fn package(root: &Path, id: &str, extra: &str) {
        let package = root.join(id);
        fs::create_dir_all(package.join("skills")).unwrap();
        fs::write(package.join("blu.toml"), manifest(id, extra)).unwrap();
    }

    fn discover_test(
        project: Option<PathBuf>,
        user: Option<PathBuf>,
        trusted: bool,
    ) -> (ExtensionCatalog, Vec<borg_provider::mcp::ExternalMcpServer>) {
        let extension_config = ExtensionConfig {
            allow_project_mcp: trusted,
            project_access: if trusted {
                ExtensionAccess::Trusted
            } else {
                ExtensionAccess::Sandboxed
            },
            ..ExtensionConfig::default()
        };
        discover_in_dirs(
            project,
            user,
            None,
            None,
            &CapabilityConfig::default(),
            &extension_config,
        )
        .map(|(catalog, servers, _)| (catalog, servers))
        .unwrap()
    }

    #[test]
    fn trusted_package_loads_skills_and_namespaced_server() {
        let root = tempfile::tempdir().unwrap();
        package(
            root.path(),
            "docs",
            "required_capabilities = [\"multiplayer\"]",
        );
        let (catalog, servers) = discover_test(Some(root.path().to_path_buf()), None, true);
        assert!(catalog.extensions[0].active);
        assert_eq!(catalog.load_order, ["docs"]);
        assert_eq!(servers[0].name, "docs__docs");
        assert_eq!(catalog.active_skill_roots().len(), 1);
    }

    #[test]
    fn native_package_waits_for_user_policy_before_loading() {
        let root = tempfile::tempdir().unwrap();
        let package = root.path().join("native-example");
        fs::create_dir_all(&package).unwrap();
        let library = package.join("example.bin");
        fs::write(&library, b"not loaded while approval is pending").unwrap();
        let sha256 = hex::encode(Sha256::digest(fs::read(&library).unwrap()));
        fs::write(
            package.join("blu.toml"),
            format!(
                r#"manifest_version = 1
id = "native-example"
version = "1.0.0"
runtime_access = "native"

[native]
library = "example.bin"
sha256 = "{sha256}"
abi_version = 1
"#
            ),
        )
        .unwrap();
        let policy = ExtensionConfig {
            default_access: ExtensionAccess::Native,
            native_access: NativeAccessPolicy::Prompt,
            ..ExtensionConfig::default()
        };
        let (catalog, _, _) = discover_in_dirs(
            None,
            Some(root.path().to_path_buf()),
            None,
            None,
            &CapabilityConfig::default(),
            &policy,
        )
        .unwrap();
        assert_eq!(catalog.extensions.len(), 1);
        assert!(!catalog.extensions[0].active);
        assert!(
            catalog.extensions[0]
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("awaits approval"))
        );
    }

    #[test]
    fn native_v2_event_boundary_accepts_only_bounded_json() {
        let valid = CString::new(r#"{"event":"ready"}"#).unwrap();
        let invalid = CString::new("not-json").unwrap();
        // SAFETY: both C strings remain alive for the duration of each call.
        unsafe {
            assert_eq!(native_emit_event(valid.as_ptr()), 0);
            assert_eq!(native_emit_event(invalid.as_ptr()), 3);
            assert_eq!(native_emit_event(std::ptr::null()), 1);
        }
    }

    #[test]
    fn executable_workflows_are_discovered_as_part_of_the_active_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let package = root.path().join("workflow");
        fs::create_dir_all(package.join("skills")).unwrap();
        fs::create_dir_all(package.join("workflows")).unwrap();
        fs::write(
            package.join("blu.toml"),
            r#"
manifest_version = 1
id = "workflow"
version = "1.0.0"
skill_roots = ["skills"]

[workflows.review]
entrypoint = "workflows/review.blu"
description = "Review the current change"
"#,
        )
        .unwrap();
        fs::write(
            package.join("workflows/review.blu"),
            "borg_emit(\"audit\", \"extension.review\", \"{}\")",
        )
        .unwrap();

        let (catalog, servers, workflows) = discover_in_dirs(
            None,
            Some(root.path().to_path_buf()),
            None,
            None,
            &CapabilityConfig::default(),
            &ExtensionConfig::default(),
        )
        .unwrap();
        assert!(servers.is_empty());
        assert!(catalog.extensions[0].active);
        assert_eq!(catalog.active_workflows().len(), 1);
        assert_eq!(workflows[0].extension_id, "workflow");
        assert_eq!(workflows[0].name, "review");
        assert_eq!(
            workflows[0].description.as_deref(),
            Some("Review the current change")
        );
        assert_eq!(
            workflows[0].source,
            "borg_emit(\"audit\", \"extension.review\", \"{}\")"
        );
        assert_eq!(workflows[0].runtime, borg_remote::WorkflowRuntime::Blu);
    }

    #[test]
    fn api_manifest_becomes_a_validated_provider_neutral_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let package = root.path().join("api");
        fs::create_dir_all(package.join("skills")).unwrap();
        fs::create_dir_all(package.join("workflows")).unwrap();
        fs::write(
            package.join("blu.toml"),
            r#"
manifest_version = 1
id = "api"
version = "1.0.0"
skill_roots = ["skills"]

[workflows.review]
entrypoint = "workflows/review.blu"

[api]
version = 1
[api.transforms.concise]
append_system_prompt = "Use concise notes."
append_context = "Keep the release checklist in view."
[api.hooks.after_turn]
event = "turn_completed"
workflow = "review"
[api.tools.review]
workflow = "review"
description = "Review the change"
input_schema = { type = "object" }
[api.commands.review]
workflow = "review"
description = "Run review"
"#,
        )
        .unwrap();
        fs::write(
            package.join("workflows/review.blu"),
            "return borg_workflow_arguments(1)",
        )
        .unwrap();
        let (catalog, _, _) = discover_in_dirs(
            None,
            Some(root.path().to_path_buf()),
            None,
            None,
            &CapabilityConfig::default(),
            &ExtensionConfig::default(),
        )
        .unwrap();
        assert!(!catalog.has_errors(), "{:#?}", catalog.diagnostics);
        let snapshot = catalog.api_snapshot();
        assert_eq!(snapshot.api_version, borg_remote::EXTENSION_API_VERSION);
        assert_eq!(snapshot.transforms.len(), 1);
        assert_eq!(
            snapshot.transforms[0].append_context,
            "Keep the release checklist in view."
        );
        assert_eq!(snapshot.hooks[0].event, "turn_completed");
        assert_eq!(snapshot.tools[0].wire_name, "ext__api__review");
        assert_eq!(snapshot.commands[0].name, "review");
        assert_eq!(snapshot.command_wires(), ["extcmd__api__review"]);
    }

    #[test]
    fn external_runtime_workflows_are_discovered_with_command_and_package_context() {
        let root = tempfile::tempdir().unwrap();
        let package = root.path().join("analysis");
        fs::create_dir_all(package.join("skills")).unwrap();
        fs::create_dir_all(package.join("workflows")).unwrap();
        fs::write(
            package.join("blu.toml"),
            r#"
manifest_version = 1
id = "analysis"
version = "1.0.0"
skill_roots = ["skills"]

[workflows.inspect]
runtime = "ipython"
entrypoint = "workflows/inspect.py"
command = "ipython"
args = ["--no-banner"]
"#,
        )
        .unwrap();
        fs::write(package.join("workflows/inspect.py"), "print('ok')").unwrap();

        let (catalog, _, workflows) = discover_in_dirs(
            None,
            Some(root.path().to_path_buf()),
            None,
            None,
            &CapabilityConfig::default(),
            &ExtensionConfig::default(),
        )
        .unwrap();
        assert!(!catalog.has_errors(), "{:#?}", catalog.diagnostics);
        assert_eq!(workflows[0].runtime, borg_remote::WorkflowRuntime::Ipython);
        assert_eq!(workflows[0].command.as_deref(), Some("ipython"));
        assert_eq!(workflows[0].args, ["--no-banner"]);
        assert_eq!(
            workflows[0].working_directory,
            package.canonicalize().unwrap()
        );
        assert_eq!(
            workflows[0].entrypoint,
            package.join("workflows/inspect.py").canonicalize().unwrap()
        );
        assert_eq!(
            catalog.extensions[0].workflow_runtimes["inspect"],
            "ipython"
        );
    }

    #[test]
    fn blu_owns_lua_and_luau_workflow_entrypoints() {
        let root = tempfile::tempdir().unwrap();
        let package = root.path().join("lua-family");
        fs::create_dir_all(package.join("skills")).unwrap();
        fs::create_dir_all(package.join("workflows")).unwrap();
        fs::write(
            package.join("blu.toml"),
            r#"
manifest_version = 1
id = "lua-family"
version = "1.0.0"
skill_roots = ["skills"]

[workflows.lua]
runtime = "blu"
entrypoint = "workflows/lua.lua"

[workflows.luau]
runtime = "blu"
entrypoint = "workflows/luau.luau"
"#,
        )
        .unwrap();
        fs::write(package.join("workflows/lua.lua"), "return 42").unwrap();
        fs::write(package.join("workflows/luau.luau"), "return 42").unwrap();

        let (catalog, _, workflows) = discover_in_dirs(
            None,
            Some(root.path().to_path_buf()),
            None,
            None,
            &CapabilityConfig::default(),
            &ExtensionConfig::default(),
        )
        .unwrap();
        assert!(!catalog.has_errors(), "{:#?}", catalog.diagnostics);
        assert_eq!(workflows.len(), 2);
        assert!(
            workflows
                .iter()
                .all(|workflow| workflow.runtime == borg_remote::WorkflowRuntime::Blu)
        );
        assert_eq!(catalog.extensions[0].workflow_runtimes["lua"], "blu");
        assert_eq!(catalog.extensions[0].workflow_runtimes["luau"], "blu");
    }

    #[test]
    fn invalid_package_is_isolated_without_bricking_valid_catalog() {
        let root = tempfile::tempdir().unwrap();
        package(root.path(), "good", "");
        fs::write(root.path().join("broken.toml"), "not = [valid").unwrap();
        let (catalog, servers) = discover_test(None, Some(root.path().to_path_buf()), false);
        assert_eq!(catalog.extensions.len(), 1);
        assert_eq!(catalog.diagnostics.len(), 1);
        assert_eq!(servers.len(), 1);
    }

    #[test]
    fn invalid_state_is_reported_without_bricking_valid_packages() {
        let root = tempfile::tempdir().unwrap();
        package(root.path(), "good", "");
        let state_path = root.path().join("state.toml");
        fs::write(&state_path, "not = [valid").unwrap();
        let (catalog, servers, _) = discover_in_dirs(
            None,
            Some(root.path().to_path_buf()),
            None,
            Some(state_path),
            &CapabilityConfig::default(),
            &ExtensionConfig::default(),
        )
        .unwrap();
        assert_eq!(catalog.extensions.len(), 1);
        assert!(catalog.has_errors());
        assert_eq!(servers.len(), 1);
    }

    #[test]
    fn package_content_and_root_changes_advance_live_revision() {
        let root = tempfile::tempdir().unwrap();
        package(root.path(), "watched", "");
        let (first, _) = discover_test(None, Some(root.path().to_path_buf()), false);
        fs::write(root.path().join("watched/skills/SKILL.md"), "changed").unwrap();
        let (second, _) = discover_test(None, Some(root.path().to_path_buf()), false);
        assert_ne!(first.revision, second.revision);

        fs::remove_dir_all(root.path().join("watched/skills")).unwrap();
        let (third, _) = discover_test(None, Some(root.path().to_path_buf()), false);
        assert_ne!(second.revision, third.revision);
        assert!(third.has_errors());
    }

    #[test]
    fn missing_runtime_setting_keeps_extension_visible_and_inactive() {
        let root = tempfile::tempdir().unwrap();
        package(
            root.path(),
            "configured",
            "[config.token]\ntype=\"string\"\nrequired=true",
        );
        let (catalog, servers) = discover_test(None, Some(root.path().to_path_buf()), false);
        assert_eq!(catalog.extensions.len(), 1);
        assert!(!catalog.extensions[0].active);
        assert!(
            catalog.extensions[0]
                .reason
                .as_deref()
                .unwrap()
                .contains("requires setting `token`")
        );
        assert!(!catalog.has_errors());
        assert!(servers.is_empty());
    }

    #[test]
    fn project_package_shadows_user_and_requires_explicit_trust() {
        let project = tempfile::tempdir().unwrap();
        let user = tempfile::tempdir().unwrap();
        package(project.path(), "same", "");
        package(user.path(), "same", "");
        let (catalog, servers) = discover_test(
            Some(project.path().to_path_buf()),
            Some(user.path().to_path_buf()),
            false,
        );
        assert_eq!(catalog.extensions.len(), 1);
        assert!(!catalog.extensions[0].active);
        assert_eq!(catalog.extensions[0].scope, ExtensionScope::Project);
        assert!(servers.is_empty());
        assert_eq!(catalog.diagnostics.len(), 1);
        assert_eq!(
            catalog.diagnostics[0].level,
            ExtensionDiagnosticLevel::Warning
        );
        assert!(!catalog.has_errors());
    }

    #[test]
    fn dependencies_are_version_checked_and_loaded_first() {
        let root = tempfile::tempdir().unwrap();
        package(root.path(), "base", "");
        package(root.path(), "consumer", "[dependencies]\nbase = \"^1\"");
        let (catalog, _) = discover_test(None, Some(root.path().to_path_buf()), false);
        assert_eq!(catalog.load_order, ["base", "consumer"]);
        assert!(catalog.extensions.iter().all(|extension| extension.active));
    }

    #[test]
    fn dependency_cycles_are_inactive() {
        let root = tempfile::tempdir().unwrap();
        package(root.path(), "one", "[dependencies]\ntwo = \"*\"");
        package(root.path(), "two", "[dependencies]\none = \"*\"");
        let (catalog, servers) = discover_test(None, Some(root.path().to_path_buf()), false);
        assert!(catalog.extensions.iter().all(|extension| !extension.active));
        assert!(servers.is_empty());
    }

    #[test]
    fn typed_settings_render_into_server_configuration_and_secrets_are_redacted() {
        let root = tempfile::tempdir().unwrap();
        let package = root.path().join("configured");
        fs::create_dir_all(package.join("skills")).unwrap();
        fs::write(
            package.join("blu.toml"),
            "manifest_version=1\nid=\"configured\"\nversion=\"1.0.0\"\nskill_roots=[\"skills\"]\n[config.token]\ntype=\"string\"\nrequired=true\nsecret=true\n[mcp.api]\ncommand=\"api-mcp\"\nenv={ TOKEN=\"${config.token}\" }\n",
        )
        .unwrap();
        let state_path = root.path().join("state.toml");
        fs::write(
            &state_path,
            "state_version=1\n[extensions.configured.config]\ntoken=\"secret\"\n",
        )
        .unwrap();
        let (catalog, servers, _) = discover_in_dirs(
            None,
            Some(root.path().to_path_buf()),
            None,
            Some(state_path),
            &CapabilityConfig::default(),
            &ExtensionConfig::default(),
        )
        .unwrap();
        assert_eq!(servers[0].env["TOKEN"], "secret");
        assert_eq!(catalog.extensions[0].settings["token"], "<redacted>");
    }

    #[test]
    fn active_packages_customize_editor_keybindings_and_aliases() {
        let workspace = tempfile::tempdir().unwrap();
        let packages = workspace.path().join("extensions");
        package(
            &packages,
            "visuals",
            r##"
[api.editor.transcript]
assistant_label = "friend"
assistant_label_color = "#112233"

[api.keybindings]
send = ["ctrl+enter"]

[api.aliases]
ship = "/fast on"
"##,
        );
        let (catalog, _) = discover_test(None, Some(packages), true);
        assert!(!catalog.has_errors());
        let mut editor = crate::editor_preferences::EditorPreferences::default();
        let mut agent = crate::agent_config::AgentConfig::default();
        catalog
            .apply_editor_customization(&mut editor, &mut agent)
            .unwrap();
        assert_eq!(editor.transcript.assistant_label, "friend");
        assert_eq!(editor.transcript.assistant_label_color, "#112233");
        assert_eq!(agent.keybindings.send, ["ctrl+enter"]);
        assert_eq!(agent.commands.aliases["ship"], "/fast on");
    }

    #[test]
    fn path_escape_and_symlink_escape_are_rejected_per_package() {
        let root = tempfile::tempdir().unwrap();
        let bad = root.path().join("bad");
        fs::create_dir_all(&bad).unwrap();
        fs::write(
            bad.join("blu.toml"),
            "manifest_version=1\nid=\"bad\"\nversion=\"1.0.0\"\nskill_roots=[\"../escape\"]\n",
        )
        .unwrap();
        let (catalog, _) = discover_test(None, Some(root.path().to_path_buf()), false);
        assert!(catalog.extensions.is_empty());
        assert_eq!(catalog.diagnostics.len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_package_root_cannot_escape_extension_directory() {
        use std::os::unix::fs::symlink;

        let extension_dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        package(outside.path(), "outside", "");
        symlink(
            outside.path().join("outside"),
            extension_dir.path().join("linked"),
        )
        .unwrap();

        let (catalog, _) = discover_test(None, Some(extension_dir.path().to_path_buf()), false);

        assert!(catalog.extensions.is_empty());
        assert!(catalog.diagnostics.iter().any(|diagnostic| {
            diagnostic
                .message
                .contains("configured extension directory")
        }));
    }

    #[test]
    fn local_install_is_atomic_and_removable() {
        let source = tempfile::tempdir().unwrap();
        package(source.path(), "sample", "");
        let workspace = tempfile::tempdir().unwrap();
        let id = install(
            workspace.path(),
            source.path().join("sample").to_str().unwrap(),
            true,
            false,
        )
        .unwrap();
        assert_eq!(id, "sample");
        assert!(
            workspace
                .path()
                .join(".borg/extensions/sample/blu.toml")
                .is_file()
        );
        remove(workspace.path(), "sample", true).unwrap();
        assert!(!workspace.path().join(".borg/extensions/sample").exists());
    }

    #[test]
    fn force_install_keeps_previous_package_when_replacement_is_invalid() {
        let good_source = tempfile::tempdir().unwrap();
        package(good_source.path(), "sample", "");
        let workspace = tempfile::tempdir().unwrap();
        install(
            workspace.path(),
            good_source.path().join("sample").to_str().unwrap(),
            true,
            false,
        )
        .unwrap();

        let bad_source = tempfile::tempdir().unwrap();
        let bad_package = bad_source.path().join("sample");
        fs::create_dir_all(&bad_package).unwrap();
        fs::write(
            bad_package.join("blu.toml"),
            "manifest_version=1\nid=\"sample\"\nversion=\"2.0.0\"\nskill_roots=[\"missing\"]\n",
        )
        .unwrap();
        assert!(install(workspace.path(), bad_package.to_str().unwrap(), true, true,).is_err());
        let installed =
            fs::read_to_string(workspace.path().join(".borg/extensions/sample/blu.toml")).unwrap();
        assert!(installed.contains("version = \"1.0.0\""));
    }

    #[test]
    fn standalone_manifest_install_does_not_copy_its_parent_directory() {
        let source = tempfile::tempdir().unwrap();
        let manifest = source.path().join("sample.toml");
        fs::write(
            &manifest,
            "manifest_version=1\nid=\"sample\"\nversion=\"1.0.0\"\n",
        )
        .unwrap();
        fs::write(source.path().join("unrelated-secret"), "do not copy").unwrap();
        let workspace = tempfile::tempdir().unwrap();
        install(workspace.path(), manifest.to_str().unwrap(), true, false).unwrap();
        let installed = workspace.path().join(".borg/extensions/sample");
        assert!(installed.join("blu.toml").is_file());
        assert!(!installed.join("unrelated-secret").exists());
    }
}
