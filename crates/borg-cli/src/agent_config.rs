use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct AgentConfig {
    pub(crate) capabilities: CapabilityConfig,
    pub(crate) extensions: ExtensionConfig,
    pub(crate) team: TeamConfig,
    pub(crate) commands: CommandConfig,
    pub(crate) keybindings: KeybindingConfig,
    pub(crate) mcp: McpConfig,
    pub(crate) approvals: ApprovalConfig,
    pub(crate) updates: UpdateConfig,
    pub(crate) local: LocalProviderConfig,
    /// Named OpenAI-compatible routes. The durable session keeps the generic
    /// native provider kind and records the stable `provider/model` alias.
    pub(crate) providers: BTreeMap<String, ConfiguredProvider>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ConfiguredProvider {
    /// Currently `openai-compatible`; the field makes the config forward
    /// compatible with future provider protocols without hiding semantics.
    pub(crate) protocol: String,
    pub(crate) name: Option<String>,
    /// Base URL or a complete `/chat/completions` URL.
    pub(crate) base_url: String,
    /// Prefer an environment reference for secrets. `api_key` is supported
    /// for local-only setups but is never copied into durable session state.
    pub(crate) api_key_env: Option<String>,
    pub(crate) api_key: Option<String>,
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) models: BTreeMap<String, ConfiguredModel>,
}

impl Default for ConfiguredProvider {
    fn default() -> Self {
        Self {
            protocol: "openai-compatible".to_string(),
            name: None,
            base_url: String::new(),
            api_key_env: None,
            api_key: None,
            headers: BTreeMap::new(),
            models: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ConfiguredModel {
    pub(crate) name: Option<String>,
    pub(crate) context_window_tokens: Option<u64>,
    pub(crate) max_output_tokens: Option<u64>,
    /// Variant names normally match Borg effort values (`low`, `high`, ...).
    pub(crate) variants: BTreeMap<String, ConfiguredModelVariant>,
    /// Extra request fields for this model, such as `temperature` or a vendor
    /// routing object. Core conversation/tool fields are protected by the
    /// provider adapter.
    pub(crate) body: BTreeMap<String, toml::Value>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ConfiguredModelVariant {
    pub(crate) body: BTreeMap<String, toml::Value>,
}

/// Declarative setup for a locally hosted OpenAI-compatible server
/// (`llama-server`, vLLM, Ollama). Without this, pointing Borg at a local
/// model requires exporting `BORG_OPENAI_COMPATIBLE_*` by hand before every
/// run, which is not a configuration story.
///
/// Applied by [`AgentConfig::apply_local_provider_env`]. The process
/// environment always wins, so an explicit export or `--model` still
/// overrides the file. Values injected for one session are restored when that
/// session ends; this prevents a later session from inheriting an old local
/// endpoint or model after its config is reloaded.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct LocalProviderConfig {
    /// Base URL of the OpenAI-compatible endpoint, including `/v1`.
    /// Defaults to `http://127.0.0.1:8000/v1` in the provider when unset.
    pub(crate) base_url: Option<String>,
    /// Model name to request. For Ollama this is the tag (`qwen3.6:35b-a3b`);
    /// for `llama-server` it is whatever `/v1/models` reports.
    pub(crate) model: Option<String>,
    /// Advertised context window. The `Generic` profile reports no context
    /// metadata, so without this Borg cannot size auto-compaction and long
    /// local sessions fail at the wall instead of compacting.
    pub(crate) context_window_tokens: Option<u64>,
    /// Most local servers ignore this; the `Generic` profile is keyless.
    pub(crate) api_key: Option<String>,
    /// Path to a GGUF selected by the local-model catalog. Relative paths are
    /// resolved against the active project directory when a server is started.
    pub(crate) model_path: Option<PathBuf>,
    /// Directories offered to local-model discovery. Discovery owns the scan;
    /// this config is the shared seam for that future/runtime integration.
    pub(crate) model_dirs: Vec<PathBuf>,
    /// Read Ollama's model blobs without starting the Ollama daemon.
    pub(crate) include_ollama_store: bool,
    /// Include the user's Hugging Face cache in local-model discovery.
    pub(crate) include_hf_cache: bool,
    /// Binary used when Borg owns a local llama.cpp-compatible server.
    pub(crate) server_bin: Option<PathBuf>,
    /// Bind host for an owned server when `base_url` does not specify one.
    pub(crate) host: Option<String>,
    /// Bind port for an owned server when `base_url` does not specify one.
    pub(crate) port: Option<u16>,
    /// Start/reuse a local server for OpenAI-compatible sessions. This is
    /// opt-in so existing manually managed endpoints remain unchanged.
    pub(crate) auto_start: bool,
    /// llama-server context size. `context_window_tokens` remains the provider
    /// metadata override; when both are set, this launch setting wins.
    pub(crate) ctx_size: Option<u64>,
    /// Enable llama.cpp's Jinja chat template support for tool calling.
    pub(crate) jinja: bool,
    /// llama.cpp reasoning output format, normally `deepseek` for models that
    /// emit `reasoning_content`. `None` uses the server's default.
    pub(crate) reasoning_format: Option<String>,
}

impl Default for LocalProviderConfig {
    fn default() -> Self {
        Self {
            base_url: None,
            model: None,
            context_window_tokens: None,
            api_key: None,
            model_path: None,
            model_dirs: Vec::new(),
            include_ollama_store: false,
            include_hf_cache: false,
            server_bin: None,
            host: None,
            port: None,
            auto_start: false,
            ctx_size: None,
            jinja: true,
            reasoning_format: Some("deepseek".to_string()),
        }
    }
}

const LOCAL_PROVIDER_ENV_KEYS: [&str; 4] = [
    "BORG_OPENAI_COMPATIBLE_BASE_URL",
    "BORG_OPENAI_COMPATIBLE_MODEL",
    "BORG_OPENAI_COMPATIBLE_API_KEY",
    "BORG_OPENAI_COMPATIBLE_CONTEXT_WINDOW_TOKENS",
];

/// Restores the provider environment snapshot taken before a local session.
/// The guard also covers values written by the optional local-server launcher,
/// which uses the same environment variables to communicate its bound
/// endpoint to the provider adapter.
pub(crate) struct LocalProviderEnvGuard {
    previous: Vec<(&'static str, Option<OsString>)>,
}

impl Drop for LocalProviderEnvGuard {
    fn drop(&mut self) {
        for (key, value) in &self.previous {
            // SAFETY: local sessions are run serially by the CLI. The guard is
            // held for the entire session and restores exactly the snapshot it
            // took before any provider worker was started.
            unsafe {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExtensionAccess {
    Sandboxed,
    #[default]
    Trusted,
    Native,
}

impl ExtensionAccess {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Sandboxed => "sandboxed",
            Self::Trusted => "trusted",
            Self::Native => "native",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NativeAccessPolicy {
    Deny,
    #[default]
    Prompt,
    Allow,
}

/// User-owned trust controls for extension catalogs.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ExtensionConfig {
    /// Compatibility switch for project MCP packages.
    pub(crate) allow_project_mcp: bool,
    pub(crate) default_access: ExtensionAccess,
    pub(crate) project_access: ExtensionAccess,
    pub(crate) native_access: NativeAccessPolicy,
}

impl Default for ExtensionConfig {
    fn default() -> Self {
        Self {
            allow_project_mcp: false,
            default_access: ExtensionAccess::Trusted,
            project_access: ExtensionAccess::Sandboxed,
            native_access: NativeAccessPolicy::Prompt,
        }
    }
}

/// Opt-in autonomous-team policy. Leaving `preset` unset keeps the existing
/// manual subagent coordinator unchanged.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct TeamConfig {
    pub(crate) preset: Option<borg_remote::TeamPreset>,
    pub(crate) worker_concurrency: Option<u32>,
    pub(crate) max_total_assignments: Option<u32>,
    pub(crate) max_total_reports: Option<u32>,
    pub(crate) max_total_escalations: Option<u32>,
    pub(crate) max_specialists: Option<u32>,
    pub(crate) max_tokens: Option<u64>,
    pub(crate) max_cost_microusd: Option<u64>,
    pub(crate) max_wall_time_ms: Option<u64>,
}

/// Provider-neutral feature switches for optional Borg subsystems.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct CapabilityConfig {
    pub(crate) multiplayer: bool,
    pub(crate) subagents: bool,
    pub(crate) autonomous_team: bool,
    pub(crate) shared_work: bool,
    pub(crate) presence: bool,
    pub(crate) cloud_sync: bool,
    pub(crate) web_relay: bool,
    pub(crate) telemetry: bool,
    /// Native-harness tool presentation. `compact` is the focused coding
    /// surface; `both` preserves the legacy catalog while adding programmatic
    /// dispatch.
    pub(crate) tool_mode: borg_remote::ToolMode,
}

impl Default for CapabilityConfig {
    fn default() -> Self {
        Self {
            multiplayer: true,
            subagents: true,
            autonomous_team: true,
            shared_work: true,
            presence: true,
            cloud_sync: true,
            web_relay: true,
            telemetry: false,
            tool_mode: borg_remote::ToolMode::Both,
        }
    }
}

impl From<&CapabilityConfig> for borg_remote::SessionCapabilities {
    fn from(value: &CapabilityConfig) -> Self {
        Self {
            multiplayer: value.multiplayer,
            subagents: value.subagents,
            autonomous_team: value.autonomous_team,
            shared_work: value.shared_work,
            presence: value.presence,
            cloud_sync: value.cloud_sync,
            web_relay: value.web_relay,
            telemetry: value.telemetry,
            provider_capabilities: Vec::new(),
            runtime_mcp_context: None,
            resource_limits: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct UpdateConfig {
    /// Download verified stable releases in the background for the next launch.
    pub(crate) auto_install: bool,
    /// Minimum interval between release checks.
    pub(crate) check_interval_hours: u64,
}

impl Default for UpdateConfig {
    fn default() -> Self {
        Self {
            auto_install: true,
            check_interval_hours: 24,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct CommandConfig {
    /// User-defined slash-command aliases. The key omits the leading slash;
    /// the value is a built-in slash command and may include fixed arguments.
    pub(crate) aliases: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct McpConfig {
    /// Local stdio MCP servers exposed as tools to every provider.
    pub(crate) servers: BTreeMap<String, McpServerConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct McpServerConfig {
    pub(crate) enabled: bool,
    pub(crate) command: String,
    pub(crate) args: Vec<String>,
    pub(crate) env: BTreeMap<String, String>,
    /// Optional allowlist of wire names (`search`) or namespaced tool names
    /// (`mcp__docs__search`). Empty exposes every tool from the server.
    pub(crate) allowed_tools: Vec<String>,
}

impl Default for McpServerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            command: String::new(),
            args: Vec::new(),
            env: BTreeMap::new(),
            allowed_tools: Vec::new(),
        }
    }
}

pub(crate) use borg_ui::KeybindingConfig;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ApprovalConfig {
    /// Optional faster/cheaper model for native Auto command reviews.
    pub(crate) reviewer_model: Option<String>,
    /// Reviewer reasoning effort. Defaults to `low` when a dedicated model is set.
    pub(crate) reviewer_effort: Option<String>,
}

impl AgentConfig {
    pub(crate) fn path(explicit: Option<&Path>) -> Option<PathBuf> {
        explicit.map(Path::to_path_buf).or_else(default_path)
    }

    pub(crate) fn load(explicit: Option<&Path>) -> Result<Self> {
        let path = Self::path(explicit);
        let Some(path) = path else {
            return Ok(Self::default());
        };
        if !path.exists() && explicit.is_none() {
            return Ok(Self::default());
        }
        let source = fs::read_to_string(&path)
            .with_context(|| format!("failed to read agent config {}", path.display()))?;
        let config: Self = toml::from_str(&source)
            .with_context(|| format!("invalid agent config {}", path.display()))?;
        config
            .validate()
            .with_context(|| format!("invalid agent config {}", path.display()))?;
        Ok(config)
    }

    pub(crate) fn has_configured_model(&self, alias: &str) -> bool {
        self.providers.iter().any(|(provider_id, provider)| {
            provider.models.keys().any(|model_id| {
                format_configured_model_alias(provider_id, model_id) == alias.trim()
            })
        })
    }

    pub(crate) fn configured_model_gateways(
        &self,
    ) -> BTreeMap<String, borg_provider::provider::ModelGateway> {
        self.providers
            .iter()
            .flat_map(|(provider_id, provider)| {
                let token = provider
                    .api_key_env
                    .as_deref()
                    .and_then(|name| std::env::var(name).ok())
                    .or_else(|| provider.api_key.clone())
                    .unwrap_or_default();
                let endpoint = chat_completions_endpoint(&provider.base_url);
                provider.models.iter().map(move |(model_id, model)| {
                    let body = model
                        .body
                        .iter()
                        .map(|(key, value)| {
                            (
                                key.clone(),
                                serde_json::to_value(value)
                                    .expect("validated TOML values are JSON-compatible"),
                            )
                        })
                        .collect();
                    let variant_bodies = model
                        .variants
                        .iter()
                        .map(|(name, variant)| {
                            let body = variant
                                .body
                                .iter()
                                .map(|(key, value)| {
                                    (
                                        key.clone(),
                                        serde_json::to_value(value)
                                            .expect("validated TOML values are JSON-compatible"),
                                    )
                                })
                                .collect();
                            (name.clone(), body)
                        })
                        .collect();
                    let alias = format_configured_model_alias(provider_id, model_id);
                    let label = provider
                        .name
                        .as_deref()
                        .filter(|name| !name.trim().is_empty())
                        .unwrap_or(provider_id)
                        .to_string();
                    (
                        alias,
                        borg_provider::provider::ModelGateway {
                            endpoint: endpoint.clone(),
                            bearer_token: token.clone(),
                            model: Some(model_id.clone()),
                            label: Some(label),
                            headers: provider.headers.clone(),
                            body,
                            variant_bodies,
                            context_window_tokens: model.context_window_tokens,
                            max_output_tokens: model.max_output_tokens,
                        },
                    )
                })
            })
            .collect()
    }

    pub(crate) fn configured_model_entries(&self) -> Vec<borg_provider::DynamicModelEntry> {
        self.providers
            .iter()
            .flat_map(|(provider_id, provider)| {
                provider.models.iter().map(move |(model_id, model)| {
                    let alias = format_configured_model_alias(provider_id, model_id);
                    let provider_label = provider
                        .name
                        .as_deref()
                        .filter(|name| !name.trim().is_empty())
                        .unwrap_or(provider_id);
                    let model_label = model
                        .name
                        .as_deref()
                        .filter(|name| !name.trim().is_empty())
                        .unwrap_or(model_id);
                    let mut details = Vec::new();
                    if let Some(context) = model.context_window_tokens {
                        details.push(format!("{context} context"));
                    }
                    if !model.variants.is_empty() {
                        details.push(format!("{} variants", model.variants.len()));
                    }
                    borg_provider::DynamicModelEntry {
                        id: alias,
                        label: format!("{provider_label} · {model_label}"),
                        detail: (!details.is_empty()).then(|| details.join(" · ")),
                    }
                })
            })
            .collect()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if let Some(worker_concurrency) = self.team.worker_concurrency {
            anyhow::ensure!(
                worker_concurrency > 0,
                "team.worker_concurrency must be positive"
            );
        }
        if self.team.preset.is_some() {
            for (name, value) in [
                (
                    "team.max_total_assignments",
                    self.team.max_total_assignments,
                ),
                ("team.max_total_reports", self.team.max_total_reports),
                (
                    "team.max_total_escalations",
                    self.team.max_total_escalations,
                ),
                ("team.max_specialists", self.team.max_specialists),
            ] {
                anyhow::ensure!(value != Some(0), "{name} must be positive when set");
            }
            for (name, value) in [
                ("team.max_tokens", self.team.max_tokens),
                ("team.max_cost_microusd", self.team.max_cost_microusd),
                ("team.max_wall_time_ms", self.team.max_wall_time_ms),
            ] {
                anyhow::ensure!(value != Some(0), "{name} must be positive when set");
            }
        }
        for (alias, target) in &self.commands.aliases {
            anyhow::ensure!(
                valid_alias(alias),
                "command alias `{alias}` must contain only ASCII letters, digits, `-`, or `_`"
            );
            anyhow::ensure!(
                target.starts_with('/') && target.len() > 1,
                "command alias `{alias}` target must be a slash command"
            );
            anyhow::ensure!(
                !target.starts_with(&format!("/{alias}")),
                "command alias `{alias}` cannot expand to itself"
            );
        }
        for (action, bindings) in self.keybindings.entries() {
            anyhow::ensure!(
                !bindings.is_empty(),
                "keybinding action `{action}` must have at least one key"
            );
            for binding in bindings {
                borg_ui::validate_key_chord(binding)
                    .with_context(|| format!("invalid `{action}` keybinding `{binding}`"))?;
            }
        }
        for (name, server) in &self.mcp.servers {
            anyhow::ensure!(
                valid_alias(name),
                "MCP server name `{name}` must contain only ASCII letters, digits, `-`, or `_`"
            );
            anyhow::ensure!(
                !server.enabled || !server.command.trim().is_empty(),
                "enabled MCP server `{name}` must define a command"
            );
            anyhow::ensure!(
                server.args.iter().all(|argument| !argument.contains('\0')),
                "MCP server `{name}` arguments cannot contain NUL bytes"
            );
            anyhow::ensure!(
                server.env.iter().all(|(key, value)| !key.is_empty()
                    && !key.contains(['=', '\0'])
                    && !value.contains('\0')),
                "MCP server `{name}` has an invalid environment entry"
            );
            anyhow::ensure!(
                server
                    .allowed_tools
                    .iter()
                    .all(|tool| valid_allowed_tool(tool)),
                "MCP server `{name}` has an invalid allowed tool"
            );
        }
        if let Some(model) = &self.approvals.reviewer_model {
            anyhow::ensure!(!model.trim().is_empty(), "reviewer_model must not be empty");
        }
        if let Some(effort) = &self.approvals.reviewer_effort {
            anyhow::ensure!(
                !effort.trim().is_empty(),
                "reviewer_effort must not be empty"
            );
        }
        for (provider_id, provider) in &self.providers {
            anyhow::ensure!(
                !is_reserved_provider_id(provider_id),
                "provider id is reserved for a built-in Borg route"
            );
            anyhow::ensure!(
                !provider.models.is_empty(),
                "configured provider must declare at least one model"
            );
            anyhow::ensure!(
                valid_alias(provider_id),
                "provider id `{provider_id}` must contain only ASCII letters, digits, `-`, or `_`"
            );
            anyhow::ensure!(
                matches!(
                    provider.protocol.trim().to_ascii_lowercase().as_str(),
                    "openai-compatible" | "openai_compatible"
                ),
                "provider `{provider_id}` uses unsupported protocol `{}`",
                provider.protocol
            );
            let base_url = provider.base_url.trim();
            anyhow::ensure!(
                base_url.starts_with("http://") || base_url.starts_with("https://"),
                "provider `{provider_id}` base_url must start with http:// or https://"
            );
            anyhow::ensure!(
                !base_url.contains(['\0', '\r', '\n']),
                "provider `{provider_id}` base_url contains invalid characters"
            );
            if let Some(name) = &provider.name {
                anyhow::ensure!(
                    !name.trim().is_empty(),
                    "provider `{provider_id}` name must not be empty"
                );
            }
            if let Some(api_key_env) = &provider.api_key_env {
                anyhow::ensure!(
                    valid_env_name(api_key_env),
                    "provider `{provider_id}` api_key_env is invalid"
                );
            }
            anyhow::ensure!(
                provider.headers.iter().all(|(key, value)| {
                    !key.trim().is_empty()
                        && !key.contains(['\0', '\r', '\n'])
                        && !value.contains(['\0', '\r', '\n'])
                }),
                "provider `{provider_id}` has an invalid header"
            );
            for (model_id, model) in &provider.models {
                anyhow::ensure!(
                    model_id.trim() == model_id,
                    "configured model ids cannot have leading or trailing whitespace"
                );
                anyhow::ensure!(
                    !model_id.trim().is_empty(),
                    "provider `{provider_id}` has an empty model id"
                );
                anyhow::ensure!(
                    !model_id.contains(['\0', '\r', '\n']),
                    "provider `{provider_id}` model `{model_id}` contains invalid characters"
                );
                if let Some(name) = &model.name {
                    anyhow::ensure!(
                        !name.trim().is_empty(),
                        "provider `{provider_id}` model `{model_id}` name must not be empty"
                    );
                }
                if let Some(context) = model.context_window_tokens {
                    anyhow::ensure!(
                        context > 0,
                        "provider `{provider_id}` model `{model_id}` context_window_tokens must be positive"
                    );
                }
                if let Some(max_output) = model.max_output_tokens {
                    anyhow::ensure!(
                        max_output > 0,
                        "provider `{provider_id}` model `{model_id}` max_output_tokens must be positive"
                    );
                }
                for (key, value) in &model.body {
                    anyhow::ensure!(
                        !key.contains(['\0', '\r', '\n']),
                        "configured body field contains invalid characters"
                    );
                    anyhow::ensure!(
                        serde_json::to_value(value).is_ok(),
                        "configured body field is not JSON-compatible"
                    );
                }
                for variant in model.variants.values() {
                    for (key, value) in &variant.body {
                        anyhow::ensure!(
                            !key.contains(['\0', '\r', '\n']),
                            "configured variant body field contains invalid characters"
                        );
                        anyhow::ensure!(
                            serde_json::to_value(value).is_ok(),
                            "configured variant body field is not JSON-compatible"
                        );
                    }
                }
                for variant in model.variants.keys() {
                    anyhow::ensure!(
                        !variant.trim().is_empty(),
                        "provider `{provider_id}` model `{model_id}` has an empty variant name"
                    );
                }
            }
        }
        anyhow::ensure!(
            (1..=24 * 30).contains(&self.updates.check_interval_hours),
            "updates.check_interval_hours must be between 1 and 720"
        );
        if let Some(base_url) = &self.local.base_url {
            let trimmed = base_url.trim();
            anyhow::ensure!(!trimmed.is_empty(), "local.base_url must not be empty");
            anyhow::ensure!(
                trimmed.starts_with("http://") || trimmed.starts_with("https://"),
                "local.base_url must start with http:// or https://"
            );
        }
        if let Some(model) = &self.local.model {
            anyhow::ensure!(!model.trim().is_empty(), "local.model must not be empty");
        }
        if let Some(context_window_tokens) = self.local.context_window_tokens {
            anyhow::ensure!(
                context_window_tokens > 0,
                "local.context_window_tokens must be positive"
            );
        }
        if let Some(model_path) = &self.local.model_path {
            anyhow::ensure!(
                !model_path.as_os_str().is_empty(),
                "local.model_path must not be empty"
            );
        }
        anyhow::ensure!(
            self.local
                .model_dirs
                .iter()
                .all(|path| !path.as_os_str().is_empty()),
            "local.model_dirs must not contain empty paths"
        );
        if let Some(server_bin) = &self.local.server_bin {
            anyhow::ensure!(
                !server_bin.as_os_str().is_empty(),
                "local.server_bin must not be empty"
            );
        }
        if let Some(host) = &self.local.host {
            anyhow::ensure!(!host.trim().is_empty(), "local.host must not be empty");
            anyhow::ensure!(
                !host.contains(['\0', ' ', '\t', '\r', '\n']),
                "local.host must not contain whitespace or NUL bytes"
            );
        }
        if self.local.port == Some(0) {
            anyhow::bail!("local.port must be between 1 and 65535");
        }
        if let Some(ctx_size) = self.local.ctx_size {
            anyhow::ensure!(ctx_size > 0, "local.ctx_size must be positive");
        }
        if let Some(reasoning_format) = &self.local.reasoning_format {
            anyhow::ensure!(
                !reasoning_format.trim().is_empty(),
                "local.reasoning_format must not be empty"
            );
            anyhow::ensure!(
                !reasoning_format.contains('\0'),
                "local.reasoning_format must not contain NUL bytes"
            );
        }
        Ok(())
    }

    /// Export `[local]` settings into the process environment so the
    /// `OpenAiCompatible` provider picks them up. Existing environment values
    /// are never overwritten: an explicit export or `--model` still wins.
    pub(crate) fn apply_local_provider_env(&self) -> LocalProviderEnvGuard {
        let previous = LOCAL_PROVIDER_ENV_KEYS
            .into_iter()
            .map(|key| (key, std::env::var_os(key)))
            .collect();
        let entries = [
            (
                "BORG_OPENAI_COMPATIBLE_BASE_URL",
                self.local.base_url.clone(),
            ),
            ("BORG_OPENAI_COMPATIBLE_MODEL", self.local.model.clone()),
            ("BORG_OPENAI_COMPATIBLE_API_KEY", self.local.api_key.clone()),
            (
                "BORG_OPENAI_COMPATIBLE_CONTEXT_WINDOW_TOKENS",
                self.local
                    .ctx_size
                    .or(self.local.context_window_tokens)
                    .map(|value| value.to_string()),
            ),
        ];
        for (key, value) in entries {
            let Some(value) = value else {
                continue;
            };
            let value = value.trim();
            if value.is_empty() {
                continue;
            }
            if key == "BORG_OPENAI_COMPATIBLE_CONTEXT_WINDOW_TOKENS"
                && std::env::var_os("BORG_LOCAL_CTX_SIZE")
                    .is_some_and(|existing| !existing.is_empty())
            {
                continue;
            }
            if std::env::var_os(key).is_some_and(|existing| !existing.is_empty()) {
                continue;
            }
            // SAFETY: called once during startup, before worker threads that
            // read provider environment are spawned.
            unsafe { std::env::set_var(key, value) };
        }
        LocalProviderEnvGuard { previous }
    }

    pub(crate) fn autonomous_team_policy(
        &self,
        capabilities: &borg_remote::SessionCapabilities,
        provider: borg_remote::CodingProvider,
        session_id: uuid::Uuid,
    ) -> Option<borg_remote::TeamPolicy> {
        let preset = self.team.preset?;
        let effective = capabilities.effective();
        if !effective
            .active
            .contains(&borg_remote::SessionCapability::AutonomousTeam)
        {
            return None;
        }
        let worker_concurrency = self.subagent_concurrency_limit();
        let mut policy = preset.policy(
            session_id,
            session_id,
            session_id,
            std::iter::empty(),
            borg_remote::ProviderId(format!("{provider:?}").to_ascii_lowercase()),
        );
        policy.limits.max_concurrent_assignments = worker_concurrency;
        policy.limits.max_total_assignments = self.team.max_total_assignments.unwrap_or(u32::MAX);
        policy.limits.max_total_reports = self.team.max_total_reports.unwrap_or(u32::MAX);
        policy.limits.max_total_escalations = self.team.max_total_escalations.unwrap_or(u32::MAX);
        policy.specialists.max_specialists = self.team.max_specialists.unwrap_or(u32::MAX);
        policy.limits.per_role_concurrency = vec![borg_remote::RoleConcurrencyLimit {
            role: borg_remote::TeamRole::Worker,
            max_concurrent_assignments: worker_concurrency,
        }];
        policy.limits.budget.max_tokens = self.team.max_tokens;
        policy.limits.budget.max_cost_microusd = self.team.max_cost_microusd;
        policy.limits.budget.max_wall_time_ms = self.team.max_wall_time_ms;
        Some(policy)
    }

    pub(crate) fn subagent_concurrency_limit(&self) -> u32 {
        self.team
            .worker_concurrency
            .unwrap_or(borg_remote::DEFAULT_MAX_SUBAGENTS as u32)
    }

    pub(crate) fn expand_command(&self, line: &str) -> String {
        let Some(command) = line.strip_prefix('/') else {
            return line.to_string();
        };
        let split = command.find(char::is_whitespace).unwrap_or(command.len());
        let (name, suffix) = command.split_at(split);
        self.commands
            .aliases
            .get(name)
            .map(|target| format!("{target}{suffix}"))
            .unwrap_or_else(|| line.to_string())
    }

    pub(crate) fn external_mcp_servers(&self) -> Vec<borg_provider::mcp::ExternalMcpServer> {
        self.mcp
            .servers
            .iter()
            .filter(|(_, server)| server.enabled)
            .map(|(name, server)| borg_provider::mcp::ExternalMcpServer {
                name: name.clone(),
                command: server.command.clone(),
                args: server.args.clone(),
                env: server.env.clone(),
                allowed_tools: server.allowed_tools.clone(),
            })
            .collect()
    }
}

fn default_path() -> Option<PathBuf> {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .map(|root| root.join("borg").join("agent.toml"))
}

fn valid_alias(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn valid_env_name(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        && value.as_bytes()[0].is_ascii_alphabetic()
}

fn is_reserved_provider_id(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "codex"
            | "claude"
            | "opencode"
            | "open-code"
            | "kimi"
            | "openrouter"
            | "open-router"
            | "openai-compatible"
            | "openai_compatible"
            | "open-ai-compatible"
            | "open-ai_compatible"
    )
}

fn format_configured_model_alias(provider_id: &str, model_id: &str) -> String {
    format!("{provider_id}/{model_id}")
}

fn chat_completions_endpoint(base_url: &str) -> String {
    let trimmed = base_url.trim_end_matches('/');
    if trimmed.ends_with("/chat/completions") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/chat/completions")
    }
}

fn valid_allowed_tool(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    static LOCAL_ENV_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    struct LocalEnvReset {
        previous: Vec<(&'static str, Option<OsString>)>,
    }

    impl LocalEnvReset {
        fn new() -> Self {
            let previous = LOCAL_PROVIDER_ENV_KEYS
                .into_iter()
                .map(|key| (key, std::env::var_os(key)))
                .collect::<Vec<_>>();
            for key in LOCAL_PROVIDER_ENV_KEYS {
                // SAFETY: this test holds the process-local environment lock.
                unsafe { std::env::remove_var(key) };
            }
            Self { previous }
        }
    }

    impl Drop for LocalEnvReset {
        fn drop(&mut self) {
            for (key, value) in &self.previous {
                // SAFETY: this test holds the process-local environment lock.
                unsafe {
                    match value {
                        Some(value) => std::env::set_var(key, value),
                        None => std::env::remove_var(key),
                    }
                }
            }
        }
    }

    #[test]
    fn local_block_configures_an_openai_compatible_endpoint() {
        let config: AgentConfig = toml::from_str(
            r#"
[local]
base_url = "http://127.0.0.1:11434/v1"
model = "qwen3.6:35b-a3b"
context_window_tokens = 32768
"#,
        )
        .expect("local block parses");
        config.validate().expect("local block is valid");
        assert_eq!(
            config.local.base_url.as_deref(),
            Some("http://127.0.0.1:11434/v1")
        );
        assert_eq!(config.local.model.as_deref(), Some("qwen3.6:35b-a3b"));
        assert_eq!(config.local.context_window_tokens, Some(32768));
    }

    #[test]
    fn local_defaults_stay_empty_so_hosted_providers_are_untouched() {
        let config = AgentConfig::default();
        config.validate().expect("empty config is valid");
        assert_eq!(config.local, LocalProviderConfig::default());
        assert!(config.local.model.is_none());
    }

    #[test]
    fn local_provider_environment_is_scoped_across_config_switches() {
        let _lock = LOCAL_ENV_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap();
        let _reset = LocalEnvReset::new();
        let first: AgentConfig = toml::from_str(
            "[local]\nbase_url='http://127.0.0.1:8000/v1'\nmodel='first'\napi_key='first-key'\ncontext_window_tokens=8192\n",
        )
        .unwrap();
        let second: AgentConfig = toml::from_str(
            "[local]\nbase_url='http://127.0.0.1:9000/v1'\nmodel='second'\napi_key='second-key'\ncontext_window_tokens=16384\n",
        )
        .unwrap();

        {
            let _guard = first.apply_local_provider_env();
            assert_eq!(
                std::env::var("BORG_OPENAI_COMPATIBLE_BASE_URL").unwrap(),
                "http://127.0.0.1:8000/v1"
            );
            assert_eq!(
                std::env::var("BORG_OPENAI_COMPATIBLE_MODEL").unwrap(),
                "first"
            );
        }
        assert!(std::env::var_os("BORG_OPENAI_COMPATIBLE_BASE_URL").is_none());

        {
            let _guard = second.apply_local_provider_env();
            assert_eq!(
                std::env::var("BORG_OPENAI_COMPATIBLE_BASE_URL").unwrap(),
                "http://127.0.0.1:9000/v1"
            );
            assert_eq!(
                std::env::var("BORG_OPENAI_COMPATIBLE_MODEL").unwrap(),
                "second"
            );
        }
        assert!(std::env::var_os("BORG_OPENAI_COMPATIBLE_MODEL").is_none());
    }

    #[test]
    fn local_base_url_must_be_an_http_endpoint() {
        let config: AgentConfig =
            toml::from_str("[local]\nbase_url = \"127.0.0.1:11434\"\n").expect("parses");
        let error = config.validate().expect_err("scheme is required");
        assert!(
            error.to_string().contains("http://"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn local_context_window_must_be_positive() {
        let config: AgentConfig =
            toml::from_str("[local]\ncontext_window_tokens = 0\n").expect("parses");
        config
            .validate()
            .expect_err("zero context window is rejected");
    }

    #[test]
    fn local_server_config_parses_with_discovery_seam() {
        let config: AgentConfig = toml::from_str(
            r#"
[local]
model_path = "models/qwen.gguf"
model_dirs = ["models", "/mnt/gguf"]
include_ollama_store = true
include_hf_cache = true
server_bin = "/usr/lib/ollama/llama-server"
host = "127.0.0.1"
port = 8123
auto_start = true
ctx_size = 32768
jinja = true
reasoning_format = "deepseek"
"#,
        )
        .expect("server config parses");
        config.validate().expect("server config is valid");
        assert_eq!(config.local.port, Some(8123));
        assert_eq!(config.local.ctx_size, Some(32768));
        assert!(config.local.include_ollama_store);
        assert!(config.local.include_hf_cache);
    }

    #[test]
    fn local_server_rejects_zero_port() {
        let config: AgentConfig =
            toml::from_str("[local]\nport = 0\n").expect("port parses as u16");
        config
            .validate()
            .expect_err("zero port must not be accepted");
    }

    #[test]
    fn aliases_preserve_user_arguments() {
        let config = AgentConfig {
            commands: CommandConfig {
                aliases: BTreeMap::from([("quick".into(), "/fast on".into())]),
            },
            ..AgentConfig::default()
        };
        assert_eq!(config.expand_command("/quick"), "/fast on");
        assert_eq!(config.expand_command("/quick extra"), "/fast on extra");
        assert_eq!(config.expand_command("quick"), "quick");
    }

    #[test]
    fn checked_in_example_matches_the_typed_config() {
        let config: AgentConfig =
            toml::from_str(include_str!("../../../configs/agent.example.toml")).unwrap();
        config.validate().unwrap();
        assert_eq!(config.expand_command("/quick"), "/fast on");
    }

    #[test]
    fn disabling_parent_capabilities_cascades_without_extra_config() {
        let config: AgentConfig =
            toml::from_str("[capabilities]\nmultiplayer = false\nsubagents = false\n").unwrap();
        config.validate().expect("valid parent-only disablement");
        let effective = borg_remote::SessionCapabilities::from(&config.capabilities).effective();
        assert!(
            effective
                .inactive
                .iter()
                .any(|item| item.capability == borg_remote::SessionCapability::SharedWork)
        );
        assert!(
            effective
                .inactive
                .iter()
                .any(|item| item.capability == borg_remote::SessionCapability::AutonomousTeam)
        );
    }

    #[test]
    fn capabilities_default_to_full_runtime_with_private_telemetry() {
        let config: AgentConfig = toml::from_str("").unwrap();
        assert!(config.capabilities.multiplayer);
        assert!(config.capabilities.subagents);
        assert!(config.capabilities.autonomous_team);
        assert!(!config.capabilities.telemetry);
        assert!(!config.extensions.allow_project_mcp);
        assert_eq!(config.extensions.default_access, ExtensionAccess::Trusted);
        assert_eq!(config.extensions.project_access, ExtensionAccess::Sandboxed);
        assert_eq!(config.extensions.native_access, NativeAccessPolicy::Prompt);
        assert_eq!(config.capabilities.tool_mode, borg_remote::ToolMode::Both);
    }

    #[test]
    fn native_tool_mode_is_configurable_without_changing_other_capabilities() {
        let config: AgentConfig = toml::from_str(
            r#"
            [capabilities]
            tool_mode = "code"
            "#,
        )
        .unwrap();
        assert_eq!(config.capabilities.tool_mode, borg_remote::ToolMode::Code);
        assert!(config.capabilities.subagents);

        let compact: AgentConfig = toml::from_str(
            r#"
            [capabilities]
            tool_mode = "compact"
            "#,
        )
        .unwrap();
        assert_eq!(
            compact.capabilities.tool_mode,
            borg_remote::ToolMode::Compact
        );
    }

    #[test]
    fn project_extension_trust_is_user_controlled() {
        let config: AgentConfig = toml::from_str(
            r#"
            [extensions]
            allow_project_mcp = true
            "#,
        )
        .unwrap();
        assert!(config.extensions.allow_project_mcp);
    }

    #[test]
    fn extension_access_policy_can_be_made_prompt_free() {
        let config: AgentConfig = toml::from_str(
            r#"
            [extensions]
            default_access = "native"
            project_access = "native"
            native_access = "allow"
            "#,
        )
        .unwrap();
        assert_eq!(config.extensions.default_access, ExtensionAccess::Native);
        assert_eq!(config.extensions.project_access, ExtensionAccess::Native);
        assert_eq!(config.extensions.native_access, NativeAccessPolicy::Allow);
    }

    #[test]
    fn team_policy_is_opt_in_and_uses_the_existing_preset_limits() {
        let disabled: AgentConfig = toml::from_str("").unwrap();
        assert_eq!(
            disabled.subagent_concurrency_limit(),
            borg_remote::DEFAULT_MAX_SUBAGENTS as u32
        );
        assert!(
            disabled
                .autonomous_team_policy(
                    &borg_remote::SessionCapabilities::from(&disabled.capabilities),
                    borg_remote::CodingProvider::Codex,
                    uuid::Uuid::nil(),
                )
                .is_none()
        );

        let config: AgentConfig = toml::from_str(
            r#"
            [team]
            preset = "xhigh_director_low_workers"
            worker_concurrency = 2
            max_tokens = 5000
            max_cost_microusd = 120000
            max_wall_time_ms = 30000
            "#,
        )
        .unwrap();
        config.validate().unwrap();
        let policy = config
            .autonomous_team_policy(
                &borg_remote::SessionCapabilities::from(&config.capabilities),
                borg_remote::CodingProvider::Codex,
                uuid::Uuid::nil(),
            )
            .expect("opt-in policy");
        assert_eq!(policy.limits.max_concurrent_assignments, 2);
        assert_eq!(policy.limits.max_total_assignments, u32::MAX);
        assert_eq!(policy.limits.max_total_reports, u32::MAX);
        assert_eq!(policy.limits.max_total_escalations, u32::MAX);
        assert_eq!(policy.specialists.max_specialists, u32::MAX);
        assert_eq!(policy.limits.budget.max_tokens, Some(5000));
        assert_eq!(policy.limits.budget.max_cost_microusd, Some(120000));
        assert_eq!(policy.limits.budget.max_wall_time_ms, Some(30000));
        assert_eq!(
            policy.topology.members[0]
                .profile
                .reasoning_effort
                .as_deref(),
            Some("xhigh")
        );
    }

    #[test]
    fn manual_team_concurrency_can_be_lowered_without_enabling_a_preset() {
        let config: AgentConfig = toml::from_str(
            r#"
            [team]
            worker_concurrency = 4
            "#,
        )
        .unwrap();

        config.validate().unwrap();
        assert_eq!(config.subagent_concurrency_limit(), 4);
        assert!(config.team.preset.is_none());

        let invalid: AgentConfig = toml::from_str(
            r#"
            [team]
            worker_concurrency = 0
            "#,
        )
        .unwrap();
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn team_policy_stays_off_when_autonomous_team_is_not_effective() {
        let config: AgentConfig = toml::from_str(
            r#"
            [capabilities]
            autonomous_team = false

            [team]
            preset = "xhigh_director_low_workers"
            "#,
        )
        .unwrap();
        assert!(
            config
                .autonomous_team_policy(
                    &borg_remote::SessionCapabilities::from(&config.capabilities),
                    borg_remote::CodingProvider::Codex,
                    uuid::Uuid::nil(),
                )
                .is_none()
        );
    }

    #[test]
    fn partial_keybinding_tables_keep_unspecified_defaults() {
        let config: AgentConfig = toml::from_str(
            r#"
            [keybindings]
            send = ["ctrl+s"]
            "#,
        )
        .expect("config");
        config.validate().expect("valid config");
        assert_eq!(config.keybindings.send, ["ctrl+s"]);
        assert_eq!(config.keybindings.queue, ["tab"]);
        assert_eq!(config.keybindings.interrupt, ["esc"]);
    }

    #[test]
    fn enabled_mcp_servers_become_provider_neutral_tool_extensions() {
        let config: AgentConfig = toml::from_str(
            r#"
            [mcp.servers.docs]
            command = "docs-mcp"
            args = ["--stdio"]
            allowed_tools = ["search"]

            [mcp.servers.disabled]
            enabled = false
            "#,
        )
        .expect("config");
        config.validate().expect("valid config");

        let servers = config.external_mcp_servers();
        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].name, "docs");
        assert_eq!(servers[0].command, "docs-mcp");
        assert_eq!(servers[0].args, ["--stdio"]);
        assert_eq!(servers[0].allowed_tools, ["search"]);
    }

    #[test]
    fn configured_provider_models_keep_qualified_alias_and_raw_wire_id() {
        let config: AgentConfig = toml::from_str(
            r#"
            [providers.groq]
            name = "Groq"
            base_url = "https://api.groq.com/openai/v1"
            api_key_env = "GROQ_API_KEY"

            [providers.groq.models."openai/gpt-oss-120b"]
            name = "GPT-OSS 120B"
            context_window_tokens = 131072
            max_output_tokens = 32768

            [providers.groq.models."openai/gpt-oss-120b".variants.high]
            body = { reasoning_effort = "high" }
            "#,
        )
        .expect("configured provider parses");
        config.validate().expect("configured provider is valid");

        let alias = "groq/openai/gpt-oss-120b";
        assert!(config.has_configured_model(alias));
        let entries = config.configured_model_entries();
        assert_eq!(entries[0].id, alias);
        assert!(entries[0].label.contains("GPT-OSS 120B"));
        let gateway = config
            .configured_model_gateways()
            .remove(alias)
            .expect("gateway");
        assert_eq!(
            gateway.endpoint,
            "https://api.groq.com/openai/v1/chat/completions"
        );
        assert_eq!(gateway.model.as_deref(), Some("openai/gpt-oss-120b"));
        assert_eq!(gateway.context_window_tokens, Some(131072));
        assert_eq!(gateway.max_output_tokens, Some(32768));
        assert_eq!(gateway.variant_bodies["high"]["reasoning_effort"], "high");
    }

    #[test]
    fn configured_provider_environment_key_overrides_inline_key() {
        let _lock = LOCAL_ENV_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap();
        let previous = std::env::var_os("BORG_TEST_PROVIDER_KEY");
        // SAFETY: this test holds the process-local environment lock.
        unsafe { std::env::set_var("BORG_TEST_PROVIDER_KEY", "environment-key") };
        let config: AgentConfig = toml::from_str(
            r#"
            [providers.example]
            base_url = "http://127.0.0.1:9000/v1"
            api_key = "inline-key"
            api_key_env = "BORG_TEST_PROVIDER_KEY"
            [providers.example.models.demo]
            "#,
        )
        .expect("configured provider parses");
        config.validate().expect("configured provider is valid");
        let gateway = config
            .configured_model_gateways()
            .remove("example/demo")
            .expect("gateway");
        assert_eq!(gateway.bearer_token, "environment-key");
        // SAFETY: restore the process-local environment snapshot.
        unsafe {
            match previous {
                Some(value) => std::env::set_var("BORG_TEST_PROVIDER_KEY", value),
                None => std::env::remove_var("BORG_TEST_PROVIDER_KEY"),
            }
        }
    }

    #[test]
    fn built_in_provider_ids_are_reserved_for_named_routes() {
        let config: AgentConfig = toml::from_str(
            r#"
            [providers.openrouter]
            base_url = "https://example.test/v1"
            [providers.openrouter.models.auto]
            "#,
        )
        .expect("configured provider parses");
        assert!(config.validate().is_err());
    }
}
