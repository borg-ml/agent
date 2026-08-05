use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

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
}

/// Declarative setup for a locally hosted OpenAI-compatible server
/// (`llama-server`, vLLM, Ollama). Without this, pointing Borg at a local
/// model requires exporting `BORG_OPENAI_COMPATIBLE_*` by hand before every
/// run, which is not a configuration story.
///
/// Applied by [`AgentConfig::apply_local_provider_env`]. The process
/// environment always wins, so an explicit export or `--model` still
/// overrides the file.
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

/// Trust controls for declarative extension catalogs.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ExtensionConfig {
    /// Project checkouts are not trusted to launch local MCP processes by default.
    pub(crate) allow_project_mcp: bool,
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct KeybindingConfig {
    pub(crate) send: Vec<String>,
    pub(crate) queue: Vec<String>,
    pub(crate) newline: Vec<String>,
    pub(crate) keybindings: Vec<String>,
    pub(crate) interrupt: Vec<String>,
    pub(crate) clear_or_exit: Vec<String>,
    pub(crate) exit: Vec<String>,
    pub(crate) attach_image: Vec<String>,
    pub(crate) copy: Vec<String>,
    pub(crate) scroll_up: Vec<String>,
    pub(crate) scroll_down: Vec<String>,
    pub(crate) select_previous: Vec<String>,
    pub(crate) select_next: Vec<String>,
    pub(crate) approve: Vec<String>,
    pub(crate) deny: Vec<String>,
}

impl Default for KeybindingConfig {
    fn default() -> Self {
        Self {
            send: vec!["enter".into()],
            queue: vec!["tab".into()],
            newline: vec!["shift+enter".into(), "alt+enter".into()],
            keybindings: vec!["?".into()],
            interrupt: vec!["esc".into()],
            clear_or_exit: vec!["ctrl+c".into()],
            exit: vec!["ctrl+d".into()],
            attach_image: vec!["ctrl+v".into()],
            copy: vec!["ctrl+y".into()],
            scroll_up: vec!["pageup".into()],
            scroll_down: vec!["pagedown".into()],
            select_previous: vec!["alt+up".into()],
            select_next: vec!["alt+down".into()],
            approve: vec!["y".into()],
            deny: vec!["n".into(), "esc".into()],
        }
    }
}

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

    fn validate(&self) -> Result<()> {
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
                validate_key_chord(binding)
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
    pub(crate) fn apply_local_provider_env(&self) {
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

impl KeybindingConfig {
    pub(crate) fn entries(&self) -> [(&'static str, &[String]); 15] {
        [
            ("send", &self.send),
            ("queue", &self.queue),
            ("newline", &self.newline),
            ("keybindings", &self.keybindings),
            ("interrupt", &self.interrupt),
            ("clear_or_exit", &self.clear_or_exit),
            ("exit", &self.exit),
            ("attach_image", &self.attach_image),
            ("copy", &self.copy),
            ("scroll_up", &self.scroll_up),
            ("scroll_down", &self.scroll_down),
            ("select_previous", &self.select_previous),
            ("select_next", &self.select_next),
            ("approve", &self.approve),
            ("deny", &self.deny),
        ]
    }
}

fn validate_key_chord(value: &str) -> Result<()> {
    let mut parts = value.split('+').peekable();
    let mut key = None;
    while let Some(part) = parts.next() {
        let part = part.trim().to_ascii_lowercase();
        if parts.peek().is_some() && matches!(part.as_str(), "ctrl" | "alt" | "shift") {
            continue;
        }
        anyhow::ensure!(key.is_none(), "only one non-modifier key is allowed");
        anyhow::ensure!(
            matches!(
                part.as_str(),
                "enter"
                    | "esc"
                    | "tab"
                    | "backspace"
                    | "delete"
                    | "up"
                    | "down"
                    | "left"
                    | "right"
                    | "pageup"
                    | "pagedown"
                    | "home"
                    | "end"
                    | "space"
            ) || part.chars().count() == 1,
            "unsupported key `{part}`"
        );
        key = Some(part);
    }
    anyhow::ensure!(key.is_some(), "key chord must include a key");
    Ok(())
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
}
