use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use borg_provider::provider::{
    ChatApprovalDecision, ChatStreamControl, ChatStreamEvent, ChatStreamRequest,
    ClaudeSubscriptionPool, CodexSubscriptionPool, LocalAgentPermission, SteerAdmission,
    run_claude_chat_stream_with_control, run_claude_local_chat_stream,
    run_claude_local_chat_stream_pooled, run_codex_chat_stream_with_control,
    run_codex_local_chat_stream, run_codex_local_chat_stream_pooled,
    run_opencode_local_chat_stream,
};
use borg_provider::{ProviderCallUsage, ProviderChannel};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, mpsc};
use uuid::Uuid;

use crate::{
    CodingProvider, EventActor, MessageStatus, PermissionMode, ResponseLanguage, SessionEventKind,
    SessionStatus, ToolMode, WorkflowRuntime, native_harness::NativeHarness,
};

pub(crate) const CODING_SYSTEM_PROMPT: &str = "\
You are Borg, a practical agent working in the user's local project. \
Inspect before changing, keep solutions small, preserve user work, explain consequential actions, \
and continue until the requested outcome is implemented and verified. \
While work is ongoing, keep the user informed with concise progress updates and do not leave them \
without an update for more than about 60 seconds. \
The Borg Agent source is https://github.com/borg-ml/agent; when diagnosing Borg Agent behavior and the \
source is not already available, inspect or clone that public repository as needed. \
Write simple mathematical notation as readable Unicode or plain text. For complex notation, use \
valid Markdown math delimiters (`$...$` or `$$...$$`); never emit bare TeX commands in prose. \
Use the tools from the borg_agent MCP server for durable goals, plans, and subagents. \
For a substantial multi-step user request, call get_goal first, create a concise goal when none \
exists, then create the plan. Before updating an existing plan, call get_plan and reuse its exact \
item UUIDs; omit IDs for new items. \
Use the canonical update_plan shape `{\"explanation\":\"optional\",\"plan\":[{\"id\":\"UUID\",\"content\":\"step\",\"status\":\"pending|in_progress|completed\"}]}`; \
plan content is limited to 500 characters and only one item may be in_progress. \
Use `lsp_workspace_diagnostics` for a project-wide diagnostic pass when the workspace language is supported; use `lsp_diagnostics` for a targeted file and the other LSP tools for semantic navigation. \
After editing supported source files, run LSP diagnostics before finishing and repair errors caused by the edit. \
When the user starts a message with `/ask PROFILE`, `/claude`, `/gpt`, or `/codex`, treat it as a \
request for a second opinion. Use `consult_peer` for the normal case: it keeps the opposite GPT/Claude \
peer thread alive across calls and returns the peer's answer to you privately so you can reconcile \
it before answering. Use `consult_model` only when a deliberately isolated one-shot opinion is wanted. \
Call the peer only when another viewpoint would materially help; do not call it reflexively on every turn. \
Preserve an explicit `@EFFORT` suffix in a profile when the intent includes one (for example, \
`claude-opus-5@high` or `gpt-5.6-sol@xhigh`). \
You choose the complete freeform briefing: include the relevant objective, evidence, constraints, and \
exact question, while omitting unrelated transcript noise. Never ask the human to relay messages manually. \
The peer cannot invoke another peer; after the response returns, reconcile it with your own judgment and \
remain the sole voice that answers the user. Before every tool call, emit exactly one standalone narration \
item in the form `[[BORG_ACTION:label]]`, then generate the tool call. Use a short action noun for `label`, \
such as `edit`, `command`, `plan update`, `web search`, `file read`, or `diagnostics`. Do not include any \
other text in that narration item.";

pub(crate) const COMPACT_CODING_SYSTEM_PROMPT: &str = "\
You are Borg, a focused coding agent working in the user's local project. \
Inspect the workspace, make the smallest requested changes, and verify the result. \
While work is ongoing, keep the user informed with concise progress updates and do not leave them \
without an update for more than about 60 seconds. \
Use the available workspace tools directly, preserve user work, and report what you verified. Before every \
tool call, emit exactly one standalone narration item in the form `[[BORG_ACTION:label]]`, then generate the \
tool call. Use a short action noun for `label` and no other text in that narration item.";

fn action_intent_label(text: &str) -> Option<&str> {
    const PREFIX: &str = "[[BORG_ACTION:";
    let mut remaining = text.trim();
    let mut last_label = None;
    while !remaining.is_empty() {
        remaining = remaining.strip_prefix(PREFIX)?;
        let end = remaining.find("]]")?;
        let label = remaining[..end].trim();
        if label.is_empty()
            || label.len() > 64
            || !label.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, ' ' | '-' | '/')
            })
        {
            return None;
        }
        last_label = Some(label);
        remaining = remaining[end + 2..].trim();
    }
    last_label
}

fn action_intent_is_streaming(text: &str) -> bool {
    const PREFIX: &str = "[[BORG_ACTION:";
    let mut remaining = text.trim();
    loop {
        if PREFIX.starts_with(remaining) {
            return !remaining.is_empty();
        }
        let Some(label_and_rest) = remaining.strip_prefix(PREFIX) else {
            return false;
        };
        let Some(end) = label_and_rest.find("]]") else {
            return label_and_rest.len() <= 64
                && label_and_rest.chars().all(|character| {
                    character.is_ascii_alphanumeric() || matches!(character, ' ' | '-' | '/')
                });
        };
        let label = label_and_rest[..end].trim();
        if label.is_empty()
            || label.len() > 64
            || !label.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, ' ' | '-' | '/')
            })
        {
            return false;
        }
        remaining = label_and_rest[end + 2..].trim();
        if remaining.is_empty() {
            return true;
        }
    }
}

fn without_action_intent_marker(text: &str) -> Option<&str> {
    let trimmed = text.trim_start();
    if trimmed.is_empty() {
        return Some(text);
    }
    if "[[BORG_ACTION".starts_with(trimmed) || trimmed.starts_with("[[BORG_ACTION") {
        return trimmed
            .split_once('\n')
            .map(|(_, remainder)| remainder.trim_start())
            .filter(|remainder| !remainder.is_empty());
    }
    Some(text)
}

fn provider_native_agent_tool(name: &str) -> bool {
    matches!(name, "subAgentActivity" | "collabAgentToolCall")
}

#[derive(Clone)]
pub struct AgentTurn {
    pub session_id: Uuid,
    pub message_id: Uuid,
    /// Durable canonical-context epoch used to derive provider cache identity.
    /// It changes only at an explicit context boundary, not on reconnect or
    /// ordinary tool rounds.
    pub context_generation: u64,
    pub provider: CodingProvider,
    pub provider_session_id: Option<String>,
    /// Completed provider turn to fork through when recovering an uncertain
    /// Codex tail from a durable checkpoint.
    pub provider_fork_turn_id: Option<String>,
    pub cwd: PathBuf,
    /// The new durable user input represented by this turn. A reusable
    /// subscription checkpoint receives only this delta; a cold turn's
    /// `prompt` contains the complete canonical replay.
    pub prompt_delta: String,
    pub prompt: String,
    pub attachments: Vec<PathBuf>,
    pub output_schema: Option<Value>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub fast: Option<bool>,
    pub response_language: crate::ResponseLanguage,
    pub permission_mode: PermissionMode,
    /// Provider-neutral conversation reconstructed for native providers.
    pub conversation: Vec<borg_provider::provider::ModelMessage>,
    /// One local MCP transport for Borg-owned goal, plan, and subagent tools.
    pub agent_mcp_server: borg_provider::mcp::ExternalMcpServer,
    /// Direct in-process access to the same Borg-owned tools. Native harnesses
    /// use this instead of round-tripping through their MCP transport.
    pub agent_tools: crate::AgentToolDispatcher,
    /// Product/user MCP integrations available to a Borg-native turn.
    pub external_mcp_servers: Vec<borg_provider::mcp::ExternalMcpServer>,
    /// Scoped Web MCP identity and token fetched for this host session. The
    /// session actor keeps it in memory and provider setup consumes it only
    /// for the current request.
    pub runtime_mcp_context: crate::RuntimeMcpContext,
    /// Trusted extension-owned skill roots supplied by the launch contract.
    pub extension_skill_roots: Vec<PathBuf>,
    /// Executable workflows from the same atomic extension snapshot as the
    /// skill roots and MCP servers. Blu remains the compatibility default;
    /// external runtimes are supervised by the host.
    pub extension_workflows: Vec<BluWorkflowDefinition>,
    /// Declarative extension API captured with the workflow snapshot. It is
    /// never read from the live catalog while a turn is running.
    pub extension_api: crate::ExtensionApiSnapshot,
    /// Trusted runtime context appended to the provider system prompt.
    pub system_prompt_appendix: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct BluWorkflowDefinition {
    pub extension_id: String,
    pub name: String,
    pub description: Option<String>,
    pub runtime: WorkflowRuntime,
    pub source: String,
    pub entrypoint: PathBuf,
    pub working_directory: PathBuf,
    pub command: Option<String>,
    pub args: Vec<String>,
}

/// A one-shot second-opinion request selected by the main model. The session
/// actor resolves the user-facing profile alias before handing this request to
/// the provider executor, so the provider call never shares the main thread's
/// conversation or provider session.
#[derive(Debug, Clone)]
pub struct ConsultationRequest {
    pub owner_session_id: Uuid,
    pub message_id: Uuid,
    pub provider: CodingProvider,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub cwd: PathBuf,
    pub prompt: String,
    pub response_language: ResponseLanguage,
}

#[derive(Debug, Clone)]
pub struct ConsultationResult {
    pub provider: CodingProvider,
    pub model: Option<String>,
    pub final_text: String,
    pub usage: ProviderCallUsage,
}

#[derive(Debug, Clone)]
pub struct AgentTurnResult {
    pub provider_session_id: Option<String>,
    pub final_text: String,
}

#[derive(Debug, Clone)]
pub struct AgentCompaction {
    pub summary: String,
    pub usage: borg_provider::ProviderCallUsage,
    /// A newly created provider conversation, when compaction had to rebuild
    /// context after switching providers.
    pub provider_session_id: Option<String>,
}

#[derive(Debug)]
pub enum AgentTurnControl {
    Steer {
        message_id: Uuid,
        text: String,
        attachments: Vec<PathBuf>,
        admission: SteerAdmission,
        ack: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    },
    Approval {
        approval_id: String,
        decision: crate::ApprovalDecision,
    },
    ProviderInteractionResponse {
        interaction_id: String,
        response: serde_json::Value,
    },
    Interrupt,
}

/// Executes one provider turn for the durable Borg session actor.
///
/// The actor owns conversation state, goals, todos, subagents, approvals, and
/// journaling. Execution location is deliberately outside that state machine:
/// enrolled hosts use [`LocalAgentTurnExecutor`], while Borg-managed
/// workspaces can inject a server executor without creating a second agent
/// loop.
#[async_trait::async_trait]
pub trait AgentTurnExecutor: Send + Sync {
    async fn execute(
        &self,
        turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult>;

    /// Whether a successful subscription turn can append only its new input
    /// to a provider-owned process on the next turn. The session actor uses
    /// this to avoid measuring the whole durable replay when the executor will
    /// send only the delta; executors without a pool must keep using the full
    /// replay budget.
    fn supports_subscription_context_reuse(&self, _provider: CodingProvider) -> bool {
        false
    }

    /// Return a live view of the trusted executable Blu workflows available to
    /// the session. The view is intentionally a closure so extension reloads
    /// become visible to model tools without rebuilding the dispatcher.
    fn extension_workflow_snapshot(
        &self,
    ) -> Option<Arc<dyn Fn() -> Vec<BluWorkflowDefinition> + Send + Sync>> {
        None
    }

    fn extension_api_snapshot(&self) -> Option<crate::ExtensionApiSnapshot> {
        None
    }

    /// Return the optional provider-neutral web-search capability for this
    /// execution host. The session actor injects it into the shared Borg tool
    /// dispatcher so native and subscription lanes see the same contract.
    fn web_search_provider(&self) -> Option<Arc<dyn borg_search::WebSearchProvider>> {
        None
    }

    /// Run an isolated, one-shot consultation without attaching it to the
    /// main session's provider conversation or exposing the main session's
    /// tools. Providers that cannot offer this path report a normal tool error.
    async fn consult(&self, _request: ConsultationRequest) -> Result<ConsultationResult> {
        anyhow::bail!("model consultation is not supported by this executor")
    }

    async fn compact(&self, _turn: AgentTurn) -> Result<Option<ProviderCallUsage>> {
        anyhow::bail!("manual context compaction is not supported by this provider")
    }

    async fn compact_native(
        &self,
        _provider: CodingProvider,
        _model: &str,
        _effort: Option<&str>,
        _conversation: Vec<borg_provider::provider::ModelMessage>,
    ) -> Result<AgentCompaction> {
        anyhow::bail!("native context compaction is not supported by this provider")
    }

    /// Compact a durable transcript when the selected provider has no native
    /// conversation yet, as happens immediately after a provider switch.
    async fn compact_retained_context(&self, _turn: AgentTurn) -> Result<AgentCompaction> {
        anyhow::bail!("cross-provider context compaction is not supported by this provider")
    }

    async fn stop_session(&self, _session_id: Uuid) -> Result<()> {
        Ok(())
    }
}

/// Direct provider execution used by the CLI and enrolled hosts.
#[derive(Clone)]
pub struct LocalAgentTurnExecutor {
    native_harness: NativeHarness,
    runtime_extensions: Arc<RwLock<RuntimeExtensions>>,
    runtime_extension_loader: Option<RuntimeExtensionLoader>,
    subscription_pools: Arc<SubscriptionPoolRegistry>,
    web_search: Option<Arc<dyn borg_search::WebSearchProvider>>,
    #[cfg(feature = "profiling")]
    profiler: Option<Arc<crate::RuntimeProfiler>>,
}

impl Default for LocalAgentTurnExecutor {
    fn default() -> Self {
        let web_search = match borg_search::SearchService::from_env() {
            Ok(service) => {
                service.map(|service| Arc::new(service) as Arc<dyn borg_search::WebSearchProvider>)
            }
            Err(error) => {
                tracing::warn!(%error, "web search configuration is invalid; search tool disabled");
                None
            }
        };
        Self {
            native_harness: NativeHarness::default(),
            runtime_extensions: Arc::new(RwLock::new(RuntimeExtensions::default())),
            runtime_extension_loader: None,
            subscription_pools: Arc::new(SubscriptionPoolRegistry::default()),
            web_search,
            #[cfg(feature = "profiling")]
            profiler: None,
        }
    }
}

type RuntimeExtensionLoader = Arc<
    dyn Fn() -> Result<(
            Vec<borg_provider::mcp::ExternalMcpServer>,
            Vec<PathBuf>,
            Vec<BluWorkflowDefinition>,
            crate::ExtensionApiSnapshot,
        )> + Send
        + Sync,
>;

#[derive(Clone, Default)]
struct RuntimeExtensions {
    external_mcp_servers: Vec<borg_provider::mcp::ExternalMcpServer>,
    skill_roots: Vec<PathBuf>,
    workflows: Vec<BluWorkflowDefinition>,
    api: crate::ExtensionApiSnapshot,
}

#[derive(Default)]
struct SubscriptionPoolRegistry {
    slots: Mutex<HashMap<Uuid, SubscriptionPoolSlot>>,
}

struct SubscriptionPoolSlot {
    provider: CodingProvider,
    lifecycle_key: String,
    context_generation: u64,
    epoch: u64,
    healthy: bool,
    pool: SubscriptionPool,
}

struct PreparedSubscriptionTurn {
    prompt: String,
    lifecycle_key: String,
    pool: SubscriptionPool,
    reused: bool,
    resume_session_id: Option<String>,
    fork_turn_id: Option<String>,
    resume_unavailable_prompt: Option<String>,
}

struct SubscriptionTurnInput {
    context_generation: u64,
    provider: CodingProvider,
    provider_session_id: Option<String>,
    provider_fork_turn_id: Option<String>,
    prompt: String,
    prompt_delta: String,
    lifecycle_key: String,
}

#[derive(Clone)]
enum SubscriptionPool {
    Claude(ClaudeSubscriptionPool),
    Codex(CodexSubscriptionPool),
}

impl SubscriptionPoolRegistry {
    async fn prepare(
        &self,
        session_id: Uuid,
        input: SubscriptionTurnInput,
    ) -> PreparedSubscriptionTurn {
        let SubscriptionTurnInput {
            context_generation,
            provider,
            provider_session_id,
            provider_fork_turn_id,
            prompt,
            prompt_delta,
            lifecycle_key,
        } = input;
        let mut slots = self.slots.lock().await;
        let slot = slots
            .entry(session_id)
            .or_insert_with(|| SubscriptionPoolSlot {
                provider: CodingProvider::Claude,
                lifecycle_key: String::new(),
                context_generation,
                epoch: 0,
                healthy: false,
                pool: SubscriptionPool::Claude(ClaudeSubscriptionPool::default()),
            });
        let append = slot.provider == provider
            && slot.healthy
            && slot.context_generation == context_generation
            && slot.lifecycle_key == lifecycle_key;
        // Reserve the volatile process pessimistically. If the executor task
        // is aborted, the success callback below cannot run. Codex may still
        // resume a separately persisted, acknowledged thread checkpoint;
        // otherwise the next turn replays Borg's durable journal.
        slot.healthy = false;
        if !append {
            if slot.provider != provider {
                slot.pool = match provider {
                    CodingProvider::Claude => {
                        SubscriptionPool::Claude(ClaudeSubscriptionPool::default())
                    }
                    CodingProvider::Codex => {
                        SubscriptionPool::Codex(CodexSubscriptionPool::default())
                    }
                    CodingProvider::OpenCode
                    | CodingProvider::Kimi
                    | CodingProvider::OpenRouter
                    | CodingProvider::OpenAiCompatible => {
                        unreachable!("native providers do not use subscription pools")
                    }
                };
            }
            slot.epoch = slot.epoch.saturating_add(1);
            slot.provider = provider;
            slot.lifecycle_key = lifecycle_key.clone();
            slot.context_generation = context_generation;
            slot.healthy = false;
        }
        let resume_session_id = (!append && provider == CodingProvider::Codex)
            .then_some(provider_session_id)
            .flatten();
        let fork_turn_id = resume_session_id.as_ref().and(provider_fork_turn_id);
        let reusing_native_context = append || resume_session_id.is_some();
        let resume_unavailable_prompt =
            (reusing_native_context && prompt != prompt_delta).then(|| prompt.clone());
        let effective_key = format!("{lifecycle_key}#epoch={}", slot.epoch);
        PreparedSubscriptionTurn {
            prompt: if reusing_native_context {
                prompt_delta
            } else {
                prompt.clone()
            },
            lifecycle_key: effective_key,
            pool: slot.pool.clone(),
            reused: reusing_native_context,
            resume_session_id,
            fork_turn_id,
            resume_unavailable_prompt,
        }
    }

    async fn mark(&self, session_id: Uuid, provider: CodingProvider, healthy: bool) {
        if let Some(slot) = self.slots.lock().await.get_mut(&session_id)
            && slot.provider == provider
        {
            slot.healthy = healthy;
            if !healthy {
                slot.epoch = slot.epoch.saturating_add(1);
            }
        }
    }
}

fn subscription_lifecycle_key(
    turn: &AgentTurn,
    request: &ChatStreamRequest,
    permission: PermissionMode,
) -> String {
    let mcp_servers = request
        .mcp_external_servers
        .iter()
        .map(|server| {
            serde_json::json!({
                "name": server.name,
                "command": server.command,
                "args": server.args,
                "env": server.env,
                "allowed_tools": server.allowed_tools,
            })
        })
        .collect::<Vec<_>>();
    let material = serde_json::json!({
        "version": 2,
        "session_id": turn.session_id,
        "context_generation": turn.context_generation,
        "provider": turn.provider.catalog_backend(),
        "model": request.model,
        "effort": request.effort,
        "fast": request.fast,
        "permission": format!("{permission:?}"),
        "cwd": request.working_directory.as_ref().map(|path| path.to_string_lossy()),
        "system_prompt": request.system_prompt,
        "output_schema": request.output_schema,
        "mcp_servers": mcp_servers,
        "provider_channel": request.provider_channel.as_str(),
        "web_search_allowed": request.web_search_allowed,
    });
    let digest = Sha256::digest(
        serde_json::to_vec(&material).expect("subscription lifecycle material is serializable"),
    );
    let digest = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("borg-{}-{digest}", turn.provider.catalog_backend())
}

#[derive(Debug, Clone, Default)]
pub struct LocalAgentSettings {
    pub approval_reviewer_model: Option<String>,
    pub approval_reviewer_effort: Option<String>,
    /// Presentation mode for the native harness tool catalog.
    pub tool_mode: ToolMode,
    /// Host-local snapshot of named OpenAI-compatible routes. Secrets stay in
    /// memory and are never part of LaunchSession or durable events.
    pub configured_model_gateways: BTreeMap<String, borg_provider::provider::ModelGateway>,
}

impl LocalAgentTurnExecutor {
    pub fn with_settings(settings: LocalAgentSettings) -> Self {
        Self {
            native_harness: NativeHarness::with_settings(&settings),
            ..Self::default()
        }
    }

    pub fn with_model_gateway(gateway: borg_provider::provider::ModelGateway) -> Self {
        Self::with_model_gateway_and_settings(gateway, LocalAgentSettings::default())
    }

    pub fn with_model_gateway_and_settings(
        gateway: borg_provider::provider::ModelGateway,
        settings: LocalAgentSettings,
    ) -> Self {
        Self {
            native_harness: NativeHarness::with_model_gateway(gateway, &settings),
            ..Self::default()
        }
    }

    /// Use a different execution world for native tools and persistent
    /// runtimes while preserving the model-facing Borg tool contract.
    pub fn with_execution_provider(mut self, provider: Arc<dyn crate::ExecutionProvider>) -> Self {
        self.native_harness = self.native_harness.with_execution_provider(provider);
        self
    }

    pub fn with_web_search_provider(
        mut self,
        provider: Arc<dyn borg_search::WebSearchProvider>,
    ) -> Self {
        self.web_search = Some(provider);
        self
    }

    #[cfg(feature = "profiling")]
    pub fn with_profiler(mut self, profiler: Arc<crate::RuntimeProfiler>) -> Self {
        self.profiler = Some(profiler);
        self
    }

    pub fn with_external_mcp_servers(
        self,
        servers: Vec<borg_provider::mcp::ExternalMcpServer>,
    ) -> Self {
        self.runtime_extensions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .external_mcp_servers = servers;
        self
    }

    pub fn with_extension_skill_roots(self, roots: Vec<PathBuf>) -> Self {
        self.runtime_extensions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .skill_roots = roots;
        self
    }

    pub fn with_extension_workflows(self, workflows: Vec<BluWorkflowDefinition>) -> Self {
        self.runtime_extensions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .workflows = workflows;
        self
    }

    pub fn with_extension_api(self, api: crate::ExtensionApiSnapshot) -> Self {
        self.runtime_extensions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .api = api;
        self
    }

    pub fn replace_runtime_extensions(
        &self,
        servers: Vec<borg_provider::mcp::ExternalMcpServer>,
        skill_roots: Vec<PathBuf>,
        workflows: Vec<BluWorkflowDefinition>,
    ) {
        let api = self
            .runtime_extensions
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .api
            .clone();
        *self
            .runtime_extensions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = RuntimeExtensions {
            external_mcp_servers: servers,
            skill_roots,
            workflows,
            api,
        };
    }

    pub fn replace_runtime_extensions_with_api(
        &self,
        servers: Vec<borg_provider::mcp::ExternalMcpServer>,
        skill_roots: Vec<PathBuf>,
        workflows: Vec<BluWorkflowDefinition>,
        api: crate::ExtensionApiSnapshot,
    ) {
        *self
            .runtime_extensions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = RuntimeExtensions {
            external_mcp_servers: servers,
            skill_roots,
            workflows,
            api,
        };
    }

    pub fn with_runtime_extension_loader<F>(mut self, loader: F) -> Self
    where
        F: Fn() -> Result<(
                Vec<borg_provider::mcp::ExternalMcpServer>,
                Vec<PathBuf>,
                Vec<BluWorkflowDefinition>,
                crate::ExtensionApiSnapshot,
            )> + Send
            + Sync
            + 'static,
    {
        self.runtime_extension_loader = Some(Arc::new(loader));
        self
    }

    async fn refresh_runtime_extensions(&self) {
        let Some(loader) = self.runtime_extension_loader.clone() else {
            return;
        };
        match tokio::task::spawn_blocking(move || loader()).await {
            Ok(Ok((servers, roots, workflows, api))) => {
                self.replace_runtime_extensions_with_api(servers, roots, workflows, api)
            }
            Ok(Err(error)) => {
                tracing::warn!(%error, "kept last-known-good runtime extension snapshot");
            }
            Err(error) => {
                tracing::warn!(%error, "runtime extension loader stopped unexpectedly");
            }
        }
    }

    async fn prepare_local_turn(&self, turn: &mut AgentTurn) -> Result<()> {
        self.refresh_runtime_extensions().await;
        let runtime_extensions = self
            .runtime_extensions
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        turn.agent_tools
            .configure_runtime_mcp_extensions(runtime_extensions.external_mcp_servers.clone())
            .await?;
        turn.agent_tools
            .configure_extension_workflows(runtime_extensions.workflows.clone());
        turn.agent_tools
            .configure_extension_api(runtime_extensions.api.clone())?;
        turn.extension_api = runtime_extensions.api.clone();
        for wire_name in runtime_extensions.api.tool_wires() {
            let wire_name = format!("mcp__borg_agent__{wire_name}");
            if !turn.agent_mcp_server.allowed_tools.contains(&wire_name) {
                turn.agent_mcp_server.allowed_tools.push(wire_name);
            }
        }
        for wire_name in runtime_extensions.api.command_wires() {
            let wire_name = format!("mcp__borg_agent__{wire_name}");
            if !turn.agent_mcp_server.allowed_tools.contains(&wire_name) {
                turn.agent_mcp_server.allowed_tools.push(wire_name);
            }
        }
        turn.system_prompt_appendix
            .push_str(&runtime_extensions.api.prompt_appendix());
        turn.system_prompt_appendix
            .push_str(&runtime_extensions.api.context_appendix());
        self.run_extension_hooks(
            turn,
            "turn_started",
            serde_json::json!({
                "event": "turn_started",
                "session_id": turn.session_id,
                "message_id": turn.message_id,
                "context_generation": turn.context_generation,
                "provider": turn.provider,
                "model": turn.model.clone(),
                "prompt_delta": turn.prompt_delta.chars().take(16_384).collect::<String>(),
                "attachments": turn.attachments.clone(),
            }),
        )
        .await?;
        turn.external_mcp_servers
            .extend(runtime_extensions.external_mcp_servers);
        turn.extension_skill_roots
            .extend(runtime_extensions.skill_roots);
        turn.extension_workflows
            .extend(runtime_extensions.workflows);
        if !turn.extension_skill_roots.is_empty() {
            turn.system_prompt_appendix.push_str(
                &crate::native_context::extension_skill_prompt_appendix(
                    turn.extension_skill_roots.clone(),
                )
                .await?,
            );
        }
        Ok(())
    }

    async fn run_extension_hooks(
        &self,
        turn: &AgentTurn,
        event: &str,
        arguments: Value,
    ) -> Result<()> {
        turn.agent_tools
            .run_extension_hooks(event, turn.message_id, arguments)
            .await
    }
}

fn completed_hook_arguments(turn: &AgentTurn, result: &Result<AgentTurnResult>) -> Value {
    let outcome = match result {
        Ok(result) => serde_json::json!({
            "provider_session_id": result.provider_session_id,
            "final_text": result.final_text.chars().take(16_384).collect::<String>(),
        }),
        Err(error) => serde_json::json!({"error": error.to_string()}),
    };
    serde_json::json!({
        "event": "turn_completed",
        "session_id": turn.session_id,
        "message_id": turn.message_id,
        "context_generation": turn.context_generation,
        "provider": turn.provider,
        "model": turn.model,
        "result": outcome,
    })
}

#[async_trait::async_trait]
impl AgentTurnExecutor for LocalAgentTurnExecutor {
    fn web_search_provider(&self) -> Option<Arc<dyn borg_search::WebSearchProvider>> {
        self.web_search.clone()
    }

    fn supports_subscription_context_reuse(&self, provider: CodingProvider) -> bool {
        matches!(provider, CodingProvider::Codex | CodingProvider::Claude)
    }

    fn extension_workflow_snapshot(
        &self,
    ) -> Option<Arc<dyn Fn() -> Vec<BluWorkflowDefinition> + Send + Sync>> {
        let runtime_extensions = Arc::clone(&self.runtime_extensions);
        Some(Arc::new(move || {
            runtime_extensions
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .workflows
                .clone()
        }))
    }

    fn extension_api_snapshot(&self) -> Option<crate::ExtensionApiSnapshot> {
        Some(
            self.runtime_extensions
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .api
                .clone(),
        )
    }

    async fn execute(
        &self,
        mut turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        #[cfg(feature = "profiling")]
        let profile_started = self
            .profiler
            .as_ref()
            .map(|profiler| profiler.begin_turn(turn.provider));
        match self.prepare_local_turn(&mut turn).await {
            Ok(()) => {}
            Err(error) => {
                #[cfg(feature = "profiling")]
                if let Some((profiler, started)) = self.profiler.as_ref().zip(profile_started) {
                    profiler.finish_turn(turn.provider, started, false);
                }
                return Err(error);
            }
        }
        #[cfg(feature = "profiling")]
        let profile_provider = turn.provider;
        #[cfg(feature = "profiling")]
        if let Some(profiler) = self.profiler.as_ref() {
            profiler.set_phase("provider_start");
        }
        if turn.provider.uses_native_harness() {
            let result = self
                .native_harness
                .run(turn.clone(), events, controls)
                .await;
            #[cfg(feature = "profiling")]
            if let Some((profiler, started)) = self.profiler.as_ref().zip(profile_started) {
                profiler.finish_turn(profile_provider, started, result.is_ok());
            }
            if let Err(error) = self
                .run_extension_hooks(
                    &turn,
                    "turn_completed",
                    completed_hook_arguments(&turn, &result),
                )
                .await
            {
                tracing::warn!(%error, "extension turn_completed hook failed");
            }
            return result;
        }
        let completed_hook_turn = turn.clone();
        let result = match turn.provider {
            CodingProvider::Codex | CodingProvider::Claude | CodingProvider::OpenCode => {
                run_borg_provider_turn(
                    turn,
                    events,
                    controls,
                    BorgProviderTurnRuntime {
                        request_template: None,
                        local: true,
                        subscription_pools: Some(Arc::clone(&self.subscription_pools)),
                        #[cfg(feature = "profiling")]
                        profiler: self.profiler.clone(),
                    },
                    true,
                )
                .await
            }
            CodingProvider::Kimi
            | CodingProvider::OpenRouter
            | CodingProvider::OpenAiCompatible => unreachable!("native provider handled above"),
        };
        #[cfg(feature = "profiling")]
        if let Some((profiler, started)) = self.profiler.as_ref().zip(profile_started) {
            profiler.finish_turn(profile_provider, started, result.is_ok());
        }
        if let Err(error) = self
            .run_extension_hooks(
                &completed_hook_turn,
                "turn_completed",
                completed_hook_arguments(&completed_hook_turn, &result),
            )
            .await
        {
            tracing::warn!(%error, "extension turn_completed hook failed");
        }
        result
    }

    async fn consult(&self, request: ConsultationRequest) -> Result<ConsultationResult> {
        if request.provider.uses_native_harness() {
            let model = request
                .model
                .as_deref()
                .context("native consultation requires an explicit model")?;
            let (final_text, usage) = self
                .native_harness
                .consult(
                    request.provider,
                    model,
                    request.effort.as_deref(),
                    request.response_language,
                    &request.prompt,
                )
                .await?;
            return Ok(ConsultationResult {
                provider: request.provider,
                model: request.model,
                final_text,
                usage,
            });
        }
        bail!(
            "{:?} consultation is unavailable without a native provider route",
            request.provider
        )
    }

    async fn compact(&self, mut turn: AgentTurn) -> Result<Option<ProviderCallUsage>> {
        anyhow::ensure!(
            turn.provider == CodingProvider::Codex,
            "provider-session compaction is unavailable for {:?}",
            turn.provider
        );
        let provider_session_id = turn
            .provider_session_id
            .clone()
            .context("Codex native compaction requires a provider thread")?;
        self.prepare_local_turn(&mut turn).await?;
        let permission = local_permission(turn.permission_mode);
        let mut request = direct_chat_stream_request(&turn, true, "");
        let lifecycle_key = subscription_lifecycle_key(&turn, &request, turn.permission_mode);
        let prepared = self
            .subscription_pools
            .prepare(
                turn.session_id,
                SubscriptionTurnInput {
                    context_generation: turn.context_generation,
                    provider: turn.provider,
                    provider_session_id: Some(provider_session_id.clone()),
                    provider_fork_turn_id: None,
                    prompt: String::new(),
                    prompt_delta: String::new(),
                    lifecycle_key,
                },
            )
            .await;
        request.prompt = prepared.prompt.clone();
        request.lifecycle_key = Some(prepared.lifecycle_key.clone());
        request.session_id = prepared.resume_session_id.clone();
        request.fork_turn_id = None;
        request.resume_unavailable_prompt = prepared.resume_unavailable_prompt.clone();
        request.persist_session = Some(true);
        let pool = match prepared.pool {
            SubscriptionPool::Codex(pool) => pool,
            SubscriptionPool::Claude(_) => unreachable!("Codex slot contained Claude pool"),
        };
        match pool
            .compact(request, permission, &provider_session_id)
            .await
        {
            Ok(usage) => {
                self.subscription_pools
                    .mark(turn.session_id, CodingProvider::Codex, true)
                    .await;
                Ok(Some(usage))
            }
            Err(error) => {
                self.subscription_pools
                    .mark(turn.session_id, CodingProvider::Codex, false)
                    .await;
                Err(error)
            }
        }
    }

    async fn compact_native(
        &self,
        provider: CodingProvider,
        model: &str,
        effort: Option<&str>,
        conversation: Vec<borg_provider::provider::ModelMessage>,
    ) -> Result<AgentCompaction> {
        let (summary, usage) = self
            .native_harness
            .compact(provider, model, effort, conversation)
            .await?;
        Ok(AgentCompaction {
            summary,
            usage,
            provider_session_id: None,
        })
    }

    async fn compact_retained_context(&self, turn: AgentTurn) -> Result<AgentCompaction> {
        anyhow::ensure!(
            matches!(
                turn.provider,
                CodingProvider::Codex | CodingProvider::Claude
            ),
            "{:?} does not support subscription context compaction",
            turn.provider
        );

        // Compaction is another Borg-owned, ephemeral provider call. The CLI
        // may use the subscription login to generate the summary, but its
        // session files are not used and the summary is committed by the
        // session actor as a Borg context_compaction event.
        let (provider_events_tx, mut provider_events) = mpsc::channel(128);
        let task = tokio::spawn(run_borg_provider_turn(
            turn,
            provider_events_tx,
            None,
            BorgProviderTurnRuntime {
                request_template: None,
                local: true,
                subscription_pools: None,
                #[cfg(feature = "profiling")]
                profiler: None,
            },
            false,
        ));
        let mut usage = ProviderCallUsage::default();
        while let Some(event) = provider_events.recv().await {
            if let SessionEventKind::UsageUpdated {
                provider_duration_ms,
                input_tokens,
                output_tokens,
                cached_input_tokens,
                cache_creation_input_tokens,
                total_tokens,
                context_tokens,
                context_window_tokens,
                cost_microusd,
                ..
            } = event
            {
                usage = ProviderCallUsage {
                    duration_ms: provider_duration_ms,
                    input_tokens,
                    output_tokens,
                    cached_input_tokens,
                    cache_creation_input_tokens,
                    total_tokens,
                    context_tokens,
                    context_window_tokens,
                    cost_microusd,
                    ..ProviderCallUsage::default()
                };
            }
        }
        let result = task
            .await
            .context("subscription compaction task stopped unexpectedly")??;
        anyhow::ensure!(
            !result.final_text.trim().is_empty(),
            "subscription compaction returned an empty summary"
        );
        Ok(AgentCompaction {
            summary: result.final_text,
            usage,
            provider_session_id: None,
        })
    }

    async fn stop_session(&self, session_id: Uuid) -> Result<()> {
        // The Borg journal is always durable. Let an idle Codex app-server
        // flush its own acknowledged rollout too, so a later actor can resume
        // that cache-preserving checkpoint rather than replaying the journal.
        let slot = self
            .subscription_pools
            .slots
            .lock()
            .await
            .remove(&session_id);
        if let Some(SubscriptionPoolSlot {
            pool: SubscriptionPool::Codex(pool),
            ..
        }) = slot
        {
            pool.shutdown().await;
        }
        self.native_harness.stop_session(session_id).await;
        Ok(())
    }
}

pub async fn run_agent_turn(
    turn: AgentTurn,
    events: mpsc::Sender<SessionEventKind>,
) -> Result<AgentTurnResult> {
    run_agent_turn_controlled(turn, events, None).await
}

pub async fn run_agent_turn_controlled(
    turn: AgentTurn,
    events: mpsc::Sender<SessionEventKind>,
    controls: Option<mpsc::Receiver<AgentTurnControl>>,
) -> Result<AgentTurnResult> {
    events
        .send(SessionEventKind::StatusChanged {
            status: SessionStatus::Running,
            detail: None,
        })
        .await
        .ok();
    match turn.provider {
        CodingProvider::Kimi | CodingProvider::OpenRouter | CodingProvider::OpenAiCompatible => {
            NativeHarness::default().run(turn, events, controls).await
        }
        CodingProvider::Codex | CodingProvider::Claude | CodingProvider::OpenCode => {
            run_borg_provider_turn(
                turn,
                events,
                controls,
                BorgProviderTurnRuntime {
                    request_template: None,
                    local: true,
                    subscription_pools: None,
                    #[cfg(feature = "profiling")]
                    profiler: None,
                },
                true,
            )
            .await
        }
    }
}

struct BorgProviderTurnRuntime {
    request_template: Option<ChatStreamRequest>,
    local: bool,
    subscription_pools: Option<Arc<SubscriptionPoolRegistry>>,
    #[cfg(feature = "profiling")]
    profiler: Option<Arc<crate::RuntimeProfiler>>,
}

fn direct_chat_stream_request(
    turn: &AgentTurn,
    tools_enabled: bool,
    prompt_context: &str,
) -> ChatStreamRequest {
    let response_language_instruction = turn.response_language.instruction();
    let mcp_external_servers = if tools_enabled {
        let mut servers = turn.external_mcp_servers.clone();
        servers.push(turn.agent_mcp_server.clone());
        servers
    } else {
        Vec::new()
    };
    ChatStreamRequest {
        prompt: append_prompt_context(&turn.prompt, prompt_context),
        lifecycle_key: None,
        owner_session_id: Some(turn.session_id.to_string()),
        client_user_message_id: Some(turn.message_id.to_string()),
        attachments: turn.attachments.clone(),
        model: turn.model.clone(),
        effort: turn.effort.clone(),
        fast: turn.fast.unwrap_or(false),
        system_prompt: match response_language_instruction {
            Some(instruction) => format!("{CODING_SYSTEM_PROMPT}\n\n{instruction}"),
            None => CODING_SYSTEM_PROMPT.to_string(),
        } + if turn.system_prompt_appendix.is_empty() {
            ""
        } else {
            "\n\n"
        } + &turn.system_prompt_appendix,
        output_schema: turn.output_schema.clone(),
        mcp_owner_id: turn.runtime_mcp_context.owner_id.clone(),
        mcp_allowed_scopes: turn.runtime_mcp_context.allowed_scopes.clone(),
        mcp_user_id: turn.runtime_mcp_context.user_id.clone(),
        mcp_external_servers,
        mcp_api_token: turn.runtime_mcp_context.api_token.clone(),
        provider_auth: None,
        git_credentials: Vec::new(),
        working_directory: Some(turn.cwd.clone()),
        session_id: None,
        fork_turn_id: None,
        provider_channel: ProviderChannel::Direct,
        persist_session: Some(false),
        web_search_allowed: true,
        resume_unavailable_prompt: None,
    }
}

fn append_prompt_context(prompt: &str, context: &str) -> String {
    if context.is_empty() {
        return prompt.to_string();
    }
    if prompt.is_empty() {
        return context.to_string();
    }
    format!(
        "{}\n{}",
        prompt,
        crate::session::format_subscription_prompt_context(context)
    )
}

const CLEARED_HARNESS_PROMPT_CONTEXT: &str = "## Continual harness state\nNo persistent harness state is currently configured. Ignore earlier harness state snapshots.";

fn next_harness_prompt_context(
    conversation: &[borg_provider::provider::ModelMessage],
    current: String,
) -> Option<String> {
    let previous = conversation.iter().rev().find_map(|message| match message {
        borg_provider::provider::ModelMessage::User { content, .. }
            if content
                .trim_start()
                .starts_with("## Continual harness state") =>
        {
            Some(content.as_str())
        }
        _ => None,
    });
    if current.is_empty() {
        return previous
            .filter(|content| *content != CLEARED_HARNESS_PROMPT_CONTEXT)
            .map(|_| CLEARED_HARNESS_PROMPT_CONTEXT.to_string());
    }
    (previous != Some(current.as_str())).then_some(current)
}

#[cfg(test)]
mod prompt_context_tests {
    use super::{
        CLEARED_HARNESS_PROMPT_CONTEXT, append_prompt_context, next_harness_prompt_context,
    };

    use borg_provider::provider::ModelMessage;

    #[test]
    fn mutable_context_is_appended_after_the_existing_prompt() {
        let prompt = "<borg-message>{\"role\":\"user\",\"content\":\"request\"}</borg-message>";
        let result = append_prompt_context(prompt, "mutable harness snapshot");

        assert!(result.starts_with(&format!("{prompt}\n")));
        assert!(result.ends_with("</borg-message>"));
        assert!(!result[..prompt.len()].contains("mutable harness snapshot"));
    }

    #[test]
    fn unchanged_harness_context_is_not_replayed_into_the_warm_tail() {
        let context = "\n\n## Continual harness state\nstate";
        let conversation = vec![ModelMessage::user(context)];

        assert!(next_harness_prompt_context(&conversation, context.to_string()).is_none());
        assert_eq!(
            next_harness_prompt_context(&conversation, "updated".to_string()).as_deref(),
            Some("updated")
        );
        assert_eq!(
            next_harness_prompt_context(&conversation, String::new()).as_deref(),
            Some(CLEARED_HARNESS_PROMPT_CONTEXT)
        );
    }
}

async fn run_borg_provider_turn(
    turn: AgentTurn,
    events: mpsc::Sender<SessionEventKind>,
    controls: Option<mpsc::Receiver<AgentTurnControl>>,
    runtime: BorgProviderTurnRuntime,
    tools_enabled: bool,
) -> Result<AgentTurnResult> {
    let BorgProviderTurnRuntime {
        request_template,
        local,
        subscription_pools,
        #[cfg(feature = "profiling")]
        profiler,
    } = runtime;
    let provider_turn_started = Instant::now();
    let ttft_session_id = turn.session_id;
    let ttft_message_id = turn.message_id;
    let pool_turn = turn.clone();
    let response_language_instruction = turn.response_language.instruction();
    let prompt_context = if tools_enabled
        && matches!(
            turn.provider,
            CodingProvider::Codex | CodingProvider::Claude | CodingProvider::OpenCode
        ) {
        next_harness_prompt_context(
            &turn.conversation,
            turn.agent_tools.harness_prompt_appendix().await?,
        )
    } else {
        None
    };
    if let Some(prompt_context) = prompt_context.as_ref() {
        let context_message = borg_provider::provider::ModelMessage::user(prompt_context.clone());
        crate::native_harness::record_native_prompt_context(
            &events,
            turn.provider,
            &context_message,
        )
        .await?;
    }
    let mut request = match request_template {
        Some(mut request) => {
            request.prompt = turn.prompt.clone();
            if let Some(prompt_context) = prompt_context.as_deref() {
                request.prompt = append_prompt_context(&request.prompt, prompt_context);
            }
            request.owner_session_id = Some(turn.session_id.to_string());
            request.client_user_message_id = Some(turn.message_id.to_string());
            request.attachments = turn.attachments;
            request.output_schema = turn.output_schema;
            request.model = turn.model.clone().or(request.model);
            request.effort = turn.effort.clone().or(request.effort);
            if let Some(fast) = turn.fast {
                request.fast = fast;
            }
            request.working_directory = Some(turn.cwd.clone());
            request.mcp_owner_id = turn.runtime_mcp_context.owner_id.clone();
            request.mcp_allowed_scopes = turn.runtime_mcp_context.allowed_scopes.clone();
            request.mcp_user_id = turn.runtime_mcp_context.user_id.clone();
            request.mcp_api_token = turn.runtime_mcp_context.api_token.clone();
            // Borg's journal is the canonical provider context for
            // subscription lanes. Replaying a provider-owned session here
            // would silently omit durable Borg tool events or duplicate the
            // locally reconstructed prefix.
            request.session_id = None;
            request.fork_turn_id = None;
            request.persist_session = Some(false);
            request.resume_unavailable_prompt = None;
            if tools_enabled {
                request
                    .mcp_external_servers
                    .extend(turn.external_mcp_servers);
                request.mcp_external_servers.push(turn.agent_mcp_server);
            }
            if let Some(instruction) = response_language_instruction {
                request.system_prompt.push_str("\n\n");
                request.system_prompt.push_str(instruction);
            }
            if !turn.system_prompt_appendix.is_empty() {
                request.system_prompt.push_str("\n\n");
                request.system_prompt.push_str(&turn.system_prompt_appendix);
            }
            request
        }
        None => direct_chat_stream_request(
            &turn,
            tools_enabled,
            prompt_context.as_deref().unwrap_or_default(),
        ),
    };
    let permission = local_permission(turn.permission_mode);
    let pool_invocation = if local
        && matches!(
            turn.provider,
            CodingProvider::Claude | CodingProvider::Codex
        )
        && let Some(registry) = subscription_pools.as_ref()
    {
        let lifecycle_key = subscription_lifecycle_key(&pool_turn, &request, turn.permission_mode);
        let prepared = registry
            .prepare(
                turn.session_id,
                SubscriptionTurnInput {
                    context_generation: turn.context_generation,
                    provider: turn.provider,
                    provider_session_id: turn.provider_session_id.clone(),
                    provider_fork_turn_id: turn.provider_fork_turn_id.clone(),
                    prompt: append_prompt_context(
                        &turn.prompt,
                        prompt_context.as_deref().unwrap_or_default(),
                    ),
                    prompt_delta: append_prompt_context(
                        &turn.prompt_delta,
                        prompt_context.as_deref().unwrap_or_default(),
                    ),
                    lifecycle_key,
                },
            )
            .await;
        request.prompt = prepared.prompt.clone();
        request.lifecycle_key = Some(prepared.lifecycle_key.clone());
        request.session_id = prepared.resume_session_id.clone();
        request.fork_turn_id = prepared.fork_turn_id.clone();
        request.resume_unavailable_prompt = prepared.resume_unavailable_prompt.clone();
        // Codex app-server threads are the cache-preserving continuation of
        // Borg's durable journal. Persist them so an idle process restart can
        // resume the acknowledged checkpoint instead of replaying a possibly
        // over-limit transcript. Borg remains the source of truth and sends a
        // full replay whenever the checkpoint is absent or invalidated.
        if turn.provider == CodingProvider::Codex {
            request.persist_session = Some(true);
        }
        Some((Arc::clone(registry), prepared))
    } else {
        None
    };
    let pooled_claude = pool_invocation
        .as_ref()
        .and_then(|(_, prepared)| match &prepared.pool {
            SubscriptionPool::Claude(pool) => Some(pool.clone()),
            SubscriptionPool::Codex(_) => None,
        });
    let pooled_codex = pool_invocation
        .as_ref()
        .and_then(|(_, prepared)| match &prepared.pool {
            SubscriptionPool::Claude(_) => None,
            SubscriptionPool::Codex(pool) => Some(pool.clone()),
        });
    let interrupted = Arc::new(AtomicBool::new(false));
    let mut stream = match turn.provider {
        CodingProvider::Codex => {
            let control_rx = map_controls(controls, Arc::clone(&interrupted));
            if let Some(pool) = pooled_codex {
                run_codex_local_chat_stream_pooled(request, control_rx, permission, pool)
            } else if local {
                run_codex_local_chat_stream(request, control_rx, permission)
            } else {
                run_codex_chat_stream_with_control(request, control_rx)
            }
        }
        CodingProvider::Claude if local => {
            if let Some(pool) = pooled_claude {
                run_claude_local_chat_stream_pooled(
                    request,
                    map_controls(controls, Arc::clone(&interrupted)),
                    permission,
                    pool,
                )
            } else {
                run_claude_local_chat_stream(
                    request,
                    map_controls(controls, Arc::clone(&interrupted)),
                    permission,
                )
            }
        }
        CodingProvider::Claude => run_claude_chat_stream_with_control(
            request,
            map_controls(controls, Arc::clone(&interrupted)),
        ),
        CodingProvider::OpenCode if local => run_opencode_local_chat_stream(request, permission),
        CodingProvider::OpenCode => {
            bail!("OpenCode execution is only supported on an enrolled host")
        }
        provider => bail!("{provider:?} must use a NativeHarness-compatible route"),
    };
    tracing::debug!(
        target: "borg_ttft",
        stage = "provider_stream_created",
        elapsed_ms = provider_turn_started.elapsed().as_millis(),
        session_id = %ttft_session_id,
        message_id = %ttft_message_id,
        "Borg provider stage"
    );
    #[cfg(feature = "profiling")]
    if let Some(profiler) = profiler.as_ref() {
        profiler.set_phase("provider_wait");
    }
    let mut assistant_message_id = Uuid::new_v4();
    let mut text = String::new();
    let mut final_output = String::new();
    let mut completed_segment = false;
    let mut last_text_emit = Instant::now() - Duration::from_millis(50);
    let mut provider_session_id = turn.provider_session_id;
    let mut first_model_output = true;
    let mut terminal_seen = false;
    let mut reasoning_text = String::new();
    let mut pending_reasoning = String::new();
    let mut last_reasoning_emit = Instant::now() - Duration::from_millis(50);
    let mut last_completed_reasoning = None;
    while let Some(event) = stream.recv().await {
        match event {
            ChatStreamEvent::ProviderEvent { kind, payload, .. } => {
                if let Some(usage) = live_context_usage(&kind, &payload) {
                    send(
                        &events,
                        SessionEventKind::ContextWindowUpdated {
                            context_tokens: usage.total_tokens,
                            context_window_tokens: usage.context_window_tokens,
                        },
                    )
                    .await;
                    continue;
                }
                if provider_event_is_transient(&kind) {
                    continue;
                }
                send(
                    &events,
                    SessionEventKind::ProviderEvent {
                        provider: turn.provider,
                        kind,
                        payload,
                    },
                )
                .await;
            }
            ChatStreamEvent::Delta(delta) => {
                if first_model_output {
                    first_model_output = false;
                    #[cfg(feature = "profiling")]
                    if let Some(profiler) = profiler.as_ref() {
                        profiler.set_phase("model_output");
                    }
                    tracing::debug!(
                        target: "borg_ttft",
                        stage = "first_model_output",
                        output_kind = "text",
                        elapsed_ms = provider_turn_started.elapsed().as_millis(),
                        session_id = %ttft_session_id,
                        message_id = %ttft_message_id,
                        "Borg provider stage"
                    );
                }
                text.push_str(&delta);
                let Some(visible_text) = without_action_intent_marker(&text) else {
                    continue;
                };
                if last_text_emit.elapsed() >= live_output_interval(text.len()) {
                    send(
                        &events,
                        SessionEventKind::Message {
                            message_id: assistant_message_id,
                            actor: EventActor::Assistant,
                            text: visible_text.to_string(),
                            attachments: Vec::new(),
                            status: MessageStatus::InProgress,
                            delivery: None,
                        },
                    )
                    .await;
                    last_text_emit = Instant::now();
                }
            }
            ChatStreamEvent::ReasoningDelta(delta) => {
                let Some(delta) = normalize_reasoning_delta_after_completion(
                    &mut reasoning_text,
                    &mut last_completed_reasoning,
                    &delta,
                ) else {
                    continue;
                };
                if first_model_output {
                    first_model_output = false;
                    #[cfg(feature = "profiling")]
                    if let Some(profiler) = profiler.as_ref() {
                        profiler.set_phase("model_output");
                    }
                    tracing::debug!(
                        target: "borg_ttft",
                        stage = "first_model_output",
                        output_kind = "reasoning",
                        elapsed_ms = provider_turn_started.elapsed().as_millis(),
                        session_id = %ttft_session_id,
                        message_id = %ttft_message_id,
                        "Borg provider stage"
                    );
                }
                pending_reasoning.push_str(&delta);
                if last_reasoning_emit.elapsed() >= live_output_interval(reasoning_text.len()) {
                    flush_pending_reasoning(&events, &mut pending_reasoning).await;
                    last_reasoning_emit = Instant::now();
                }
            }
            ChatStreamEvent::Narration {
                text: narration_text,
            } => {
                flush_pending_reasoning(&events, &mut pending_reasoning).await;
                reasoning_text.clear();
                last_completed_reasoning = None;
                if first_model_output {
                    first_model_output = false;
                    #[cfg(feature = "profiling")]
                    if let Some(profiler) = profiler.as_ref() {
                        profiler.set_phase("model_output");
                    }
                    tracing::debug!(
                        target: "borg_ttft",
                        stage = "first_model_output",
                        output_kind = "narration",
                        elapsed_ms = provider_turn_started.elapsed().as_millis(),
                        session_id = %ttft_session_id,
                        message_id = %ttft_message_id,
                        "Borg provider stage"
                    );
                }
                if let Some(label) = action_intent_label(&narration_text) {
                    text.clear();
                    assistant_message_id = Uuid::new_v4();
                    send(
                        &events,
                        SessionEventKind::ProviderEvent {
                            provider: turn.provider,
                            kind: "action/preparing".to_string(),
                            payload: serde_json::json!({"label": label}),
                        },
                    )
                    .await;
                    continue;
                }
                if without_action_intent_marker(&narration_text).is_none() {
                    text.clear();
                    continue;
                }
                text = narration_text;
                send(
                    &events,
                    SessionEventKind::Message {
                        message_id: assistant_message_id,
                        actor: EventActor::Assistant,
                        text: text.clone(),
                        attachments: Vec::new(),
                        status: MessageStatus::Complete,
                        delivery: None,
                    },
                )
                .await;
                completed_segment = true;
                assistant_message_id = Uuid::new_v4();
                text.clear();
                last_text_emit = Instant::now() - Duration::from_millis(50);
            }
            ChatStreamEvent::Phase { name, input } => {
                if name == "reasoning_completed" {
                    flush_pending_reasoning(&events, &mut pending_reasoning).await;
                    last_completed_reasoning =
                        (!reasoning_text.is_empty()).then(|| reasoning_text.clone());
                    reasoning_text.clear();
                    send(&events, SessionEventKind::ReasoningCompleted).await;
                    continue;
                }
                send(
                    &events,
                    SessionEventKind::ProviderEvent {
                        provider: turn.provider,
                        kind: name,
                        payload: input,
                    },
                )
                .await;
            }
            ChatStreamEvent::ToolCall { id, name, input } => {
                anyhow::ensure!(
                    !provider_native_agent_tool(&name),
                    "Codex exposed a forbidden provider-native agent tool: {name}"
                );
                flush_pending_reasoning(&events, &mut pending_reasoning).await;
                reasoning_text.clear();
                last_completed_reasoning = None;
                if first_model_output {
                    first_model_output = false;
                    tracing::debug!(
                        target: "borg_ttft",
                        stage = "first_model_output",
                        output_kind = "tool_call",
                        elapsed_ms = provider_turn_started.elapsed().as_millis(),
                        session_id = %ttft_session_id,
                        message_id = %ttft_message_id,
                        "Borg provider stage"
                    );
                }
                send(
                    &events,
                    SessionEventKind::ToolStarted {
                        tool_call_id: id,
                        name,
                        input,
                        input_ref: None,
                    },
                )
                .await;
                #[cfg(feature = "profiling")]
                if let Some(profiler) = profiler.as_ref() {
                    profiler.set_phase("tool_execution");
                }
            }
            ChatStreamEvent::ToolCallUpdate { id, name, input } => {
                send(
                    &events,
                    SessionEventKind::ToolUpdated {
                        tool_call_id: id,
                        name,
                        input,
                    },
                )
                .await;
            }
            ChatStreamEvent::ToolResult {
                tool_use_id,
                output,
                is_error,
                input,
            } => {
                send(
                    &events,
                    SessionEventKind::ToolCompleted {
                        tool_call_id: tool_use_id,
                        output,
                        output_ref: None,
                        is_error,
                        input,
                        input_ref: None,
                    },
                )
                .await;
                #[cfg(feature = "profiling")]
                if let Some(profiler) = profiler.as_ref() {
                    profiler.set_phase("provider_wait");
                }
            }
            ChatStreamEvent::ApprovalRequested {
                approval_id,
                title,
                detail,
                command,
            } => {
                send(
                    &events,
                    SessionEventKind::StatusChanged {
                        status: SessionStatus::WaitingForApproval,
                        detail: None,
                    },
                )
                .await;
                send(
                    &events,
                    SessionEventKind::ApprovalRequested {
                        approval_id,
                        title,
                        detail,
                        command,
                    },
                )
                .await;
            }
            ChatStreamEvent::ProviderInteractionRequested {
                interaction_id,
                kind,
                title,
                detail,
                payload,
            } => {
                send(
                    &events,
                    SessionEventKind::ProviderInteractionRequested {
                        interaction_id,
                        kind,
                        title,
                        detail,
                        payload,
                    },
                )
                .await;
            }
            ChatStreamEvent::Done {
                final_text,
                usage,
                session_id,
                provider_turn_id,
            } => {
                flush_pending_reasoning(&events, &mut pending_reasoning).await;
                terminal_seen = true;
                final_output = final_text;
                if let Some(session_id) = session_id {
                    provider_session_id = Some(session_id.clone());
                    send(
                        &events,
                        SessionEventKind::ProviderSessionLinked {
                            provider_session_id: session_id,
                            provider_turn_id,
                        },
                    )
                    .await;
                }
                if let Some(usage) = usage {
                    if let (Some(context_tokens), Some(context_window_tokens)) =
                        (usage.context_tokens, usage.context_window_tokens)
                    {
                        send(
                            &events,
                            SessionEventKind::ContextWindowUpdated {
                                context_tokens,
                                context_window_tokens,
                            },
                        )
                        .await;
                    }
                    send(
                        &events,
                        SessionEventKind::UsageUpdated {
                            provider_duration_ms: usage.duration_ms,
                            turn_id: Some(turn.message_id),
                            provider_context_reused: pool_invocation
                                .as_ref()
                                .map(|(_, prepared)| prepared.reused),
                            input_tokens: usage.input_tokens,
                            output_tokens: usage.output_tokens,
                            cached_input_tokens: usage.cached_input_tokens,
                            cache_creation_input_tokens: usage.cache_creation_input_tokens,
                            total_tokens: usage.total_tokens,
                            cost_microusd: usage.cost_microusd,
                            cost_basis: usage.cost_basis.to_string(),
                            cost_usd: None,
                            context_tokens: usage.context_tokens,
                            context_window_tokens: usage.context_window_tokens,
                        },
                    )
                    .await;
                }
                // Narration closes one assistant segment and allocates a new
                // message id for the final segment. Complete the current
                // segment independently of earlier narration; otherwise its
                // last in-progress snapshot survives the terminal boundary.
                let terminal_text = match terminal_assistant_text(
                    &final_output,
                    &text,
                    completed_segment,
                    interrupted.load(Ordering::Acquire),
                ) {
                    Ok(text) => text,
                    Err(error) => {
                        if let Some((registry, _)) = pool_invocation.as_ref() {
                            registry.mark(turn.session_id, turn.provider, false).await;
                        }
                        send(
                            &events,
                            SessionEventKind::Error {
                                message: error.to_string(),
                            },
                        )
                        .await;
                        return Err(error);
                    }
                };
                if let Some(terminal_text) = terminal_text {
                    text = terminal_text.clone();
                    send(
                        &events,
                        SessionEventKind::Message {
                            message_id: assistant_message_id,
                            actor: EventActor::Assistant,
                            text: terminal_text,
                            attachments: Vec::new(),
                            status: MessageStatus::Complete,
                            delivery: None,
                        },
                    )
                    .await;
                }
            }
            ChatStreamEvent::Failed { error } => {
                flush_pending_reasoning(&events, &mut pending_reasoning).await;
                if let Some((registry, _)) = pool_invocation.as_ref() {
                    registry.mark(turn.session_id, turn.provider, false).await;
                }
                let error = user_facing_provider_error(turn.provider, &error);
                send(
                    &events,
                    SessionEventKind::Error {
                        message: error.clone(),
                    },
                )
                .await;
                bail!("{error}");
            }
        }
    }
    if let Err(error) = require_provider_stream_terminal(terminal_seen) {
        if let Some((registry, _)) = pool_invocation.as_ref() {
            registry.mark(turn.session_id, turn.provider, false).await;
        }
        send(
            &events,
            SessionEventKind::Error {
                message: error.to_string(),
            },
        )
        .await;
        return Err(error);
    }
    if let Some((registry, _)) = pool_invocation.as_ref() {
        registry
            .mark(
                turn.session_id,
                turn.provider,
                subscription_pool_turn_is_healthy(
                    turn.provider,
                    interrupted.load(Ordering::Acquire),
                ),
            )
            .await;
    }
    send(
        &events,
        SessionEventKind::StatusChanged {
            status: SessionStatus::Ready,
            detail: None,
        },
    )
    .await;
    Ok(AgentTurnResult {
        provider_session_id,
        final_text: if final_output.is_empty() {
            text
        } else {
            final_output
        },
    })
}

pub(crate) fn live_output_interval(bytes: usize) -> Duration {
    match bytes {
        0..=16_384 => Duration::from_millis(40),
        16_385..=65_536 => Duration::from_millis(80),
        65_537..=262_144 => Duration::from_millis(160),
        _ => Duration::from_millis(300),
    }
}

async fn flush_pending_reasoning(events: &mpsc::Sender<SessionEventKind>, pending: &mut String) {
    if !pending.is_empty() {
        send(
            events,
            SessionEventKind::ReasoningDelta {
                text: std::mem::take(pending),
            },
        )
        .await;
    }
}

fn require_provider_stream_terminal(terminal_seen: bool) -> Result<()> {
    anyhow::ensure!(
        terminal_seen,
        "provider stream closed without a terminal Done or Failed event"
    );
    Ok(())
}

fn subscription_pool_turn_is_healthy(provider: CodingProvider, interrupted: bool) -> bool {
    !interrupted || provider == CodingProvider::Codex
}

fn normalize_reasoning_delta(accumulated: &mut String, incoming: &str) -> Option<String> {
    if incoming.is_empty() || incoming == accumulated {
        return None;
    }
    if incoming.starts_with(accumulated.as_str()) {
        let delta = incoming[accumulated.len()..].to_string();
        accumulated.clear();
        accumulated.push_str(incoming);
        return (!delta.is_empty()).then_some(delta);
    }
    if accumulated.starts_with(incoming) {
        return None;
    }
    // Some subscription/CLI versions switch between incremental chunks and
    // cumulative snapshots without preserving the exact byte prefix (for
    // example, a chunk may repeat the last line after a reconnect). Append
    // only the non-overlapping suffix so the durable live row cannot grow a
    // second copy of the previous thought.
    let overlap = longest_suffix_prefix_overlap(accumulated, incoming);
    let delta = &incoming[overlap..];
    if delta.is_empty() {
        return None;
    }
    accumulated.push_str(delta);
    Some(delta.to_string())
}

fn normalize_reasoning_delta_after_completion(
    accumulated: &mut String,
    last_completed: &mut Option<String>,
    incoming: &str,
) -> Option<String> {
    if accumulated.is_empty() && last_completed.as_deref() == Some(incoming) {
        last_completed.take();
        return None;
    }
    last_completed.take();
    normalize_reasoning_delta(accumulated, incoming)
}

fn longest_suffix_prefix_overlap(left: &str, right: &str) -> usize {
    if left.is_empty() || right.is_empty() {
        return 0;
    }
    let pattern = right.as_bytes();
    let mut prefix = vec![0; pattern.len()];
    for index in 1..pattern.len() {
        let mut matched = prefix[index - 1];
        while matched > 0 && pattern[index] != pattern[matched] {
            matched = prefix[matched - 1];
        }
        if pattern[index] == pattern[matched] {
            matched += 1;
        }
        prefix[index] = matched;
    }

    let mut tail_start = left.len().saturating_sub(pattern.len());
    while !left.is_char_boundary(tail_start) {
        tail_start += 1;
    }
    let tail = &left.as_bytes()[tail_start..];
    let mut matched = 0;
    for (index, byte) in tail.iter().enumerate() {
        while matched > 0 && *byte != pattern[matched] {
            matched = prefix[matched - 1];
        }
        if *byte == pattern[matched] {
            matched += 1;
        }
        if matched == pattern.len() && index + 1 < tail.len() {
            matched = prefix[matched - 1];
        }
    }
    debug_assert!(right.is_char_boundary(matched));
    debug_assert!(left.is_char_boundary(left.len() - matched));
    matched
}

fn terminal_assistant_text(
    final_output: &str,
    current_text: &str,
    completed_segment: bool,
    interrupted: bool,
) -> Result<Option<String>> {
    let text = if completed_segment {
        // `final_output` is the provider's aggregate answer. Narration has
        // already committed prior segments, so only finish the current
        // post-narration segment here; otherwise the aggregate is duplicated.
        current_text
    } else if final_output.trim().is_empty() {
        current_text
    } else {
        final_output
    };
    let Some(text) = without_action_intent_marker(text) else {
        return Ok(None);
    };
    if !text.trim().is_empty() && action_intent_is_streaming(text) {
        Ok(None)
    } else if text.trim().is_empty() {
        anyhow::ensure!(
            completed_segment || interrupted,
            "provider completed without a visible response (empty result)"
        );
        Ok(None)
    } else {
        Ok(Some(text.to_string()))
    }
}

fn provider_event_is_transient(kind: &str) -> bool {
    let method = kind.split_once(':').map_or(kind, |(method, _)| method);
    let event_name = method.rsplit('/').next().unwrap_or(method);
    event_name.eq_ignore_ascii_case("delta")
        || event_name.ends_with("Delta")
        || matches!(
            method,
            "thread/tokenUsage/updated"
                | "account/rateLimits/updated"
                | "turn/diff/updated"
                | "rawResponseItem/completed"
                | "rawResponse/completed"
                | "item/commandExecution/terminalInteraction"
                | "item/reasoning/summaryPartAdded"
        )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LiveContextUsage {
    total_tokens: u64,
    context_window_tokens: u64,
}

fn live_context_usage(kind: &str, payload: &serde_json::Value) -> Option<LiveContextUsage> {
    if kind == "claude.context_usage" {
        return Some(LiveContextUsage {
            total_tokens: payload.get("total_tokens")?.as_u64()?,
            context_window_tokens: payload.get("context_window_tokens")?.as_u64()?,
        });
    }
    if kind != "thread/tokenUsage/updated" {
        return None;
    }
    let token_usage = payload
        .get("tokenUsage")
        .or_else(|| payload.get("token_usage"))
        .or_else(|| payload.pointer("/params/tokenUsage"))
        .or_else(|| payload.pointer("/params/token_usage"))
        .unwrap_or(payload);
    let last = token_usage
        .get("last")
        .or_else(|| token_usage.get("total"))
        .unwrap_or(token_usage);
    Some(LiveContextUsage {
        total_tokens: last
            .get("totalTokens")
            .or_else(|| last.get("total_tokens"))
            .and_then(serde_json::Value::as_u64)?,
        context_window_tokens: token_usage
            .get("modelContextWindow")
            .or_else(|| token_usage.get("model_context_window"))
            .or_else(|| payload.get("modelContextWindow"))
            .or_else(|| payload.get("model_context_window"))
            .and_then(serde_json::Value::as_u64)?,
    })
}

fn user_facing_provider_error(provider: CodingProvider, error: &str) -> String {
    let normalized = error.to_ascii_lowercase();
    if provider == CodingProvider::Codex
        && (normalized.contains("refresh token was revoked")
            || normalized.contains("refresh_token_invalidated")
            || normalized.contains("token_expired")
            || normalized.contains("authentication token is expired")
            || normalized.contains("not logged in")
            || normalized.contains("authentication required")
            || normalized.contains("please log in")
            || normalized.contains("please sign in")
            || normalized.contains("401 unauthorized"))
    {
        return "Codex sign-in required. Run /login to reconnect, then retry your message."
            .to_string();
    }
    if provider == CodingProvider::Claude
        && (normalized.contains("not logged in")
            || normalized.contains("authentication required")
            || normalized.contains("authentication_error")
            || normalized.contains("invalid x-api-key")
            || normalized.contains("oauth token")
            || normalized.contains("please run /login")
            || normalized.contains("please sign in")
            || normalized.contains("401 unauthorized"))
    {
        return "Claude sign-in required. Run /login to reconnect, then retry your message."
            .to_string();
    }
    error.to_string()
}

fn map_controls(
    controls: Option<mpsc::Receiver<AgentTurnControl>>,
    interrupted: Arc<AtomicBool>,
) -> Option<mpsc::Receiver<ChatStreamControl>> {
    controls.map(|mut controls| {
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            while let Some(control) = controls.recv().await {
                let delivered = match control {
                    AgentTurnControl::Steer {
                        message_id,
                        text,
                        attachments,
                        admission,
                        ack,
                    } => {
                        match tx
                            .send(ChatStreamControl::Steer {
                                client_user_message_id: Some(message_id.to_string()),
                                text,
                                attachments,
                                admission,
                                ack,
                            })
                            .await
                        {
                            Ok(()) => true,
                            Err(error) => {
                                if let ChatStreamControl::Steer { ack, .. } = error.0 {
                                    let _ = ack.send(Err(
                                        "provider turn ended before the steer was delivered"
                                            .to_string(),
                                    ));
                                }
                                false
                            }
                        }
                    }
                    AgentTurnControl::Approval {
                        approval_id,
                        decision,
                    } => {
                        let decision = match decision {
                            crate::ApprovalDecision::AllowOnce => ChatApprovalDecision::ApproveOnce,
                            crate::ApprovalDecision::AllowSession => {
                                ChatApprovalDecision::ApproveSession
                            }
                            crate::ApprovalDecision::Deny => ChatApprovalDecision::Reject,
                        };
                        tx.send(ChatStreamControl::Approval {
                            approval_id,
                            decision,
                        })
                        .await
                        .is_ok()
                    }
                    AgentTurnControl::ProviderInteractionResponse {
                        interaction_id,
                        response,
                    } => tx
                        .send(ChatStreamControl::ProviderInteractionResponse {
                            interaction_id,
                            response,
                        })
                        .await
                        .is_ok(),
                    AgentTurnControl::Interrupt => {
                        interrupted.store(true, Ordering::Release);
                        tx.send(ChatStreamControl::Interrupt).await.is_ok()
                    }
                };
                if !delivered {
                    break;
                }
            }
        });
        rx
    })
}

fn local_permission(permission: PermissionMode) -> LocalAgentPermission {
    match permission {
        PermissionMode::FullAccess => LocalAgentPermission::FullAccess,
        PermissionMode::Auto => LocalAgentPermission::Auto,
        PermissionMode::Manual => LocalAgentPermission::Manual,
    }
}

async fn send(events: &mpsc::Sender<SessionEventKind>, event: SessionEventKind) {
    events.send(event).await.ok();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_intent_markers_are_exact_and_bounded() {
        assert_eq!(
            action_intent_label("[[BORG_ACTION:plan update]]"),
            Some("plan update")
        );
        assert_eq!(action_intent_label("before [[BORG_ACTION:edit]]"), None);
        assert_eq!(action_intent_label("[[BORG_ACTION:edit!]]"), None);
        assert_eq!(
            action_intent_label("[[BORG_ACTION:command]][[BORG_ACTION:file read]]"),
            Some("file read")
        );
        assert!(action_intent_is_streaming("[["));
        assert!(action_intent_is_streaming("[[BORG_ACTION:edit"));
        assert!(action_intent_is_streaming("[[BORG_ACTION:edit]]"));
        assert!(action_intent_is_streaming(
            "[[BORG_ACTION:command]][[BORG_ACTION:file read]]"
        ));
        assert!(action_intent_is_streaming(
            "[[BORG_ACTION:command]][[BORG_ACTION:file"
        ));
        assert!(!action_intent_is_streaming("ordinary narration"));
        assert_eq!(without_action_intent_marker("[[BORG_ACTION[["), None);
        assert_eq!(
            without_action_intent_marker("[[BORG_ACTION:command]]\nvisible result"),
            Some("visible result")
        );
        assert!(provider_native_agent_tool("subAgentActivity"));
        assert!(provider_native_agent_tool("collabAgentToolCall"));
        assert!(!provider_native_agent_tool("mcp__borg_agent__spawn_agent"));
    }

    fn test_server(name: &str) -> borg_provider::mcp::ExternalMcpServer {
        borg_provider::mcp::ExternalMcpServer {
            name: name.to_string(),
            command: "server".to_string(),
            args: Vec::new(),
            env: std::collections::BTreeMap::new(),
            allowed_tools: Vec::new(),
        }
    }

    #[test]
    fn runtime_extension_swap_is_atomic_at_turn_snapshot_boundary() {
        let executor = LocalAgentTurnExecutor::default()
            .with_external_mcp_servers(vec![test_server("old")])
            .with_extension_skill_roots(vec![PathBuf::from("old-skills")]);
        let in_flight_snapshot = executor.runtime_extensions.read().unwrap().clone();

        executor.replace_runtime_extensions(
            vec![test_server("new")],
            vec![PathBuf::from("new-skills")],
            vec![BluWorkflowDefinition {
                extension_id: "new".to_string(),
                name: "workflow".to_string(),
                description: None,
                runtime: WorkflowRuntime::Blu,
                source: "borg_emit(\"call\", \"kind\", \"{}\")".to_string(),
                entrypoint: PathBuf::from("new.blu"),
                working_directory: PathBuf::from("."),
                command: None,
                args: Vec::new(),
            }],
        );

        assert_eq!(in_flight_snapshot.external_mcp_servers[0].name, "old");
        assert_eq!(
            in_flight_snapshot.skill_roots,
            [PathBuf::from("old-skills")]
        );
        let next_turn = executor.runtime_extensions.read().unwrap();
        assert_eq!(next_turn.external_mcp_servers[0].name, "new");
        assert_eq!(next_turn.skill_roots, [PathBuf::from("new-skills")]);
        assert_eq!(next_turn.workflows[0].name, "workflow");
    }

    #[tokio::test]
    async fn runtime_extension_loader_refreshes_without_restarting_the_executor() {
        let executor = LocalAgentTurnExecutor::default()
            .with_external_mcp_servers(vec![test_server("old")])
            .with_extension_skill_roots(vec![PathBuf::from("old-skills")])
            .with_runtime_extension_loader(|| {
                Ok((
                    vec![test_server("reloaded")],
                    vec![PathBuf::from("reloaded-skills")],
                    vec![BluWorkflowDefinition {
                        extension_id: "reloaded".to_string(),
                        name: "workflow".to_string(),
                        description: None,
                        runtime: WorkflowRuntime::Blu,
                        source: "borg_emit(\"call\", \"kind\", \"{}\")".to_string(),
                        entrypoint: PathBuf::from("reloaded.blu"),
                        working_directory: PathBuf::from("."),
                        command: None,
                        args: Vec::new(),
                    }],
                    crate::ExtensionApiSnapshot::default(),
                ))
            });

        executor.refresh_runtime_extensions().await;

        let snapshot = executor.runtime_extensions.read().unwrap();
        assert_eq!(snapshot.external_mcp_servers[0].name, "reloaded");
        assert_eq!(snapshot.skill_roots, [PathBuf::from("reloaded-skills")]);
        assert_eq!(snapshot.workflows[0].extension_id, "reloaded");
    }

    #[tokio::test]
    async fn subscription_pool_replays_after_failure_and_appends_only_when_healthy() {
        let registry = SubscriptionPoolRegistry::default();
        let session_id = Uuid::new_v4();
        let first = registry
            .prepare(
                session_id,
                SubscriptionTurnInput {
                    context_generation: 0,
                    provider: CodingProvider::Claude,
                    provider_session_id: None,
                    provider_fork_turn_id: None,
                    prompt: "canonical history + first".to_string(),
                    prompt_delta: "<borg-message>first</borg-message>".to_string(),
                    lifecycle_key: "stable-config".to_string(),
                },
            )
            .await;
        assert_eq!(first.prompt, "canonical history + first");
        registry
            .mark(session_id, CodingProvider::Claude, true)
            .await;

        let appended = registry
            .prepare(
                session_id,
                SubscriptionTurnInput {
                    context_generation: 0,
                    provider: CodingProvider::Claude,
                    provider_session_id: None,
                    provider_fork_turn_id: None,
                    prompt: "canonical history + first + second".to_string(),
                    prompt_delta: "<borg-message>second</borg-message>".to_string(),
                    lifecycle_key: "stable-config".to_string(),
                },
            )
            .await;
        assert_eq!(appended.prompt, "<borg-message>second</borg-message>");

        registry
            .mark(session_id, CodingProvider::Claude, false)
            .await;
        let replay = registry
            .prepare(
                session_id,
                SubscriptionTurnInput {
                    context_generation: 0,
                    provider: CodingProvider::Claude,
                    provider_session_id: None,
                    provider_fork_turn_id: None,
                    prompt: "canonical history + first + second + third".to_string(),
                    prompt_delta: "<borg-message>third</borg-message>".to_string(),
                    lifecycle_key: "stable-config".to_string(),
                },
            )
            .await;
        assert_eq!(replay.prompt, "canonical history + first + second + third");
        assert_ne!(appended.lifecycle_key, replay.lifecycle_key);
    }

    #[tokio::test]
    async fn codex_pool_recovers_a_durable_checkpoint_with_only_the_new_delta() {
        let registry = SubscriptionPoolRegistry::default();
        let session_id = Uuid::new_v4();
        let prepared = registry
            .prepare(
                session_id,
                SubscriptionTurnInput {
                    context_generation: 4,
                    provider: CodingProvider::Codex,
                    provider_session_id: Some("durable-codex-thread".to_string()),
                    provider_fork_turn_id: Some("completed-codex-turn".to_string()),
                    prompt: "large canonical replay + next".to_string(),
                    prompt_delta: "<borg-message>next</borg-message>".to_string(),
                    lifecycle_key: "stable-config".to_string(),
                },
            )
            .await;

        assert_eq!(prepared.prompt, "<borg-message>next</borg-message>");
        assert_eq!(
            prepared.resume_session_id.as_deref(),
            Some("durable-codex-thread")
        );
        assert_eq!(
            prepared.fork_turn_id.as_deref(),
            Some("completed-codex-turn")
        );
        assert_eq!(
            prepared.resume_unavailable_prompt.as_deref(),
            Some("large canonical replay + next")
        );
        assert!(prepared.reused);

        let lazy = registry
            .prepare(
                Uuid::new_v4(),
                SubscriptionTurnInput {
                    context_generation: 4,
                    provider: CodingProvider::Codex,
                    provider_session_id: Some("durable-codex-thread".to_string()),
                    provider_fork_turn_id: None,
                    prompt: "<borg-message>next</borg-message>".to_string(),
                    prompt_delta: "<borg-message>next</borg-message>".to_string(),
                    lifecycle_key: "stable-config".to_string(),
                },
            )
            .await;
        assert!(lazy.resume_unavailable_prompt.is_none());
    }

    #[tokio::test]
    async fn pending_steer_acknowledgement_does_not_block_interrupt() {
        let (control_tx, control_rx) = mpsc::channel(4);
        let interrupted = Arc::new(AtomicBool::new(false));
        let mut provider_controls =
            map_controls(Some(control_rx), Arc::clone(&interrupted)).expect("mapped controls");
        let (ack, acknowledgement) = tokio::sync::oneshot::channel();
        let admission = SteerAdmission::pending();

        control_tx
            .send(AgentTurnControl::Steer {
                message_id: Uuid::new_v4(),
                text: "additional context".to_string(),
                attachments: Vec::new(),
                admission,
                ack,
            })
            .await
            .unwrap();
        let provider_ack = match provider_controls.recv().await {
            Some(ChatStreamControl::Steer { admission, ack, .. }) => {
                assert!(admission.accept());
                ack
            }
            other => panic!("expected provider steer, got {other:?}"),
        };

        control_tx.send(AgentTurnControl::Interrupt).await.unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), provider_controls.recv()).await,
            Ok(Some(ChatStreamControl::Interrupt))
        ));
        assert!(interrupted.load(Ordering::Acquire));

        provider_ack.send(Ok(())).unwrap();
        assert!(matches!(acknowledgement.await, Ok(Ok(()))));
    }

    #[test]
    fn transient_codex_telemetry_does_not_enter_the_durable_session_stream() {
        for kind in [
            "item/agentMessage/delta",
            "item/commandExecution/outputDelta",
            "item/reasoning/summaryTextDelta",
            "thread/tokenUsage/updated",
            "account/rateLimits/updated",
            "turn/diff/updated",
        ] {
            assert!(provider_event_is_transient(kind), "{kind}");
        }
        assert!(!provider_event_is_transient(
            "item/completed:commandExecution"
        ));
        assert!(!provider_event_is_transient(
            "item/started:contextCompaction"
        ));
        assert!(!provider_event_is_transient(
            "item/completed:contextCompaction"
        ));
    }

    #[test]
    fn codex_context_usage_is_available_before_turn_completion() {
        let usage = live_context_usage(
            "thread/tokenUsage/updated",
            &serde_json::json!({
                "last": {
                    "inputTokens": 40_000,
                    "cachedInputTokens": 1_000,
                    "outputTokens": 2_000,
                    "totalTokens": 43_000
                },
                "model_context_window": 258_400
            }),
        )
        .expect("live usage");

        assert_eq!(usage.total_tokens, 43_000);
        assert_eq!(usage.context_window_tokens, 258_400);
    }

    #[test]
    fn codex_nested_context_usage_is_available_before_turn_completion() {
        let usage = live_context_usage(
            "thread/tokenUsage/updated",
            &serde_json::json!({
                "tokenUsage": {
                    "last": {
                        "inputTokens": 12_000,
                        "cachedInputTokens": 210_000,
                        "outputTokens": 800,
                        "totalTokens": 222_800
                    },
                    "modelContextWindow": 258_400
                }
            }),
        )
        .expect("nested live usage");

        assert_eq!(usage.total_tokens, 222_800);
        assert_eq!(usage.context_window_tokens, 258_400);
    }

    #[test]
    fn claude_context_usage_is_available_before_turn_completion() {
        let usage = live_context_usage(
            "claude.context_usage",
            &serde_json::json!({
                "total_tokens": 91_000,
                "context_window_tokens": 200_000,
            }),
        )
        .expect("Claude context usage");

        assert_eq!(usage.total_tokens, 91_000);
        assert_eq!(usage.context_window_tokens, 200_000);
    }

    #[test]
    fn codex_auth_failures_have_one_actionable_terminal_message() {
        let message = user_facing_provider_error(
            CodingProvider::Codex,
            "401 Unauthorized: refresh_token_invalidated",
        );
        assert_eq!(
            message,
            "Codex sign-in required. Run /login to reconnect, then retry your message."
        );
        assert!(!message.contains("401"));
    }

    #[test]
    fn claude_auth_failures_have_one_actionable_terminal_message() {
        let message = user_facing_provider_error(
            CodingProvider::Claude,
            "claude SDK error: authentication_error: invalid x-api-key",
        );
        assert_eq!(
            message,
            "Claude sign-in required. Run /login to reconnect, then retry your message."
        );
        assert!(!message.contains("x-api-key"));
    }

    #[test]
    fn provider_stream_cannot_succeed_without_a_terminal_event() {
        assert!(require_provider_stream_terminal(true).is_ok());
        assert_eq!(
            require_provider_stream_terminal(false)
                .unwrap_err()
                .to_string(),
            "provider stream closed without a terminal Done or Failed event"
        );
    }

    #[test]
    fn acknowledged_codex_interrupt_keeps_the_native_thread_reusable() {
        assert!(subscription_pool_turn_is_healthy(
            CodingProvider::Codex,
            true
        ));
        assert!(!subscription_pool_turn_is_healthy(
            CodingProvider::Claude,
            true
        ));
        assert!(subscription_pool_turn_is_healthy(
            CodingProvider::Claude,
            false
        ));
    }

    #[test]
    fn cumulative_reasoning_snapshots_are_not_appended_twice() {
        let mut accumulated = String::new();
        assert_eq!(
            normalize_reasoning_delta(&mut accumulated, "Considering code modifications"),
            Some("Considering code modifications".to_string())
        );
        assert_eq!(
            normalize_reasoning_delta(
                &mut accumulated,
                "Considering code modifications\nI’m checking the repository"
            ),
            Some("\nI’m checking the repository".to_string())
        );
        assert_eq!(
            normalize_reasoning_delta(
                &mut accumulated,
                "Considering code modifications\nI’m checking the repository"
            ),
            None
        );
        assert_eq!(
            accumulated,
            "Considering code modifications\nI’m checking the repository"
        );
    }

    #[test]
    fn reasoning_overlap_keeps_the_longest_utf8_boundary_match() {
        let mut samples = Vec::new();
        for len in 0..=7 {
            for bits in 0..(1_usize << len) {
                samples.push(
                    (0..len)
                        .map(|index| if bits & (1 << index) == 0 { 'a' } else { 'b' })
                        .collect::<String>(),
                );
            }
        }
        samples.extend(["🦀step".to_string(), "step 🦀".to_string()]);
        for left in &samples {
            for right in &samples {
                let expected = (1..=left.len().min(right.len()))
                    .rev()
                    .find(|overlap| {
                        left.is_char_boundary(left.len() - overlap)
                            && right.is_char_boundary(*overlap)
                            && left.as_bytes()[left.len() - overlap..]
                                == right.as_bytes()[..*overlap]
                    })
                    .unwrap_or(0);
                assert_eq!(
                    longest_suffix_prefix_overlap(left, right),
                    expected,
                    "left={left:?}, right={right:?}"
                );
            }
        }

        let mut accumulated = "first step".to_string();
        assert_eq!(
            normalize_reasoning_delta(&mut accumulated, "step two"),
            Some(" two".to_string())
        );
        assert_eq!(accumulated, "first step two");
    }

    #[test]
    #[ignore = "manual pathological reasoning-overlap profile"]
    fn reasoning_overlap_profile() {
        let bytes = 256 * 1024;
        let mut left = "a".repeat(bytes - 1);
        left.push('b');
        let right = "a".repeat(bytes);
        let started = std::time::Instant::now();
        let overlap = std::hint::black_box(longest_suffix_prefix_overlap(&left, &right));
        let elapsed = started.elapsed();
        eprintln!("256 KiB pathological reasoning overlap: {elapsed:?}");
        assert_eq!(overlap, 0);
        assert!(
            elapsed < Duration::from_millis(50),
            "reasoning overlap exceeded 50 ms: {elapsed:?}"
        );
    }

    #[test]
    fn immediate_completed_reasoning_replay_is_dropped() {
        let mut accumulated = String::new();
        let mut last_completed = None;
        let thought = "I’m checking the repository";

        assert_eq!(
            normalize_reasoning_delta_after_completion(
                &mut accumulated,
                &mut last_completed,
                thought,
            ),
            Some(thought.to_string())
        );
        last_completed = Some(accumulated.clone());
        accumulated.clear();

        assert_eq!(
            normalize_reasoning_delta_after_completion(
                &mut accumulated,
                &mut last_completed,
                thought,
            ),
            None
        );
        assert!(last_completed.is_none());
    }

    #[test]
    fn different_reasoning_after_completion_is_preserved() {
        let mut accumulated = String::new();
        let mut last_completed = Some("first thought".to_string());

        assert_eq!(
            normalize_reasoning_delta_after_completion(
                &mut accumulated,
                &mut last_completed,
                "second thought",
            ),
            Some("second thought".to_string())
        );
    }

    #[test]
    fn terminal_result_completes_the_current_segment_after_narration() {
        assert_eq!(
            terminal_assistant_text("aggregate response", "partial response", true, false).unwrap(),
            Some("partial response".to_string())
        );
        assert_eq!(
            terminal_assistant_text("aggregate response", "", true, false).unwrap(),
            None
        );
        assert_eq!(
            terminal_assistant_text("final response", "partial response", false, false).unwrap(),
            Some("final response".to_string())
        );
        assert_eq!(
            terminal_assistant_text("", "partial response", false, false).unwrap(),
            Some("partial response".to_string())
        );
        assert_eq!(
            terminal_assistant_text("", "", false, false)
                .unwrap_err()
                .to_string(),
            "provider completed without a visible response (empty result)"
        );
        assert_eq!(terminal_assistant_text("", "", false, true).unwrap(), None);
        assert_eq!(
            terminal_assistant_text("[[BORG_ACTION:command]]", "", false, true).unwrap(),
            None
        );
        assert_eq!(
            terminal_assistant_text("[[BORG_ACTION:command", "", false, true).unwrap(),
            None
        );
        assert_eq!(
            terminal_assistant_text("[[BORG_ACTION[[", "", false, true).unwrap(),
            None
        );
        assert_eq!(
            terminal_assistant_text("[[BORG_ACTION:command]]\nvisible result", "", false, false)
                .unwrap(),
            Some("visible result".to_string())
        );
    }
}
