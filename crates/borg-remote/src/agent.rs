use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use borg_provider::provider::{
    ChatApprovalDecision, ChatStreamControl, ChatStreamEvent, ChatStreamRequest, ClaudeAgentsPool,
    CodexAppServerPool, LocalAgentPermission, run_claude_chat_stream_with_control,
    run_claude_local_chat_stream, run_codex_chat_stream_with_control, run_codex_local_chat_stream,
    run_opencode_local_chat_stream, run_pooled_claude_local_chat_stream,
    run_pooled_codex_local_chat_stream,
};
use borg_provider::{CostBasis, ProviderCallUsage, ProviderChannel};
use serde_json::Value;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    CodingProvider, EventActor, MessageStatus, PermissionMode, ResponseLanguage, SessionEventKind,
    SessionStatus, native_harness::NativeHarness,
};

pub(crate) const CODING_SYSTEM_PROMPT: &str = "\
You are Borg, a practical agent working in the user's local project. \
Inspect before changing, keep solutions small, preserve user work, explain consequential actions, \
and continue until the requested outcome is implemented and verified. \
The Borg CLI source is https://github.com/borg-ml/cli; when diagnosing Borg CLI behavior and the \
source is not already available, inspect or clone that public repository as needed. \
Write simple mathematical notation as readable Unicode or plain text. For complex notation, use \
valid Markdown math delimiters (`$...$` or `$$...$$`); never emit bare TeX commands in prose. \
Use the tools from the borg_agent MCP server for durable goals, plans, and subagents. \
For a substantial multi-step user request, call get_goal first, create a concise goal when none \
exists, then create the plan. Before updating an existing plan, call get_plan and reuse its exact \
item UUIDs; omit IDs for new items. \
Use its LSP tools for diagnostics and semantic code navigation when the workspace language is supported. \
After editing supported source files, run LSP diagnostics before finishing and repair errors caused by the edit. \
Do not use a provider-native spawn or collaboration tool because those children are not part of \
the Borg session tree and cannot be controlled from Borg Remote. \
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
remain the sole voice that answers the user.";

#[derive(Clone)]
pub struct AgentTurn {
    pub session_id: Uuid,
    pub message_id: Uuid,
    pub provider: CodingProvider,
    pub provider_session_id: Option<String>,
    pub cwd: PathBuf,
    pub prompt: String,
    pub attachments: Vec<PathBuf>,
    pub output_schema: Option<Value>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub fast: Option<bool>,
    pub response_language: crate::ResponseLanguage,
    pub permission_mode: PermissionMode,
    /// Provider-neutral conversation reconstructed from the durable journal.
    pub conversation: Vec<borg_provider::provider::ModelMessage>,
    /// One local MCP transport for Borg-owned goal, plan, and subagent tools.
    pub agent_mcp_server: borg_provider::mcp::ExternalMcpServer,
    /// Direct in-process access to the same Borg-owned tools. Native harnesses
    /// use this instead of round-tripping through their MCP transport.
    pub agent_tools: crate::AgentToolDispatcher,
    /// Product/user MCP integrations available to a Borg-native turn.
    pub external_mcp_servers: Vec<borg_provider::mcp::ExternalMcpServer>,
    /// Trusted extension-owned skill roots supplied by the launch contract.
    pub extension_skill_roots: Vec<PathBuf>,
    /// Trusted runtime context appended to the provider system prompt.
    pub system_prompt_appendix: String,
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

    /// Run an isolated, one-shot consultation without attaching it to the
    /// main session's provider conversation or exposing the main session's
    /// tools. Providers that cannot offer this path report a normal tool error.
    async fn consult(&self, _request: ConsultationRequest) -> Result<ConsultationResult> {
        anyhow::bail!("model consultation is not supported by this executor")
    }

    async fn compact(
        &self,
        _provider: CodingProvider,
        _provider_session_id: &str,
    ) -> Result<Option<ProviderCallUsage>> {
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
#[derive(Clone, Default)]
pub struct LocalAgentTurnExecutor {
    codex_pool: CodexAppServerPool,
    claude_pool: ClaudeAgentsPool,
    claude_sessions: Arc<Mutex<HashMap<String, ClaudeSessionSnapshot>>>,
    native_harness: NativeHarness,
    runtime_extensions: Arc<RwLock<RuntimeExtensions>>,
    runtime_extension_loader: Option<RuntimeExtensionLoader>,
}

type RuntimeExtensionLoader = Arc<
    dyn Fn() -> Result<(Vec<borg_provider::mcp::ExternalMcpServer>, Vec<PathBuf>)> + Send + Sync,
>;

#[derive(Clone, Default)]
struct RuntimeExtensions {
    external_mcp_servers: Vec<borg_provider::mcp::ExternalMcpServer>,
    skill_roots: Vec<PathBuf>,
}

#[derive(Clone)]
struct ClaudeSessionSnapshot {
    request: ChatStreamRequest,
    permission: LocalAgentPermission,
    pool: Option<ClaudeAgentsPool>,
}

#[derive(Debug, Clone, Default)]
pub struct LocalAgentSettings {
    pub approval_reviewer_model: Option<String>,
    pub approval_reviewer_effort: Option<String>,
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

    /// Add trusted live skill roots to every subsequent turn. Native runtimes
    /// rescan them when the turn starts, so no provider/session restart is
    /// required.
    pub fn with_extension_skill_roots(self, roots: Vec<PathBuf>) -> Self {
        self.runtime_extensions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .skill_roots = roots;
        self
    }

    /// Atomically replace the live extension snapshot. An in-flight turn keeps
    /// the immutable snapshot it started with; the next turn observes the new
    /// MCP catalog and skill roots.
    pub fn replace_runtime_extensions(
        &self,
        servers: Vec<borg_provider::mcp::ExternalMcpServer>,
        skill_roots: Vec<PathBuf>,
    ) {
        *self
            .runtime_extensions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = RuntimeExtensions {
            external_mcp_servers: servers,
            skill_roots,
        };
    }

    /// Refresh the extension snapshot at each turn boundary. Failed reloads
    /// retain the last-known-good snapshot and are retried on the next turn.
    pub fn with_runtime_extension_loader<F>(mut self, loader: F) -> Self
    where
        F: Fn() -> Result<(Vec<borg_provider::mcp::ExternalMcpServer>, Vec<PathBuf>)>
            + Send
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
            Ok(Ok((servers, roots))) => self.replace_runtime_extensions(servers, roots),
            Ok(Err(error)) => {
                tracing::warn!(%error, "kept last-known-good runtime extension snapshot");
            }
            Err(error) => {
                tracing::warn!(%error, "runtime extension loader stopped unexpectedly");
            }
        }
    }

    pub fn prewarm(&self, provider: CodingProvider) {
        if provider == CodingProvider::Codex {
            self.codex_pool.prewarm_local(true);
        }
    }
}

#[async_trait::async_trait]
impl AgentTurnExecutor for LocalAgentTurnExecutor {
    async fn execute(
        &self,
        mut turn: AgentTurn,
        events: mpsc::Sender<SessionEventKind>,
        controls: Option<mpsc::Receiver<AgentTurnControl>>,
    ) -> Result<AgentTurnResult> {
        self.refresh_runtime_extensions().await;
        let runtime_extensions = self
            .runtime_extensions
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        turn.external_mcp_servers
            .extend(runtime_extensions.external_mcp_servers);
        turn.extension_skill_roots
            .extend(runtime_extensions.skill_roots);
        if turn.provider.uses_native_harness() {
            return self.native_harness.run(turn, events, controls).await;
        }
        if !turn.extension_skill_roots.is_empty() {
            turn.system_prompt_appendix.push_str(
                &crate::native_context::extension_skill_prompt_appendix(
                    turn.extension_skill_roots.clone(),
                )
                .await?,
            );
        }
        events
            .send(SessionEventKind::StatusChanged {
                status: SessionStatus::Running,
                detail: None,
            })
            .await
            .ok();
        run_borg_provider_turn(
            turn,
            events,
            controls,
            BorgProviderTurnRuntime {
                request_template: None,
                local: true,
                codex_pool: Some(self.codex_pool.clone()),
                claude_pool: Some(self.claude_pool.clone()),
                claude_sessions: Some(self.claude_sessions.clone()),
            },
            true,
        )
        .await
    }

    async fn consult(&self, request: ConsultationRequest) -> Result<ConsultationResult> {
        if request.provider.uses_native_harness() {
            let (final_text, usage) = self
                .native_harness
                .consult(
                    request.provider,
                    request
                        .model
                        .as_deref()
                        .context("native consultation requires an explicit model")?,
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

        let (final_text, usage) = run_local_consultation_stream(&request).await?;
        Ok(ConsultationResult {
            provider: request.provider,
            model: request.model,
            final_text,
            usage,
        })
    }

    async fn compact(
        &self,
        provider: CodingProvider,
        provider_session_id: &str,
    ) -> Result<Option<ProviderCallUsage>> {
        match provider {
            CodingProvider::Codex => self.codex_pool.compact(provider_session_id).map(|()| None),
            CodingProvider::Claude => {
                let snapshot = self
                    .claude_sessions
                    .lock()
                    .expect("Claude session registry lock poisoned")
                    .get(provider_session_id)
                    .cloned()
                    .context("Claude context is not available until the current turn finishes")?;
                compact_claude_session(snapshot, provider_session_id)
                    .await
                    .map(Some)
            }
            _ => bail!("manual context compaction is not supported by this provider"),
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
            "cross-provider context compaction is not supported by {:?}",
            turn.provider
        );

        let mut turn = turn;
        // The preparation turn must never mutate the workspace. The prompt
        // also asks the model not to use tools, while this permission mode
        // makes any accidental tool request require an approval that the
        // private collector below will deny.
        turn.permission_mode = PermissionMode::Manual;
        let (events, mut event_rx) = mpsc::channel(128);
        let (control_tx, control_rx) = mpsc::channel(16);
        let usage = Arc::new(Mutex::new(ProviderCallUsage::default()));
        let usage_sink = Arc::clone(&usage);
        let collector = tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                if let SessionEventKind::ApprovalRequested { approval_id, .. } = &event {
                    let _ = control_tx
                        .send(AgentTurnControl::Approval {
                            approval_id: approval_id.clone(),
                            decision: crate::ApprovalDecision::Deny,
                        })
                        .await;
                }
                if let Some(observed) = usage_from_session_event(&event) {
                    *usage_sink
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = observed;
                }
            }
        });

        let provider = turn.provider;
        let result = run_borg_provider_turn(
            turn,
            events.clone(),
            Some(control_rx),
            BorgProviderTurnRuntime {
                request_template: None,
                local: true,
                codex_pool: Some(self.codex_pool.clone()),
                claude_pool: Some(self.claude_pool.clone()),
                claude_sessions: Some(self.claude_sessions.clone()),
            },
            false,
        )
        .await;
        drop(events);
        collector
            .await
            .context("compaction event collector stopped unexpectedly")?;
        let result = result?;
        let provider_session_id = result
            .provider_session_id
            .context("provider compaction preparation did not create a conversation")?;

        let compaction_usage = match provider {
            CodingProvider::Codex => {
                self.codex_pool.compact(&provider_session_id)?;
                None
            }
            CodingProvider::Claude => Some(
                compact_claude_session(
                    self.claude_session_snapshot(&provider_session_id)?,
                    &provider_session_id,
                )
                .await?,
            ),
            _ => unreachable!("provider was validated above"),
        };

        anyhow::ensure!(
            !result.final_text.trim().is_empty(),
            "provider compaction preparation returned an empty summary"
        );
        let mut usage = usage
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(compaction_usage) = compaction_usage.as_ref() {
            add_provider_usage(&mut usage, compaction_usage);
        }
        Ok(AgentCompaction {
            summary: result.final_text,
            usage,
            provider_session_id: Some(provider_session_id),
        })
    }

    async fn stop_session(&self, session_id: Uuid) -> Result<()> {
        let pool = self.codex_pool.clone();
        let owner_session_id = session_id.to_string();
        tokio::task::spawn_blocking(move || pool.stop_owner(&owner_session_id))
            .await
            .context("Codex provider cleanup worker panicked")??;
        self.native_harness.stop_session(session_id).await;
        Ok(())
    }
}

impl LocalAgentTurnExecutor {
    fn claude_session_snapshot(&self, provider_session_id: &str) -> Result<ClaudeSessionSnapshot> {
        self.claude_sessions
            .lock()
            .expect("Claude session registry lock poisoned")
            .get(provider_session_id)
            .cloned()
            .context("Claude context is not available after compaction preparation")
    }
}

const CONSULTATION_SYSTEM_PROMPT: &str = "You are a second-opinion consultant in a Borg multi-model workflow. Analyze the complete briefing supplied by the caller, identify important omissions or disagreements, and return a self-contained response that the main agent can reconcile. Do not modify files, call tools, or ask the user for clarification.";

async fn run_local_consultation_stream(
    request: &ConsultationRequest,
) -> Result<(String, ProviderCallUsage)> {
    anyhow::ensure!(
        matches!(
            request.provider,
            CodingProvider::Codex | CodingProvider::Claude | CodingProvider::OpenCode
        ),
        "unsupported local consultation provider: {:?}",
        request.provider
    );

    let mut system_prompt = CONSULTATION_SYSTEM_PROMPT.to_string();
    if let Some(instruction) = request.response_language.instruction() {
        system_prompt.push_str("\n\n");
        system_prompt.push_str(instruction);
    }
    let provider_request = ChatStreamRequest {
        prompt: request.prompt.clone(),
        owner_session_id: Some(format!("consultation:{}", request.owner_session_id)),
        client_user_message_id: Some(request.message_id.to_string()),
        attachments: Vec::new(),
        model: request.model.clone(),
        effort: request.effort.clone(),
        fast: false,
        system_prompt,
        output_schema: None,
        mcp_owner_id: None,
        mcp_allowed_scopes: Vec::new(),
        mcp_user_id: None,
        mcp_external_servers: Vec::new(),
        mcp_api_token: None,
        provider_auth: None,
        git_credentials: Vec::new(),
        working_directory: Some(request.cwd.clone()),
        session_id: None,
        provider_channel: ProviderChannel::Direct,
        persist_session: Some(false),
        web_search_allowed: false,
        resume_unavailable_prompt: None,
    };

    let (control_tx, control_rx) = mpsc::channel(16);
    let mut stream = match request.provider {
        CodingProvider::Codex => run_codex_local_chat_stream(
            provider_request,
            Some(control_rx),
            LocalAgentPermission::Manual,
        ),
        CodingProvider::Claude => run_claude_local_chat_stream(
            provider_request,
            Some(control_rx),
            LocalAgentPermission::Manual,
        ),
        CodingProvider::OpenCode => {
            drop(control_rx);
            run_opencode_local_chat_stream(provider_request, LocalAgentPermission::Manual)
        }
        CodingProvider::Kimi | CodingProvider::OpenRouter | CodingProvider::OpenAiCompatible => {
            unreachable!("native provider handled above")
        }
    };

    let mut text = String::new();
    let mut final_text = None;
    let mut usage = ProviderCallUsage::default();
    while let Some(event) = stream.recv().await {
        match event {
            ChatStreamEvent::Delta(delta) => text.push_str(&delta),
            ChatStreamEvent::Narration { text: narration } => {
                if text.is_empty() {
                    text = narration;
                }
            }
            ChatStreamEvent::ApprovalRequested { approval_id, .. } => {
                control_tx
                    .send(ChatStreamControl::Approval {
                        approval_id,
                        decision: ChatApprovalDecision::Reject,
                    })
                    .await
                    .ok();
            }
            ChatStreamEvent::ToolCall { name, .. } => {
                bail!("consultant attempted to use tool `{name}`")
            }
            ChatStreamEvent::ProviderInteractionRequested { title, .. } => {
                bail!("consultant requested interactive input: {title}")
            }
            ChatStreamEvent::Done {
                final_text: result,
                usage: result_usage,
                ..
            } => {
                final_text = Some(result);
                if let Some(result_usage) = result_usage {
                    usage = result_usage;
                }
                break;
            }
            ChatStreamEvent::Failed { error } => bail!("{error}"),
            ChatStreamEvent::ProviderEvent { .. }
            | ChatStreamEvent::ReasoningDelta(_)
            | ChatStreamEvent::Phase { .. }
            | ChatStreamEvent::ToolResult { .. } => {}
        }
    }
    let final_text = final_text.unwrap_or(text);
    anyhow::ensure!(
        !final_text.trim().is_empty(),
        "consultant returned an empty response"
    );
    Ok((final_text, usage))
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
        CodingProvider::Codex | CodingProvider::Claude | CodingProvider::OpenCode => {
            run_borg_provider_turn(
                turn,
                events,
                controls,
                BorgProviderTurnRuntime {
                    request_template: None,
                    local: true,
                    codex_pool: None,
                    claude_pool: None,
                    claude_sessions: None,
                },
                true,
            )
            .await
        }
        CodingProvider::Kimi | CodingProvider::OpenRouter | CodingProvider::OpenAiCompatible => {
            NativeHarness::default().run(turn, events, controls).await
        }
    }
}

struct BorgProviderTurnRuntime {
    request_template: Option<ChatStreamRequest>,
    local: bool,
    codex_pool: Option<CodexAppServerPool>,
    claude_pool: Option<ClaudeAgentsPool>,
    claude_sessions: Option<Arc<Mutex<HashMap<String, ClaudeSessionSnapshot>>>>,
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
        codex_pool,
        claude_pool,
        claude_sessions,
    } = runtime;
    let provider_turn_started = Instant::now();
    let ttft_session_id = turn.session_id;
    let ttft_message_id = turn.message_id;
    let response_language_instruction = turn.response_language.instruction();
    let request = match request_template {
        Some(mut request) => {
            request.prompt = turn.prompt.clone();
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
            request.session_id = turn.provider_session_id.clone().or(request.session_id);
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
        None => {
            let mcp_external_servers = if tools_enabled {
                let mut servers = turn.external_mcp_servers;
                servers.push(turn.agent_mcp_server);
                servers
            } else {
                Vec::new()
            };
            ChatStreamRequest {
                prompt: turn.prompt.clone(),
                owner_session_id: Some(turn.session_id.to_string()),
                client_user_message_id: Some(turn.message_id.to_string()),
                attachments: turn.attachments,
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
                output_schema: turn.output_schema,
                mcp_owner_id: None,
                mcp_allowed_scopes: Vec::new(),
                mcp_user_id: None,
                mcp_external_servers,
                mcp_api_token: None,
                provider_auth: None,
                git_credentials: Vec::new(),
                working_directory: Some(turn.cwd.clone()),
                session_id: turn.provider_session_id.clone(),
                provider_channel: ProviderChannel::Direct,
                persist_session: Some(true),
                web_search_allowed: true,
                resume_unavailable_prompt: None,
            }
        }
    };
    let permission = local_permission(turn.permission_mode);
    let claude_snapshot =
        (turn.provider == CodingProvider::Claude).then(|| ClaudeSessionSnapshot {
            request: request.clone(),
            permission,
            pool: claude_pool.clone(),
        });
    let interrupted = Arc::new(AtomicBool::new(false));
    let mut stream = match turn.provider {
        CodingProvider::Codex => {
            let control_rx = map_controls(controls, Arc::clone(&interrupted));
            if local && let Some(pool) = codex_pool {
                run_pooled_codex_local_chat_stream(request, control_rx, permission, pool)
            } else if local {
                run_codex_local_chat_stream(request, control_rx, permission)
            } else {
                run_codex_chat_stream_with_control(request, control_rx)
            }
        }
        CodingProvider::Claude if local && request_can_use_claude_pool(&request) => {
            run_pooled_claude_local_chat_stream(
                request,
                map_controls(controls, Arc::clone(&interrupted)),
                permission,
                claude_pool.unwrap_or_default(),
            )
        }
        CodingProvider::Claude if local => run_claude_local_chat_stream(
            request,
            map_controls(controls, Arc::clone(&interrupted)),
            permission,
        ),
        CodingProvider::Claude => run_claude_chat_stream_with_control(
            request,
            map_controls(controls, Arc::clone(&interrupted)),
        ),
        CodingProvider::OpenCode if local => run_opencode_local_chat_stream(request, permission),
        CodingProvider::OpenCode => {
            bail!("OpenCode execution is only supported on an enrolled host")
        }
        CodingProvider::Kimi | CodingProvider::OpenRouter | CodingProvider::OpenAiCompatible => {
            bail!("native providers must execute through Borg's native harness")
        }
    };
    tracing::debug!(
        target: "borg_ttft",
        stage = "provider_stream_created",
        elapsed_ms = provider_turn_started.elapsed().as_millis(),
        session_id = %ttft_session_id,
        message_id = %ttft_message_id,
        "Borg provider stage"
    );
    let mut assistant_message_id = Uuid::new_v4();
    let mut text = String::new();
    let mut final_output = String::new();
    let mut completed_segment = false;
    let mut last_text_emit = Instant::now() - Duration::from_millis(50);
    let mut provider_session_id = turn.provider_session_id;
    let mut first_model_output = true;
    let mut terminal_seen = false;
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
                if last_text_emit.elapsed() >= Duration::from_millis(40) || delta.ends_with('\n') {
                    send(
                        &events,
                        SessionEventKind::Message {
                            message_id: assistant_message_id,
                            actor: EventActor::Assistant,
                            text: text.clone(),
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
                if first_model_output {
                    first_model_output = false;
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
                send(&events, SessionEventKind::ReasoningDelta { text: delta }).await;
            }
            ChatStreamEvent::Narration {
                text: narration_text,
            } => {
                if first_model_output {
                    first_model_output = false;
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
            } => {
                terminal_seen = true;
                final_output = final_text;
                if let Some(session_id) = session_id {
                    provider_session_id = Some(session_id.clone());
                    send(
                        &events,
                        SessionEventKind::ProviderSessionLinked {
                            provider_session_id: session_id,
                        },
                    )
                    .await;
                }
                if let Some(usage) = usage {
                    send(
                        &events,
                        SessionEventKind::UsageUpdated {
                            provider_duration_ms: usage.duration_ms,
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
        send(
            &events,
            SessionEventKind::Error {
                message: error.to_string(),
            },
        )
        .await;
        return Err(error);
    }
    send(
        &events,
        SessionEventKind::StatusChanged {
            status: SessionStatus::Ready,
            detail: None,
        },
    )
    .await;
    if let (Some(session_id), Some(snapshot), Some(registry)) = (
        provider_session_id.as_ref(),
        claude_snapshot,
        claude_sessions,
    ) {
        registry
            .lock()
            .expect("Claude session registry lock poisoned")
            .insert(session_id.clone(), snapshot);
    }
    Ok(AgentTurnResult {
        provider_session_id,
        final_text: if final_output.is_empty() {
            text
        } else {
            final_output
        },
    })
}

fn require_provider_stream_terminal(terminal_seen: bool) -> Result<()> {
    anyhow::ensure!(
        terminal_seen,
        "provider stream closed without a terminal Done or Failed event"
    );
    Ok(())
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
    if text.trim().is_empty() {
        anyhow::ensure!(
            completed_segment || interrupted,
            "provider completed without a visible response (empty result)"
        );
        Ok(None)
    } else {
        Ok(Some(text.to_string()))
    }
}

fn request_can_use_claude_pool(request: &ChatStreamRequest) -> bool {
    request.provider_auth.is_none() && request.git_credentials.is_empty()
}

fn usage_from_session_event(event: &SessionEventKind) -> Option<ProviderCallUsage> {
    let SessionEventKind::UsageUpdated {
        provider_duration_ms,
        input_tokens,
        output_tokens,
        cached_input_tokens,
        cache_creation_input_tokens,
        total_tokens,
        cost_microusd,
        cost_basis,
        context_tokens,
        context_window_tokens,
        ..
    } = event
    else {
        return None;
    };
    Some(ProviderCallUsage {
        duration_ms: *provider_duration_ms,
        input_tokens: *input_tokens,
        output_tokens: *output_tokens,
        cached_input_tokens: *cached_input_tokens,
        cache_creation_input_tokens: *cache_creation_input_tokens,
        total_tokens: *total_tokens,
        context_tokens: *context_tokens,
        context_window_tokens: *context_window_tokens,
        cost_microusd: *cost_microusd,
        cost_basis: match cost_basis.as_str() {
            "provider_reported" => CostBasis::ProviderReported,
            "estimated_from_pricing" => CostBasis::EstimatedFromPricing,
            _ => CostBasis::Unavailable,
        },
    })
}

async fn compact_claude_session(
    snapshot: ClaudeSessionSnapshot,
    provider_session_id: &str,
) -> Result<ProviderCallUsage> {
    let mut request = snapshot.request;
    request.prompt = "/compact".to_string();
    request.attachments.clear();
    request.output_schema = None;
    request.session_id = Some(provider_session_id.to_string());
    request.resume_unavailable_prompt = None;
    let mut stream = if let Some(pool) = snapshot.pool
        && request_can_use_claude_pool(&request)
    {
        run_pooled_claude_local_chat_stream(request, None, snapshot.permission, pool)
    } else {
        run_claude_local_chat_stream(request, None, snapshot.permission)
    };
    while let Some(event) = stream.recv().await {
        match event {
            ChatStreamEvent::Done { usage, .. } => return Ok(usage.unwrap_or_default()),
            ChatStreamEvent::Failed { error } => bail!("Claude context compaction failed: {error}"),
            _ => {}
        }
    }
    bail!("Claude context compaction ended without confirmation")
}

fn add_provider_usage(total: &mut ProviderCallUsage, additional: &ProviderCallUsage) {
    total.duration_ms = total.duration_ms.saturating_add(additional.duration_ms);
    total.input_tokens = total.input_tokens.saturating_add(additional.input_tokens);
    total.cached_input_tokens = total
        .cached_input_tokens
        .saturating_add(additional.cached_input_tokens);
    total.cache_creation_input_tokens = total
        .cache_creation_input_tokens
        .saturating_add(additional.cache_creation_input_tokens);
    total.output_tokens = total.output_tokens.saturating_add(additional.output_tokens);
    total.total_tokens = total.total_tokens.saturating_add(additional.total_tokens);
    total.context_tokens = additional.context_tokens.or(total.context_tokens);
    total.context_window_tokens = additional
        .context_window_tokens
        .or(total.context_window_tokens);
    total.cost_microusd = match (total.cost_microusd, additional.cost_microusd) {
        (Some(left), Some(right)) => Some(left.saturating_add(right)),
        (left, right) => right.or(left),
    };
    if additional.cost_microusd.is_some() {
        total.cost_basis = additional.cost_basis;
    }
}

fn provider_event_is_transient(kind: &str) -> bool {
    if provider_event_is_compaction_lifecycle(kind) {
        return true;
    }
    let method = kind.split_once(':').map_or(kind, |(method, _)| method);
    let event_name = method.rsplit('/').next().unwrap_or(method);
    event_name.eq_ignore_ascii_case("delta")
        || event_name.ends_with("Delta")
        || matches!(
            method,
            "thread/tokenUsage/updated"
                | "account/rateLimits/updated"
                | "turn/diff/updated"
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
    let last = payload.get("last")?;
    Some(LiveContextUsage {
        total_tokens: last.get("totalTokens")?.as_u64()?,
        context_window_tokens: payload.get("model_context_window")?.as_u64()?,
    })
}

fn provider_event_is_compaction_lifecycle(kind: &str) -> bool {
    let Some((method, item_type)) = kind.split_once(':') else {
        return false;
    };
    matches!(method, "item/started" | "item/completed")
        && matches!(
            item_type
                .to_ascii_lowercase()
                .replace(['-', '_'], "")
                .as_str(),
            "contextcompaction"
        )
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
                        ack,
                    } => {
                        match tx
                            .send(ChatStreamControl::Steer {
                                client_user_message_id: Some(message_id.to_string()),
                                text,
                                attachments,
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
        );

        assert_eq!(in_flight_snapshot.external_mcp_servers[0].name, "old");
        assert_eq!(
            in_flight_snapshot.skill_roots,
            [PathBuf::from("old-skills")]
        );
        let next_turn = executor.runtime_extensions.read().unwrap();
        assert_eq!(next_turn.external_mcp_servers[0].name, "new");
        assert_eq!(next_turn.skill_roots, [PathBuf::from("new-skills")]);
    }

    #[test]
    fn compaction_usage_is_added_to_preparation_usage() {
        let mut preparation = ProviderCallUsage {
            duration_ms: 10,
            input_tokens: 100,
            cached_input_tokens: 40,
            cache_creation_input_tokens: 5,
            output_tokens: 20,
            total_tokens: 165,
            cost_microusd: Some(100),
            cost_basis: CostBasis::ProviderReported,
            ..Default::default()
        };
        let compaction = ProviderCallUsage {
            duration_ms: 25,
            input_tokens: 200,
            cached_input_tokens: 80,
            cache_creation_input_tokens: 7,
            output_tokens: 30,
            total_tokens: 317,
            cost_microusd: Some(200),
            cost_basis: CostBasis::ProviderReported,
            ..Default::default()
        };

        add_provider_usage(&mut preparation, &compaction);

        assert_eq!(preparation.duration_ms, 35);
        assert_eq!(preparation.input_tokens, 300);
        assert_eq!(preparation.cached_input_tokens, 120);
        assert_eq!(preparation.cache_creation_input_tokens, 12);
        assert_eq!(preparation.output_tokens, 50);
        assert_eq!(preparation.total_tokens, 482);
        assert_eq!(preparation.cost_microusd, Some(300));
        assert_eq!(preparation.cost_basis, CostBasis::ProviderReported);
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
                ))
            });

        executor.refresh_runtime_extensions().await;

        let snapshot = executor.runtime_extensions.read().unwrap();
        assert_eq!(snapshot.external_mcp_servers[0].name, "reloaded");
        assert_eq!(snapshot.skill_roots, [PathBuf::from("reloaded-skills")]);
    }

    #[tokio::test]
    async fn pending_steer_acknowledgement_does_not_block_interrupt() {
        let (control_tx, control_rx) = mpsc::channel(4);
        let interrupted = Arc::new(AtomicBool::new(false));
        let mut provider_controls =
            map_controls(Some(control_rx), Arc::clone(&interrupted)).expect("mapped controls");
        let (ack, acknowledgement) = tokio::sync::oneshot::channel();

        control_tx
            .send(AgentTurnControl::Steer {
                message_id: Uuid::new_v4(),
                text: "additional context".to_string(),
                attachments: Vec::new(),
                ack,
            })
            .await
            .unwrap();
        let provider_ack = match provider_controls.recv().await {
            Some(ChatStreamControl::Steer { ack, .. }) => ack,
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
        assert!(provider_event_is_transient(
            "item/started:contextCompaction"
        ));
        assert!(provider_event_is_transient(
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
    }
}
