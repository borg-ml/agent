use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use borg_provider::ProviderChannel;
use borg_provider::provider::{
    ChatApprovalDecision, ChatStreamControl, ChatStreamEvent, ChatStreamRequest,
    CodexAppServerPool, LocalAgentPermission, run_claude_chat_stream, run_claude_local_chat_stream,
    run_codex_chat_stream_with_control, run_codex_local_chat_stream,
    run_opencode_local_chat_stream, run_pooled_codex_local_chat_stream,
};
use serde_json::Value;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    CodingProvider, EventActor, MessageStatus, PermissionMode, SessionEventKind, SessionStatus,
    native_harness::NativeHarness,
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
the Borg session tree and cannot be controlled from Borg Remote.";

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

    async fn compact(&self, _provider: CodingProvider, _provider_session_id: &str) -> Result<()> {
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

    async fn stop_session(&self, _session_id: Uuid) -> Result<()> {
        Ok(())
    }
}

/// Direct provider execution used by the CLI and enrolled hosts.
#[derive(Clone, Default)]
pub struct LocalAgentTurnExecutor {
    codex_pool: CodexAppServerPool,
    native_harness: NativeHarness,
    external_mcp_servers: Vec<borg_provider::mcp::ExternalMcpServer>,
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
        mut self,
        servers: Vec<borg_provider::mcp::ExternalMcpServer>,
    ) -> Self {
        self.external_mcp_servers = servers;
        self
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
        turn.external_mcp_servers
            .extend(self.external_mcp_servers.clone());
        if turn.provider.uses_native_harness() {
            return self.native_harness.run(turn, events, controls).await;
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
            None,
            true,
            Some(self.codex_pool.clone()),
        )
        .await
    }

    async fn compact(&self, provider: CodingProvider, provider_session_id: &str) -> Result<()> {
        anyhow::ensure!(
            provider == CodingProvider::Codex,
            "manual context compaction is currently supported for Codex sessions"
        );
        self.codex_pool.compact(provider_session_id)
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
        Ok(AgentCompaction { summary, usage })
    }

    async fn stop_session(&self, session_id: Uuid) -> Result<()> {
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
        CodingProvider::Codex | CodingProvider::Claude | CodingProvider::OpenCode => {
            run_borg_provider_turn(turn, events, controls, None, true, None).await
        }
        CodingProvider::Kimi | CodingProvider::OpenRouter | CodingProvider::OpenAiCompatible => {
            NativeHarness::default().run(turn, events, controls).await
        }
    }
}

async fn run_borg_provider_turn(
    turn: AgentTurn,
    events: mpsc::Sender<SessionEventKind>,
    controls: Option<mpsc::Receiver<AgentTurnControl>>,
    request_template: Option<ChatStreamRequest>,
    local: bool,
    codex_pool: Option<CodexAppServerPool>,
) -> Result<AgentTurnResult> {
    let provider_turn_started = Instant::now();
    let ttft_session_id = turn.session_id;
    let ttft_message_id = turn.message_id;
    let response_language_instruction = turn.response_language.instruction();
    let request = match request_template {
        Some(mut request) => {
            request.prompt = turn.prompt.clone();
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
            request
                .mcp_external_servers
                .extend(turn.external_mcp_servers);
            request.mcp_external_servers.push(turn.agent_mcp_server);
            if let Some(instruction) = response_language_instruction {
                request.system_prompt.push_str("\n\n");
                request.system_prompt.push_str(instruction);
            }
            request
        }
        None => {
            let mut mcp_external_servers = turn.external_mcp_servers;
            mcp_external_servers.push(turn.agent_mcp_server);
            ChatStreamRequest {
                prompt: turn.prompt.clone(),
                attachments: turn.attachments,
                model: turn.model.clone(),
                effort: turn.effort.clone(),
                fast: turn.fast.unwrap_or(false),
                system_prompt: match response_language_instruction {
                    Some(instruction) => format!("{CODING_SYSTEM_PROMPT}\n\n{instruction}"),
                    None => CODING_SYSTEM_PROMPT.to_string(),
                },
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
    let mut stream = match turn.provider {
        CodingProvider::Codex => {
            let control_rx = map_controls(controls);
            if local && let Some(pool) = codex_pool {
                run_pooled_codex_local_chat_stream(request, control_rx, permission, pool)
            } else if local {
                run_codex_local_chat_stream(request, control_rx, permission)
            } else {
                run_codex_chat_stream_with_control(request, control_rx)
            }
        }
        CodingProvider::Claude if local => run_claude_local_chat_stream(request, permission),
        CodingProvider::Claude => run_claude_chat_stream(request),
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
                if !completed_segment {
                    text = final_output.clone();
                    if !text.trim().is_empty() {
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
                    }
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
    error.to_string()
}

fn map_controls(
    controls: Option<mpsc::Receiver<AgentTurnControl>>,
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

    #[tokio::test]
    async fn pending_steer_acknowledgement_does_not_block_interrupt() {
        let (control_tx, control_rx) = mpsc::channel(4);
        let mut provider_controls = map_controls(Some(control_rx)).expect("mapped controls");
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
}
