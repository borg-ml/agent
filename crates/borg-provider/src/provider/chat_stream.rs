//! External command adapters for the subscription-backed Codex and Claude routes.
//!
//! These adapters intentionally keep subscription authentication and execution at
//! the CLI boundary. They launch the providers' native streaming protocols as
//! thin wire adapters and do not own Borg's tool/runtime loop; the
//! provider-neutral NativeHarness owns API-key/OpenAI-compatible model routes.

use crate::mcp::{ExternalMcpServer, ProviderMcpSetup, prepare_external_provider_mcp};
use crate::runtime::ProviderCallUsage;
use crate::{ProviderAuthBundle, ProviderAuthProvider, ProviderChannel};
use anyhow::{Context, Result, bail};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{Mutex, mpsc};

// app-server rejects a single text input larger than 1 MiB. Keep this guard at
// the resume fallback boundary so an unavailable native thread returns control
// to Borg for durable compaction instead of issuing a request known to fail.
const CODEX_APP_SERVER_TEXT_INPUT_LIMIT_CHARS: usize = 1 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubscriptionProvider {
    Codex,
    Claude,
}

/// Billing/authentication lane for the native CLI adapter. The provider name
/// alone is insufficient: both Codex and Claude CLIs can run with either an
/// OAuth subscription session or an API key. Keeping this distinction beside
/// the usage parser prevents subscription-equivalent counters from being
/// rendered as API charges while preserving real API-key billing telemetry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum ProviderBillingMode {
    Subscription,
    ApiKey,
    #[default]
    Unknown,
}

/// The Claude native runtime does not currently carry Borg's client message
/// id through its steer control enum. Keep the correlation at this adapter
/// boundary instead: controls are serialized into the runtime, and Claude's
/// command lifecycle events are serialized back out on the same stream.
#[derive(Default)]
struct ClaudeSteerCorrelation {
    pending: VecDeque<String>,
    commands: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum ChatStreamEvent {
    ProviderEvent {
        kind: String,
        payload: Value,
        raw_payload: Option<Value>,
        stream_channel: Option<String>,
        content_text: Option<String>,
        provider_item_id: Option<String>,
        tool_use_id: Option<String>,
        tool_name: Option<String>,
    },
    Delta(String),
    ReasoningDelta(String),
    Narration {
        text: String,
    },
    Phase {
        name: String,
        input: Value,
    },
    ToolCall {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        tool_use_id: String,
        output: String,
        is_error: bool,
        input: Option<Value>,
    },
    ApprovalRequested {
        approval_id: String,
        title: String,
        detail: String,
        command: Option<String>,
    },
    ProviderInteractionRequested {
        interaction_id: String,
        kind: String,
        title: String,
        detail: String,
        payload: Value,
    },
    Done {
        final_text: String,
        usage: Option<ProviderCallUsage>,
        session_id: Option<String>,
    },
    Failed {
        error: String,
    },
}

#[derive(Debug)]
pub enum ChatStreamControl {
    Steer {
        client_user_message_id: Option<String>,
        text: String,
        attachments: Vec<PathBuf>,
        ack: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    },
    Approval {
        approval_id: String,
        decision: ChatApprovalDecision,
    },
    ProviderInteractionResponse {
        interaction_id: String,
        response: Value,
    },
    Interrupt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatApprovalDecision {
    ApproveOnce,
    ApproveSession,
    Reject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalAgentPermission {
    FullAccess,
    Auto,
    Manual,
}

#[derive(Debug, Clone)]
pub struct ChatProviderAuth {
    pub provider: ProviderAuthProvider,
    pub bundle: ProviderAuthBundle,
    pub codex_home: Option<PathBuf>,
}

#[derive(Clone)]
pub struct ChatGitCredential {
    pub host: String,
    pub username: String,
    pub token: String,
}

impl fmt::Debug for ChatGitCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChatGitCredential")
            .field("host", &self.host)
            .field("username", &self.username)
            .field("token", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct ChatStreamRequest {
    pub prompt: String,
    /// Stable identity for the provider-native conversation configuration.
    /// The prompt is deliberately excluded: a healthy pooled process receives
    /// only the new user delta after its first full replay.
    pub lifecycle_key: Option<String>,
    pub owner_session_id: Option<String>,
    pub client_user_message_id: Option<String>,
    pub attachments: Vec<PathBuf>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub fast: bool,
    pub system_prompt: String,
    pub output_schema: Option<Value>,
    pub mcp_owner_id: Option<String>,
    pub mcp_allowed_scopes: Vec<String>,
    pub mcp_user_id: Option<String>,
    pub mcp_external_servers: Vec<ExternalMcpServer>,
    pub mcp_api_token: Option<String>,
    pub provider_auth: Option<ChatProviderAuth>,
    pub git_credentials: Vec<ChatGitCredential>,
    pub working_directory: Option<PathBuf>,
    pub session_id: Option<String>,
    pub provider_channel: ProviderChannel,
    pub persist_session: Option<bool>,
    pub web_search_allowed: bool,
    pub resume_unavailable_prompt: Option<String>,
}

/// A volatile, per-Borg-session Claude subscription lane.
///
/// Borg's SQLite journal remains authoritative. This pool only keeps the
/// provider process and its already-authenticated native conversation alive so
/// that ordinary turns follow the same append-only path as the first-party CLI.
/// A lifecycle-key change causes the next call to start a fresh native process;
/// callers must then send the complete durable prompt again.
#[derive(Clone, Default)]
pub struct ClaudeSubscriptionPool {
    inner: Arc<Mutex<ClaudeSubscriptionPoolState>>,
}

#[derive(Default)]
struct ClaudeSubscriptionPoolState {
    native: claude_agents::ClaudePool,
    lifecycle_key: Option<String>,
    command: Option<claude_agents::CommandSpec>,
    _auth_home: Option<TempDir>,
    _mcp_setup: Option<(TempDir, ProviderMcpSetup)>,
}

/// A per-Borg-session Codex app-server lane. The live process is volatile, but
/// acknowledged threads may also be persisted and resumed after an idle
/// process restart. Borg's journal remains authoritative; an uncertain turn
/// invalidates the checkpoint and forces a canonical replay.
#[derive(Clone, Default)]
pub struct CodexSubscriptionPool {
    inner: Arc<Mutex<CodexSubscriptionPoolState>>,
}

#[derive(Default)]
struct CodexSubscriptionPoolState {
    lifecycle_key: Option<String>,
    process: Option<PooledCodexProcess>,
    _auth_home: Option<TempDir>,
}

struct PooledCodexProcess {
    child: Child,
    stdin: ChildStdin,
    lines: tokio::io::Lines<BufReader<ChildStdout>>,
    stderr: Arc<Mutex<Vec<u8>>>,
    thread_id: String,
    next_request_id: u64,
}

struct StartedCodexProcess {
    process: PooledCodexProcess,
    resumed: bool,
}

impl CodexSubscriptionPool {
    /// Close an idle app-server cleanly so its persisted rollout is available
    /// to the next Borg process. A bounded hard kill remains the final fallback
    /// for a provider that does not react to stdin shutdown.
    pub async fn shutdown(&self) {
        let process = {
            let mut state = self.inner.lock().await;
            state.lifecycle_key = None;
            state.process.take()
        };
        if let Some(process) = process {
            shutdown_pooled_codex_process(process).await;
        }
    }
}

pub fn run_claude_chat_stream(request: ChatStreamRequest) -> mpsc::Receiver<ChatStreamEvent> {
    run_subscription_stream(
        request,
        None,
        SubscriptionProvider::Claude,
        LocalAgentPermission::FullAccess,
    )
}

pub fn run_claude_chat_stream_with_control(
    request: ChatStreamRequest,
    controls: Option<mpsc::Receiver<ChatStreamControl>>,
) -> mpsc::Receiver<ChatStreamEvent> {
    run_subscription_stream(
        request,
        controls,
        SubscriptionProvider::Claude,
        LocalAgentPermission::FullAccess,
    )
}

pub fn run_claude_local_chat_stream(
    request: ChatStreamRequest,
    controls: Option<mpsc::Receiver<ChatStreamControl>>,
    permission: LocalAgentPermission,
) -> mpsc::Receiver<ChatStreamEvent> {
    run_subscription_stream(request, controls, SubscriptionProvider::Claude, permission)
}

/// Run Claude Code on the shared native process for this Borg session.
pub fn run_claude_local_chat_stream_pooled(
    request: ChatStreamRequest,
    controls: Option<mpsc::Receiver<ChatStreamControl>>,
    permission: LocalAgentPermission,
    pool: ClaudeSubscriptionPool,
) -> mpsc::Receiver<ChatStreamEvent> {
    let (events, receiver) = mpsc::channel(64);
    tokio::spawn(async move {
        if let Err(error) = run_claude_subscription_process_pooled(
            request,
            controls,
            permission,
            events.clone(),
            pool,
        )
        .await
        {
            let _ = events
                .send(ChatStreamEvent::Failed {
                    error: format!("{error:#}"),
                })
                .await;
        }
    });
    receiver
}

pub fn run_codex_chat_stream(request: ChatStreamRequest) -> mpsc::Receiver<ChatStreamEvent> {
    run_subscription_stream(
        request,
        None,
        SubscriptionProvider::Codex,
        LocalAgentPermission::FullAccess,
    )
}

pub fn run_codex_chat_stream_with_control(
    request: ChatStreamRequest,
    controls: Option<mpsc::Receiver<ChatStreamControl>>,
) -> mpsc::Receiver<ChatStreamEvent> {
    run_subscription_stream(
        request,
        controls,
        SubscriptionProvider::Codex,
        LocalAgentPermission::FullAccess,
    )
}

pub fn run_codex_local_chat_stream(
    request: ChatStreamRequest,
    controls: Option<mpsc::Receiver<ChatStreamControl>>,
    permission: LocalAgentPermission,
) -> mpsc::Receiver<ChatStreamEvent> {
    run_subscription_stream(request, controls, SubscriptionProvider::Codex, permission)
}

/// Run Codex app-server on the shared native thread for this Borg session.
pub fn run_codex_local_chat_stream_pooled(
    request: ChatStreamRequest,
    controls: Option<mpsc::Receiver<ChatStreamControl>>,
    permission: LocalAgentPermission,
    pool: CodexSubscriptionPool,
) -> mpsc::Receiver<ChatStreamEvent> {
    let (events, receiver) = mpsc::channel(64);
    tokio::spawn(async move {
        if let Err(error) = run_codex_subscription_process_pooled(
            request,
            controls,
            permission,
            events.clone(),
            pool,
        )
        .await
        {
            let _ = events
                .send(ChatStreamEvent::Failed {
                    error: format!("{error:#}"),
                })
                .await;
        }
    });
    receiver
}

pub fn run_codex_freeform_chat_stream(
    request: ChatStreamRequest,
) -> mpsc::Receiver<ChatStreamEvent> {
    run_codex_chat_stream(request)
}

fn run_subscription_stream(
    request: ChatStreamRequest,
    controls: Option<mpsc::Receiver<ChatStreamControl>>,
    provider: SubscriptionProvider,
    permission: LocalAgentPermission,
) -> mpsc::Receiver<ChatStreamEvent> {
    let (events, receiver) = mpsc::channel(64);
    tokio::spawn(async move {
        if let Err(error) =
            run_subscription_process(request, controls, provider, permission, events.clone()).await
        {
            let _ = events
                .send(ChatStreamEvent::Failed {
                    error: format!("{error:#}"),
                })
                .await;
        }
    });
    receiver
}

async fn run_subscription_process(
    request: ChatStreamRequest,
    controls: Option<mpsc::Receiver<ChatStreamControl>>,
    provider: SubscriptionProvider,
    permission: LocalAgentPermission,
    events: mpsc::Sender<ChatStreamEvent>,
) -> Result<()> {
    match provider {
        SubscriptionProvider::Claude => {
            run_claude_subscription_process(request, controls, permission, events).await
        }
        SubscriptionProvider::Codex => {
            run_codex_subscription_process(request, controls, permission, events).await
        }
    }
}

async fn run_claude_subscription_process(
    request: ChatStreamRequest,
    controls: Option<mpsc::Receiver<ChatStreamControl>>,
    permission: LocalAgentPermission,
    events: mpsc::Sender<ChatStreamEvent>,
) -> Result<()> {
    let auth_home = restore_auth_home(request.provider_auth.as_ref())?;
    let billing_mode =
        provider_billing_mode(SubscriptionProvider::Claude, &request, auth_home.as_ref());
    let mcp_setup = if request.mcp_external_servers.is_empty() {
        None
    } else {
        let directory = tempfile::tempdir().context("failed to create Claude MCP directory")?;
        let setup = prepare_external_provider_mcp(directory.path(), &request.mcp_external_servers)
            .context("failed to prepare Claude MCP config")?;
        Some((directory, setup))
    };
    let mcp_config_path = mcp_setup
        .as_ref()
        .and_then(|(_, setup)| setup.claude_config_path.as_deref());
    let command =
        build_claude_command_spec(&request, permission, auth_home.as_ref(), mcp_config_path)?;
    let claude_request = claude_agents::ChatStreamRequest {
        prompt: request.prompt,
        attachments: request.attachments,
        system_prompt: request.system_prompt,
        command,
        runtime_directory: None,
        lifecycle_key: request
            .lifecycle_key
            .unwrap_or_else(|| "borg-claude-subscription".to_string()),
    };

    relay_claude_runtime(claude_request, controls, events, None, billing_mode).await
}

async fn run_claude_subscription_process_pooled(
    request: ChatStreamRequest,
    controls: Option<mpsc::Receiver<ChatStreamControl>>,
    permission: LocalAgentPermission,
    events: mpsc::Sender<ChatStreamEvent>,
    pool: ClaudeSubscriptionPool,
) -> Result<()> {
    let lifecycle_key = request
        .lifecycle_key
        .clone()
        .unwrap_or_else(|| "borg-claude-subscription".to_string());
    let mut state = pool.inner.lock().await;
    if state.lifecycle_key.as_deref() != Some(lifecycle_key.as_str()) {
        let auth_home = restore_auth_home(request.provider_auth.as_ref())?;
        let mcp_setup = if request.mcp_external_servers.is_empty() {
            None
        } else {
            let directory = tempfile::tempdir().context("failed to create Claude MCP directory")?;
            let setup =
                prepare_external_provider_mcp(directory.path(), &request.mcp_external_servers)
                    .context("failed to prepare Claude MCP config")?;
            Some((directory, setup))
        };
        let mcp_config_path = mcp_setup
            .as_ref()
            .and_then(|(_, setup)| setup.claude_config_path.as_deref());
        let command =
            build_claude_command_spec(&request, permission, auth_home.as_ref(), mcp_config_path)?;
        state.lifecycle_key = Some(lifecycle_key.clone());
        state.command = Some(command);
        state._auth_home = auth_home;
        state._mcp_setup = mcp_setup;
    }
    let command = state
        .command
        .clone()
        .context("pooled Claude command was not initialized")?;
    let native_pool = state.native.clone();
    let billing_mode = provider_billing_mode(
        SubscriptionProvider::Claude,
        &request,
        state._auth_home.as_ref(),
    );
    drop(state);

    let claude_request = claude_agents::ChatStreamRequest {
        prompt: request.prompt,
        attachments: request.attachments,
        system_prompt: request.system_prompt,
        command,
        runtime_directory: None,
        lifecycle_key,
    };
    relay_claude_runtime(
        claude_request,
        controls,
        events,
        Some(native_pool),
        billing_mode,
    )
    .await
}

async fn relay_claude_runtime(
    claude_request: claude_agents::ChatStreamRequest,
    controls: Option<mpsc::Receiver<ChatStreamControl>>,
    events: mpsc::Sender<ChatStreamEvent>,
    pool: Option<claude_agents::ClaudePool>,
    billing_mode: ProviderBillingMode,
) -> Result<()> {
    let (native_events, mut native_events_receiver) = mpsc::channel(64);
    let steer_correlation = Arc::new(StdMutex::new(ClaudeSteerCorrelation::default()));
    let (native_controls, mut control_forwarder) = match controls {
        Some(mut controls) => {
            let (sender, receiver) = mpsc::channel(64);
            let steer_correlation = Arc::clone(&steer_correlation);
            let forwarder = tokio::spawn(async move {
                while let Some(control) = controls.recv().await {
                    let client_user_message_id =
                        claude_steer_client_message_id(&control).map(str::to_owned);
                    if let Some(client_user_message_id) = client_user_message_id.as_deref() {
                        register_claude_steer(
                            &steer_correlation,
                            client_user_message_id.to_string(),
                        );
                    }
                    if sender.send(map_claude_control(control)).await.is_err() {
                        if let Some(client_user_message_id) = client_user_message_id.as_deref() {
                            unregister_claude_steer(&steer_correlation, client_user_message_id);
                        }
                        break;
                    }
                }
            });
            (Some(receiver), Some(forwarder))
        }
        None => (None, None),
    };
    let mut runner = Some(tokio::spawn(async move {
        match pool {
            Some(pool) => {
                claude_agents::run_pooled(claude_request, native_events, native_controls, pool)
                    .await
            }
            None => claude_agents::run(claude_request, native_events, native_controls).await,
        }
    }));

    loop {
        tokio::select! {
            // The session actor may abort its consumer on timeout, interrupt,
            // or provider switch. The provider task is separate from that
            // actor future, so watching the output channel here is what makes
            // cancellation reach the Claude child instead of leaving an idle
            // subscription process behind.
            _ = events.closed() => {
                if let Some(runner) = runner.take() {
                    runner.abort();
                    let _ = runner.await;
                }
                if let Some(forwarder) = control_forwarder.take() {
                    forwarder.abort();
                    let _ = forwarder.await;
                }
                return Ok(());
            }
            event = native_events_receiver.recv() => {
                let Some(event) = event else {
                    break;
                };
                if events
                    .send(map_claude_event_with_correlation(
                        event,
                        billing_mode,
                        Some(&steer_correlation),
                    ))
                    .await
                    .is_err()
                {
                    if let Some(runner) = runner.take() {
                        runner.abort();
                        let _ = runner.await;
                    }
                    if let Some(forwarder) = control_forwarder.take() {
                        forwarder.abort();
                        let _ = forwarder.await;
                    }
                    return Ok(());
                }
            }
        }
    }

    let runner_result = runner
        .expect("Claude subscription runner should still be active")
        .await;
    if let Some(forwarder) = control_forwarder.take() {
        forwarder.abort();
        let _ = forwarder.await;
    }
    runner_result.context("Claude subscription runtime task failed")??;
    Ok(())
}

fn claude_steer_client_message_id(control: &ChatStreamControl) -> Option<&str> {
    match control {
        ChatStreamControl::Steer {
            client_user_message_id,
            ..
        } => client_user_message_id.as_deref(),
        _ => None,
    }
}

fn register_claude_steer(correlation: &StdMutex<ClaudeSteerCorrelation>, message_id: String) {
    let mut correlation = correlation
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    correlation.pending.push_back(message_id);
}

fn unregister_claude_steer(correlation: &StdMutex<ClaudeSteerCorrelation>, message_id: &str) {
    let mut correlation = correlation
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(index) = correlation
        .pending
        .iter()
        .rposition(|pending| pending == message_id)
    {
        correlation.pending.remove(index);
    }
}

fn map_claude_control(control: ChatStreamControl) -> claude_agents::ChatStreamControl {
    match control {
        ChatStreamControl::Steer {
            client_user_message_id: _,
            text,
            attachments,
            ack,
        } => claude_agents::ChatStreamControl::Steer {
            text,
            attachments,
            ack,
        },
        ChatStreamControl::Approval {
            approval_id,
            decision,
        } => claude_agents::ChatStreamControl::Approval {
            approval_id,
            decision: match decision {
                ChatApprovalDecision::ApproveOnce => {
                    claude_agents::ChatApprovalDecision::ApproveOnce
                }
                ChatApprovalDecision::ApproveSession => {
                    claude_agents::ChatApprovalDecision::ApproveSession
                }
                ChatApprovalDecision::Reject => claude_agents::ChatApprovalDecision::Reject,
            },
        },
        ChatStreamControl::ProviderInteractionResponse {
            interaction_id,
            response,
        } => claude_agents::ChatStreamControl::ProviderInteractionResponse {
            interaction_id,
            response,
        },
        ChatStreamControl::Interrupt => claude_agents::ChatStreamControl::Interrupt,
    }
}

#[cfg(test)]
fn map_claude_event(
    event: claude_agents::ChatStreamEvent,
    billing_mode: ProviderBillingMode,
) -> ChatStreamEvent {
    map_claude_event_with_correlation(event, billing_mode, None)
}

fn map_claude_event_with_correlation(
    event: claude_agents::ChatStreamEvent,
    billing_mode: ProviderBillingMode,
    steer_correlation: Option<&StdMutex<ClaudeSteerCorrelation>>,
) -> ChatStreamEvent {
    match event {
        claude_agents::ChatStreamEvent::ProviderEvent {
            kind,
            mut payload,
            raw_payload,
            stream_channel,
            content_text,
            provider_item_id,
            tool_use_id,
            tool_name,
        } => {
            enrich_claude_lifecycle_payload(
                &kind,
                &mut payload,
                raw_payload.as_ref(),
                steer_correlation,
            );
            ChatStreamEvent::ProviderEvent {
                kind,
                payload,
                raw_payload,
                stream_channel,
                content_text,
                provider_item_id,
                tool_use_id,
                tool_name,
            }
        }
        claude_agents::ChatStreamEvent::Delta(text) => ChatStreamEvent::Delta(text),
        claude_agents::ChatStreamEvent::ReasoningDelta(text) => {
            ChatStreamEvent::ReasoningDelta(text)
        }
        claude_agents::ChatStreamEvent::Narration { text } => ChatStreamEvent::Narration { text },
        claude_agents::ChatStreamEvent::Phase { name, input } => {
            ChatStreamEvent::Phase { name, input }
        }
        claude_agents::ChatStreamEvent::ToolCall { id, name, input } => {
            ChatStreamEvent::ToolCall { id, name, input }
        }
        claude_agents::ChatStreamEvent::ToolResult {
            tool_use_id,
            output,
            is_error,
            input,
        } => ChatStreamEvent::ToolResult {
            tool_use_id,
            output,
            is_error,
            input,
        },
        claude_agents::ChatStreamEvent::ApprovalRequested {
            approval_id,
            title,
            detail,
            command,
        } => ChatStreamEvent::ApprovalRequested {
            approval_id,
            title,
            detail,
            command,
        },
        claude_agents::ChatStreamEvent::ProviderInteractionRequested {
            interaction_id,
            kind,
            title,
            detail,
            payload,
        } => ChatStreamEvent::ProviderInteractionRequested {
            interaction_id,
            kind,
            title,
            detail,
            payload,
        },
        claude_agents::ChatStreamEvent::Done {
            final_text,
            usage,
            session_id,
        } => ChatStreamEvent::Done {
            final_text,
            usage: usage.map(|usage| map_claude_usage(usage, billing_mode)),
            session_id,
        },
        claude_agents::ChatStreamEvent::Failed { error } => ChatStreamEvent::Failed { error },
    }
}

fn enrich_claude_lifecycle_payload(
    kind: &str,
    payload: &mut Value,
    raw_payload: Option<&Value>,
    steer_correlation: Option<&StdMutex<ClaudeSteerCorrelation>>,
) {
    if kind != "claude.command_lifecycle" {
        return;
    }
    let Some(raw_payload) = raw_payload else {
        return;
    };
    let Some(payload) = payload.as_object_mut() else {
        return;
    };
    for key in ["command_uuid", "state", "uuid"] {
        if let Some(value) = raw_payload.get(key) {
            payload.insert(key.to_string(), value.clone());
        }
    }
    let (Some(command_uuid), Some(state)) = (
        raw_payload.get("command_uuid").and_then(Value::as_str),
        raw_payload.get("state").and_then(Value::as_str),
    ) else {
        return;
    };
    let Some(steer_correlation) = steer_correlation else {
        return;
    };
    let mut steer_correlation = steer_correlation
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    match state {
        "queued" => {
            if !steer_correlation.commands.contains_key(command_uuid)
                && let Some(message_id) = steer_correlation.pending.pop_front()
            {
                payload.insert(
                    "client_user_message_id".to_string(),
                    Value::String(message_id.clone()),
                );
                steer_correlation
                    .commands
                    .insert(command_uuid.to_string(), message_id);
            }
        }
        "started" | "completed" | "failed" | "cancelled" | "error" => {
            if let Some(message_id) = steer_correlation.commands.get(command_uuid).cloned() {
                payload.insert(
                    "client_user_message_id".to_string(),
                    Value::String(message_id),
                );
                if matches!(state, "completed" | "failed" | "cancelled" | "error") {
                    steer_correlation.commands.remove(command_uuid);
                }
            }
        }
        _ => {}
    }
}

fn map_claude_usage(
    usage: claude_agents::ProviderCallUsage,
    billing_mode: ProviderBillingMode,
) -> ProviderCallUsage {
    let cost_basis = usage_cost_basis(usage.cost_microusd, billing_mode);
    ProviderCallUsage {
        duration_ms: usage.duration_ms,
        input_tokens: usage.input_tokens,
        cached_input_tokens: usage.cached_input_tokens,
        cache_creation_input_tokens: usage.cache_creation_input_tokens,
        output_tokens: usage.output_tokens,
        total_tokens: usage.total_tokens,
        context_tokens: usage.context_tokens,
        context_window_tokens: usage.context_window_tokens,
        cost_microusd: usage.cost_microusd,
        cost_basis,
    }
}

async fn run_codex_subscription_process_pooled(
    mut request: ChatStreamRequest,
    controls: Option<mpsc::Receiver<ChatStreamControl>>,
    permission: LocalAgentPermission,
    events: mpsc::Sender<ChatStreamEvent>,
    pool: CodexSubscriptionPool,
) -> Result<()> {
    let lifecycle_key = request
        .lifecycle_key
        .clone()
        .unwrap_or_else(|| "borg-codex-subscription".to_string());
    let mut state = pool.inner.lock().await;
    if state.lifecycle_key.as_deref() != Some(lifecycle_key.as_str()) {
        if let Some(process) = state.process.take() {
            shutdown_pooled_codex_process(process).await;
        }
        state.lifecycle_key = Some(lifecycle_key);
        state._auth_home = restore_auth_home(request.provider_auth.as_ref())?;
    }
    let billing_mode = provider_billing_mode(
        SubscriptionProvider::Codex,
        &request,
        state._auth_home.as_ref(),
    );
    if state.process.is_none() {
        let started =
            start_pooled_codex_process(&request, permission, state._auth_home.as_ref()).await?;
        if request.session_id.is_some() && !started.resumed {
            request.prompt = request
                .resume_unavailable_prompt
                .take()
                .context("Codex resume failed without a durable replay prompt")?;
            request.session_id = None;
        }
        state.process = Some(started.process);
    }
    let mut process = state
        .process
        .take()
        .context("pooled Codex process was not initialized")?;
    let reusable = run_pooled_codex_turn(
        &mut process,
        &request,
        controls,
        permission,
        &events,
        billing_mode,
    )
    .await;
    match reusable {
        Ok(true) => {
            state.process = Some(process);
            Ok(())
        }
        Ok(false) => Ok(()),
        Err(error) => Err(error),
    }
}

async fn shutdown_pooled_codex_process(mut process: PooledCodexProcess) {
    let _ = process.stdin.shutdown().await;
    if tokio::time::timeout(Duration::from_secs(2), process.child.wait())
        .await
        .is_err()
    {
        let _ = process.child.kill().await;
        let _ = process.child.wait().await;
    }
}

async fn start_pooled_codex_process(
    request: &ChatStreamRequest,
    permission: LocalAgentPermission,
    auth_home: Option<&TempDir>,
) -> Result<StartedCodexProcess> {
    let mut command = codex_app_server_command(request, auth_home)?;
    let mut child = command.spawn().with_context(|| {
        format!(
            "failed to start {}",
            SubscriptionProvider::Codex.executable()
        )
    })?;
    let mut stdin = child
        .stdin
        .take()
        .context("pooled Codex stdin pipe missing")?;
    let stdout = child
        .stdout
        .take()
        .context("pooled Codex stdout pipe missing")?;
    let stderr = child
        .stderr
        .take()
        .context("pooled Codex stderr pipe missing")?;
    let stderr_buffer = Arc::new(Mutex::new(Vec::new()));
    let stderr_buffer_task = Arc::clone(&stderr_buffer);
    tokio::spawn(async move {
        let mut stderr = stderr;
        let mut output = Vec::new();
        let _ = tokio::io::AsyncReadExt::read_to_end(&mut stderr, &mut output).await;
        stderr_buffer_task.lock().await.extend(output);
    });

    write_codex_request(
        &mut stdin,
        1,
        "initialize",
        serde_json::json!({
            "clientInfo": {
                "name": "borg",
                "title": "Borg",
                "version": env!("CARGO_PKG_VERSION")
            },
            "capabilities": {
                "experimentalApi": true,
                "optOutNotificationMethods": []
            }
        }),
    )
    .await
    .context("failed to initialize Codex app server")?;
    let mut lines = BufReader::new(stdout).lines();
    read_codex_response(&mut lines, 1).await?;
    write_codex_notification(&mut stdin, "initialized", Value::Object(Default::default())).await?;

    let mut next_request_id = 3;
    let (thread_response, resumed) = if let Some(thread_id) = request
        .session_id
        .as_deref()
        .filter(|thread_id| !thread_id.trim().is_empty())
    {
        write_codex_request(
            &mut stdin,
            2,
            "thread/resume",
            codex_thread_resume_params(request, permission, thread_id),
        )
        .await?;
        match read_codex_response(&mut lines, 2).await {
            Ok(response) => (response, true),
            Err(resume_error) => {
                let replay = request
                    .resume_unavailable_prompt
                    .as_deref()
                    .with_context(|| {
                        format!("failed to resume Codex thread {thread_id}: {resume_error:#}")
                    })?;
                anyhow::ensure!(
                    replay.chars().count() <= CODEX_APP_SERVER_TEXT_INPUT_LIMIT_CHARS,
                    "Codex durable thread resume unavailable; retry from Borg's durable journal after context compaction: {resume_error:#}"
                );
                tracing::warn!(
                    thread_id,
                    error = %resume_error,
                    "Codex durable thread was unavailable; starting from Borg's canonical replay"
                );
                write_codex_request(
                    &mut stdin,
                    next_request_id,
                    "thread/start",
                    codex_thread_start_params(request, permission),
                )
                .await?;
                let response = read_codex_response(&mut lines, next_request_id).await?;
                next_request_id = next_request_id.saturating_add(1);
                (response, false)
            }
        }
    } else {
        write_codex_request(
            &mut stdin,
            2,
            "thread/start",
            codex_thread_start_params(request, permission),
        )
        .await?;
        (read_codex_response(&mut lines, 2).await?, false)
    };
    let thread_id = thread_response
        .pointer("/result/thread/id")
        .or_else(|| thread_response.pointer("/thread/id"))
        .and_then(Value::as_str)
        .context("Codex app server did not return a thread id")?
        .to_string();

    Ok(StartedCodexProcess {
        process: PooledCodexProcess {
            child,
            stdin,
            lines,
            stderr: stderr_buffer,
            thread_id,
            next_request_id,
        },
        resumed,
    })
}

async fn run_pooled_codex_turn(
    process: &mut PooledCodexProcess,
    request: &ChatStreamRequest,
    mut controls: Option<mpsc::Receiver<ChatStreamControl>>,
    permission: LocalAgentPermission,
    events: &mpsc::Sender<ChatStreamEvent>,
    billing_mode: ProviderBillingMode,
) -> Result<bool> {
    let started_at = Instant::now();
    let turn_request_id = process.next_request_id;
    process.next_request_id = process.next_request_id.saturating_add(1);
    write_codex_request(
        &mut process.stdin,
        turn_request_id,
        "turn/start",
        codex_turn_start_params(request, permission, &process.thread_id),
    )
    .await?;
    let turn_response = read_codex_response(&mut process.lines, turn_request_id).await?;
    let turn_id = turn_response
        .pointer("/result/turn/id")
        .or_else(|| turn_response.pointer("/turn/id"))
        .and_then(Value::as_str)
        .context("Codex app server did not return a turn id")?
        .to_string();

    let mut text = String::new();
    let mut final_text = None;
    let session_id = Some(process.thread_id.clone());
    let mut usage = ProviderCallUsage::default();
    let mut pending_steers: HashMap<
        u64,
        tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    > = HashMap::new();
    let mut pending_approvals = HashMap::new();
    let mut turn_completed = false;
    let mut turn_status = None;
    let mut reasoning_state = CodexReasoningState::default();

    loop {
        tokio::select! {
            _ = events.closed() => {
                break;
            }
            line = process.lines.next_line() => {
                let Some(line) = line.context("failed to read pooled Codex output")? else {
                    let stderr = String::from_utf8_lossy(&process.stderr.lock().await).trim().to_string();
                    bail!("Codex app server closed unexpectedly{}", if stderr.is_empty() { String::new() } else { format!(": {stderr}") });
                };
                if line.trim().is_empty() {
                    continue;
                }
                let value = serde_json::from_str::<Value>(&line).unwrap_or_else(|_| Value::String(line.clone()));
                if let Some(response_id) = codex_response_id(&value) {
                    if let Some(ack) = pending_steers.remove(&response_id) {
                        let result = value.get("error")
                            .map(codex_rpc_error)
                            .map_or(Ok(()), Err);
                        let _ = ack.send(result);
                    }
                    continue;
                }
                emit_provider_event(events, &value).await;
                observe_codex_output_event(
                    events,
                    &value,
                    &mut reasoning_state,
                    &mut text,
                    &mut final_text,
                )
                .await;
                if let Some(event_usage) = event_usage_with_billing_mode(&value, billing_mode) {
                    usage = event_usage;
                }
                if codex_event_kind(&value).is_some_and(|method| method == "turn/completed") {
                    turn_status = codex_turn_status(&value).map(str::to_string);
                    turn_completed = true;
                    break;
                }
                if let Some(method) = codex_event_kind(&value)
                    && method.ends_with("/requestApproval")
                    && let Some(rpc_id) = value.get("id")
                {
                    let params = value.get("params").cloned().unwrap_or(Value::Null);
                    let approval_id = params
                        .get("approvalId")
                        .or_else(|| params.get("itemId"))
                        .and_then(Value::as_str)
                        .map(str::to_string)
                        .unwrap_or_else(|| rpc_id.to_string());
                    pending_approvals.insert(approval_id.clone(), rpc_id.clone());
                    events.send(ChatStreamEvent::ApprovalRequested {
                        approval_id,
                        title: method.to_string(),
                        detail: params.get("reason")
                            .and_then(Value::as_str)
                            .unwrap_or("Codex requested approval")
                            .to_string(),
                        command: params.get("command").and_then(Value::as_str).map(str::to_string),
                    }).await.ok();
                }
            }
            control = receive_control(&mut controls), if controls.is_some() => {
                let Some(control) = control else {
                    controls = None;
                    continue;
                };
                match control {
                    ChatStreamControl::Interrupt => {
                        let request_id = process.next_request_id;
                        process.next_request_id = process.next_request_id.saturating_add(1);
                        write_codex_request(
                            &mut process.stdin,
                            request_id,
                            "turn/interrupt",
                            serde_json::json!({"threadId": process.thread_id, "turnId": turn_id}),
                        ).await?;
                    }
                    ChatStreamControl::Steer {
                        client_user_message_id,
                        text: steer_text,
                        attachments,
                        ack,
                    } => {
                        let request_id = process.next_request_id;
                        process.next_request_id = process.next_request_id.saturating_add(1);
                        write_codex_request(
                            &mut process.stdin,
                            request_id,
                            "turn/steer",
                            codex_turn_steer_params(
                                &process.thread_id,
                                &turn_id,
                                &steer_text,
                                &attachments,
                                client_user_message_id.as_deref(),
                            ),
                        ).await?;
                        pending_steers.insert(request_id, ack);
                    }
                    ChatStreamControl::Approval { approval_id, decision } => {
                        if let Some(rpc_id) = pending_approvals.remove(&approval_id) {
                            let decision = match decision {
                                ChatApprovalDecision::ApproveOnce => "accept",
                                ChatApprovalDecision::ApproveSession => "acceptForSession",
                                ChatApprovalDecision::Reject => "decline",
                            };
                            write_codex_response(
                                &mut process.stdin,
                                rpc_id,
                                serde_json::json!({"decision": decision}),
                            ).await?;
                        }
                    }
                    ChatStreamControl::ProviderInteractionResponse { .. } => {}
                }
            }
        }
    }

    for (_, ack) in pending_steers {
        let _ = ack.send(Err(
            "Codex turn completed before the steer was delivered".to_string()
        ));
    }
    if !turn_completed {
        return Ok(false);
    }
    let final_text = codex_terminal_text(final_text, text, turn_status.as_deref())?;
    usage.duration_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
    if events
        .send(ChatStreamEvent::Done {
            final_text,
            usage: Some(usage),
            session_id,
        })
        .await
        .is_err()
    {
        return Ok(false);
    }
    Ok(true)
}

async fn run_codex_subscription_process(
    request: ChatStreamRequest,
    mut controls: Option<mpsc::Receiver<ChatStreamControl>>,
    permission: LocalAgentPermission,
    events: mpsc::Sender<ChatStreamEvent>,
) -> Result<()> {
    let started_at = Instant::now();
    let auth_home = restore_auth_home(request.provider_auth.as_ref())?;
    let billing_mode =
        provider_billing_mode(SubscriptionProvider::Codex, &request, auth_home.as_ref());
    let mut command = codex_app_server_command(&request, auth_home.as_ref())?;
    let mut child = command.spawn().with_context(|| {
        format!(
            "failed to start {}",
            SubscriptionProvider::Codex.executable()
        )
    })?;
    let mut stdin = child
        .stdin
        .take()
        .context("subscription stdin pipe missing")?;
    let stdout = child
        .stdout
        .take()
        .context("subscription stdout pipe missing")?;
    let stderr = child
        .stderr
        .take()
        .context("subscription stderr pipe missing")?;

    let mut stderr_task = tokio::spawn(async move {
        let mut stderr = stderr;
        let mut output = Vec::new();
        let _ = tokio::io::AsyncReadExt::read_to_end(&mut stderr, &mut output).await;
        output
    });

    stdin
        .write_all(
            serde_json::to_string(&serde_json::json!({
                "method": "initialize",
                "id": 1,
                "params": {
                    "clientInfo": {
                        "name": "borg",
                        "title": "Borg",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "capabilities": {
                        "experimentalApi": true,
                        "optOutNotificationMethods": []
                    }
                }
            }))?
            .as_bytes(),
        )
        .await
        .context("failed to initialize Codex app server")?;
    stdin.write_all(b"\n").await?;

    let mut lines = BufReader::new(stdout).lines();
    read_codex_response(&mut lines, 1).await?;
    write_codex_notification(&mut stdin, "initialized", Value::Object(Default::default())).await?;

    let thread_request_id = 2;
    write_codex_request(
        &mut stdin,
        thread_request_id,
        "thread/start",
        codex_thread_start_params(&request, permission),
    )
    .await?;
    let thread_response = read_codex_response(&mut lines, thread_request_id).await?;
    let thread_id = thread_response
        .pointer("/result/thread/id")
        .or_else(|| thread_response.pointer("/thread/id"))
        .and_then(Value::as_str)
        .context("Codex app server did not return a thread id")?
        .to_string();

    let turn_request_id = 3;
    write_codex_request(
        &mut stdin,
        turn_request_id,
        "turn/start",
        codex_turn_start_params(&request, permission, &thread_id),
    )
    .await?;
    let turn_response = read_codex_response(&mut lines, turn_request_id).await?;
    let turn_id = turn_response
        .pointer("/result/turn/id")
        .or_else(|| turn_response.pointer("/turn/id"))
        .and_then(Value::as_str)
        .context("Codex app server did not return a turn id")?
        .to_string();

    let mut text = String::new();
    let mut final_text = None;
    let session_id = Some(thread_id.clone());
    let mut usage = ProviderCallUsage::default();
    let mut next_request_id = 4_u64;
    let mut pending_steers: HashMap<
        u64,
        tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    > = HashMap::new();
    let mut pending_approvals = HashMap::new();
    let mut turn_completed = false;
    let mut turn_status = None;
    let mut reasoning_state = CodexReasoningState::default();

    loop {
        tokio::select! {
            // Dropping the consumer is how the session actor cancels a
            // subscription turn after a timeout or interrupt. Do not leave
            // the app-server child waiting on its pipe in that case; fall
            // through the normal bounded child cleanup below.
            _ = events.closed() => {
                turn_completed = true;
                break;
            }
            line = lines.next_line() => {
                let Some(line) = line.context("failed to read subscription output")? else {
                    break;
                };
                if line.trim().is_empty() {
                    continue;
                }
                let value = serde_json::from_str::<Value>(&line).unwrap_or_else(|_| Value::String(line.clone()));
                if let Some(response_id) = codex_response_id(&value) {
                    if let Some(ack) = pending_steers.remove(&response_id) {
                        let result = value.get("error")
                            .map(codex_rpc_error)
                            .map_or(Ok(()), Err);
                        let _ = ack.send(result);
                    }
                    continue;
                }
                emit_provider_event(&events, &value).await;
                observe_codex_output_event(
                    &events,
                    &value,
                    &mut reasoning_state,
                    &mut text,
                    &mut final_text,
                )
                .await;
                if let Some(event_usage) = event_usage_with_billing_mode(&value, billing_mode) {
                    usage = event_usage;
                }
                if let Some(method) = codex_event_kind(&value)
                    && method == "turn/completed"
                {
                    turn_status = codex_turn_status(&value).map(str::to_string);
                    turn_completed = true;
                    break;
                }
                if let Some(method) = codex_event_kind(&value)
                    && method.ends_with("/requestApproval")
                    && let Some(rpc_id) = value.get("id")
                {
                        let params = value.get("params").cloned().unwrap_or(Value::Null);
                        let approval_id = params
                            .get("approvalId")
                            .or_else(|| params.get("itemId"))
                            .and_then(Value::as_str)
                            .map(str::to_string)
                            .unwrap_or_else(|| rpc_id.to_string());
                        pending_approvals.insert(approval_id.clone(), rpc_id.clone());
                        events.send(ChatStreamEvent::ApprovalRequested {
                            approval_id,
                            title: method.to_string(),
                            detail: params.get("reason")
                                .and_then(Value::as_str)
                                .unwrap_or("Codex requested approval")
                                .to_string(),
                            command: params.get("command").and_then(Value::as_str).map(str::to_string),
                        }).await.ok();
                }
            }
            control = receive_control(&mut controls), if controls.is_some() => {
                let Some(control) = control else {
                    controls = None;
                    continue;
                };
                match control {
                    ChatStreamControl::Interrupt => {
                        write_codex_request(
                            &mut stdin,
                            next_request_id,
                            "turn/interrupt",
                            serde_json::json!({"threadId": thread_id, "turnId": turn_id}),
                        ).await?;
                        next_request_id += 1;
                    }
                    ChatStreamControl::Steer {
                        client_user_message_id,
                        text: steer_text,
                        attachments,
                        ack,
                    } => {
                        let request_id = next_request_id;
                        next_request_id += 1;
                        write_codex_request(
                            &mut stdin,
                            request_id,
                            "turn/steer",
                            codex_turn_steer_params(
                                &thread_id,
                                &turn_id,
                                &steer_text,
                                &attachments,
                                client_user_message_id.as_deref(),
                            ),
                        ).await?;
                        pending_steers.insert(request_id, ack);
                    }
                    ChatStreamControl::Approval { approval_id, decision } => {
                        if let Some(rpc_id) = pending_approvals.remove(&approval_id) {
                            let decision = match decision {
                                ChatApprovalDecision::ApproveOnce => "accept",
                                ChatApprovalDecision::ApproveSession => "acceptForSession",
                                ChatApprovalDecision::Reject => "decline",
                            };
                            write_codex_response(
                                &mut stdin,
                                rpc_id,
                                serde_json::json!({"decision": decision}),
                            ).await?;
                        }
                    }
                    ChatStreamControl::ProviderInteractionResponse { .. } => {}
                }
            }
        }
    }

    for (_, ack) in pending_steers {
        let _ = ack.send(Err(
            "Codex turn completed before the steer was delivered".to_string()
        ));
    }
    child.start_kill().ok();
    let status = tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .ok()
        .transpose()
        .context("failed waiting for subscription provider")?;
    let stderr = match tokio::time::timeout(Duration::from_secs(2), &mut stderr_task).await {
        Ok(Ok(output)) => output,
        _ => {
            stderr_task.abort();
            Vec::new()
        }
    };
    if !turn_completed && status.is_some_and(|status| !status.success()) {
        let detail = String::from_utf8_lossy(&stderr).trim().to_string();
        let status_detail = status.map_or_else(
            || "shutdown timed out".to_string(),
            |status| status.to_string(),
        );
        bail!(
            "{} exited with {}{}",
            SubscriptionProvider::Codex.executable(),
            status_detail,
            if detail.is_empty() {
                String::new()
            } else {
                format!(": {detail}")
            }
        );
    }

    let final_text = codex_terminal_text(final_text, text, turn_status.as_deref())?;

    usage.duration_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
    events
        .send(ChatStreamEvent::Done {
            final_text,
            usage: Some(usage),
            session_id,
        })
        .await
        .ok();
    Ok(())
}

async fn read_codex_response(
    lines: &mut tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    request_id: u64,
) -> Result<Value> {
    loop {
        let line = lines
            .next_line()
            .await
            .context("failed to read Codex app-server response")?
            .context("Codex app server closed before replying")?;
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&line)
            .with_context(|| format!("invalid Codex app-server JSON: {line}"))?;
        if codex_response_id(&value) == Some(request_id) {
            if value.get("error").is_some() {
                bail!(
                    "Codex app-server request failed: {}",
                    codex_rpc_error(&value)
                );
            }
            return Ok(value);
        }
    }
}

async fn write_codex_request(
    stdin: &mut ChildStdin,
    id: u64,
    method: &str,
    params: Value,
) -> Result<()> {
    let mut value = serde_json::json!({"id": id, "method": method, "params": params});
    write_codex_value(stdin, &mut value).await
}

async fn write_codex_notification(
    stdin: &mut ChildStdin,
    method: &str,
    params: Value,
) -> Result<()> {
    let mut value = serde_json::json!({"method": method, "params": params});
    write_codex_value(stdin, &mut value).await
}

async fn write_codex_response(stdin: &mut ChildStdin, id: Value, result: Value) -> Result<()> {
    let mut value = serde_json::json!({"id": id, "result": result});
    write_codex_value(stdin, &mut value).await
}

async fn write_codex_value(stdin: &mut ChildStdin, value: &mut Value) -> Result<()> {
    let mut line = serde_json::to_vec(value)?;
    line.push(b'\n');
    stdin
        .write_all(&line)
        .await
        .context("failed to write Codex app-server message")?;
    stdin
        .flush()
        .await
        .context("failed to flush Codex app-server message")?;
    Ok(())
}

fn codex_response_id(value: &Value) -> Option<u64> {
    value.get("id").and_then(Value::as_u64)
}

fn codex_rpc_error(value: &Value) -> String {
    value
        .pointer("/error/message")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| {
            value
                .get("error")
                .map_or_else(|| "unknown error".to_string(), Value::to_string)
        })
}

fn codex_app_server_command(
    request: &ChatStreamRequest,
    auth_home: Option<&TempDir>,
) -> Result<Command> {
    let mut command = Command::new("codex");
    command.args(["app-server", "--stdio"]);
    if let Some(auth_home) = auth_home {
        command.env("HOME", auth_home.path());
        let codex_home = crate::provider_auth::ensure_codex_home(auth_home.path())?;
        command.env("CODEX_HOME", codex_home);
    }
    if let Some(cwd) = request.working_directory.as_deref() {
        command.current_dir(cwd);
    }
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    Ok(command)
}

fn codex_thread_start_params(
    request: &ChatStreamRequest,
    permission: LocalAgentPermission,
) -> Value {
    let mut params = serde_json::json!({
        "model": request.model,
        "cwd": request.working_directory.as_ref().map(|path| path.to_string_lossy().into_owned()),
        "ephemeral": request.persist_session == Some(false),
        "approvalPolicy": match permission {
            LocalAgentPermission::FullAccess => "never",
            LocalAgentPermission::Auto | LocalAgentPermission::Manual => "on-request",
        },
        "sandbox": match permission {
            LocalAgentPermission::FullAccess => "danger-full-access",
            LocalAgentPermission::Auto | LocalAgentPermission::Manual => "workspace-write",
        },
        // Borg's policy augments Codex; it must not replace the model's own
        // base instructions. In particular, those base instructions govern
        // visible progress/commentary between tool calls. Replacing them made
        // long Codex turns collapse to reasoning + tools with no narration.
        "developerInstructions": request.system_prompt,
    });
    let mut config = serde_json::Map::new();
    if !request.mcp_external_servers.is_empty()
        && let Some(mcp_config) =
            codex_app_server_mcp_config(&request.mcp_external_servers).as_object()
    {
        config.extend(mcp_config.clone());
    }
    if request.web_search_allowed {
        config.insert(
            "features".to_string(),
            serde_json::json!({"web_search_request": true}),
        );
    }
    if !config.is_empty() {
        params["config"] = Value::Object(config);
    }
    params
}

fn codex_thread_resume_params(
    request: &ChatStreamRequest,
    permission: LocalAgentPermission,
    thread_id: &str,
) -> Value {
    let mut params = codex_thread_start_params(request, permission);
    let object = params
        .as_object_mut()
        .expect("Codex thread parameters are always an object");
    object.remove("ephemeral");
    object.insert("threadId".to_string(), Value::String(thread_id.to_string()));
    // Borg already owns and renders the durable transcript. Avoid returning a
    // potentially enormous duplicate turn array just to reopen the thread.
    object.insert("excludeTurns".to_string(), Value::Bool(true));
    params
}

fn codex_turn_start_params(
    request: &ChatStreamRequest,
    _permission: LocalAgentPermission,
    thread_id: &str,
) -> Value {
    serde_json::json!({
        "threadId": thread_id,
        "input": codex_user_input(&request.prompt, &request.attachments),
        "model": request.model,
        "cwd": request.working_directory.as_ref().map(|path| path.to_string_lossy().into_owned()),
        "effort": request.effort,
        "summary": "detailed",
        "outputSchema": request.output_schema,
    })
}

fn codex_turn_steer_params(
    thread_id: &str,
    turn_id: &str,
    text: &str,
    attachments: &[PathBuf],
    client_user_message_id: Option<&str>,
) -> Value {
    serde_json::json!({
        "threadId": thread_id,
        "expectedTurnId": turn_id,
        "input": codex_user_input(text, attachments),
        "clientUserMessageId": client_user_message_id,
    })
}

fn codex_user_input(text: &str, attachments: &[PathBuf]) -> Vec<Value> {
    let mut input = vec![serde_json::json!({"type": "text", "text": text})];
    input.extend(
        attachments
            .iter()
            .map(|path| serde_json::json!({"type": "localImage", "path": path.to_string_lossy()})),
    );
    input
}

fn codex_app_server_mcp_config(servers: &[ExternalMcpServer]) -> Value {
    let mut configs = serde_json::Map::new();
    for server in servers {
        if server.name.trim().is_empty() || configs.contains_key(&server.name) {
            continue;
        }
        let mut config = serde_json::json!({
            "command": server.command,
            "args": server.args,
            "env": server.env,
        });
        if server.name == "borg_agent" {
            // Borg's tools are part of the turn contract. Make Codex wait for
            // this local bridge before exposing the turn to the model instead
            // of racing its asynchronous MCP startup and emitting a noisy
            // "not ready for this step" resource failure.
            config["required"] = Value::Bool(true);
            config["startup_timeout_sec"] = Value::from(10);
        }
        if !server.allowed_tools.is_empty() {
            config["enabled_tools"] = Value::Array(
                server
                    .allowed_tools
                    .iter()
                    .filter_map(|tool| {
                        tool.strip_prefix(&format!("mcp__{}__", server.name))
                            .or_else(|| (!tool.starts_with("mcp__")).then_some(tool.as_str()))
                            .map(|tool| Value::String(tool.to_string()))
                    })
                    .collect(),
            );
        }
        configs.insert(server.name.clone(), config);
    }
    serde_json::json!({"mcp_servers": configs})
}

fn claude_command_args(
    request: &ChatStreamRequest,
    permission: LocalAgentPermission,
    mcp_config_path: Option<&Path>,
) -> Vec<String> {
    let mut args = vec![
        "--print".to_string(),
        // The adapter speaks Claude Code's realtime JSON input protocol. The
        // CLI defaults to plain-text stdin; without this flag it waits for a
        // complete text prompt/EOF and never consumes the initialize and user
        // frames that claude-agents writes.
        "--input-format".to_string(),
        "stream-json".to_string(),
        "--output-format".to_string(),
        "stream-json".to_string(),
        "--verbose".to_string(),
        "--include-partial-messages".to_string(),
    ];
    if permission == LocalAgentPermission::FullAccess {
        args.push("--dangerously-skip-permissions".to_string());
    } else {
        args.extend([
            "--permission-mode".to_string(),
            match permission {
                LocalAgentPermission::Auto => "auto".to_string(),
                LocalAgentPermission::Manual => "manual".to_string(),
                LocalAgentPermission::FullAccess => unreachable!(),
            },
        ]);
    }
    if request.persist_session == Some(false) {
        args.push("--no-session-persistence".to_string());
    }
    if let Some(model) = request
        .model
        .as_deref()
        .filter(|model| !model.trim().is_empty())
    {
        args.extend(["--model".to_string(), model.to_string()]);
    }
    if let Some(effort) = request
        .effort
        .as_deref()
        .filter(|effort| !effort.trim().is_empty())
    {
        args.extend(["--effort".to_string(), effort.to_string()]);
    }
    if let Some(session_id) = request
        .session_id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
    {
        args.extend(["--resume".to_string(), session_id.to_string()]);
    }
    if let Some(mcp_config_path) = mcp_config_path {
        args.extend([
            "--mcp-config".to_string(),
            mcp_config_path.to_string_lossy().into_owned(),
        ]);
    }
    args
}

fn build_claude_command_spec(
    request: &ChatStreamRequest,
    permission: LocalAgentPermission,
    auth_home: Option<&TempDir>,
    mcp_config_path: Option<&Path>,
) -> Result<claude_agents::CommandSpec> {
    let mut environment = Vec::new();
    if let Some(auth_home) = auth_home {
        environment.push(("HOME".to_string(), auth_home.path().display().to_string()));
    }
    Ok(claude_agents::CommandSpec {
        program: PathBuf::from("claude"),
        args: claude_command_args(request, permission, mcp_config_path),
        current_dir: request
            .working_directory
            .clone()
            .unwrap_or(std::env::current_dir().context("failed to resolve current directory")?),
        environment,
        environment_remove: Vec::new(),
    })
}

fn restore_auth_home(auth: Option<&ChatProviderAuth>) -> Result<Option<TempDir>> {
    let Some(auth) = auth else {
        return Ok(None);
    };
    let home = tempfile::tempdir().context("failed to create subscription auth home")?;
    crate::provider_auth::restore_bundle(auth.provider, &auth.bundle, home.path())
        .context("failed to restore subscription auth bundle")?;
    Ok(Some(home))
}

async fn receive_control(
    controls: &mut Option<mpsc::Receiver<ChatStreamControl>>,
) -> Option<ChatStreamControl> {
    controls.as_mut()?.recv().await
}

async fn emit_provider_event(events: &mpsc::Sender<ChatStreamEvent>, value: &Value) {
    let raw_kind = codex_event_kind(value).unwrap_or("event");
    let kind = codex_subscription_event_kind(value, raw_kind);
    let item = codex_event_item(value);
    events
        .send(ChatStreamEvent::ProviderEvent {
            kind,
            payload: value.clone(),
            raw_payload: Some(value.clone()),
            stream_channel: Some(
                item.and_then(|item| item.get("type"))
                    .and_then(Value::as_str)
                    .unwrap_or(raw_kind)
                    .to_string(),
            ),
            content_text: codex_event_delta(value),
            provider_item_id: item
                .and_then(|item| item.get("id"))
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| event_session_id(value)),
            tool_use_id: None,
            tool_name: None,
        })
        .await
        .ok();
}

// Keep Codex's event names in the same shape as its app-server events. The
// remote agent already uses the `method:item_type` suffix to recognize
// compaction lifecycle events and transient deltas.
fn codex_subscription_event_kind(value: &Value, raw_kind: &str) -> String {
    codex_event_item(value)
        .and_then(|item| item.get("type"))
        .and_then(Value::as_str)
        .filter(|_| {
            matches!(
                raw_kind.replace('.', "/").as_str(),
                "item/started" | "item/completed"
            )
        })
        .map(|item_type| {
            let method = raw_kind
                .replace('.', "/")
                .strip_prefix("item/")
                .map_or_else(|| raw_kind.to_string(), str::to_string);
            format!("item/{method}:{item_type}")
        })
        .unwrap_or_else(|| raw_kind.replace('.', "/"))
}

#[derive(Debug, Default)]
struct CodexReasoningState {
    /// Codex has emitted both incremental deltas and cumulative snapshots
    /// across app-server versions. Track each reasoning item separately and
    /// normalize either wire form into one incremental stream.
    streams: HashMap<String, String>,
}

impl CodexReasoningState {
    fn observe_delta(&mut self, value: &Value, incoming: &str) -> Option<String> {
        let key = codex_reasoning_stream_key(value);
        let previous = self.streams.entry(key).or_default();
        let emitted = normalize_provider_delta(previous, incoming);
        if incoming.starts_with(previous.as_str()) {
            previous.clear();
            previous.push_str(incoming);
        } else if previous.starts_with(incoming) {
            // A reconnect or lifecycle boundary replayed an older snapshot.
        } else {
            previous.push_str(&emitted);
        }
        if emitted.is_empty() {
            return None;
        }
        Some(emitted)
    }

    fn completion_suffix(&mut self, value: &Value, aggregate: &str) -> Option<String> {
        let mut key = codex_reasoning_stream_key(value);
        if !self.streams.contains_key(&key)
            && let Some(prefix) = key.strip_suffix(":-")
            && let Some(existing) = self
                .streams
                .keys()
                .find(|candidate| candidate.starts_with(&format!("{prefix}:")))
                .cloned()
        {
            key = existing;
        }
        let previous = self.streams.entry(key).or_default();
        let emitted = normalize_provider_delta(previous, aggregate);
        if aggregate.starts_with(previous.as_str()) {
            previous.clear();
            previous.push_str(aggregate);
        } else if previous.starts_with(aggregate) {
            // The completed item carried a shorter snapshot than the stream.
        } else {
            previous.push_str(&emitted);
        }
        if emitted.is_empty() {
            return None;
        }
        Some(emitted)
    }
}

fn normalize_provider_delta(previous: &str, incoming: &str) -> String {
    if incoming.is_empty() || incoming == previous || previous.starts_with(incoming) {
        return String::new();
    }
    if let Some(delta) = incoming.strip_prefix(previous) {
        return delta.to_string();
    }
    let overlap = longest_suffix_prefix_overlap(previous, incoming);
    incoming[overlap..].to_string()
}

fn longest_suffix_prefix_overlap(left: &str, right: &str) -> usize {
    let maximum = left.len().min(right.len());
    (1..=maximum)
        .rev()
        .find(|overlap| {
            left.is_char_boundary(left.len() - overlap)
                && right.is_char_boundary(*overlap)
                && left.as_bytes()[left.len() - overlap..] == right.as_bytes()[..*overlap]
        })
        .unwrap_or(0)
}

fn codex_reasoning_stream_key(value: &Value) -> String {
    let item_id = codex_event_item(value)
        .and_then(|item| item.get("id"))
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/params/itemId").and_then(Value::as_str))
        .unwrap_or("default");
    let summary_index = value
        .pointer("/params/summaryIndex")
        .or_else(|| codex_event_item(value).and_then(|item| item.get("summaryIndex")))
        .map_or_else(|| "-".to_string(), Value::to_string);
    format!("{item_id}:{summary_index}")
}

#[cfg(test)]
async fn emit_codex_events(events: &mpsc::Sender<ChatStreamEvent>, value: &Value) {
    emit_codex_events_with_state(events, value, &mut CodexReasoningState::default()).await;
}

async fn emit_codex_events_with_state(
    events: &mpsc::Sender<ChatStreamEvent>,
    value: &Value,
    reasoning_state: &mut CodexReasoningState,
) {
    let Some(kind) = codex_event_kind(value) else {
        return;
    };
    let normalized = kind.replace('.', "/").to_ascii_lowercase();

    match normalized.as_str() {
        // Text and reasoning deltas are emitted by the outer stream loop so
        // they are appended exactly once and reasoning never becomes answer
        // text. This function handles item lifecycle events only.
        "item/started" => {
            let Some(item) = codex_event_item(value) else {
                return;
            };
            let item_type = codex_item_type(item);
            if codex_item_is_non_rendered(item_type) {
                return;
            }
            let id = codex_item_id(item);
            if id.is_empty() {
                tracing::warn!(item_type, "Codex item started without an id");
                return;
            }
            let (name, input) = codex_tool_signature(item_type, item);
            events
                .send(ChatStreamEvent::ToolCall { id, name, input })
                .await
                .ok();
        }
        "item/completed" => {
            let Some(item) = codex_event_item(value) else {
                return;
            };
            let item_type = codex_item_type(item);
            if codex_item_is_agent_message(item_type) {
                if let Some(message) = codex_agent_message_text(item) {
                    // App-server streams the item deltas separately. Emit
                    // only the completed narration here; this also keeps the
                    // adapter correct for Codex versions that send only the
                    // aggregate completed item.
                    events
                        .send(ChatStreamEvent::Narration { text: message })
                        .await
                        .ok();
                }
                return;
            }
            if codex_item_is_reasoning(item_type) {
                if let Some(reasoning) = codex_reasoning_text(item)
                    && let Some(reasoning) = reasoning_state.completion_suffix(value, &reasoning)
                    && !reasoning.is_empty()
                {
                    events
                        .send(ChatStreamEvent::ReasoningDelta(reasoning))
                        .await
                        .ok();
                }
                events
                    .send(ChatStreamEvent::Phase {
                        name: "reasoning_completed".to_string(),
                        input: Value::Null,
                    })
                    .await
                    .ok();
                return;
            }
            if codex_item_is_non_rendered(item_type) {
                return;
            }
            let id = codex_item_id(item);
            if id.is_empty() {
                tracing::warn!(item_type, "Codex item completed without an id");
                return;
            }
            events
                .send(ChatStreamEvent::ToolResult {
                    tool_use_id: id,
                    output: codex_tool_output(item_type, item),
                    is_error: codex_tool_is_error(item_type, item),
                    input: codex_tool_completion_input(item_type, item),
                })
                .await
                .ok();
        }
        _ => {}
    }
}

async fn observe_codex_output_event(
    events: &mpsc::Sender<ChatStreamEvent>,
    value: &Value,
    reasoning_state: &mut CodexReasoningState,
    text: &mut String,
    final_text: &mut Option<String>,
) {
    emit_codex_events_with_state(events, value, reasoning_state).await;
    if let Some(delta) = codex_event_delta(value) {
        if codex_event_is_reasoning_delta(value) {
            if let Some(delta) = reasoning_state.observe_delta(value, &delta) {
                events
                    .send(ChatStreamEvent::ReasoningDelta(delta))
                    .await
                    .ok();
            }
        } else if codex_event_is_assistant_text_delta(value) {
            text.push_str(&delta);
            events.send(ChatStreamEvent::Delta(delta)).await.ok();
        }
    }
    if let Some(result) = codex_event_result(value) {
        *final_text = Some(result);
    }
}

fn codex_item_type(item: &Value) -> &str {
    item.get("type").and_then(Value::as_str).unwrap_or("")
}

fn codex_item_id(item: &Value) -> String {
    item.get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn codex_item_is_agent_message(item_type: &str) -> bool {
    matches!(
        item_type,
        "agentMessage" | "agent_message" | "assistantMessage" | "assistant_message"
    )
}

fn codex_item_is_reasoning(item_type: &str) -> bool {
    let normalized = item_type.replace('_', "").to_ascii_lowercase();
    normalized == "reasoning" || normalized.contains("reasoningsummary")
}

fn codex_item_is_non_rendered(item_type: &str) -> bool {
    codex_item_is_agent_message(item_type)
        || codex_item_is_reasoning(item_type)
        || matches!(
            item_type,
            "contextCompaction"
                | "context_compaction"
                | "userMessage"
                | "user_message"
                | "enteredReviewMode"
                | "entered_review_mode"
                | "exitedReviewMode"
                | "exited_review_mode"
                | "hookPrompt"
                | "hook_prompt"
        )
}

fn codex_tool_signature(item_type: &str, item: &Value) -> (String, Value) {
    match item_type {
        "commandExecution" | "command_execution" | "shellCommand" => {
            let command = item
                .get("command")
                .or_else(|| item.get("commandLine"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            (
                "command_execution".to_string(),
                serde_json::json!({"command": command}),
            )
        }
        "mcpToolCall" | "mcp_tool_call" => {
            let server = item
                .get("serverName")
                .or_else(|| item.get("server"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            let tool = item
                .get("toolName")
                .or_else(|| item.get("tool"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            let name = if server.is_empty() || tool.is_empty() {
                "mcp_tool_call".to_string()
            } else {
                format!("mcp__{server}__{tool}")
            };
            (
                name,
                item.get("input")
                    .or_else(|| item.get("arguments"))
                    .cloned()
                    .unwrap_or(Value::Null),
            )
        }
        "webSearch" | "web_search" | "webSearchCall" | "web_search_call" => (
            "web_search".to_string(),
            codex_search_query(item)
                .map(|query| serde_json::json!({"query": query}))
                .unwrap_or(Value::Null),
        ),
        "fileChange" | "file_change" | "patchApply" | "patch_apply" | "fileEdit" | "fileWrite" => {
            ("Edit".to_string(), codex_sanitized_item(item))
        }
        "plan" => ("todo_list".to_string(), codex_sanitized_item(item)),
        other => (other.to_string(), codex_sanitized_item(item)),
    }
}

fn codex_tool_completion_input(item_type: &str, item: &Value) -> Option<Value> {
    let (_, input) = codex_tool_signature(item_type, item);
    (!input.is_null()).then_some(input)
}

fn codex_search_query(item: &Value) -> Option<String> {
    item.pointer("/action/query")
        .or_else(|| item.get("query"))
        .and_then(Value::as_str)
        .filter(|query| !query.trim().is_empty())
        .map(str::to_string)
}

fn codex_sanitized_item(item: &Value) -> Value {
    let mut copy = item.clone();
    if let Some(object) = copy.as_object_mut() {
        for key in [
            "id",
            "type",
            "status",
            "aggregatedOutput",
            "aggregated_output",
            "output",
            "exitCode",
            "exit_code",
            "text",
            "content",
        ] {
            object.remove(key);
        }
    }
    copy
}

fn codex_agent_message_text(item: &Value) -> Option<String> {
    codex_text_field(item, &["text", "content"])
}

fn codex_reasoning_text(item: &Value) -> Option<String> {
    codex_text_field(item, &["summary", "text", "content", "reasoning"])
}

fn codex_text_field(item: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .find_map(|field| item.get(*field).and_then(codex_text_value))
        .filter(|text| !text.trim().is_empty())
}

fn codex_text_value(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Array(items) => {
            let text = items
                .iter()
                .filter_map(|item| {
                    item.get("text")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                        .or_else(|| codex_text_value(item))
                })
                .collect::<Vec<_>>()
                .join("");
            (!text.is_empty()).then_some(text)
        }
        Value::Object(object) => object
            .get("text")
            .or_else(|| object.get("summary"))
            .or_else(|| object.get("content"))
            .and_then(codex_text_value),
        _ => None,
    }
}

fn codex_tool_output(item_type: &str, item: &Value) -> String {
    if codex_tool_is_error(item_type, item)
        && let Some(error) = item.get("error").filter(|error| !error.is_null())
    {
        return error
            .get("message")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| error.to_string());
    }
    let value = match item_type {
        "commandExecution" | "command_execution" | "shellCommand" => item
            .get("aggregatedOutput")
            .or_else(|| item.get("aggregated_output"))
            .or_else(|| item.get("output")),
        _ => item
            .get("output")
            .or_else(|| item.get("result"))
            .or_else(|| item.get("content"))
            .or_else(|| item.get("text")),
    };
    value.map(codex_value_to_string).unwrap_or_default()
}

fn codex_value_to_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Array(items) => items
            .iter()
            .filter_map(|item| {
                item.get("text")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .or_else(|| item.as_str().map(str::to_string))
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => value.to_string(),
    }
}

fn codex_tool_is_error(item_type: &str, item: &Value) -> bool {
    if item
        .get("isError")
        .or_else(|| item.get("is_error"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return true;
    }
    if item
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("failed"))
    {
        return true;
    }
    if item.get("error").is_some_and(|error| !error.is_null()) {
        return true;
    }
    matches!(
        item_type,
        "commandExecution" | "command_execution" | "shellCommand"
    ) && item
        .get("exitCode")
        .or_else(|| item.get("exit_code"))
        .and_then(Value::as_i64)
        .is_some_and(|code| code != 0)
}

fn codex_event_delta(value: &Value) -> Option<String> {
    value
        .pointer("/params/delta")
        .or_else(|| value.get("delta"))
        .and_then(Value::as_str)
        .filter(|_| {
            codex_event_kind(value).is_some_and(|kind| kind.to_ascii_lowercase().contains("delta"))
        })
        .map(str::to_string)
}

fn codex_event_is_reasoning_delta(value: &Value) -> bool {
    codex_event_kind(value).is_some_and(|kind| {
        let kind = kind.to_ascii_lowercase();
        kind.contains("reasoning") && kind.contains("delta")
    })
}

fn codex_event_is_tool_output_delta(value: &Value) -> bool {
    let Some(kind) = codex_event_kind(value) else {
        return false;
    };
    let kind = kind.replace('.', "/").to_ascii_lowercase();
    if !kind.contains("delta") {
        return false;
    }
    if kind.contains("reasoning") || kind.contains("agentmessage") || kind.contains("agent_message")
    {
        return false;
    }
    let item_type = codex_event_item(value)
        .map(codex_item_type)
        .unwrap_or_default()
        .replace('_', "")
        .to_ascii_lowercase();
    item_type.contains("commandexecution")
        || item_type.contains("toolcall")
        || item_type.contains("toolresult")
        || kind.contains("commandexecution")
        || kind.contains("toolcall")
        || kind.contains("toolresult")
        || (kind.contains("output") && !kind.contains("message") && !kind.contains("output_text"))
}

fn codex_event_is_assistant_text_delta(value: &Value) -> bool {
    let Some(kind) = codex_event_kind(value) else {
        return false;
    };
    if codex_event_is_tool_output_delta(value) {
        return false;
    }
    let kind = kind.replace(['.', '_', '-'], "").to_ascii_lowercase();
    if !kind.contains("delta")
        || kind.contains("reasoning")
        || kind.contains("commandexecution")
        || kind.contains("toolcall")
        || kind.contains("toolresult")
        || kind.contains("filechange")
        || kind.contains("processoutput")
    {
        return false;
    }
    // Codex app-server's visible model stream is item/agentMessage/delta.
    // Keep the two common assistant spellings for older adapters, but never
    // treat an unknown output delta as assistant prose.
    kind.contains("agentmessage")
        || kind.contains("assistantmessage")
        || kind == "response/outputtext/delta"
}

fn codex_event_result(value: &Value) -> Option<String> {
    let is_completed = codex_event_kind(value)
        .is_some_and(|kind| matches!(kind.replace('.', "/").as_str(), "item/completed"));
    if !is_completed {
        if !codex_event_is_assistant_text_delta(value) {
            return None;
        }
        return value
            .get("result")
            .and_then(Value::as_str)
            .map(str::to_string);
    }
    codex_event_item(value)
        .filter(|item| codex_item_is_agent_message(codex_item_type(item)))
        .and_then(codex_agent_message_text)
}

fn codex_turn_status(value: &Value) -> Option<&str> {
    value
        .pointer("/params/turn/status")
        .or_else(|| value.pointer("/turn/status"))
        .and_then(Value::as_str)
}

fn codex_terminal_text(
    final_text: Option<String>,
    streamed_text: String,
    turn_status: Option<&str>,
) -> Result<String> {
    let text = final_text.unwrap_or(streamed_text);
    anyhow::ensure!(
        !text.trim().is_empty() || turn_status == Some("interrupted"),
        "{} returned an empty response",
        SubscriptionProvider::Codex.executable()
    );
    Ok(text)
}

fn codex_event_kind(value: &Value) -> Option<&str> {
    value
        .get("type")
        .or_else(|| value.get("method"))
        .and_then(Value::as_str)
}

fn codex_event_item(value: &Value) -> Option<&Value> {
    value.get("item").or_else(|| value.pointer("/params/item"))
}

fn event_session_id(value: &Value) -> Option<String> {
    [
        "/session_id",
        "/sessionId",
        "/thread_id",
        "/threadId",
        "/conversation_id",
    ]
    .into_iter()
    .filter_map(|pointer| value.pointer(pointer).and_then(Value::as_str))
    .find(|id| !id.is_empty())
    .map(str::to_string)
}

fn event_usage_with_billing_mode(
    value: &Value,
    billing_mode: ProviderBillingMode,
) -> Option<ProviderCallUsage> {
    let container = value
        .get("usage")
        .or_else(|| value.pointer("/event/usage"))
        .or_else(|| value.pointer("/params/usage"))
        .or_else(|| value.pointer("/params/tokenUsage"))
        .or_else(|| value.get("tokenUsage"))?;
    let app_server_usage =
        value.pointer("/params/tokenUsage").is_some() || value.get("tokenUsage").is_some();
    let usage = if app_server_usage {
        container
            .get("last")
            .or_else(|| container.get("total"))
            .unwrap_or(container)
    } else {
        container
    };
    let cached_input_tokens = usage
        .get("cached_input_tokens")
        .or_else(|| usage.get("cachedInputTokens"))
        .or_else(|| usage.pointer("/input_tokens_details/cached_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let cache_creation_input_tokens = usage
        .get("cache_write_input_tokens")
        .or_else(|| usage.get("cacheWriteInputTokens"))
        .or_else(|| usage.get("cache_creation_input_tokens"))
        .or_else(|| usage.get("cacheCreationInputTokens"))
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let reported_input_tokens = usage
        .get("input_tokens")
        .or_else(|| usage.get("inputTokens"))
        .and_then(Value::as_u64)
        .unwrap_or_default();
    // `codex exec --json` reports input_tokens as the whole input bucket and
    // exposes cached and cache-write portions alongside it. App-server's
    // TokenUsageBreakdown reports inputTokens as its own exclusive bucket.
    let input_tokens = if app_server_usage {
        reported_input_tokens
    } else {
        reported_input_tokens
            .saturating_sub(cached_input_tokens)
            .saturating_sub(cache_creation_input_tokens)
    };
    let output_tokens = usage
        .get("output_tokens")
        .or_else(|| usage.get("outputTokens"))
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let reported_total_tokens = usage
        .get("total_tokens")
        .or_else(|| usage.get("totalTokens"))
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let prompt_tokens = if app_server_usage {
        reported_input_tokens
    } else {
        reported_input_tokens.max(
            input_tokens
                .saturating_add(cached_input_tokens)
                .saturating_add(cache_creation_input_tokens),
        )
    };
    let total_tokens = if reported_total_tokens > 0 {
        reported_total_tokens
    } else {
        prompt_tokens.saturating_add(output_tokens)
    };
    let context_window_tokens = container
        .get("modelContextWindow")
        .or_else(|| container.get("model_context_window"))
        .or_else(|| container.get("contextWindowTokens"))
        .or_else(|| container.get("context_window_tokens"))
        .or_else(|| value.pointer("/params/modelContextWindow"))
        .or_else(|| value.pointer("/params/model_context_window"))
        .and_then(Value::as_u64);
    let cost_microusd = usage
        .get("cost_microusd")
        .or_else(|| usage.get("costMicrousd"))
        .or_else(|| usage.get("cost_usd"))
        .or_else(|| usage.get("costUsd"))
        .and_then(|value| {
            value.as_u64().or_else(|| {
                value
                    .as_f64()
                    .filter(|cost| cost.is_finite() && *cost >= 0.0)
                    .map(|cost| (cost * 1_000_000.0).round() as u64)
            })
        });
    Some(ProviderCallUsage {
        input_tokens,
        cached_input_tokens,
        cache_creation_input_tokens,
        output_tokens,
        total_tokens,
        context_tokens: (total_tokens > 0).then_some(total_tokens),
        context_window_tokens,
        cost_microusd,
        cost_basis: usage_cost_basis(cost_microusd, billing_mode),
        ..ProviderCallUsage::default()
    })
}

fn usage_cost_basis(
    cost_microusd: Option<u64>,
    billing_mode: ProviderBillingMode,
) -> crate::runtime::CostBasis {
    match (cost_microusd, billing_mode) {
        (Some(_), ProviderBillingMode::Subscription) => {
            crate::runtime::CostBasis::SubscriptionEquivalent
        }
        (Some(_), ProviderBillingMode::ApiKey | ProviderBillingMode::Unknown) => {
            crate::runtime::CostBasis::ProviderReported
        }
        (None, _) => crate::runtime::CostBasis::Unavailable,
    }
}

fn provider_billing_mode(
    provider: SubscriptionProvider,
    request: &ChatStreamRequest,
    auth_home: Option<&TempDir>,
) -> ProviderBillingMode {
    match provider {
        SubscriptionProvider::Claude => {
            // Claude Code gives explicit API-key environment variables
            // precedence over its OAuth credential file. Mirror that choice
            // for billing attribution without ever logging the key.
            if has_nonempty_env("ANTHROPIC_API_KEY") || has_nonempty_env("ANTHROPIC_AUTH_TOKEN") {
                return ProviderBillingMode::ApiKey;
            }
            let credentials_path = auth_home
                .map(|home| home.path().join(".claude/.credentials.json"))
                .or_else(|| {
                    std::env::var_os("HOME")
                        .map(PathBuf::from)
                        .map(|home| home.join(".claude/.credentials.json"))
                });
            if request
                .provider_auth
                .as_ref()
                .is_some_and(|auth| auth.provider == ProviderAuthProvider::Claude)
                || credentials_path.is_some_and(|path| path.is_file())
            {
                ProviderBillingMode::Subscription
            } else {
                ProviderBillingMode::Unknown
            }
        }
        SubscriptionProvider::Codex => {
            if has_nonempty_env("OPENAI_API_KEY") {
                return ProviderBillingMode::ApiKey;
            }
            let auth_path = auth_home
                .map(|home| home.path().join(".codex/auth.json"))
                .or_else(|| {
                    std::env::var_os("CODEX_HOME")
                        .map(PathBuf::from)
                        .map(|home| home.join("auth.json"))
                })
                .or_else(|| {
                    std::env::var_os("HOME")
                        .map(PathBuf::from)
                        .map(|home| home.join(".codex/auth.json"))
                });
            let Some(auth_path) = auth_path else {
                return ProviderBillingMode::Unknown;
            };
            let Ok(contents) = std::fs::read_to_string(auth_path) else {
                return ProviderBillingMode::Unknown;
            };
            let Ok(auth_json) = serde_json::from_str::<Value>(&contents) else {
                return ProviderBillingMode::Unknown;
            };
            if crate::provider_auth::auth_json_holds_chatgpt_session(&auth_json) {
                ProviderBillingMode::Subscription
            } else {
                ProviderBillingMode::ApiKey
            }
        }
    }
}

fn has_nonempty_env(name: &str) -> bool {
    std::env::var_os(name).is_some_and(|value| !value.is_empty())
}

impl SubscriptionProvider {
    fn executable(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claude_command_preserves_subscription_flags() {
        let request = ChatStreamRequest {
            prompt: "hello".to_string(),
            lifecycle_key: None,
            owner_session_id: None,
            client_user_message_id: None,
            attachments: Vec::new(),
            model: None,
            effort: None,
            fast: false,
            system_prompt: "system".to_string(),
            output_schema: None,
            mcp_owner_id: None,
            mcp_allowed_scopes: Vec::new(),
            mcp_user_id: None,
            mcp_external_servers: Vec::new(),
            mcp_api_token: None,
            provider_auth: None,
            git_credentials: Vec::new(),
            working_directory: None,
            session_id: Some("session-1".to_string()),
            provider_channel: ProviderChannel::Direct,
            persist_session: Some(false),
            web_search_allowed: false,
            resume_unavailable_prompt: None,
        };
        assert_eq!(
            claude_command_args(&request, LocalAgentPermission::Manual, None),
            vec![
                "--print",
                "--input-format",
                "stream-json",
                "--output-format",
                "stream-json",
                "--verbose",
                "--include-partial-messages",
                "--permission-mode",
                "manual",
                "--no-session-persistence",
                "--resume",
                "session-1",
            ]
        );
    }

    #[test]
    fn subscription_commands_attach_provider_mcp_config() {
        let root = tempfile::tempdir().expect("temporary provider home");
        let server = ExternalMcpServer {
            name: "borg_agent".to_string(),
            command: "/bin/borg".to_string(),
            args: vec!["__agent-mcp".to_string()],
            env: std::collections::BTreeMap::from([(
                "BORG_AGENT_TOOL_SOCKET".to_string(),
                "/tmp/borg.sock".to_string(),
            )]),
            allowed_tools: vec![
                "mcp__borg_agent__get_goal".to_string(),
                "mcp__borg_agent__update_plan".to_string(),
            ],
        };
        let request = ChatStreamRequest {
            prompt: "hello".to_string(),
            lifecycle_key: None,
            owner_session_id: None,
            client_user_message_id: None,
            attachments: Vec::new(),
            model: None,
            effort: None,
            fast: false,
            system_prompt: "system".to_string(),
            output_schema: None,
            mcp_owner_id: None,
            mcp_allowed_scopes: Vec::new(),
            mcp_user_id: None,
            mcp_external_servers: vec![server.clone()],
            mcp_api_token: None,
            provider_auth: None,
            git_credentials: Vec::new(),
            working_directory: Some(root.path().to_path_buf()),
            session_id: None,
            provider_channel: ProviderChannel::Direct,
            persist_session: Some(false),
            web_search_allowed: false,
            resume_unavailable_prompt: None,
        };

        let codex_config = codex_thread_start_params(&request, LocalAgentPermission::FullAccess);
        let borg_agent_config = codex_config
            .get("config")
            .and_then(|value| value.get("mcp_servers"))
            .and_then(|value| value.get("borg_agent"))
            .expect("Borg agent Codex config");
        assert_eq!(borg_agent_config.get("required"), Some(&Value::Bool(true)));
        assert_eq!(
            borg_agent_config.get("startup_timeout_sec"),
            Some(&Value::from(10))
        );
        assert_eq!(
            codex_config.get("developerInstructions"),
            Some(&Value::String("system".to_string()))
        );
        assert!(codex_config.get("baseInstructions").is_none());
        assert_eq!(
            codex_config
                .get("config")
                .and_then(|value| value.get("mcp_servers"))
                .and_then(|value| value.get("borg_agent"))
                .and_then(|value| value.get("enabled_tools"))
                .and_then(Value::as_array)
                .expect("Codex enabled tools")
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>(),
            ["get_goal", "update_plan"]
        );
        let codex_command = codex_app_server_command(&request, None).expect("Codex command");
        let codex_args = codex_command
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(
            codex_args
                .windows(2)
                .any(|args| args[0] == "app-server" && args[1] == "--stdio")
        );

        let mcp_setup =
            prepare_external_provider_mcp(root.path(), &[server]).expect("Claude MCP config");
        let claude_args = claude_command_args(
            &request,
            LocalAgentPermission::FullAccess,
            mcp_setup.claude_config_path.as_deref(),
        );
        let config_path = mcp_setup
            .claude_config_path
            .expect("Claude MCP path")
            .to_string_lossy()
            .into_owned();
        assert!(
            claude_args
                .windows(2)
                .any(|args| { args[0] == "--mcp-config" && args[1] == config_path })
        );
        let claude_config = serde_json::from_slice::<serde_json::Value>(
            &std::fs::read(&config_path).expect("Claude MCP config contents"),
        )
        .expect("Claude MCP config should be valid JSON");
        assert_eq!(
            claude_config
                .get("mcpServers")
                .and_then(|value| value.get("borg_agent"))
                .and_then(|value| value.get("env"))
                .and_then(|value| value.get("BORG_AGENT_TOOL_SOCKET"))
                .and_then(serde_json::Value::as_str),
            Some("/tmp/borg.sock")
        );
    }

    #[test]
    fn claude_agent_events_map_to_borg_contract() {
        let event = claude_agents::ChatStreamEvent::Done {
            final_text: "done".to_string(),
            usage: Some(claude_agents::ProviderCallUsage {
                input_tokens: 12,
                cached_input_tokens: 4,
                output_tokens: 8,
                total_tokens: 20,
                ..Default::default()
            }),
            session_id: Some("session-1".to_string()),
        };
        assert!(matches!(
            map_claude_event(event, ProviderBillingMode::Unknown),
            ChatStreamEvent::Done {
                final_text,
                usage: Some(ProviderCallUsage {
                    input_tokens: 12,
                    cached_input_tokens: 4,
                    output_tokens: 8,
                    total_tokens: 20,
                    ..
                }),
                session_id: Some(session_id),
            } if final_text == "done" && session_id == "session-1"
        ));
    }

    #[test]
    fn claude_command_lifecycle_correlates_steers_at_the_provider_boundary() {
        let correlation = StdMutex::new(ClaudeSteerCorrelation {
            pending: VecDeque::from(["borg-message-1".to_string()]),
            commands: HashMap::new(),
        });
        let command_uuid = "claude-command-1";
        let queued = map_claude_event_with_correlation(
            claude_agents::ChatStreamEvent::ProviderEvent {
                kind: "claude.command_lifecycle".to_string(),
                payload: serde_json::json!({"type": "command_lifecycle"}),
                raw_payload: Some(serde_json::json!({
                    "type": "command_lifecycle",
                    "command_uuid": command_uuid,
                    "state": "queued",
                    "uuid": "event-queued",
                })),
                stream_channel: None,
                content_text: None,
                provider_item_id: None,
                tool_use_id: None,
                tool_name: None,
            },
            ProviderBillingMode::Unknown,
            Some(&correlation),
        );
        assert!(matches!(
            queued,
            ChatStreamEvent::ProviderEvent { ref payload, .. }
                if payload.get("command_uuid").and_then(Value::as_str) == Some(command_uuid)
                    && payload.get("state").and_then(Value::as_str) == Some("queued")
                    && payload.get("client_user_message_id").and_then(Value::as_str)
                        == Some("borg-message-1")
        ));

        let started = map_claude_event_with_correlation(
            claude_agents::ChatStreamEvent::ProviderEvent {
                kind: "claude.command_lifecycle".to_string(),
                payload: serde_json::json!({"type": "command_lifecycle"}),
                raw_payload: Some(serde_json::json!({
                    "type": "command_lifecycle",
                    "command_uuid": command_uuid,
                    "state": "started",
                    "uuid": "event-started",
                })),
                stream_channel: None,
                content_text: None,
                provider_item_id: None,
                tool_use_id: None,
                tool_name: None,
            },
            ProviderBillingMode::Unknown,
            Some(&correlation),
        );
        assert!(matches!(
            started,
            ChatStreamEvent::ProviderEvent { ref payload, .. }
                if payload.get("client_user_message_id").and_then(Value::as_str)
                    == Some("borg-message-1")
        ));
        assert!(
            correlation
                .lock()
                .unwrap()
                .commands
                .contains_key(command_uuid)
        );
    }

    #[test]
    fn codex_usage_preserves_cached_input_counters() {
        let usage = event_usage_with_billing_mode(
            &serde_json::json!({
            "usage": {
                "input_tokens": 222_000,
                "cached_input_tokens": 210_000,
                "cache_write_input_tokens": 1_024,
                "output_tokens": 800,
                "total_tokens": 222_800
            }
            }),
            ProviderBillingMode::Unknown,
        )
        .expect("Codex usage");
        assert_eq!(usage.input_tokens, 10_976);
        assert_eq!(usage.cached_input_tokens, 210_000);
        assert_eq!(usage.cache_creation_input_tokens, 1_024);
        assert_eq!(usage.total_tokens, 222_800);
        assert_eq!(usage.context_tokens, Some(222_800));
    }

    #[test]
    fn subscription_and_api_key_usage_have_distinct_billing_bases() {
        assert_eq!(
            usage_cost_basis(Some(1_000), ProviderBillingMode::Subscription),
            crate::runtime::CostBasis::SubscriptionEquivalent
        );
        assert_eq!(
            usage_cost_basis(Some(1_000), ProviderBillingMode::ApiKey),
            crate::runtime::CostBasis::ProviderReported
        );
        assert_eq!(
            usage_cost_basis(Some(1_000), ProviderBillingMode::Unknown),
            crate::runtime::CostBasis::ProviderReported
        );
    }

    #[test]
    fn codex_usage_derives_context_when_exec_omits_total_tokens() {
        let usage = event_usage_with_billing_mode(
            &serde_json::json!({
            "usage": {
                "input_tokens": 222_000,
                "cached_input_tokens": 210_000,
                "cache_write_input_tokens": 1_024,
                "output_tokens": 800
            }
            }),
            ProviderBillingMode::Unknown,
        )
        .expect("Codex usage");
        assert_eq!(usage.total_tokens, 222_800);
        assert_eq!(usage.context_tokens, Some(222_800));
        assert_eq!(usage.context_window_tokens, None);
    }

    #[test]
    fn codex_app_server_usage_keeps_breakdown_buckets_and_context_window() {
        let usage = event_usage_with_billing_mode(
            &serde_json::json!({
            "method": "thread/tokenUsage/updated",
            "params": {
                "tokenUsage": {
                    "last": {
                        "inputTokens": 6_741,
                        "cachedInputTokens": 0,
                        "cacheWriteInputTokens": 6_738,
                        "outputTokens": 5,
                        "totalTokens": 6_746
                    },
                    "modelContextWindow": 258_400
                }
            }
            }),
            ProviderBillingMode::Unknown,
        )
        .expect("Codex app-server usage");
        assert_eq!(usage.input_tokens, 6_741);
        assert_eq!(usage.cache_creation_input_tokens, 6_738);
        assert_eq!(usage.total_tokens, 6_746);
        assert_eq!(usage.context_tokens, Some(6_746));
        assert_eq!(usage.context_window_tokens, Some(258_400));
    }

    #[test]
    fn codex_app_server_reasoning_deltas_are_not_answer_deltas() {
        let value = serde_json::json!({
            "method": "item/reasoning/summaryTextDelta",
            "params": {"delta": "checking the plan"}
        });
        assert_eq!(
            codex_event_delta(&value).as_deref(),
            Some("checking the plan")
        );
        assert!(codex_event_is_reasoning_delta(&value));
    }

    #[test]
    fn codex_command_output_deltas_are_not_answer_deltas() {
        let value = serde_json::json!({
            "method": "item/commandExecution/outputDelta",
            "params": {
                "delta": "Finished cargo test",
                "item": {"id": "command-1", "type": "commandExecution"}
            }
        });
        assert_eq!(
            codex_event_delta(&value).as_deref(),
            Some("Finished cargo test")
        );
        assert!(codex_event_is_tool_output_delta(&value));
        assert!(!codex_event_is_assistant_text_delta(&value));
    }

    #[test]
    fn only_known_assistant_delta_channels_enter_the_answer() {
        let assistant = serde_json::json!({
            "method": "item/agentMessage/delta",
            "params": {"delta": "answer", "itemId": "message-1"}
        });
        let process = serde_json::json!({
            "method": "process/outputDelta",
            "params": {"delta": "tool output", "processId": "process-1"}
        });
        let file_change = serde_json::json!({
            "method": "item/fileChange/outputDelta",
            "params": {"delta": "patch output", "itemId": "file-1"}
        });
        assert!(codex_event_is_assistant_text_delta(&assistant));
        assert!(!codex_event_is_assistant_text_delta(&process));
        assert!(!codex_event_is_assistant_text_delta(&file_change));
    }

    #[test]
    fn cumulative_reasoning_deltas_are_reduced_to_one_stream() {
        let mut state = CodexReasoningState::default();
        let first = serde_json::json!({
            "method": "item/reasoning/summaryTextDelta",
            "params": {"delta": "Considering code modifications", "itemId": "reasoning-1", "summaryIndex": 0}
        });
        let second = serde_json::json!({
            "method": "item/reasoning/summaryTextDelta",
            "params": {"delta": "Considering code modifications\nI’m checking the repository", "itemId": "reasoning-1", "summaryIndex": 0}
        });
        let duplicate = second.clone();
        assert_eq!(
            state.observe_delta(
                &first,
                first.pointer("/params/delta").unwrap().as_str().unwrap()
            ),
            Some("Considering code modifications".to_string())
        );
        assert_eq!(
            state.observe_delta(
                &second,
                second.pointer("/params/delta").unwrap().as_str().unwrap()
            ),
            Some("\nI’m checking the repository".to_string())
        );
        assert_eq!(
            state.observe_delta(
                &duplicate,
                duplicate
                    .pointer("/params/delta")
                    .unwrap()
                    .as_str()
                    .unwrap()
            ),
            None
        );
    }

    #[test]
    fn overlapping_reasoning_chunks_do_not_replay_the_previous_suffix() {
        let mut state = CodexReasoningState::default();
        let first = serde_json::json!({
            "method": "item/reasoning/summaryTextDelta",
            "params": {"delta": "checking the repository", "itemId": "reasoning-1", "summaryIndex": 0}
        });
        let second = serde_json::json!({
            "method": "item/reasoning/summaryTextDelta",
            "params": {"delta": "repository for duplicate output", "itemId": "reasoning-1", "summaryIndex": 0}
        });
        assert_eq!(
            state.observe_delta(&first, "checking the repository"),
            Some("checking the repository".to_string())
        );
        assert_eq!(
            state.observe_delta(&second, "repository for duplicate output"),
            Some(" for duplicate output".to_string())
        );
    }

    #[tokio::test]
    async fn codex_reasoning_completion_does_not_replay_streamed_summary() {
        let (sender, mut receiver) = mpsc::channel(8);
        let mut state = CodexReasoningState::default();
        let delta = serde_json::json!({
            "method": "item/reasoning/summaryTextDelta",
            "params": {
                "delta": "checking the plan",
                "itemId": "reasoning-1",
                "summaryIndex": 0
            }
        });
        state.observe_delta(
            &delta,
            delta.pointer("/params/delta").unwrap().as_str().unwrap(),
        );
        emit_codex_events_with_state(
            &sender,
            &serde_json::json!({
                "type": "item.completed",
                "item": {
                    "id": "reasoning-1",
                    "type": "reasoning",
                    "summary": ["checking the plan"]
                }
            }),
            &mut state,
        )
        .await;

        assert!(matches!(
            receiver.recv().await,
            Some(ChatStreamEvent::Phase { name, .. }) if name == "reasoning_completed"
        ));
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn codex_session_id_is_extracted() {
        assert_eq!(
            event_session_id(&serde_json::json!({"thread_id":"thread-1"})),
            Some("thread-1".to_string())
        );
    }

    #[test]
    fn interrupted_codex_turn_accepts_an_empty_terminal_message() {
        let completed = serde_json::json!({
            "method": "turn/completed",
            "params": {"turn": {"status": "interrupted"}}
        });

        assert_eq!(codex_turn_status(&completed), Some("interrupted"));
        assert_eq!(
            codex_terminal_text(None, String::new(), codex_turn_status(&completed)).unwrap(),
            ""
        );
        assert!(codex_terminal_text(None, String::new(), Some("completed")).is_err());
    }

    #[test]
    fn codex_resume_reopens_persisted_thread_without_returning_its_history() {
        let request = ChatStreamRequest {
            prompt: "next".to_string(),
            lifecycle_key: None,
            owner_session_id: None,
            client_user_message_id: None,
            attachments: Vec::new(),
            model: Some("gpt-5.6-luna".to_string()),
            effort: Some("max".to_string()),
            fast: false,
            system_prompt: "Borg policy".to_string(),
            output_schema: None,
            mcp_owner_id: None,
            mcp_allowed_scopes: Vec::new(),
            mcp_user_id: None,
            mcp_external_servers: vec![ExternalMcpServer {
                name: "borg_agent".to_string(),
                command: "/opt/borg/replacement-borg".to_string(),
                args: vec!["__agent-mcp".to_string()],
                env: Default::default(),
                allowed_tools: vec!["mcp__borg_agent__get_goal".to_string()],
            }],
            mcp_api_token: None,
            provider_auth: None,
            git_credentials: Vec::new(),
            working_directory: Some(PathBuf::from("/tmp/workspace")),
            session_id: Some("thread-1".to_string()),
            provider_channel: ProviderChannel::Direct,
            persist_session: Some(true),
            web_search_allowed: true,
            resume_unavailable_prompt: Some("full replay + next".to_string()),
        };

        let params =
            codex_thread_resume_params(&request, LocalAgentPermission::FullAccess, "thread-1");
        assert_eq!(params.get("threadId"), Some(&Value::from("thread-1")));
        assert_eq!(params.get("excludeTurns"), Some(&Value::Bool(true)));
        assert!(params.get("ephemeral").is_none());
        assert_eq!(
            params.get("developerInstructions"),
            Some(&Value::from("Borg policy"))
        );
        assert_eq!(
            params.pointer("/config/mcp_servers/borg_agent/command"),
            Some(&Value::from("/opt/borg/replacement-borg"))
        );
        assert_eq!(
            params.pointer("/config/mcp_servers/borg_agent/args/0"),
            Some(&Value::from("__agent-mcp"))
        );
    }

    #[test]
    fn codex_subscription_item_kind_preserves_item_type() {
        let value = serde_json::json!({
            "type": "item.completed",
            "item": {"id": "item-1", "type": "command_execution"}
        });
        assert_eq!(
            codex_subscription_event_kind(&value, "item.completed"),
            "item/completed:command_execution"
        );
    }

    #[tokio::test]
    async fn codex_subscription_items_become_tool_events() {
        let (sender, mut receiver) = mpsc::channel(8);
        emit_codex_events(
            &sender,
            &serde_json::json!({
                "type": "item.started",
                "item": {
                    "id": "item-1",
                    "type": "command_execution",
                    "command": "/usr/bin/bash -lc pwd"
                }
            }),
        )
        .await;
        emit_codex_events(
            &sender,
            &serde_json::json!({
                "type": "item.completed",
                "item": {
                    "id": "item-1",
                    "type": "command_execution",
                    "command": "/usr/bin/bash -lc pwd",
                    "aggregated_output": "/home/shulgin/borg-cli\n",
                    "exit_code": 0,
                    "status": "completed"
                }
            }),
        )
        .await;

        assert!(matches!(
            receiver.recv().await,
            Some(ChatStreamEvent::ToolCall { id, name, input })
                if id == "item-1"
                    && name == "command_execution"
                    && input["command"] == "/usr/bin/bash -lc pwd"
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(ChatStreamEvent::ToolResult {
                tool_use_id,
                output,
                is_error: false,
                ..
            }) if tool_use_id == "item-1" && output == "/home/shulgin/borg-cli\n"
        ));
    }

    #[tokio::test]
    async fn codex_completed_agent_items_are_committed_as_segments() {
        let (sender, mut receiver) = mpsc::channel(8);
        emit_codex_events(
            &sender,
            &serde_json::json!({
                "type": "item.completed",
                "item": {"id": "item-2", "type": "agent_message", "text": "done"}
            }),
        )
        .await;

        assert!(matches!(
            receiver.recv().await,
            Some(ChatStreamEvent::Narration { text }) if text == "done"
        ));
    }

    #[tokio::test]
    async fn codex_commentary_between_tool_calls_remains_visible_and_ordered() {
        let (sender, mut receiver) = mpsc::channel(16);
        let mut reasoning_state = CodexReasoningState::default();
        let mut text = String::new();
        let mut final_text = None;
        let values = [
            serde_json::json!({
                "method": "item/agentMessage/delta",
                "params": {"delta": "Before the tool."}
            }),
            serde_json::json!({
                "method": "item/completed",
                "params": {"item": {
                    "id": "message-1",
                    "type": "agentMessage",
                    "text": "Before the tool.",
                    "phase": "commentary"
                }}
            }),
            serde_json::json!({
                "method": "item/started",
                "params": {"item": {
                    "id": "command-1",
                    "type": "commandExecution",
                    "command": "/usr/bin/bash -lc pwd"
                }}
            }),
            serde_json::json!({
                "method": "item/completed",
                "params": {"item": {
                    "id": "command-1",
                    "type": "commandExecution",
                    "command": "/usr/bin/bash -lc pwd",
                    "aggregatedOutput": "/workspace\n",
                    "exitCode": 0,
                    "status": "completed"
                }}
            }),
            serde_json::json!({
                "method": "item/agentMessage/delta",
                "params": {"delta": "After the tool."}
            }),
            serde_json::json!({
                "method": "item/completed",
                "params": {"item": {
                    "id": "message-2",
                    "type": "agentMessage",
                    "text": "After the tool.",
                    "phase": "commentary"
                }}
            }),
        ];
        for value in &values {
            observe_codex_output_event(
                &sender,
                value,
                &mut reasoning_state,
                &mut text,
                &mut final_text,
            )
            .await;
        }

        assert!(matches!(
            receiver.recv().await,
            Some(ChatStreamEvent::Delta(text)) if text == "Before the tool."
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(ChatStreamEvent::Narration { text }) if text == "Before the tool."
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(ChatStreamEvent::ToolCall { id, .. }) if id == "command-1"
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(ChatStreamEvent::ToolResult { tool_use_id, .. }) if tool_use_id == "command-1"
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(ChatStreamEvent::Delta(text)) if text == "After the tool."
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(ChatStreamEvent::Narration { text }) if text == "After the tool."
        ));
        assert_eq!(text, "Before the tool.After the tool.");
        assert_eq!(final_text.as_deref(), Some("After the tool."));
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn codex_reasoning_items_are_not_answer_text() {
        let value = serde_json::json!({
            "type": "item.completed",
            "item": {"type": "reasoning", "summary": ["checking the plan"]}
        });
        assert_eq!(codex_event_result(&value), None);
        assert_eq!(
            codex_reasoning_text(value.get("item").unwrap()),
            Some("checking the plan".to_string())
        );
    }

    #[test]
    fn codex_command_items_never_become_the_assistant_result() {
        let value = serde_json::json!({
            "type": "item.completed",
            "item": {
                "id": "command-1",
                "type": "command_execution",
                "text": "Finished `cargo check`",
                "content": "tool output"
            }
        });
        assert_eq!(codex_event_result(&value), None);
    }
}
