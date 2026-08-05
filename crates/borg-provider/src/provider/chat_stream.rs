//! External command adapters for the subscription-backed Codex and Claude routes.
//!
//! These adapters intentionally keep subscription authentication and execution at
//! the CLI boundary. They do not embed a provider SDK or app-server, and they
//! do not own Borg's tool/runtime loop; the provider-neutral NativeHarness owns
//! API-key/OpenAI-compatible model routes.

use crate::mcp::ExternalMcpServer;
use crate::runtime::ProviderCallUsage;
use crate::{ProviderAuthBundle, ProviderAuthProvider, ProviderChannel};
use anyhow::{Context, Result, bail};
use serde_json::Value;
use std::fmt;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Instant;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubscriptionProvider {
    Codex,
    Claude,
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
    let command = build_claude_command_spec(&request, permission, auth_home.as_ref())?;
    let claude_request = claude_agents::ChatStreamRequest {
        prompt: request.prompt,
        attachments: request.attachments,
        system_prompt: request.system_prompt,
        command,
        runtime_directory: None,
        lifecycle_key: "borg-claude-subscription".to_string(),
    };

    let (native_events, mut native_events_receiver) = mpsc::channel(64);
    let (native_controls, mut control_forwarder) = match controls {
        Some(mut controls) => {
            let (sender, receiver) = mpsc::channel(64);
            let forwarder = tokio::spawn(async move {
                while let Some(control) = controls.recv().await {
                    if sender.send(map_claude_control(control)).await.is_err() {
                        break;
                    }
                }
            });
            (Some(receiver), Some(forwarder))
        }
        None => (None, None),
    };
    let mut runner = Some(tokio::spawn(claude_agents::run(
        claude_request,
        native_events,
        native_controls,
    )));

    while let Some(event) = native_events_receiver.recv().await {
        if events.send(map_claude_event(event)).await.is_err() {
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

fn map_claude_event(event: claude_agents::ChatStreamEvent) -> ChatStreamEvent {
    match event {
        claude_agents::ChatStreamEvent::ProviderEvent {
            kind,
            payload,
            raw_payload,
            stream_channel,
            content_text,
            provider_item_id,
            tool_use_id,
            tool_name,
        } => ChatStreamEvent::ProviderEvent {
            kind,
            payload,
            raw_payload,
            stream_channel,
            content_text,
            provider_item_id,
            tool_use_id,
            tool_name,
        },
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
            usage: usage.map(map_claude_usage),
            session_id,
        },
        claude_agents::ChatStreamEvent::Failed { error } => ChatStreamEvent::Failed { error },
    }
}

fn map_claude_usage(usage: claude_agents::ProviderCallUsage) -> ProviderCallUsage {
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
        ..ProviderCallUsage::default()
    }
}

async fn run_codex_subscription_process(
    request: ChatStreamRequest,
    mut controls: Option<mpsc::Receiver<ChatStreamControl>>,
    permission: LocalAgentPermission,
    events: mpsc::Sender<ChatStreamEvent>,
) -> Result<()> {
    let started_at = Instant::now();
    let auth_home = restore_auth_home(request.provider_auth.as_ref())?;
    let output_file =
        tempfile::NamedTempFile::new().context("failed to create subscription output file")?;
    let mut command =
        build_codex_command(&request, permission, output_file.path(), auth_home.as_ref())?;
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

    let stderr_task = tokio::spawn(async move {
        let mut stderr = stderr;
        let mut output = Vec::new();
        let _ = tokio::io::AsyncReadExt::read_to_end(&mut stderr, &mut output).await;
        output
    });

    let prompt = codex_prompt(&request);
    stdin
        .write_all(prompt.as_bytes())
        .await
        .context("failed to write subscription prompt")?;
    stdin
        .shutdown()
        .await
        .context("failed to close subscription prompt")?;
    drop(stdin);

    let mut lines = BufReader::new(stdout).lines();
    let mut text = String::new();
    let mut final_text = None;
    let mut session_id = request.session_id.clone();
    let mut usage = ProviderCallUsage::default();

    loop {
        tokio::select! {
            line = lines.next_line() => {
                let Some(line) = line.context("failed to read subscription output")? else {
                    break;
                };
                if line.trim().is_empty() {
                    continue;
                }
                let value = serde_json::from_str::<Value>(&line).unwrap_or_else(|_| Value::String(line.clone()));
                emit_provider_event(&events, &value).await;
                emit_codex_events(&events, &value).await;
                if let Some(delta) = codex_event_delta(&value) {
                    text.push_str(&delta);
                    events.send(ChatStreamEvent::Delta(delta)).await.ok();
                }
                if let Some(result) = codex_event_result(&value) {
                    final_text = Some(result);
                }
                if let Some(id) = event_session_id(&value) {
                    session_id = Some(id);
                }
                if let Some(event_usage) = event_usage(&value) {
                    usage = event_usage;
                }
            }
            control = receive_control(&mut controls), if controls.is_some() => {
                let Some(control) = control else {
                    controls = None;
                    continue;
                };
                match control {
                    ChatStreamControl::Interrupt => {
                        child.kill().await.ok();
                        bail!("subscription provider interrupted");
                    }
                    ChatStreamControl::Steer { ack, .. } => {
                        let _ = ack.send(Err("subscription provider does not support live steering".to_string()));
                    }
                    ChatStreamControl::Approval { .. }
                    | ChatStreamControl::ProviderInteractionResponse { .. } => {}
                }
            }
        }
    }

    let status = child
        .wait()
        .await
        .context("failed waiting for subscription provider")?;
    let stderr = stderr_task.await.unwrap_or_default();
    if !status.success() {
        let detail = String::from_utf8_lossy(&stderr).trim().to_string();
        bail!(
            "{} exited with {}{}",
            SubscriptionProvider::Codex.executable(),
            status,
            if detail.is_empty() {
                String::new()
            } else {
                format!(": {detail}")
            }
        );
    }

    let output_text = std::fs::read_to_string(output_file.path()).unwrap_or_default();
    let final_text = final_text
        .or_else(|| (!output_text.trim().is_empty()).then_some(output_text))
        .unwrap_or(text);
    anyhow::ensure!(
        !final_text.trim().is_empty(),
        "{} returned an empty response",
        SubscriptionProvider::Codex.executable()
    );

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

fn build_codex_command(
    request: &ChatStreamRequest,
    permission: LocalAgentPermission,
    output_file: &std::path::Path,
    auth_home: Option<&TempDir>,
) -> Result<Command> {
    let mut command = Command::new("codex");
    command.args([
        "exec",
        "--json",
        "--color",
        "never",
        "--skip-git-repo-check",
        "--output-last-message",
    ]);
    command.arg(output_file);
    if permission == LocalAgentPermission::FullAccess {
        command.arg("--dangerously-bypass-approvals-and-sandbox");
    } else {
        command.args(["-a", "on-request", "-s", "workspace-write"]);
    }
    if request.persist_session == Some(false) {
        command.arg("--ephemeral");
    }
    if let Some(model) = request
        .model
        .as_deref()
        .filter(|model| !model.trim().is_empty())
    {
        command.args(["--model", model]);
    }
    if let Some(effort) = request
        .effort
        .as_deref()
        .filter(|effort| !effort.trim().is_empty())
    {
        command.args(["-c", &format!("model_reasoning_effort=\"{effort}\"")]);
    }
    if let Some(cwd) = request.working_directory.as_deref() {
        command.current_dir(cwd);
    }
    if let Some(auth_home) = auth_home {
        command.env("HOME", auth_home.path());
        let codex_home = crate::provider_auth::ensure_codex_home(auth_home.path())?;
        command.env("CODEX_HOME", codex_home);
    }
    // Both subscription CLIs consume the prompt from stdin in this adapter
    // mode. Explicitly pipe all three standard streams so the async runner
    // can write the prompt and drain provider diagnostics without inheriting
    // Borg's terminal descriptors.
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    Ok(command)
}

fn codex_prompt(request: &ChatStreamRequest) -> String {
    let mut prompt = String::new();
    if !request.system_prompt.trim().is_empty() {
        prompt.push_str(request.system_prompt.trim());
        prompt.push_str("\n\n");
    }
    prompt.push_str(&request.prompt);
    prompt
}

fn claude_command_args(
    request: &ChatStreamRequest,
    permission: LocalAgentPermission,
) -> Vec<String> {
    let mut args = vec![
        "--print".to_string(),
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
    args
}

fn build_claude_command_spec(
    request: &ChatStreamRequest,
    permission: LocalAgentPermission,
    auth_home: Option<&TempDir>,
) -> Result<claude_agents::CommandSpec> {
    let mut environment = Vec::new();
    if let Some(auth_home) = auth_home {
        environment.push(("HOME".to_string(), auth_home.path().display().to_string()));
    }
    Ok(claude_agents::CommandSpec {
        program: PathBuf::from("claude"),
        args: claude_command_args(request, permission),
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
    let raw_kind = value.get("type").and_then(Value::as_str).unwrap_or("event");
    let kind = codex_subscription_event_kind(value, raw_kind);
    let item = value.get("item");
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
    value
        .get("item")
        .and_then(|item| item.get("type"))
        .and_then(Value::as_str)
        .filter(|_| matches!(raw_kind, "item.started" | "item.completed"))
        .map(|item_type| {
            let method = raw_kind
                .replace('.', "/")
                .strip_prefix("item/")
                .map_or_else(|| raw_kind.to_string(), str::to_string);
            format!("item/{method}:{item_type}")
        })
        .unwrap_or_else(|| raw_kind.replace('.', "/"))
}

async fn emit_codex_events(events: &mpsc::Sender<ChatStreamEvent>, value: &Value) {
    let Some(kind) = value.get("type").and_then(Value::as_str) else {
        return;
    };

    match kind {
        // These are emitted by Codex app-server versions that stream text,
        // while current `codex exec --json` generally emits completed items.
        "item/agentMessage/delta" | "item.agentMessage.delta" => {
            if let Some(delta) = value
                .pointer("/params/delta")
                .or_else(|| value.get("delta"))
                .and_then(Value::as_str)
                && !delta.is_empty()
            {
                events
                    .send(ChatStreamEvent::Delta(delta.to_string()))
                    .await
                    .ok();
            }
        }
        "item/reasoning/summaryTextDelta"
        | "item/reasoning/textDelta"
        | "item.reasoning.summaryTextDelta"
        | "item.reasoning.textDelta" => {
            if let Some(delta) = value
                .pointer("/params/delta")
                .or_else(|| value.get("delta"))
                .and_then(Value::as_str)
                && !delta.is_empty()
            {
                events
                    .send(ChatStreamEvent::ReasoningDelta(delta.to_string()))
                    .await
                    .ok();
            }
        }
        "item.started" | "item/started" => {
            let Some(item) = value.get("item") else {
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
        "item.completed" | "item/completed" => {
            let Some(item) = value.get("item") else {
                return;
            };
            let item_type = codex_item_type(item);
            if codex_item_is_agent_message(item_type) {
                if let Some(message) = codex_agent_message_text(item) {
                    // `codex exec --json` reports the complete assistant item,
                    // not token deltas. Commit it as a narration segment so a
                    // later tool/assistant item cannot be replaced by the
                    // aggregate final answer at turn completion.
                    events
                        .send(ChatStreamEvent::Delta(message.clone()))
                        .await
                        .ok();
                    events
                        .send(ChatStreamEvent::Narration { text: message })
                        .await
                        .ok();
                }
                return;
            }
            if codex_item_is_reasoning(item_type) {
                if let Some(reasoning) = codex_reasoning_text(item) {
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
    matches!(item_type, "agentMessage" | "agent_message")
}

fn codex_item_is_reasoning(item_type: &str) -> bool {
    matches!(
        item_type,
        "reasoning" | "reasoningSummary" | "reasoning_item" | "reasoningItem"
    )
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
        .get("delta")
        .and_then(Value::as_str)
        .filter(|_| {
            value
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| kind.to_ascii_lowercase().contains("delta"))
        })
        .map(str::to_string)
}

fn codex_event_result(value: &Value) -> Option<String> {
    if let Some(result) = value.get("result").and_then(Value::as_str) {
        return Some(result.to_string());
    }
    let is_completed = value
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| matches!(kind, "item.completed" | "item/completed"));
    is_completed
        .then(|| value.get("item").and_then(codex_agent_message_text))
        .flatten()
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

fn event_usage(value: &Value) -> Option<ProviderCallUsage> {
    let usage = value
        .get("usage")
        .or_else(|| value.pointer("/event/usage"))?;
    Some(ProviderCallUsage {
        input_tokens: usage
            .get("input_tokens")
            .or_else(|| usage.get("inputTokens"))
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        output_tokens: usage
            .get("output_tokens")
            .or_else(|| usage.get("outputTokens"))
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        total_tokens: usage
            .get("total_tokens")
            .or_else(|| usage.get("totalTokens"))
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        ..ProviderCallUsage::default()
    })
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
            claude_command_args(&request, LocalAgentPermission::Manual),
            vec![
                "--print",
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
            map_claude_event(event),
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
    fn codex_session_id_is_extracted() {
        assert_eq!(
            event_session_id(&serde_json::json!({"thread_id":"thread-1"})),
            Some("thread-1".to_string())
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
            Some(ChatStreamEvent::Delta(text)) if text == "done"
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(ChatStreamEvent::Narration { text }) if text == "done"
        ));
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
}
