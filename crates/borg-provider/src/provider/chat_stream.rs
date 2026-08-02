//! Streaming chat runners for the typed Codex app-server and Claude SDK paths.

use crate::{ProviderAuthBundle, ProviderAuthProvider, ProviderChannel};
use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc as std_mpsc,
};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;

use super::codex_app_server::{CodexAppServerClient, CodexTurnInput, JsonRpcMessage};
mod claude_native;
mod claude_stream;
mod codex_items;
mod codex_stream;
mod opencode_stream;

use super::{default_effort_for_backend, default_model_for_backend};
use crate::mcp::{
    ExternalMcpServer, ProviderMcpSetup, prepare_external_provider_mcp,
    prepare_provider_mcp_with_scope,
};
use crate::provider_auth::{
    codex_home_holds_chatgpt_session_checked, ensure_codex_home, restore_bundle,
};
use crate::runtime::{ProviderCallUsage, elapsed_millis_u64};
use claude_stream::ClaudeStreamState;
#[cfg(test)]
use codex_items::*;
use codex_stream::{CodexStreamMapper, codex_turn_usage};

const CODEX_PREWARM_TIMEOUT: Duration = Duration::from_secs(45);
const CODEX_FORCE_STOP_TIMEOUT: Duration = Duration::from_secs(2);

struct CancelCodexWorkerOnDrop(Arc<AtomicBool>);

impl Drop for CancelCodexWorkerOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct ProviderEventTelemetry {
    pub(super) stream_channel: Option<String>,
    pub(super) content_text: Option<String>,
    pub(super) provider_item_id: Option<String>,
    pub(super) tool_use_id: Option<String>,
    pub(super) tool_name: Option<String>,
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
    /// Provider reasoning that belongs in the activity timeline rather than
    /// the assistant's final answer.
    ReasoningDelta(String),
    /// A completed interim assistant text segment (one provider "agent
    /// message" / assistant turn block). Callers persist segments that are
    /// followed by more work as thinking breadcrumbs so the transcript
    /// interleaves narration with tool actions chronologically; the last
    /// segment of the turn is the final answer.
    Narration {
        text: String,
    },
    /// A non-tool provider/runtime milestone that should appear in the
    /// user-facing timeline. Used for events such as context compaction.
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
        /// The backend's session identifier (Claude Agent SDK
        /// `session_id` from the init message, Codex app-server
        /// `workspace_id`). Callers persist it and pass it back via
        /// `ChatStreamRequest.session_id` on the next turn so the
        /// backend reuses its server-side context.
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
        ack: tokio::sync::oneshot::Sender<Result<(), String>>,
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChatGitCredential")
            .field("host", &self.host)
            .field("username", &self.username)
            .field("token", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct ChatStreamRequest {
    pub prompt: String,
    /// Borg session that owns this provider worker. Local pools use it to
    /// synchronously reap a worker before the session may report Ready.
    pub owner_session_id: Option<String>,
    /// Durable Borg message identity forwarded to providers that support
    /// client-correlated user messages.
    pub client_user_message_id: Option<String>,
    /// Provider-native local files/images attached to this user turn.
    pub attachments: Vec<PathBuf>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub fast: bool,
    pub system_prompt: String,
    /// When set, the provider enforces structured output against this JSON
    /// schema server-side (Agent SDK `outputFormat` / Codex app-server
    /// `outputSchema`). When unset, the final `Done.final_text` is free-form
    /// text.
    pub output_schema: Option<Value>,
    pub mcp_owner_id: Option<String>,
    pub mcp_allowed_scopes: Vec<String>,
    pub mcp_user_id: Option<String>,
    pub mcp_external_servers: Vec<ExternalMcpServer>,
    /// Short-lived API token scoped to the chat caller. When present, Borg MCP
    /// uses this instead of the global internal impersonation token.
    pub mcp_api_token: Option<String>,
    pub provider_auth: Option<ChatProviderAuth>,
    /// Host-scoped git credentials made available to provider shell commands
    /// through a temporary askpass helper. Secrets are not included in prompts
    /// or MCP tool results; this only lets git authenticate for matching hosts.
    pub git_credentials: Vec<ChatGitCredential>,
    /// Durable workspace filesystem root used as the provider working
    /// directory. Provider auth, MCP config, and tool homes stay isolated in a
    /// per-turn home; this path is where repo checkouts, scratch, and artifacts
    /// live across turns.
    pub working_directory: Option<PathBuf>,
    /// Backend session to resume, if any. When Some, the provider
    /// passes this to the bundled Agent SDK (`resume: session_id`)
    /// or Codex app-server (`thread_resume`) so the conversation
    /// picks up where the prior call left off — skips re-hydrating
    /// the system prompt + prior turns, which lets prompt caches
    /// hit on the server side.
    pub session_id: Option<String>,
    /// Claude: activates `CLAUDE_CODE_USE_VERTEX` / `CLAUDE_CODE_USE_BEDROCK`
    /// env vars on the bundled Agent SDK binary. Codex: reserved for future
    /// Azure OpenAI routing. `Direct` is the default, matches current
    /// behaviour, and is what Pro-tier runs always use.
    pub provider_channel: ProviderChannel,
    /// Claude: when `Some(false)`, the Agent SDK skips writing session
    /// history to `~/.claude/projects/`. `None` keeps the SDK default
    /// (persist), so developers and support engineers can replay a run
    /// from the `claude` CLI for debugging. Enterprise tier flips this
    /// to `Some(false)` for the no-disk-persistence privacy guarantee.
    /// For Codex, this controls whether app-server threads are materialized
    /// so a later process can call `thread/resume`.
    pub persist_session: Option<bool>,
    /// Codex app-server web-search policy for this request. Comparable
    /// benchmark runs should set this false unless replayable authority
    /// snapshots are being tested.
    pub web_search_allowed: bool,
    /// Full prompt to use if provider-level resume is unavailable. This lets
    /// callers send a small delta prompt on the resume path without making
    /// hidden provider state the only way to reconstruct a turn.
    pub resume_unavailable_prompt: Option<String>,
}

pub fn run_claude_chat_stream(req: ChatStreamRequest) -> mpsc::Receiver<ChatStreamEvent> {
    run_claude_stream(req, None, false, LocalAgentPermission::FullAccess)
}

pub fn run_claude_chat_stream_with_control(
    req: ChatStreamRequest,
    control_rx: Option<mpsc::Receiver<ChatStreamControl>>,
) -> mpsc::Receiver<ChatStreamEvent> {
    run_claude_stream(req, control_rx, false, LocalAgentPermission::FullAccess)
}

pub fn run_claude_local_chat_stream(
    req: ChatStreamRequest,
    control_rx: Option<mpsc::Receiver<ChatStreamControl>>,
    permission: LocalAgentPermission,
) -> mpsc::Receiver<ChatStreamEvent> {
    run_claude_stream(req, control_rx, true, permission)
}

#[derive(Clone, Default)]
pub struct ClaudeSdkPool {
    inner: Arc<tokio::sync::Mutex<Option<PooledClaudeSdk>>>,
}

struct PooledClaudeSdk {
    child: tokio::process::Child,
    stdin: tokio::process::ChildStdin,
    stdout: BufReader<tokio::process::ChildStdout>,
    stderr: Arc<tokio::sync::Mutex<String>>,
    _provider_home: tempfile::TempDir,
    channel: ProviderChannel,
    started: bool,
}

pub fn run_pooled_claude_local_chat_stream(
    req: ChatStreamRequest,
    control_rx: Option<mpsc::Receiver<ChatStreamControl>>,
    permission: LocalAgentPermission,
    pool: ClaudeSdkPool,
) -> mpsc::Receiver<ChatStreamEvent> {
    let (tx, rx) = mpsc::channel::<ChatStreamEvent>(64);
    tokio::spawn(async move {
        if let Err(error) =
            run_pooled_claude_sdk_inner(req, tx.clone(), control_rx, permission, pool).await
        {
            let _ = tx
                .send(ChatStreamEvent::Failed {
                    error: format!("{error:#}"),
                })
                .await;
        }
    });
    rx
}

fn run_claude_stream(
    req: ChatStreamRequest,
    control_rx: Option<mpsc::Receiver<ChatStreamControl>>,
    local_auth: bool,
    permission: LocalAgentPermission,
) -> mpsc::Receiver<ChatStreamEvent> {
    let (tx, rx) = mpsc::channel::<ChatStreamEvent>(64);
    tokio::spawn(async move {
        // Native path talks to the `claude` binary directly; the sidecar path
        // goes through packages/borg-claude-sdk. Both emit identical events.
        let result = if claude_native::native_enabled() {
            claude_native::run(req, tx.clone(), control_rx, local_auth, permission).await
        } else {
            run_claude_sdk_inner(req, tx.clone(), control_rx, local_auth, permission).await
        };
        if let Err(err) = result {
            let _ = tx
                .send(ChatStreamEvent::Failed {
                    error: format!("{err:#}"),
                })
                .await;
        }
    });
    rx
}

pub fn run_codex_chat_stream(req: ChatStreamRequest) -> mpsc::Receiver<ChatStreamEvent> {
    run_codex_stream(req, None, false, LocalAgentPermission::FullAccess, None)
}

pub fn run_codex_chat_stream_with_control(
    req: ChatStreamRequest,
    control_rx: Option<mpsc::Receiver<ChatStreamControl>>,
) -> mpsc::Receiver<ChatStreamEvent> {
    run_codex_stream(
        req,
        control_rx,
        false,
        LocalAgentPermission::FullAccess,
        None,
    )
}

pub fn run_codex_local_chat_stream(
    req: ChatStreamRequest,
    control_rx: Option<mpsc::Receiver<ChatStreamControl>>,
    permission: LocalAgentPermission,
) -> mpsc::Receiver<ChatStreamEvent> {
    run_codex_stream(req, control_rx, true, permission, None)
}

#[derive(Clone, Default)]
pub struct CodexAppServerPool {
    inner: Arc<Mutex<Option<PooledCodexAppServer>>>,
    prewarm: Arc<Mutex<Option<CodexPrewarm>>>,
    active: Arc<Mutex<HashMap<String, CodexActiveWorker>>>,
}

struct CodexPrewarm {
    receiver: std_mpsc::Receiver<Result<CodexAppServerClient>>,
    cancellation: Arc<AtomicBool>,
    completed: Arc<(Mutex<bool>, Condvar)>,
}

struct CodexPrewarmCompletion(Arc<(Mutex<bool>, Condvar)>);

impl Drop for CodexPrewarmCompletion {
    fn drop(&mut self) {
        let (completed, wake) = &*self.0;
        *completed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        wake.notify_all();
    }
}

#[derive(Clone)]
struct CodexActiveWorker {
    cancellation: Arc<AtomicBool>,
    completed: Arc<(Mutex<bool>, Condvar)>,
}

struct CodexActiveWorkerGuard {
    owner_session_id: String,
    active: Arc<Mutex<HashMap<String, CodexActiveWorker>>>,
    cancellation: Arc<AtomicBool>,
    completed: Arc<(Mutex<bool>, Condvar)>,
    pooled: Arc<Mutex<Option<PooledCodexAppServer>>>,
}

impl Drop for CodexActiveWorkerGuard {
    fn drop(&mut self) {
        if self.cancellation.load(Ordering::Acquire) {
            let stale = {
                let mut pooled = self
                    .pooled
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if pooled.as_ref().is_some_and(|pooled| {
                    pooled.owner_session_id.as_deref() == Some(self.owner_session_id.as_str())
                }) {
                    pooled.take()
                } else {
                    None
                }
            };
            // Dropping the client terminates its process group. Do this before
            // publishing the completion acknowledgement consumed by Escape.
            drop(stale);
        }
        let (completed, wake) = &*self.completed;
        *completed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        wake.notify_all();
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if active
            .get(&self.owner_session_id)
            .is_some_and(|worker| Arc::ptr_eq(&worker.completed, &self.completed))
        {
            active.remove(&self.owner_session_id);
        }
    }
}

impl CodexAppServerPool {
    fn register_active_worker(
        &self,
        owner_session_id: String,
        cancellation: Arc<AtomicBool>,
    ) -> CodexActiveWorkerGuard {
        let completed = Arc::new((Mutex::new(false), Condvar::new()));
        self.active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                owner_session_id.clone(),
                CodexActiveWorker {
                    cancellation: Arc::clone(&cancellation),
                    completed: Arc::clone(&completed),
                },
            );
        CodexActiveWorkerGuard {
            owner_session_id,
            active: Arc::clone(&self.active),
            cancellation,
            completed,
            pooled: Arc::clone(&self.inner),
        }
    }

    /// Force the active provider worker for one Borg session to terminate and
    /// wait until its app-server process tree has been reaped.
    pub fn stop_owner(&self, owner_session_id: &str) -> Result<()> {
        let worker = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(owner_session_id)
            .cloned();
        let Some(worker) = worker else {
            return Ok(());
        };
        worker.cancellation.store(true, Ordering::Release);
        let (completed, wake) = &*worker.completed;
        let completed = completed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (completed, _timeout) = wake
            .wait_timeout_while(completed, CODEX_FORCE_STOP_TIMEOUT, |completed| !*completed)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        anyhow::ensure!(
            *completed,
            "Codex provider worker for Borg session {owner_session_id} did not finish process-tree cleanup within {} ms",
            CODEX_FORCE_STOP_TIMEOUT.as_millis()
        );
        Ok(())
    }

    pub fn prewarm_local(&self, web_search_allowed: bool) {
        if self
            .inner
            .lock()
            .expect("Codex app-server pool lock poisoned")
            .is_some()
        {
            return;
        }
        let mut prewarm = self
            .prewarm
            .lock()
            .expect("Codex app-server prewarm lock poisoned");
        if prewarm.is_some() {
            return;
        }
        let (tx, rx) = std_mpsc::sync_channel(1);
        let cancellation = Arc::new(AtomicBool::new(false));
        let completed = Arc::new((Mutex::new(false), Condvar::new()));
        *prewarm = Some(CodexPrewarm {
            receiver: rx,
            cancellation: Arc::clone(&cancellation),
            completed: Arc::clone(&completed),
        });
        tracing::debug!(
            target: "borg_ttft",
            stage = "codex_prewarm_started",
            "Codex startup stage"
        );
        std::thread::spawn(move || {
            let _completion = CodexPrewarmCompletion(completed);
            let started_at = Instant::now();
            let result = CodexAppServerClient::start_with_cancellation(
                true,
                web_search_allowed,
                None,
                false,
                &[],
                Some(cancellation),
            );
            tracing::debug!(
                target: "borg_ttft",
                stage = "codex_prewarm_finished",
                elapsed_ms = started_at.elapsed().as_millis(),
                success = result.is_ok(),
                "Codex startup stage"
            );
            let _ = tx.send(result);
        });
    }

    fn take_prewarmed(&self, cancellation: &AtomicBool) -> Option<Result<CodexAppServerClient>> {
        let prewarm = self
            .prewarm
            .lock()
            .expect("Codex app-server prewarm lock poisoned")
            .take()?;
        let deadline = Instant::now() + CODEX_PREWARM_TIMEOUT;
        loop {
            if cancellation.load(Ordering::Acquire) {
                if let Err(error) = Self::cancel_and_join_prewarm(&prewarm) {
                    return Some(Err(error));
                }
                return Some(Err(anyhow!(
                    "Codex app-server prewarm cancelled with its owning turn"
                )));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                if let Err(error) = Self::cancel_and_join_prewarm(&prewarm) {
                    return Some(Err(error));
                }
                return Some(Err(anyhow!(
                    "Codex app-server prewarm timed out after {} ms",
                    CODEX_PREWARM_TIMEOUT.as_millis()
                )));
            }
            match prewarm
                .receiver
                .recv_timeout(remaining.min(Duration::from_millis(50)))
            {
                Ok(result) => return Some(result),
                Err(std_mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std_mpsc::RecvTimeoutError::Disconnected) => {
                    return Some(Err(anyhow!(
                        "Codex app-server prewarm worker stopped unexpectedly"
                    )));
                }
            }
        }
    }

    fn cancel_and_join_prewarm(prewarm: &CodexPrewarm) -> Result<()> {
        prewarm.cancellation.store(true, Ordering::Release);
        let (completed, wake) = &*prewarm.completed;
        let completed = completed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (completed, _timeout) = wake
            .wait_timeout_while(completed, CODEX_FORCE_STOP_TIMEOUT, |completed| !*completed)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        anyhow::ensure!(
            *completed,
            "Codex app-server prewarm did not finish process-tree cleanup within {} ms",
            CODEX_FORCE_STOP_TIMEOUT.as_millis()
        );
        Ok(())
    }

    pub fn compact(&self, thread_id: &str) -> Result<()> {
        let mut guard = self
            .inner
            .lock()
            .expect("Codex app-server pool lock poisoned");
        let pooled = guard
            .as_mut()
            .context("Codex context is not available until the current turn finishes")?;
        anyhow::ensure!(
            pooled.client.workspace_id() == Some(thread_id),
            "the active Codex thread does not match this Borg session"
        );
        pooled.client.thread_compact(thread_id)
    }
}

struct PooledCodexAppServer {
    client: CodexAppServerClient,
    key: CodexAppServerKey,
    owner_session_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CodexAppServerKey {
    model: Option<String>,
    effort: Option<String>,
    working_directory: String,
    permission: LocalAgentPermission,
    web_search_allowed: bool,
}

pub fn run_pooled_codex_local_chat_stream(
    req: ChatStreamRequest,
    control_rx: Option<mpsc::Receiver<ChatStreamControl>>,
    permission: LocalAgentPermission,
    pool: CodexAppServerPool,
) -> mpsc::Receiver<ChatStreamEvent> {
    run_codex_stream(req, control_rx, true, permission, Some(pool))
}

fn run_codex_stream(
    req: ChatStreamRequest,
    control_rx: Option<mpsc::Receiver<ChatStreamControl>>,
    local_auth: bool,
    permission: LocalAgentPermission,
    pool: Option<CodexAppServerPool>,
) -> mpsc::Receiver<ChatStreamEvent> {
    let (tx, rx) = mpsc::channel::<ChatStreamEvent>(64);
    let cancellation = Arc::new(AtomicBool::new(false));
    let active_worker_guard = pool.as_ref().and_then(|pool| {
        req.owner_session_id.as_ref().map(|owner_session_id| {
            pool.register_active_worker(owner_session_id.clone(), Arc::clone(&cancellation))
        })
    });
    let cancel_closed_stream = Arc::clone(&cancellation);
    tokio::spawn(async move {
        let provider = run_codex_app_server_inner(
            req,
            tx.clone(),
            control_rx,
            local_auth,
            permission,
            pool,
            cancellation,
            active_worker_guard,
        );
        tokio::pin!(provider);
        tokio::select! {
            result = &mut provider => {
                if let Err(err) = result {
                    let _ = tx
                        .send(ChatStreamEvent::Failed {
                            error: format!("{err:#}"),
                        })
                        .await;
                }
            }
            _ = tx.closed() => {
                tracing::debug!("Codex stream receiver closed; cancelling provider worker");
                cancel_closed_stream.store(true, Ordering::Release);
                if let Err(err) = provider.await {
                    tracing::debug!(?err, "cancelled Codex provider worker finished cleanup");
                }
            }
        }
    });
    rx
}

pub fn run_codex_freeform_chat_stream(req: ChatStreamRequest) -> mpsc::Receiver<ChatStreamEvent> {
    let mut req = req;
    req.output_schema = None;
    run_codex_chat_stream(req)
}

pub fn run_opencode_local_chat_stream(
    req: ChatStreamRequest,
    permission: LocalAgentPermission,
) -> mpsc::Receiver<ChatStreamEvent> {
    let (tx, rx) = mpsc::channel::<ChatStreamEvent>(64);
    tokio::spawn(async move {
        if let Err(error) = opencode_stream::run(req, tx.clone(), permission).await {
            let _ = tx
                .send(ChatStreamEvent::Failed {
                    error: format!("{error:#}"),
                })
                .await;
        }
    });
    rx
}

async fn run_claude_sdk_inner(
    req: ChatStreamRequest,
    tx: mpsc::Sender<ChatStreamEvent>,
    control_rx: Option<mpsc::Receiver<ChatStreamControl>>,
    local_auth: bool,
    permission: LocalAgentPermission,
) -> Result<()> {
    let started_at = Instant::now();
    let provider_home = tempfile::tempdir().context("failed to create Claude provider home")?;
    let workspace_dir = req
        .working_directory
        .clone()
        .unwrap_or_else(|| provider_home.path().to_path_buf());
    fs::create_dir_all(&workspace_dir).with_context(|| {
        format!(
            "failed to create Claude workspace directory {}",
            workspace_dir.display()
        )
    })?;
    if let Some(auth) = req.provider_auth.as_ref()
        && auth.provider == ProviderAuthProvider::Claude
    {
        restore_bundle(
            ProviderAuthProvider::Claude,
            &auth.bundle,
            provider_home.path(),
        )
        .context("failed to restore Claude provider auth bundle")?;
    }
    let mcp_setup = prepare_request_mcp(provider_home.path(), &req, local_auth)?;
    let provider_path =
        resolve_claude_sdk_provider_path().context("failed to resolve Claude SDK provider path")?;
    let provider_dir = provider_path
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| {
            anyhow!(
                "invalid Claude SDK provider path: {}",
                provider_path.display()
            )
        })?;
    let mcp_servers = read_mcp_servers_from_config(mcp_setup.claude_config_path.as_deref())
        .context("failed to load Claude MCP server config")?;
    let git_env = prepare_git_credential_env(provider_home.path(), &req.git_credentials)
        .context("failed to prepare git credential helper")?;

    let model = req
        .model
        .or_else(|| {
            (!local_auth)
                .then(|| default_model_for_backend("claude"))
                .flatten()
        })
        .filter(|model| !model.trim().is_empty());
    let mut config = serde_json::json!({
        "prompt": req.prompt,
        "attachments": req.attachments,
        "workspace_dir": workspace_dir,
        "effort": req.effort.unwrap_or_else(|| "medium".to_string()),
        "permission_mode": match permission {
            LocalAgentPermission::FullAccess => "bypassPermissions",
            LocalAgentPermission::Auto => "acceptEdits",
            LocalAgentPermission::Manual => "default",
        },
        "system_prompt": req.system_prompt,
    });
    if let Some(model) = model {
        config["model"] = Value::String(model);
    }
    if req.fast {
        config["fast"] = Value::Bool(true);
    }
    if let Some(schema) = req.output_schema.as_ref() {
        config["output_schema"] = schema.clone();
    }
    if let Some(servers) = mcp_servers {
        config["mcp_servers"] = servers;
    }
    if !mcp_setup.allowed_tools.is_empty() {
        config["allowed_tools"] = Value::String(mcp_setup.allowed_tools.clone());
    }
    if let Some(resume_id) = req.session_id.as_deref()
        && is_nonempty_session_id(resume_id)
    {
        // Tells the bundled provider.ts to pass `resume: session_id`
        // to @anthropic-ai/claude-agent-sdk's query(). The SDK will
        // pick up the prior conversation server-side rather than
        // sending a fresh system prompt + tool list.
        //
        // We filter to session_ids the SDK itself issued (captured on
        // a prior Done). Random UUIDs generated by the runner for its
        // own tracking purposes are rejected so the SDK doesn't error
        // on an unrecognised id.
        config["resume"] = Value::String(resume_id.to_string());
    }
    if let Some(persist) = req.persist_session {
        // Forwarded as a tri-state — only emit the key when explicitly
        // set so the bun side keeps the SDK's own default (persist)
        // for pass-through callers that didn't decide.
        config["persist_session"] = Value::Bool(persist);
    }

    let mut command = Command::new("node");
    command
        .arg(&provider_path)
        .current_dir(&provider_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // If the runner drops this future (cancellation, parent stage
        // failure, panic) the bun subprocess + its children would
        // otherwise outlive the request and keep paying the API.
        // kill_on_drop hands the cleanup to tokio.
        .kill_on_drop(true);
    if !local_auth {
        command.env("HOME", provider_home.path());
    }
    apply_git_credential_env(&mut command, &git_env);
    if std::env::var("ENABLE_TOOL_SEARCH").is_err() {
        // Claude Agent SDK tool search defers large MCP schemas until the
        // model asks for them. Borg exposes a compact catalogue up front and
        // lets the SDK discover concrete tools on demand when supported.
        command.env("ENABLE_TOOL_SEARCH", "auto:5");
    }
    if std::env::var_os("ANTHROPIC_API_KEY").is_none()
        && let Some(key) =
            crate::credentials::stored_api_key(crate::credentials::ApiKeyCredential::Anthropic)
    {
        // Key auth for users who chose "add an API key" over a subscription
        // sign-in; the bundled SDK reads it from the environment we hand the
        // subprocess, and never from a file it could leak elsewhere.
        command.env("ANTHROPIC_API_KEY", key);
    }
    apply_claude_channel_env(&mut command, req.provider_channel)?;
    crate::subprocess::isolate_async_process_from_terminal(&mut command);

    let mut child = command
        .spawn()
        .with_context(|| format!("failed to spawn bun for {}", provider_path.display()))?;

    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("claude SDK stdin pipe missing"))?;
    let config_bytes = serde_json::to_vec(&config)?;
    stdin
        .write_all(&config_bytes)
        .await
        .context("failed to write Claude SDK config to stdin")?;
    stdin
        .write_all(b"\n")
        .await
        .context("failed to delimit Claude SDK config on stdin")?;
    stdin
        .flush()
        .await
        .context("failed to flush Claude SDK config")?;

    let control_task = tokio::spawn(forward_claude_controls(stdin, control_rx));

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("claude SDK stdout pipe missing"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("claude SDK stderr pipe missing"))?;

    let stderr_buf = std::sync::Arc::new(tokio::sync::Mutex::new(String::new()));
    {
        let stderr_buf = stderr_buf.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stderr);
            let mut buf = String::new();
            loop {
                buf.clear();
                match reader.read_line(&mut buf).await {
                    Ok(0) => break,
                    Ok(_) => stderr_buf.lock().await.push_str(&buf),
                    Err(_) => break,
                }
            }
        });
    }

    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    let mut state = ClaudeStreamState::default();

    loop {
        line.clear();
        let read = reader
            .read_line(&mut line)
            .await
            .context("failed reading Claude SDK stdout")?;
        if read == 0 {
            break;
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(trimmed)
            .with_context(|| format!("failed to parse Claude SDK line: {trimmed}"))?;
        if let Some(event) = claude_adapter_event(&value) {
            if tx.send(event).await.is_err() {
                break;
            }
            continue;
        }
        let telemetry = classify_claude_provider_event(&value);
        let _ = tx
            .send(ChatStreamEvent::ProviderEvent {
                kind: format!(
                    "claude.{}",
                    value
                        .get("type")
                        .and_then(Value::as_str)
                        .unwrap_or("message")
                ),
                payload: summarize_claude_provider_event(&value),
                raw_payload: Some(value.clone()),
                stream_channel: telemetry.stream_channel,
                content_text: telemetry.content_text,
                provider_item_id: telemetry.provider_item_id,
                tool_use_id: telemetry.tool_use_id,
                tool_name: telemetry.tool_name,
            })
            .await;
        if state.handle_message(&value, &tx).await? {
            break;
        }
    }

    let status = child
        .wait()
        .await
        .context("failed waiting for Claude SDK process")?;
    control_task.abort();

    if state.emitted_failure {
        return Ok(());
    }

    if !status.success() {
        let stderr_text = stderr_buf.lock().await.clone();
        let trimmed = stderr_text.trim();
        let suffix = if trimmed.is_empty() {
            String::new()
        } else {
            format!(": {trimmed}")
        };
        let _ = tx
            .send(ChatStreamEvent::Failed {
                error: format!(
                    "claude SDK exited with status {}{}",
                    status
                        .code()
                        .map(|code| code.to_string())
                        .unwrap_or_else(|| "?".into()),
                    suffix
                ),
            })
            .await;
        return Ok(());
    }

    let final_text = state
        .final_text
        .take()
        .unwrap_or_else(|| state.delta_accumulator.clone());
    let usage = Some(state.final_usage.unwrap_or_else(|| ProviderCallUsage {
        duration_ms: elapsed_millis_u64(started_at),
        ..ProviderCallUsage::default()
    }));
    let session_id = state.session_id.take();
    let _ = tx
        .send(ChatStreamEvent::Done {
            final_text,
            usage,
            session_id,
        })
        .await;
    Ok(())
}

async fn run_pooled_claude_sdk_inner(
    req: ChatStreamRequest,
    tx: mpsc::Sender<ChatStreamEvent>,
    mut controls: Option<mpsc::Receiver<ChatStreamControl>>,
    permission: LocalAgentPermission,
    pool: ClaudeSdkPool,
) -> Result<()> {
    anyhow::ensure!(
        req.provider_auth.is_none() && req.git_credentials.is_empty(),
        "credential-scoped Claude requests cannot reuse a pooled adapter process"
    );
    let started_at = Instant::now();
    let mut guard = pool.inner.lock().await;
    let mut pooled = match guard.take() {
        Some(pooled) if pooled.channel == req.provider_channel => pooled,
        Some(mut stale) => {
            let _ = stale.child.kill().await;
            start_pooled_claude_sdk(req.provider_channel).await?
        }
        None => start_pooled_claude_sdk(req.provider_channel).await?,
    };
    let config = build_pooled_claude_config(&req, pooled._provider_home.path(), permission)?;
    let wire_value = if pooled.started {
        json!({ "type": "start", "config": config })
    } else {
        pooled.started = true;
        config
    };
    write_claude_control(&mut pooled.stdin, &wire_value)
        .await
        .context("failed to start pooled Claude turn")?;

    let mut state = ClaudeStreamState::default();
    let mut line = String::new();
    let mut terminal_seen = false;
    loop {
        line.clear();
        tokio::select! {
            read = pooled.stdout.read_line(&mut line) => {
                let read = read.context("failed reading pooled Claude SDK stdout")?;
                if read == 0 {
                    let stderr = pooled.stderr.lock().await.clone();
                    bail!("pooled Claude SDK exited unexpectedly: {}", stderr.trim());
                }
                let trimmed = line.trim_end_matches(['\r', '\n']);
                if trimmed.is_empty() {
                    continue;
                }
                let value: Value = serde_json::from_str(trimmed)
                    .with_context(|| format!("failed to parse Claude SDK line: {trimmed}"))?;
                if let Some(event) = claude_adapter_event(&value) {
                    if tx.send(event).await.is_err() {
                        break;
                    }
                    continue;
                }
                let telemetry = classify_claude_provider_event(&value);
                let _ = tx.send(ChatStreamEvent::ProviderEvent {
                    kind: format!(
                        "claude.{}",
                        value.get("type").and_then(Value::as_str).unwrap_or("message")
                    ),
                    payload: summarize_claude_provider_event(&value),
                    raw_payload: Some(value.clone()),
                    stream_channel: telemetry.stream_channel,
                    content_text: telemetry.content_text,
                    provider_item_id: telemetry.provider_item_id,
                    tool_use_id: telemetry.tool_use_id,
                    tool_name: telemetry.tool_name,
                }).await;
                let terminal = value.get("type").and_then(Value::as_str) == Some("result");
                terminal_seen |= terminal;
                if state.handle_message(&value, &tx).await? || terminal {
                    break;
                }
            }
            control = receive_claude_control(&mut controls), if controls.is_some() => {
                let Some(control) = control else {
                    controls = None;
                    continue;
                };
                forward_one_claude_control(&mut pooled.stdin, control).await?;
            }
        }
    }

    if terminal_seen && !state.emitted_failure {
        let final_text = state
            .final_text
            .take()
            .unwrap_or_else(|| state.delta_accumulator.clone());
        let usage = Some(state.final_usage.unwrap_or_else(|| ProviderCallUsage {
            duration_ms: elapsed_millis_u64(started_at),
            ..ProviderCallUsage::default()
        }));
        let _ = tx
            .send(ChatStreamEvent::Done {
                final_text,
                usage,
                session_id: state.session_id.take(),
            })
            .await;
    }
    if terminal_seen {
        *guard = Some(pooled);
    } else {
        let _ = pooled.child.kill().await;
    }
    Ok(())
}

async fn start_pooled_claude_sdk(channel: ProviderChannel) -> Result<PooledClaudeSdk> {
    let provider_home = tempfile::tempdir().context("failed to create Claude provider home")?;
    let provider_path =
        resolve_claude_sdk_provider_path().context("failed to resolve Claude SDK provider path")?;
    let provider_dir = provider_path
        .parent()
        .map(Path::to_path_buf)
        .context("Claude SDK adapter has no parent directory")?;
    let mut command = Command::new("node");
    command
        .arg(&provider_path)
        .current_dir(provider_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if std::env::var("ENABLE_TOOL_SEARCH").is_err() {
        command.env("ENABLE_TOOL_SEARCH", "auto:5");
    }
    apply_claude_channel_env(&mut command, channel)?;
    crate::subprocess::isolate_async_process_from_terminal(&mut command);
    let mut child = command.spawn().with_context(|| {
        format!(
            "failed to spawn pooled Claude SDK adapter {}",
            provider_path.display()
        )
    })?;
    let stdin = child
        .stdin
        .take()
        .context("pooled Claude SDK stdin pipe missing")?;
    let stdout = BufReader::new(
        child
            .stdout
            .take()
            .context("pooled Claude SDK stdout pipe missing")?,
    );
    let stderr = Arc::new(tokio::sync::Mutex::new(String::new()));
    let child_stderr = child
        .stderr
        .take()
        .context("pooled Claude SDK stderr pipe missing")?;
    {
        let stderr = stderr.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(child_stderr);
            let mut line = String::new();
            while reader.read_line(&mut line).await.is_ok_and(|read| read > 0) {
                stderr.lock().await.push_str(&line);
                line.clear();
            }
        });
    }
    Ok(PooledClaudeSdk {
        child,
        stdin,
        stdout,
        stderr,
        _provider_home: provider_home,
        channel,
        started: false,
    })
}

fn build_pooled_claude_config(
    req: &ChatStreamRequest,
    provider_home: &Path,
    permission: LocalAgentPermission,
) -> Result<Value> {
    let workspace_dir = req
        .working_directory
        .clone()
        .unwrap_or_else(|| provider_home.to_path_buf());
    fs::create_dir_all(&workspace_dir)?;
    let mcp_setup = prepare_request_mcp(provider_home, req, true)?;
    let mcp_servers = read_mcp_servers_from_config(mcp_setup.claude_config_path.as_deref())?;
    let mut config = json!({
        "prompt": req.prompt,
        "attachments": req.attachments,
        "workspace_dir": workspace_dir,
        "effort": req.effort.clone().unwrap_or_else(|| "medium".to_string()),
        "permission_mode": match permission {
            LocalAgentPermission::FullAccess => "bypassPermissions",
            LocalAgentPermission::Auto => "acceptEdits",
            LocalAgentPermission::Manual => "default",
        },
        "system_prompt": req.system_prompt,
    });
    if let Some(model) = req.model.as_ref().filter(|model| !model.trim().is_empty()) {
        config["model"] = Value::String(model.clone());
    }
    if req.fast {
        config["fast"] = Value::Bool(true);
    }
    if let Some(schema) = req.output_schema.as_ref() {
        config["output_schema"] = schema.clone();
    }
    if let Some(servers) = mcp_servers {
        config["mcp_servers"] = servers;
    }
    if !mcp_setup.allowed_tools.is_empty() {
        config["allowed_tools"] = Value::String(mcp_setup.allowed_tools);
    }
    if let Some(session_id) = req
        .session_id
        .as_deref()
        .filter(|id| is_nonempty_session_id(id))
    {
        config["resume"] = Value::String(session_id.to_string());
    }
    if let Some(persist) = req.persist_session {
        config["persist_session"] = Value::Bool(persist);
    }
    Ok(config)
}

async fn forward_claude_controls(
    mut stdin: tokio::process::ChildStdin,
    mut controls: Option<mpsc::Receiver<ChatStreamControl>>,
) -> Result<()> {
    let Some(controls) = controls.as_mut() else {
        stdin.shutdown().await?;
        return Ok(());
    };
    while let Some(control) = controls.recv().await {
        if forward_one_claude_control(&mut stdin, control)
            .await
            .is_err()
        {
            break;
        }
    }
    stdin.shutdown().await?;
    Ok(())
}

async fn receive_claude_control(
    controls: &mut Option<mpsc::Receiver<ChatStreamControl>>,
) -> Option<ChatStreamControl> {
    match controls {
        Some(controls) => controls.recv().await,
        None => std::future::pending().await,
    }
}

async fn forward_one_claude_control(
    stdin: &mut tokio::process::ChildStdin,
    control: ChatStreamControl,
) -> Result<()> {
    let value = match control {
        ChatStreamControl::Steer {
            text,
            attachments,
            ack,
            ..
        } => {
            let value = json!({
                "type": "steer",
                "text": text,
                "attachments": attachments,
            });
            match write_claude_control(stdin, &value).await {
                Ok(()) => {
                    let _ = ack.send(Ok(()));
                    return Ok(());
                }
                Err(error) => {
                    let _ = ack.send(Err(
                        "Claude turn ended before the steer was delivered".to_string()
                    ));
                    return Err(error);
                }
            }
        }
        ChatStreamControl::Interrupt => json!({ "type": "interrupt" }),
        ChatStreamControl::Approval {
            approval_id,
            decision,
        } => json!({
            "type": "approval",
            "approval_id": approval_id,
            "decision": match decision {
                ChatApprovalDecision::ApproveOnce => "approve_once",
                ChatApprovalDecision::ApproveSession => "approve_session",
                ChatApprovalDecision::Reject => "reject",
            },
        }),
        ChatStreamControl::ProviderInteractionResponse {
            interaction_id,
            response,
        } => json!({
            "type": "provider_interaction_response",
            "interaction_id": interaction_id,
            "response": response,
        }),
    };
    write_claude_control(stdin, &value).await
}

fn claude_adapter_event(value: &Value) -> Option<ChatStreamEvent> {
    if value.get("type").and_then(Value::as_str) == Some("borg_context_usage") {
        return Some(ChatStreamEvent::ProviderEvent {
            kind: "claude.context_usage".to_string(),
            payload: value.clone(),
            raw_payload: Some(value.clone()),
            stream_channel: Some("usage".to_string()),
            content_text: None,
            provider_item_id: None,
            tool_use_id: None,
            tool_name: None,
        });
    }
    if value.get("type").and_then(Value::as_str) == Some("borg_provider_interaction") {
        return Some(ChatStreamEvent::ProviderInteractionRequested {
            interaction_id: value.get("interaction_id")?.as_str()?.to_string(),
            kind: value
                .get("kind")
                .and_then(Value::as_str)
                .unwrap_or("provider_interaction")
                .to_string(),
            title: value
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or("Claude requests input")
                .to_string(),
            detail: value
                .get("detail")
                .and_then(Value::as_str)
                .unwrap_or("Claude needs additional input.")
                .to_string(),
            payload: value.get("payload").cloned().unwrap_or(Value::Null),
        });
    }
    if value.get("type").and_then(Value::as_str) != Some("borg_permission_request") {
        return None;
    }
    let approval_id = value.get("approval_id")?.as_str()?.to_string();
    let title = value
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or("Claude tool permission")
        .to_string();
    let detail = value
        .get("detail")
        .and_then(Value::as_str)
        .unwrap_or("Claude requested permission to use a tool.")
        .to_string();
    let command = value
        .get("command")
        .and_then(Value::as_str)
        .map(str::to_string);
    Some(ChatStreamEvent::ApprovalRequested {
        approval_id,
        title,
        detail,
        command,
    })
}

async fn write_claude_control(stdin: &mut tokio::process::ChildStdin, value: &Value) -> Result<()> {
    stdin.write_all(&serde_json::to_vec(value)?).await?;
    stdin.write_all(b"\n").await?;
    stdin.flush().await?;
    Ok(())
}

async fn run_codex_app_server_inner(
    req: ChatStreamRequest,
    tx: mpsc::Sender<ChatStreamEvent>,
    control_rx: Option<mpsc::Receiver<ChatStreamControl>>,
    local_auth: bool,
    permission: LocalAgentPermission,
    pool: Option<CodexAppServerPool>,
    cancellation: Arc<AtomicBool>,
    active_worker_guard: Option<CodexActiveWorkerGuard>,
) -> Result<()> {
    // Codex only supports the `Direct` channel today. When OpenAI's
    // self-serve ZDR path lands (or when we wire up Azure OpenAI's
    // Responses API against the Codex CLI binary), flip this gate.
    // We refuse rather than silently downgrade so an Enterprise customer
    // who expected ZDR can't quietly end up on the standard-retention
    // rails for their GPT runs.
    match req.provider_channel {
        ProviderChannel::Direct => {}
        ProviderChannel::AzureOpenAi => {
            bail!(
                "Azure OpenAI channel routing is not yet wired for Codex runs. \
                 Set BORG_FORCE_PROVIDER_CHANNEL=direct for now, or use a Claude \
                 backend (which has Vertex AI routing on Enterprise)."
            );
        }
        other => {
            bail!(
                "{other} channel is not supported for Codex; Codex routes through \
                 the direct OpenAI API only"
            );
        }
    }
    let _cancel_worker_on_drop = CancelCodexWorkerOnDrop(Arc::clone(&cancellation));
    tokio::task::spawn_blocking(move || -> Result<()> {
        // Declared before the client so this guard is dropped last. Its
        // completion signal therefore means any cancelled app-server process
        // tree has already been killed and reaped.
        let _active_worker_guard = active_worker_guard;
        let started_at = Instant::now();
        let mut control_rx = control_rx;
        let provider_home = tempfile::tempdir().context("failed to create Codex provider home")?;
        let workspace_dir = req
            .working_directory
            .clone()
            .unwrap_or_else(|| provider_home.path().to_path_buf());
        fs::create_dir_all(&workspace_dir).with_context(|| {
            format!(
                "failed to create Codex workspace directory {}",
                workspace_dir.display()
            )
        })?;
        let mcp_setup = prepare_request_mcp(provider_home.path(), &req, local_auth)?;
        tracing::debug!(
            target: "borg_ttft",
            stage = "codex_request_mcp_ready",
            elapsed_ms = started_at.elapsed().as_millis(),
            "Codex request stage"
        );
        let mcp_config_path = mcp_setup
            .claude_config_path
            .as_ref()
            .map(|path| path.to_string_lossy().to_string());
        let git_env = prepare_git_credential_env(provider_home.path(), &req.git_credentials)
            .context("failed to prepare git credential helper")?;
        let model = req
            .model
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .or_else(|| default_model_for_backend("codex"));
        let effort = match req.effort.as_deref().map(str::trim) {
            Some(value) if crate::codex_effort_supported(value) => Some(value.to_string()),
            _ => default_effort_for_backend("codex"),
        };
        let mut mapper = CodexStreamMapper::default();
        let working_directory = workspace_dir.to_string_lossy().to_string();
        let codex_auth = resolve_codex_auth_home(
            provider_home.path(),
            req.provider_auth.as_ref(),
            &mcp_setup,
            local_auth,
        )?;
        tracing::debug!(
            target: "borg_ttft",
            stage = "codex_request_setup_ready",
            elapsed_ms = started_at.elapsed().as_millis(),
            "Codex request stage"
        );

        let pool_key = CodexAppServerKey {
            model: model.clone(),
            effort: effort.clone(),
            working_directory: working_directory.clone(),
            permission,
            web_search_allowed: req.web_search_allowed,
        };
        let mut pooled = pool.as_ref().and_then(|pool| {
            pool.inner
                .lock()
                .expect("Codex app-server pool lock poisoned")
                .take()
        });
        let can_reuse = pooled.as_ref().is_some_and(|pooled| {
            pooled.key == pool_key
                && pooled.owner_session_id == req.owner_session_id
                && req.session_id.as_deref().is_some_and(|session_id| {
                    pooled.client.workspace_id() == Some(session_id)
                })
        });
        if !can_reuse
            && let Some(mut stale) = pooled.take()
        {
            stale
                .client
                .attach_cancellation(Arc::clone(&cancellation));
            if let Err(error) = stale.client.shutdown() {
                tracing::warn!(?error, "failed to shut down stale Codex app-server");
            }
        }
        if cancellation.load(Ordering::Acquire) {
            return Ok(());
        }
        let (mut client, client_source) = if let Some(mut pooled) = pooled {
            pooled
                .client
                .attach_cancellation(Arc::clone(&cancellation));
            (pooled.client, "pooled_thread")
        } else if let Some(prewarmed) = pool
            .as_ref()
            .and_then(|pool| pool.take_prewarmed(&cancellation))
        {
            match prewarmed {
                Ok(client) => (client, "prewarmed_process"),
                Err(error) => {
                    if cancellation.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    tracing::warn!(?error, "Codex app-server prewarm failed; retrying inline");
                    (
                        CodexAppServerClient::start_with_cancellation(
                            true,
                            req.web_search_allowed,
                            codex_auth.codex_home_override.as_deref(),
                            codex_auth.use_managed_openai_api_key,
                            &git_env,
                            Some(Arc::clone(&cancellation)),
                        )
                        .context("failed to start codex app-server")?,
                        "cold_spawn",
                    )
                }
            }
        } else {
            (
                CodexAppServerClient::start_with_cancellation(
                    true,
                    req.web_search_allowed,
                    codex_auth.codex_home_override.as_deref(),
                    codex_auth.use_managed_openai_api_key,
                    &git_env,
                    Some(Arc::clone(&cancellation)),
                )
                .context("failed to start codex app-server")?,
                "cold_spawn",
            )
        };
        client.attach_cancellation(Arc::clone(&cancellation));
        tracing::debug!(
            target: "borg_ttft",
            stage = "codex_client_ready",
            elapsed_ms = started_at.elapsed().as_millis(),
            source = client_source,
            "Codex request stage"
        );
        if cancellation.load(Ordering::Acquire) {
            return Ok(());
        }
        let persist_session = req.persist_session.unwrap_or(true);
        let mut turn_prompt = req.prompt.clone();
        let mut first_notification = true;
        let mut on_notification = |message: &JsonRpcMessage| {
            if first_notification {
                first_notification = false;
                tracing::debug!(
                    target: "borg_ttft",
                    stage = "codex_first_notification",
                    elapsed_ms = started_at.elapsed().as_millis(),
                    method = message.method.as_deref().unwrap_or_default(),
                    "Codex request stage"
                );
            }
            mapper.handle(message, &tx)
        };
        let mut resumed_turn_result = None;
        let resumed = if can_reuse {
            true
        } else if let Some(session_id) = req.session_id.as_deref()
            && !session_id.trim().is_empty()
        {
            if cancellation.load(Ordering::Acquire) {
                return Ok(());
            }
            match client.thread_resume_with_permission_streaming(
                session_id,
                &req.system_prompt,
                model.as_deref(),
                effort.as_deref(),
                mcp_config_path.as_deref(),
                req.fast,
                &working_directory,
                permission,
                req.output_schema.is_some(),
                |message| on_notification(message),
            ) {
                Ok((_, result)) => {
                    resumed_turn_result = result;
                    true
                }
                Err(error) => {
                    if cancellation.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    tracing::warn!(
                        %error,
                        session_id,
                        "codex app-server thread resume failed; starting a fresh thread"
                    );
                    let fallback_prompt = req.resume_unavailable_prompt.as_ref().with_context(
                        || {
                            format!(
                                "codex app-server could not resume thread {session_id}; refusing to discard conversation context"
                            )
                        },
                    )?;
                    turn_prompt = fallback_prompt.clone();
                    client
                        .thread_start_with_permission(
                            &req.system_prompt,
                            model.as_deref(),
                            effort.as_deref(),
                            mcp_config_path.as_deref(),
                            req.fast,
                            persist_session,
                            permission,
                        )
                        .context("failed to start codex app-server fallback thread")?;
                    false
                }
            }
        } else {
            if cancellation.load(Ordering::Acquire) {
                return Ok(());
            }
            client
                .thread_start_with_permission(
                    &req.system_prompt,
                    model.as_deref(),
                    effort.as_deref(),
                    mcp_config_path.as_deref(),
                    req.fast,
                    persist_session,
                    permission,
                )
                .context("failed to start codex app-server thread")?;
            false
        };
        tracing::debug!(
            target: "borg_ttft",
            stage = "codex_thread_ready",
            elapsed_ms = started_at.elapsed().as_millis(),
            resumed,
            "Codex request stage"
        );
        if cancellation.load(Ordering::Acquire) {
            return Ok(());
        }

        if can_reuse {
            client
                .settle_pooled_thread_before_input()
                .context("failed to establish an idle pooled Codex thread")?;
        }

        let result = if let Some(result) = resumed_turn_result {
            result
        } else {
            client
                .turn_execute_streaming_with_schema_steering_and_attachments(
                    CodexTurnInput {
                        prompt: &turn_prompt,
                        attachments: &req.attachments,
                        client_user_message_id: req.client_user_message_id.as_deref(),
                    },
                    &working_directory,
                    req.output_schema.as_ref(),
                    control_rx.as_mut(),
                    |message| on_notification(message),
                )
                .context("codex app-server turn failed")?
        };
        if cancellation.load(Ordering::Acquire) {
            return Ok(());
        }
        if let Some(pool) = pool.as_ref() {
            // `turn/completed` is not enough to establish Borg's Ready
            // boundary: a provider extension can start another turn from its
            // idle hook. Reconcile the bounded live turn page and stop any
            // unowned continuation before transferring this client to the
            // idle pool.
            client
                .settle_pooled_thread_before_input()
                .context("failed to establish an idle Codex thread after turn completion")?;
            if cancellation.load(Ordering::Acquire) {
                return Ok(());
            }
            client.detach_cancellation();
            *pool
                .inner
                .lock()
                .expect("Codex app-server pool lock poisoned") =
                Some(PooledCodexAppServer {
                    client,
                    key: pool_key,
                    owner_session_id: req.owner_session_id.clone(),
                });
        } else if let Err(error) = client.shutdown() {
            tracing::warn!(?error, "failed to shut down codex app-server after streamed turn");
        }
        let duration_ms = elapsed_millis_u64(started_at);
        let usage = codex_turn_usage(
            result.turn_token_usage.as_ref(),
            result.total_token_usage.as_ref(),
            result.model_context_window,
            model.as_deref(),
            duration_ms,
        )
        .or_else(|| {
            Some(ProviderCallUsage {
                duration_ms,
                ..ProviderCallUsage::default()
            })
        });

        // Codex app-server persists non-ephemeral thread ids in CODEX_HOME.
        // Surface the id so later calls can resume provider-side context.
        let session_id = Some(result.workspace_id.clone()).filter(|s| !s.is_empty());
        if tx
            .blocking_send(ChatStreamEvent::Done {
                final_text: result.output_text,
                usage,
                session_id,
            })
            .is_err()
        {
            return Ok(());
        }
        Ok(())
    })
    .await
    .context("codex app-server worker panicked")?
}

struct CodexAuthHomeResolution {
    codex_home_override: Option<PathBuf>,
    use_managed_openai_api_key: bool,
}

fn prepare_request_mcp(
    provider_home: &Path,
    req: &ChatStreamRequest,
    local_auth: bool,
) -> Result<ProviderMcpSetup> {
    let explicitly_configured = req.mcp_owner_id.is_some()
        || !req.mcp_allowed_scopes.is_empty()
        || req.mcp_user_id.is_some()
        || !req.mcp_external_servers.is_empty()
        || req.mcp_api_token.is_some();
    if local_auth && !explicitly_configured {
        return Ok(ProviderMcpSetup::default());
    }
    if local_auth
        && std::env::var_os("BORG_API_BASE_URL").is_none()
        && !req.mcp_external_servers.is_empty()
        && req.mcp_owner_id.is_none()
        && req.mcp_allowed_scopes.is_empty()
        && req.mcp_user_id.is_none()
        && req.mcp_api_token.is_none()
    {
        return prepare_external_provider_mcp(provider_home, &req.mcp_external_servers)
            .context("failed to prepare local MCP config");
    }
    prepare_provider_mcp_with_scope(
        provider_home,
        req.mcp_owner_id.as_deref(),
        &req.mcp_allowed_scopes,
        req.mcp_user_id.as_deref(),
        &req.mcp_external_servers,
        req.mcp_api_token.as_deref(),
    )
    .context("failed to prepare MCP config")
}

fn resolve_codex_auth_home(
    sandbox_path: &Path,
    provider_auth: Option<&ChatProviderAuth>,
    mcp_setup: &ProviderMcpSetup,
    force_local_codex_auth: bool,
) -> Result<CodexAuthHomeResolution> {
    let prefer_local_codex_auth = force_local_codex_auth || use_local_codex_auth();
    let provider_codex_home = provider_auth
        .and_then(|auth| {
            (auth.provider == ProviderAuthProvider::Openai)
                .then_some(auth.codex_home.as_ref())
                .flatten()
        })
        .filter(|home| codex_auth_file_in_home(home).exists())
        .cloned();
    let mut use_managed_openai_api_key = provider_auth
        .is_none_or(|auth| auth.provider != ProviderAuthProvider::Openai)
        && !prefer_local_codex_auth
        && provider_codex_home.is_none();

    let codex_home_override = if let Some(codex_home) = provider_codex_home {
        install_codex_mcp_config(mcp_setup.codex_home.as_deref(), &codex_home)
            .context("failed to install Codex MCP config into BYO auth home")?;
        use_managed_openai_api_key = false;
        Some(codex_home)
    } else if force_local_codex_auth {
        // The native executable is the authority for its local auth home.
        // This matters for packaged/wrapped Codex installations that select
        // a CODEX_HOME internally: forcing Borg's fallback path here makes
        // `/login` update one home while app-server reads another.
        use_managed_openai_api_key = false;
        None
    } else if prefer_local_codex_auth {
        let codex_home = local_codex_home().context("failed to resolve local Codex auth home")?;
        use_managed_openai_api_key = false;
        Some(codex_home)
    } else if let Some(auth) = provider_auth
        && auth.provider == ProviderAuthProvider::Openai
    {
        let auth_home = sandbox_path.join(".provider-auth-home");
        restore_bundle(ProviderAuthProvider::Openai, &auth.bundle, &auth_home)
            .context("failed to restore OpenAI provider auth bundle")?;
        let codex_home = ensure_codex_home(&auth_home)?;
        if codex_home_holds_chatgpt_session_checked(&codex_home)
            .context("failed to inspect restored OpenAI auth.json")?
        {
            // ChatGPT refresh tokens are single-use: running Codex against
            // a throwaway copy rotates the token inside the sandbox and
            // logs out every other holder of the session. Only the
            // persistent per-account CODEX_HOME may execute this auth.
            bail!(
                "BYO ChatGPT Codex session has no persistent CODEX_HOME on this \
                 server; refusing to run against a throwaway credential copy. \
                 Re-link the OpenAI account from Settings -> Provider auth."
            );
        }
        install_codex_mcp_config(mcp_setup.codex_home.as_deref(), &codex_home)
            .context("failed to install Codex MCP config into restored auth home")?;
        use_managed_openai_api_key = false;
        Some(codex_home)
    } else {
        match mcp_setup.codex_home.clone() {
            Some(home) => Some(home),
            None => {
                let home = sandbox_path.join(".codex-isolated-home");
                fs::create_dir_all(&home).context("failed to create isolated Codex home")?;
                Some(home)
            }
        }
    };

    Ok(CodexAuthHomeResolution {
        codex_home_override,
        use_managed_openai_api_key,
    })
}

fn use_local_codex_auth() -> bool {
    match std::env::var("BORG_CODEX_USE_LOCAL_AUTH")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("1" | "true" | "yes" | "on") => return true,
        Some("0" | "false" | "no" | "off") => return false,
        Some(_) => return false,
        None => {}
    }

    local_codex_auth_file().exists()
}

fn local_codex_home() -> Result<PathBuf> {
    let codex_home = local_codex_home_path();
    if !codex_home.exists() {
        bail!(
            "local Codex auth home does not exist: {}",
            codex_home.display()
        );
    }
    Ok(codex_home)
}

fn local_codex_home_path() -> PathBuf {
    std::env::var("CODEX_HOME")
        .ok()
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(".codex")
        })
}

fn local_codex_auth_file() -> PathBuf {
    codex_auth_file_in_home(&local_codex_home_path())
}

fn codex_auth_file_in_home(codex_home: &Path) -> PathBuf {
    codex_home.join("auth.json")
}

fn install_codex_mcp_config(
    source_codex_home: Option<&Path>,
    target_codex_home: &Path,
) -> Result<()> {
    let Some(source_codex_home) = source_codex_home else {
        return Ok(());
    };
    let source_config = source_codex_home.join("config.toml");
    if !source_config.exists() {
        return Ok(());
    }
    fs::create_dir_all(target_codex_home)
        .with_context(|| format!("failed to create {}", target_codex_home.display()))?;
    fs::copy(&source_config, target_codex_home.join("config.toml")).with_context(|| {
        format!(
            "failed to copy {} into {}",
            source_config.display(),
            target_codex_home.display()
        )
    })?;
    Ok(())
}

fn summarize_claude_provider_event(value: &Value) -> Value {
    let mut out = serde_json::Map::new();
    if let Some(kind) = value.get("type").and_then(Value::as_str) {
        out.insert("type".to_string(), Value::String(kind.to_string()));
    }
    if let Some(subtype) = value.get("subtype").and_then(Value::as_str) {
        out.insert("subtype".to_string(), Value::String(subtype.to_string()));
    }
    if let Some(session_id) = value.get("session_id").and_then(Value::as_str) {
        out.insert(
            "session_id".to_string(),
            Value::String(session_id.to_string()),
        );
    }
    if let Some(result) = value.get("result").and_then(Value::as_str) {
        out.insert("result_chars".to_string(), json!(result.chars().count()));
    }
    if let Some(message) = value.get("message")
        && let Some(content) = message.get("content").and_then(Value::as_array)
    {
        out.insert("content_blocks".to_string(), json!(content.len()));
        let block_types: Vec<_> = content
            .iter()
            .filter_map(|block| block.get("type").and_then(Value::as_str))
            .collect();
        out.insert("content_block_types".to_string(), json!(block_types));
    }
    Value::Object(out)
}

fn classify_claude_provider_event(value: &Value) -> ProviderEventTelemetry {
    let message_type = value.get("type").and_then(Value::as_str).unwrap_or("");
    match message_type {
        "stream_event" => {
            let Some(event) = value.get("event") else {
                return ProviderEventTelemetry::default();
            };
            if event.get("type").and_then(Value::as_str) == Some("content_block_delta")
                && let Some(delta) = event.get("delta")
            {
                let (stream_channel, content_text) =
                    match delta.get("type").and_then(Value::as_str).unwrap_or("") {
                        "text_delta" => (
                            Some("assistant_text".to_string()),
                            delta
                                .get("text")
                                .and_then(Value::as_str)
                                .map(str::to_string),
                        ),
                        "thinking_delta" => (
                            Some("reasoning".to_string()),
                            delta
                                .get("thinking")
                                .and_then(Value::as_str)
                                .map(str::to_string),
                        ),
                        _ => (None, None),
                    };
                if stream_channel.is_some() {
                    return ProviderEventTelemetry {
                        stream_channel,
                        content_text,
                        provider_item_id: event
                            .get("index")
                            .and_then(Value::as_i64)
                            .map(|index| index.to_string()),
                        ..ProviderEventTelemetry::default()
                    };
                }
            }
            ProviderEventTelemetry {
                stream_channel: Some("provider_event".to_string()),
                ..ProviderEventTelemetry::default()
            }
        }
        "assistant" => {
            let content = value
                .get("message")
                .and_then(|message| message.get("content"))
                .and_then(Value::as_array);
            if let Some(blocks) = content
                && let Some(tool) = blocks
                    .iter()
                    .find(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"))
            {
                return ProviderEventTelemetry {
                    stream_channel: Some("tool_call".to_string()),
                    provider_item_id: tool.get("id").and_then(Value::as_str).map(str::to_string),
                    tool_use_id: tool.get("id").and_then(Value::as_str).map(str::to_string),
                    tool_name: tool.get("name").and_then(Value::as_str).map(str::to_string),
                    content_text: tool.get("input").map(Value::to_string),
                };
            }
            ProviderEventTelemetry {
                stream_channel: Some("assistant_message".to_string()),
                content_text: content.map(|blocks| {
                    blocks
                        .iter()
                        .filter_map(extract_text_block)
                        .collect::<Vec<_>>()
                        .join("\n\n")
                }),
                ..ProviderEventTelemetry::default()
            }
        }
        "user" => {
            let content = value
                .get("message")
                .and_then(|message| message.get("content"))
                .and_then(Value::as_array);
            if let Some(blocks) = content
                && let Some(tool_result) = blocks
                    .iter()
                    .find(|block| block.get("type").and_then(Value::as_str) == Some("tool_result"))
            {
                return ProviderEventTelemetry {
                    stream_channel: Some("tool_result".to_string()),
                    tool_use_id: tool_result
                        .get("tool_use_id")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    content_text: Some(extract_tool_result_content(tool_result.get("content"))),
                    ..ProviderEventTelemetry::default()
                };
            }
            ProviderEventTelemetry::default()
        }
        "result" => ProviderEventTelemetry {
            stream_channel: Some("terminal".to_string()),
            content_text: value
                .get("result")
                .and_then(Value::as_str)
                .map(str::to_string),
            ..ProviderEventTelemetry::default()
        },
        "error" => ProviderEventTelemetry {
            stream_channel: Some("error".to_string()),
            content_text: value
                .get("message")
                .and_then(Value::as_str)
                .map(str::to_string),
            ..ProviderEventTelemetry::default()
        },
        _ => ProviderEventTelemetry::default(),
    }
}

fn extract_tool_result_content(content: Option<&Value>) -> String {
    let Some(content) = content else {
        return String::new();
    };
    match content {
        Value::String(text) => text.clone(),
        Value::Array(items) => {
            let mut out = String::new();
            for item in items {
                if let Some(text) = item.get("text").and_then(Value::as_str) {
                    if !out.is_empty() {
                        out.push('\n');
                    }
                    out.push_str(text);
                } else if let Value::String(text) = item {
                    if !out.is_empty() {
                        out.push('\n');
                    }
                    out.push_str(text);
                } else {
                    if !out.is_empty() {
                        out.push('\n');
                    }
                    out.push_str(&item.to_string());
                }
            }
            out
        }
        other => other.to_string(),
    }
}

fn extract_text_block(item: &Value) -> Option<String> {
    if item.get("type").and_then(Value::as_str) != Some("text") {
        return None;
    }
    item.get("text")
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn resolve_claude_sdk_provider_path() -> Result<PathBuf> {
    if let Ok(path) = std::env::var("BORG_CLAUDE_SDK_PROVIDER") {
        let path = PathBuf::from(path);
        if path.exists() {
            return path
                .canonicalize()
                .with_context(|| format!("failed to canonicalize {}", path.display()));
        }
    }

    let installed = std::env::var_os("BORG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".borg")))
        .map(|home| home.join("providers/claude-sdk/dist/provider.js"));
    if let Some(installed) = installed
        && installed.exists()
    {
        return installed
            .canonicalize()
            .with_context(|| format!("failed to canonicalize {}", installed.display()));
    }

    let adjacent_to_executable = std::env::current_exe()
        .ok()
        .and_then(|executable| executable.parent().map(Path::to_path_buf))
        .map(|bin_dir| bin_dir.join("providers/claude-sdk/dist/provider.js"));
    if let Some(adjacent) = adjacent_to_executable
        && adjacent.exists()
    {
        return adjacent
            .canonicalize()
            .with_context(|| format!("failed to canonicalize {}", adjacent.display()));
    }

    let manifest_relative = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../packages/borg-claude-sdk/dist/provider.js");
    if manifest_relative.exists() {
        return manifest_relative
            .canonicalize()
            .with_context(|| format!("failed to canonicalize {}", manifest_relative.display()));
    }

    let cwd_relative = std::env::current_dir()
        .unwrap_or_default()
        .join("packages/borg-claude-sdk/dist/provider.js");
    if cwd_relative.exists() {
        return cwd_relative
            .canonicalize()
            .with_context(|| format!("failed to canonicalize {}", cwd_relative.display()));
    }

    Err(anyhow!(
        "Claude Agent SDK provider is not installed; run `just claude-sdk` from a Borg CLI checkout or set BORG_CLAUDE_SDK_PROVIDER"
    ))
}

fn is_nonempty_session_id(id: &str) -> bool {
    !id.trim().is_empty()
}

fn read_mcp_servers_from_config(config_path: Option<&Path>) -> Result<Option<Value>> {
    let Some(config_path) = config_path else {
        return Ok(None);
    };
    let raw = super::read_provider_mcp_config_text(config_path)?;
    let value: Value = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse {}", config_path.display()))?;
    Ok(value.get("mcpServers").cloned())
}

fn prepare_git_credential_env(
    provider_home: &Path,
    credentials: &[ChatGitCredential],
) -> Result<Vec<(String, String)>> {
    let credentials = credentials
        .iter()
        .filter(|credential| {
            !credential.host.trim().is_empty()
                && !credential.username.trim().is_empty()
                && !credential.token.trim().is_empty()
                && !credential.host.chars().any(char::is_control)
        })
        .collect::<Vec<_>>();
    if credentials.is_empty() {
        return Ok(Vec::new());
    }
    let helper_path = provider_home.join("borg-git-askpass.sh");
    fs::write(
        &helper_path,
        r#"#!/bin/sh
prompt="${1:-}"
i=0
while [ "$i" -lt "${BORG_GIT_CREDENTIAL_COUNT:-0}" ]; do
  eval "host=\${BORG_GIT_HOST_$i:-}"
  case "$prompt" in
    *"$host"*)
      case "$prompt" in
        *Username*) eval "value=\${BORG_GIT_USERNAME_$i:-}" ;;
        *Password*) eval "value=\${BORG_GIT_TOKEN_$i:-}" ;;
        *) value="" ;;
      esac
      printf '%s\n' "$value"
      exit 0
      ;;
  esac
  i=$((i + 1))
done
printf '\n'
"#,
    )
    .with_context(|| format!("failed to write {}", helper_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&helper_path, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("failed to chmod {}", helper_path.display()))?;
    }

    let mut env = vec![
        ("GIT_ASKPASS".to_string(), helper_path.display().to_string()),
        ("SSH_ASKPASS".to_string(), helper_path.display().to_string()),
        ("GIT_TERMINAL_PROMPT".to_string(), "0".to_string()),
        (
            "BORG_GIT_CREDENTIAL_COUNT".to_string(),
            credentials.len().to_string(),
        ),
    ];
    for (index, credential) in credentials.into_iter().enumerate() {
        env.push((format!("BORG_GIT_HOST_{index}"), credential.host.clone()));
        env.push((
            format!("BORG_GIT_USERNAME_{index}"),
            credential.username.clone(),
        ));
        env.push((format!("BORG_GIT_TOKEN_{index}"), credential.token.clone()));
    }
    Ok(env)
}

fn apply_git_credential_env(command: &mut Command, env: &[(String, String)]) {
    for (key, value) in env {
        command.env(key, value);
    }
}

/// Flip the Claude Agent SDK binary onto Vertex or Bedrock by setting the
/// env vars the bundled `@anthropic-ai/claude-agent-sdk` reads at startup.
/// For `Direct` this is a no-op — the SDK hits the Anthropic API with the
/// credentials already on the process.
///
/// Missing credentials are treated as configuration errors rather than
/// silent fall-back to Direct. An Enterprise-tier run that expected Vertex
/// must fail loudly if `BORG_VERTEX_PROJECT_ID` isn't set, not quietly send
/// traffic to the wrong network.
fn apply_claude_channel_env(command: &mut Command, channel: ProviderChannel) -> Result<()> {
    match channel {
        ProviderChannel::Direct => Ok(()),
        ProviderChannel::Vertex => {
            let project_id = std::env::var("BORG_VERTEX_PROJECT_ID")
                .or_else(|_| std::env::var("ANTHROPIC_VERTEX_PROJECT_ID"))
                .context(
                    "Vertex channel selected but neither BORG_VERTEX_PROJECT_ID nor \
                     ANTHROPIC_VERTEX_PROJECT_ID is set",
                )?;
            let region = std::env::var("BORG_VERTEX_REGION")
                .or_else(|_| std::env::var("CLOUD_ML_REGION"))
                .unwrap_or_else(|_| "global".to_string());
            command
                .env("CLAUDE_CODE_USE_VERTEX", "1")
                .env("ANTHROPIC_VERTEX_PROJECT_ID", project_id)
                .env("CLOUD_ML_REGION", region);
            if let Ok(creds) = std::env::var("BORG_VERTEX_CREDENTIALS_PATH")
                && !creds.trim().is_empty()
            {
                command.env("GOOGLE_APPLICATION_CREDENTIALS", creds);
            }
            Ok(())
        }
        ProviderChannel::Bedrock => {
            let region = std::env::var("BORG_BEDROCK_REGION")
                .or_else(|_| std::env::var("AWS_REGION"))
                .context(
                    "Bedrock channel selected but neither BORG_BEDROCK_REGION nor \
                     AWS_REGION is set",
                )?;
            command
                .env("CLAUDE_CODE_USE_BEDROCK", "1")
                .env("AWS_REGION", region);
            Ok(())
        }
        ProviderChannel::AzureOpenAi => {
            bail!(
                "AzureOpenAi channel is not supported for Claude; this channel is \
                 reserved for future Codex/OpenAI routing"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn stopping_an_owner_waits_for_provider_cleanup_acknowledgement() {
        let pool = CodexAppServerPool::default();
        let cancellation = Arc::new(AtomicBool::new(false));
        let guard = pool.register_active_worker("session-1".to_string(), Arc::clone(&cancellation));
        let cleanup_finished = Arc::new(AtomicBool::new(false));
        let cleanup_finished_worker = Arc::clone(&cleanup_finished);
        let worker = std::thread::spawn(move || {
            while !cancellation.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            std::thread::sleep(Duration::from_millis(25));
            cleanup_finished_worker.store(true, Ordering::Release);
            drop(guard);
        });

        pool.stop_owner("session-1")
            .expect("stop waits for provider cleanup");
        assert!(cleanup_finished.load(Ordering::Acquire));
        worker.join().expect("cleanup worker");
        assert!(
            !pool
                .active
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .contains_key("session-1")
        );
    }

    #[test]
    fn cancelling_a_turn_joins_an_inflight_codex_prewarm() {
        let pool = CodexAppServerPool::default();
        let (tx, receiver) = std_mpsc::sync_channel(1);
        let prewarm_cancellation = Arc::new(AtomicBool::new(false));
        let completed = Arc::new((Mutex::new(false), Condvar::new()));
        *pool.prewarm.lock().expect("Codex prewarm lock") = Some(CodexPrewarm {
            receiver,
            cancellation: Arc::clone(&prewarm_cancellation),
            completed: Arc::clone(&completed),
        });
        let cleanup_finished = Arc::new(AtomicBool::new(false));
        let cleanup_finished_worker = Arc::clone(&cleanup_finished);
        let worker = std::thread::spawn(move || {
            let _completion = CodexPrewarmCompletion(completed);
            while !prewarm_cancellation.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            std::thread::sleep(Duration::from_millis(25));
            cleanup_finished_worker.store(true, Ordering::Release);
            let _ = tx.send(Err(anyhow!("cancelled test prewarm")));
        });
        let owner_cancellation = AtomicBool::new(true);

        let error = match pool
            .take_prewarmed(&owner_cancellation)
            .expect("prewarm existed")
        {
            Ok(_) => panic!("owner cancellation must reject the prewarm"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("cancelled with its owning turn"));
        assert!(cleanup_finished.load(Ordering::Acquire));
        worker.join().expect("prewarm worker");
    }

    struct TestEnvGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl TestEnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var(key).ok();
            unsafe { std::env::set_var(key, value) };
            Self { key, previous }
        }
    }

    impl Drop for TestEnvGuard {
        fn drop(&mut self) {
            unsafe {
                match self.previous.as_deref() {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    fn test_claude_request(workspace: &Path) -> ChatStreamRequest {
        ChatStreamRequest {
            prompt: "hello".to_string(),
            owner_session_id: None,
            client_user_message_id: None,
            attachments: Vec::new(),
            model: Some("test-model".to_string()),
            effort: Some("medium".to_string()),
            fast: false,
            system_prompt: "test system".to_string(),
            output_schema: None,
            mcp_owner_id: None,
            mcp_allowed_scopes: Vec::new(),
            mcp_user_id: None,
            mcp_external_servers: Vec::new(),
            mcp_api_token: None,
            provider_auth: None,
            git_credentials: Vec::new(),
            working_directory: Some(workspace.to_path_buf()),
            session_id: None,
            provider_channel: ProviderChannel::Direct,
            persist_session: Some(false),
            web_search_allowed: false,
            resume_unavailable_prompt: None,
        }
    }

    async fn final_text(mut stream: mpsc::Receiver<ChatStreamEvent>) -> String {
        while let Some(event) = stream.recv().await {
            match event {
                ChatStreamEvent::Done { final_text, .. } => return final_text,
                ChatStreamEvent::Failed { error } => panic!("Claude stream failed: {error}"),
                _ => {}
            }
        }
        panic!("Claude stream ended without Done")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pooled_claude_adapter_reuses_one_process_across_turns() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        let root = tempfile::tempdir().unwrap();
        let adapter = root.path().join("fake-claude-adapter.mjs");
        fs::write(
            &adapter,
            r#"
import { createInterface } from "node:readline";
let count = 0;
for await (const line of createInterface({ input: process.stdin })) {
  const wire = JSON.parse(line);
  const config = wire.type === "start" ? wire.config : wire;
  if (!config.prompt) continue;
  count += 1;
  process.stdout.write(JSON.stringify({
    type: "system", subtype: "init", session_id: "session-1"
  }) + "\n");
  process.stdout.write(JSON.stringify({
    type: "result", subtype: "success", result: `process-turn-${count}`,
    session_id: "session-1"
  }) + "\n");
}
"#,
        )
        .unwrap();
        let _adapter = TestEnvGuard::set("BORG_CLAUDE_SDK_PROVIDER", adapter.to_str().unwrap());
        let pool = ClaudeSdkPool::default();

        let first = final_text(run_pooled_claude_local_chat_stream(
            test_claude_request(root.path()),
            None,
            LocalAgentPermission::FullAccess,
            pool.clone(),
        ))
        .await;
        let second = final_text(run_pooled_claude_local_chat_stream(
            test_claude_request(root.path()),
            None,
            LocalAgentPermission::FullAccess,
            pool,
        ))
        .await;

        assert_eq!(first, "process-turn-1");
        assert_eq!(second, "process-turn-2");
    }

    #[test]
    fn pooled_claude_config_preserves_resume_attachments_schema_and_permissions() {
        let root = tempfile::tempdir().unwrap();
        let attachment = root.path().join("screen.png");
        fs::write(&attachment, b"image").unwrap();
        let mut request = test_claude_request(root.path());
        request.attachments = vec![attachment.clone()];
        request.session_id = Some("claude-session-1".to_string());
        request.output_schema = Some(json!({
            "type": "object",
            "properties": { "ok": { "type": "boolean" } },
            "required": ["ok"]
        }));

        let config =
            build_pooled_claude_config(&request, root.path(), LocalAgentPermission::Manual)
                .expect("Claude config");

        assert_eq!(config["attachments"], json!([attachment]));
        assert_eq!(config["resume"], "claude-session-1");
        assert_eq!(config["permission_mode"], "default");
        assert_eq!(config["persist_session"], false);
        assert_eq!(config["output_schema"]["required"], json!(["ok"]));
    }

    #[test]
    fn claude_adapter_permission_request_maps_to_provider_neutral_approval() {
        let event = claude_adapter_event(&json!({
            "type": "borg_permission_request",
            "approval_id": "permission-1",
            "tool_name": "Bash",
            "title": "Run command",
            "detail": "Claude wants to run cargo test.",
            "command": "cargo test"
        }))
        .expect("permission event");

        assert!(matches!(
            event,
            ChatStreamEvent::ApprovalRequested {
                approval_id,
                title,
                detail,
                command: Some(command),
            } if approval_id == "permission-1"
                && title == "Run command"
                && detail == "Claude wants to run cargo test."
                && command == "cargo test"
        ));
    }

    #[test]
    fn claude_adapter_context_usage_maps_to_transient_provider_telemetry() {
        let event = claude_adapter_event(&json!({
            "type": "borg_context_usage",
            "total_tokens": 12_345,
            "context_window_tokens": 200_000,
            "model": "claude-sonnet-5"
        }))
        .expect("context usage event");

        assert!(matches!(
            event,
            ChatStreamEvent::ProviderEvent {
                ref kind,
                ref payload,
                ref stream_channel,
                ..
            } if kind == "claude.context_usage"
                && payload["total_tokens"] == 12_345
                && stream_channel.as_deref() == Some("usage")
        ));
    }

    #[test]
    fn claude_adapter_elicitation_maps_to_provider_neutral_interaction() {
        let event = claude_adapter_event(&json!({
            "type": "borg_provider_interaction",
            "interaction_id": "elicitation-1",
            "kind": "mcp_elicitation",
            "title": "Deployment region",
            "detail": "Choose a region.",
            "payload": {
                "serverName": "deploy",
                "requestedSchema": {"type": "object"}
            }
        }))
        .expect("provider interaction");

        assert!(matches!(
            event,
            ChatStreamEvent::ProviderInteractionRequested {
                interaction_id,
                kind,
                title,
                detail,
                payload,
            } if interaction_id == "elicitation-1"
                && kind == "mcp_elicitation"
                && title == "Deployment region"
                && detail == "Choose a region."
                && payload["serverName"] == "deploy"
        ));
    }

    #[test]
    fn git_askpass_helper_is_host_scoped_and_debug_redacted() {
        let dir = tempfile::tempdir().expect("provider home");
        let credential = ChatGitCredential {
            host: "github.com".to_string(),
            username: "x-access-token".to_string(),
            token: "secret-token".to_string(),
        };
        assert!(!format!("{credential:?}").contains("secret-token"));

        let env = prepare_git_credential_env(dir.path(), &[credential]).expect("git env");
        let helper = env
            .iter()
            .find_map(|(key, value)| (key == "GIT_ASKPASS").then_some(value))
            .expect("askpass path");
        let run_helper = |prompt: &str| {
            let mut command = std::process::Command::new(helper);
            command.arg(prompt);
            for (key, value) in &env {
                command.env(key, value);
            }
            let output = command.output().expect("run askpass helper");
            assert!(output.status.success());
            String::from_utf8(output.stdout).expect("utf8 stdout")
        };

        assert_eq!(
            run_helper("Username for 'https://github.com':"),
            "x-access-token\n"
        );
        assert_eq!(
            run_helper("Password for 'https://github.com':"),
            "secret-token\n"
        );
        assert_eq!(run_helper("Password for 'https://gitlab.com':"), "\n");
    }

    #[test]
    fn extracts_web_search_query_from_response_done_item() {
        let item = json!({
            "type": "web_search_call",
            "id": "ws-1",
            "status": "completed",
            "action": { "type": "search", "query": "weather seattle" }
        });

        assert_eq!(
            codex_search_query(&item).as_deref(),
            Some("weather seattle")
        );
        assert_eq!(
            codex_tool_completion_input("web_search_call", &item),
            Some(json!({ "query": "weather seattle" }))
        );
    }

    #[test]
    fn extracts_web_search_detail_from_open_page_action() {
        let item = json!({
            "type": "web_search_call",
            "id": "ws-2",
            "status": "completed",
            "action": { "type": "open_page", "url": "https://openai.com/api/" }
        });

        assert_eq!(
            codex_search_query(&item).as_deref(),
            Some("https://openai.com/api/")
        );
    }

    #[test]
    fn web_search_started_without_action_has_no_placeholder_query() {
        let item = json!({
            "type": "web_search_call",
            "id": "ws-start",
            "query": "",
            "action": null
        });

        assert_eq!(codex_search_query(&item), None);
        assert_eq!(
            codex_tool_signature("web_search_call", &item),
            ("web_search".to_string(), Value::Null)
        );
    }

    #[test]
    fn web_search_action_detail_precedes_top_level_query() {
        let item = json!({
            "type": "web_search_call",
            "id": "ws-open",
            "query": "weather seattle",
            "action": { "type": "open_page", "url": "https://example.com/weather" }
        });

        assert_eq!(
            codex_search_query(&item).as_deref(),
            Some("https://example.com/weather")
        );
    }

    #[test]
    fn extracts_web_search_detail_from_find_in_page_action() {
        let item = json!({
            "type": "web_search_call",
            "id": "ws-3",
            "status": "completed",
            "action": {
                "type": "find_in_page",
                "url": "https://openai.com/api/",
                "pattern": "models"
            }
        });

        assert_eq!(
            codex_search_query(&item).as_deref(),
            Some("models in https://openai.com/api/")
        );
    }

    #[test]
    fn skips_internal_codex_thread_items() {
        for item_type in [
            "userMessage",
            "hookPrompt",
            "enteredReviewMode",
            "exitedReviewMode",
        ] {
            assert!(should_skip_codex_item(item_type), "{item_type}");
        }
        assert!(!should_skip_codex_item("contextCompaction"));
    }

    #[test]
    fn mapper_surfaces_context_compaction_start_and_completion_phases() {
        let (tx, mut rx) = mpsc::channel(8);
        let mut mapper = CodexStreamMapper::default();
        let started = JsonRpcMessage {
            id: None,
            method: Some("item/started".to_string()),
            message: None,
            result: None,
            error: None,
            params: Some(json!({
                "item": {
                    "type": "contextCompaction",
                    "id": "compact-1",
                    "status": "started",
                    "summary": "Earlier conversation was compacted"
                }
            })),
        };
        let completed = JsonRpcMessage {
            id: None,
            method: Some("item/completed".to_string()),
            message: None,
            result: None,
            error: None,
            params: Some(json!({
                "item": {
                    "type": "contextCompaction",
                    "id": "compact-1",
                    "status": "completed"
                }
            })),
        };

        mapper.handle(&started, &tx).unwrap();
        mapper.handle(&completed, &tx).unwrap();
        drop(tx);

        let mut phases = Vec::new();
        while let Ok(event) = rx.try_recv() {
            if let ChatStreamEvent::Phase { name, input } = event {
                phases.push((name, input));
            }
        }
        assert_eq!(phases.len(), 2);
        assert_eq!(phases[0].0, "context_compaction");
        assert_eq!(
            phases[0].1.get("status").and_then(Value::as_str),
            Some("started")
        );
        assert_eq!(
            phases[1].1.get("status").and_then(Value::as_str),
            Some("completed")
        );
    }

    #[test]
    fn codex_and_claude_normalize_mcp_tool_lifecycle_identically() {
        let (codex_tx, mut codex_rx) = mpsc::channel(16);
        let mut codex = CodexStreamMapper::default();
        for message in [
            JsonRpcMessage {
                id: None,
                method: Some("item/started".to_string()),
                message: None,
                result: None,
                error: None,
                params: Some(json!({
                    "item": {
                        "type": "mcpToolCall",
                        "id": "tool-1",
                        "serverName": "borg",
                        "toolName": "read_file",
                        "input": {"path": "README.md"}
                    }
                })),
            },
            JsonRpcMessage {
                id: None,
                method: Some("item/completed".to_string()),
                message: None,
                result: None,
                error: None,
                params: Some(json!({
                    "item": {
                        "type": "mcpToolCall",
                        "id": "tool-1",
                        "serverName": "borg",
                        "toolName": "read_file",
                        "input": {"path": "README.md"},
                        "output": "contents"
                    }
                })),
            },
        ] {
            codex.handle(&message, &codex_tx).unwrap();
        }
        drop(codex_tx);
        let codex_events = {
            let mut events = Vec::new();
            while let Ok(event) = codex_rx.try_recv() {
                if !matches!(event, ChatStreamEvent::ProviderEvent { .. }) {
                    events.push(event);
                }
            }
            events
        };

        let claude_events = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let (claude_tx, mut claude_rx) = mpsc::channel(16);
                let mut claude = ClaudeStreamState::default();
                claude
                    .handle_message(
                        &json!({
                            "type": "assistant",
                            "message": {"content": [{
                                "type": "tool_use",
                                "id": "tool-1",
                                "name": "mcp__borg__read_file",
                                "input": {"path": "README.md"}
                            }]}
                        }),
                        &claude_tx,
                    )
                    .await
                    .unwrap();
                claude
                    .handle_message(
                        &json!({
                            "type": "user",
                            "message": {"content": [{
                                "type": "tool_result",
                                "tool_use_id": "tool-1",
                                "content": "contents",
                                "is_error": false
                            }]}
                        }),
                        &claude_tx,
                    )
                    .await
                    .unwrap();
                drop(claude_tx);
                let mut events = Vec::new();
                while let Ok(event) = claude_rx.try_recv() {
                    events.push(event);
                }
                events
            });

        assert!(
            matches!(codex_events.first(), Some(ChatStreamEvent::ToolCall { id, name, input }) if id == "tool-1" && name == "mcp__borg__read_file" && input["path"] == "README.md")
        );
        assert!(
            matches!(claude_events.first(), Some(ChatStreamEvent::ToolCall { id, name, input }) if id == "tool-1" && name == "mcp__borg__read_file" && input["path"] == "README.md")
        );
        assert!(
            matches!(codex_events.get(1), Some(ChatStreamEvent::ToolResult { tool_use_id, output, is_error: false, .. }) if tool_use_id == "tool-1" && output == "contents")
        );
        assert!(
            matches!(claude_events.get(1), Some(ChatStreamEvent::ToolResult { tool_use_id, output, is_error: false, .. }) if tool_use_id == "tool-1" && output == "contents")
        );
    }

    #[test]
    fn codex_and_claude_usage_maps_share_billing_buckets() {
        let claude = super::super::extract_claude_usage(&json!({
            "usage": {
                "input_tokens": 12,
                "cache_read_input_tokens": 3,
                "cache_creation_input_tokens": 2,
                "output_tokens": 4
            },
            "duration_ms": 23
        }));
        let codex = codex_turn_usage(
            Some(&super::super::TokenUsage {
                input_tokens: 17,
                cached_input_tokens: 3,
                cache_write_input_tokens: 2,
                output_tokens: 4,
                total_tokens: 21,
                ..Default::default()
            }),
            None,
            None,
            None,
            23,
        )
        .expect("Codex usage");

        assert_eq!(claude.duration_ms, codex.duration_ms);
        assert_eq!(claude.input_tokens, codex.input_tokens);
        assert_eq!(claude.cached_input_tokens, codex.cached_input_tokens);
        assert_eq!(
            claude.cache_creation_input_tokens,
            codex.cache_creation_input_tokens
        );
        assert_eq!(claude.output_tokens, codex.output_tokens);
        assert_eq!(claude.total_tokens, codex.total_tokens);
    }

    #[test]
    fn maps_collab_agent_tool_call_to_subagent_action() {
        let item = json!({
            "type": "collabAgentToolCall",
            "id": "call-1",
            "tool": "spawnAgent",
            "prompt": "reply ok",
            "senderThreadId": "sender",
            "receiverThreadIds": ["receiver"]
        });

        let (name, input) = codex_tool_signature("collabAgentToolCall", &item);

        assert_eq!(name, "collab_tool_call");
        assert_eq!(
            input.get("tool").and_then(Value::as_str),
            Some("spawnAgent")
        );
        assert_eq!(
            input.get("prompt").and_then(Value::as_str),
            Some("reply ok")
        );
    }

    #[test]
    fn maps_file_change_paths_from_changes() {
        let item = json!({
            "type": "fileChange",
            "id": "edit-1",
            "changes": [
                { "path": "src/lib.rs", "kind": "add", "diff": "pub fn added() {}\n" },
                { "path": "README.md", "kind": "update", "diff": "@@ -1 +1 @@\n-old\n+new" }
            ]
        });

        let (name, input) = codex_tool_signature("fileChange", &item);

        assert_eq!(name, "Edit");
        assert_eq!(
            input.get("file_path").and_then(Value::as_str),
            Some("src/lib.rs")
        );
        assert_eq!(
            input.get("paths").and_then(Value::as_array).map(Vec::len),
            Some(2)
        );
        assert_eq!(
            input.get("diff").and_then(Value::as_str),
            Some(
                "*** Add File: src/lib.rs\n+pub fn added() {}\n\n\
                 *** Update File: README.md\n@@ -1 +1 @@\n-old\n+new"
            )
        );
    }

    #[test]
    fn mapper_separates_distinct_agent_message_deltas() {
        let (tx, mut rx) = mpsc::channel(8);
        let mut mapper = CodexStreamMapper::default();
        let first = JsonRpcMessage {
            id: None,
            method: Some("item/agentMessage/delta".to_string()),
            message: None,
            result: None,
            error: None,
            params: Some(json!({ "itemId": "agent-1", "delta": "First." })),
        };
        let second = JsonRpcMessage {
            id: None,
            method: Some("item/agentMessage/delta".to_string()),
            message: None,
            result: None,
            error: None,
            params: Some(json!({ "itemId": "agent-2", "delta": "Second." })),
        };

        mapper.handle(&first, &tx).unwrap();
        mapper.handle(&second, &tx).unwrap();
        drop(tx);

        let mut chunks = Vec::new();
        while let Ok(event) = rx.try_recv() {
            if let ChatStreamEvent::Delta(chunk) = event {
                chunks.push(chunk);
            }
        }
        assert_eq!(chunks.join(""), "First.\n\nSecond.");
    }

    #[test]
    fn mapper_surfaces_raw_observable_reasoning_and_tool_argument_deltas() {
        let (tx, mut rx) = mpsc::channel(8);
        let mut mapper = CodexStreamMapper::default();
        let reasoning = JsonRpcMessage {
            id: None,
            method: Some("item/reasoning/summaryTextDelta".to_string()),
            message: None,
            result: None,
            error: None,
            params: Some(json!({ "itemId": "reason-1", "delta": "checking source" })),
        };
        let tool_args = JsonRpcMessage {
            id: None,
            method: Some("response.function_call_arguments.delta".to_string()),
            message: None,
            result: None,
            error: None,
            params: Some(json!({ "itemId": "call-1", "delta": "{\"document_model\"" })),
        };

        mapper.handle(&reasoning, &tx).unwrap();
        mapper.handle(&tool_args, &tx).unwrap();
        drop(tx);

        let first = rx.try_recv().expect("reasoning provider event");
        let second = rx.try_recv().expect("reasoning stream event");
        let third = rx.try_recv().expect("tool args provider event");
        match first {
            ChatStreamEvent::ProviderEvent {
                stream_channel,
                content_text,
                provider_item_id,
                raw_payload,
                ..
            } => {
                assert_eq!(stream_channel.as_deref(), Some("reasoning"));
                assert_eq!(content_text.as_deref(), Some("checking source"));
                assert_eq!(provider_item_id.as_deref(), Some("reason-1"));
                assert_eq!(
                    raw_payload
                        .as_ref()
                        .and_then(|value| value.pointer("/params/delta"))
                        .and_then(Value::as_str),
                    Some("checking source")
                );
            }
            other => panic!("expected provider event, got {other:?}"),
        }
        match second {
            ChatStreamEvent::ReasoningDelta(text) => {
                assert_eq!(text, "checking source");
            }
            other => panic!("expected reasoning delta, got {other:?}"),
        }
        match third {
            ChatStreamEvent::ProviderEvent {
                stream_channel,
                content_text,
                tool_use_id,
                raw_payload,
                ..
            } => {
                assert_eq!(stream_channel.as_deref(), Some("tool_arguments"));
                assert_eq!(content_text.as_deref(), Some("{\"document_model\""));
                assert_eq!(tool_use_id.as_deref(), Some("call-1"));
                assert_eq!(
                    raw_payload
                        .as_ref()
                        .and_then(|value| value.pointer("/params/delta"))
                        .and_then(Value::as_str),
                    Some("{\"document_model\"")
                );
            }
            other => panic!("expected provider event, got {other:?}"),
        }
    }

    #[test]
    fn local_codex_home_uses_live_codex_home() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|err| err.into_inner());
        let previous = std::env::var_os("CODEX_HOME");
        let home = tempfile::tempdir().expect("codex home");
        unsafe { std::env::set_var("CODEX_HOME", home.path()) };

        let resolved = local_codex_home().expect("local codex home");

        assert_eq!(resolved, home.path());
        unsafe {
            match previous {
                Some(value) => std::env::set_var("CODEX_HOME", value),
                None => std::env::remove_var("CODEX_HOME"),
            }
        }
    }

    #[test]
    fn forced_local_codex_auth_leaves_home_selection_to_the_executable() {
        let sandbox = tempfile::tempdir().expect("sandbox");
        let resolved =
            resolve_codex_auth_home(sandbox.path(), None, &ProviderMcpSetup::default(), true)
                .expect("resolve local auth");

        assert!(resolved.codex_home_override.is_none());
        assert!(!resolved.use_managed_openai_api_key);
    }

    #[test]
    fn local_codex_auth_auto_detects_debug_codex_home() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|err| err.into_inner());
        let previous_flag = std::env::var_os("BORG_CODEX_USE_LOCAL_AUTH");
        let previous_home = std::env::var_os("CODEX_HOME");
        let home = tempfile::tempdir().expect("codex home");
        std::fs::write(home.path().join("auth.json"), "{}").expect("auth json");
        unsafe {
            std::env::remove_var("BORG_CODEX_USE_LOCAL_AUTH");
            std::env::set_var("CODEX_HOME", home.path());
        }

        assert_eq!(use_local_codex_auth(), cfg!(debug_assertions));

        unsafe {
            match previous_flag {
                Some(value) => std::env::set_var("BORG_CODEX_USE_LOCAL_AUTH", value),
                None => std::env::remove_var("BORG_CODEX_USE_LOCAL_AUTH"),
            }
            match previous_home {
                Some(value) => std::env::set_var("CODEX_HOME", value),
                None => std::env::remove_var("CODEX_HOME"),
            }
        }
    }

    #[test]
    fn explicit_local_codex_auth_false_overrides_auto_detect() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|err| err.into_inner());
        let previous_flag = std::env::var_os("BORG_CODEX_USE_LOCAL_AUTH");
        let previous_home = std::env::var_os("CODEX_HOME");
        let home = tempfile::tempdir().expect("codex home");
        std::fs::write(home.path().join("auth.json"), "{}").expect("auth json");
        unsafe {
            std::env::set_var("BORG_CODEX_USE_LOCAL_AUTH", "0");
            std::env::set_var("CODEX_HOME", home.path());
        }

        assert!(!use_local_codex_auth());

        unsafe {
            match previous_flag {
                Some(value) => std::env::set_var("BORG_CODEX_USE_LOCAL_AUTH", value),
                None => std::env::remove_var("BORG_CODEX_USE_LOCAL_AUTH"),
            }
            match previous_home {
                Some(value) => std::env::set_var("CODEX_HOME", value),
                None => std::env::remove_var("CODEX_HOME"),
            }
        }
    }

    // ---------------- apply_claude_channel_env ----------------

    mod claude_channel_env {
        use super::*;

        struct EnvVarGuard {
            key: &'static str,
            previous: Option<String>,
        }

        impl EnvVarGuard {
            fn set(key: &'static str, value: &str) -> Self {
                let previous = std::env::var(key).ok();
                unsafe { std::env::set_var(key, value) };
                Self { key, previous }
            }
            fn unset(key: &'static str) -> Self {
                let previous = std::env::var(key).ok();
                unsafe { std::env::remove_var(key) };
                Self { key, previous }
            }
        }

        impl Drop for EnvVarGuard {
            fn drop(&mut self) {
                unsafe {
                    match self.previous.as_deref() {
                        Some(value) => std::env::set_var(self.key, value),
                        None => std::env::remove_var(self.key),
                    }
                }
            }
        }

        #[test]
        fn direct_channel_is_a_no_op() {
            let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let mut command = Command::new("true");
            apply_claude_channel_env(&mut command, ProviderChannel::Direct).expect("direct is ok");
            // No easy way to assert the Command didn't mutate other than
            // "it didn't panic" — this guards against a regression that adds
            // unconditional env vars.
        }

        #[test]
        fn vertex_without_project_id_fails_fast() {
            let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let _g1 = EnvVarGuard::unset("BORG_VERTEX_PROJECT_ID");
            let _g2 = EnvVarGuard::unset("ANTHROPIC_VERTEX_PROJECT_ID");
            let mut command = Command::new("true");
            let err = apply_claude_channel_env(&mut command, ProviderChannel::Vertex)
                .expect_err("missing project id should fail");
            let message = format!("{err:#}");
            assert!(
                message.contains("Vertex channel selected"),
                "unexpected error: {message}"
            );
        }

        #[test]
        fn vertex_uses_configured_project_and_region() {
            let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let _g1 = EnvVarGuard::set("BORG_VERTEX_PROJECT_ID", "proj-test-42");
            let _g2 = EnvVarGuard::set("BORG_VERTEX_REGION", "europe-west1");
            let mut command = Command::new("true");
            apply_claude_channel_env(&mut command, ProviderChannel::Vertex)
                .expect("vertex config should succeed");
            // Command doesn't expose its env map publicly, so we rely on
            // the success path + hand-off to the spawned binary. Failure
            // to read project id would have bailed above.
        }

        #[test]
        fn bedrock_without_region_fails_fast() {
            let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let _g1 = EnvVarGuard::unset("BORG_BEDROCK_REGION");
            let _g2 = EnvVarGuard::unset("AWS_REGION");
            let mut command = Command::new("true");
            let err = apply_claude_channel_env(&mut command, ProviderChannel::Bedrock)
                .expect_err("missing region should fail");
            let message = format!("{err:#}");
            assert!(
                message.contains("Bedrock channel selected"),
                "unexpected error: {message}"
            );
        }

        #[test]
        fn bedrock_with_region_succeeds() {
            let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let _g1 = EnvVarGuard::set("BORG_BEDROCK_REGION", "us-east-1");
            let mut command = Command::new("true");
            apply_claude_channel_env(&mut command, ProviderChannel::Bedrock)
                .expect("bedrock with region should succeed");
        }

        #[test]
        fn azure_openai_is_rejected_for_claude() {
            let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let mut command = Command::new("true");
            let err = apply_claude_channel_env(&mut command, ProviderChannel::AzureOpenAi)
                .expect_err("azure openai is not a claude channel");
            let message = format!("{err:#}");
            assert!(
                message.contains("AzureOpenAi channel is not supported for Claude"),
                "unexpected error: {message}"
            );
        }
    }

    // ---------------- provider session_id gate ----------------
    mod sdk_session_id {
        use super::super::is_nonempty_session_id;

        #[test]
        fn accepts_returned_provider_ids() {
            assert!(is_nonempty_session_id("ses-abc123"));
            assert!(is_nonempty_session_id("sess_abc"));
            assert!(is_nonempty_session_id("conv_12345"));
            assert!(is_nonempty_session_id("cnv_xyz"));
            assert!(is_nonempty_session_id(
                "550e8400-e29b-41d4-a716-446655440000"
            ));
        }

        #[test]
        fn rejects_empty_and_whitespace() {
            assert!(!is_nonempty_session_id(""));
            assert!(!is_nonempty_session_id("   "));
        }
    }
}
