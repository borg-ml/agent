//! Streaming chat runners for the typed Codex app-server and native
//! `claude-agents` paths.

use crate::{ProviderAuthBundle, ProviderAuthProvider, ProviderChannel};
use anyhow::{Context, Result, anyhow, bail};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc as std_mpsc,
};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use super::codex_app_server::{CodexAppServerClient, CodexTurnInput, JsonRpcMessage};
mod claude_native;
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
#[cfg(test)]
use codex_items::*;
use codex_stream::{CodexStreamMapper, codex_turn_usage};

const CODEX_PREWARM_TIMEOUT: Duration = Duration::from_secs(45);
const CODEX_FORCE_STOP_TIMEOUT: Duration = Duration::from_secs(2);

#[cfg(test)]
pub(super) static CLAUDE_ENV_LOCK: Mutex<()> = Mutex::new(());

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
pub struct ClaudeAgentsPool {
    native_inner: claude_agents::ClaudePool,
}

pub fn run_pooled_claude_local_chat_stream(
    req: ChatStreamRequest,
    control_rx: Option<mpsc::Receiver<ChatStreamControl>>,
    permission: LocalAgentPermission,
    pool: ClaudeAgentsPool,
) -> mpsc::Receiver<ChatStreamEvent> {
    let (tx, rx) = mpsc::channel::<ChatStreamEvent>(64);
    tokio::spawn(async move {
        let result = claude_native::run_pooled(
            req,
            tx.clone(),
            control_rx,
            permission,
            pool.native_inner.clone(),
        )
        .await;
        if let Err(error) = result {
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
        let result = claude_native::run(req, tx.clone(), control_rx, local_auth, permission).await;
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
    /// System instructions and external MCP definitions are part of the
    /// provider thread bootstrap. A pooled client must be replaced when live
    /// Blu reloads change either input.
    context_fingerprint: String,
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
            context_fingerprint: codex_context_fingerprint(&req),
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
        if !result.discard_client {
            if let Some(pool) = pool.as_ref() {
                // `turn/completed` is not enough to establish Borg's Ready
                // boundary: a provider extension can start another turn from
                // its idle hook. Reconcile the bounded live turn page and stop
                // any unowned continuation before transferring this client to
                // the idle pool.
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

fn codex_context_fingerprint(req: &ChatStreamRequest) -> String {
    let mut digest = Sha256::new();
    digest.update(b"borg-codex-context-v1");
    hash_fingerprint_part(&mut digest, &req.system_prompt);
    hash_fingerprint_optional(&mut digest, req.mcp_owner_id.as_deref());
    hash_fingerprint_optional(&mut digest, req.mcp_user_id.as_deref());
    hash_fingerprint_optional(&mut digest, req.mcp_api_token.as_deref());
    for scope in &req.mcp_allowed_scopes {
        hash_fingerprint_part(&mut digest, scope);
    }
    let mut servers = req.mcp_external_servers.iter().collect::<Vec<_>>();
    servers.sort_by(|left, right| left.name.cmp(&right.name));
    for server in servers {
        hash_fingerprint_part(&mut digest, &server.name);
        hash_fingerprint_part(&mut digest, &server.command);
        for argument in &server.args {
            hash_fingerprint_part(&mut digest, argument);
        }
        for (key, value) in &server.env {
            hash_fingerprint_part(&mut digest, key);
            hash_fingerprint_part(&mut digest, value);
        }
        for tool in &server.allowed_tools {
            hash_fingerprint_part(&mut digest, tool);
        }
    }
    hex::encode(digest.finalize())
}

fn hash_fingerprint_part(digest: &mut Sha256, value: impl AsRef<[u8]>) {
    let value = value.as_ref();
    digest.update((value.len() as u64).to_le_bytes());
    digest.update(value);
}

fn hash_fingerprint_optional(digest: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            digest.update([1]);
            hash_fingerprint_part(digest, value);
        }
        None => digest.update([0]),
    }
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
    let target_config = target_codex_home.join("config.toml");
    let config = fs::read(&source_config).with_context(|| {
        format!(
            "failed to read {} for installation into {}",
            source_config.display(),
            target_codex_home.display()
        )
    })?;
    crate::mcp::write_private_file(&target_config, &config).with_context(|| {
        format!(
            "failed to install {} into {}",
            source_config.display(),
            target_codex_home.display()
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(target_codex_home, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("failed to secure {}", target_codex_home.display()))?;
        fs::set_permissions(&target_config, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to secure {}", target_config.display()))?;
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    use super::CLAUDE_ENV_LOCK as ENV_LOCK;

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
    fn codex_and_claude_usage_maps_share_billing_buckets() {
        let claude = claude_agents::extract_usage(&json!({
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
}
