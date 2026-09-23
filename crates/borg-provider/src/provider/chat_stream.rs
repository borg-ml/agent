//! Claude and OpenCode command adapters and subscription account metadata.
//!
//! These adapters intentionally keep subscription authentication and execution at
//! the CLI boundary. They launch the providers' native streaming protocols as
//! thin wire adapters and do not own Borg's tool/runtime loop; the
//! provider-neutral NativeHarness owns API-key/OpenAI-compatible model routes.

#![cfg_attr(not(any(feature = "codex", feature = "claude")), allow(dead_code))]

use crate::mcp::{ExternalMcpServer, ProviderMcpSetup, prepare_external_provider_mcp};
use crate::runtime::ProviderCallUsage;
use crate::{ProviderAuthBundle, ProviderAuthProvider, ProviderChannel};
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::ChildStdin;
use tokio::sync::{Mutex, mpsc};

#[path = "opencode_stream.rs"]
mod opencode_stream;

pub use opencode_stream::run_opencode_local_chat_stream;

#[path = "grok_stream.rs"]
mod grok_stream;

pub use grok_stream::run_grok_local_chat_stream;

#[path = "muse_stream.rs"]
mod muse_stream;

pub use muse_stream::run_muse_local_chat_stream;

#[cfg(not(feature = "claude"))]
#[allow(dead_code)]
mod claude_agents {
    use std::path::PathBuf;

    use anyhow::{Result, bail};
    use serde_json::Value;

    #[derive(Debug, Clone)]
    pub struct CommandSpec {
        pub program: PathBuf,
        pub args: Vec<String>,
        pub current_dir: PathBuf,
        pub environment: Vec<(String, String)>,
        pub environment_remove: Vec<String>,
    }

    #[derive(Debug, Clone)]
    pub struct ChatStreamRequest {
        pub prompt: String,
        pub attachments: Vec<PathBuf>,
        pub system_prompt: String,
        pub command: CommandSpec,
        pub runtime_directory: Option<()>,
        pub lifecycle_key: String,
    }

    #[derive(Debug, Clone, Copy)]
    pub enum ChatApprovalDecision {
        ApproveOnce,
        ApproveSession,
        Reject,
    }

    #[derive(Debug)]
    pub enum ChatStreamControl {
        Steer {
            text: String,
            attachments: Vec<PathBuf>,
            message_id: Option<String>,
            preempt: bool,
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

    #[derive(Debug, Clone, Default)]
    pub struct ProviderCallUsage {
        pub duration_ms: u64,
        pub input_tokens: u64,
        pub cached_input_tokens: u64,
        pub cache_creation_input_tokens: u64,
        pub output_tokens: u64,
        pub total_tokens: u64,
        pub context_tokens: Option<u64>,
        pub context_window_tokens: Option<u64>,
        pub cost_microusd: Option<u64>,
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

    #[derive(Clone, Default)]
    pub struct ClaudePool;

    pub async fn run(
        _request: ChatStreamRequest,
        _events: tokio::sync::mpsc::Sender<ChatStreamEvent>,
        _controls: Option<tokio::sync::mpsc::Receiver<ChatStreamControl>>,
    ) -> Result<()> {
        bail!("Claude adapter is not compiled; enable the claude feature")
    }

    pub async fn run_pooled(
        _request: ChatStreamRequest,
        _events: tokio::sync::mpsc::Sender<ChatStreamEvent>,
        _controls: Option<tokio::sync::mpsc::Receiver<ChatStreamControl>>,
        _pool: ClaudePool,
    ) -> Result<()> {
        bail!("Claude adapter is not compiled; enable the claude feature")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubscriptionProvider {
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

#[derive(Default)]
struct ClaudeToolGenerationState {
    blocks: HashMap<u64, ClaudeToolGenerationBlock>,
    /// Claude streams nothing between `message_start` and its first content
    /// block while the model reasons without visible thinking, so the
    /// generation window is surfaced as an explicit reasoning phase.
    reasoning_phase_open: bool,
}

#[derive(Default)]
struct ClaudeToolGenerationBlock {
    id: Option<String>,
    arguments: String,
    action_parser: super::StreamedToolAction,
    generating_emitted: bool,
    action_emitted: bool,
}

impl ClaudeToolGenerationState {
    fn observe(&mut self, raw: &Value) -> Vec<ChatStreamEvent> {
        if raw.get("type").and_then(Value::as_str) != Some("stream_event") {
            return Vec::new();
        }
        let Some(event) = raw.get("event") else {
            return Vec::new();
        };
        let event_type = event.get("type").and_then(Value::as_str);
        match event_type {
            Some("message_start") => {
                self.reasoning_phase_open = true;
                return vec![ChatStreamEvent::Phase {
                    name: "reasoning/started".to_string(),
                    input: Value::Null,
                }];
            }
            Some("message_delta" | "message_stop") if self.reasoning_phase_open => {
                self.reasoning_phase_open = false;
                return vec![ChatStreamEvent::Phase {
                    name: "reasoning_completed".to_string(),
                    input: Value::Null,
                }];
            }
            _ => {}
        }
        let Some(index) = event.get("index").and_then(Value::as_u64) else {
            return Vec::new();
        };
        match event_type {
            Some("content_block_start") => {
                let Some(content_block) = event.get("content_block") else {
                    return Vec::new();
                };
                let block_type = content_block.get("type").and_then(Value::as_str);
                let mut progress = Vec::new();
                if self.reasoning_phase_open && block_type != Some("thinking") {
                    self.reasoning_phase_open = false;
                    progress.push(ChatStreamEvent::Phase {
                        name: "reasoning_completed".to_string(),
                        input: Value::Null,
                    });
                }
                if block_type != Some("tool_use") {
                    self.blocks.remove(&index);
                    return progress;
                }
                let id = content_block
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .map(str::to_string);
                let mut block = ClaudeToolGenerationBlock {
                    id: id.clone(),
                    generating_emitted: true,
                    ..ClaudeToolGenerationBlock::default()
                };
                let mut progress = vec![ChatStreamEvent::ToolCallGenerating { id: id.clone() }];
                if let (Some(id), Some(action)) = (
                    id,
                    content_block.get("input").and_then(complete_tool_action),
                ) {
                    block.action_emitted = true;
                    progress.push(ChatStreamEvent::ToolCallAction {
                        id: Some(id),
                        action,
                    });
                }
                self.blocks.insert(index, block);
                progress
            }
            Some("content_block_delta") => {
                let Some(delta) = event.get("delta") else {
                    return Vec::new();
                };
                if delta.get("type").and_then(Value::as_str) != Some("input_json_delta") {
                    return Vec::new();
                }
                let Some(partial) = delta
                    .get("partial_json")
                    .and_then(Value::as_str)
                    .filter(|partial| !partial.is_empty())
                else {
                    return Vec::new();
                };
                let block = self.blocks.entry(index).or_default();
                let mut progress = Vec::new();
                let generation_already_visible = block.generating_emitted;
                if !generation_already_visible {
                    block.generating_emitted = true;
                    progress.push(ChatStreamEvent::ToolCallGenerating {
                        id: block.id.clone(),
                    });
                } else {
                    progress.push(ChatStreamEvent::ToolCallInputDelta {
                        id: block.id.clone(),
                    });
                }
                block.arguments.push_str(partial);
                if !block.action_emitted
                    && let Some(action) = block.action_parser.observe(&block.arguments)
                {
                    block.action_emitted = true;
                    progress.push(ChatStreamEvent::ToolCallAction {
                        id: block.id.clone(),
                        action,
                    });
                }
                progress
            }
            Some("content_block_stop") => {
                self.blocks.remove(&index);
                Vec::new()
            }
            _ => Vec::new(),
        }
    }
}

fn complete_tool_action(input: &Value) -> Option<String> {
    let action = input.get("action")?.as_str()?.trim();
    (!action.is_empty() && action.chars().count() <= 64).then(|| action.to_string())
}

/// Why a provider turn failed, decided where the cause is still known.
///
/// Downstream retry policy used to be recovered by matching substrings against
/// whatever prose the failure happened to carry. That is guesswork: a genuine
/// disconnect worded as "unexpected EOF during chunk size line" matched nothing
/// and was billed as a goal failure. The transport layer holds a typed
/// `reqwest::Error`/`io::Error` and knows the answer exactly, so it decides
/// once, here, and the decision travels with the error.
///
/// `Unknown` is not a failure of this scheme: it marks errors reported *by* a
/// provider as text (a model API's own message), where prose matching remains
/// the only signal available and stays the fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderErrorKind {
    /// The transport died mid-flight: reset, refused, timed out, DNS failure,
    /// or a body that stopped before it was complete. Always worth retrying,
    /// and never evidence that the work itself is failing.
    ConnectionLost,
    /// The provider is alive and refusing for a reason retrying cannot fix
    /// (auth, billing, quota). Retrying burns budget and hides the cause.
    Fatal,
    /// The request was refused because the conversation, plus the output still
    /// to be generated, does not fit the model's context window. Distinct from
    /// `Fatal` because the harness can recover it by dropping the oldest
    /// context and retrying, and distinct from `ConnectionLost` because
    /// retrying the identical request cannot succeed.
    ContextLength,
    /// No typed signal available; fall back to inspecting the message text.
    Unknown,
}

/// A provider failure that still knows why it happened.
///
/// Carried through the existing `anyhow` plumbing so the session layer can
/// downcast for the typed kind instead of re-reading the formatted string.
#[derive(Debug, Clone)]
pub struct ProviderStreamError {
    pub kind: ProviderErrorKind,
    pub message: String,
}

impl std::fmt::Display for ProviderStreamError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ProviderStreamError {}

impl ProviderErrorKind {
    /// Classify a transport error while the typed cause is still in hand.
    ///
    /// A truncated body is the case that motivated all of this: hyper reports
    /// it as a decode error, but the turn died because the connection did.
    pub fn from_transport(error: &reqwest::Error) -> Self {
        if error.is_connect() || error.is_timeout() || error.is_request() {
            return Self::ConnectionLost;
        }
        if error.is_body() || error.is_decode() {
            // Distinguish a body that stopped early from a body that arrived
            // intact but was unparseable: only the former is a lost connection.
            let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
            while let Some(error) = source {
                let text = error.to_string().to_ascii_lowercase();
                if text.contains("unexpected eof")
                    || text.contains("incomplete")
                    || text.contains("connection")
                    || text.contains("end of file")
                {
                    return Self::ConnectionLost;
                }
                source = error.source();
            }
        }
        Self::Unknown
    }
}

/// Recover the typed kind from an error that has already been wrapped in
/// context on its way up.
///
/// `anyhow` keeps the whole cause chain, so the original `reqwest::Error` is
/// still reachable behind however many `.context(...)` layers were added. That
/// is the difference between reading the cause and guessing from the rendered
/// message: the classification here is the same one the transport would have
/// made at the moment it failed.
pub fn classify_provider_error(error: &anyhow::Error) -> ProviderErrorKind {
    for cause in error.chain() {
        if let Some(typed) = cause.downcast_ref::<ProviderStreamError>() {
            return typed.kind;
        }
        // The native path fails with this instead, and it flattens its cause
        // into a string, so its recorded kind is the only signal left.
        if let Some(typed) = cause.downcast_ref::<super::ProviderCallError>()
            && typed.kind != ProviderErrorKind::Unknown
        {
            return typed.kind;
        }
        if let Some(transport) = cause.downcast_ref::<reqwest::Error>() {
            let kind = ProviderErrorKind::from_transport(transport);
            if kind != ProviderErrorKind::Unknown {
                return kind;
            }
        }
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            use std::io::ErrorKind;
            if matches!(
                io.kind(),
                ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::ConnectionRefused
                    | ErrorKind::BrokenPipe
                    | ErrorKind::UnexpectedEof
                    | ErrorKind::TimedOut
                    | ErrorKind::NotConnected
            ) {
                return ProviderErrorKind::ConnectionLost;
            }
        }
    }
    // Many provider APIs report a context-length refusal only as prose in an
    // error body, and the native path flattens its typed cause into that
    // message. It is not a transport failure and cannot be fixed by retrying
    // the identical request, so it must not fall through to `Unknown`, where
    // the caller would treat it as an ordinary transient failure.
    if provider_error_is_context_length(&format!("{error:#}")) {
        return ProviderErrorKind::ContextLength;
    }
    ProviderErrorKind::Unknown
}

/// Whether a rendered provider error reports that the request exceeded the
/// model's context window. Phrasing varies across vendors, so match the
/// established spellings rather than one vendor's wording.
fn provider_error_is_context_length(text: &str) -> bool {
    let text = text.to_ascii_lowercase();
    [
        "context length",
        "context_length",
        "context limit",
        "maximum context",
        "max context",
        "context window",
        "too many tokens",
        "input is too long",
        "prompt is too long",
        "reduce the length",
        "exceeds the maximum",
    ]
    .iter()
    .any(|pattern| text.contains(pattern))
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
    ToolCallGenerating {
        id: Option<String>,
    },
    ToolCallInputDelta {
        id: Option<String>,
    },
    ToolCallAction {
        id: Option<String>,
        action: String,
    },
    ToolCall {
        id: String,
        name: String,
        input: Value,
    },
    ToolCallUpdate {
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
        provider_turn_id: Option<String>,
    },
    Failed {
        error: String,
        /// Decided at the point of failure; `Unknown` means the message text is
        /// the only signal and prose matching remains the fallback.
        kind: ProviderErrorKind,
    },
}

#[derive(Debug)]
pub enum ChatStreamControl {
    Steer {
        client_user_message_id: Option<String>,
        text: String,
        attachments: Vec<PathBuf>,
        admission: SteerAdmission,
        /// Human input: providers that can should end the running turn after
        /// the tool in flight and answer this message next (Claude Code's
        /// priority `now`), instead of folding it into the current task.
        preempt: bool,
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

const STEER_PENDING: u8 = 0;
const STEER_ACCEPTED: u8 = 1;
const STEER_RECALLED: u8 = 2;

/// Atomic ownership handoff for an active-turn steer. The session may recall
/// the prompt while it is pending; a provider claims it immediately before
/// the first irreversible delivery action. Exactly one side can win.
#[derive(Clone, Debug)]
pub struct SteerAdmission(Arc<AtomicU8>);

impl SteerAdmission {
    pub fn pending() -> Self {
        Self(Arc::new(AtomicU8::new(STEER_PENDING)))
    }

    pub fn accept(&self) -> bool {
        self.0
            .compare_exchange(
                STEER_PENDING,
                STEER_ACCEPTED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub fn recall(&self) -> bool {
        self.0
            .compare_exchange(
                STEER_PENDING,
                STEER_RECALLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub fn is_accepted(&self) -> bool {
        self.0.load(Ordering::Acquire) == STEER_ACCEPTED
    }
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
    /// When set with `session_id`, fork that persisted Codex thread through
    /// this completed turn instead of resuming an uncertain tail.
    pub fork_turn_id: Option<String>,
    pub provider_channel: ProviderChannel,
    pub persist_session: Option<bool>,
    pub web_search_allowed: bool,
    pub resume_unavailable_prompt: Option<String>,
    /// Offer Claude Code's Agent tool, whose subagents run in this process.
    pub native_subagents: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexRateLimitWindow {
    pub used_percent: u8,
    pub window_duration_mins: u64,
    pub resets_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexAccountRateLimits {
    pub plan_type: Option<String>,
    pub primary: Option<CodexRateLimitWindow>,
    pub secondary: Option<CodexRateLimitWindow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeRateLimitWindow {
    pub label: String,
    pub used_percent: u8,
    pub resets_at: Option<DateTime<Utc>>,
    pub global: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeAccountRateLimits {
    pub subscription_type: Option<String>,
    pub rate_limits_available: bool,
    pub windows: Vec<ClaudeRateLimitWindow>,
    pub extra_usage_available: bool,
    /// Set when Claude answered from its persisted usage cache instead of a
    /// fresh fetch; the value is when that cache was last refreshed.
    pub last_known_at: Option<DateTime<Utc>>,
}

/// Read native ChatGPT subscription usage without starting a provider process.
pub async fn read_codex_account_rate_limits() -> Result<CodexAccountRateLimits> {
    let response =
        tokio::time::timeout(Duration::from_secs(10), crate::openai_subscription::usage())
            .await
            .context("timed out reading ChatGPT account limits")??;
    parse_codex_account_rate_limits(&response)
}

/// Read Claude's structured `/usage` data through the native CLI control
/// protocol. This does not submit a model prompt or consume a model turn.
pub async fn read_claude_account_rate_limits() -> Result<ClaudeAccountRateLimits> {
    #[cfg(not(feature = "claude"))]
    {
        bail!("Claude account limits require the Claude subscription adapter")
    }
    #[cfg(feature = "claude")]
    {
        let read = || async {
            tokio::time::timeout(
                Duration::from_secs(10),
                read_claude_account_rate_limits_inner(),
            )
            .await
            .context("timed out reading Claude account limits")?
        };
        let mut limits = read().await?;
        if limits.last_known_at.is_some() {
            // The CLI silently serves its persisted cache when the live fetch
            // fails (typically a momentary rate limit on the usage endpoint).
            tokio::time::sleep(Duration::from_millis(1500)).await;
            if let Ok(fresh) = read().await
                && fresh.last_known_at.is_none()
            {
                limits = fresh;
            }
        }
        Ok(limits)
    }
}

/// Claude Code refreshes `cachedUsageUtilization` in its config file on every
/// successful live fetch (throttled to once per five minutes) and serves that
/// cache for up to an hour when the fetch fails, without marking the answer.
/// A cache older than the throttle window after a `get_usage` call therefore
/// means the answer was not fresh.
#[cfg(feature = "claude")]
fn claude_usage_cache_last_known_at() -> Option<DateTime<Utc>> {
    const CONFIG_MAX_BYTES: u64 = 8 * 1024 * 1024;
    const FRESH_WINDOW: chrono::TimeDelta = chrono::TimeDelta::minutes(6);
    let directory = std::env::var_os("CLAUDE_CONFIG_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(PathBuf::from))?;
    let path = directory.join(".claude.json");
    if std::fs::metadata(&path).ok()?.len() > CONFIG_MAX_BYTES {
        return None;
    }
    let config: Value = serde_json::from_slice(&std::fs::read(&path).ok()?).ok()?;
    let fetched_at_ms = config
        .pointer("/cachedUsageUtilization/fetchedAtMs")
        .and_then(Value::as_i64)?;
    let fetched_at = DateTime::<Utc>::from_timestamp_millis(fetched_at_ms)?;
    (Utc::now() - fetched_at > FRESH_WINDOW).then_some(fetched_at)
}

#[cfg(feature = "claude")]
async fn read_claude_account_rate_limits_inner() -> Result<ClaudeAccountRateLimits> {
    let mut command = crate::provider_bin::command(crate::provider_bin::Runtime::Claude).await?;
    command
        .args([
            "--print",
            "--output-format",
            "stream-json",
            "--verbose",
            "--input-format",
            "stream-json",
            "--permission-mode",
            "dontAsk",
            "--tools",
            "",
            "--no-session-persistence",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .context("failed to start Claude for account limits")?;
    let mut stdin = child
        .stdin
        .take()
        .context("Claude usage stdin pipe missing")?;
    let stdout = child
        .stdout
        .take()
        .context("Claude usage stdout pipe missing")?;
    let mut lines = BufReader::new(stdout).lines();
    let init_id = format!("borg-usage-init-{}", uuid::Uuid::new_v4());
    let usage_id = format!("borg-usage-read-{}", uuid::Uuid::new_v4());
    write_claude_control_request(
        &mut stdin,
        &init_id,
        serde_json::json!({"subtype": "initialize"}),
    )
    .await?;
    write_claude_control_request(
        &mut stdin,
        &usage_id,
        serde_json::json!({"subtype": "get_usage", "skip_behaviors": true}),
    )
    .await?;

    let result = loop {
        let line = lines
            .next_line()
            .await
            .context("failed reading Claude account limits")?
            .context("Claude exited before returning account limits")?;
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let Some(response) = value.get("response") else {
            continue;
        };
        if response.get("request_id").and_then(Value::as_str) != Some(usage_id.as_str()) {
            continue;
        }
        if response.get("subtype").and_then(Value::as_str) == Some("error") {
            bail!(
                "Claude account limits request failed: {}",
                response
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown control error")
            );
        }
        let mut limits =
            parse_claude_account_rate_limits(response.get("response").unwrap_or(&Value::Null))?;
        if limits.rate_limits_available {
            limits.last_known_at = claude_usage_cache_last_known_at();
        }
        break Ok(limits);
    };
    drop(lines);
    drop(stdin);
    child.start_kill().ok();
    let _ = tokio::time::timeout(Duration::from_secs(1), child.wait()).await;
    result
}

#[cfg(feature = "claude")]
async fn write_claude_control_request(
    stdin: &mut ChildStdin,
    request_id: &str,
    request: Value,
) -> Result<()> {
    let mut line = serde_json::to_vec(&serde_json::json!({
        "type": "control_request",
        "request_id": request_id,
        "request": request,
    }))?;
    line.push(b'\n');
    stdin
        .write_all(&line)
        .await
        .context("failed to write Claude account limits request")?;
    stdin
        .flush()
        .await
        .context("failed to flush Claude account limits request")
}

fn parse_claude_account_rate_limits(value: &Value) -> Result<ClaudeAccountRateLimits> {
    let rate_limits_available = value
        .get("rate_limits_available")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let limits = value.get("rate_limits").filter(|value| value.is_object());
    let mut windows = Vec::new();
    for (key, label, global) in [
        ("five_hour", "5-hour", true),
        ("seven_day", "Weekly", true),
        ("seven_day_opus", "Weekly · Opus", false),
        ("seven_day_sonnet", "Weekly · Sonnet", false),
    ] {
        let Some(window) = limits.and_then(|limits| limits.get(key)) else {
            continue;
        };
        let Some(used_percent) = window.get("utilization").and_then(Value::as_f64) else {
            continue;
        };
        let resets_at = window
            .get("resets_at")
            .and_then(Value::as_str)
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
            .map(|value| value.with_timezone(&Utc));
        windows.push(ClaudeRateLimitWindow {
            label: label.to_string(),
            used_percent: used_percent.clamp(0.0, 100.0).round() as u8,
            resets_at,
            global,
        });
    }
    // Newer CLIs report per-model weekly windows (for example Fable) as a
    // separate list rather than fixed `seven_day_<model>` keys.
    for scoped in limits
        .and_then(|limits| limits.get("model_scoped"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let (Some(name), Some(used_percent)) = (
            scoped.get("display_name").and_then(Value::as_str),
            scoped.get("utilization").and_then(Value::as_f64),
        ) else {
            continue;
        };
        let label = format!("Weekly · {name}");
        if windows.iter().any(|window| window.label == label) {
            continue;
        }
        windows.push(ClaudeRateLimitWindow {
            label,
            used_percent: used_percent.clamp(0.0, 100.0).round() as u8,
            resets_at: scoped
                .get("resets_at")
                .and_then(Value::as_str)
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .map(|value| value.with_timezone(&Utc)),
            global: false,
        });
    }
    let extra_usage = limits.and_then(|limits| limits.get("extra_usage"));
    let extra_usage_available = extra_usage
        .and_then(|usage| usage.get("is_enabled"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
        && !extra_usage
            .and_then(|usage| usage.get("spend_limit_reached"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
    Ok(ClaudeAccountRateLimits {
        subscription_type: value
            .get("subscription_type")
            .and_then(Value::as_str)
            .map(str::to_string),
        rate_limits_available,
        windows,
        extra_usage_available,
        last_known_at: None,
    })
}

/// Read the explicitly selected native subscription authority without refreshing tokens.
pub async fn read_codex_subscription_status() -> Result<bool> {
    Ok(crate::openai_subscription::account()?.is_some())
}

fn parse_codex_account_rate_limits(response: &Value) -> Result<CodexAccountRateLimits> {
    let limits = response
        .get("rate_limit")
        .filter(|limits| limits.is_object())
        .context("ChatGPT usage response omitted rate limits")?;
    Ok(CodexAccountRateLimits {
        plan_type: response
            .get("plan_type")
            .and_then(Value::as_str)
            .map(str::to_owned),
        primary: parse_codex_rate_limit_window(limits.get("primary_window"))?,
        secondary: parse_codex_rate_limit_window(limits.get("secondary_window"))?,
    })
}

fn parse_codex_rate_limit_window(value: Option<&Value>) -> Result<Option<CodexRateLimitWindow>> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    let used_percent = value
        .get("used_percent")
        .and_then(Value::as_u64)
        .context("ChatGPT usage response omitted rate-limit usage")?
        .min(100) as u8;
    let seconds = value
        .get("limit_window_seconds")
        .and_then(Value::as_u64)
        .context("ChatGPT usage response omitted rate-limit window")?;
    Ok(Some(CodexRateLimitWindow {
        used_percent,
        window_duration_mins: seconds.div_ceil(60),
        resets_at: value.get("reset_at").and_then(Value::as_i64),
    }))
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

type ClaudeCostTracker = Arc<StdMutex<ClaudeCostState>>;

#[derive(Default)]
struct ClaudeCostState {
    session_id: Option<String>,
    total_microusd: Option<u64>,
}

#[derive(Default)]
struct ClaudeSubscriptionPoolState {
    native: claude_agents::ClaudePool,
    lifecycle_key: Option<String>,
    command: Option<claude_agents::CommandSpec>,
    // Claude's streaming-input result cost is cumulative for the live process;
    // keep the prior total beside the pooled process so Borg emits a turn delta.
    cost_tracker: ClaudeCostTracker,
    /// Model/effort the cached command was built for. A change rebuilds the
    /// command; the native pool then switches the live process in place.
    model_effort: Option<(Option<String>, Option<String>)>,
    _auth_home: Option<TempDir>,
    _mcp_setup: Option<(TempDir, ProviderMcpSetup)>,
}

pub fn run_claude_chat_stream(request: ChatStreamRequest) -> mpsc::Receiver<ChatStreamEvent> {
    #[cfg(not(feature = "claude"))]
    {
        let _ = request;
        unavailable_stream("Claude", "claude")
    }
    #[cfg(feature = "claude")]
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
    #[cfg(not(feature = "claude"))]
    {
        let _ = (request, controls);
        unavailable_stream("Claude", "claude")
    }
    #[cfg(feature = "claude")]
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
    #[cfg(not(feature = "claude"))]
    {
        let _ = (request, controls, permission);
        unavailable_stream("Claude", "claude")
    }
    #[cfg(feature = "claude")]
    run_subscription_stream(request, controls, SubscriptionProvider::Claude, permission)
}

/// Run Claude Code on the shared native process for this Borg session.
pub fn run_claude_local_chat_stream_pooled(
    request: ChatStreamRequest,
    controls: Option<mpsc::Receiver<ChatStreamControl>>,
    permission: LocalAgentPermission,
    pool: ClaudeSubscriptionPool,
) -> mpsc::Receiver<ChatStreamEvent> {
    #[cfg(not(feature = "claude"))]
    {
        let _ = (request, controls, permission, pool);
        unavailable_stream("Claude", "claude")
    }
    #[cfg(feature = "claude")]
    {
        let (events, receiver) = mpsc::channel(64);
        tokio::spawn(async move {
            let result = tokio::select! {
                biased;
                _ = events.closed() => return,
                result = run_claude_subscription_process_pooled(
                    request, controls, permission, events.clone(), pool,
                ) => result,
            };
            if let Err(error) = result {
                let _ = events
                    .send(ChatStreamEvent::Failed {
                        kind: classify_provider_error(&error),
                        error: format!("{error:#}"),
                    })
                    .await;
            }
        });
        receiver
    }
}

fn run_subscription_stream(
    request: ChatStreamRequest,
    controls: Option<mpsc::Receiver<ChatStreamControl>>,
    provider: SubscriptionProvider,
    permission: LocalAgentPermission,
) -> mpsc::Receiver<ChatStreamEvent> {
    let (events, receiver) = mpsc::channel(64);
    tokio::spawn(async move {
        let result = tokio::select! {
            biased;
            _ = events.closed() => return,
            result = run_subscription_process(
                request, controls, provider, permission, events.clone(),
            ) => result,
        };
        if let Err(error) = result {
            let _ = events
                .send(ChatStreamEvent::Failed {
                    kind: classify_provider_error(&error),
                    error: format!("{error:#}"),
                })
                .await;
        }
    });
    receiver
}

#[cfg(not(feature = "claude"))]
fn unavailable_stream(provider: &str, feature: &str) -> mpsc::Receiver<ChatStreamEvent> {
    let (events, receiver) = mpsc::channel(1);
    let message =
        format!("{provider} subscription adapter is not compiled; enable the {feature} feature");
    tokio::spawn(async move {
        let _ = events
            .send(ChatStreamEvent::Failed {
                error: message,
                // A missing compile-time feature is not going to fix itself.
                kind: ProviderErrorKind::Fatal,
            })
            .await;
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
        build_claude_command_spec(&request, permission, auth_home.as_ref(), mcp_config_path)
            .await?;
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

    relay_claude_runtime(claude_request, controls, events, None, billing_mode, None).await
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
            build_claude_command_spec(&request, permission, auth_home.as_ref(), mcp_config_path)
                .await?;
        state.lifecycle_key = Some(lifecycle_key.clone());
        state.command = Some(command);
        state.model_effort = Some((request.model.clone(), request.effort.clone()));
        reset_claude_cost_tracker(&state.cost_tracker);
        state._auth_home = auth_home;
        state._mcp_setup = mcp_setup;
    } else if state.model_effort.as_ref() != Some(&(request.model.clone(), request.effort.clone()))
    {
        let mcp_config_path = state
            ._mcp_setup
            .as_ref()
            .and_then(|(_, setup)| setup.claude_config_path.as_deref());
        let command = build_claude_command_spec(
            &request,
            permission,
            state._auth_home.as_ref(),
            mcp_config_path,
        )
        .await?;
        state.command = Some(command);
        state.model_effort = Some((request.model.clone(), request.effort.clone()));
    }
    let command = state
        .command
        .clone()
        .context("pooled Claude command was not initialized")?;
    let native_pool = state.native.clone();
    let cost_tracker = Arc::clone(&state.cost_tracker);
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
        Some(cost_tracker),
    )
    .await
}

struct AbortOnDrop(tokio::task::AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn relay_claude_runtime(
    claude_request: claude_agents::ChatStreamRequest,
    controls: Option<mpsc::Receiver<ChatStreamControl>>,
    events: mpsc::Sender<ChatStreamEvent>,
    pool: Option<claude_agents::ClaudePool>,
    billing_mode: ProviderBillingMode,
    cost_tracker: Option<ClaudeCostTracker>,
) -> Result<()> {
    let (native_events, mut native_events_receiver) = mpsc::channel(64);
    let steer_correlation = Arc::new(StdMutex::new(ClaudeSteerCorrelation::default()));
    let (native_controls, mut control_forwarder) = match controls {
        Some(mut controls) => {
            let (sender, receiver) = mpsc::channel(64);
            let steer_correlation = Arc::clone(&steer_correlation);
            let forwarder = tokio::spawn(async move {
                while let Some(control) = controls.recv().await {
                    match control {
                        ChatStreamControl::Steer {
                            client_user_message_id,
                            text,
                            attachments,
                            admission,
                            preempt,
                            ack,
                        } => {
                            let permit = match sender.reserve().await {
                                Ok(permit) => permit,
                                Err(_) => {
                                    let _ = ack.send(Err(
                                        "Claude turn ended before the steer was delivered"
                                            .to_string(),
                                    ));
                                    break;
                                }
                            };
                            if !admission.accept() {
                                let _ =
                                    ack.send(Err("steer was recalled before delivery".to_string()));
                                continue;
                            }
                            if let Some(message_id) = client_user_message_id.as_deref() {
                                register_claude_steer(&steer_correlation, message_id.to_string());
                            }
                            let (native_ack, _native_acknowledgement) =
                                tokio::sync::oneshot::channel();
                            // The Borg message id becomes the stdin uuid, so the
                            // CLI's command_lifecycle frames name it directly.
                            permit.send(claude_agents::ChatStreamControl::Steer {
                                text,
                                attachments,
                                message_id: client_user_message_id.clone(),
                                preempt,
                                ack: native_ack,
                            });
                            let _ = ack.send(Ok(()));
                        }
                        control => {
                            if sender.send(map_claude_control(control)).await.is_err() {
                                break;
                            }
                        }
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
    let _runner_guard = AbortOnDrop(runner.as_ref().unwrap().abort_handle());
    let _forwarder_guard = control_forwarder
        .as_ref()
        .map(|task| AbortOnDrop(task.abort_handle()));
    let mut tool_generation = ClaudeToolGenerationState::default();
    let mut saw_done = false;

    loop {
        tokio::select! {
            // The session actor may abort its consumer on timeout, interrupt,
            // or provider switch. The provider task is separate from that
            // actor future, so watching the output channel here is what makes
            // cancellation reach the Claude child instead of leaving an idle
            // subscription process behind.
            _ = events.closed() => {
                if let Some(tracker) = cost_tracker.as_ref() {
                    reset_claude_cost_tracker(tracker);
                }
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
                let mut outgoing = match &event {
                    claude_agents::ChatStreamEvent::ProviderEvent {
                        raw_payload: Some(raw),
                        ..
                    } => tool_generation.observe(raw),
                    _ => Vec::new(),
                };
                let event = map_claude_event_with_correlation(
                    event,
                    billing_mode,
                    Some(&steer_correlation),
                );
                let event = normalize_claude_cost(event, cost_tracker.as_ref());
                saw_done |= matches!(event, ChatStreamEvent::Done { .. });
                outgoing.push(event);
                for event in outgoing {
                    if events.send(event).await.is_err()
                    {
                        if let Some(tracker) = cost_tracker.as_ref() {
                            reset_claude_cost_tracker(tracker);
                        }
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
    }

    let runner_result = runner
        .expect("Claude subscription runner should still be active")
        .await;
    if let Some(forwarder) = control_forwarder.take() {
        forwarder.abort();
        let _ = forwarder.await;
    }
    let result = runner_result
        .context("Claude subscription runtime task failed")
        .and_then(|result| result);
    if (!saw_done || result.is_err())
        && let Some(tracker) = cost_tracker.as_ref()
    {
        reset_claude_cost_tracker(tracker);
    }
    result
}

fn register_claude_steer(correlation: &StdMutex<ClaudeSteerCorrelation>, message_id: String) {
    let mut correlation = correlation
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    correlation.pending.push_back(message_id);
}

fn map_claude_control(control: ChatStreamControl) -> claude_agents::ChatStreamControl {
    match control {
        ChatStreamControl::Steer { .. } => {
            unreachable!("Claude steers require an atomic admission handoff")
        }
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

/// Claude Code reports its own context compaction through `system` frames:
/// `subtype: "status", status: "compacting"` while it runs and
/// `subtype: "compact_boundary"` (with `compact_metadata`) once the new
/// transcript boundary lands. Codex compaction reaches Borg as a
/// `context_compaction` provider event, and every Borg client renders its
/// compaction card from that kind, so Claude's frames are mapped onto the same
/// contract here. Claude keeps its own compacted session, so the payload marks
/// the provider context as preserved: the durable journal must not restart
/// replay or treat the boundary as a Borg-owned recovery checkpoint.
fn claude_context_compaction_payload(raw: &Value) -> Option<Value> {
    if raw.get("type").and_then(Value::as_str) != Some("system") {
        return None;
    }
    match raw.get("subtype").and_then(Value::as_str)? {
        "compact_boundary" => {
            let metadata = raw.get("compact_metadata");
            let trigger = metadata
                .and_then(|metadata| metadata.get("trigger"))
                .and_then(Value::as_str)
                .filter(|trigger| !trigger.trim().is_empty());
            let pre_tokens = metadata
                .and_then(|metadata| metadata.get("pre_tokens"))
                .and_then(Value::as_u64);
            let mut detail = String::from("Claude Code compacted its transcript");
            match (trigger, pre_tokens) {
                (Some(trigger), Some(tokens)) => {
                    detail.push_str(&format!(" ({trigger}, {tokens} tokens before)"));
                }
                (Some(trigger), None) => detail.push_str(&format!(" ({trigger})")),
                (None, Some(tokens)) => detail.push_str(&format!(" ({tokens} tokens before)")),
                (None, None) => {}
            }
            Some(serde_json::json!({
                "status": "completed",
                "summary": detail,
                "trigger": trigger,
                "pre_tokens": pre_tokens,
                "provider_context_preserved": true,
            }))
        }
        "status" => {
            let compacting = raw
                .get("status")
                .and_then(Value::as_str)
                .is_some_and(|status| status.eq_ignore_ascii_case("compacting"));
            compacting.then(|| {
                serde_json::json!({
                    "status": "started",
                    "summary": "Compacting context…",
                    "provider_context_preserved": true,
                })
            })
        }
        _ => None,
    }
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
            if let Some(compaction) = raw_payload
                .as_ref()
                .and_then(claude_context_compaction_payload)
            {
                return ChatStreamEvent::ProviderEvent {
                    kind: "context_compaction".to_string(),
                    payload: compaction,
                    raw_payload,
                    stream_channel,
                    content_text,
                    provider_item_id,
                    tool_use_id,
                    tool_name,
                };
            }
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
            provider_turn_id: None,
        },
        claude_agents::ChatStreamEvent::Failed { error } => ChatStreamEvent::Failed {
            error,
            // Reported by the Claude agent SDK as text; prose matching stays
            // the only available signal for these.
            kind: ProviderErrorKind::Unknown,
        },
    }
}

fn normalize_claude_cost(
    event: ChatStreamEvent,
    cost_tracker: Option<&ClaudeCostTracker>,
) -> ChatStreamEvent {
    let Some(cost_tracker) = cost_tracker else {
        return event;
    };
    if matches!(event, ChatStreamEvent::Failed { .. }) {
        reset_claude_cost_tracker(cost_tracker);
        return event;
    }
    let ChatStreamEvent::Done {
        final_text,
        usage,
        session_id,
        provider_turn_id,
    } = event
    else {
        return event;
    };
    let usage = usage.map(|mut usage| {
        usage.cost_microusd =
            claude_cost_delta(cost_tracker, session_id.as_deref(), usage.cost_microusd);
        usage
    });
    ChatStreamEvent::Done {
        final_text,
        usage,
        session_id,
        provider_turn_id,
    }
}

fn reset_claude_cost_tracker(tracker: &ClaudeCostTracker) {
    *tracker
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = ClaudeCostState::default();
}

fn claude_cost_delta(
    tracker: &ClaudeCostTracker,
    session_id: Option<&str>,
    cumulative: Option<u64>,
) -> Option<u64> {
    let mut state = tracker
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(session_id) = session_id
        && state.session_id.as_deref() != Some(session_id)
    {
        state.session_id = Some(session_id.to_string());
        state.total_microusd = None;
    }
    let cumulative = cumulative?;
    let delta = state
        .total_microusd
        .map(|previous| {
            if cumulative >= previous {
                cumulative - previous
            } else {
                cumulative
            }
        })
        .unwrap_or(cumulative);
    state.total_microusd = Some(cumulative);
    Some(delta)
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
            // Exact match first: the stdin uuid is the Borg message id. Fall
            // back to arrival order for steers registered without an id.
            let exact = steer_correlation
                .pending
                .iter()
                .position(|message_id| message_id == command_uuid)
                .and_then(|index| steer_correlation.pending.remove(index));
            if !steer_correlation.commands.contains_key(command_uuid)
                && let Some(message_id) = exact.or_else(|| steer_correlation.pending.pop_front())
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
        // Delegation and monitoring go through Borg's own tools so every
        // provider shares one implementation, journal, and UI. The opt-in
        // exception is Claude Code's Agent tool: its subagents share this
        // process instead of each starting another Claude runtime.
        "--disallowedTools".to_string(),
        if request.native_subagents {
            "Monitor,Watch"
        } else {
            "Agent,Task,Monitor,Watch"
        }
        .to_string(),
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

async fn build_claude_command_spec(
    request: &ChatStreamRequest,
    permission: LocalAgentPermission,
    auth_home: Option<&TempDir>,
    mcp_config_path: Option<&Path>,
) -> Result<claude_agents::CommandSpec> {
    // Borg owns long-running work through its own watchers. Claude Code's
    // automatic backgrounding of slow Bash commands withholds the turn's
    // `result` until the task finishes, which left sessions "running" for
    // minutes after the model had already delivered its final answer.
    let mut environment = vec![(
        "CLAUDE_CODE_DISABLE_BACKGROUND_TASKS".to_string(),
        "1".to_string(),
    )];
    if let Some(auth_home) = auth_home {
        environment.push(("HOME".to_string(), auth_home.path().display().to_string()));
    }
    Ok(claude_agents::CommandSpec {
        // Resolve rather than trusting PATH, exactly as every other spawn does.
        program: crate::provider_bin::executable(crate::provider_bin::Runtime::Claude).await?,
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
    }
}

fn has_nonempty_env(name: &str) -> bool {
    std::env::var_os(name).is_some_and(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_context_length_refusal_classifies_as_recoverable_not_unknown() {
        // A prose refusal from an API error body, and the native path flattens
        // typed causes into exactly this kind of message.
        let refused = anyhow::anyhow!(
            "This model's maximum context length is 128000 tokens, however you requested 140000 tokens"
        );
        assert_eq!(
            classify_provider_error(&refused),
            ProviderErrorKind::ContextLength
        );
        // Codex's own wording, which reaches the classifier as prose too.
        let codex = anyhow::anyhow!(
            "Codex context limit reached; compact the conversation before trying again. HTTP 400."
        );
        assert_eq!(
            classify_provider_error(&codex),
            ProviderErrorKind::ContextLength
        );
        // An unrelated provider failure must stay `Unknown`, so it is not
        // mistaken for something compaction can fix.
        let other = anyhow::anyhow!("the model produced malformed tool arguments");
        assert_eq!(classify_provider_error(&other), ProviderErrorKind::Unknown);
    }

    #[test]
    fn codex_account_rate_limits_parse_native_usage_shape() {
        let parsed = parse_codex_account_rate_limits(&serde_json::json!({
            "plan_type": "pro",
            "rate_limit": {
                "primary_window": {
                    "used_percent": 49,
                    "limit_window_seconds": 604800,
                    "reset_at": 1788137121
                },
                "secondary_window": null
            }
        }))
        .expect("Codex account limits");

        assert_eq!(parsed.plan_type.as_deref(), Some("pro"));
        assert_eq!(
            parsed.primary,
            Some(CodexRateLimitWindow {
                used_percent: 49,
                window_duration_mins: 10080,
                resets_at: Some(1788137121),
            })
        );
        assert_eq!(parsed.secondary, None);
    }

    #[test]
    fn steer_admission_has_exactly_one_winner() {
        let recalled = SteerAdmission::pending();
        assert!(recalled.recall());
        assert!(!recalled.accept());
        assert!(!recalled.is_accepted());

        let accepted = SteerAdmission::pending();
        assert!(accepted.accept());
        assert!(!accepted.recall());
        assert!(accepted.is_accepted());
    }

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
            fork_turn_id: None,
            provider_channel: ProviderChannel::Direct,
            persist_session: Some(false),
            web_search_allowed: false,
            resume_unavailable_prompt: None,
            native_subagents: false,
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
                "--disallowedTools",
                "Agent,Task,Monitor,Watch",
                "--permission-mode",
                "manual",
                "--no-session-persistence",
                "--resume",
                "session-1",
            ]
        );
        let native = ChatStreamRequest {
            native_subagents: true,
            ..request.clone()
        };
        let native_args = claude_command_args(&native, LocalAgentPermission::Manual, None);
        assert!(
            native_args
                .windows(2)
                .any(|args| args[0] == "--disallowedTools" && args[1] == "Monitor,Watch"),
            "an opted-in session is offered Claude Code's Agent tool and nothing else"
        );
        let auto_args = claude_command_args(&request, LocalAgentPermission::Auto, None);
        assert!(
            auto_args
                .windows(2)
                .any(|args| args[0] == "--permission-mode" && args[1] == "auto")
        );
    }

    #[test]
    fn claude_account_rate_limits_parse_control_response_shape() {
        let parsed = parse_claude_account_rate_limits(&serde_json::json!({
            "subscription_type": "pro",
            "rate_limits_available": true,
            "rate_limits": {
                "five_hour": {
                    "utilization": 100,
                    "resets_at": "2026-08-30T23:40:00Z"
                },
                "seven_day": {
                    "utilization": 33.4,
                    "resets_at": "2026-09-01T23:00:00Z"
                },
                "seven_day_opus": null,
                "extra_usage": {
                    "is_enabled": false,
                    "spend_limit_reached": false
                }
            }
        }))
        .unwrap();

        assert_eq!(parsed.subscription_type.as_deref(), Some("pro"));
        assert!(parsed.rate_limits_available);
        assert_eq!(parsed.windows.len(), 2);
        assert_eq!(parsed.windows[0].label, "5-hour");
        assert_eq!(parsed.windows[0].used_percent, 100);
        assert!(parsed.windows[0].global);
        assert_eq!(parsed.windows[1].used_percent, 33);
        assert!(!parsed.extra_usage_available);
    }

    #[tokio::test]
    async fn subscription_commands_attach_provider_mcp_config() {
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
            fork_turn_id: None,
            provider_channel: ProviderChannel::Direct,
            persist_session: Some(false),
            web_search_allowed: false,
            resume_unavailable_prompt: None,
            native_subagents: false,
        };

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
                provider_turn_id: None,
            } if final_text == "done" && session_id == "session-1"
        ));
    }

    #[test]
    fn claude_tool_input_stream_generates_then_refines_one_action() {
        let mut state = ClaudeToolGenerationState::default();
        let started = state.observe(&serde_json::json!({
            "type": "stream_event",
            "event": {
                "type": "content_block_start",
                "index": 1,
                "content_block": {
                    "type": "tool_use",
                    "id": "tool-1",
                    "name": "mcp__borg_agent__update_plan",
                    "input": {}
                }
            }
        }));
        assert!(matches!(
            started.as_slice(),
            [ChatStreamEvent::ToolCallGenerating { id: Some(id) }] if id == "tool-1"
        ));

        assert!(matches!(
            state
                .observe(&serde_json::json!({
                    "type": "stream_event",
                    "event": {
                        "type": "content_block_delta",
                        "index": 1,
                        "delta": {"type": "input_json_delta", "partial_json": "{\"act"}
                    }
                }))
                .as_slice(),
            [ChatStreamEvent::ToolCallInputDelta { id: Some(id) }] if id == "tool-1"
        ));
        let refined = state.observe(&serde_json::json!({
            "type": "stream_event",
            "event": {
                "type": "content_block_delta",
                "index": 1,
                "delta": {
                    "type": "input_json_delta",
                    "partial_json": "ion\":\"edit\",\"plan\":["
                }
            }
        }));
        assert!(matches!(
            refined.as_slice(),
            [
                ChatStreamEvent::ToolCallInputDelta { id: Some(delta_id) },
                ChatStreamEvent::ToolCallAction { id, action }
            ]
                if delta_id == "tool-1" && id.as_deref() == Some("tool-1") && action == "edit"
        ));

        for partial in [None, Some(""), Some("{")] {
            let progress = state.observe(&serde_json::json!({
                "type": "stream_event",
                "event": {
                    "type": "content_block_delta",
                    "index": 2,
                    "delta": {"type": "input_json_delta", "partial_json": partial}
                }
            }));
            if partial == Some("{") {
                assert!(matches!(
                    progress.as_slice(),
                    [ChatStreamEvent::ToolCallGenerating { id: None }]
                ));
            } else {
                assert!(progress.is_empty());
            }
        }
    }

    fn claude_system_frame(raw: Value) -> claude_agents::ChatStreamEvent {
        claude_agents::ChatStreamEvent::ProviderEvent {
            kind: "claude.system".to_string(),
            payload: serde_json::json!({"type": "system"}),
            raw_payload: Some(raw),
            stream_channel: None,
            content_text: None,
            provider_item_id: None,
            tool_use_id: None,
            tool_name: None,
        }
    }

    #[test]
    fn claude_compaction_frames_map_to_borg_context_compaction_events() {
        let boundary = map_claude_event(
            claude_system_frame(serde_json::json!({
                "type": "system",
                "subtype": "compact_boundary",
                "session_id": "session-1",
                "compact_metadata": {"trigger": "auto", "pre_tokens": 142_000},
            })),
            ProviderBillingMode::Unknown,
        );
        let ChatStreamEvent::ProviderEvent { kind, payload, .. } = boundary else {
            panic!("compact boundary must stay a provider event");
        };
        assert_eq!(kind, "context_compaction");
        assert_eq!(payload["status"], "completed");
        assert_eq!(payload["provider_context_preserved"], true);
        assert!(payload.get("provider_recovery_checkpoint").is_none());
        assert_eq!(payload["pre_tokens"], 142_000);
        assert_eq!(payload["trigger"], "auto");
        let summary = payload["summary"].as_str().expect("summary");
        assert!(
            summary.contains("auto") && summary.contains("142000"),
            "{summary}"
        );

        let compacting = map_claude_event(
            claude_system_frame(serde_json::json!({
                "type": "system",
                "subtype": "status",
                "status": "compacting",
            })),
            ProviderBillingMode::Unknown,
        );
        let ChatStreamEvent::ProviderEvent { kind, payload, .. } = compacting else {
            panic!("compacting status must stay a provider event");
        };
        assert_eq!(kind, "context_compaction");
        assert_eq!(payload["status"], "started");
        assert_eq!(payload["provider_context_preserved"], true);

        for opaque in [
            serde_json::json!({"type": "system", "subtype": "init", "session_id": "s"}),
            serde_json::json!({"type": "system", "subtype": "status", "status": null}),
            serde_json::json!({"type": "system", "subtype": "background_tasks_changed"}),
        ] {
            let ChatStreamEvent::ProviderEvent { kind, .. } =
                map_claude_event(claude_system_frame(opaque), ProviderBillingMode::Unknown)
            else {
                panic!("system frames stay provider events");
            };
            assert_eq!(kind, "claude.system");
        }
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
    fn pooled_claude_done_event_reports_the_running_total_delta() {
        let tracker = ClaudeCostTracker::default();
        let done = |session_id: &str, cost_microusd| ChatStreamEvent::Done {
            final_text: "done".to_string(),
            usage: Some(ProviderCallUsage {
                cost_microusd,
                ..Default::default()
            }),
            session_id: Some(session_id.to_string()),
            provider_turn_id: None,
        };
        let cost = |event| match normalize_claude_cost(event, Some(&tracker)) {
            ChatStreamEvent::Done {
                usage: Some(ProviderCallUsage { cost_microusd, .. }),
                ..
            } => cost_microusd,
            _ => unreachable!("cost normalization must preserve the done event"),
        };

        assert_eq!(cost(done("first-process", Some(100))), Some(100));
        assert_eq!(cost(done("first-process", Some(160))), Some(60));
        assert_eq!(cost(done("first-process", None)), None);
        assert_eq!(cost(done("first-process", Some(220))), Some(60));
        assert_eq!(cost(done("second-process", Some(300))), Some(300));
        assert_eq!(cost(done("second-process", Some(315))), Some(15));
        assert!(matches!(
            normalize_claude_cost(
                ChatStreamEvent::Failed {
                    error: "turn failed".to_string(),
                    kind: ProviderErrorKind::Unknown,
                },
                Some(&tracker),
            ),
            ChatStreamEvent::Failed { .. }
        ));
        assert_eq!(cost(done("second-process", Some(400))), Some(400));
    }
}
