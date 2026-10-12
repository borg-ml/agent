//! Shared stream contracts, subscription account metadata, and compatibility adapters.
//!
//! Claude turns run through Borg's native harness. The legacy Claude stream
//! entrypoints remain only to reject calls without starting an external runtime.

#![cfg_attr(not(any(feature = "codex", feature = "claude")), allow(dead_code))]

use crate::mcp::ExternalMcpServer;
use crate::runtime::ProviderCallUsage;
use crate::{ProviderAuthBundle, ProviderAuthProvider, ProviderChannel};
#[cfg(not(feature = "claude"))]
use anyhow::bail;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

#[path = "opencode_stream.rs"]
mod opencode_stream;

pub use opencode_stream::run_opencode_local_chat_stream;

#[path = "grok_stream.rs"]
mod grok_stream;

pub use grok_stream::run_grok_local_chat_stream;

#[path = "muse_stream.rs"]
mod muse_stream;

pub use muse_stream::run_muse_local_chat_stream;

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

    pub fn is_recalled(&self) -> bool {
        self.0.load(Ordering::Acquire) == STEER_RECALLED
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

#[derive(Clone)]
pub struct ChatProviderAuth {
    pub provider: ProviderAuthProvider,
    pub bundle: ProviderAuthBundle,
    /// Persistent login authority. Native turns never copy rotating tokens.
    pub codex_home: Option<PathBuf>,
    pub claude_config_dir: Option<PathBuf>,
}

impl fmt::Debug for ChatProviderAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChatProviderAuth")
            .field("provider", &self.provider)
            .field("bundle", &"<redacted>")
            .field("codex_home", &self.codex_home)
            .field("claude_config_dir", &self.claude_config_dir)
            .finish()
    }
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

/// Read subscription usage directly. No provider process or model turn is started.
pub async fn read_claude_account_rate_limits() -> Result<ClaudeAccountRateLimits> {
    #[cfg(not(feature = "claude"))]
    {
        bail!("Claude account limits require the Claude subscription adapter")
    }
    #[cfg(feature = "claude")]
    {
        tokio::time::timeout(Duration::from_secs(10), async {
            let authority = crate::claude_subscription::AuthAuthority::resolve(None)?;
            let access = crate::claude_subscription::discover(authority).await?;
            let usage =
                crate::claude_subscription::usage(&access, &access.profile.identity).await?;
            // The raw endpoint uses percentages. Do not pass the fraction-valued
            // normalized windows into the existing percentage UI parser.
            parse_claude_account_rate_limits(&serde_json::json!({
                "subscription_type":access.profile.subscription_type,
                "rate_limits_available":true,
                "rate_limits":usage.raw,
            }))
        })
        .await
        .context("timed out reading Claude account limits")?
    }
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

/// Check Claude's current local login without starting a model turn or changing credentials.
/// A bounded failure observation from the selected native subscription route;
/// this neither launches software nor reads or rewrites credential contents.
pub fn claude_native_account_verification_failed_recently() -> Result<bool> {
    #[cfg(feature = "claude")]
    {
        super::claude_connector::recent_account_verification_failure(None)
    }
    #[cfg(not(feature = "claude"))]
    {
        Ok(false)
    }
}

pub async fn read_claude_subscription_status() -> Result<bool> {
    #[cfg(feature = "claude")]
    {
        let authority = crate::claude_subscription::AuthAuthority::resolve(None)?;
        crate::claude_subscription::credentials_present(&authority).await
    }
    #[cfg(not(feature = "claude"))]
    {
        Ok(false)
    }
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

/// Compatibility marker for callers of the retired Claude Code pool.
/// Claude turns require Borg's native harness; this type owns no runtime.
#[derive(Clone, Default)]
pub struct ClaudeSubscriptionPool {
    _private: (),
}

/// Retired compatibility entrypoint. Use Borg's native harness for Claude turns.
pub fn run_claude_chat_stream(_request: ChatStreamRequest) -> mpsc::Receiver<ChatStreamEvent> {
    claude_native_harness_required()
}

/// Retired compatibility entrypoint. Use Borg's native harness for Claude turns.
pub fn run_claude_chat_stream_with_control(
    _request: ChatStreamRequest,
    _controls: Option<mpsc::Receiver<ChatStreamControl>>,
) -> mpsc::Receiver<ChatStreamEvent> {
    claude_native_harness_required()
}

/// Retired compatibility entrypoint. Use Borg's native harness for Claude turns.
pub fn run_claude_local_chat_stream(
    _request: ChatStreamRequest,
    _controls: Option<mpsc::Receiver<ChatStreamControl>>,
    _permission: LocalAgentPermission,
) -> mpsc::Receiver<ChatStreamEvent> {
    claude_native_harness_required()
}

/// Retired compatibility entrypoint. Use Borg's native harness for Claude turns.
pub fn run_claude_local_chat_stream_pooled(
    _request: ChatStreamRequest,
    _controls: Option<mpsc::Receiver<ChatStreamControl>>,
    _permission: LocalAgentPermission,
    _pool: ClaudeSubscriptionPool,
) -> mpsc::Receiver<ChatStreamEvent> {
    claude_native_harness_required()
}

fn claude_native_harness_required() -> mpsc::Receiver<ChatStreamEvent> {
    let (events, receiver) = mpsc::channel(1);
    let _ = events.try_send(ChatStreamEvent::Failed {
        error: "Claude turns require Borg native harness".to_string(),
        kind: ProviderErrorKind::Fatal,
    });
    receiver
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
}
