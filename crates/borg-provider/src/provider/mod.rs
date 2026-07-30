pub mod chat_stream;
mod codex_app_server;
mod model_turn;
mod openai_compatible;
mod openrouter;
mod sdk_providers;

pub use chat_stream::{
    ChatApprovalDecision, ChatGitCredential, ChatProviderAuth, ChatStreamControl, ChatStreamEvent,
    ChatStreamRequest, ClaudeSdkPool, CodexAppServerPool, LocalAgentPermission,
    run_claude_chat_stream, run_claude_chat_stream_with_control, run_claude_local_chat_stream,
    run_codex_chat_stream, run_codex_chat_stream_with_control, run_codex_freeform_chat_stream,
    run_codex_local_chat_stream, run_opencode_local_chat_stream,
    run_pooled_claude_local_chat_stream, run_pooled_codex_local_chat_stream,
};
pub use codex_app_server::{CodexAppServerClient, CodexWeeklyUsage, TokenUsage};
pub use model_turn::{
    ModelFunctionCall, ModelInputAttachment, ModelMessage, ModelToolCall, ModelToolDefinition,
    ModelTurnRequest, ModelTurnResult,
};
pub use openai_compatible::{
    ModelGateway, OpenAiCompatibleProfile, OpenAiCompatibleProvider, kimi_cost_microusd,
    kimi_usage_from_response,
};
pub use openrouter::OpenRouterProvider;
pub use sdk_providers::{
    ClaudeSdkProvider, CodexAppServerProvider, await_freeform_result, await_structured_result,
};

use std::collections::VecDeque;
use std::error::Error as StdError;
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc::UnboundedSender};

use crate::bounded_io::read_file_text_with_limit;
pub(crate) use crate::env::nonempty_var as nonempty_env;
use crate::runtime::{CostBasis, ProviderCallUsage};

/// If a provider process emits no stdout/stderr for this long, we treat it as
/// stalled and kill it. Override with `BORG_PROVIDER_STALL_TIMEOUT_SECS` (set
/// to `0` to disable). 20 min default: covers legitimately long model thinking
/// and first-byte queueing under parallel load without keeping a truly dead
/// process locked up overnight.
const PROVIDER_STALL_TIMEOUT_DEFAULT_SECS: u64 = 1200;

/// Absolute wall-clock ceiling for a single provider call. This is deliberately
/// separate from the stall timeout: app-server streams can emit internal
/// progress indefinitely without producing a terminal model answer.
const PROVIDER_CALL_TIMEOUT_DEFAULT_SECS: u64 = 3600;
const PROVIDER_HTTP_ERROR_BODY_MAX_BYTES: usize = 64 * 1024;
const PROVIDER_HTTP_SUCCESS_BODY_MAX_BYTES: usize = 128 * 1024 * 1024;
const PROVIDER_MCP_CONFIG_MAX_BYTES: u64 = 1024 * 1024;

pub fn provider_stall_timeout() -> Option<Duration> {
    provider_timeout_from_env(
        "BORG_PROVIDER_STALL_TIMEOUT_SECS",
        PROVIDER_STALL_TIMEOUT_DEFAULT_SECS,
    )
}

pub fn provider_call_timeout() -> Option<Duration> {
    provider_timeout_from_env(
        "BORG_PROVIDER_CALL_TIMEOUT_SECS",
        PROVIDER_CALL_TIMEOUT_DEFAULT_SECS,
    )
}

pub(crate) fn apply_provider_request_timeout(
    request: reqwest::RequestBuilder,
) -> reqwest::RequestBuilder {
    match provider_call_timeout() {
        Some(timeout) => request.timeout(timeout),
        None => request,
    }
}

fn provider_timeout_from_env(name: &str, default_secs: u64) -> Option<Duration> {
    match std::env::var(name) {
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(0) => None,
            Ok(secs) => Some(Duration::from_secs(secs)),
            Err(error) => {
                tracing::warn!(
                    var = name,
                    %error,
                    "invalid provider timeout environment value; using default"
                );
                Some(Duration::from_secs(default_secs))
            }
        },
        Err(_) => Some(Duration::from_secs(default_secs)),
    }
}

pub(crate) async fn read_provider_error_response_text(
    response: reqwest::Response,
) -> Result<String> {
    read_provider_response_text_with_limit(
        response,
        "provider error response",
        PROVIDER_HTTP_ERROR_BODY_MAX_BYTES,
    )
    .await
}

pub(crate) async fn read_provider_success_response_text(
    response: reqwest::Response,
) -> Result<String> {
    read_provider_response_text_with_limit(
        response,
        "provider response",
        PROVIDER_HTTP_SUCCESS_BODY_MAX_BYTES,
    )
    .await
}

pub(super) fn read_provider_mcp_config_text(path: &Path) -> Result<String> {
    read_file_text_with_limit(path, "provider MCP config", PROVIDER_MCP_CONFIG_MAX_BYTES)
}

async fn read_provider_response_text_with_limit(
    mut response: reqwest::Response,
    label: &'static str,
    max_bytes: usize,
) -> Result<String> {
    if response
        .content_length()
        .is_some_and(|len| len > max_bytes as u64)
    {
        bail!("{label} exceeded {max_bytes} byte limit");
    }

    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if provider_response_body_would_exceed_limit(bytes.len(), chunk.len(), max_bytes) {
            bail!("{label} exceeded {max_bytes} byte limit");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(String::from_utf8(bytes)
        .unwrap_or_else(|error| String::from_utf8_lossy(&error.into_bytes()).into_owned()))
}

fn provider_response_body_would_exceed_limit(
    current_len: usize,
    next_len: usize,
    max_bytes: usize,
) -> bool {
    current_len.saturating_add(next_len) > max_bytes
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChatCompletionResponseFormat {
    JsonObject,
    JsonSchema,
}

pub(crate) fn chat_completion_response_format(
    schema: &Value,
    format: ChatCompletionResponseFormat,
) -> Value {
    match format {
        ChatCompletionResponseFormat::JsonObject => json!({ "type": "json_object" }),
        ChatCompletionResponseFormat::JsonSchema => json!({
            "type": "json_schema",
            "json_schema": {
                "name": "borg_response",
                "strict": true,
                "schema": schema,
            },
        }),
    }
}

pub(crate) fn managed_openai_api_key() -> Option<String> {
    nonempty_env("BORG_OPENAI_API_KEY").or_else(|| nonempty_env("OPENAI_API_KEY"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StructuredOutputDialect {
    FlexibleJson,
    StrictObjects,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderProgressStream {
    Stdout,
    Stderr,
}

/// A live signal from a running provider call. Byte chunks (Stdout /
/// Stderr) are buffered into the attempt's stored stdout/stderr; narration
/// and tool events are forwarded directly into `stream_events` with owner
/// `RunAttempt` so the run timeline shows the same action feed shape chat
/// persists for `ChatMessage`-owned events.
#[derive(Debug, Clone)]
pub enum ProviderProgress {
    Bytes {
        stream: ProviderProgressStream,
        chunk: Vec<u8>,
    },
    Narration {
        text: String,
    },
    Phase {
        name: String,
        input: Value,
    },
    ToolCallStarted {
        id: String,
        name: String,
        input: Value,
    },
    ToolCallCompleted {
        tool_use_id: String,
        output: String,
        is_error: bool,
        input: Option<Value>,
    },
    ProviderEvent {
        kind: String,
        payload: Value,
        raw_payload: Option<Value>,
        stream_channel: Option<String>,
        content_text: Option<String>,
        provider_item_id: Option<String>,
        tool_use_id: Option<String>,
        tool_name: Option<String>,
        model: Option<String>,
        effort: Option<String>,
    },
}

impl ProviderProgress {
    pub fn stdout(chunk: Vec<u8>) -> Self {
        Self::Bytes {
            stream: ProviderProgressStream::Stdout,
            chunk,
        }
    }
    pub fn stderr(chunk: Vec<u8>) -> Self {
        Self::Bytes {
            stream: ProviderProgressStream::Stderr,
            chunk,
        }
    }
}

#[async_trait]
pub trait Provider: Send + Sync {
    fn as_any(&self) -> &dyn std::any::Any;
    /// Make a structured JSON call. If `session_id` is Some, the provider
    /// should resume that conversation (multi-turn) rather than starting fresh.
    async fn structured_call_with_progress(
        &self,
        prompt: &str,
        schema: &Value,
        session_id: Option<&str>,
        progress: Option<UnboundedSender<ProviderProgress>>,
    ) -> std::result::Result<ProviderCallResult, ProviderCallError>;
    async fn structured_call(
        &self,
        prompt: &str,
        schema: &Value,
        session_id: Option<&str>,
    ) -> std::result::Result<ProviderCallResult, ProviderCallError> {
        self.structured_call_with_progress(prompt, schema, session_id, None)
            .await
    }
    async fn freeform_call_with_progress(
        &self,
        prompt: &str,
        session_id: Option<&str>,
        progress: Option<UnboundedSender<ProviderProgress>>,
    ) -> std::result::Result<ProviderCallResult, ProviderCallError> {
        self.structured_call_with_progress(
            prompt,
            &serde_json::json!({ "type": "string" }),
            session_id,
            progress,
        )
        .await
    }
    async fn freeform_call(
        &self,
        prompt: &str,
        session_id: Option<&str>,
    ) -> std::result::Result<ProviderCallResult, ProviderCallError> {
        self.freeform_call_with_progress(prompt, session_id, None)
            .await
    }
    fn label(&self) -> &'static str;
    fn structured_output_dialect(&self) -> StructuredOutputDialect;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ProviderInvocation {
    pub provider_label: String,
    pub executable: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub cwd: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ProviderAttemptTrace {
    pub invocation: ProviderInvocation,
    pub exit_status: Option<i32>,
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
}

#[derive(Debug, Clone)]
pub struct ProviderCallResult {
    pub value: Value,
    pub raw_response: Value,
    pub usage: ProviderCallUsage,
    pub trace: ProviderAttemptTrace,
    /// The backend's conversation / session identifier, if the
    /// provider surfaced one. Callers (the stage runner) persist it
    /// and pass it back via `structured_call_with_progress`'s
    /// `session_id` arg on the next turn, letting the backend
    /// reuse its server-side context (prompt cache, tool history,
    /// reasoning chain) instead of rebuilding from scratch.
    pub session_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ProviderCallError {
    pub message: String,
    pub trace: ProviderAttemptTrace,
    pub session_id: Option<String>,
}

impl fmt::Display for ProviderCallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl StdError for ProviderCallError {}

impl ProviderCallError {
    fn new(message: impl Into<String>, trace: ProviderAttemptTrace) -> Self {
        Self {
            message: message.into(),
            trace,
            session_id: None,
        }
    }

    fn with_session(
        message: impl Into<String>,
        trace: ProviderAttemptTrace,
        session_id: Option<String>,
    ) -> Self {
        Self {
            message: message.into(),
            trace,
            session_id,
        }
    }
}

fn trace_from_buffers(
    invocation: ProviderInvocation,
    exit_status: Option<i32>,
    stdout: &[u8],
    stderr: &[u8],
) -> ProviderAttemptTrace {
    ProviderAttemptTrace {
        invocation,
        exit_status,
        stdout: String::from_utf8_lossy(stdout).into_owned(),
        stderr: String::from_utf8_lossy(stderr).into_owned(),
    }
}

#[derive(Debug, Clone, Default)]
pub struct MockProvider {
    responses: Arc<Mutex<VecDeque<MockResponse>>>,
}

impl MockProvider {
    pub fn new(responses: Vec<Value>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(VecDeque::from(
                responses
                    .into_iter()
                    .map(MockResponse::Value)
                    .collect::<Vec<_>>(),
            ))),
        }
    }

    pub fn scripted(responses: Vec<MockResponse>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(VecDeque::from(responses))),
        }
    }
}

#[derive(Debug, Clone)]
pub enum MockResponse {
    Value(Value),
    Failure(String),
}

#[async_trait]
impl Provider for MockProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    async fn structured_call_with_progress(
        &self,
        _prompt: &str,
        _schema: &Value,
        _session_id: Option<&str>,
        _progress: Option<UnboundedSender<ProviderProgress>>,
    ) -> std::result::Result<ProviderCallResult, ProviderCallError> {
        match self.responses.lock().await.pop_front().ok_or_else(|| {
            ProviderCallError::new(
                "mock provider ran out of responses",
                ProviderAttemptTrace {
                    invocation: ProviderInvocation {
                        provider_label: self.label().to_string(),
                        executable: "mock".to_string(),
                        args: Vec::new(),
                        cwd: None,
                        model: None,
                        effort: None,
                    },
                    exit_status: None,
                    stdout: String::new(),
                    stderr: String::new(),
                },
            )
        })? {
            MockResponse::Value(value) => {
                let stdout = serde_json::to_string_pretty(&value).unwrap_or_else(|error| {
                    tracing::warn!(
                        %error,
                        "failed to serialize mock provider value for trace stdout; using compact JSON"
                    );
                    value.to_string()
                });
                Ok(ProviderCallResult {
                    raw_response: value.clone(),
                    value,
                    usage: ProviderCallUsage::default(),
                    session_id: None,
                    trace: ProviderAttemptTrace {
                        invocation: ProviderInvocation {
                            provider_label: self.label().to_string(),
                            executable: "mock".to_string(),
                            args: Vec::new(),
                            cwd: None,
                            model: None,
                            effort: None,
                        },
                        exit_status: Some(0),
                        stdout,
                        stderr: String::new(),
                    },
                })
            }
            MockResponse::Failure(message) => Err(ProviderCallError::new(
                message.clone(),
                ProviderAttemptTrace {
                    invocation: ProviderInvocation {
                        provider_label: self.label().to_string(),
                        executable: "mock".to_string(),
                        args: Vec::new(),
                        cwd: None,
                        model: None,
                        effort: None,
                    },
                    exit_status: Some(1),
                    stdout: String::new(),
                    stderr: message,
                },
            )),
        }
    }

    async fn freeform_call_with_progress(
        &self,
        _prompt: &str,
        _session_id: Option<&str>,
        _progress: Option<UnboundedSender<ProviderProgress>>,
    ) -> std::result::Result<ProviderCallResult, ProviderCallError> {
        let result = self
            .structured_call_with_progress("", &serde_json::json!({ "type": "string" }), None, None)
            .await?;
        let text = match &result.value {
            Value::String(text) => text.clone(),
            other => serde_json::to_string_pretty(other)
                .expect("serializing serde_json::Value for mock provider text should not fail"),
        };
        Ok(ProviderCallResult {
            value: Value::String(text.clone()),
            raw_response: serde_json::json!({ "text": text }),
            ..result
        })
    }

    fn label(&self) -> &'static str {
        "mock"
    }

    fn structured_output_dialect(&self) -> StructuredOutputDialect {
        StructuredOutputDialect::FlexibleJson
    }
}

pub use crate::CLAUDE_SELECTABLE_MODELS;
pub const CLAUDE_DEFAULT_MODEL: &str = crate::CLAUDE_MODEL_CATALOG.default_model;

pub fn default_model_for_backend(backend: &str) -> Option<String> {
    match backend {
        "claude" => Some(CLAUDE_DEFAULT_MODEL.to_string()),
        "codex" => Some(crate::codex_product_model().to_string()),
        "openrouter" => Some("deepseek/deepseek-v4-flash".to_string()),
        "openai-compatible" => Some("openai-compatible-model".to_string()),
        _ => None,
    }
}

pub fn default_effort_for_backend(backend: &str) -> Option<String> {
    match backend {
        "codex" => Some(crate::codex_default_effort().to_string()),
        "openrouter" | "openai-compatible" => Some("medium".to_string()),
        _ => None,
    }
}

/// Project a Claude Agent SDK message envelope into a `ProviderCallUsage`.
/// Used by the live chat-stream parser (chat_stream.rs) to harvest token
/// usage / cost off the streamed `assistant` envelope. Public so it can
/// be called across modules in this crate.
pub(crate) fn extract_claude_usage(envelope: &Value) -> ProviderCallUsage {
    // SDK assistant envelopes keep Anthropic usage under `message.usage`,
    // while terminal result envelopes expose cumulative usage at the top
    // level. Accept both shapes so final turn telemetry is never replaced by
    // an all-zero fallback.
    let usage = envelope
        .get("usage")
        .or_else(|| envelope.pointer("/message/usage"));
    let input_tokens = usage
        .and_then(|value| value.get("input_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cached_input_tokens = usage
        .and_then(|value| value.get("cache_read_input_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cache_creation_input_tokens = usage
        .and_then(|value| value.get("cache_creation_input_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output_tokens = usage
        .and_then(|value| value.get("output_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let duration_ms = envelope
        .get("duration_ms")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let total_tokens = input_tokens
        .saturating_add(cached_input_tokens)
        .saturating_add(cache_creation_input_tokens)
        .saturating_add(output_tokens);
    let cost_microusd = envelope
        .get("total_cost_usd")
        .and_then(Value::as_f64)
        .and_then(provider_cost_usd_to_microusd);

    ProviderCallUsage {
        duration_ms,
        input_tokens,
        cached_input_tokens,
        cache_creation_input_tokens,
        output_tokens,
        total_tokens,
        context_tokens: None,
        context_window_tokens: None,
        cost_microusd,
        cost_basis: if cost_microusd.is_some() {
            CostBasis::ProviderReported
        } else {
            CostBasis::Unavailable
        },
    }
}

pub(crate) fn extract_chat_completions_usage(
    raw: &Value,
    duration_ms: u64,
    cost_microusd: Option<u64>,
) -> ProviderCallUsage {
    let usage = raw.get("usage").unwrap_or(&Value::Null);
    let prompt_tokens = usage
        .get("prompt_tokens")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let output_tokens = usage
        .get("completion_tokens")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let cached_input_tokens = usage
        .pointer("/prompt_tokens_details/cached_tokens")
        .and_then(Value::as_u64)
        .unwrap_or_default()
        .min(prompt_tokens);
    let input_tokens = prompt_tokens.saturating_sub(cached_input_tokens);
    ProviderCallUsage {
        input_tokens,
        output_tokens,
        cached_input_tokens,
        cache_creation_input_tokens: 0,
        total_tokens: usage
            .get("total_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_else(|| prompt_tokens.saturating_add(output_tokens)),
        context_tokens: None,
        context_window_tokens: None,
        duration_ms,
        cost_microusd,
        cost_basis: if cost_microusd.is_some() {
            CostBasis::ProviderReported
        } else {
            CostBasis::Unavailable
        },
    }
}

fn estimate_openai_cost_microusd(model: &str, usage: &ProviderCallUsage) -> Option<u64> {
    let pricing = openai_model_pricing(model)?;
    let long_context_pricing = pricing
        .long_context_input_threshold
        .is_some_and(|threshold| {
            usage.input_tokens.saturating_add(usage.cached_input_tokens) > threshold
        });
    let input_rate = if long_context_pricing {
        pricing.input_microusd_per_million.saturating_mul(2)
    } else {
        pricing.input_microusd_per_million
    };
    let cached_input_rate = if long_context_pricing {
        pricing.cached_input_microusd_per_million.saturating_mul(2)
    } else {
        pricing.cached_input_microusd_per_million
    };
    let cache_creation_input_rate = if long_context_pricing {
        pricing
            .cache_creation_input_microusd_per_million
            .saturating_mul(2)
    } else {
        pricing.cache_creation_input_microusd_per_million
    };
    let output_rate = if long_context_pricing {
        pricing.output_microusd_per_million.saturating_mul(3) / 2
    } else {
        pricing.output_microusd_per_million
    };
    Some(
        microusd_for_tokens(usage.input_tokens, input_rate)
            .saturating_add(microusd_for_tokens(
                usage.cached_input_tokens,
                cached_input_rate,
            ))
            .saturating_add(microusd_for_tokens(
                usage.cache_creation_input_tokens,
                cache_creation_input_rate,
            ))
            .saturating_add(microusd_for_tokens(usage.output_tokens, output_rate)),
    )
}

/// Estimate the extra API-equivalent cost of reprocessing tokens that would
/// otherwise have been served from OpenAI's prompt cache.
pub fn estimate_openai_cache_miss_microusd(
    model: &str,
    missed_tokens: u64,
    prompt_tokens: u64,
) -> Option<u64> {
    let pricing = openai_model_pricing(model)?;
    let long_context_pricing = pricing
        .long_context_input_threshold
        .is_some_and(|threshold| prompt_tokens > threshold);
    let multiplier = if long_context_pricing { 2 } else { 1 };
    let input_rate = pricing
        .input_microusd_per_million
        .saturating_mul(multiplier);
    let cached_rate = pricing
        .cached_input_microusd_per_million
        .saturating_mul(multiplier);
    Some(microusd_for_tokens(
        missed_tokens,
        input_rate.saturating_sub(cached_rate),
    ))
}

fn microusd_for_tokens(tokens: u64, microusd_per_million: u64) -> u64 {
    let numerator = u128::from(tokens).saturating_mul(u128::from(microusd_per_million));
    u64::try_from((numerator + 500_000) / 1_000_000).unwrap_or(u64::MAX)
}

fn provider_cost_usd_to_microusd(cost_usd: f64) -> Option<u64> {
    if !cost_usd.is_finite() || cost_usd < 0.0 {
        return None;
    }
    let cost_microusd = (cost_usd * 1_000_000.0).round();
    if !cost_microusd.is_finite() || cost_microusd > u64::MAX as f64 {
        return None;
    }
    Some(cost_microusd as u64)
}

struct OpenAiModelPricing {
    input_microusd_per_million: u64,
    cached_input_microusd_per_million: u64,
    cache_creation_input_microusd_per_million: u64,
    output_microusd_per_million: u64,
    long_context_input_threshold: Option<u64>,
}

fn openai_model_pricing(model: &str) -> Option<OpenAiModelPricing> {
    let normalized = model.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "gpt-5.5" => Some(OpenAiModelPricing {
            input_microusd_per_million: 5_000_000,
            cached_input_microusd_per_million: 500_000,
            cache_creation_input_microusd_per_million: 5_000_000,
            output_microusd_per_million: 30_000_000,
            long_context_input_threshold: Some(272_000),
        }),
        model if model == crate::codex_product_model() => Some(OpenAiModelPricing {
            input_microusd_per_million: 5_000_000,
            cached_input_microusd_per_million: 500_000,
            cache_creation_input_microusd_per_million: 6_250_000,
            output_microusd_per_million: 30_000_000,
            long_context_input_threshold: Some(272_000),
        }),
        _ => None,
    }
}

pub(crate) fn parse_chat_completion_json_text(text: &str) -> Option<Value> {
    let trimmed = text.trim();
    if let Some(value) = parse_any_json_value_or_sanitized(trimmed) {
        return Some(value);
    }
    if let Some(fenced) = extract_fenced_json(trimmed)
        && let Some(value) = parse_any_json_value_or_sanitized(fenced)
    {
        return Some(value);
    }
    parse_balanced_json_candidates(trimmed)
}

fn parse_any_json_value_or_sanitized(text: &str) -> Option<Value> {
    serde_json::from_str::<Value>(text).ok().or_else(|| {
        sanitize_json_control_chars(text)
            .as_deref()
            .and_then(|sanitized| serde_json::from_str::<Value>(sanitized).ok())
    })
}

pub(crate) fn truncate_provider_text(text: &str, max: usize) -> String {
    let mut chars = text.chars();
    let truncated: String = chars.by_ref().take(max).collect();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        text.to_string()
    }
}

fn extract_fenced_json(text: &str) -> Option<&str> {
    next_fenced_block(text, 0).map(|(block, _)| block)
}

fn next_fenced_block(text: &str, search_from: usize) -> Option<(&str, usize)> {
    let tail = text.get(search_from..)?;
    let fence_start = tail.find("```")?;
    let content_start = search_from + fence_start + 3;
    let raw_rest = &text[content_start..];
    let rest = strip_json_fence_prefix(raw_rest);
    let skipped_prefix_len = raw_rest.len() - rest.len();
    let fence_end = rest.find("```")?;
    let next_search_from = content_start + skipped_prefix_len + fence_end + 3;
    Some((rest[..fence_end].trim(), next_search_from))
}

fn strip_json_fence_prefix(rest: &str) -> &str {
    let rest = rest
        .strip_prefix("json")
        .or_else(|| rest.strip_prefix("JSON"))
        .unwrap_or(rest);
    rest.strip_prefix('\n').unwrap_or(rest)
}

fn sanitize_json_control_chars(text: &str) -> Option<String> {
    text.chars().any(is_json_control_char).then(|| {
        text.chars()
            .map(|character| {
                if is_json_control_char(character) {
                    ' '
                } else {
                    character
                }
            })
            .collect()
    })
}

fn is_json_control_char(character: char) -> bool {
    character <= '\u{001f}'
}

fn parse_balanced_json_candidates(text: &str) -> Option<Value> {
    for (start, byte) in text.as_bytes().iter().enumerate() {
        if !b"{[".contains(byte) {
            continue;
        }
        let Some(candidate) = extract_balanced_json_from_start(text, start) else {
            continue;
        };
        if let Some(value) = parse_any_json_value_or_sanitized(candidate) {
            return Some(value);
        }
    }
    None
}

fn extract_balanced_json_from_start(text: &str, start: usize) -> Option<&str> {
    let bytes = text.as_bytes();
    let opening = *bytes.get(start)?;
    if opening != b'{' && opening != b'[' {
        return None;
    }
    let closing = if opening == b'{' { b'}' } else { b']' };
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (offset, byte) in bytes[start..].iter().enumerate() {
        if in_string {
            if escaped {
                escaped = false;
                continue;
            }
            match *byte {
                b'\\' => escaped = true,
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }

        match *byte {
            b'"' => in_string = true,
            byte if byte == opening => depth += 1,
            byte if byte == closing => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    let end = start + offset + 1;
                    return text.get(start..end);
                }
            }
            _ => {}
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        CLAUDE_DEFAULT_MODEL, CLAUDE_SELECTABLE_MODELS, PROVIDER_HTTP_ERROR_BODY_MAX_BYTES,
        PROVIDER_MCP_CONFIG_MAX_BYTES, estimate_openai_cache_miss_microusd,
        estimate_openai_cost_microusd, extract_chat_completions_usage, extract_claude_usage,
        microusd_for_tokens, parse_chat_completion_json_text, provider_cost_usd_to_microusd,
        provider_response_body_would_exceed_limit, read_provider_mcp_config_text,
        truncate_provider_text,
    };

    #[test]
    fn provider_error_body_limit_allows_exact_boundary() {
        assert!(!provider_response_body_would_exceed_limit(
            PROVIDER_HTTP_ERROR_BODY_MAX_BYTES - 1,
            1,
            PROVIDER_HTTP_ERROR_BODY_MAX_BYTES,
        ));
        assert!(provider_response_body_would_exceed_limit(
            PROVIDER_HTTP_ERROR_BODY_MAX_BYTES,
            1,
            PROVIDER_HTTP_ERROR_BODY_MAX_BYTES,
        ));
    }

    #[test]
    fn provider_mcp_config_reader_rejects_oversized_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.json");
        let file = std::fs::File::create(&path).expect("create sparse config");
        file.set_len(PROVIDER_MCP_CONFIG_MAX_BYTES + 1)
            .expect("set sparse config length");

        let error = read_provider_mcp_config_text(&path)
            .expect_err("oversized provider MCP config should fail")
            .to_string();

        assert!(
            error.contains("provider MCP config")
                && error.contains("exceeds")
                && error.contains("byte limit"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn openai_cost_estimate_uses_current_codex_pricing() {
        let usage = crate::runtime::ProviderCallUsage {
            input_tokens: 100_000,
            cached_input_tokens: 100_000,
            output_tokens: 100_000,
            ..Default::default()
        };

        let cost = estimate_openai_cost_microusd(crate::codex_product_model(), &usage).unwrap();
        assert_eq!(cost, 3_550_000);
    }

    #[test]
    fn openai_cost_estimate_rejects_non_gpt_5_5_models() {
        let usage = crate::runtime::ProviderCallUsage {
            input_tokens: 1_000,
            output_tokens: 1_000,
            ..Default::default()
        };

        assert!(estimate_openai_cost_microusd("gpt-5.4", &usage).is_none());
    }

    #[test]
    fn openai_cost_estimate_applies_long_context_uplift() {
        let usage = crate::runtime::ProviderCallUsage {
            input_tokens: 272_001,
            cached_input_tokens: 0,
            output_tokens: 1_000_000,
            ..Default::default()
        };

        let cost = estimate_openai_cost_microusd(crate::codex_product_model(), &usage).unwrap();
        assert_eq!(cost, 47_720_010);
    }

    #[test]
    fn openai_cache_miss_estimate_is_the_uncached_read_premium() {
        assert_eq!(
            estimate_openai_cache_miss_microusd(crate::codex_product_model(), 100_000, 100_000,),
            Some(450_000)
        );
    }

    #[test]
    fn token_cost_estimate_saturates_when_rounded_cost_exceeds_u64() {
        assert_eq!(microusd_for_tokens(u64::MAX, u64::MAX), u64::MAX);
    }

    #[test]
    fn provider_reported_cost_usd_conversion_rejects_invalid_values() {
        assert_eq!(provider_cost_usd_to_microusd(0.001234), Some(1234));
        assert_eq!(provider_cost_usd_to_microusd(-0.01), None);
        assert_eq!(provider_cost_usd_to_microusd(f64::NAN), None);
        assert_eq!(provider_cost_usd_to_microusd(f64::INFINITY), None);
        assert_eq!(provider_cost_usd_to_microusd(f64::MAX), None);
    }

    #[test]
    fn chat_completions_usage_extracts_tokens_and_provider_cost() {
        let raw = json!({
            "usage": {
                "prompt_tokens": 100,
                "completion_tokens": 25,
                "total_tokens": 130,
                "prompt_tokens_details": {
                    "cached_tokens": 40
                }
            }
        });

        let usage = extract_chat_completions_usage(&raw, 250, Some(1234));

        assert_eq!(usage.input_tokens, 60);
        assert_eq!(usage.output_tokens, 25);
        assert_eq!(usage.cached_input_tokens, 40);
        assert_eq!(usage.total_tokens, 130);
        assert_eq!(usage.duration_ms, 250);
        assert_eq!(usage.cost_microusd, Some(1234));
    }

    #[test]
    fn claude_usage_accepts_assistant_and_result_envelopes() {
        let assistant = json!({
            "type": "assistant",
            "message": {
                "usage": {
                    "input_tokens": 11,
                    "cache_read_input_tokens": 7,
                    "cache_creation_input_tokens": 5,
                    "output_tokens": 3
                }
            }
        });
        let assistant_usage = extract_claude_usage(&assistant);
        assert_eq!(assistant_usage.input_tokens, 11);
        assert_eq!(assistant_usage.cached_input_tokens, 7);
        assert_eq!(assistant_usage.cache_creation_input_tokens, 5);
        assert_eq!(assistant_usage.output_tokens, 3);
        assert_eq!(assistant_usage.total_tokens, 26);

        let result = json!({
            "type": "result",
            "duration_ms": 250,
            "total_cost_usd": 0.012345,
            "usage": {
                "input_tokens": 20,
                "cache_read_input_tokens": 10,
                "output_tokens": 4
            }
        });
        let result_usage = extract_claude_usage(&result);
        assert_eq!(result_usage.total_tokens, 34);
        assert_eq!(result_usage.duration_ms, 250);
        assert_eq!(result_usage.cost_microusd, Some(12_345));
    }

    #[test]
    fn chat_completion_json_text_parses_fenced_and_balanced_json() {
        assert_eq!(
            parse_chat_completion_json_text("```json\n{\"ok\":true}\n```").unwrap(),
            json!({ "ok": true })
        );
        assert_eq!(
            parse_chat_completion_json_text("```\n{\"ok\":true}\n```").unwrap(),
            json!({ "ok": true })
        );
        assert_eq!(
            parse_chat_completion_json_text("Answer: {\"ok\":true} trailing").unwrap(),
            json!({ "ok": true })
        );
        assert_eq!(
            parse_chat_completion_json_text(
                "{\"arguments\":{\"facts\":[{\"text\":\"cut\n\n{\"arguments\":{\"facts\":[]}}"
            )
            .unwrap(),
            json!({ "arguments": { "facts": [] } })
        );
        assert_eq!(
            parse_chat_completion_json_text("{\"text\":\"line one\nline two\"}").unwrap(),
            json!({ "text": "line one line two" })
        );
        assert_eq!(
            parse_chat_completion_json_text("\"plain json string\"").unwrap(),
            json!("plain json string")
        );
    }

    #[test]
    fn provider_text_truncation_preserves_utf8_boundaries() {
        assert_eq!(truncate_provider_text("abé", 2), "ab...");
        assert_eq!(truncate_provider_text("é", 1), "é");
        assert_eq!(truncate_provider_text("éx", 1), "é...");
    }

    #[test]
    fn claude_catalog_contains_only_current_selectable_models() {
        assert_eq!(
            CLAUDE_SELECTABLE_MODELS,
            [
                ("claude-opus-5", "Opus 5"),
                ("claude-sonnet-5", "Sonnet 5"),
                ("claude-fable-5", "Fable 5"),
            ]
        );
        assert!(
            CLAUDE_SELECTABLE_MODELS
                .iter()
                .any(|(model, _)| *model == CLAUDE_DEFAULT_MODEL)
        );
    }
}
