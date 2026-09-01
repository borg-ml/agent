use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use serde_json::{Map, Value, json};
use tokio::sync::mpsc::UnboundedSender;

use crate::runtime::elapsed_millis_u64;

use super::{
    ChatCompletionResponseFormat, ModelMessage, ModelToolCall, ModelTurnRequest, ModelTurnResult,
    Provider, ProviderAttemptTrace, ProviderCallError, ProviderCallResult, ProviderInvocation,
    ProviderProgress, StructuredOutputDialect, apply_provider_request_timeout,
    chat_completion_response_format, extract_chat_completions_usage, nonempty_env,
    parse_chat_completion_json_text, provider_cost_usd_to_microusd,
    read_provider_error_response_text, read_provider_success_response_text, truncate_provider_text,
};

const COMPATIBLE_STREAM_MAX_BYTES: usize = 128 * 1024 * 1024;
static OPENROUTER_MODEL_LIMITS: OnceLock<Mutex<HashMap<String, OpenRouterModelLimits>>> =
    OnceLock::new();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct OpenRouterModelLimits {
    context_window_tokens: u64,
    max_completion_tokens: Option<u64>,
}

#[derive(Clone)]
pub struct ModelGateway {
    pub endpoint: String,
    pub bearer_token: String,
    /// Optional upstream model id when the gateway is addressed by a Borg
    /// `provider/model` alias.
    pub model: Option<String>,
    /// Human-readable provider identity used in traces and diagnostics.
    pub label: Option<String>,
    /// Additional headers for a configured endpoint. Values are never shown
    /// in the gateway's debug representation.
    pub headers: BTreeMap<String, String>,
    /// Provider-owned request fields. Core conversation/tool fields are
    /// protected by the request builder below.
    pub body: Map<String, Value>,
    /// Variant-specific request fields, keyed by the selected effort name.
    pub variant_bodies: BTreeMap<String, Map<String, Value>>,
    pub context_window_tokens: Option<u64>,
    pub max_output_tokens: Option<u64>,
}

impl ModelGateway {
    pub fn new(endpoint: impl Into<String>, bearer_token: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            bearer_token: bearer_token.into(),
            model: None,
            label: None,
            headers: BTreeMap::new(),
            body: Map::new(),
            variant_bodies: BTreeMap::new(),
            context_window_tokens: None,
            max_output_tokens: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenAiCompatibleProfile {
    Kimi,
    OpenRouter,
    Generic,
}

impl OpenAiCompatibleProfile {
    fn label(self) -> &'static str {
        match self {
            Self::Kimi => "kimi",
            Self::OpenRouter => "openrouter",
            Self::Generic => "openai-compatible",
        }
    }

    fn endpoint(self) -> String {
        match self {
            Self::Kimi => kimi_chat_completions_endpoint(),
            Self::OpenRouter => openrouter_chat_completions_endpoint(),
            Self::Generic => chat_completions_endpoint(),
        }
    }

    fn api_key(self) -> Option<String> {
        match self {
            Self::Kimi => {
                nonempty_env("BORG_KIMI_API_KEY").or_else(|| nonempty_env("MOONSHOT_API_KEY"))
            }
            Self::OpenRouter => {
                crate::credentials::api_key(crate::credentials::ApiKeyCredential::OpenRouter)
            }
            Self::Generic => nonempty_env("BORG_OPENAI_COMPATIBLE_API_KEY")
                .or_else(|| nonempty_env("BORG_OPENAI_API_KEY"))
                .or_else(|| nonempty_env("OPENAI_API_KEY")),
        }
    }
}

impl std::fmt::Debug for ModelGateway {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ModelGateway")
            .field("endpoint", &self.endpoint)
            .field("bearer_token", &"[redacted]")
            .field("model", &self.model)
            .field("label", &self.label)
            .field("header_names", &self.headers.keys().collect::<Vec<_>>())
            .field("body_fields", &self.body.keys().collect::<Vec<_>>())
            .field(
                "variant_names",
                &self.variant_bodies.keys().collect::<Vec<_>>(),
            )
            .field("context_window_tokens", &self.context_window_tokens)
            .field("max_output_tokens", &self.max_output_tokens)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct OpenAiCompatibleProvider {
    pub model: String,
    pub effort: Option<String>,
    pub system_prompt: &'static str,
}

#[async_trait]
impl Provider for OpenAiCompatibleProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn structured_call_with_progress(
        &self,
        prompt: &str,
        schema: &Value,
        _session_id: Option<&str>,
        progress: Option<UnboundedSender<ProviderProgress>>,
    ) -> std::result::Result<ProviderCallResult, ProviderCallError> {
        self.call(prompt, Some(schema), progress).await
    }

    async fn freeform_call_with_progress(
        &self,
        prompt: &str,
        _session_id: Option<&str>,
        progress: Option<UnboundedSender<ProviderProgress>>,
    ) -> std::result::Result<ProviderCallResult, ProviderCallError> {
        self.call(prompt, None, progress).await
    }

    fn label(&self) -> &'static str {
        "openai-compatible"
    }

    fn structured_output_dialect(&self) -> StructuredOutputDialect {
        StructuredOutputDialect::FlexibleJson
    }
}

impl OpenAiCompatibleProvider {
    /// Execute one provider-neutral model turn without executing tools.
    ///
    /// The Borg harness owns the conversation and tool loop. This adapter only
    /// translates a typed turn to an OpenAI-compatible chat-completions wire
    /// contract and returns the complete assistant message.
    pub async fn model_turn(
        &self,
        request: ModelTurnRequest,
        progress: Option<UnboundedSender<ProviderProgress>>,
    ) -> std::result::Result<ModelTurnResult, ProviderCallError> {
        self.model_turn_via(request, progress, None).await
    }

    pub async fn model_turn_via(
        &self,
        request: ModelTurnRequest,
        progress: Option<UnboundedSender<ProviderProgress>>,
        gateway: Option<&ModelGateway>,
    ) -> std::result::Result<ModelTurnResult, ProviderCallError> {
        self.model_turn_via_profile(request, progress, gateway, OpenAiCompatibleProfile::Generic)
            .await
    }

    pub async fn model_turn_via_profile(
        &self,
        request: ModelTurnRequest,
        progress: Option<UnboundedSender<ProviderProgress>>,
        gateway: Option<&ModelGateway>,
        profile: OpenAiCompatibleProfile,
    ) -> std::result::Result<ModelTurnResult, ProviderCallError> {
        let started_at = Instant::now();
        let endpoint = gateway
            .map(|gateway| gateway.endpoint.clone())
            .unwrap_or_else(|| profile.endpoint());
        let provider_label = gateway
            .and_then(|gateway| gateway.label.as_deref())
            .unwrap_or_else(|| profile.label());
        let request_model = gateway
            .and_then(|gateway| gateway.model.as_deref())
            .unwrap_or(&self.model);
        let mut trace = ProviderAttemptTrace {
            invocation: ProviderInvocation {
                provider_label: provider_label.to_string(),
                executable: endpoint.clone(),
                args: vec![request_model.to_string()],
                cwd: None,
                model: Some(request_model.to_string()),
                effort: self.effort.clone(),
            },
            exit_status: None,
            stdout: String::new(),
            stderr: String::new(),
        };
        let api_key = gateway
            .and_then(|gateway| {
                (!gateway.bearer_token.trim().is_empty()).then(|| gateway.bearer_token.clone())
            })
            .or_else(|| profile.api_key());
        if api_key.is_none() && profile != OpenAiCompatibleProfile::Generic {
            return Err(ProviderCallError {
                message: match profile {
                    OpenAiCompatibleProfile::Kimi => {
                        "BORG_KIMI_API_KEY or MOONSHOT_API_KEY is not set".to_string()
                    }
                    OpenAiCompatibleProfile::OpenRouter => {
                        "OPENROUTER_API_KEY is not set".to_string()
                    }
                    OpenAiCompatibleProfile::Generic => unreachable!(),
                },
                trace: Box::new(trace),
                session_id: None,
            });
        }
        let request_id = request.request_id.clone();
        let deepseek_model = request_model.to_ascii_lowercase().contains("deepseek");
        let wire_messages = request
            .messages
            .iter()
            .map(|message| model_message_wire_value(message, deepseek_model))
            .collect::<Vec<_>>();
        let mut body = json!({
            "model": request_model,
            "messages": wire_messages,
            "stream": true,
            "stream_options": { "include_usage": true },
        });
        let session_id = request
            .session_id
            .as_deref()
            .filter(|value| !value.trim().is_empty());
        let prompt_cache_key = request
            .prompt_cache_key
            .as_deref()
            .filter(|value| !value.trim().is_empty());
        if profile == OpenAiCompatibleProfile::OpenRouter {
            if let Some(session_id) = session_id {
                // Keep provider affinity stable across context-generation
                // changes. DeepSeek and Z.AI cache automatically; the stable
                // session lane is what lets OpenRouter keep routing the
                // evolving prefix to the same upstream cache.
                body["session_id"] = json!(session_id);
            } else if let Some(prompt_cache_key) = prompt_cache_key {
                // Preserve the pre-session_id request contract for callers
                // that only provide the older cache-key field.
                body["session_id"] = json!(prompt_cache_key);
            }
            if let Some(prompt_cache_key) = prompt_cache_key {
                body["prompt_cache_key"] = json!(prompt_cache_key);
            }
        }
        match profile {
            OpenAiCompatibleProfile::Kimi => {
                body["reasoning_effort"] = json!(kimi_reasoning_effort(self.effort.as_deref()));
                body["max_completion_tokens"] = json!(kimi_max_completion_tokens());
            }
            OpenAiCompatibleProfile::OpenRouter => {
                if let Some(reasoning) = compatible_reasoning(self.effort.as_deref()) {
                    body["reasoning"] = reasoning;
                }
                if let Some(max_tokens) = nonempty_env("BORG_OPENROUTER_MAX_COMPLETION_TOKENS")
                    .and_then(|value| value.parse::<u64>().ok())
                    .filter(|value| *value > 0)
                {
                    body["max_tokens"] = json!(max_tokens);
                }
            }
            OpenAiCompatibleProfile::Generic => {
                if let Some(max_tokens) = openai_compatible_max_tokens() {
                    body["max_tokens"] = json!(max_tokens);
                }
                if let Some(temperature) = openai_compatible_temperature() {
                    body["temperature"] = json!(temperature);
                }
                if let Some(extra) =
                    openai_compatible_extra_body().map_err(|message| ProviderCallError {
                        message,
                        trace: Box::new(trace.clone()),
                        session_id: None,
                    })?
                {
                    let body_object = body.as_object_mut().expect("request body is an object");
                    body_object.extend(extra);
                }
            }
        }
        if let Some(gateway) = gateway {
            merge_gateway_body(&mut body, &gateway.body);
            if let Some(effort) = self.effort.as_deref()
                && let Some(variant) = gateway.variant_bodies.get(effort)
            {
                merge_gateway_body(&mut body, variant);
            }
            if body.get("max_tokens").is_none()
                && let Some(max_output_tokens) = gateway.max_output_tokens
            {
                body["max_tokens"] = json!(max_output_tokens);
            }
        }
        if !request.tools.is_empty() {
            body["tools"] = Value::Array(
                request
                    .tools
                    .iter()
                    .map(|tool| tool.chat_completions_value())
                    .collect(),
            );
            body["tool_choice"] = json!("auto");
        }
        if let Some(schema) = request.output_schema.as_ref() {
            let format = match profile {
                OpenAiCompatibleProfile::Kimi => Some("json_schema".to_string()),
                OpenAiCompatibleProfile::OpenRouter => {
                    nonempty_env("BORG_OPENROUTER_RESPONSE_FORMAT")
                        .or_else(|| Some("json_schema".to_string()))
                }
                OpenAiCompatibleProfile::Generic => {
                    nonempty_env("BORG_OPENAI_COMPATIBLE_RESPONSE_FORMAT")
                }
            };
            match format.as_deref() {
                Some("none") => {}
                Some("json_object") => {
                    body["response_format"] = chat_completion_response_format(
                        schema,
                        ChatCompletionResponseFormat::JsonObject,
                    );
                }
                Some(_) => {
                    body["response_format"] = chat_completion_response_format(
                        schema,
                        ChatCompletionResponseFormat::JsonSchema,
                    );
                }
                None => {}
            }
        }
        if profile == OpenAiCompatibleProfile::OpenRouter
            && let Some(provider) = compatible_openrouter_provider_preferences(
                !request.tools.is_empty()
                    || body.get("reasoning").is_some()
                    || body.get("response_format").is_some(),
            )
        {
            body["provider"] = provider;
        }

        let client = compatible_http_client();
        let max_attempts = 3;
        let mut attempt = 0_u32;
        let response = loop {
            attempt += 1;
            let mut request = client.post(&endpoint).json(&body);
            if let Some(api_key) = api_key.as_deref() {
                request = request.bearer_auth(api_key);
            }
            if let Some(gateway) = gateway {
                for (name, value) in &gateway.headers {
                    request = request.header(name, value);
                }
            }
            if profile == OpenAiCompatibleProfile::OpenRouter {
                request = request
                    .header("HTTP-Referer", "https://borg.ml")
                    .header("X-Title", "Borg");
                if let Some(session_id) = session_id {
                    request = request.header("x-session-id", session_id);
                }
            }
            if let Some(request_id) = request_id.as_deref() {
                request = request.header("x-borg-request-id", request_id);
            }
            match apply_provider_request_timeout(request).send().await {
                Ok(response)
                    if attempt < max_attempts && compatible_retryable_status(response.status()) =>
                {
                    let delay = compatible_retry_delay(&response, attempt);
                    emit_compatible_retry_event(
                        progress.as_ref(),
                        profile,
                        &self.model,
                        CompatibleRetryAttempt {
                            attempt,
                            max_attempts,
                            delay,
                        },
                        "http_status",
                        Some(response.status().as_u16()),
                    );
                    tokio::time::sleep(delay).await;
                }
                Ok(response) => break response,
                Err(error) if attempt < max_attempts && error.is_connect() => {
                    let delay = compatible_retry_delay_without_response(attempt);
                    emit_compatible_retry_event(
                        progress.as_ref(),
                        profile,
                        &self.model,
                        CompatibleRetryAttempt {
                            attempt,
                            max_attempts,
                            delay,
                        },
                        "connect",
                        None,
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(error) => {
                    trace.exit_status = Some(1);
                    trace.stderr = error.to_string();
                    return Err(ProviderCallError {
                        message: format!("{provider_label} request failed: {error}"),
                        trace: Box::new(trace),
                        session_id: None,
                    });
                }
            }
        };
        trace.invocation.args.push(format!("attempts={attempt}"));
        let status = response.status();
        if !status.is_success() {
            let raw_text = read_provider_error_response_text(response)
                .await
                .unwrap_or_else(|error| error.to_string());
            trace.exit_status = Some(1);
            trace.stderr = raw_text.clone();
            return Err(ProviderCallError {
                message: format!(
                    "{provider_label} request failed with HTTP {}: {}",
                    status.as_u16(),
                    truncate_provider_text(&raw_text, 500)
                ),
                trace: Box::new(trace),
                session_id: None,
            });
        }

        let streamed = read_compatible_model_stream(
            response,
            progress.as_ref(),
            &self.model,
            self.effort.as_deref(),
        )
        .await
        .map_err(|error| ProviderCallError {
            message: format!("{provider_label} streaming response failed: {error}"),
            trace: Box::new(trace.clone()),
            session_id: None,
        })?;
        trace.stdout = streamed.raw.to_string();
        trace.exit_status = Some(0);
        let duration_ms = elapsed_millis_u64(started_at);
        let mut usage = match profile {
            OpenAiCompatibleProfile::Kimi => kimi_usage_from_response(&streamed.raw, duration_ms),
            OpenAiCompatibleProfile::OpenRouter => extract_chat_completions_usage(
                &streamed.raw,
                duration_ms,
                openrouter_cost_microusd(&streamed.raw),
            ),
            OpenAiCompatibleProfile::Generic => {
                extract_chat_completions_usage(&streamed.raw, duration_ms, None)
            }
        };
        if profile == OpenAiCompatibleProfile::Generic {
            // Local servers report no model metadata, so the context window has
            // to be declared. Without it `context_window_tokens` stays `None`
            // and auto-compaction never engages, which strands long local
            // sessions at the context wall instead of compacting them.
            apply_generic_context_window(&mut usage);
            if let Some(context_window_tokens) = gateway
                .and_then(|gateway| gateway.context_window_tokens)
                .filter(|tokens| *tokens > 0)
            {
                usage.context_tokens = Some(usage.context_tokens.unwrap_or_else(|| {
                    usage
                        .input_tokens
                        .saturating_add(usage.cached_input_tokens)
                        .saturating_add(usage.cache_creation_input_tokens)
                }));
                usage.context_window_tokens = Some(context_window_tokens);
            }
        }
        if profile == OpenAiCompatibleProfile::OpenRouter {
            usage.context_tokens = Some(
                usage
                    .input_tokens
                    .saturating_add(usage.cached_input_tokens)
                    .saturating_add(usage.cache_creation_input_tokens),
            );
            if let Some(limits) =
                openrouter_model_limits(client, &endpoint, api_key.as_deref(), &self.model).await
            {
                usage.context_window_tokens = Some(
                    limits
                        .context_window_tokens
                        .saturating_sub(limits.max_completion_tokens.unwrap_or(0)),
                );
            }
        }
        Ok(ModelTurnResult {
            message: streamed.message,
            finish_reason: streamed.finish_reason,
            usage,
            raw_response: streamed.raw,
            trace,
        })
    }

    async fn call(
        &self,
        prompt: &str,
        schema: Option<&Value>,
        progress: Option<UnboundedSender<ProviderProgress>>,
    ) -> std::result::Result<ProviderCallResult, ProviderCallError> {
        let started_at = Instant::now();
        let endpoint = chat_completions_endpoint();
        let mut trace = ProviderAttemptTrace {
            invocation: ProviderInvocation {
                provider_label: self.label().to_string(),
                executable: endpoint.clone(),
                args: vec![self.model.clone()],
                cwd: None,
                model: Some(self.model.clone()),
                effort: self.effort.clone(),
            },
            exit_status: None,
            stdout: String::new(),
            stderr: String::new(),
        };

        let api_key =
            nonempty_env("BORG_OPENAI_COMPATIBLE_API_KEY").unwrap_or_else(|| "local".to_string());

        let mut messages = Vec::new();
        if !self.system_prompt.trim().is_empty() {
            messages.push(json!({ "role": "system", "content": self.system_prompt }));
        }
        messages.push(json!({ "role": "user", "content": prompt }));

        let mut body = json!({
            "model": self.model,
            "messages": messages,
        });
        if let Some(max_tokens) = openai_compatible_max_tokens() {
            body["max_tokens"] = json!(max_tokens);
        }
        if let Some(temperature) = openai_compatible_temperature() {
            body["temperature"] = json!(temperature);
        }
        if let Some(extra_body) =
            openai_compatible_extra_body().map_err(|error| ProviderCallError {
                message: error,
                trace: Box::new(trace.clone()),
                session_id: None,
            })?
        {
            merge_object(&mut body, extra_body);
        }
        if let Some(schema) = schema
            && let Some(response_format) = openai_compatible_response_format(schema)
        {
            body["response_format"] = response_format;
        }

        let client = reqwest::Client::new();
        let request = client.post(&endpoint).bearer_auth(&api_key).json(&body);
        let response = apply_provider_request_timeout(request)
            .send()
            .await
            .map_err(|error| {
                trace.exit_status = Some(1);
                trace.stderr = error.to_string();
                ProviderCallError {
                    message: format!("OpenAI-compatible request failed: {error}"),
                    trace: Box::new(trace.clone()),
                    session_id: None,
                }
            })?;
        trace.invocation.args.push("attempts=1".to_string());

        let status = response.status();
        let raw_text = if status.is_success() {
            match read_provider_success_response_text(response).await {
                Ok(text) => text,
                Err(error) => {
                    trace.exit_status = Some(1);
                    trace.stderr = error.to_string();
                    return Err(ProviderCallError {
                        message: format!("OpenAI-compatible response read failed: {error}"),
                        trace: Box::new(trace),
                        session_id: None,
                    });
                }
            }
        } else {
            match read_provider_error_response_text(response).await {
                Ok(text) => text,
                Err(error) => {
                    trace.exit_status = Some(1);
                    trace.stderr = error.to_string();
                    return Err(ProviderCallError {
                        message: format!("OpenAI-compatible error response read failed: {error}"),
                        trace: Box::new(trace),
                        session_id: None,
                    });
                }
            }
        };
        trace.stdout = raw_text.clone();
        trace.exit_status = Some(if status.is_success() { 0 } else { 1 });
        if !status.is_success() {
            trace.stderr = raw_text.clone();
            return Err(ProviderCallError {
                message: format!(
                    "OpenAI-compatible request failed with HTTP {}: {}",
                    status.as_u16(),
                    truncate_provider_text(&raw_text, 500)
                ),
                trace: Box::new(trace),
                session_id: None,
            });
        }

        let raw: Value = match serde_json::from_str(&raw_text) {
            Ok(value) => value,
            Err(error) => {
                trace.stderr = error.to_string();
                return Err(ProviderCallError {
                    message: format!("OpenAI-compatible endpoint returned invalid JSON: {error}"),
                    trace: Box::new(trace),
                    session_id: None,
                });
            }
        };
        let text = raw
            .pointer("/choices/0/message/content")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        if !text.is_empty()
            && let Some(sender) = progress.as_ref()
        {
            let _ = sender.send(ProviderProgress::stdout(text.as_bytes().to_vec()));
        }

        let value = if schema.is_some() {
            parse_chat_completion_json_text(&text).unwrap_or_else(|| Value::String(text.clone()))
        } else {
            Value::String(text)
        };

        let mut usage = extract_chat_completions_usage(&raw, elapsed_millis_u64(started_at), None);
        apply_generic_context_window(&mut usage);
        Ok(ProviderCallResult {
            value,
            raw_response: raw.clone(),
            usage,
            trace,
            session_id: None,
        })
    }
}

const PROTECTED_GATEWAY_FIELDS: [&str; 8] = [
    "model",
    "messages",
    "stream",
    "stream_options",
    "tools",
    "tool_choice",
    "response_format",
    "provider",
];

fn merge_gateway_body(body: &mut Value, extras: &Map<String, Value>) {
    let Some(target) = body.as_object_mut() else {
        return;
    };
    for (key, value) in extras {
        if !PROTECTED_GATEWAY_FIELDS.contains(&key.as_str()) {
            target.insert(key.clone(), value.clone());
        }
    }
}

fn kimi_chat_completions_endpoint() -> String {
    let base = nonempty_env("BORG_KIMI_BASE_URL")
        .unwrap_or_else(|| "https://api.moonshot.ai/v1".to_string());
    let trimmed = base.trim_end_matches('/');
    if trimmed.ends_with("/chat/completions") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/chat/completions")
    }
}

fn kimi_reasoning_effort(effort: Option<&str>) -> &'static str {
    match effort.map(str::trim) {
        Some("low") => "low",
        Some("max") | Some("xhigh") | Some("ultra") => "max",
        _ => "high",
    }
}

fn kimi_max_completion_tokens() -> u64 {
    nonempty_env("BORG_KIMI_MAX_COMPLETION_TOKENS")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(32_768)
        .clamp(1, 1_048_576)
}

pub fn kimi_usage_from_response(
    raw: &Value,
    duration_ms: u64,
) -> crate::runtime::ProviderCallUsage {
    extract_chat_completions_usage(raw, duration_ms, Some(kimi_cost_microusd(raw)))
}

pub fn kimi_cost_microusd(raw: &Value) -> u64 {
    let input = raw
        .pointer("/usage/prompt_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cached = raw
        .pointer("/usage/prompt_tokens_details/cached_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0)
        .min(input);
    let output = raw
        .pointer("/usage/completion_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    input
        .saturating_sub(cached)
        .saturating_mul(3)
        .saturating_add(cached.saturating_mul(3).div_ceil(10))
        .saturating_add(output.saturating_mul(15))
}

/// Declared context window for a local OpenAI-compatible server, set from the
/// `[local]` agent-config block or exported directly. Local servers do not
/// advertise this, so it cannot be probed the way OpenRouter's is.
fn generic_context_window_tokens() -> Option<u64> {
    nonempty_env("BORG_OPENAI_COMPATIBLE_CONTEXT_WINDOW_TOKENS")
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|tokens| *tokens > 0)
}

fn apply_generic_context_window(usage: &mut crate::runtime::ProviderCallUsage) {
    // Local servers report no model metadata, so the context window must be
    // declared by Borg or by the server lifecycle. Without it auto-compaction
    // never engages at the context wall.
    if let Some(context_window_tokens) = generic_context_window_tokens() {
        usage.context_tokens = Some(
            usage
                .input_tokens
                .saturating_add(usage.cached_input_tokens)
                .saturating_add(usage.cache_creation_input_tokens),
        );
        usage.context_window_tokens = Some(context_window_tokens);
    }
}

async fn openrouter_model_limits(
    client: &reqwest::Client,
    chat_endpoint: &str,
    api_key: Option<&str>,
    model: &str,
) -> Option<OpenRouterModelLimits> {
    if let Some(context_window_tokens) = nonempty_env("BORG_OPENROUTER_CONTEXT_WINDOW_TOKENS")
        .and_then(|value| value.parse::<u64>().ok())
    {
        return Some(OpenRouterModelLimits {
            context_window_tokens,
            max_completion_tokens: nonempty_env("BORG_OPENROUTER_MAX_COMPLETION_TOKENS")
                .and_then(|value| value.parse::<u64>().ok()),
        });
    }
    let key = format!("{chat_endpoint}\n{model}");
    if let Some(cached) = OPENROUTER_MODEL_LIMITS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .ok()
        .and_then(|cache| cache.get(&key).copied())
    {
        return Some(cached);
    }
    let base = chat_endpoint.strip_suffix("/chat/completions")?;
    let mut request = client.get(format!("{base}/model/{model}"));
    if let Some(api_key) = api_key {
        request = request.bearer_auth(api_key);
    }
    let limits = match apply_provider_request_timeout(request).send().await {
        Ok(response) if response.status().is_success() => response
            .json::<Value>()
            .await
            .ok()
            .and_then(|value| openrouter_model_limits_from_response(&value)),
        _ => None,
    };
    if let Some(limits) = limits
        && let Ok(mut cache) = OPENROUTER_MODEL_LIMITS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
    {
        cache.insert(key, limits);
    }
    limits
}

fn openrouter_model_limits_from_response(raw: &Value) -> Option<OpenRouterModelLimits> {
    Some(OpenRouterModelLimits {
        context_window_tokens: raw.pointer("/data/context_length")?.as_u64()?,
        max_completion_tokens: raw
            .pointer("/data/top_provider/max_completion_tokens")
            .and_then(Value::as_u64),
    })
}

fn compatible_http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

fn model_message_wire_value(message: &ModelMessage, deepseek_model: bool) -> Value {
    let mut wire = match message {
        ModelMessage::User {
            content,
            attachments,
        } if !attachments.is_empty() => {
            let mut blocks = vec![json!({ "type": "text", "text": content })];
            blocks.extend(attachments.iter().map(|attachment| {
                json!({
                    "type": "image_url",
                    "image_url": {
                        "url": format!(
                            "data:{};base64,{}",
                            attachment.media_type, attachment.data_base64
                        )
                    }
                })
            }));
            json!({ "role": "user", "content": blocks })
        }
        _ => serde_json::to_value(message).expect("model messages are serializable"),
    };

    // DeepSeek requires every replayed assistant message to carry a reasoning
    // part, including tool-call messages with no visible reasoning. Keep the
    // empty field on the wire so the provider sees the same message shape on
    // every round and can extend the exact cached prefix.
    if deepseek_model
        && matches!(
            message,
            ModelMessage::Assistant {
                reasoning_content: None,
                reasoning_details: None,
                ..
            }
        )
        && let Some(object) = wire.as_object_mut()
    {
        object.insert(
            "reasoning_content".to_string(),
            Value::String(String::new()),
        );
    }

    wire
}

fn chat_completions_endpoint() -> String {
    let base = nonempty_env("BORG_OPENAI_COMPATIBLE_BASE_URL")
        .unwrap_or_else(|| "http://127.0.0.1:8000/v1".to_string());
    let trimmed = base.trim_end_matches('/');
    if trimmed.ends_with("/chat/completions") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/chat/completions")
    }
}

fn openrouter_chat_completions_endpoint() -> String {
    let base = nonempty_env("BORG_OPENROUTER_BASE_URL")
        .unwrap_or_else(|| "https://openrouter.ai/api/v1".to_string());
    let trimmed = base.trim_end_matches('/');
    if trimmed.ends_with("/chat/completions") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/chat/completions")
    }
}

struct CompatibleModelStream {
    message: ModelMessage,
    finish_reason: String,
    raw: Value,
}

#[derive(Default)]
struct PartialToolCall {
    id: String,
    name: String,
    arguments: String,
}

fn emit_compatible_reasoning_progress(
    progress: Option<&UnboundedSender<ProviderProgress>>,
    model: &str,
    effort: Option<&str>,
    text: &str,
) {
    if text.is_empty() {
        return;
    }
    if let Some(sender) = progress {
        let _ = sender.send(ProviderProgress::ProviderEvent {
            kind: "reasoning_delta".to_string(),
            payload: json!({ "text": text }),
            raw_payload: Box::new(None),
            stream_channel: Some("reasoning".to_string()),
            content_text: Some(text.to_string()),
            provider_item_id: None,
            tool_use_id: None,
            tool_name: None,
            model: Some(model.to_string()),
            effort: effort.map(str::to_string),
        });
    }
}

fn reasoning_detail_text(detail: &Value) -> Option<String> {
    match detail {
        Value::String(text) => (!text.is_empty()).then(|| text.clone()),
        Value::Array(items) => {
            let text = items
                .iter()
                .filter_map(reasoning_detail_text)
                .collect::<Vec<_>>()
                .join("\n");
            (!text.is_empty()).then_some(text)
        }
        Value::Object(object) => ["text", "summary", "content", "reasoning"]
            .iter()
            .find_map(|field| object.get(*field).and_then(reasoning_detail_text)),
        _ => None,
    }
}

async fn read_compatible_model_stream(
    response: reqwest::Response,
    progress: Option<&UnboundedSender<ProviderProgress>>,
    model: &str,
    effort: Option<&str>,
) -> Result<CompatibleModelStream, String> {
    let mut stream = response.bytes_stream();
    let mut pending = Vec::new();
    let mut total_bytes = 0_usize;
    let mut content = String::new();
    let mut reasoning_content = String::new();
    let mut reasoning_details = Vec::new();
    let mut tool_calls = BTreeMap::<usize, PartialToolCall>::new();
    let mut generating_tool_calls = HashSet::new();
    let mut started_tool_calls = HashSet::new();
    let mut described_tool_calls = HashSet::new();
    let mut finish_reason = None;
    let mut usage = None;
    let mut saw_done = false;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| error.to_string())?;
        total_bytes = total_bytes.saturating_add(chunk.len());
        if total_bytes > COMPATIBLE_STREAM_MAX_BYTES {
            return Err(format!(
                "stream exceeded the {COMPATIBLE_STREAM_MAX_BYTES} byte response limit"
            ));
        }
        pending.extend_from_slice(&chunk);
        let mut consumed = 0_usize;
        while let Some(newline) = pending[consumed..].iter().position(|byte| *byte == b'\n') {
            let line_start = consumed;
            let line_end = line_start + newline;
            consumed = line_end + 1;
            let mut line = &pending[line_start..line_end];
            if line.last() == Some(&b'\r') {
                line = &line[..line.len() - 1];
            }
            let line = std::str::from_utf8(line)
                .map_err(|error| format!("stream contained invalid UTF-8: {error}"))?;
            let Some(data) = line.strip_prefix("data:") else {
                continue;
            };
            let data = data.trim();
            if data == "[DONE]" {
                saw_done = true;
                break;
            }
            if data.is_empty() {
                continue;
            }
            let chunk: Value = serde_json::from_str(data)
                .map_err(|error| format!("invalid SSE JSON chunk: {error}"))?;
            if let Some(delta) = chunk
                .pointer("/choices/0/delta/content")
                .and_then(Value::as_str)
            {
                content.push_str(delta);
                if let Some(sender) = progress {
                    let _ = sender.send(ProviderProgress::stdout(delta.as_bytes().to_vec()));
                }
            }
            let mut emitted_reasoning_delta = false;
            if let Some(delta) = chunk
                .pointer("/choices/0/delta/reasoning_content")
                .or_else(|| chunk.pointer("/choices/0/delta/reasoning"))
                .and_then(Value::as_str)
            {
                reasoning_content.push_str(delta);
                emit_compatible_reasoning_progress(progress, model, effort, delta);
                emitted_reasoning_delta = !delta.is_empty();
            }
            if let Some(details) = chunk
                .pointer("/choices/0/delta/reasoning_details")
                .and_then(Value::as_array)
            {
                reasoning_details.extend(details.iter().cloned());
                // OpenRouter/GPT-compatible providers may put the visible
                // thinking summary only in reasoning_details. Preserve the
                // details for replay and also surface their text live; when a
                // direct reasoning field is present it carries the same text.
                if !emitted_reasoning_delta {
                    let detail_text = details
                        .iter()
                        .filter_map(reasoning_detail_text)
                        .collect::<Vec<_>>()
                        .join("\n");
                    reasoning_content.push_str(&detail_text);
                    emit_compatible_reasoning_progress(progress, model, effort, &detail_text);
                }
            }
            if let Some(deltas) = chunk
                .pointer("/choices/0/delta/tool_calls")
                .and_then(Value::as_array)
            {
                for delta in deltas {
                    let index = delta
                        .get("index")
                        .and_then(Value::as_u64)
                        .and_then(|index| usize::try_from(index).ok())
                        .ok_or_else(|| "tool-call delta is missing a valid index".to_string())?;
                    let call = tool_calls.entry(index).or_default();
                    if let Some(id) = delta.get("id").and_then(Value::as_str)
                        && !id.is_empty()
                    {
                        call.id = id.to_string();
                    }
                    if let Some(name) = delta.pointer("/function/name").and_then(Value::as_str) {
                        call.name.push_str(name);
                    }
                    if let Some(arguments) =
                        delta.pointer("/function/arguments").and_then(Value::as_str)
                    {
                        call.arguments.push_str(arguments);
                    }
                    if generating_tool_calls.insert(index)
                        && let Some(sender) = progress
                    {
                        let _ = sender.send(ProviderProgress::ToolCallGenerating {
                            id: (!call.id.is_empty()).then(|| call.id.clone()),
                        });
                    }
                    if !call.id.is_empty()
                        && !call.name.is_empty()
                        && started_tool_calls.insert(index)
                        && let Some(sender) = progress
                    {
                        let _ = sender.send(ProviderProgress::ToolCallStarted {
                            id: call.id.clone(),
                            name: call.name.clone(),
                            input: Value::Null,
                        });
                    }
                    if !call.id.is_empty()
                        && !described_tool_calls.contains(&index)
                        && let Some(action) = streamed_tool_action(&call.arguments)
                    {
                        described_tool_calls.insert(index);
                        if let Some(sender) = progress {
                            let _ = sender.send(ProviderProgress::ToolCallAction {
                                id: call.id.clone(),
                                action,
                            });
                        }
                    }
                }
            }
            if let Some(reason) = chunk
                .pointer("/choices/0/finish_reason")
                .and_then(Value::as_str)
            {
                finish_reason = Some(reason.to_string());
            }
            if let Some(chunk_usage) = chunk
                .get("usage")
                .or_else(|| chunk.pointer("/choices/0/usage"))
            {
                usage = Some(chunk_usage.clone());
            }
        }
        if consumed == pending.len() {
            pending.clear();
        } else if consumed > 0 {
            pending.drain(..consumed);
        }
        if saw_done {
            break;
        }
    }

    if !saw_done {
        return Err("stream ended before the required data: [DONE] marker".to_string());
    }
    let finish_reason =
        finish_reason.ok_or_else(|| "stream ended without a finish_reason".to_string())?;
    let tool_calls = tool_calls
        .into_values()
        .map(|call| {
            if call.id.is_empty() {
                return Err("completed tool call is missing its id".to_string());
            }
            if call.name.is_empty() {
                return Err(format!(
                    "tool call {} is missing its function name",
                    call.id
                ));
            }
            Ok(ModelToolCall::function(call.id, call.name, call.arguments))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let mut tool_call_ids = HashSet::with_capacity(tool_calls.len());
    for tool_call in &tool_calls {
        if !tool_call_ids.insert(tool_call.id.as_str()) {
            return Err(format!(
                "stream returned duplicate tool call id `{}`",
                tool_call.id
            ));
        }
    }
    let message = ModelMessage::assistant(
        (!content.is_empty()).then_some(content),
        (!reasoning_content.is_empty()).then_some(reasoning_content),
        (!reasoning_details.is_empty()).then_some(Value::Array(reasoning_details)),
        tool_calls,
    );
    let raw = json!({
        "choices": [{
            "message": message,
            "finish_reason": finish_reason,
        }],
        "usage": usage.unwrap_or_else(|| json!({})),
    });
    Ok(CompatibleModelStream {
        message,
        finish_reason,
        raw,
    })
}

fn streamed_tool_action(arguments: &str) -> Option<String> {
    let mut remaining = arguments.trim_start().strip_prefix('{')?.trim_start();
    let (field, rest) = parse_complete_json_string(remaining)?;
    if field != "action" {
        return None;
    }
    remaining = rest.trim_start().strip_prefix(':')?.trim_start();
    let (action, _) = parse_complete_json_string(remaining)?;
    let action = action.trim();
    (!action.is_empty() && action.chars().count() <= 64).then(|| action.to_string())
}

fn parse_complete_json_string(input: &str) -> Option<(String, &str)> {
    if !input.starts_with('"') {
        return None;
    }
    let mut escaped = false;
    for (offset, character) in input[1..].char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match character {
            '\\' => escaped = true,
            '"' => {
                let end = offset + 2;
                let value = serde_json::from_str::<String>(&input[..end]).ok()?;
                return Some((value, &input[end..]));
            }
            _ => {}
        }
    }
    None
}

fn compatible_retryable_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

#[derive(Clone, Copy)]
struct CompatibleRetryAttempt {
    attempt: u32,
    max_attempts: u32,
    delay: Duration,
}

fn emit_compatible_retry_event(
    progress: Option<&UnboundedSender<ProviderProgress>>,
    profile: OpenAiCompatibleProfile,
    model: &str,
    retry: CompatibleRetryAttempt,
    reason: &str,
    status: Option<u16>,
) {
    let Some(sender) = progress else {
        return;
    };
    let _ = sender.send(ProviderProgress::ProviderEvent {
        kind: "provider_retry".to_string(),
        payload: json!({
            "provider": profile.label(),
            "reason": reason,
            "status": status,
            "attempt": retry.attempt,
            "max_attempts": retry.max_attempts,
            "delay_ms": retry.delay.as_millis().min(u128::from(u64::MAX)) as u64,
        }),
        raw_payload: Box::new(None),
        stream_channel: None,
        content_text: None,
        provider_item_id: None,
        tool_use_id: None,
        tool_name: None,
        model: Some(model.to_string()),
        effort: None,
    });
}

fn compatible_reasoning(effort: Option<&str>) -> Option<Value> {
    match effort.map(str::trim) {
        Some("low") => Some(json!({ "effort": "low" })),
        Some("medium") => Some(json!({ "effort": "medium" })),
        Some("high") => Some(json!({ "effort": "high" })),
        Some("xhigh") | Some("max") | Some("ultra") => Some(json!({ "effort": "max" })),
        _ => None,
    }
}

fn compatible_openrouter_provider_preferences(require_parameters: bool) -> Option<Value> {
    let order = nonempty_env("BORG_OPENROUTER_PROVIDER_ORDER")
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let allow_fallbacks = nonempty_env("BORG_OPENROUTER_ALLOW_FALLBACKS")
        .and_then(|value| value.parse::<bool>().ok())
        .unwrap_or(true);
    (require_parameters || !order.is_empty()).then(|| {
        let mut provider = json!({
            "allow_fallbacks": allow_fallbacks,
            "require_parameters": require_parameters,
        });
        if !order.is_empty() {
            provider["order"] = json!(order);
        }
        provider
    })
}

fn openrouter_cost_microusd(raw: &Value) -> Option<u64> {
    raw.pointer("/usage/cost")
        .and_then(Value::as_f64)
        .and_then(provider_cost_usd_to_microusd)
}

fn compatible_retry_delay(response: &reqwest::Response, attempt: u32) -> std::time::Duration {
    response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|seconds| std::time::Duration::from_secs(seconds.min(30)))
        .unwrap_or_else(|| compatible_retry_delay_without_response(attempt))
}

fn compatible_retry_delay_without_response(attempt: u32) -> std::time::Duration {
    std::time::Duration::from_millis(500_u64.saturating_mul(1_u64 << attempt.saturating_sub(1)))
}

fn openai_compatible_max_tokens() -> Option<u64> {
    nonempty_env("BORG_OPENAI_COMPATIBLE_MAX_TOKENS").and_then(|value| value.parse().ok())
}

fn openai_compatible_temperature() -> Option<f64> {
    nonempty_env("BORG_OPENAI_COMPATIBLE_TEMPERATURE").and_then(|value| value.parse().ok())
}

fn openai_compatible_extra_body() -> Result<Option<Map<String, Value>>, String> {
    let Some(raw) = nonempty_env("BORG_OPENAI_COMPATIBLE_EXTRA_BODY_JSON") else {
        return Ok(None);
    };
    let value: Value = serde_json::from_str(&raw).map_err(|error| {
        format!("BORG_OPENAI_COMPATIBLE_EXTRA_BODY_JSON is invalid JSON: {error}")
    })?;
    match value {
        Value::Object(object) => Ok(Some(object)),
        _ => Err("BORG_OPENAI_COMPATIBLE_EXTRA_BODY_JSON must be a JSON object".to_string()),
    }
}

fn openai_compatible_response_format(schema: &Value) -> Option<Value> {
    match nonempty_env("BORG_OPENAI_COMPATIBLE_RESPONSE_FORMAT")
        .as_deref()
        .map(str::to_lowercase)
        .as_deref()
    {
        Some("json_object") => Some(chat_completion_response_format(
            schema,
            ChatCompletionResponseFormat::JsonObject,
        )),
        Some("json_schema") => Some(chat_completion_response_format(
            schema,
            ChatCompletionResponseFormat::JsonSchema,
        )),
        _ => None,
    }
}

fn merge_object(target: &mut Value, extra: Map<String, Value>) {
    let Some(target_object) = target.as_object_mut() else {
        return;
    };
    for (key, value) in extra {
        target_object.insert(key, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write as _;

    #[test]
    fn native_images_use_chat_completions_multimodal_blocks() {
        let message = ModelMessage::user_with_attachments(
            "inspect",
            vec![super::super::ModelInputAttachment {
                media_type: "image/png".to_string(),
                data_base64: "aW1hZ2U=".to_string(),
                filename: Some("screen.png".to_string()),
            }],
        );
        let wire = model_message_wire_value(&message, false);
        assert_eq!(wire["content"][0]["type"], "text");
        assert_eq!(wire["content"][1]["type"], "image_url");
        assert_eq!(
            wire["content"][1]["image_url"]["url"],
            "data:image/png;base64,aW1hZ2U="
        );
    }

    #[test]
    fn deepseek_replays_empty_reasoning_for_assistant_tool_rounds() {
        let message = ModelMessage::assistant(
            None,
            None,
            None,
            vec![ModelToolCall::function(
                "call-1".to_string(),
                "read_file".to_string(),
                "{}".to_string(),
            )],
        );

        let deepseek_wire = model_message_wire_value(&message, true);
        assert_eq!(deepseek_wire["reasoning_content"], "");

        let generic_wire = model_message_wire_value(&message, false);
        assert!(generic_wire.get("reasoning_content").is_none());
    }

    #[test]
    fn configured_gateway_body_cannot_replace_core_request_fields() {
        let mut body = json!({
            "model": "qualified/model",
            "messages": [],
            "stream": true,
            "temperature": 0.2
        });
        merge_gateway_body(
            &mut body,
            serde_json::json!({
                "model": "attacker/model",
                "messages": ["replace"],
                "stream": false,
                "temperature": 0.7,
                "reasoning_effort": "high"
            })
            .as_object()
            .expect("object"),
        );
        assert_eq!(body["model"], "qualified/model");
        assert_eq!(body["messages"], json!([]));
        assert_eq!(body["stream"], true);
        assert_eq!(body["temperature"], 0.7);
        assert_eq!(body["reasoning_effort"], "high");
    }

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    static OPENROUTER_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    struct TestEnvGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl TestEnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var(key).ok();
            // SAFETY: tests that mutate these OpenRouter variables serialize
            // through OPENROUTER_ENV_LOCK and restore them on drop.
            unsafe { std::env::set_var(key, value) };
            Self { key, previous }
        }
    }

    impl Drop for TestEnvGuard {
        fn drop(&mut self) {
            // SAFETY: see TestEnvGuard::set.
            unsafe {
                match self.previous.as_deref() {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    #[test]
    fn openrouter_is_model_neutral_and_only_requests_reasoning_explicitly() {
        assert_eq!(crate::openrouter_product_model(), "openrouter/auto");
        assert_eq!(compatible_reasoning(None), None);
        assert_eq!(
            compatible_reasoning(Some("low")),
            Some(json!({ "effort": "low" }))
        );
        assert_eq!(
            compatible_reasoning(Some("high")),
            Some(json!({ "effort": "high" }))
        );
        assert_eq!(
            compatible_reasoning(Some("ultra")),
            Some(json!({ "effort": "max" }))
        );
    }

    #[test]
    fn openrouter_requires_endpoint_support_for_agent_parameters() {
        let preferences =
            compatible_openrouter_provider_preferences(true).expect("routing preferences");
        assert_eq!(preferences["require_parameters"], true);
    }

    #[test]
    fn compatible_retries_only_rate_limits_and_server_failures() {
        assert!(compatible_retryable_status(
            reqwest::StatusCode::TOO_MANY_REQUESTS
        ));
        assert!(compatible_retryable_status(
            reqwest::StatusCode::SERVICE_UNAVAILABLE
        ));
        assert!(!compatible_retryable_status(
            reqwest::StatusCode::BAD_REQUEST
        ));
    }

    #[test]
    fn compatible_retry_event_carries_structured_attempt_and_backoff() {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

        emit_compatible_retry_event(
            Some(&sender),
            OpenAiCompatibleProfile::Generic,
            "local-model",
            CompatibleRetryAttempt {
                attempt: 1,
                max_attempts: 3,
                delay: Duration::from_millis(750),
            },
            "http_status",
            Some(429),
        );

        let ProviderProgress::ProviderEvent { kind, payload, .. } =
            receiver.try_recv().expect("retry event")
        else {
            panic!("expected provider event");
        };
        assert_eq!(kind, "provider_retry");
        assert_eq!(payload["provider"], "openai-compatible");
        assert_eq!(payload["status"], 429);
        assert_eq!(payload["attempt"], 1);
        assert_eq!(payload["max_attempts"], 3);
        assert_eq!(payload["delay_ms"], 750);
    }

    #[test]
    fn kimi_cost_accounts_for_cached_input_at_provider_list_price() {
        let raw = json!({
            "usage": {
                "prompt_tokens": 1_000_000,
                "prompt_tokens_details": { "cached_tokens": 200_000 },
                "completion_tokens": 100_000
            }
        });
        assert_eq!(kimi_cost_microusd(&raw), 3_960_000);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn kimi_profile_uses_the_canonical_native_wire_contract() {
        let _lock = OPENROUTER_ENV_LOCK.lock().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Kimi test server");
        let address = listener.local_addr().expect("test server address");
        let _base = TestEnvGuard::set("BORG_KIMI_BASE_URL", &format!("http://{address}/v1"));
        let _key = TestEnvGuard::set("BORG_KIMI_API_KEY", "test-kimi-key");
        let _max = TestEnvGuard::set("BORG_KIMI_MAX_COMPLETION_TOKENS", "1234");

        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        let response_body = [
            r#"data: {"choices":[{"delta":{"reasoning_content":"inspect "},"finish_reason":null}]}"#,
            r#"data: {"choices":[{"delta":{"content":"done"},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":3}}"#,
            "data: [DONE]",
            "",
        ]
        .join("\n");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = Vec::new();
            let expected_len = loop {
                let mut chunk = [0_u8; 8192];
                let read = socket.read(&mut chunk).await.expect("read request");
                assert!(read > 0, "request closed before headers");
                request.extend_from_slice(&chunk[..read]);
                let Some(header_end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&request[..header_end]);
                let content_len = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(str::trim)
                            .and_then(|value| value.parse::<usize>().ok())
                    })
                    .expect("content-length header");
                break header_end + 4 + content_len;
            };
            while request.len() < expected_len {
                let mut chunk = [0_u8; 8192];
                let read = socket.read(&mut chunk).await.expect("read request body");
                assert!(read > 0, "request closed before body");
                request.extend_from_slice(&chunk[..read]);
            }
            let header_end = request
                .windows(4)
                .position(|bytes| bytes == b"\r\n\r\n")
                .expect("request headers");
            let body: Value = serde_json::from_slice(&request[header_end + 4..expected_len])
                .expect("Kimi JSON request");
            request_tx.send(body).expect("return captured Kimi request");
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                        response_body.len(), response_body
                    )
                    .as_bytes(),
                )
                .await
                .expect("write Kimi response");
        });

        let provider = OpenAiCompatibleProvider {
            model: "kimi-k3".to_string(),
            effort: Some("max".to_string()),
            system_prompt: "",
        };
        let result = provider
            .model_turn_via_profile(
                ModelTurnRequest {
                    request_id: Some("kimi-test".to_string()),
                    session_id: None,
                    prompt_cache_key: None,
                    messages: vec![ModelMessage::user("inspect")],
                    tools: Vec::new(),
                    output_schema: Some(json!({
                        "type": "object",
                        "properties": { "ok": { "type": "boolean" } }
                    })),
                },
                None,
                None,
                OpenAiCompatibleProfile::Kimi,
            )
            .await
            .expect("Kimi native turn");
        let body = request_rx.await.expect("Kimi request body");
        server.await.expect("Kimi test server task");

        assert_eq!(body["model"], "kimi-k3");
        assert_eq!(body["reasoning_effort"], "max");
        assert_eq!(body["max_completion_tokens"], 1234);
        assert_eq!(body["response_format"]["type"], "json_schema");
        assert_eq!(result.finish_reason, "stop");
        assert_eq!(result.usage.input_tokens, 12);
        assert_eq!(result.usage.output_tokens, 3);
    }

    #[tokio::test]
    async fn compatible_stream_preserves_reasoning_and_incremental_tool_calls() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let body = [
            r#"data: {"choices":[{"delta":{"reasoning_details":[{"type":"reasoning.text","text":"inspect " }]},"finish_reason":null}]}"#,
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"action\":\"ed"}}]},"finish_reason":null}]}"#,
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"read_file","arguments":"it\",\"path\":\"src/lib.rs\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":5}}"#,
            "data: [DONE]",
            "",
        ]
        .join("\n");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 2048];
            let _ = socket.read(&mut request).await.expect("read request");
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                    .as_bytes(),
                )
                .await
                .expect("write response");
        });
        let response = reqwest::get(format!("http://{address}"))
            .await
            .expect("request test stream");
        let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();
        let streamed =
            read_compatible_model_stream(response, Some(&progress_tx), "local-model", Some("high"))
                .await
                .expect("parse stream");
        server.await.expect("test server task");

        let ProviderProgress::ProviderEvent {
            kind, content_text, ..
        } = progress_rx.try_recv().expect("reasoning progress event")
        else {
            panic!("expected reasoning progress event");
        };
        assert_eq!(kind, "reasoning_delta");
        assert_eq!(content_text.as_deref(), Some("inspect "));
        let ProviderProgress::ToolCallGenerating { id } = progress_rx
            .try_recv()
            .expect("tool generation progress event")
        else {
            panic!("expected tool generation progress event");
        };
        assert_eq!(id, None);
        let ProviderProgress::ToolCallStarted { id, name, input } =
            progress_rx.try_recv().expect("tool-call progress event")
        else {
            panic!("expected tool-call progress event");
        };
        assert_eq!(id, "call-1");
        assert_eq!(name, "read_file");
        assert_eq!(input, Value::Null);
        let ProviderProgress::ToolCallAction { id, action } =
            progress_rx.try_recv().expect("tool action progress event")
        else {
            panic!("expected tool action progress event");
        };
        assert_eq!(id, "call-1");
        assert_eq!(action, "edit");
        assert_eq!(streamed.finish_reason, "tool_calls");
        let ModelMessage::Assistant {
            reasoning_content,
            reasoning_details,
            tool_calls,
            ..
        } = streamed.message
        else {
            panic!("expected assistant message")
        };
        assert_eq!(reasoning_content.as_deref(), Some("inspect "));
        assert_eq!(
            reasoning_details,
            Some(json!([{
                "type": "reasoning.text",
                "text": "inspect "
            }]))
        );
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].id, "call-1");
        assert_eq!(tool_calls[0].function.name, "read_file");
        assert_eq!(
            tool_calls[0].function.arguments,
            r#"{"action":"edit","path":"src/lib.rs"}"#
        );
        assert_eq!(streamed.raw["usage"]["prompt_tokens"], 10);
    }

    #[test]
    fn streamed_action_must_be_the_first_complete_argument_field() {
        assert_eq!(
            streamed_tool_action(r#"{"action":"delete files","#).as_deref(),
            Some("delete files")
        );
        assert_eq!(streamed_tool_action(r#"{"action":"edi"#), None);
        assert_eq!(
            streamed_tool_action(r#"{"cmd":"pwd","action":"inspect"}"#),
            None
        );
    }

    #[tokio::test]
    #[ignore = "explicit compatible SSE framing performance gate"]
    async fn compatible_sse_framing_profile() {
        const DELTAS: usize = 50_000;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let mut body = String::with_capacity(DELTAS * 72);
        for _ in 0..DELTAS {
            body.push_str(
                "data: {\"choices\":[{\"delta\":{\"content\":\"x\"},\"finish_reason\":null}]}\n",
            );
        }
        writeln!(
            body,
            "data: {{\"choices\":[{{\"delta\":{{}},\"finish_reason\":\"stop\"}}]}}"
        )
        .expect("write final delta");
        body.push_str("data: [DONE]\n");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 2048];
            let _ = socket.read(&mut request).await.expect("read request");
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                        body.len(), body
                    )
                    .as_bytes(),
                )
                .await
                .expect("write response");
        });
        let response = reqwest::get(format!("http://{address}"))
            .await
            .expect("request test stream");

        let started = Instant::now();
        let streamed = read_compatible_model_stream(response, None, "local-model", None)
            .await
            .expect("parse stream");
        let elapsed = started.elapsed();
        eprintln!("50k compatible SSE deltas: {elapsed:?}");

        server.await.expect("test server task");
        let ModelMessage::Assistant { content, .. } = streamed.message else {
            panic!("expected assistant response");
        };
        assert_eq!(content.expect("stream content").len(), DELTAS);
        assert!(
            elapsed < Duration::from_millis(120),
            "compatible SSE parsing exceeded 120 ms: {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn openrouter_arbitrary_model_runs_the_complete_native_wire_contract() {
        let _lock = OPENROUTER_ENV_LOCK.lock().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind OpenRouter test server");
        let address = listener.local_addr().expect("test server address");
        let base_url = format!("http://{address}/api/v1");
        let _base = TestEnvGuard::set("BORG_OPENROUTER_BASE_URL", &base_url);
        let _key = TestEnvGuard::set("OPENROUTER_API_KEY", "test-openrouter-key");
        let _max = TestEnvGuard::set("BORG_OPENROUTER_MAX_COMPLETION_TOKENS", "24000");

        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        let response_body = [
            r#"data: {"model":"vendor/future-model","choices":[{"delta":{"reasoning":"inspect ","reasoning_details":[{"type":"reasoning.text","text":"inspect "}]},"finish_reason":null}]}"#,
            r#"data: {"choices":[{"delta":{"content":"{\"ok\":true}","tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"read_file","arguments":"{\"path\":\"README.md\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":21,"completion_tokens":8,"total_tokens":29,"cost":0.000123}}"#,
            "data: [DONE]",
            "",
        ]
        .join("\n");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = Vec::new();
            let expected_len = loop {
                let mut chunk = [0_u8; 8192];
                let read = socket.read(&mut chunk).await.expect("read request");
                assert!(read > 0, "request closed before headers");
                request.extend_from_slice(&chunk[..read]);
                let Some(header_end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                else {
                    continue;
                };
                let headers = String::from_utf8_lossy(&request[..header_end]);
                let content_len = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(str::trim)
                            .and_then(|value| value.parse::<usize>().ok())
                    })
                    .expect("content-length header");
                break header_end + 4 + content_len;
            };
            while request.len() < expected_len {
                let mut chunk = [0_u8; 8192];
                let read = socket.read(&mut chunk).await.expect("read request body");
                assert!(read > 0, "request closed before body");
                request.extend_from_slice(&chunk[..read]);
            }
            request_tx
                .send(request)
                .expect("return captured OpenRouter request");
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                        response_body.len(),
                        response_body
                    )
                    .as_bytes(),
                )
                .await
                .expect("write OpenRouter response");
        });

        let provider = OpenAiCompatibleProvider {
            model: "vendor/future-model".to_string(),
            effort: Some("high".to_string()),
            system_prompt: "",
        };
        let result = provider
            .model_turn_via_profile(
                ModelTurnRequest {
                    request_id: Some("openrouter-test".to_string()),
                    session_id: Some("borg-session:stable".to_string()),
                    prompt_cache_key: Some("borg-prefix:test".to_string()),
                    messages: vec![ModelMessage::user("inspect the repository")],
                    tools: vec![
                        super::super::ModelToolDefinition::new(
                            "read_file",
                            "Read a file.",
                            json!({
                                "type": "object",
                                "properties": {"path": {"type": "string"}},
                                "required": ["path"]
                            }),
                        )
                        .unwrap(),
                    ],
                    output_schema: Some(json!({
                        "type": "object",
                        "properties": {"ok": {"type": "boolean"}},
                        "required": ["ok"]
                    })),
                },
                None,
                None,
                OpenAiCompatibleProfile::OpenRouter,
            )
            .await
            .expect("OpenRouter native turn");
        server.await.expect("OpenRouter test server task");

        let request = request_rx.await.expect("captured request");
        let header_end = request
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .expect("request headers");
        let headers = String::from_utf8_lossy(&request[..header_end]).to_ascii_lowercase();
        assert!(headers.contains("authorization: bearer test-openrouter-key"));
        assert!(headers.contains("x-borg-request-id: openrouter-test"));
        assert!(headers.contains("x-session-id: borg-session:stable"));
        let body: Value =
            serde_json::from_slice(&request[header_end + 4..]).expect("request JSON body");
        assert_eq!(body["model"], "vendor/future-model");
        assert_eq!(body["session_id"], "borg-session:stable");
        assert_eq!(body["prompt_cache_key"], "borg-prefix:test");
        assert_eq!(body["reasoning"]["effort"], "high");
        assert_eq!(body["max_tokens"], 24000);
        assert_eq!(body["tool_choice"], "auto");
        assert_eq!(body["response_format"]["type"], "json_schema");
        assert_eq!(body["provider"]["require_parameters"], true);

        let ModelMessage::Assistant {
            content,
            reasoning_content,
            reasoning_details,
            tool_calls,
        } = result.message
        else {
            panic!("assistant response expected");
        };
        assert_eq!(content.as_deref(), Some(r#"{"ok":true}"#));
        assert_eq!(reasoning_content.as_deref(), Some("inspect "));
        assert!(reasoning_details.is_some());
        assert_eq!(tool_calls[0].function.name, "read_file");
        assert_eq!(result.usage.total_tokens, 29);
        assert_eq!(result.usage.cost_microusd, Some(123));
    }

    #[test]
    fn openrouter_model_metadata_exposes_effective_context_reserves() {
        let limits = openrouter_model_limits_from_response(&json!({
            "data": {
                "context_length": 200_000,
                "top_provider": { "max_completion_tokens": 32_000 }
            }
        }))
        .expect("model limits");
        assert_eq!(
            limits,
            OpenRouterModelLimits {
                context_window_tokens: 200_000,
                max_completion_tokens: Some(32_000),
            }
        );
        assert_eq!(
            limits
                .context_window_tokens
                .saturating_sub(limits.max_completion_tokens.unwrap_or(0)),
            168_000
        );
    }
}
