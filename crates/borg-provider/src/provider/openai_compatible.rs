use std::collections::{BTreeMap, HashSet};
use std::sync::OnceLock;
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

const KIMI_STREAM_MAX_BYTES: usize = 128 * 1024 * 1024;

#[derive(Clone)]
pub struct ModelGateway {
    pub endpoint: String,
    pub bearer_token: String,
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
            Self::Kimi => chat_completions_endpoint(true),
            Self::OpenRouter => openrouter_chat_completions_endpoint(),
            Self::Generic => chat_completions_endpoint(false),
        }
    }

    fn api_key(self) -> Option<String> {
        match self {
            Self::Kimi => {
                nonempty_env("BORG_KIMI_API_KEY").or_else(|| nonempty_env("MOONSHOT_API_KEY"))
            }
            Self::OpenRouter => nonempty_env("OPENROUTER_API_KEY"),
            Self::Generic => nonempty_env("BORG_OPENAI_COMPATIBLE_API_KEY")
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
    /// translates a typed turn to Kimi's chat-completions wire contract and
    /// returns the complete assistant message, including reasoning content.
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
        let profile = if gateway.is_some() || self.is_kimi() {
            OpenAiCompatibleProfile::Kimi
        } else {
            OpenAiCompatibleProfile::Generic
        };
        self.model_turn_via_profile(request, progress, gateway, profile)
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
        let provider_label = profile.label();
        let mut trace = ProviderAttemptTrace {
            invocation: ProviderInvocation {
                provider_label: provider_label.to_string(),
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
        let api_key = gateway
            .map(|gateway| gateway.bearer_token.clone())
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
                trace,
                session_id: None,
            });
        }
        let request_id = request.request_id.clone();
        let wire_messages = request
            .messages
            .iter()
            .map(model_message_wire_value)
            .collect::<Vec<_>>();
        let mut body = json!({
            "model": self.model,
            "messages": wire_messages,
            "stream": true,
            "stream_options": { "include_usage": true },
        });
        match profile {
            OpenAiCompatibleProfile::Kimi => {
                body["reasoning_effort"] = json!(kimi_reasoning_effort(self.effort.as_deref()));
                body["max_completion_tokens"] = json!(kimi_max_completion_tokens());
            }
            OpenAiCompatibleProfile::OpenRouter => {
                if let Some(reasoning) = compatible_reasoning(self.effort.as_deref()) {
                    body["reasoning"] = reasoning;
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
                        trace: trace.clone(),
                        session_id: None,
                    })?
                {
                    let body_object = body.as_object_mut().expect("request body is an object");
                    body_object.extend(extra);
                }
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
                OpenAiCompatibleProfile::OpenRouter => {
                    nonempty_env("BORG_OPENROUTER_RESPONSE_FORMAT")
                        .or_else(|| Some("json_schema".to_string()))
                }
                OpenAiCompatibleProfile::Generic => {
                    nonempty_env("BORG_OPENAI_COMPATIBLE_RESPONSE_FORMAT")
                }
                OpenAiCompatibleProfile::Kimi => Some("json_schema".to_string()),
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
            if profile == OpenAiCompatibleProfile::OpenRouter {
                request = request
                    .header("HTTP-Referer", "https://borg.ml")
                    .header("X-Title", "Borg");
            }
            if let Some(request_id) = request_id.as_deref() {
                request = request.header("x-borg-request-id", request_id);
            }
            match apply_provider_request_timeout(request).send().await {
                Ok(response)
                    if attempt < max_attempts && kimi_retryable_status(response.status()) =>
                {
                    let delay = kimi_retry_delay(&response, attempt);
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
                    let delay = kimi_retry_delay_without_response(attempt);
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
                        trace,
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
                trace,
                session_id: None,
            });
        }

        let streamed = read_kimi_model_stream(
            response,
            progress.as_ref(),
            &self.model,
            self.effort.as_deref(),
        )
        .await
        .map_err(|error| ProviderCallError {
            message: format!("{provider_label} streaming response failed: {error}"),
            trace: trace.clone(),
            session_id: None,
        })?;
        trace.stdout = streamed.raw.to_string();
        trace.exit_status = Some(0);
        let duration_ms = elapsed_millis_u64(started_at);
        let usage = match profile {
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
        if self.is_kimi() {
            let mut messages = Vec::new();
            if !self.system_prompt.trim().is_empty() {
                messages.push(ModelMessage::System {
                    content: self.system_prompt.to_string(),
                });
            }
            messages.push(ModelMessage::user(prompt));
            let result = self
                .model_turn(
                    ModelTurnRequest {
                        request_id: None,
                        messages,
                        tools: Vec::new(),
                        output_schema: schema.cloned(),
                    },
                    progress,
                )
                .await?;
            let ModelMessage::Assistant {
                content,
                tool_calls,
                ..
            } = &result.message
            else {
                unreachable!("model_turn always returns an assistant message")
            };
            if !tool_calls.is_empty() {
                return Err(ProviderCallError {
                    message: "Kimi returned an unexpected tool call for a tool-free request"
                        .to_string(),
                    trace: result.trace,
                    session_id: None,
                });
            }
            let text = content.clone().unwrap_or_default();
            let value = if schema.is_some() {
                parse_chat_completion_json_text(&text).ok_or_else(|| ProviderCallError {
                    message: format!(
                        "Kimi returned non-JSON content for structured call: {}",
                        truncate_provider_text(&text, 500)
                    ),
                    trace: result.trace.clone(),
                    session_id: None,
                })?
            } else {
                Value::String(text)
            };
            return Ok(ProviderCallResult {
                value,
                raw_response: result.raw_response,
                usage: result.usage,
                trace: result.trace,
                session_id: None,
            });
        }

        let started_at = Instant::now();
        let endpoint = chat_completions_endpoint(false);
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
                trace: trace.clone(),
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
                    trace: trace.clone(),
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
                        trace,
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
                        trace,
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
                trace,
                session_id: None,
            });
        }

        let raw: Value = match serde_json::from_str(&raw_text) {
            Ok(value) => value,
            Err(error) => {
                trace.stderr = error.to_string();
                return Err(ProviderCallError {
                    message: format!("OpenAI-compatible endpoint returned invalid JSON: {error}"),
                    trace,
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

        Ok(ProviderCallResult {
            value,
            raw_response: raw.clone(),
            usage: extract_chat_completions_usage(&raw, elapsed_millis_u64(started_at), None),
            trace,
            session_id: None,
        })
    }
    fn is_kimi(&self) -> bool {
        self.model == crate::kimi_product_model()
    }
}

fn compatible_http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

fn model_message_wire_value(message: &ModelMessage) -> Value {
    match message {
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
    }
}

fn chat_completions_endpoint(kimi: bool) -> String {
    let base = if kimi {
        nonempty_env("BORG_KIMI_BASE_URL")
            .unwrap_or_else(|| "https://api.moonshot.ai/v1".to_string())
    } else {
        nonempty_env("BORG_OPENAI_COMPATIBLE_BASE_URL")
            .unwrap_or_else(|| "http://127.0.0.1:8000/v1".to_string())
    };
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

struct KimiModelStream {
    message: ModelMessage,
    finish_reason: String,
    raw: Value,
}

#[derive(Default)]
struct PartialKimiToolCall {
    id: String,
    name: String,
    arguments: String,
}

async fn read_kimi_model_stream(
    response: reqwest::Response,
    progress: Option<&UnboundedSender<ProviderProgress>>,
    model: &str,
    effort: Option<&str>,
) -> Result<KimiModelStream, String> {
    let mut stream = response.bytes_stream();
    let mut pending = Vec::new();
    let mut total_bytes = 0_usize;
    let mut content = String::new();
    let mut reasoning_content = String::new();
    let mut reasoning_details = Vec::new();
    let mut tool_calls = BTreeMap::<usize, PartialKimiToolCall>::new();
    let mut finish_reason = None;
    let mut usage = None;
    let mut saw_done = false;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| error.to_string())?;
        total_bytes = total_bytes.saturating_add(chunk.len());
        if total_bytes > KIMI_STREAM_MAX_BYTES {
            return Err(format!(
                "stream exceeded the {KIMI_STREAM_MAX_BYTES} byte response limit"
            ));
        }
        pending.extend_from_slice(&chunk);
        while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
            let mut line = pending.drain(..=newline).collect::<Vec<_>>();
            if line.last() == Some(&b'\n') {
                line.pop();
            }
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            let line = std::str::from_utf8(&line)
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
            if let Some(delta) = chunk
                .pointer("/choices/0/delta/reasoning_content")
                .or_else(|| chunk.pointer("/choices/0/delta/reasoning"))
                .and_then(Value::as_str)
            {
                reasoning_content.push_str(delta);
                if let Some(sender) = progress {
                    let _ = sender.send(ProviderProgress::ProviderEvent {
                        kind: "reasoning_delta".to_string(),
                        payload: json!({ "text": delta }),
                        raw_payload: None,
                        stream_channel: Some("reasoning".to_string()),
                        content_text: Some(delta.to_string()),
                        provider_item_id: None,
                        tool_use_id: None,
                        tool_name: None,
                        model: Some(model.to_string()),
                        effort: effort.map(str::to_string),
                    });
                }
            }
            if let Some(details) = chunk
                .pointer("/choices/0/delta/reasoning_details")
                .and_then(Value::as_array)
            {
                reasoning_details.extend(details.iter().cloned());
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
    Ok(KimiModelStream {
        message,
        finish_reason,
        raw,
    })
}

fn kimi_reasoning_effort(effort: Option<&str>) -> &'static str {
    match effort.map(str::trim) {
        Some("low") => "low",
        Some("high") => "high",
        Some("max") | Some("xhigh") | Some("ultra") => "max",
        _ => "max",
    }
}

fn kimi_retryable_status(status: reqwest::StatusCode) -> bool {
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
        raw_payload: None,
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

fn kimi_retry_delay(response: &reqwest::Response, attempt: u32) -> std::time::Duration {
    response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|seconds| std::time::Duration::from_secs(seconds.min(30)))
        .unwrap_or_else(|| kimi_retry_delay_without_response(attempt))
}

fn kimi_retry_delay_without_response(attempt: u32) -> std::time::Duration {
    std::time::Duration::from_millis(500_u64.saturating_mul(1_u64 << attempt.saturating_sub(1)))
}

fn kimi_max_completion_tokens() -> u64 {
    nonempty_env("BORG_KIMI_MAX_COMPLETION_TOKENS")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(131_072)
        .clamp(1, 1_048_576)
}

pub fn kimi_usage_from_response(
    raw: &Value,
    duration_ms: u64,
) -> crate::runtime::ProviderCallUsage {
    let mut usage = extract_chat_completions_usage(raw, duration_ms, Some(kimi_cost_microusd(raw)));
    // Kimi K3 exposes a 1M-token context window. Report the provider's prompt
    // occupancy so Borg can drive the same context UI and compaction warnings
    // as provider-native integrations.
    usage.context_tokens = Some(usage.input_tokens.saturating_add(usage.cached_input_tokens));
    usage.context_window_tokens = Some(1_048_576);
    usage
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
    use std::sync::Mutex;

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
        let wire = model_message_wire_value(&message);
        assert_eq!(wire["content"][0]["type"], "text");
        assert_eq!(wire["content"][1]["type"], "image_url");
        assert_eq!(
            wire["content"][1]["image_url"]["url"],
            "data:image/png;base64,aW1hZ2U="
        );
    }
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    static OPENROUTER_ENV_LOCK: Mutex<()> = Mutex::new(());

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
    fn product_kimi_model_selects_the_direct_route() {
        let provider = OpenAiCompatibleProvider {
            model: crate::kimi_product_model().to_string(),
            effort: None,
            system_prompt: "",
        };
        assert!(provider.is_kimi());
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

    #[test]
    fn kimi_usage_reports_k3_context_occupancy() {
        let raw = json!({
            "usage": {
                "prompt_tokens": 1_000,
                "prompt_tokens_details": { "cached_tokens": 200 },
                "completion_tokens": 100
            }
        });
        let usage = kimi_usage_from_response(&raw, 5);
        assert_eq!(usage.context_tokens, Some(1_000));
        assert_eq!(usage.context_window_tokens, Some(1_048_576));
    }

    #[test]
    fn kimi_effort_matches_the_k3_wire_contract() {
        assert_eq!(kimi_reasoning_effort(Some("low")), "low");
        assert_eq!(kimi_reasoning_effort(Some("high")), "high");
        assert_eq!(kimi_reasoning_effort(Some("max")), "max");
        assert_eq!(kimi_reasoning_effort(None), "max");
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
    fn kimi_retries_only_rate_limits_and_server_failures() {
        assert!(kimi_retryable_status(
            reqwest::StatusCode::TOO_MANY_REQUESTS
        ));
        assert!(kimi_retryable_status(
            reqwest::StatusCode::SERVICE_UNAVAILABLE
        ));
        assert!(!kimi_retryable_status(reqwest::StatusCode::BAD_REQUEST));
    }

    #[test]
    fn kimi_retry_event_carries_structured_attempt_and_backoff() {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

        emit_compatible_retry_event(
            Some(&sender),
            OpenAiCompatibleProfile::Kimi,
            "kimi-k3",
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
        assert_eq!(payload["provider"], "kimi");
        assert_eq!(payload["status"], 429);
        assert_eq!(payload["attempt"], 1);
        assert_eq!(payload["max_attempts"], 3);
        assert_eq!(payload["delay_ms"], 750);
    }

    #[tokio::test]
    async fn kimi_stream_preserves_reasoning_and_incremental_tool_calls() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let body = [
            r#"data: {"choices":[{"delta":{"reasoning_content":"inspect ","reasoning_details":[{"type":"reasoning.text","text":"inspect " }]},"finish_reason":null}]}"#,
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"read_","arguments":"{\"pa"}}]},"finish_reason":null}]}"#,
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"name":"file","arguments":"th\":\"src/lib.rs\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":5}}"#,
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
        let streamed = read_kimi_model_stream(response, None, "kimi-k3", Some("high"))
            .await
            .expect("parse stream");
        server.await.expect("test server task");

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
        assert_eq!(tool_calls[0].function.arguments, r#"{"path":"src/lib.rs"}"#);
        assert_eq!(streamed.raw["usage"]["prompt_tokens"], 10);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn openrouter_arbitrary_model_runs_the_complete_native_wire_contract() {
        let _lock = OPENROUTER_ENV_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind OpenRouter test server");
        let address = listener.local_addr().expect("test server address");
        let base_url = format!("http://{address}/api/v1");
        let _base = TestEnvGuard::set("BORG_OPENROUTER_BASE_URL", &base_url);
        let _key = TestEnvGuard::set("OPENROUTER_API_KEY", "test-openrouter-key");

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
        let body: Value =
            serde_json::from_slice(&request[header_end + 4..]).expect("request JSON body");
        assert_eq!(body["model"], "vendor/future-model");
        assert_eq!(body["reasoning"]["effort"], "high");
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
}
