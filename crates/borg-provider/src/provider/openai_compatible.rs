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
    PromptCacheRefresh, Provider, ProviderAttemptTrace, ProviderCallError, ProviderCallResult,
    ProviderCallUsage, ProviderErrorKind, ProviderInvocation, ProviderProgress, StreamedToolAction,
    StructuredOutputDialect, apply_provider_request_timeout, chat_completion_response_format,
    extract_chat_completions_usage, nonempty_env, parse_chat_completion_json_text,
    provider_cost_usd_to_microusd, read_provider_error_response_text,
    read_provider_success_response_text, truncate_provider_text,
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
    /// How long this route keeps a prompt cache entry, when the operator has
    /// stated it. Borg documents no default here: warming spends money on a
    /// schedule derived from this number, so it is taken only from someone who
    /// knows the upstream, never guessed from a vendor's observed behaviour.
    pub prompt_cache_ttl_seconds: Option<u64>,
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
            prompt_cache_ttl_seconds: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenAiCompatibleProfile {
    Kimi,
    /// Z.ai GLM, reached over the same OpenAI-compatible wire format.
    Glm,
    /// Alibaba Cloud Model Studio (Qwen), reached over the same
    /// OpenAI-compatible wire format.
    Qwen,
    OpenRouter,
    Generic,
}

impl OpenAiCompatibleProfile {
    fn label(self) -> &'static str {
        match self {
            Self::Kimi => "kimi",
            Self::Glm => "glm",
            Self::Qwen => "qwen",
            Self::OpenRouter => "openrouter",
            Self::Generic => "openai-compatible",
        }
    }

    fn endpoint(self) -> String {
        match self {
            Self::Kimi => kimi_chat_completions_endpoint(),
            Self::Glm => glm_chat_completions_endpoint(),
            Self::Qwen => qwen_chat_completions_endpoint(),
            Self::OpenRouter => openrouter_chat_completions_endpoint(),
            Self::Generic => chat_completions_endpoint(),
        }
    }

    fn api_key(self) -> Option<String> {
        match self {
            Self::Kimi => {
                // A selected Kimi Code plan supplies the key for its own quota;
                // the pay-as-you-go variables remain the fallback.
                subscription_key(crate::subscription::Plan::KimiCode)
                    .or_else(|| nonempty_env("BORG_KIMI_API_KEY"))
                    .or_else(|| nonempty_env("MOONSHOT_API_KEY"))
            }
            Self::Glm => subscription_key(crate::subscription::Plan::GlmCoding)
                .or_else(|| nonempty_env("BORG_GLM_API_KEY"))
                .or_else(|| nonempty_env("ZHIPUAI_API_KEY")),
            Self::Qwen => {
                // A selected Coding Plan supplies its own `sk-sp-` key; the
                // pay-as-you-go DashScope key remains the fallback.
                subscription_key(crate::subscription::Plan::QwenCoding)
                    .or_else(|| nonempty_env("BORG_QWEN_API_KEY"))
                    .or_else(|| nonempty_env("DASHSCOPE_API_KEY"))
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
        self.model_turn_capped(request, progress, gateway, profile, None)
            .await
    }

    /// Re-send `request` with a minimal output budget so the provider resets
    /// the expiry on the prompt cache entry it already holds.
    ///
    /// This goes out over the same authenticated route as the real turn — the
    /// same endpoint, key, gateway headers and `prompt_cache_key` — because a
    /// refresh sent anywhere else would warm a cache the next real request
    /// never reads. The assistant message is discarded: only the usage is
    /// returned, so a refresh can be billed and shown without any chance of
    /// its content reaching the conversation or its tool calls being run.
    pub async fn refresh_prompt_cache_via_profile(
        &self,
        request: ModelTurnRequest,
        gateway: Option<&ModelGateway>,
        profile: OpenAiCompatibleProfile,
        refresh: PromptCacheRefresh,
    ) -> std::result::Result<ProviderCallUsage, ProviderCallError> {
        self.model_turn_capped(request, None, gateway, profile, Some(refresh))
            .await
            .map(|result| result.usage)
    }

    async fn model_turn_capped(
        &self,
        request: ModelTurnRequest,
        progress: Option<UnboundedSender<ProviderProgress>>,
        gateway: Option<&ModelGateway>,
        profile: OpenAiCompatibleProfile,
        refresh: Option<PromptCacheRefresh>,
    ) -> std::result::Result<ModelTurnResult, ProviderCallError> {
        let mut request = request;
        // A refresh must not reuse the real turn's client request id. An
        // upstream that deduplicates on it could answer from the original
        // response without refreshing anything, and the paid refresh would be
        // indistinguishable from the turn it is protecting in any log. The
        // cache entry is keyed by `prompt_cache_key`, which is left alone.
        if refresh.is_some()
            && let Some(id) = request.request_id.as_mut()
        {
            id.push_str(":warm");
        }
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
        if request.fast {
            return Err(ProviderCallError {
                message: "fast mode is not supported by this compatible model route".to_string(),
                trace: Box::new(trace),
                session_id: None,
                kind: ProviderErrorKind::Unknown,
            });
        }
        let api_key = gateway
            .and_then(|gateway| {
                (!gateway.bearer_token.trim().is_empty()).then(|| gateway.bearer_token.clone())
            })
            .or_else(|| profile.api_key());
        if api_key.is_none() && profile != OpenAiCompatibleProfile::Generic {
            return Err(ProviderCallError {
                message: match profile {
                    OpenAiCompatibleProfile::Kimi => {
                        "no Kimi key: set BORG_SUBSCRIPTION=kimi with KIMI_API_KEY for a \
                         Kimi Code plan, or BORG_KIMI_API_KEY / MOONSHOT_API_KEY for the \
                         pay-as-you-go API"
                            .to_string()
                    }
                    OpenAiCompatibleProfile::Glm => {
                        "no GLM key: set BORG_SUBSCRIPTION=glm with ZAI_API_KEY for a \
                         GLM Coding Plan, or BORG_GLM_API_KEY for the pay-as-you-go API"
                            .to_string()
                    }
                    OpenAiCompatibleProfile::Qwen => {
                        "no Qwen key: select the Qwen Coding Plan with `borg login qwen` \
                         (BAILIAN_CODING_PLAN_API_KEY), or set DASHSCOPE_API_KEY for the \
                         pay-as-you-go Model Studio API"
                            .to_string()
                    }
                    OpenAiCompatibleProfile::OpenRouter => {
                        "OPENROUTER_API_KEY is not set".to_string()
                    }
                    OpenAiCompatibleProfile::Generic => unreachable!(),
                },
                trace: Box::new(trace),
                session_id: None,
                kind: ProviderErrorKind::Unknown,
            });
        }
        let request_id = request.request_id.clone();
        let deepseek_model = request_model.to_ascii_lowercase().contains("deepseek");
        let wire_messages = request
            .messages
            .iter()
            .flat_map(|message| model_message_wire_values(message, deepseek_model))
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
            OpenAiCompatibleProfile::Glm => {
                // Z.ai takes the OpenAI `reasoning_effort` spelling; it has no
                // separate completion-token knob to set here.
                body["reasoning_effort"] = json!(glm_reasoning_effort(self.effort.as_deref()));
            }
            OpenAiCompatibleProfile::Qwen => {
                // Alibaba's OpenAI-compatible route gates reasoning with
                // `enable_thinking` rather than the OpenAI `reasoning_effort`
                // spelling. Thinking is on by default; a caller asking for no
                // effort turns it off so a cheap turn stays cheap.
                body["enable_thinking"] = json!(qwen_thinking_enabled(self.effort.as_deref()));
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
                        kind: ProviderErrorKind::Unknown,
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
                OpenAiCompatibleProfile::Kimi
                | OpenAiCompatibleProfile::Glm
                | OpenAiCompatibleProfile::Qwen => Some("json_schema".to_string()),
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

        // Applied last so it overrides every profile, env and gateway budget
        // above, but only in the spelling the body already uses: an OpenAI
        // style endpoint rejects `max_tokens` outright for a reasoning model,
        // so writing both would turn a refresh into a failed request. With
        // neither present the profile is the only evidence of what the
        // upstream accepts.
        if let Some(refresh) = refresh {
            let field = if body.get("max_completion_tokens").is_some() {
                "max_completion_tokens"
            } else if body.get("max_tokens").is_some() {
                "max_tokens"
            } else if profile == OpenAiCompatibleProfile::Generic {
                "max_completion_tokens"
            } else {
                "max_tokens"
            };
            body[field] = json!(refresh.max_output_tokens);
        }

        // Prompt cache markers are per model: this wire format is shared with
        // vendors that answer 400 for the field, so it goes out only where the
        // gateway was verified to accept it.
        if anthropic_cache_markers_enabled(provider_label, request_model) {
            apply_anthropic_cache_markers(&mut body);
        }
        let client = compatible_http_client();
        // A refresh is best-effort and must never queue retries behind the
        // agent's real traffic: if the first attempt fails the cache entry is
        // lost, and the next real request pays for it once.
        let max_attempts = if refresh.is_some() { 1 } else { 3 };
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
                        kind: ProviderErrorKind::from_transport(&error),
                    });
                }
            }
        };
        trace.invocation.args.push(format!("attempts={attempt}"));
        let status = response.status();
        if !status.is_success() {
            // A refusal with no body is unactionable, and a 400 usually means the
            // request itself was malformed, so record what the response says about
            // itself before the body is consumed.
            let content_type = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("none")
                .to_string();
            let request_id = response
                .headers()
                .get("x-request-id")
                .and_then(|value| value.to_str().ok())
                .unwrap_or("none")
                .to_string();
            let log_id = response
                .headers()
                .get("x-opencode-log-id")
                .and_then(|value| value.to_str().ok())
                .unwrap_or("none")
                .to_string();
            let shape = request_shape(&body);
            let raw_text = read_provider_error_response_text(response)
                .await
                .unwrap_or_else(|error| error.to_string());
            let body = if raw_text.trim().is_empty() {
                format!(
                    "the provider sent no body (content-type {content_type}, request-id {request_id}, provider-log-id {log_id})"
                )
            } else {
                truncate_provider_text(&raw_text, 500)
            };
            trace.exit_status = Some(1);
            trace.stderr = format!(
                "{raw_text}
request: {shape}"
            );
            return Err(ProviderCallError {
                message: format!(
                    "{provider_label} request failed with HTTP {}: {} [request: {shape}]",
                    status.as_u16(),
                    body
                ),
                trace: Box::new(trace),
                session_id: None,
                kind: compatible_provider_error_kind(&raw_text, status.as_u16()),
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
            message: format!(
                "{provider_label} streaming response failed: {}",
                error.message
            ),
            trace: Box::new(trace.clone()),
            session_id: None,
            kind: error.kind,
        })?;
        trace.stdout = streamed.raw.to_string();
        trace.exit_status = Some(0);
        let duration_ms = elapsed_millis_u64(started_at);
        let mut usage = match profile {
            OpenAiCompatibleProfile::Kimi => kimi_usage_from_response(&streamed.raw, duration_ms),
            // Plan usage is quota-metered rather than priced per token, so the
            // generic extractor (which reports no cost) is the honest one.
            OpenAiCompatibleProfile::Glm => {
                extract_chat_completions_usage(&streamed.raw, duration_ms, None)
            }
            OpenAiCompatibleProfile::Qwen => {
                extract_chat_completions_usage(&streamed.raw, duration_ms, None)
            }
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
                kind: ProviderErrorKind::Unknown,
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
                    kind: ProviderErrorKind::from_transport(&error),
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
                        kind: ProviderErrorKind::Unknown,
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
                        kind: ProviderErrorKind::Unknown,
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
                    body
                ),
                trace: Box::new(trace),
                session_id: None,
                kind: compatible_provider_error_kind(&raw_text, status.as_u16()),
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
                    kind: ProviderErrorKind::Unknown,
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

/// The key a selected coding plan provides, if it is the plan for this profile.
fn subscription_key(plan: crate::subscription::Plan) -> Option<String> {
    crate::subscription::active_for(plan).and_then(|plan| plan.api_key())
}

/// The base URL a selected coding plan serves from, if any.
///
/// An explicit `BORG_*_BASE_URL` always wins: an operator pointing Borg at a
/// proxy or a regional host must not be overridden by a plan default.
fn subscription_base_url(plan: crate::subscription::Plan) -> Option<String> {
    crate::subscription::active_for(plan).map(|plan| plan.base_url().to_string())
}

fn chat_completions_url(base: String) -> String {
    let trimmed = base.trim_end_matches('/');
    if trimmed.ends_with("/chat/completions") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/chat/completions")
    }
}

fn kimi_chat_completions_endpoint() -> String {
    // Kimi Code is served from `api.kimi.com/coding`, not the pay-as-you-go
    // `api.moonshot.ai`. Defaulting to the latter under an active plan would
    // spend credits while the plan sat unused.
    let base = nonempty_env("BORG_KIMI_BASE_URL")
        .or_else(|| subscription_base_url(crate::subscription::Plan::KimiCode))
        .unwrap_or_else(|| "https://api.moonshot.ai/v1".to_string());
    chat_completions_url(base)
}

/// Z.ai accepts the OpenAI `reasoning_effort` vocabulary. Borg's own effort
/// levels are wider, so clamp rather than pass an unknown value through.
fn glm_reasoning_effort(effort: Option<&str>) -> &'static str {
    match effort.map(str::trim) {
        Some("none") | Some("low") => "low",
        Some("max") | Some("xhigh") | Some("ultra") | Some("high") => "high",
        _ => "medium",
    }
}

fn glm_chat_completions_endpoint() -> String {
    let base = nonempty_env("BORG_GLM_BASE_URL")
        .or_else(|| subscription_base_url(crate::subscription::Plan::GlmCoding))
        // Without a plan, the general API host is the right default.
        .unwrap_or_else(|| "https://api.z.ai/api/paas/v4".to_string());
    chat_completions_url(base)
}

fn qwen_chat_completions_endpoint() -> String {
    // An active Coding Plan is served from the regional `coding.` host; without
    // one, the international pay-as-you-go compatible-mode host is the default.
    let base = nonempty_env("BORG_QWEN_BASE_URL")
        .or_else(|| subscription_base_url(crate::subscription::Plan::QwenCoding))
        .unwrap_or_else(|| "https://dashscope-intl.aliyuncs.com/compatible-mode/v1".to_string());
    chat_completions_url(base)
}

/// Qwen gates reasoning with `enable_thinking`. A caller that asked for no
/// effort gets thinking off; every other level leaves the vendor default on.
fn qwen_thinking_enabled(effort: Option<&str>) -> bool {
    !matches!(effort.map(str::trim), Some("none") | Some("off"))
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

/// Model ids that are established to accept Anthropic-style prompt cache
/// markers on the OpenCode Go route, keyed by the id the wire carries.
///
/// This is a list and not a name pattern because the gateway *rejects* the
/// field everywhere else instead of ignoring it: on the same body and endpoint,
/// glm-5.3 answered 400 with "Extra inputs are not permitted ... cache_control"
/// and kimi-k3 answered 400 with "The parameter cache_control is not supported
/// for the requested model", while every model listed here answered 200. A
/// wrong guess is therefore a failed turn, not a silent no-op. Verified live
/// against the Go gateway: a repeated 6418-token prefix reported 6412 cached
/// tokens with the markers, and no cache accounting at all without them.
const ANTHROPIC_CACHE_MARKER_MODELS: [&str; 5] = [
    "qwen3.6-plus",
    "qwen3.7-plus",
    "qwen3.7-max",
    "qwen3.8-flash",
    "qwen3.8-max",
];

/// The only route that may carry the markers: the OpenCode Go gateway these
/// models were verified against. Every other compatible endpoint - a
/// user-configured one, Kimi, GLM, Qwen through Alibaba Model Studio, or
/// OpenRouter - keeps the request body it sends today, byte for byte.
const ANTHROPIC_CACHE_MARKER_LABEL: &str = "opencode-go";

/// Override for a model that has not been added to the list yet, and for
/// turning the markers off if an upstream change breaks them. It can only widen
/// or narrow the selection within ANTHROPIC_CACHE_MARKER_LABEL: no value of it
/// makes another vendor receive the field.
const ANTHROPIC_CACHE_MARKER_ENV: &str = "BORG_ANTHROPIC_CACHE_MARKERS";

/// Whether this turn may carry Anthropic-style cache_control markers.
///
/// Off unless an entry in the list, or the environment override, says
/// otherwise.
fn anthropic_cache_markers_enabled(provider_label: &str, model: &str) -> bool {
    anthropic_cache_markers_for(
        provider_label,
        model,
        nonempty_env(ANTHROPIC_CACHE_MARKER_ENV).as_deref(),
    )
}

/// The enabled check with the override supplied by the caller, so the selection
/// can be exercised without touching the environment.
fn anthropic_cache_markers_for(
    provider_label: &str,
    model: &str,
    override_value: Option<&str>,
) -> bool {
    if provider_label != ANTHROPIC_CACHE_MARKER_LABEL {
        return false;
    }
    match override_value {
        Some("off") => false,
        Some("on") => true,
        _ => ANTHROPIC_CACHE_MARKER_MODELS.contains(&model),
    }
}

/// Mark the instruction block, the end of the conversation, and the tool list
/// boundary: the positions this upstream caches on.
///
/// Applied to the finished body, so no profile or configured gateway body can
/// add a marker and nothing built afterwards can drop one.
fn apply_anthropic_cache_markers(body: &mut Value) {
    let marker = json!({ "type": "ephemeral" });
    if let Some(messages) = body.get_mut("messages").and_then(Value::as_array_mut) {
        // The system prompt is the part of the prefix that never changes.
        if let Some(first) = messages.first_mut()
            && first.get("role").and_then(Value::as_str) == Some("system")
        {
            mark_last_text_part(first, &marker);
        }
        // The last message is the breakpoint the next turn extends.
        if let Some(last) = messages.last_mut()
            && last.get("role").and_then(Value::as_str) != Some("system")
        {
            mark_last_text_part(last, &marker);
        }
    }
    if let Some(tools) = body.get_mut("tools").and_then(Value::as_array_mut)
        && let Some(last) = tools.last_mut()
    {
        last["cache_control"] = marker;
    }
}

/// Point cache_control at the last text part of a message, which is the only
/// part an upstream accepts it on.
///
/// A plain string body becomes a single text part. A message with nothing to
/// mark - an assistant turn that is only tool calls - is left alone rather than
/// given invented content. Returns whether a marker was placed.
fn mark_last_text_part(message: &mut Value, marker: &Value) -> bool {
    let Some(content) = message.get_mut("content") else {
        return false;
    };
    if let Some(text) = content.as_str().map(str::to_string) {
        if text.is_empty() {
            return false;
        }
        *content = json!([{ "type": "text", "text": text, "cache_control": marker.clone() }]);
        return true;
    }
    let Some(parts) = content.as_array_mut() else {
        return false;
    };
    for part in parts.iter_mut().rev() {
        if part.get("type").and_then(Value::as_str) == Some("text")
            && part
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| !text.is_empty())
        {
            part["cache_control"] = marker.clone();
            return true;
        }
    }
    false
}

fn compatible_http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

/// Chat-completions `tool` messages are text-only on every server, so a tool
/// result that carries images is followed by a `user` message holding them,
/// labelled with the call id so the model can tie the pixels to the result.
fn model_message_wire_values(message: &ModelMessage, deepseek_model: bool) -> Vec<Value> {
    match message {
        ModelMessage::Tool {
            tool_call_id,
            content,
            attachments,
        } if !attachments.is_empty() => {
            let mut blocks = vec![json!({
                "type": "text",
                "text": format!("Image output of tool call {tool_call_id}:")
            })];
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
            vec![
                json!({ "role": "tool", "tool_call_id": tool_call_id, "content": content }),
                json!({ "role": "user", "content": blocks }),
            ]
        }
        _ => vec![model_message_wire_value(message, deepseek_model)],
    }
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

    if let Some(object) = wire.as_object_mut() {
        object.remove("provider_state");
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
    action_parser: StreamedToolAction,
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

/// A streaming-read failure that remembers whether the transport died.
///
/// The body of a streaming response can stop mid-frame, which is a lost
/// connection and worth retrying — but it reads as a parse failure once it has
/// been turned into a bare string. Keeping the kind here is what stops a
/// truncated stream from being billed as a failed turn.
#[derive(Debug)]
struct CompatibleStreamError {
    kind: ProviderErrorKind,
    message: String,
}

impl std::fmt::Display for CompatibleStreamError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl From<String> for CompatibleStreamError {
    /// Content-level problems (bad UTF-8, malformed SSE JSON) carry no
    /// transport signal, so they stay `Unknown` and fall back to prose.
    fn from(message: String) -> Self {
        Self {
            kind: ProviderErrorKind::Unknown,
            message,
        }
    }
}

impl From<&str> for CompatibleStreamError {
    fn from(message: &str) -> Self {
        Self::from(message.to_string())
    }
}

async fn read_compatible_model_stream(
    response: reqwest::Response,
    progress: Option<&UnboundedSender<ProviderProgress>>,
    model: &str,
    effort: Option<&str>,
) -> Result<CompatibleModelStream, CompatibleStreamError> {
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
        let chunk = chunk.map_err(|error| CompatibleStreamError {
            kind: ProviderErrorKind::from_transport(&error),
            message: error.to_string(),
        })?;
        total_bytes = total_bytes.saturating_add(chunk.len());
        if total_bytes > COMPATIBLE_STREAM_MAX_BYTES {
            return Err(format!(
                "stream exceeded the {COMPATIBLE_STREAM_MAX_BYTES} byte response limit"
            )
            .into());
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
                    let was_unkeyed = call.id.is_empty();
                    let name_delta = delta
                        .pointer("/function/name")
                        .and_then(Value::as_str)
                        .filter(|fragment| !fragment.is_empty());
                    let arguments_delta = delta
                        .pointer("/function/arguments")
                        .and_then(Value::as_str)
                        .filter(|fragment| !fragment.is_empty());
                    if let Some(id) = delta.get("id").and_then(Value::as_str)
                        && !id.is_empty()
                    {
                        call.id = id.to_string();
                    }
                    if let Some(name) = name_delta {
                        call.name.push_str(name);
                    }
                    if let Some(arguments) = arguments_delta {
                        call.arguments.push_str(arguments);
                    }
                    if name_delta.is_some() || arguments_delta.is_some() {
                        let event = if generating_tool_calls.insert(index) {
                            ProviderProgress::ToolCallGenerating {
                                id: (!call.id.is_empty()).then(|| call.id.clone()),
                            }
                        } else {
                            ProviderProgress::ToolCallInputDelta {
                                id: (!call.id.is_empty()).then(|| call.id.clone()),
                            }
                        };
                        if let Some(sender) = progress {
                            let _ = sender.send(event);
                        }
                    }
                    if !call.id.is_empty()
                        && !call.name.is_empty()
                        && !described_tool_calls.contains(&index)
                        && started_tool_calls.insert(index)
                        && let Some(sender) = progress
                    {
                        let _ = sender.send(ProviderProgress::ToolCallStarted {
                            id: call.id.clone(),
                            name: call.name.clone(),
                            input: Value::Null,
                        });
                    }
                    if (!described_tool_calls.contains(&index)
                        || (was_unkeyed && !call.id.is_empty()))
                        && let Some(action) = call.action_parser.observe(&call.arguments)
                    {
                        described_tool_calls.insert(index);
                        if let Some(sender) = progress {
                            let _ = sender.send(ProviderProgress::ToolCallAction {
                                id: (!call.id.is_empty()).then(|| call.id.clone()),
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

    // Termination contract: a `finish_reason` is the model's own statement
    // that the turn is complete, so a stream carrying one that closes before
    // `data: [DONE]` (proxies and local servers do this) is complete, and
    // `[DONE]` without a `finish_reason` is a complete stream from a server
    // that omits the field. Only a stream that ends with neither is truncated.
    let finish_reason = match (finish_reason, saw_done) {
        (Some(reason), _) => reason,
        (None, true) => if tool_calls.is_empty() {
            "stop"
        } else {
            "tool_calls"
        }
        .to_string(),
        (None, false) => {
            return Err(CompatibleStreamError {
                kind: ProviderErrorKind::ConnectionLost,
                message: format!(
                    "stream ended before a finish_reason or data: [DONE] marker ({} content characters and {} tool calls received)",
                    content.chars().count(),
                    tool_calls.len()
                ),
            });
        }
    };
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
            return Err(
                format!("stream returned duplicate tool call id `{}`", tool_call.id).into(),
            );
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

fn compatible_retryable_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

/// Classify a refused compatible request from its own body.
///
/// A gateway in front of a vendor reports the vendor failure under the gateway
/// status rather than the vendor status: OpenCode Go answers a transient
/// upstream failure with `403` and a `server_error` body. Read by status alone
/// that is a permission refusal, which is never retried, so a blip that a
/// resend would clear instead ends the turn. The declared error type decides
/// when the two disagree, and every other refusal keeps the old answer.
/// The shape of the request a refusal answered.
///
/// Bounded on purpose: the roles in order (capped), the serialized size, how many
/// messages carry images, how many tool calls were sent and how many of them
/// nothing answers. A provider can refuse a request with no body at all, and then
/// this is the only evidence of what we sent - without it the refusal is
/// unattributable and the next reader is left guessing.
fn request_shape(body: &Value) -> String {
    let Some(messages) = body.get("messages").and_then(Value::as_array) else {
        return "messages=<none>".to_string();
    };
    let mut roles = String::new();
    let mut images = 0_usize;
    let mut calls = 0_usize;
    let mut sent: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut answered: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for message in messages {
        roles.push_str(match message.get("role").and_then(Value::as_str) {
            Some("system") => "s",
            Some("user") => "u",
            Some("assistant") => "a",
            Some("tool") => "t",
            _ => "?",
        });
        if message
            .get("content")
            .and_then(Value::as_array)
            .is_some_and(|parts| {
                parts.iter().any(|part| {
                    part.get("type").and_then(Value::as_str) == Some("image")
                        || part.get("image_url").is_some()
                })
            })
        {
            images += 1;
        }
        for call in message
            .get("tool_calls")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            calls += 1;
            if let Some(id) = call.get("id").and_then(Value::as_str) {
                sent.insert(id);
            }
        }
        if let Some(id) = message.get("tool_call_id").and_then(Value::as_str) {
            answered.insert(id);
        }
    }
    let shown: String = roles.chars().take(32).collect();
    let elided = roles.chars().count().saturating_sub(32);
    format!(
        "messages={}, roles={shown}{}, bytes={}, images={images}, tool_calls={calls}, unanswered_calls={}",
        messages.len(),
        if elided > 0 {
            format!("+{elided}")
        } else {
            String::new()
        },
        body.to_string().len(),
        sent.difference(&answered).count(),
    )
}

fn compatible_provider_error_kind(body: &str, status: u16) -> ProviderErrorKind {
    // A body too large for the endpoint is a deterministic refusal: a resend is
    // byte-identical, so retrying it can only repeat the same answer. Shrinking the
    // request is the one remedy that can succeed, and the context-length recovery
    // already knows how to do that, so this routes there instead of to the
    // connection-retry path. The status decides, because the body is the gateway
    // own wording and does not name the size.
    if status == 413 {
        return ProviderErrorKind::ContextLength;
    }
    let compact = body
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect::<String>();
    let declares_server_error = [r#""type":"server_error""#, r#""code":"server_error""#]
        .iter()
        .any(|marker| compact.contains(marker));
    if declares_server_error {
        ProviderErrorKind::ConnectionLost
    } else {
        ProviderErrorKind::Unknown
    }
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
                    fast: false,
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
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let first = [
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":""}}]},"finish_reason":null}]}"#,
            r#"data: {"choices":[{"delta":{"reasoning_details":[{"type":"reasoning.text","text":"inspect " }]},"finish_reason":null}]}"#,
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{"}}]},"finish_reason":null}]}"#,
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"path\":\"src/lib.rs\",\"action\":\"edit\","}}]},"finish_reason":null}]}"#,
            "",
        ].join("\n");
        let body = [
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"read_file","arguments":"\"offset\":1}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":5}}"#,
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
                        first.len() + body.len(),
                        first
                    )
                    .as_bytes(),
                )
                .await
                .expect("write first character");
            release_rx
                .await
                .expect("generation observed before remainder");
            socket
                .write_all(body.as_bytes())
                .await
                .expect("write remainder");
        });
        let response = reqwest::get(format!("http://{address}"))
            .await
            .expect("request test stream");
        let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();
        let stream = tokio::spawn(async move {
            read_compatible_model_stream(response, Some(&progress_tx), "local-model", Some("high"))
                .await
                .expect("parse stream")
        });

        let ProviderProgress::ProviderEvent {
            kind, content_text, ..
        } = tokio::time::timeout(Duration::from_secs(2), progress_rx.recv())
            .await
            .expect("early reasoning")
            .expect("reasoning progress event")
        else {
            panic!("expected reasoning progress event");
        };
        assert_eq!(kind, "reasoning_delta");
        assert_eq!(content_text.as_deref(), Some("inspect "));
        let ProviderProgress::ToolCallGenerating { id } =
            tokio::time::timeout(Duration::from_secs(2), progress_rx.recv())
                .await
                .expect("generation on first character")
                .expect("tool generation progress event")
        else {
            panic!("expected tool generation progress event");
        };
        assert_eq!(id, None);
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), progress_rx.recv())
                .await
                .unwrap(),
            Some(ProviderProgress::ToolCallInputDelta { id: None })
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), progress_rx.recv()).await.unwrap(),
            Some(ProviderProgress::ToolCallAction { id: None, action }) if action == "edit"
        ));
        assert!(progress_rx.try_recv().is_err());
        release_tx.send(()).expect("release remaining arguments");
        let streamed = stream.await.expect("stream task");
        server.await.expect("test server task");
        assert!(matches!(
            progress_rx.try_recv(),
            Ok(ProviderProgress::ToolCallInputDelta { id: Some(id) }) if id == "call-1"
        ));
        let ProviderProgress::ToolCallAction { id, action } =
            progress_rx.try_recv().expect("tool action progress event")
        else {
            panic!("expected tool action progress event");
        };
        assert_eq!(id.as_deref(), Some("call-1"));
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
            r#"{"path":"src/lib.rs","action":"edit","offset":1}"#
        );
        assert_eq!(streamed.raw["usage"]["prompt_tokens"], 10);
    }

    #[test]
    fn streamed_action_is_early_even_when_reordered() {
        let streamed_tool_action = |input: &str| StreamedToolAction::default().observe(input);
        assert_eq!(
            streamed_tool_action(r#"{"action":"delete files","#).as_deref(),
            Some("delete files")
        );
        assert_eq!(streamed_tool_action(r#"{"action":"edi"#), None);
        assert_eq!(
            streamed_tool_action(r#"{"cmd":"pwd","action":"inspect","#).as_deref(),
            Some("inspect")
        );
        assert_eq!(
            streamed_tool_action(r#"{"payload":{"action":"nested"},"action":"edi"#),
            None
        );
        assert_eq!(
            streamed_tool_action(r#"{"payload":{"action":"nested"},"action":"edit","#).as_deref(),
            Some("edit")
        );
    }

    #[test]
    fn tool_images_are_carried_in_a_follow_up_user_message() {
        let plain = ModelMessage::tool("call-1", "ok");
        assert_eq!(model_message_wire_values(&plain, false).len(), 1);
        let message = ModelMessage::Tool {
            tool_call_id: "call-1".to_string(),
            content: "screenshot taken".to_string(),
            attachments: vec![crate::provider::ModelInputAttachment {
                media_type: "image/png".to_string(),
                data_base64: "AAAA".to_string(),
                filename: None,
            }],
        };
        let wire = model_message_wire_values(&message, false);
        assert_eq!(wire.len(), 2);
        assert_eq!(wire[0]["role"], "tool");
        assert_eq!(wire[0]["tool_call_id"], "call-1");
        assert_eq!(wire[0]["content"], "screenshot taken");
        assert_eq!(wire[1]["role"], "user");
        assert!(
            wire[1]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("call-1")
        );
        assert_eq!(wire[1]["content"][1]["type"], "image_url");
        assert_eq!(
            wire[1]["content"][1]["image_url"]["url"],
            "data:image/png;base64,AAAA"
        );
    }

    async fn serve_sse_body(body: String) -> reqwest::Response {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        tokio::spawn(async move {
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
        reqwest::get(format!("http://{address}"))
            .await
            .expect("request test stream")
    }

    #[tokio::test]
    async fn stream_termination_requires_a_finish_reason_or_done_marker_not_both() {
        // finish_reason without [DONE]: complete.
        let streamed = read_compatible_model_stream(
            serve_sse_body(
                [
                    r#"data: {"choices":[{"delta":{"content":"done"},"finish_reason":"stop"}]}"#,
                    "",
                ]
                .join("\n"),
            )
            .await,
            None,
            "local-model",
            None,
        )
        .await
        .expect("finish_reason alone completes the turn");
        assert_eq!(streamed.finish_reason, "stop");

        // [DONE] without finish_reason: complete, reason inferred.
        let streamed = read_compatible_model_stream(
            serve_sse_body(
                [
                    r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"read_file","arguments":"{}"}}]},"finish_reason":null}]}"#,
                    "data: [DONE]",
                    "",
                ]
                .join("\n"),
            )
            .await,
            None,
            "local-model",
            None,
        )
        .await
        .expect("[DONE] alone completes the turn");
        assert_eq!(streamed.finish_reason, "tool_calls");

        // Neither: truncated, never reported as a complete turn.
        let error = read_compatible_model_stream(
            serve_sse_body(
                [
                    r#"data: {"choices":[{"delta":{"content":"half"},"finish_reason":null}]}"#,
                    "",
                ]
                .join("\n"),
            )
            .await,
            None,
            "local-model",
            None,
        )
        .await;
        let Err(error) = error else {
            panic!("a stream with neither marker is truncated");
        };
        assert!(error.message.contains("4 content characters"), "{error}");
        // A stream that stops before its terminator is a lost connection, and
        // must say so in its type rather than relying on the wording happening
        // to match a pattern downstream.
        assert_eq!(error.kind, ProviderErrorKind::ConnectionLost);
    }

    // A gateway carries a transient upstream failure on its own status, so the
    // status alone cannot decide whether a resend is worth it. Reading the
    // declared type keeps such a blip from ending the turn without turning
    // every refusal into a retry.
    #[test]
    fn a_declared_server_error_is_retryable_but_a_genuine_refusal_is_not() {
        assert_eq!(
            compatible_provider_error_kind(
                r#"{"error":{"type":"server_error","code":"server_error","message":"Upstream request failed: [server_error] Upstream response was not valid JSON"}}"#,
                403
            ),
            ProviderErrorKind::ConnectionLost
        );
        assert_eq!(
            compatible_provider_error_kind(
                r#"{"error":{"type":"invalid_request_error","message":"unknown model"}}"#,
                400
            ),
            ProviderErrorKind::Unknown
        );
        assert_eq!(
            compatible_provider_error_kind("403 Forbidden", 403),
            ProviderErrorKind::Unknown
        );
        // A body too large for the endpoint is deterministic: a resend is
        // byte-identical, so retrying it can only repeat the same answer. The
        // remedy is to shrink the request, which is what the context-length
        // recovery does, so this must not be read as a transient loss however
        // the gateway words the body.
        assert_eq!(
            compatible_provider_error_kind(
                r#"{"error":{"type":"server_error","code":"server_error","message":"Upstream request failed: [server_error] Upstream response was not valid JSON"}}"#,
                413
            ),
            ProviderErrorKind::ContextLength
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
                    fast: false,
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
            ..
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

    /// The gateway answers 400 for cache_control on a model that does not
    /// accept it, so a wrong selection is a failed turn. This pins the
    /// selection: an unlisted model, another route, and another vendor all keep
    /// the request body they send today.
    #[test]
    fn anthropic_cache_markers_are_selected_only_where_the_gateway_accepts_them() {
        assert!(anthropic_cache_markers_for(
            "opencode-go",
            "qwen3.6-plus",
            None
        ));
        assert!(!anthropic_cache_markers_for("opencode-go", "glm-5.3", None));
        assert!(!anthropic_cache_markers_for("opencode-go", "kimi-k3", None));
        assert!(!anthropic_cache_markers_for("qwen", "qwen3.6-plus", None));
        assert!(!anthropic_cache_markers_for(
            "openai-compatible",
            "qwen3.6-plus",
            None
        ));
        // The override adds a model or disables the field, but it can never
        // move the field onto a different vendor.
        assert!(anthropic_cache_markers_for(
            "opencode-go",
            "qwen3.9-plus",
            Some("on")
        ));
        assert!(!anthropic_cache_markers_for(
            "opencode-go",
            "qwen3.6-plus",
            Some("off")
        ));
        assert!(!anthropic_cache_markers_for(
            "openai-compatible",
            "qwen3.9-plus",
            Some("on")
        ));
    }

    /// A marker on the wrong part caches nothing while still paying for the
    /// cache, so placement is part of the contract: the instruction block, the
    /// end of the conversation, and the last tool.
    #[test]
    fn anthropic_cache_markers_mark_system_conversation_end_and_tools() {
        let mut body = json!({
            "model": "qwen3.6-plus",
            "messages": [
                { "role": "system", "content": "You are Borg." },
                { "role": "assistant", "content": null, "tool_calls": [] },
                {
                    "role": "user",
                    "content": [
                        { "type": "image_url", "image_url": { "url": "data:image/png;base64,aW1hZ2U=" } },
                        { "type": "text", "text": "inspect the repository" }
                    ]
                }
            ],
            "tools": [
                { "type": "function", "function": { "name": "read_file" } },
                { "type": "function", "function": { "name": "write_file" } }
            ]
        });
        apply_anthropic_cache_markers(&mut body);

        assert_eq!(
            body["messages"][0]["content"],
            json!([{ "type": "text", "text": "You are Borg.", "cache_control": { "type": "ephemeral" } }])
        );
        // The last text part, not the last part: an image cannot carry it.
        assert_eq!(
            body["messages"][2]["content"][1]["cache_control"],
            json!({ "type": "ephemeral" })
        );
        assert!(
            body["messages"][2]["content"][0]
                .get("cache_control")
                .is_none()
        );
        assert_eq!(
            body["tools"][1]["cache_control"],
            json!({ "type": "ephemeral" })
        );
        assert!(body["tools"][0].get("cache_control").is_none());
        // An assistant turn that is only tool calls has no text to mark and is
        // left untouched rather than given invented content.
        assert_eq!(body["messages"][1]["content"], json!(null));
        assert!(body["messages"][1].get("cache_control").is_none());
    }
}
