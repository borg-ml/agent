//! OpenAI model access for ChatGPT subscriptions and explicitly selected API
//! billing. Borg owns the conversation and tools in both modes.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use borg_core::{CostBasis, ModelProviderState, ProviderCallUsage};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc::UnboundedSender;

use super::{
    ModelMessage, ModelToolCall, ModelTurnRequest, ModelTurnResult, ProviderAttemptTrace,
    ProviderCallError, ProviderInvocation, ProviderProgress, ProviderProgressStream,
    StreamedToolAction, apply_provider_request_timeout,
};

const ENDPOINT: &str = "https://chatgpt.com/backend-api/codex/responses";
const API_ENDPOINT: &str = "https://api.openai.com/v1/responses";
const MODELS_ENDPOINT: &str = "https://chatgpt.com/backend-api/codex/models";
const MAX_STREAM_BYTES: usize = 128 * 1024 * 1024;
const MAX_EVENT_BYTES: usize = 8 * 1024 * 1024;

pub struct CodexModelProvider {
    pub model: String,
    pub effort: String,
}

#[derive(Clone, Deserialize)]
struct ModelCapabilities {
    slug: String,
    supported_reasoning_levels: Vec<ReasoningLevel>,
    #[serde(default)]
    service_tiers: Vec<ServiceTier>,
    #[serde(default)]
    additional_speed_tiers: Vec<String>,
    context_window: Option<u64>,
    max_context_window: Option<u64>,
    #[serde(default = "default_context_percent")]
    effective_context_window_percent: u64,
}

#[derive(Clone, Deserialize)]
struct ReasoningLevel {
    effort: String,
}

#[derive(Clone, Deserialize)]
struct ServiceTier {
    id: String,
}

// This is the wire protocol's default when the catalog omits the field.
fn default_context_percent() -> u64 {
    95
}

impl ModelCapabilities {
    fn supports_fast(&self) -> bool {
        self.service_tiers.iter().any(|tier| tier.id == "priority")
            || self
                .additional_speed_tiers
                .iter()
                .any(|tier| tier == "fast")
    }

    fn usable_context_window(&self) -> Result<u64> {
        let window = self
            .context_window
            .or(self.max_context_window)
            .filter(|window| *window > 0)
            .context("Codex model catalog omitted the context window")?;
        ensure!(
            (1..=100).contains(&self.effective_context_window_percent),
            "Codex model catalog returned an invalid usable context percentage"
        );
        let usable = u128::from(window) * u128::from(self.effective_context_window_percent) / 100;
        ensure!(
            usable > 0,
            "Codex model catalog returned an empty usable context window"
        );
        Ok(usable as u64)
    }
}

struct CachedModels {
    account: String,
    fetched: Instant,
    models: Vec<ModelCapabilities>,
}

// Never serialize or debug credentials. Native auth owns refresh-token persistence.
struct SubscriptionAccess {
    token: String,
    account_id: String,
}

impl SubscriptionAccess {
    fn is_api_key(&self) -> bool {
        self.account_id.is_empty()
    }

    fn endpoint(&self) -> &'static str {
        if self.is_api_key() {
            API_ENDPOINT
        } else {
            ENDPOINT
        }
    }

    async fn send_with_recovery(
        &mut self,
        request: reqwest::RequestBuilder,
        expected_account: &str,
        refresh: impl std::future::Future<Output = Result<Self>>,
    ) -> Result<reqwest::Response> {
        ensure!(
            self.identity() == expected_account,
            "OpenAI credentials changed during this turn; retry to use the currently selected account"
        );
        if self.is_api_key() {
            return request
                .bearer_auth(&self.token)
                .send()
                .await
                .context("OpenAI API connection failed");
        }
        let retry = request
            .try_clone()
            .context("Codex request cannot be replayed for authentication recovery")?;
        let response = request
            .bearer_auth(&self.token)
            .header("ChatGPT-Account-Id", &self.account_id)
            .send()
            .await
            .context("Codex subscription connection failed")?;
        if response.status() != reqwest::StatusCode::UNAUTHORIZED {
            return Ok(response);
        }
        drop(response);
        let refreshed = refresh.await?;
        ensure!(
            refreshed.identity() == expected_account,
            "OpenAI account changed during authentication recovery; retry to use the currently selected account"
        );
        *self = refreshed;
        retry
            .bearer_auth(&self.token)
            .header("ChatGPT-Account-Id", &self.account_id)
            .send()
            .await
            .context("Codex subscription connection failed")
    }

    fn identity(&self) -> String {
        if self.is_api_key() {
            return format!(
                "api-sha256:{}",
                hex::encode(Sha256::digest(format!("borg:openai:api:{}", self.token)))
            );
        }
        format!(
            "sha256:{}",
            hex::encode(Sha256::digest(format!(
                "borg:codex:account:{}",
                self.account_id
            )))
        )
    }

    async fn model_capabilities(
        &mut self,
        client: &reqwest::Client,
        model: &str,
    ) -> Result<ModelCapabilities> {
        static CACHE: OnceLock<tokio::sync::Mutex<Option<CachedModels>>> = OnceLock::new();
        let mut cache = CACHE
            .get_or_init(|| tokio::sync::Mutex::new(None))
            .lock()
            .await;
        let account = self.identity();
        if !cache.as_ref().is_some_and(|entry| {
            entry.account == account && entry.fetched.elapsed() < Duration::from_secs(300)
        }) {
            // The catalog gates model visibility on its client protocol version, not Borg releases.
            // Keep Borg identified by originator; no installed executable is needed.
            let version = "0.154.0";
            let rejected_token = self.token.clone();
            let response = self
                .send_with_recovery(
                    client
                        .get(MODELS_ENDPOINT)
                        .query(&[("client_version", version)])
                        .header("originator", "borg")
                        .timeout(Duration::from_secs(30)),
                    &account,
                    Self::read(Some(rejected_token)),
                )
                .await?;
            let response = check_subscription_response(response).await?;
            let mut stream = response.bytes_stream();
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.context("Codex model catalog disconnected")?;
                ensure!(
                    bytes.len().saturating_add(chunk.len()) <= MAX_EVENT_BYTES,
                    "Codex model catalog exceeds size limit"
                );
                bytes.extend_from_slice(&chunk);
            }
            #[derive(Deserialize)]
            struct Catalog {
                models: Vec<ModelCapabilities>,
            }
            let catalog: Catalog =
                serde_json::from_slice(&bytes).context("invalid Codex model catalog")?;
            *cache = Some(CachedModels {
                account,
                fetched: Instant::now(),
                models: catalog.models,
            });
        }
        cache
            .as_ref()
            .and_then(|entry| entry.models.iter().find(|entry| entry.slug == model))
            .cloned()
            .context("selected model is not available in this Codex account's catalog")
    }

    async fn read(rejected_token: Option<String>) -> Result<Self> {
        use crate::credentials::{openai_api_key, openai_auth_mode, openai_uses_api_key};
        openai_auth_mode()?;
        if openai_uses_api_key() {
            return Ok(Self {
                token: openai_api_key()
                    .context("OpenAI API key missing; add one with borg login codex --api-key")?,
                account_id: String::new(),
            });
        }
        let access = crate::openai_subscription::access(rejected_token).await?;
        Ok(Self {
            token: access.token,
            account_id: access.account_id,
        })
    }
}

impl CodexModelProvider {
    /// Non-secret access identity, captured by Borg for this turn only.
    pub async fn account_identity() -> Result<String> {
        Ok(SubscriptionAccess::read(None).await?.identity())
    }

    /// Keep the credentials selected at turn admission stable while this turn runs.
    pub async fn model_turn_for_account(
        &self,
        mut request: ModelTurnRequest,
        progress: Option<UnboundedSender<ProviderProgress>>,
        expected_account: &str,
    ) -> std::result::Result<ModelTurnResult, ProviderCallError> {
        let started = Instant::now();
        let mut trace = ProviderAttemptTrace {
            invocation: ProviderInvocation {
                provider_label: "codex-model".into(),
                executable: ENDPOINT.into(),
                args: Vec::new(),
                cwd: None,
                model: Some(self.model.clone()),
                effort: Some(self.effort.clone()),
            },
            exit_status: None,
            stdout: String::new(),
            stderr: String::new(),
        };
        let result = async {
            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(30))
                .build()?;
            let mut access = SubscriptionAccess::read(None).await?;
            trace.invocation.executable = access.endpoint().into();
            ensure!(access.identity() == expected_account,
                "OpenAI credentials changed during this turn; retry to use the currently selected account");
            let context_window = if access.is_api_key() {
                None
            } else {
                let capabilities = access.model_capabilities(&client, &self.model).await?;
                ensure!(capabilities.supported_reasoning_levels.iter().any(|level| level.effort == self.effort),
                    "selected effort is not supported by this Codex model");
                ensure!(!request.fast || capabilities.supports_fast(),
                    "fast mode is not supported by this Codex model");
                Some(capabilities.usable_context_window()?)
            };
            let body = self.request_body_for_account(&mut request, expected_account)?;
            let endpoint = access.endpoint();
            let response = self
                .send(
                    &client,
                    endpoint,
                    &mut access,
                    expected_account,
                    &request,
                    &body,
                )
                .await?;
            let (mut message, response) = self.read_stream(response, progress.as_ref()).await?;
            if let ModelMessage::Assistant { provider_state: Some(ModelProviderState::OpenAiResponses { account_identity, .. }), .. } = &mut message {
                *account_identity = Some(expected_account.to_string());
            }
            Ok::<_, anyhow::Error>((message, response, context_window, access.is_api_key()))
        }
        .await;
        match result {
            Ok((message, raw_response, context_window, api_key)) => {
                trace.exit_status = Some(0);
                let input = raw_response
                    .pointer("/usage/input_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let cached = raw_response
                    .pointer("/usage/input_tokens_details/cached_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or(0)
                    .min(input);
                let output = raw_response
                    .pointer("/usage/output_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let has_tools = matches!(&message, ModelMessage::Assistant { tool_calls, .. } if !tool_calls.is_empty());
                let finish_reason = if response_hit_output_limit(&raw_response) {
                    "length"
                } else if has_tools {
                    "tool_calls"
                } else {
                    "stop"
                };
                Ok(ModelTurnResult {
                    message,
                    finish_reason: finish_reason.into(),
                    usage: ProviderCallUsage {
                        duration_ms: crate::runtime::elapsed_millis_u64(started),
                        input_tokens: input - cached,
                        cached_input_tokens: cached,
                        output_tokens: output,
                        total_tokens: input.saturating_add(output),
                        context_tokens: Some(input),
                        context_window_tokens: context_window,
                        cost_basis: if api_key {
                            CostBasis::Unavailable
                        } else {
                            CostBasis::SubscriptionEquivalent
                        },
                        ..Default::default()
                    },
                    raw_response,
                    trace,
                })
            }
            Err(error) => {
                let message = if trace.invocation.executable == API_ENDPOINT {
                    error
                        .to_string()
                        .replace("Codex subscription", "OpenAI API")
                } else {
                    error.to_string()
                };
                trace.exit_status = Some(1);
                trace.stderr = message.clone();
                Err(ProviderCallError {
                    message,
                    trace: Box::new(trace),
                    session_id: None,
                    // The subscription call fails through anyhow, so the
                    // transport cause is still reachable in its chain.
                    kind: crate::provider::classify_provider_error(&error),
                })
            }
        }
    }

    fn request_body_for_account(
        &self,
        request: &mut ModelTurnRequest,
        expected_account: &str,
    ) -> Result<Value> {
        for message in &mut request.messages {
            if let ModelMessage::Assistant { provider_state, .. } = message
                && let Some(ModelProviderState::OpenAiResponses {
                    account_identity, ..
                }) = provider_state
                && account_identity.as_deref() != Some(expected_account)
            {
                *provider_state = None;
            }
        }
        self.request_body(request)
    }

    fn request_body(&self, request: &ModelTurnRequest) -> Result<Value> {
        let mut input = Vec::new();
        let mut instructions = Vec::new();
        for message in &request.messages {
            match message {
                ModelMessage::System { content } => instructions.push(content.as_str()),
                ModelMessage::User {
                    content,
                    attachments,
                } => {
                    let mut blocks = vec![json!({"type": "input_text", "text": content})];
                    for attachment in attachments {
                        ensure!(
                            attachment.media_type.starts_with("image/"),
                            "Codex model attachment must be an image"
                        );
                        blocks.push(
                            json!({"type": "input_image", "image_url": format!("data:{};base64,{}",
                            attachment.media_type, attachment.data_base64)}),
                        );
                    }
                    input.push(json!({"role": "user", "content": blocks}));
                }
                ModelMessage::Assistant {
                    provider_state: Some(ModelProviderState::OpenAiResponses { output, .. }),
                    ..
                } => {
                    input.extend(output.iter().cloned());
                }
                ModelMessage::Assistant {
                    content,
                    tool_calls,
                    ..
                } => {
                    if let Some(content) = content {
                        input.push(json!({"role": "assistant", "content": [{"type": "output_text", "text": content}]}));
                    }
                    for call in tool_calls {
                        ensure!(
                            call.kind == "function",
                            "unsupported Codex model history tool type"
                        );
                        input.push(json!({"type": "function_call", "call_id": call.id,
                            "name": call.function.name, "arguments": call.function.arguments}));
                    }
                }
                ModelMessage::Tool {
                    tool_call_id,
                    content,
                    attachments,
                } => input.push(function_call_output(tool_call_id, content, attachments)?),
            }
        }
        let tools: Vec<_> = request
            .tools
            .iter()
            .map(|tool| {
                json!({
                    "type": "function", "name": tool.name, "description": tool.description,
                    "parameters": tool.input_schema, "strict": false
                })
            })
            .collect();
        let mut body = json!({"model": self.model, "instructions": instructions.join("\n\n"),
            "input": input, "tools": tools, "tool_choice": "auto", "parallel_tool_calls": true,
            "reasoning": {"effort": self.effort, "summary": "auto"},
            "store": false, "stream": true, "include": ["reasoning.encrypted_content"]});
        if request.fast {
            body["service_tier"] = json!("priority");
        }
        if let Some(key) = &request.prompt_cache_key {
            body["prompt_cache_key"] = json!(key);
        }
        if let Some(schema) = &request.output_schema {
            body["text"] = json!({"format": {"type": "json_schema", "name": "borg_response", "strict": true, "schema": schema}});
        }
        Ok(body)
    }

    async fn send(
        &self,
        client: &reqwest::Client,
        endpoint: &str,
        access: &mut SubscriptionAccess,
        expected_account: &str,
        request: &ModelTurnRequest,
        body: &Value,
    ) -> Result<reqwest::Response> {
        let routing_hint = if request.fast {
            format!("model={};tier=priority", self.model)
        } else {
            format!("model={}", self.model)
        };
        let mut http = client
            .post(endpoint)
            .header("originator", "borg")
            .header("x-codex-routing-hint", routing_hint)
            .header("Accept", "text/event-stream")
            .json(body);
        if let Some(id) = &request.session_id {
            http = http.header("session_id", id);
        }
        if let Some(id) = &request.request_id {
            http = http.header("X-Client-Request-Id", id);
        }
        let rejected_token = access.token.clone();
        access
            .send_with_recovery(
                apply_provider_request_timeout(http),
                expected_account,
                SubscriptionAccess::read(Some(rejected_token)),
            )
            .await
    }

    async fn read_stream(
        &self,
        response: reqwest::Response,
        progress: Option<&UnboundedSender<ProviderProgress>>,
    ) -> Result<(ModelMessage, Value)> {
        let response = check_subscription_response(response).await?;
        let mut stream = response.bytes_stream();
        let mut buffer = Vec::new();
        let mut data = String::new();
        let mut total = 0usize;
        let mut state = ResponseState::default();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("Codex model stream disconnected")?;
            total = total.saturating_add(chunk.len());
            ensure!(
                total <= MAX_STREAM_BYTES,
                "Codex model stream exceeds size limit"
            );
            buffer.extend_from_slice(&chunk);
            while let Some(end) = buffer.iter().position(|byte| *byte == b'\n') {
                let line = String::from_utf8(buffer.drain(..=end).collect())
                    .context("invalid Codex stream encoding")?;
                let line = line.trim_end_matches(['\r', '\n']);
                if line.is_empty() {
                    if !data.is_empty() {
                        let event: Value = serde_json::from_str(&data)
                            .context("invalid Codex model event JSON")?;
                        data.clear();
                        if let Some(response) =
                            state.event(&event, progress, &self.model, &self.effort)?
                        {
                            return state.finish(response);
                        }
                    }
                } else if let Some(value) = line.strip_prefix("data:") {
                    data.push_str(value.strip_prefix(' ').unwrap_or(value));
                    data.push('\n');
                }
                ensure!(
                    data.len() <= MAX_EVENT_BYTES,
                    "Codex model event exceeds size limit"
                );
            }
            ensure!(
                buffer.len() <= MAX_EVENT_BYTES,
                "Codex model event exceeds size limit"
            );
        }
        bail!("Codex model stream ended before response.completed; no tools were executed")
    }
}

async fn check_subscription_response(response: reqwest::Response) -> Result<reqwest::Response> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = tokio::time::timeout(
        Duration::from_secs(5),
        super::read_provider_error_response_text(response),
    )
    .await;
    let body = match body {
        Ok(Ok(body)) => serde_json::from_str::<Value>(&body).unwrap_or(Value::Null),
        _ => Value::Null,
    };
    bail!(subscription_failure_message(
        body.get("error"),
        Some(status),
        retry_after.as_deref()
    ))
}

fn subscription_failure_message(
    error: Option<&Value>,
    status: Option<reqwest::StatusCode>,
    retry_after: Option<&str>,
) -> String {
    let error = error.unwrap_or(&Value::Null);
    let codes = [error["code"].as_str(), error["type"].as_str()];
    let limited = status == Some(reqwest::StatusCode::TOO_MANY_REQUESTS)
        || codes.iter().any(|code| {
            matches!(
                code,
                Some("usage_limit_reached" | "rate_limit_exceeded" | "insufficient_quota")
            )
        });
    let mut message = if limited {
        "Codex subscription usage or rate limit reached.".to_string()
    } else if codes.contains(&Some("context_length_exceeded")) {
        "Codex context limit reached; compact the conversation before trying again.".to_string()
    } else if status == Some(reqwest::StatusCode::UNAUTHORIZED) {
        "Codex subscription authentication was rejected after recovery; reconnect Codex."
            .to_string()
    } else if status == Some(reqwest::StatusCode::FORBIDDEN) {
        "Codex subscription access was denied; check account and model access.".to_string()
    } else {
        "Codex subscription response did not complete.".to_string()
    };
    if let Some(status) = status {
        message.push_str(&format!(" HTTP {}.", status.as_u16()));
    }
    if limited {
        let retry_date = retry_after
            .and_then(|value| chrono::DateTime::parse_from_rfc2822(value).ok())
            .map(|time| time.with_timezone(&chrono::Utc));
        if let Some(seconds) = retry_after
            .and_then(|value| value.trim().parse::<u64>().ok())
            .or_else(|| {
                if retry_date.is_none() {
                    error["resets_in_seconds"].as_u64()
                } else {
                    None
                }
            })
        {
            message.push_str(&format!(
                " Provider-reported retry delay: {seconds} seconds."
            ));
        } else if let Some(reset) = retry_date.or_else(|| {
            error["resets_at"]
                .as_i64()
                .filter(|value| *value > 0)
                .and_then(|value| chrono::DateTime::from_timestamp(value, 0))
        }) {
            message.push_str(&format!(
                " Provider-reported reset: {}.",
                reset.format("%Y-%m-%d %H:%M:%S UTC")
            ));
        }
    }
    message
        .push_str(" No billing fallback was attempted; no tools from this response were executed.");
    message
}

#[derive(Default)]
struct ResponseState {
    calls: HashMap<String, (String, String, String)>,
    generating: HashSet<String>,
    described: HashSet<String>,
    action_parsers: HashMap<String, StreamedToolAction>,
    output: BTreeMap<u64, Value>,
    reasoning: String,
    reasoning_part: Option<(Option<String>, Option<u64>)>,
}

impl ResponseState {
    fn event(
        &mut self,
        event: &Value,
        progress: Option<&UnboundedSender<ProviderProgress>>,
        model: &str,
        effort: &str,
    ) -> Result<Option<Value>> {
        let emit = |value| {
            if let Some(sender) = progress {
                let _ = sender.send(value);
            }
        };
        match event["type"].as_str().unwrap_or_default() {
            "response.output_item.added" if event["item"]["type"] == "function_call" => {
                let item = &event["item"];
                let id = item["id"].as_str().context("Codex tool item has no id")?;
                let call_id = item["call_id"]
                    .as_str()
                    .context("Codex tool item has no call id")?;
                let name = item["name"].as_str().unwrap_or_default();
                let arguments = item["arguments"].as_str().unwrap_or_default();
                self.calls
                    .insert(id.into(), (call_id.into(), name.into(), arguments.into()));
                if (!name.is_empty() || !arguments.is_empty())
                    && self.generating.insert(call_id.into())
                {
                    emit(ProviderProgress::ToolCallGenerating {
                        id: Some(call_id.into()),
                    });
                }
                if !name.is_empty() {
                    emit(ProviderProgress::ToolCallStarted {
                        id: call_id.into(),
                        name: name.into(),
                        input: Value::Null,
                    });
                }
                if !self.described.contains(call_id)
                    && let Some(action) = self
                        .action_parsers
                        .entry(call_id.into())
                        .or_default()
                        .observe(arguments)
                {
                    self.described.insert(call_id.into());
                    emit(ProviderProgress::ToolCallAction {
                        id: Some(call_id.into()),
                        action,
                    });
                }
            }
            "response.function_call_arguments.delta" => {
                let delta = event["delta"].as_str().unwrap_or_default();
                if !delta.is_empty() {
                    let id = event["item_id"]
                        .as_str()
                        .context("Codex tool delta has no item id")?;
                    let (call_id, _, arguments) = self
                        .calls
                        .get_mut(id)
                        .context("Codex tool delta has no matching item")?;
                    if self.generating.insert(call_id.clone()) {
                        emit(ProviderProgress::ToolCallGenerating {
                            id: Some(call_id.clone()),
                        });
                    } else {
                        emit(ProviderProgress::ToolCallInputDelta {
                            id: Some(call_id.clone()),
                        });
                    }
                    arguments.push_str(delta);
                    if !self.described.contains(call_id)
                        && let Some(action) = self
                            .action_parsers
                            .entry(call_id.clone())
                            .or_default()
                            .observe(arguments)
                    {
                        self.described.insert(call_id.clone());
                        emit(ProviderProgress::ToolCallAction {
                            id: Some(call_id.clone()),
                            action,
                        });
                    }
                }
            }
            "response.output_text.delta" => {
                if let Some(text) = event["delta"].as_str().filter(|s| !s.is_empty()) {
                    emit(ProviderProgress::Bytes {
                        stream: ProviderProgressStream::Stdout,
                        chunk: text.as_bytes().to_vec(),
                    });
                }
            }
            "response.reasoning_summary_text.delta" => {
                if let Some(text) = event["delta"].as_str().filter(|s| !s.is_empty()) {
                    let part = (
                        event["item_id"].as_str().map(str::to_owned),
                        event["summary_index"].as_u64(),
                    );
                    let text = if self
                        .reasoning_part
                        .as_ref()
                        .is_some_and(|last| last != &part)
                        && !self.reasoning.ends_with("\n")
                        && !text.starts_with("\n")
                    {
                        format!("\n{text}")
                    } else {
                        text.to_owned()
                    };
                    self.reasoning_part = Some(part);
                    self.reasoning.push_str(&text);
                    emit(ProviderProgress::ProviderEvent {
                        kind: "reasoning_delta".into(),
                        payload: json!({"text": text}),
                        raw_payload: Box::new(None),
                        stream_channel: Some("reasoning".into()),
                        content_text: Some(text),
                        provider_item_id: event["item_id"].as_str().map(str::to_owned),
                        tool_use_id: None,
                        tool_name: None,
                        model: Some(model.into()),
                        effort: Some(effort.into()),
                    });
                }
            }
            "response.output_item.done" => {
                let index = event["output_index"]
                    .as_u64()
                    .context("Codex output item has no index")?;
                self.output.insert(index, event["item"].clone());
            }
            "response.completed" => return Ok(Some(event["response"].clone())),
            // A reply cut at `max_output_tokens` still carries its output
            // items; it is surfaced as a `length` finish so the harness can
            // keep the text and continue, instead of discarding it.
            "response.incomplete" if response_hit_output_limit(&event["response"]) => {
                return Ok(Some(event["response"].clone()));
            }
            "response.failed" | "response.incomplete" | "error" => {
                bail!(subscription_failure_message(
                    event
                        .pointer("/response/error")
                        .or_else(|| event.get("error")),
                    None,
                    None
                ))
            }
            _ => {}
        }
        Ok(None)
    }

    fn finish(self, mut response: Value) -> Result<(ModelMessage, Value)> {
        ensure!(
            response["status"] == "completed" || response_hit_output_limit(&response),
            "Codex model response was not completed"
        );
        let output = response["output"]
            .as_array()
            .filter(|items| !items.is_empty())
            .cloned()
            .unwrap_or_else(|| self.output.into_values().collect());
        let mut content = String::new();
        let mut calls = Vec::new();
        let mut ids = HashSet::new();
        for item in &output {
            match item["type"].as_str() {
                Some("message") => {
                    if let Some(blocks) = item["content"].as_array() {
                        for block in blocks {
                            if let Some(text) =
                                block["text"].as_str().or_else(|| block["refusal"].as_str())
                            {
                                content.push_str(text);
                            }
                        }
                    }
                }
                Some("function_call") => {
                    let id = item["call_id"]
                        .as_str()
                        .filter(|s| !s.is_empty())
                        .context("Codex tool has no call id")?;
                    ensure!(ids.insert(id), "Codex returned duplicate tool call ids");
                    let name = item["name"]
                        .as_str()
                        .filter(|s| !s.is_empty())
                        .context("Codex tool has no name")?;
                    let arguments = item["arguments"]
                        .as_str()
                        .context("Codex tool has no arguments")?;
                    let parsed: Value = serde_json::from_str(arguments)
                        .context("Codex tool arguments are incomplete")?;
                    ensure!(parsed.is_object(), "Codex tool arguments must be an object");
                    calls.push(ModelToolCall::function(
                        id.into(),
                        name.into(),
                        arguments.into(),
                    ));
                }
                Some("reasoning") => {}
                _ => bail!("Codex returned an unsupported output item; no tools were executed"),
            }
        }
        ensure!(
            !content.is_empty() || !calls.is_empty(),
            "Codex returned no answer or tool calls"
        );
        response["output"] = json!(output);
        Ok((
            ModelMessage::Assistant {
                content: (!content.is_empty()).then_some(content),
                reasoning_content: (!self.reasoning.is_empty()).then_some(self.reasoning),
                reasoning_details: None,
                provider_state: Some(ModelProviderState::OpenAiResponses {
                    output,
                    account_identity: None,
                }),
                tool_calls: calls,
            },
            response,
        ))
    }
}

/// A tool result on the Responses wire. Text-only results stay a plain
/// string; results carrying images become content blocks so the model sees
/// the pixels (screenshots, rendered charts) rather than a base64 dump.
fn function_call_output(
    tool_call_id: &str,
    content: &str,
    attachments: &[borg_core::ModelInputAttachment],
) -> Result<Value> {
    if attachments.is_empty() {
        return Ok(json!({
            "type": "function_call_output", "call_id": tool_call_id, "output": content
        }));
    }
    let mut blocks = vec![json!({"type": "input_text", "text": content})];
    for attachment in attachments {
        ensure!(
            attachment.media_type.starts_with("image/"),
            "Codex model tool attachment must be an image"
        );
        blocks.push(json!({
            "type": "input_image",
            "image_url": format!("data:{};base64,{}", attachment.media_type, attachment.data_base64)
        }));
    }
    Ok(json!({
        "type": "function_call_output", "call_id": tool_call_id, "output": blocks
    }))
}

fn response_hit_output_limit(response: &Value) -> bool {
    response["status"] == "incomplete"
        && response
            .pointer("/incomplete_details/reason")
            .and_then(Value::as_str)
            == Some("max_output_tokens")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::{mpsc, oneshot};

    #[test]
    fn account_switch_replays_borg_history_without_foreign_continuation() {
        let provider = CodexModelProvider {
            model: "test-model".into(),
            effort: "low".into(),
        };
        for origin in [None, Some("account-a"), Some("account-b")] {
            let output =
                vec![json!({"type":"reasoning", "encrypted_content":"opaque-account-state"})];
            let mut request = ModelTurnRequest {
                fast: false,
                request_id: None,
                session_id: Some("same-borg-session".into()),
                prompt_cache_key: None,
                tools: vec![],
                output_schema: None,
                messages: vec![
                    ModelMessage::user("keep the task"),
                    ModelMessage::Assistant {
                        content: Some("running the tool".into()),
                        reasoning_content: None,
                        reasoning_details: None,
                        provider_state: Some(ModelProviderState::OpenAiResponses {
                            output: output.clone(),
                            account_identity: origin.map(str::to_owned),
                        }),
                        tool_calls: vec![ModelToolCall::function(
                            "call-1".into(),
                            "exec".into(),
                            "{}".into(),
                        )],
                    },
                    ModelMessage::tool("call-1", "the result"),
                ],
            };
            let body = provider
                .request_body_for_account(&mut request, "account-b")
                .unwrap();
            let input = body["input"].as_array().unwrap();
            assert_eq!(input[0]["content"][0]["text"], "keep the task");
            assert_eq!(input.last().unwrap()["output"], "the result");
            if origin == Some("account-b") {
                assert_eq!(input[1], output[0]);
            } else {
                assert_eq!(input[1]["content"][0]["text"], "running the tool");
                assert_eq!(input[2]["call_id"], "call-1");
                assert_eq!(input[2]["name"], "exec");
                assert!(!body.to_string().contains("opaque-account-state"));
            }
        }
    }

    #[tokio::test]
    async fn api_billing_uses_api_credentials_without_subscription_recovery() {
        let mut access = SubscriptionAccess {
            token: "test-api-key".into(),
            account_id: String::new(),
        };
        assert_eq!(access.endpoint(), "https://api.openai.com/v1/responses");
        let identity = access.identity();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/responses", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                let mut bytes = [0; 1024];
                let size = socket.read(&mut bytes).await.unwrap();
                assert!(size > 0);
                request.extend_from_slice(&bytes[..size]);
            }
            let request = String::from_utf8(request).unwrap().to_lowercase();
            assert!(request.contains("authorization: bearer test-api-key\r\n"));
            assert!(!request.contains("chatgpt-account-id"));
            socket
                .write_all(
                    b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
        });
        let response = access
            .send_with_recovery(
                reqwest::Client::new()
                    .get(endpoint)
                    .timeout(Duration::from_secs(2)),
                &identity,
                async { panic!("API authentication must not fall back to a ChatGPT subscription") },
            )
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn authentication_recovery_retries_once_without_crossing_accounts() {
        tokio::time::timeout(Duration::from_secs(10), async {
            for (statuses, recovery) in [
                (vec![200], "unused"),
                (vec![403], "unused"),
                (vec![401, 200], "same"),
                (vec![401, 401], "same"),
                (vec![401], "changed"),
                (vec![401], "failed"),
            ] {
                const BODY: &str = "synthetic-private-context";
                let expected_status = *statuses.last().unwrap();
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let endpoint = format!("http://{}/responses", listener.local_addr().unwrap());
                let server = tokio::spawn(async move {
                    for (index, status) in statuses.into_iter().enumerate() {
                        let (mut socket, _) = listener.accept().await.unwrap();
                        let mut request = Vec::new();
                        while !request.ends_with(BODY.as_bytes()) {
                            let mut bytes = [0; 1024];
                            let count = socket.read(&mut bytes).await.unwrap();
                            assert!(count > 0);
                            request.extend_from_slice(&bytes[..count]);
                        }
                        let request = String::from_utf8(request).unwrap();
                        let headers = request.split("\r\n\r\n").next().unwrap().to_lowercase();
                        assert!(headers.starts_with("post /responses http/1.1\r\n"));
                        assert!(headers.lines().any(|line| line == "chatgpt-account-id: account-a"));
                        let token = if index == 0 { "old-token" } else { "new-token" };
                        assert!(headers.lines().any(|line| line == format!("authorization: bearer {token}")));
                        assert_eq!(request.split("\r\n\r\n").nth(1), Some(BODY));
                        socket.write_all(format!("HTTP/1.1 {status} Probe\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
                    }
                    listener
                });
                let mut access = SubscriptionAccess { token: "old-token".into(), account_id: "account-a".into() };
                let account = access.identity();
                let refreshes = std::sync::atomic::AtomicUsize::new(0);
                let result = access.send_with_recovery(
                    reqwest::Client::new().post(endpoint).body(BODY).timeout(Duration::from_secs(1)),
                    &account,
                    async {
                        refreshes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        ensure!(recovery != "failed", "synthetic refresh failure");
                        Ok(SubscriptionAccess {
                            token: "new-token".into(),
                            account_id: if recovery == "changed" { "account-b" } else { "account-a" }.into(),
                        })
                    },
                ).await;
                assert_eq!(refreshes.load(std::sync::atomic::Ordering::SeqCst), usize::from(recovery != "unused"));
                if matches!(recovery, "changed" | "failed") {
                    assert!(result.is_err());
                    assert_eq!(access.token, "old-token");
                } else {
                    assert_eq!(result.unwrap().status().as_u16(), expected_status);
                }
                assert_eq!(access.identity(), account);
                let listener = server.await.unwrap();
                assert!(tokio::time::timeout(Duration::from_millis(25), listener.accept()).await.is_err(),
                    "no extra retry or changed-account request may reach the endpoint");
            }
        }).await.expect("authentication recovery must remain bounded");
    }

    #[tokio::test]
    async fn quota_rejection_reports_retry_delay_without_generation_or_private_details() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/responses", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.windows(4).any(|part| part == b"\r\n\r\n") {
                let mut bytes = [0; 1024];
                let count = socket.read(&mut bytes).await.unwrap();
                assert!(count > 0);
                request.extend_from_slice(&bytes[..count]);
            }
            socket.write_all(b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 12\r\nConnection: close\r\n\r\n{\"error\":{\"type\":\"usage_limit_reached\",\"message\":\"private-token private-account\",\"resets_in_seconds\":99}}").await.unwrap();
        });
        let response = reqwest::Client::new().get(endpoint).send().await.unwrap();
        let (progress, mut events) = mpsc::unbounded_channel();
        let provider = CodexModelProvider {
            model: "test-model".into(),
            effort: "low".into(),
        };
        let error = provider
            .read_stream(response, Some(&progress))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("limit reached") && error.contains("12 seconds"));
        assert!(!error.contains("private-") && !error.contains("99 seconds"));
        assert!(events.try_recv().is_err());
        server.await.unwrap();
    }

    #[test]
    fn reasoning_summary_boundaries_preserve_streamed_fragments() {
        let mut state = ResponseState::default();
        let (tx, mut rx) = mpsc::unbounded_channel();
        for (item, part, text) in [
            ("first", 0, "Running "),
            ("first", 0, "validation"),
            ("first", 1, "Reviewing the diff"),
            ("second", 0, "Checking replay"),
        ] {
            state
                .event(
                    &json!({"type":"response.reasoning_summary_text.delta",
                "item_id":item,"summary_index":part,"delta":text}),
                    Some(&tx),
                    "model",
                    "low",
                )
                .unwrap();
        }
        let expected = "Running validation\nReviewing the diff\nChecking replay";
        assert_eq!(state.reasoning, expected);
        let mut streamed = String::new();
        while let Ok(event) = rx.try_recv() {
            if let ProviderProgress::ProviderEvent {
                content_text: Some(text),
                ..
            } = event
            {
                streamed.push_str(&text);
            }
        }
        assert_eq!(streamed, expected);
    }

    #[test]
    fn streamed_limits_abort_partial_calls_and_expose_only_structured_reset_details() {
        let (progress, mut events) = mpsc::unbounded_channel();
        let mut state = ResponseState::default();
        for event in [
            json!({"type":"response.output_item.added","item":{"type":"function_call","id":"item","call_id":"call","name":"","arguments":""}}),
            json!({"type":"response.function_call_arguments.delta","item_id":"item","delta":"{"}),
        ] {
            assert!(
                state
                    .event(&event, Some(&progress), "test-model", "low")
                    .unwrap()
                    .is_none()
            );
        }
        assert!(matches!(
            events.try_recv().unwrap(),
            ProviderProgress::ToolCallGenerating { .. }
        ));
        let failure = json!({"type":"response.failed","response":{"error":{
            "code":"rate_limit_exceeded", "message":"private-account private-token", "resets_at":1893456000
        }}});
        let error = state
            .event(&failure, Some(&progress), "test-model", "low")
            .unwrap_err()
            .to_string();
        assert!(error.contains("limit reached") && error.contains("2030-01-01 00:00:00 UTC"));
        assert!(!error.contains("private-"));
        assert!(events.try_recv().is_err());
        let context_error = subscription_failure_message(
            Some(&json!({"code":"context_length_exceeded"})),
            None,
            None,
        );
        assert!(context_error.contains("compact the conversation"));
        let malformed = subscription_failure_message(
            Some(&json!({"code":"private-token", "message":"private-account"})),
            None,
            Some("private-header"),
        );
        assert!(!malformed.contains("private-"));
        let dated = subscription_failure_message(
            Some(&json!({"resets_in_seconds":99})),
            Some(reqwest::StatusCode::TOO_MANY_REQUESTS),
            Some("Wed, 21 Oct 2015 07:28:00 GMT"),
        );
        assert!(dated.contains("2015-10-21 07:28:00 UTC") && !dated.contains("99 seconds"));
    }

    #[test]
    fn native_tool_input_progress_ignores_empty_and_refreshes_after_generation() {
        let (progress, mut events) = mpsc::unbounded_channel();
        let mut state = ResponseState::default();
        for event in [
            json!({"type":"response.output_item.added","item":{"type":"function_call","id":"item","call_id":"call","name":"","arguments":""}}),
            json!({"type":"response.function_call_arguments.delta","item_id":"item","delta":""}),
            json!({"type":"response.function_call_arguments.delta","item_id":"item","delta":"{"}),
            json!({"type":"response.function_call_arguments.delta","item_id":"item","delta":"\"path\":"}),
        ] {
            state
                .event(&event, Some(&progress), "test-model", "low")
                .unwrap();
        }
        assert!(matches!(
            events.try_recv(),
            Ok(ProviderProgress::ToolCallGenerating { id: Some(id) }) if id == "call"
        ));
        assert!(matches!(
            events.try_recv(),
            Ok(ProviderProgress::ToolCallInputDelta { id: Some(id) }) if id == "call"
        ));
        assert!(events.try_recv().is_err());
        state.event(
            &json!({"type":"response.function_call_arguments.delta","item_id":"item","delta":"\"file\",\"action\":\"edit\",\"body\":\""}),
            Some(&progress), "test-model", "low",
        ).unwrap();
        assert!(matches!(
            events.try_recv(),
            Ok(ProviderProgress::ToolCallInputDelta { .. })
        ));
        assert!(
            matches!(events.try_recv(), Ok(ProviderProgress::ToolCallAction { id: Some(id), action })
            if id == "call" && action == "edit")
        );
        state.event(
            &json!({"type":"response.output_item.added","item":{"type":"function_call","id":"next","call_id":"next-call","name":"","arguments":"{\"action\":\"read\","}}),
            Some(&progress), "test-model", "low",
        ).unwrap();
        assert!(matches!(
            events.try_recv(),
            Ok(ProviderProgress::ToolCallGenerating { .. })
        ));
        assert!(
            matches!(events.try_recv(), Ok(ProviderProgress::ToolCallAction { id: Some(id), action })
            if id == "next-call" && action == "read")
        );
        assert!(events.try_recv().is_err());
    }

    #[test]
    fn fast_mode_uses_catalog_capabilities_and_explicit_priority_routing() {
        let provider = CodexModelProvider {
            model: "test-model".into(),
            effort: "low".into(),
        };
        let mut request = ModelTurnRequest {
            fast: false,
            request_id: None,
            session_id: None,
            prompt_cache_key: None,
            messages: vec![ModelMessage::user("test")],
            tools: Vec::new(),
            output_schema: None,
        };
        assert!(
            provider
                .request_body(&request)
                .unwrap()
                .get("service_tier")
                .is_none()
        );
        request.fast = true;
        assert_eq!(
            provider.request_body(&request).unwrap()["service_tier"],
            "priority"
        );
        for (tiers, legacy, expected) in [
            (json!([]), json!([]), false),
            (json!([{"id":"flex"}]), json!([]), false),
            (json!([{"id":"priority"}]), json!([]), true),
            (json!([]), json!(["fast"]), true),
        ] {
            let capabilities: ModelCapabilities = serde_json::from_value(json!({
                "slug":"test-model", "supported_reasoning_levels":[{"effort":"low"}],
                "service_tiers":tiers, "additional_speed_tiers":legacy
            }))
            .unwrap();
            assert_eq!(capabilities.supports_fast(), expected);
        }
    }

    #[test]
    fn catalog_context_limits_preserve_provider_headroom_without_overflow() {
        let mut metadata = json!({
            "slug": "test-model", "supported_reasoning_levels": [{"effort": "low"}],
            "context_window": 200_000, "max_context_window": 400_000
        });
        let usable = |metadata: &Value| {
            serde_json::from_value::<ModelCapabilities>(metadata.clone())?.usable_context_window()
        };
        assert_eq!(usable(&metadata).unwrap(), 190_000);
        metadata["effective_context_window_percent"] = json!(80);
        assert_eq!(usable(&metadata).unwrap(), 160_000);
        metadata["context_window"] = Value::Null;
        assert_eq!(usable(&metadata).unwrap(), 320_000);
        metadata["max_context_window"] = json!(u64::MAX);
        metadata["effective_context_window_percent"] = json!(100);
        assert_eq!(usable(&metadata).unwrap(), u64::MAX);
        for percent in [0, 101] {
            metadata["effective_context_window_percent"] = json!(percent);
            assert!(usable(&metadata).is_err());
        }
        metadata["effective_context_window_percent"] = json!(95);
        for window in [Value::Null, json!(0), json!(-1)] {
            metadata["max_context_window"] = window;
            assert!(usable(&metadata).is_err());
        }
    }

    #[tokio::test]
    async fn account_mismatch_is_rejected_before_connecting() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/responses", listener.local_addr().unwrap());
        let provider = CodexModelProvider {
            model: "gpt-6-astra".into(),
            effort: "low".into(),
        };
        let original = SubscriptionAccess {
            token: "old-token".into(),
            account_id: "account-a".into(),
        };
        let mut changed = SubscriptionAccess {
            token: "new-token".into(),
            account_id: "account-b".into(),
        };
        let refreshed = SubscriptionAccess {
            token: "refreshed-token".into(),
            account_id: "account-a".into(),
        };
        assert_eq!(original.identity(), refreshed.identity());
        let request = ModelTurnRequest {
            fast: false,
            request_id: None,
            session_id: Some("session".into()),
            prompt_cache_key: None,
            messages: vec![ModelMessage::user("private context")],
            tools: Vec::new(),
            output_schema: None,
        };
        let error = provider
            .send(
                &reqwest::Client::new(),
                &endpoint,
                &mut changed,
                &original.identity(),
                &request,
                &provider.request_body(&request).unwrap(),
            )
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("credentials changed during this turn")
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(25), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn first_character_precedes_complete_arguments_and_native_output_survives_replay() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}/responses", listener.local_addr().unwrap());
            let (first_tx, first_rx) = oneshot::channel();
            let (finish_tx, finish_rx) = oneshot::channel();
            let native_output = json!([
                {"type":"reasoning","id":"reason","encrypted_content":"opaque","summary":[]},
                {"type":"message","id":"comment","role":"assistant","phase":"commentary",
                    "content":[{"type":"output_text","text":"Checking."}]},
                {"type":"function_call","id":"item","call_id":"call","name":"inspect",
                    "arguments":"{\"action\":\"inspect\"}"}
            ]);
            let expected_output = native_output.clone();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let (end, len) = loop {
                    let mut bytes = [0; 4096];
                    let n = socket.read(&mut bytes).await.unwrap();
                    assert!(n > 0);
                    request.extend_from_slice(&bytes[..n]);
                    if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&request[..end]).to_lowercase();
                        assert!(headers.contains("authorization: bearer test-token"));
                        assert!(headers.contains("chatgpt-account-id: test-account"));
                        assert!(headers.contains("session_id: session"));
                        assert!(headers.contains("x-codex-routing-hint: model=gpt-6-astra;tier=priority"));
                        let len: usize = headers.lines().find_map(|line| line.strip_prefix("content-length: ")).unwrap().parse().unwrap();
                        break (end + 4, len);
                    }
                };
                while request.len() < end + len {
                    let mut bytes = [0; 4096];
                    let n = socket.read(&mut bytes).await.unwrap();
                    assert!(n > 0);
                    request.extend_from_slice(&bytes[..n]);
                }
                let body: Value = serde_json::from_slice(&request[end..end + len]).unwrap();
                assert_eq!(body["store"], false);
                assert_eq!(body["service_tier"], "priority");
                assert_eq!(body["prompt_cache_key"], "cache");
                assert_eq!(body["tools"][0]["name"], "inspect");
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n").await.unwrap();
                // Metadata and reasoning cannot cause speculative generation.
                for event in [
                    json!({"type":"response.output_item.added","item":{"type":"function_call","id":"item","call_id":"call","name":"","arguments":""}}),
                    json!({"type":"response.function_call_arguments.delta","item_id":"item","delta":""}),
                    json!({"type":"response.reasoning_summary_text.delta","delta":"Thinking"})
                ] {
                    socket.write_all(format!("data: {event}\r\n\r\n").as_bytes()).await.unwrap();
                }
                first_rx.await.unwrap();
                let first = json!({"type":"response.function_call_arguments.delta","item_id":"item","delta":"{"});
                socket.write_all(format!("data: {first}\n\n").as_bytes()).await.unwrap();
                finish_rx.await.unwrap();
                for (index, item) in native_output.as_array().unwrap().iter().enumerate() {
                    let event = json!({"type":"response.output_item.done","output_index":index,"item":item});
                    socket.write_all(format!("data: {event}\n\n").as_bytes()).await.unwrap();
                }
                let end = json!({"type":"response.completed","response":{"status":"completed","output":[],
                    "usage":{"input_tokens":12,"output_tokens":3,"input_tokens_details":{"cached_tokens":4}}}});
                // Split the SSE frame across transport chunks as well.
                let frame = format!("data: {end}\n\n");
                socket.write_all(&frame.as_bytes()[..7]).await.unwrap();
                socket.write_all(&frame.as_bytes()[7..]).await.unwrap();
            });
            let provider = CodexModelProvider { model: "gpt-6-astra".into(), effort: "low".into() };
            let mut request = ModelTurnRequest { fast: true, request_id: Some("request".into()), session_id: Some("session".into()),
                prompt_cache_key: Some("cache".into()), messages: vec![ModelMessage::user("Inspect.")],
                tools: vec![super::super::ModelToolDefinition::new("inspect", "Inspect", json!({"type":"object"})).unwrap()], output_schema: None };
            let mut access = SubscriptionAccess { token: "test-token".into(), account_id: "test-account".into() };
            let account = access.identity();
            let response = provider.send(&reqwest::Client::new(), &endpoint,
                &mut access, &account,
                &request, &provider.request_body(&request).unwrap()).await.unwrap();
            let (tx, mut rx) = mpsc::unbounded_channel();
            let read = tokio::spawn(async move { provider.read_stream(response, Some(&tx)).await });
            assert!(matches!(rx.recv().await.unwrap(), ProviderProgress::ProviderEvent { kind, .. } if kind == "reasoning_delta"));
            assert!(rx.try_recv().is_err());
            first_tx.send(()).unwrap();
            assert!(matches!(rx.recv().await.unwrap(), ProviderProgress::ToolCallGenerating { id: Some(id) } if id == "call"));
            assert!(!read.is_finished(), "complete arguments must still be withheld");
            finish_tx.send(()).unwrap();
            let (message, _) = read.await.unwrap().unwrap();
            server.await.unwrap();
            // Simulate Borg's durable serialization before the next tool round.
            request.messages.push(serde_json::from_value(serde_json::to_value(&message).unwrap()).unwrap());
            request.messages.push(ModelMessage::Tool { tool_call_id: "call".into(), content: "ok".into(), attachments: Vec::new() });
            let provider = CodexModelProvider { model: "gpt-6-astra".into(), effort: "low".into() };
            let replay = provider.request_body(&request).unwrap();
            assert_eq!(&replay["input"].as_array().unwrap()[1..4], expected_output.as_array().unwrap());
            assert_eq!(replay["input"][4], json!({"type":"function_call_output","call_id":"call","output":"ok"}));
            assert_eq!(replay["prompt_cache_key"], "cache");
        }).await.expect("stream test timed out");
    }

    #[test]
    fn incomplete_or_duplicate_calls_never_become_executable_results() {
        let call =
            json!({"type":"function_call","call_id":"call","name":"inspect","arguments":"{}"});
        let mut partial = call.clone();
        partial["arguments"] = json!("{");
        for response in [
            json!({"status":"incomplete","output":[call.clone()]}),
            json!({"status":"incomplete","incomplete_details":{"reason":"content_filter"},"output":[call.clone()]}),
            json!({"status":"completed","output":[call.clone(),call.clone()]}),
            json!({"status":"completed","output":[partial.clone()]}),
            json!({"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[partial]}),
        ] {
            assert!(ResponseState::default().finish(response).is_err());
        }
    }

    #[test]
    fn tool_results_with_images_become_input_image_blocks() {
        let plain = function_call_output("call-1", "ok", &[]).unwrap();
        assert_eq!(plain["output"], "ok");
        let image = borg_core::ModelInputAttachment {
            media_type: "image/png".to_string(),
            data_base64: "AAAA".to_string(),
            filename: None,
        };
        let rich = function_call_output("call-1", "screenshot taken", std::slice::from_ref(&image))
            .unwrap();
        assert_eq!(rich["call_id"], "call-1");
        assert_eq!(rich["output"][0]["type"], "input_text");
        assert_eq!(rich["output"][1]["type"], "input_image");
        assert_eq!(rich["output"][1]["image_url"], "data:image/png;base64,AAAA");
        let mut not_image = image;
        not_image.media_type = "application/pdf".to_string();
        assert!(function_call_output("call-1", "x", &[not_image]).is_err());
    }

    #[test]
    fn output_limit_truncation_keeps_the_partial_text_as_a_length_finish() {
        let response = json!({
            "status": "incomplete",
            "incomplete_details": {"reason": "max_output_tokens"},
            "output": [{"type": "message", "content": [{"type": "output_text", "text": "half of the"}]}],
        });
        assert!(response_hit_output_limit(&response));
        let (message, raw) = ResponseState::default()
            .finish(response)
            .expect("a max_output_tokens response keeps its text");
        assert!(
            matches!(message, ModelMessage::Assistant { content: Some(text), .. } if text == "half of the")
        );
        assert!(response_hit_output_limit(&raw));
        assert!(!response_hit_output_limit(&json!({"status": "completed"})));
    }
}
