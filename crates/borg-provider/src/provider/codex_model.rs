//! Opt-in model-only subscription adapter. Codex manages access, never a turn
//! or a tool. Production chat routing stays on app-server until parity is proven.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use base64::Engine;
use borg_core::{CostBasis, ModelProviderState, ProviderCallUsage};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc::UnboundedSender;

use super::{
    ModelMessage, ModelToolCall, ModelTurnRequest, ModelTurnResult, ProviderAttemptTrace,
    ProviderCallError, ProviderInvocation, ProviderProgress, ProviderProgressStream,
    apply_provider_request_timeout, streamed_tool_action,
};

const ENDPOINT: &str = "https://chatgpt.com/backend-api/codex/responses";
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

// Never serialize or debug credentials. Refresh tokens remain in the original
// persistent Codex credential store, owned by Codex's authentication manager.
struct SubscriptionAccess {
    token: String,
    account_id: String,
}

impl SubscriptionAccess {
    fn identity(&self) -> String {
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
            let mut command = crate::provider_bin::codex_command().await?;
            let version = tokio::time::timeout(
                Duration::from_secs(10),
                command.arg("--version").kill_on_drop(true).output(),
            )
            .await
            .context("Codex version query timed out")?
            .context("Codex version query failed")?;
            ensure!(version.status.success(), "Codex version query failed");
            let version = String::from_utf8(version.stdout).context("invalid Codex version")?;
            let version = version
                .trim()
                .strip_prefix("codex-cli ")
                .and_then(|version| version.split('-').next())
                .filter(|version| {
                    version.split('.').count() == 3
                        && version.split('.').all(|part| part.parse::<u64>().is_ok())
                })
                .context("unrecognized Codex version for model catalog")?;
            let mut refreshed = false;
            let response = loop {
                let response = client
                    .get(MODELS_ENDPOINT)
                    .query(&[("client_version", version)])
                    .bearer_auth(&self.token)
                    .header("ChatGPT-Account-Id", &self.account_id)
                    .header("originator", "borg")
                    .timeout(Duration::from_secs(30))
                    .send()
                    .await
                    .context("Codex model catalog connection failed")?;
                if response.status() == reqwest::StatusCode::UNAUTHORIZED && !refreshed {
                    drop(response);
                    let access = Self::read(true).await?;
                    ensure!(
                        access.identity() == account,
                        "Codex account changed during authentication recovery; start a new session"
                    );
                    *self = access;
                    refreshed = true;
                    continue;
                }
                break response;
            };
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

    async fn read(refresh: bool) -> Result<Self> {
        let response = tokio::time::timeout(
            Duration::from_secs(60),
            super::chat_stream::codex_account_request(
                "getAuthStatus",
                json!({
                    "includeToken": true, "refreshToken": refresh
                }),
            ),
        )
        .await
        .context("Codex subscription authentication timed out")?
        .map_err(|_| {
            anyhow::anyhow!("Codex subscription authentication failed; reconnect Codex")
        })?;
        let auth = &response["result"];
        ensure!(
            matches!(
                auth["authMethod"].as_str(),
                Some("chatgpt" | "chatgptAuthTokens")
            ),
            "model-only Codex requires a ChatGPT subscription; no API-key fallback is allowed"
        );
        let token = auth["authToken"]
            .as_str()
            .filter(|s| !s.is_empty())
            .context("Codex did not provide subscription access; reconnect Codex")?
            .to_owned();
        let payload = token
            .split('.')
            .nth(1)
            .context("invalid Codex subscription token format")?;
        let claims: Value = serde_json::from_slice(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(payload)
                .map_err(|_| anyhow::anyhow!("invalid Codex subscription token encoding"))?,
        )
        .map_err(|_| anyhow::anyhow!("invalid Codex subscription token claims"))?;
        let account_id = claims
            .pointer("/https:~1~1api.openai.com~1auth/chatgpt_account_id")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .context("Codex subscription token has no account identity")?
            .to_owned();
        Ok(Self { token, account_id })
    }
}

impl CodexModelProvider {
    /// Non-secret subscription account identity, suitable for a host-owned binding.
    pub async fn account_identity() -> Result<String> {
        Ok(SubscriptionAccess::read(false).await?.identity())
    }

    /// The host must commit this identity before transmitting a durable session's context.
    pub async fn model_turn_for_account(
        &self,
        request: ModelTurnRequest,
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
            let body = self.request_body(&request)?;
            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(30))
                .build()?;
            let mut access = SubscriptionAccess::read(false).await?;
            ensure!(access.identity() == expected_account,
                "Codex account differs from this session's bound account; reconnect the original account or start a new session");
            let capabilities = access.model_capabilities(&client, &self.model).await?;
            ensure!(capabilities.supported_reasoning_levels.iter().any(|level| level.effort == self.effort),
                "selected effort is not supported by this Codex model");
            let context_window = capabilities.usable_context_window()?;
            ensure!(!request.fast || capabilities.supports_fast(),
                "fast mode is not supported by this Codex model");
            let mut response = self
                .send(
                    &client,
                    ENDPOINT,
                    &access,
                    expected_account,
                    &request,
                    &body,
                )
                .await?;
            if response.status() == reqwest::StatusCode::UNAUTHORIZED {
                // Retry only an unaccepted HTTP request, never a partial model
                // stream or completed tool side effect.
                drop(response);
                let refreshed = SubscriptionAccess::read(true).await?;
                ensure!(
                    refreshed.account_id == access.account_id,
                    "Codex account changed during authentication recovery; start a new session"
                );
                access = refreshed;
                response = self
                    .send(
                        &client,
                        ENDPOINT,
                        &access,
                        expected_account,
                        &request,
                        &body,
                    )
                    .await?;
            }
            let (message, response) = self.read_stream(response, progress.as_ref()).await?;
            Ok::<_, anyhow::Error>((message, response, context_window))
        }
        .await;
        match result {
            Ok((message, raw_response, context_window)) => {
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
                Ok(ModelTurnResult {
                    message,
                    finish_reason: if has_tools { "tool_calls" } else { "stop" }.into(),
                    usage: ProviderCallUsage {
                        duration_ms: crate::runtime::elapsed_millis_u64(started),
                        input_tokens: input - cached,
                        cached_input_tokens: cached,
                        output_tokens: output,
                        total_tokens: input.saturating_add(output),
                        context_tokens: Some(input),
                        context_window_tokens: Some(context_window),
                        cost_basis: CostBasis::SubscriptionEquivalent,
                        ..Default::default()
                    },
                    raw_response,
                    trace,
                })
            }
            Err(error) => {
                let message = error.to_string();
                trace.exit_status = Some(1);
                trace.stderr = message.clone();
                Err(ProviderCallError {
                    message,
                    trace: Box::new(trace),
                    session_id: None,
                })
            }
        }
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
                    provider_state: Some(ModelProviderState::OpenAiResponses { output }),
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
                } => input.push(json!({
                    "type": "function_call_output", "call_id": tool_call_id, "output": content
                })),
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
        access: &SubscriptionAccess,
        expected_account: &str,
        request: &ModelTurnRequest,
        body: &Value,
    ) -> Result<reqwest::Response> {
        ensure!(
            access.identity() == expected_account,
            "Codex account differs from this session's bound account; reconnect the original account or start a new session"
        );
        let mut http = client
            .post(endpoint)
            .bearer_auth(&access.token)
            .header("ChatGPT-Account-Id", &access.account_id)
            .header("originator", "borg")
            .header("Accept", "text/event-stream")
            .json(body);
        if let Some(id) = &request.session_id {
            http = http.header("session_id", id);
        }
        if let Some(id) = &request.request_id {
            http = http.header("X-Client-Request-Id", id);
        }
        apply_provider_request_timeout(http)
            .send()
            .await
            .context("Codex model connection failed")
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
    output: BTreeMap<u64, Value>,
    reasoning: String,
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
                    }
                    arguments.push_str(delta);
                    if !self.described.contains(call_id)
                        && let Some(action) = streamed_tool_action(arguments)
                    {
                        self.described.insert(call_id.clone());
                        emit(ProviderProgress::ToolCallAction {
                            id: call_id.clone(),
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
                    self.reasoning.push_str(text);
                    emit(ProviderProgress::ProviderEvent {
                        kind: "reasoning_delta".into(),
                        payload: json!({"text": text}),
                        raw_payload: Box::new(None),
                        stream_channel: Some("reasoning".into()),
                        content_text: Some(text.into()),
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
            response["status"] == "completed",
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
                provider_state: Some(ModelProviderState::OpenAiResponses { output }),
                tool_calls: calls,
            },
            response,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::{mpsc, oneshot};

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
        let changed = SubscriptionAccess {
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
                &changed,
                &original.identity(),
                &request,
                &provider.request_body(&request).unwrap(),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("bound account"));
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
            let access = SubscriptionAccess { token: "test-token".into(), account_id: "test-account".into() };
            let response = provider.send(&reqwest::Client::new(), &endpoint,
                &access, &access.identity(),
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
            request.messages.push(ModelMessage::Tool { tool_call_id: "call".into(), content: "ok".into() });
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
            json!({"status":"completed","output":[call.clone(),call.clone()]}),
            json!({"status":"completed","output":[partial]}),
        ] {
            assert!(ResponseState::default().finish(response).is_err());
        }
    }
}
