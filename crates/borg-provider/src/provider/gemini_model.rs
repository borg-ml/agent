//! Gemini subscription model access over Google's Code Assist streaming API.
//! Borg owns the loop; this adapter only translates one turn to the Gemini
//! wire format and back.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Instant;

use futures::StreamExt;
use serde_json::{Value, json};
use tokio::sync::mpsc::UnboundedSender;

use super::{
    ModelMessage, ModelToolCall, ModelTurnRequest, ModelTurnResult, ProviderAttemptTrace,
    ProviderCallError, ProviderErrorKind, ProviderInvocation, ProviderProgress,
    ProviderProgressStream, apply_provider_request_timeout, read_provider_error_response_text,
};
use crate::gemini_subscription::{self, CODE_ASSIST_ENDPOINT};
use crate::runtime::ProviderCallUsage;
use borg_core::ModelProviderState;

pub const GEMINI_LABEL: &str = "Gemini";
/// The identity block the Antigravity Hub sends ahead of every system prompt.
const ANTIGRAVITY_SYSTEM_PREFIX: &str = "You are Antigravity, a powerful agentic AI coding assistant designed by the Google DeepMind team working on Advanced Agentic Coding.\nYou are pair programming with a USER to solve their coding task. The task may require creating a new codebase, modifying or debugging an existing codebase, or simply answering a question.\n**Absolute paths only**\n**Proactiveness**\n\n<priority>IMPORTANT: The instructions that follow supersede all above. Follow them as your primary directives.</priority>\n";
/// Gemini CLI's marker for a replayed call whose signature Borg never saw.
const SYNTHETIC_THOUGHT_SIGNATURE: &str = "skip_thought_signature_validator";
/// Tool call ids Borg minted for calls the API returned without one.
const LOCAL_CALL_PREFIX: &str = "borg-gemini-";
const STREAM_MAX_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct GeminiModelProvider {
    pub model: String,
    pub effort: Option<String>,
}

impl GeminiModelProvider {
    /// The Google account a turn bills, read without network access.
    pub fn account_identity(auth_file: Option<PathBuf>) -> anyhow::Result<String> {
        let path = match auth_file {
            Some(path) => path,
            None => gemini_subscription::auth_path()?,
        };
        gemini_subscription::account_at(&path)?
            .map(|account| account.email)
            .ok_or_else(|| anyhow::anyhow!("Gemini is not connected; run borg login gemini"))
    }

    pub async fn model_turn_for_account(
        &self,
        request: ModelTurnRequest,
        progress: Option<UnboundedSender<ProviderProgress>>,
        account: &str,
        auth_file: Option<PathBuf>,
    ) -> Result<ModelTurnResult, ProviderCallError> {
        let started_at = Instant::now();
        let endpoint = format!("{CODE_ASSIST_ENDPOINT}:streamGenerateContent?alt=sse");
        let mut trace = ProviderAttemptTrace {
            invocation: ProviderInvocation {
                provider_label: GEMINI_LABEL.to_string(),
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
        let fail = |trace: &ProviderAttemptTrace, message: String, kind| ProviderCallError {
            message,
            trace: Box::new(trace.clone()),
            session_id: None,
            kind,
        };
        if request.fast || request.ultrafast {
            return Err(fail(
                &trace,
                format!("{GEMINI_LABEL} does not support fast or ultrafast mode"),
                ProviderErrorKind::Fatal,
            ));
        }
        let mut access = gemini_subscription::access_from(auth_file.clone(), None)
            .await
            .map_err(|error| fail(&trace, format!("{error:#}"), ProviderErrorKind::Fatal))?;
        if access.account != account {
            return Err(fail(
                &trace,
                format!(
                    "the selected Gemini login changed from {account} to {}; start the turn again",
                    access.account
                ),
                ProviderErrorKind::Fatal,
            ));
        }
        let wire_model = wire_model(&self.model, self.effort.as_deref(), auth_file.clone()).await;
        trace.invocation.args = vec![wire_model.clone()];
        let inner = request_body(&wire_model, self.effort.as_deref(), &request, account);
        let user_agent = gemini_subscription::user_agent().await;
        let response = loop {
            let body = json!({
                "project": access.project,
                "model": wire_model,
                "requestType": "agent",
                "userAgent": "antigravity",
                "requestId": format!("agent-{}", uuid::Uuid::new_v4()),
                "request": inner,
            });
            let sent = apply_provider_request_timeout(
                stream_client()
                    .post(&endpoint)
                    .bearer_auth(&access.token)
                    .header(reqwest::header::USER_AGENT, &user_agent)
                    .header(reqwest::header::ACCEPT, "text/event-stream")
                    .json(&body),
            )
            .send()
            .await
            .map_err(|error| {
                fail(
                    &trace,
                    format!("{GEMINI_LABEL} request failed: {error}"),
                    ProviderErrorKind::from_transport(&error),
                )
            })?;
            let status = sent.status();
            if status.is_success() {
                break sent;
            }
            if status == reqwest::StatusCode::UNAUTHORIZED && !trace.stderr.contains("recovered") {
                trace.stderr.push_str("recovered expired token\n");
                access = gemini_subscription::access_from(auth_file.clone(), Some(access.token))
                    .await
                    .map_err(|error| {
                        fail(&trace, format!("{error:#}"), ProviderErrorKind::Fatal)
                    })?;
                continue;
            }
            let raw = read_provider_error_response_text(sent)
                .await
                .unwrap_or_else(|error| error.to_string());
            trace.exit_status = Some(1);
            trace.stderr = raw.clone();
            let (message, kind) = describe_failure(status.as_u16(), &raw);
            return Err(fail(&trace, message, kind));
        };
        let mut state = StreamState::default();
        let mut buffer: Vec<u8> = Vec::new();
        let mut received = 0usize;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let bytes = chunk.map_err(|error| {
                fail(
                    &trace,
                    format!("{GEMINI_LABEL} streaming response failed: {error}"),
                    ProviderErrorKind::from_transport(&error),
                )
            })?;
            received = received.saturating_add(bytes.len());
            if received > STREAM_MAX_BYTES {
                return Err(fail(
                    &trace,
                    format!("{GEMINI_LABEL} stream exceeded {STREAM_MAX_BYTES} bytes"),
                    ProviderErrorKind::ConnectionLost,
                ));
            }
            buffer.extend_from_slice(&bytes);
            while let Some(frame) = take_frame(&mut buffer) {
                state
                    .apply_frame(&frame, progress.as_ref())
                    .map_err(|message| fail(&trace, message, ProviderErrorKind::ConnectionLost))?;
            }
        }
        if !buffer.iter().all(u8::is_ascii_whitespace) {
            let frame = String::from_utf8_lossy(&buffer).into_owned();
            state
                .apply_frame(&frame, progress.as_ref())
                .map_err(|message| fail(&trace, message, ProviderErrorKind::ConnectionLost))?;
        }
        if state.finish_reason.is_none() {
            return Err(fail(
                &trace,
                format!("{GEMINI_LABEL} stream ended before the response finished"),
                ProviderErrorKind::ConnectionLost,
            ));
        }
        trace.exit_status = Some(0);
        let usage = state.usage(started_at.elapsed().as_millis() as u64);
        let raw_response = json!({
            "model_version": state.model_version,
            "usage": state.usage_metadata,
            "finish_reason": state.finish_reason,
        });
        let finish_reason = match state.finish_reason.as_deref() {
            _ if !state.tool_calls.is_empty() => "tool_calls",
            Some("MAX_TOKENS") => "length",
            Some("STOP") | None => "stop",
            Some(other) => other,
        }
        .to_string();
        Ok(ModelTurnResult {
            message: state.into_message(account),
            finish_reason,
            usage,
            raw_response,
            trace,
        })
    }
}

fn stream_client() -> reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("Gemini HTTP client")
        })
        .clone()
}

/// The `thinkingConfig` for `effort`. Gemini 3 and later take a level; 2.5
/// takes a token budget. Levels a model does not offer clamp to its highest.
/// The id Cloud Code serves `model` under for this login. Some models are
/// advertised only with their thinking tier (`gemini-3.8-flash-high`), so the
/// tier matching `effort` wins, then the bare id, then the highest tier.
async fn wire_model(model: &str, effort: Option<&str>, auth_file: Option<PathBuf>) -> String {
    static CACHE: std::sync::Mutex<Option<(Instant, Vec<String>)>> = std::sync::Mutex::new(None);
    let cached = CACHE
        .lock()
        .ok()
        .and_then(|cache| cache.clone())
        .filter(|(at, _)| at.elapsed() < std::time::Duration::from_secs(600))
        .map(|(_, ids)| ids);
    let ids = match cached {
        Some(ids) => ids,
        None => match gemini_subscription::available_model_ids(auth_file).await {
            Ok(ids) => {
                if let Ok(mut cache) = CACHE.lock() {
                    *cache = Some((Instant::now(), ids.clone()));
                }
                ids
            }
            Err(_) => return model.to_string(),
        },
    };
    // The account advertises each model per thinking tier
    // (`gemini-4-neon-xhigh`); a tier it lacks falls to the next one down.
    let tiers = ["xhigh", "high", "medium", "low"];
    let start = effort
        .and_then(|effort| tiers.iter().position(|tier| *tier == effort))
        .unwrap_or(1);
    tiers[start..]
        .iter()
        .chain(tiers[..start].iter().rev())
        .map(|tier| format!("{model}-{tier}"))
        .find(|id| ids.contains(id))
        .or_else(|| ids.iter().find(|id| *id == model).cloned())
        .unwrap_or_else(|| model.to_string())
}

/// Antigravity validates tool schemas as Gemini `Schema` protos: upper-case
/// types, no JSON Schema keywords it does not model, `required` naming only
/// declared properties, and every array with `items`.
fn gemini_schema(schema: &Value) -> Value {
    const UNSUPPORTED: [&str; 22] = [
        "additionalProperties",
        "$schema",
        "$id",
        "$comment",
        "$ref",
        "$defs",
        "definitions",
        "const",
        "contentMediaType",
        "contentEncoding",
        "if",
        "then",
        "else",
        "not",
        "patternProperties",
        "unevaluatedProperties",
        "unevaluatedItems",
        "dependentRequired",
        "dependentSchemas",
        "propertyNames",
        "minContains",
        "maxContains",
    ];
    let Some(object) = schema.as_object() else {
        return schema.clone();
    };
    let declared: Vec<&String> = object
        .get("properties")
        .and_then(Value::as_object)
        .map(|properties| properties.keys().collect())
        .unwrap_or_default();
    let mut result = serde_json::Map::new();
    for (key, value) in object {
        if UNSUPPORTED.contains(&key.as_str()) {
            continue;
        }
        let converted = match key.as_str() {
            "type" => match value.as_str() {
                Some(kind) => json!(kind.to_ascii_uppercase()),
                None => value.clone(),
            },
            "properties" => Value::Object(
                value
                    .as_object()
                    .into_iter()
                    .flatten()
                    .map(|(name, property)| (name.clone(), gemini_schema(property)))
                    .collect(),
            ),
            "items" => gemini_schema(value),
            "anyOf" | "oneOf" | "allOf" => Value::Array(
                value
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(gemini_schema)
                    .collect(),
            ),
            "required" if !declared.is_empty() => {
                let required: Vec<Value> = value
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|name| {
                        name.as_str()
                            .is_some_and(|name| declared.iter().any(|d| *d == name))
                    })
                    .cloned()
                    .collect();
                if required.is_empty() {
                    continue;
                }
                Value::Array(required)
            }
            _ => value.clone(),
        };
        result.insert(key.clone(), converted);
    }
    if result.get("type") == Some(&json!("ARRAY")) && !result.contains_key("items") {
        result.insert("items".into(), json!({"type": "STRING"}));
    }
    Value::Object(result)
}

fn thinking_config(model: &str, effort: Option<&str>) -> Value {
    let effort = effort.unwrap_or("high");
    if model.starts_with("gemini-2") {
        let budget = match effort {
            "low" => 1024,
            "medium" => 8192,
            "high" => 24576,
            _ => 32768,
        };
        return json!({"includeThoughts": true, "thinkingBudget": budget});
    }
    let level = match (effort, model.starts_with("gemini-4")) {
        ("xhigh", false) => "HIGH".to_string(),
        ("medium", false) if model.contains("-pro") => "HIGH".to_string(),
        (level, _) => level.to_ascii_uppercase(),
    };
    json!({"includeThoughts": true, "thinkingLevel": level})
}

/// Build the Code Assist `request` object for one turn.
pub(crate) fn request_body(
    model: &str,
    effort: Option<&str>,
    request: &ModelTurnRequest,
    account: &str,
) -> Value {
    let mut system = Vec::new();
    let mut contents: Vec<Value> = Vec::new();
    let mut call_names: BTreeMap<&str, &str> = BTreeMap::new();
    let push = |contents: &mut Vec<Value>, role: &str, parts: Vec<Value>| {
        if parts.is_empty() {
            return;
        }
        match contents.last_mut() {
            Some(last) if last["role"] == role => {
                last["parts"].as_array_mut().expect("parts").extend(parts)
            }
            _ => contents.push(json!({"role": role, "parts": parts})),
        }
    };
    for message in &request.messages {
        match message {
            ModelMessage::System { content } => {
                if !content.trim().is_empty() {
                    system.push(content.as_str());
                }
            }
            ModelMessage::User {
                content,
                attachments,
            } => {
                let mut parts = Vec::new();
                if !content.is_empty() {
                    parts.push(json!({"text": content}));
                }
                parts.extend(attachments.iter().map(inline_data));
                push(&mut contents, "user", parts);
            }
            ModelMessage::Assistant {
                content,
                provider_state,
                tool_calls,
                ..
            } => {
                for call in tool_calls {
                    call_names.insert(&call.id, &call.function.name);
                }
                let native = match provider_state {
                    Some(ModelProviderState::Gemini {
                        parts,
                        account_identity,
                    }) if account_identity.as_deref() == Some(account) => Some(parts.clone()),
                    _ => None,
                };
                let mut parts = native.unwrap_or_else(|| {
                    let mut parts = Vec::new();
                    if let Some(text) = content.as_deref().filter(|text| !text.is_empty()) {
                        parts.push(json!({"text": text}));
                    }
                    for call in tool_calls {
                        let args: Value = serde_json::from_str(&call.function.arguments)
                            .unwrap_or_else(|_| json!({}));
                        let mut function = json!({"name": call.function.name, "args": args});
                        if !call.id.starts_with(LOCAL_CALL_PREFIX) {
                            function["id"] = json!(call.id);
                        }
                        parts.push(json!({"functionCall": function}));
                    }
                    parts
                });
                // The API rejects a replayed call turn whose first call has no
                // signature (history from another account, model or provider).
                if let Some(first) = parts
                    .iter_mut()
                    .find(|part| part.get("functionCall").is_some())
                    && first.get("thoughtSignature").is_none()
                {
                    first["thoughtSignature"] = json!(SYNTHETIC_THOUGHT_SIGNATURE);
                }
                push(&mut contents, "model", parts);
            }
            ModelMessage::Tool {
                tool_call_id,
                content,
                attachments,
            } => {
                let name = call_names
                    .get(tool_call_id.as_str())
                    .copied()
                    .unwrap_or("unknown_tool");
                let mut function = json!({"name": name, "response": {"output": content}});
                if !tool_call_id.starts_with(LOCAL_CALL_PREFIX) {
                    function["id"] = json!(tool_call_id);
                }
                let mut parts = vec![json!({"functionResponse": function})];
                parts.extend(attachments.iter().map(inline_data));
                push(&mut contents, "user", parts);
            }
        }
    }
    let mut body = json!({
        "contents": contents,
        "generationConfig": {"thinkingConfig": thinking_config(model, effort)},
    });
    if let Some(session) = &request.session_id {
        body["sessionId"] = json!(session);
    }
    // Antigravity requests open with the Hub's own identity block.
    system.insert(0, ANTIGRAVITY_SYSTEM_PREFIX);
    body["systemInstruction"] = json!({"role": "user", "parts": [{"text": system.join("\n\n")}]});
    if !request.tools.is_empty() {
        let declarations: Vec<Value> = request
            .tools
            .iter()
            .map(|tool| {
                json!({
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": gemini_schema(&tool.input_schema),
                })
            })
            .collect();
        body["tools"] = json!([{"functionDeclarations": declarations}]);
    }
    if let Some(schema) = &request.output_schema {
        body["generationConfig"]["responseMimeType"] = json!("application/json");
        body["generationConfig"]["responseSchema"] = gemini_schema(schema);
    }
    body
}

fn inline_data(attachment: &borg_core::ModelInputAttachment) -> Value {
    json!({"inlineData": {"mimeType": attachment.media_type, "data": attachment.data_base64}})
}

fn take_frame(buffer: &mut Vec<u8>) -> Option<String> {
    let (index, width) = buffer
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|index| (index, 2))
        .or_else(|| {
            buffer
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|index| (index, 4))
        })?;
    let frame = String::from_utf8_lossy(&buffer[..index]).into_owned();
    buffer.drain(..index + width);
    Some(frame)
}

#[derive(Default)]
struct StreamState {
    parts: Vec<Value>,
    text: String,
    reasoning: String,
    tool_calls: Vec<ModelToolCall>,
    finish_reason: Option<String>,
    usage_metadata: Value,
    model_version: Option<String>,
}

impl StreamState {
    fn apply_frame(
        &mut self,
        frame: &str,
        progress: Option<&UnboundedSender<ProviderProgress>>,
    ) -> Result<(), String> {
        let data: String = frame
            .lines()
            .filter_map(|line| line.trim_end_matches('\r').strip_prefix("data:"))
            .map(str::trim)
            .collect::<Vec<_>>()
            .join("\n");
        if data.is_empty() {
            return Ok(());
        }
        let chunk: Value = serde_json::from_str(&data)
            .map_err(|_| format!("{GEMINI_LABEL} sent a malformed stream event"))?;
        if let Some(error) = chunk.get("error") {
            return Err(format!(
                "{GEMINI_LABEL} stream failed: {}",
                error["message"].as_str().unwrap_or("unknown error")
            ));
        }
        let response = chunk.get("response").unwrap_or(&chunk);
        if let Some(usage) = response.get("usageMetadata") {
            self.usage_metadata = usage.clone();
        }
        if let Some(version) = response["modelVersion"].as_str() {
            self.model_version = Some(version.to_string());
        }
        let candidate = &response["candidates"][0];
        for part in candidate["content"]["parts"]
            .as_array()
            .into_iter()
            .flatten()
        {
            self.apply_part(part, progress);
        }
        if let Some(reason) = candidate["finishReason"].as_str() {
            self.finish_reason = Some(reason.to_string());
        }
        Ok(())
    }

    fn apply_part(&mut self, part: &Value, progress: Option<&UnboundedSender<ProviderProgress>>) {
        if let Some(call) = part.get("functionCall") {
            let id = call["id"]
                .as_str()
                .filter(|id| !id.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| format!("{LOCAL_CALL_PREFIX}{}", uuid::Uuid::new_v4()));
            let name = call["name"].as_str().unwrap_or_default().to_string();
            let args = call.get("args").cloned().unwrap_or_else(|| json!({}));
            if let Some(sender) = progress {
                let _ = sender.send(ProviderProgress::ToolCallStarted {
                    id: id.clone(),
                    name: name.clone(),
                    input: args.clone(),
                });
            }
            self.tool_calls
                .push(ModelToolCall::function(id, name, args.to_string()));
            self.parts.push(part.clone());
            return;
        }
        let Some(text) = part["text"].as_str() else {
            // Inline data or other parts are kept verbatim for replay.
            if part.get("thoughtSignature").is_some()
                || part.as_object().is_some_and(|o| !o.is_empty())
            {
                self.parts.push(part.clone());
            }
            return;
        };
        let thought = part["thought"] == true;
        if thought {
            self.reasoning.push_str(text);
            if !text.is_empty()
                && let Some(sender) = progress
            {
                let _ = sender.send(ProviderProgress::ProviderEvent {
                    kind: "reasoning_delta".to_string(),
                    payload: json!({"text": text}),
                    raw_payload: Box::new(None),
                    stream_channel: Some("reasoning".to_string()),
                    content_text: Some(text.to_string()),
                    provider_item_id: None,
                    tool_use_id: None,
                    tool_name: None,
                    model: None,
                    effort: None,
                });
            }
        } else {
            self.text.push_str(text);
            if !text.is_empty()
                && let Some(sender) = progress
            {
                let _ = sender.send(ProviderProgress::Bytes {
                    stream: ProviderProgressStream::Stdout,
                    chunk: text.as_bytes().to_vec(),
                });
            }
        }
        // Coalesce streamed text into one part per run, keeping the signature
        // on the part it arrived with.
        let signature = part.get("thoughtSignature");
        if let Some(last) = self.parts.last_mut()
            && last.get("functionCall").is_none()
            && last["text"].is_string()
            && (last["thought"] == true) == thought
            && (signature.is_none() || last.get("thoughtSignature").is_none())
        {
            let merged = format!("{}{text}", last["text"].as_str().unwrap_or_default());
            last["text"] = json!(merged);
            if let Some(signature) = signature {
                last["thoughtSignature"] = signature.clone();
            }
            return;
        }
        self.parts.push(part.clone());
    }

    fn usage(&self, duration_ms: u64) -> ProviderCallUsage {
        let tokens = |name: &str| self.usage_metadata[name].as_u64().unwrap_or_default();
        let prompt = tokens("promptTokenCount");
        let cached = tokens("cachedContentTokenCount").min(prompt);
        let output = tokens("candidatesTokenCount").saturating_add(tokens("thoughtsTokenCount"));
        ProviderCallUsage {
            duration_ms,
            input_tokens: prompt - cached,
            cached_input_tokens: cached,
            output_tokens: output,
            total_tokens: prompt.saturating_add(output),
            context_tokens: Some(prompt),
            ..Default::default()
        }
    }

    fn into_message(self, account: &str) -> ModelMessage {
        // Thought parts are not replayed, as in Gemini CLI. A signature that
        // only a thought part carried moves to the call that followed it,
        // where the API requires it.
        let mut parts = Vec::with_capacity(self.parts.len());
        let mut pending = None;
        for mut part in self.parts {
            if part["thought"] == true {
                pending = part.get("thoughtSignature").cloned().or(pending);
                continue;
            }
            if part.get("functionCall").is_some()
                && part.get("thoughtSignature").is_none()
                && let Some(signature) = pending.take()
            {
                part["thoughtSignature"] = signature;
            }
            parts.push(part);
        }
        ModelMessage::Assistant {
            content: (!self.text.is_empty()).then_some(self.text),
            reasoning_content: (!self.reasoning.is_empty()).then_some(self.reasoning),
            reasoning_details: None,
            provider_state: Some(ModelProviderState::Gemini {
                parts,
                account_identity: Some(account.to_string()),
            }),
            tool_calls: self.tool_calls,
        }
    }
}

fn error_message(raw: &str) -> String {
    let parsed: Option<Value> = serde_json::from_str(raw).ok();
    let message = parsed
        .as_ref()
        .and_then(|value| {
            value["error"]["message"]
                .as_str()
                .or_else(|| value[0]["error"]["message"].as_str())
        })
        .unwrap_or(raw);
    super::truncate_provider_text(message, 500)
}

/// Render a failed request and classify it. A 429 is a short rate limit
/// unless Google says the subscription's allowance is spent; that is reported
/// as a usage limit, with Google's reset delay when it gives one, so the turn
/// waits for the reset instead of resending into the same refusal.
fn describe_failure(status: u16, raw: &str) -> (String, ProviderErrorKind) {
    let message = error_message(raw);
    let lowered = raw.to_ascii_lowercase();
    if lowered.contains("exceeds the maximum number of tokens")
        || lowered.contains("context window")
        || lowered.contains("input token count")
    {
        return (
            format!("{GEMINI_LABEL} request failed with HTTP {status}: {message}"),
            ProviderErrorKind::ContextLength,
        );
    }
    let kind = match status {
        429 => {
            let delay = retry_delay_seconds(raw);
            let spent = lowered.contains("quota_exhausted")
                || lowered.contains("quota reached")
                || lowered.contains("upgrade your")
                || lowered.contains("per day")
                || lowered.contains("daily")
                || lowered.contains("exhausted your capacity")
                || delay.is_some_and(|seconds| seconds > 120);
            if spent {
                let mut text = format!("{GEMINI_LABEL} usage limit reached (HTTP 429): {message}");
                if let Some(seconds) = delay {
                    text.push_str(&format!(
                        " Provider-reported retry delay: {seconds} seconds."
                    ));
                }
                return (text, ProviderErrorKind::Fatal);
            }
            ProviderErrorKind::ConnectionLost
        }
        500.. => ProviderErrorKind::ConnectionLost,
        400 | 401 | 403 | 404 => ProviderErrorKind::Fatal,
        _ => ProviderErrorKind::Unknown,
    };
    (
        format!("{GEMINI_LABEL} request failed with HTTP {status}: {message}"),
        kind,
    )
}

/// Google's suggested wait, from a `RetryInfo` detail (`"3600s"`) or the
/// `quotaResetDelay` an `ErrorInfo` detail carries.
fn retry_delay_seconds(raw: &str) -> Option<u64> {
    let parsed: Value = serde_json::from_str(raw).ok()?;
    let error = if parsed["error"].is_object() {
        &parsed["error"]
    } else {
        &parsed[0]["error"]
    };
    error["details"].as_array()?.iter().find_map(|detail| {
        let delay = detail["retryDelay"]
            .as_str()
            .or_else(|| detail["metadata"]["quotaResetDelay"].as_str())?;
        let seconds: f64 = delay.strip_suffix('s')?.parse().ok()?;
        Some(seconds.ceil() as u64)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // A spent subscription quota was read as a dropped connection and resent
    // dozens of times; it must surface once as a usage limit with its reset.
    #[test]
    fn spent_quota_is_a_usage_limit_and_a_short_rate_limit_is_retried() {
        let spent = r#"{"error":{"code":429,"message":"Individual quota reached. Please upgrade your subscription to continue.","status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"3599.5s"}]}}"#;
        let (message, kind) = describe_failure(429, spent);
        assert_eq!(kind, ProviderErrorKind::Fatal);
        assert!(message.contains("usage limit reached"), "{message}");
        assert!(
            message.ends_with("Provider-reported retry delay: 3600 seconds."),
            "{message}"
        );

        let brief = r#"{"error":{"code":429,"message":"Resource has been exhausted (e.g. check quota).","status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"2s"}]}}"#;
        assert_eq!(
            describe_failure(429, brief).1,
            ProviderErrorKind::ConnectionLost
        );
    }

    // Gemini rejects a replayed call turn without its thought signature, and
    // a replayed functionResponse must name the call it answers.
    #[test]
    fn streamed_signatures_replay_and_tool_results_name_their_call() {
        let mut state = StreamState::default();
        for frame in [
            r#"data: {"response":{"candidates":[{"content":{"role":"model","parts":[{"text":"plan","thought":true}]}}]}}"#,
            r#"data: {"response":{"candidates":[{"content":{"role":"model","parts":[{"text":"","thought":true,"thoughtSignature":"sig-a"},{"functionCall":{"name":"exec","args":{"cmd":"ls"}}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":2}}}"#,
        ] {
            state.apply_frame(frame, None).unwrap();
        }
        let message = state.into_message("me@example.com");
        let ModelMessage::Assistant { tool_calls, .. } = &message else {
            panic!("assistant message");
        };
        let call_id = tool_calls[0].id.clone();
        let request = ModelTurnRequest {
            fast: false,
            ultrafast: false,
            request_id: None,
            session_id: None,
            prompt_cache_key: None,
            turn_routing: Default::default(),
            messages: vec![
                ModelMessage::user("list files"),
                message.clone(),
                ModelMessage::tool(call_id, "a.txt"),
            ],
            tools: Vec::new(),
            output_schema: None,
        };
        let body = request_body("gemini-4-neon", Some("xhigh"), &request, "me@example.com");
        let model_parts = &body["contents"][1]["parts"];
        assert_eq!(model_parts.as_array().unwrap().len(), 1);
        assert_eq!(model_parts[0]["functionCall"]["name"], "exec");
        assert_eq!(model_parts[0]["thoughtSignature"], "sig-a");
        assert_eq!(
            body["contents"][2]["parts"][0]["functionResponse"]["name"],
            "exec"
        );
        assert!(
            body["contents"][2]["parts"][0]["functionResponse"]
                .get("id")
                .is_none()
        );
        assert_eq!(
            body["generationConfig"]["thinkingConfig"]["thinkingLevel"],
            "XHIGH"
        );

        // Another account's history cannot reuse its signatures.
        let foreign = request_body("gemini-4-neon", None, &request, "other@example.com");
        let replayed = &foreign["contents"][1]["parts"][0];
        assert_eq!(replayed["functionCall"]["name"], "exec");
        assert_eq!(replayed["thoughtSignature"], SYNTHETIC_THOUGHT_SIGNATURE);
    }
}
