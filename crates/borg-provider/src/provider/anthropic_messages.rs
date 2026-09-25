//! Native Anthropic Messages API access for the API-key lane.
//!
//! Model access only: Borg owns the agent loop, tools, persistence and policy,
//! and this route bills the Anthropic API key the user configured. It is
//! deliberately separate from the Claude subscription lane, which runs the
//! unmodified Claude Code binary. Subscription credentials are never replayed
//! here, and this key is never used to stand in for them.

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use futures::StreamExt;
use serde_json::{Value, json};
use tokio::sync::mpsc::UnboundedSender;

use borg_core::ModelProviderState;

use crate::runtime::ProviderCallUsage;
use crate::runtime::elapsed_millis_u64;

use super::{
    ModelMessage, ModelToolCall, ModelTurnRequest, ModelTurnResult, ProviderAttemptTrace,
    ProviderCallError, ProviderErrorKind, ProviderInvocation, ProviderProgress,
    ProviderProgressStream, apply_provider_request_timeout, read_provider_error_response_text,
    truncate_provider_text,
};

/// Label used in traces and diagnostics for this route.
pub const ANTHROPIC_LABEL: &str = "anthropic-api";

/// The API version Borg pins. Anthropic requires it on every request.
pub const ANTHROPIC_VERSION: &str = "2023-06-01";

/// Output budget for a request without extended thinking.
const DEFAULT_MAX_OUTPUT_TOKENS: u64 = 8192;

/// Extended thinking is spent from the same budget as the answer, so enabling
/// it raises the cap instead of shrinking the visible reply.
const THINKING_BUDGET_TOKENS: u64 = 4096;

/// A stream that grows past this is not a conversation Borg can hold in memory.
const STREAM_MAX_BYTES: usize = 128 * 1024 * 1024;

/// The cache marker the API accepts on a content block.
///
/// Anthropic caches everything up to and including a marked block, and allows
/// four markers per request. Borg spends three: the last tool definition, the
/// system block, and the newest block the conversation has produced. Tool
/// schemas get their own marker so a changed system prompt does not invalidate
/// them.
const EPHEMERAL_CACHE_CONTROL: &str = "ephemeral";

/// The Messages endpoint, overridable for a proxy or an on-premise gateway.
pub fn messages_endpoint() -> String {
    let base = crate::env::nonempty_var("BORG_ANTHROPIC_BASE_URL")
        .unwrap_or_else(|| "https://api.anthropic.com".to_string());
    format!("{}/v1/messages", base.trim_end_matches("/"))
}

/// The context window Borg assumes for a Claude API model.
///
/// Published models are 200k. A model outside that set reports no window, and
/// the harness then relies on its own estimate rather than compacting against
/// a guess.
pub fn context_window_tokens(model: &str) -> Option<u64> {
    model
        .trim()
        .to_ascii_lowercase()
        .starts_with("claude-")
        .then_some(200_000)
}

fn anthropic_http_client() -> reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default()
        })
        .clone()
}

/// Anthropic Messages access for one Borg-owned turn.
#[derive(Debug, Clone)]
pub struct AnthropicMessagesProvider {
    pub model: String,
    pub effort: Option<String>,
}

impl AnthropicMessagesProvider {
    pub async fn model_turn(
        &self,
        request: ModelTurnRequest,
        progress: Option<UnboundedSender<ProviderProgress>>,
        api_key: &str,
    ) -> Result<ModelTurnResult, ProviderCallError> {
        let started_at = Instant::now();
        let endpoint = messages_endpoint();
        let mut trace = ProviderAttemptTrace {
            invocation: ProviderInvocation {
                provider_label: ANTHROPIC_LABEL.to_string(),
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
        if api_key.trim().is_empty() {
            return Err(ProviderCallError {
                message: format!("{ANTHROPIC_LABEL} has no API key configured"),
                trace: Box::new(trace),
                session_id: None,
                kind: ProviderErrorKind::Fatal,
            });
        }
        if request.fast {
            return Err(ProviderCallError {
                message: format!("{ANTHROPIC_LABEL} does not support fast mode"),
                trace: Box::new(trace),
                session_id: None,
                kind: ProviderErrorKind::Fatal,
            });
        }
        let body = messages_request_body(&self.model, self.effort.as_deref(), &request);
        let http = anthropic_http_client()
            .post(&endpoint)
            .header("x-api-key", api_key)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .json(&body);
        let response = match apply_provider_request_timeout(http).send().await {
            Ok(response) => response,
            Err(error) => {
                return Err(ProviderCallError {
                    message: format!("{ANTHROPIC_LABEL} request failed: {error}"),
                    trace: Box::new(trace),
                    session_id: None,
                    kind: ProviderErrorKind::from_transport(&error),
                });
            }
        };
        let status = response.status();
        if !status.is_success() {
            let raw = read_provider_error_response_text(response)
                .await
                .unwrap_or_else(|error| error.to_string());
            trace.exit_status = Some(1);
            trace.stderr = raw.clone();
            return Err(ProviderCallError {
                message: format!(
                    "{ANTHROPIC_LABEL} request failed with HTTP {}: {}",
                    status.as_u16(),
                    truncate_provider_text(&raw, 500)
                ),
                trace: Box::new(trace),
                session_id: None,
                kind: anthropic_error_kind(status.as_u16(), &raw),
            });
        }

        let mut state = AnthropicStreamState::default();
        let mut buffer: Vec<u8> = Vec::new();
        let mut received_bytes: usize = 0;
        let mut stream = response.bytes_stream();
        let mut stream_failure: Option<(String, ProviderErrorKind)> = None;
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    received_bytes = received_bytes.saturating_add(bytes.len());
                    buffer.extend_from_slice(&bytes);
                }
                Err(error) => {
                    stream_failure = Some((
                        format!("{ANTHROPIC_LABEL} streaming response failed: {error}"),
                        ProviderErrorKind::from_transport(&error),
                    ));
                    break;
                }
            }
            if received_bytes > STREAM_MAX_BYTES {
                stream_failure = Some((
                    format!("{ANTHROPIC_LABEL} stream exceeded {STREAM_MAX_BYTES} bytes"),
                    ProviderErrorKind::ConnectionLost,
                ));
                break;
            }
            while let Some(frame) = take_sse_frame(&mut buffer) {
                match frame {
                    Ok(frame) => apply_sse_frame(&mut state, &frame, progress.as_ref()),
                    Err(_) => state.protocol_error("invalid UTF-8 in event"),
                }
            }
            if state.provider_error.is_some() {
                break;
            }
        }
        if let Some((message, kind)) = stream_failure {
            return Err(ProviderCallError {
                message,
                trace: Box::new(trace),
                session_id: None,
                kind,
            });
        }
        if let Some((message, kind)) = state.take_error() {
            return Err(ProviderCallError {
                message,
                trace: Box::new(trace),
                session_id: None,
                kind,
            });
        }
        if let Err(message) = state.validate_complete() {
            return Err(ProviderCallError {
                message,
                trace: Box::new(trace),
                session_id: None,
                kind: ProviderErrorKind::ConnectionLost,
            });
        }
        let usage = state.usage(elapsed_millis_u64(started_at));
        trace.exit_status = Some(0);
        Ok(ModelTurnResult {
            message: state.assistant_message(),
            finish_reason: state.finish_reason(),
            usage,
            raw_response: state.raw_response(),
            trace,
        })
    }
}

/// Build the Messages request for one turn.
///
/// The API hoists every `System` message into one top-level field and requires
/// `max_tokens`, so this is not a field-for-field rename of the portable
/// conversation: tool results become user content blocks. Native assistant
/// blocks carry the signatures needed to replay thinking across tool rounds.
pub(crate) fn messages_request_body(
    model: &str,
    effort: Option<&str>,
    request: &ModelTurnRequest,
) -> Value {
    let mut system_parts: Vec<String> = Vec::new();
    let mut messages: Vec<Value> = Vec::new();
    for message in &request.messages {
        match message {
            ModelMessage::System { content } => {
                if !content.trim().is_empty() {
                    system_parts.push(content.clone());
                }
            }
            ModelMessage::User {
                content,
                attachments,
            } => {
                let mut blocks = text_blocks(content);
                blocks.extend(image_blocks(attachments));
                push_turn(&mut messages, "user", blocks);
            }
            ModelMessage::Assistant {
                content,
                tool_calls,
                provider_state,
                ..
            } => {
                if let Some(ModelProviderState::AnthropicMessages { content, .. }) = provider_state
                {
                    push_turn(&mut messages, "assistant", content.clone());
                    continue;
                }
                let mut blocks: Vec<Value> = Vec::new();
                if let Some(text) = content.as_deref().filter(|text| !text.trim().is_empty()) {
                    blocks.push(json!({ "type": "text", "text": text }));
                }
                for call in tool_calls {
                    blocks.push(json!({
                        "type": "tool_use",
                        "id": call.id,
                        "name": call.function.name,
                        "input": parse_tool_arguments(&call.function.arguments),
                    }));
                }
                push_turn(&mut messages, "assistant", blocks);
            }
            ModelMessage::Tool {
                tool_call_id,
                content,
                attachments,
            } => {
                let result = if attachments.is_empty() {
                    json!(content)
                } else {
                    let mut blocks = text_blocks(content);
                    blocks.extend(image_blocks(attachments));
                    json!(blocks)
                };
                let blocks = vec![json!({
                    "type": "tool_result",
                    "tool_use_id": tool_call_id,
                    "content": result,
                })];
                push_turn(&mut messages, "user", blocks);
            }
        }
    }

    // Marked before assembly: `json!` takes a reference, so a marker applied to
    // the vector afterwards would never reach the request.
    mark_tail_for_cache(&mut messages);
    let mut body = json!({
        "model": model,
        "max_tokens": max_output_tokens(effort),
        "messages": messages,
        "stream": true,
    });
    if !system_parts.is_empty() {
        // Sent as a block rather than a bare string so it can carry the cache
        // marker. The tools field precedes it in the cached prefix, so this
        // marker extends the cached prefix through the instructions.
        body["system"] = json!([{
            "type": "text",
            "text": system_parts.join("\n\n"),
            "cache_control": { "type": EPHEMERAL_CACHE_CONTROL },
        }]);
    }
    if !request.tools.is_empty() {
        // The API's cache prefix runs tools, then system, then messages, and a
        // marker goes on a block rather than on the array, so the last
        // definition is the one marked: that caches every schema without
        // spending a marker per tool.
        let last_tool = request.tools.len() - 1;
        body["tools"] = Value::Array(
            request
                .tools
                .iter()
                .enumerate()
                .map(|(index, tool)| {
                    let mut definition = json!({
                        "name": tool.name,
                        "description": tool.description,
                        "input_schema": tool.input_schema,
                    });
                    if index == last_tool {
                        definition["cache_control"] = json!({ "type": EPHEMERAL_CACHE_CONTROL });
                    }
                    definition
                })
                .collect(),
        );
    }
    if let Some(budget) = thinking_budget(effort) {
        body["thinking"] = json!({ "type": "enabled", "budget_tokens": budget });
    }
    body
}

/// Move the conversation marker onto the newest cacheable block.
///
/// The marker advances with the conversation, so what this turn writes is what
/// the next turn reads instead of paying full price for it again. The API
/// accepts it on a text or tool_result block, which are the two shapes a turn
/// ends with; the tool-result shape is where most turns end, so marking text
/// alone drops the marker on exactly the turns with the newest content. Every
/// other block type is left unmarked.
fn mark_tail_for_cache(messages: &mut [Value]) {
    let Some(last) = messages.last_mut() else {
        return;
    };
    let Some(blocks) = last.get_mut("content").and_then(Value::as_array_mut) else {
        return;
    };
    let Some(block) = blocks.last_mut().and_then(Value::as_object_mut) else {
        return;
    };
    if !matches!(
        block.get("type").and_then(Value::as_str),
        Some("text" | "tool_result")
    ) {
        return;
    }
    block.insert(
        "cache_control".to_string(),
        json!({ "type": EPHEMERAL_CACHE_CONTROL }),
    );
}

/// Append one turn, merging it into the previous turn when the role repeats.
///
/// The API requires strictly alternating roles, so the shapes the portable
/// conversation legitimately produces have to collapse: one user turn per
/// parallel tool result, a tool result followed by real user text, and the
/// images a tool returned. Sent as separate turns they are rejected outright,
/// which would fail every multi-tool round.
fn push_turn(messages: &mut Vec<Value>, role: &str, blocks: Vec<Value>) {
    if blocks.is_empty() {
        return;
    }
    if let Some(previous) = messages.last_mut()
        && previous.get("role").and_then(Value::as_str) == Some(role)
        && let Some(content) = previous.get_mut("content").and_then(Value::as_array_mut)
    {
        content.extend(blocks);
        return;
    }
    messages.push(json!({ "role": role, "content": blocks }));
}

fn text_blocks(content: &str) -> Vec<Value> {
    if content.trim().is_empty() {
        return Vec::new();
    }
    vec![json!({ "type": "text", "text": content })]
}

fn image_blocks(attachments: &[super::ModelInputAttachment]) -> Vec<Value> {
    attachments
        .iter()
        .flat_map(|attachment| match tiled_image(attachment) {
            Some(pieces) => pieces
                .into_iter()
                .flat_map(|(label, media_type, data)| {
                    [
                        json!({ "type": "text", "text": label }),
                        image_block(&media_type, &data),
                    ]
                })
                .collect(),
            None => {
                let (media_type, data) = fitted_image(attachment);
                vec![image_block(&media_type, &data)]
            }
        })
        .collect()
}

fn image_block(media_type: &str, data: &str) -> Value {
    json!({
        "type": "image",
        "source": {
            "type": "base64",
            "media_type": media_type,
            "data": data,
        },
    })
}

/// A large attachment as a labelled overview plus tiles the model sees at
/// full resolution (`crate::image_tiles`), or `None` to send it whole.
/// History replays every image on every turn, so the answer is cached.
fn tiled_image(attachment: &super::ModelInputAttachment) -> Option<Vec<(String, String, String)>> {
    use base64::Engine as _;
    use std::hash::{Hash, Hasher};
    type Tiles = Option<Vec<(String, String, String)>>;
    static CACHE: OnceLock<std::sync::Mutex<HashMap<u64, Tiles>>> = OnceLock::new();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    attachment.data_base64.hash(&mut hasher);
    let key = hasher.finish();
    let cache = CACHE.get_or_init(Default::default);
    if let Some(hit) = cache.lock().unwrap_or_else(|p| p.into_inner()).get(&key) {
        return hit.clone();
    }
    let engine = base64::engine::general_purpose::STANDARD;
    let tiles: Tiles = engine
        .decode(&attachment.data_base64)
        .ok()
        .and_then(|bytes| crate::image_tiles::tile(&bytes))
        .map(|pieces| {
            pieces
                .into_iter()
                .map(|piece| {
                    (
                        piece.label,
                        piece.media_type.to_string(),
                        engine.encode(piece.bytes),
                    )
                })
                .collect()
        });
    let mut cache = cache.lock().unwrap_or_else(|p| p.into_inner());
    if cache.len() >= 64 {
        cache.clear();
    }
    cache.insert(key, tiles.clone());
    tiles
}

/// Anthropic rejects a request when any image exceeds this edge once the
/// request carries many images, which every long screenshot session does.
const MAX_IMAGE_EDGE: u32 = 2000;

/// The attachment, downscaled to fit `MAX_IMAGE_EDGE` when it is larger.
/// History replays every image on every turn, so resized copies are cached.
fn fitted_image(attachment: &super::ModelInputAttachment) -> (String, String) {
    use base64::Engine as _;
    use std::hash::{Hash, Hasher};
    let original = || {
        (
            attachment.media_type.clone(),
            attachment.data_base64.clone(),
        )
    };
    let engine = base64::engine::general_purpose::STANDARD;
    // Image headers sit at the start; decode a prefix to read the size cheaply.
    let prefix = &attachment.data_base64[..attachment.data_base64.len().min(64 * 1024) / 4 * 4];
    let dimensions = |bytes: &[u8]| {
        image::ImageReader::new(std::io::Cursor::new(bytes))
            .with_guessed_format()
            .ok()?
            .into_dimensions()
            .ok()
    };
    let size = engine
        .decode(prefix)
        .ok()
        .and_then(|bytes| dimensions(&bytes))
        .or_else(|| dimensions(&engine.decode(&attachment.data_base64).ok()?));
    if size.is_none_or(|(width, height)| width.max(height) <= MAX_IMAGE_EDGE) {
        return original();
    }

    static CACHE: OnceLock<std::sync::Mutex<HashMap<u64, (String, String)>>> = OnceLock::new();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    attachment.data_base64.hash(&mut hasher);
    let key = hasher.finish();
    let cache = CACHE.get_or_init(Default::default);
    if let Some(hit) = cache.lock().unwrap_or_else(|p| p.into_inner()).get(&key) {
        return hit.clone();
    }
    let Some(image) = engine
        .decode(&attachment.data_base64)
        .ok()
        .and_then(|bytes| image::load_from_memory(&bytes).ok())
    else {
        return original();
    };
    let resized = image.resize(
        MAX_IMAGE_EDGE,
        MAX_IMAGE_EDGE,
        image::imageops::FilterType::Triangle,
    );
    let mut encoded = std::io::Cursor::new(Vec::new());
    let (media_type, written) = if attachment.media_type == "image/jpeg" {
        let rgb = image::DynamicImage::ImageRgb8(resized.to_rgb8());
        (
            "image/jpeg",
            rgb.write_to(&mut encoded, image::ImageFormat::Jpeg),
        )
    } else {
        (
            "image/png",
            resized.write_to(&mut encoded, image::ImageFormat::Png),
        )
    };
    if written.is_err() {
        return original();
    }
    let fitted = (media_type.to_string(), engine.encode(encoded.into_inner()));
    let mut cache = cache.lock().unwrap_or_else(|p| p.into_inner());
    if cache.len() >= 64 {
        cache.clear();
    }
    cache.insert(key, fitted.clone());
    fitted
}

/// Whether extended thinking is requested, and the budget it may spend.
fn thinking_budget(effort: Option<&str>) -> Option<u64> {
    let effort = effort.map(str::trim).filter(|effort| !effort.is_empty())?;
    if matches!(
        effort.to_ascii_lowercase().as_str(),
        "none" | "off" | "minimal"
    ) {
        return None;
    }
    Some(THINKING_BUDGET_TOKENS)
}

fn max_output_tokens(effort: Option<&str>) -> u64 {
    if thinking_budget(effort).is_some() {
        DEFAULT_MAX_OUTPUT_TOKENS.saturating_add(THINKING_BUDGET_TOKENS)
    } else {
        DEFAULT_MAX_OUTPUT_TOKENS
    }
}

fn parse_tool_arguments(arguments: &str) -> Value {
    serde_json::from_str(arguments).unwrap_or_else(|_| json!({}))
}

/// Classify a refusal from its status and its own error body.
///
/// Anthropic reports an overloaded or failed upstream as `overloaded_error`,
/// sometimes under a 5xx status and sometimes not, and reports a prompt that
/// exceeded the window as an invalid request. Reading the declared type keeps a
/// blip retryable and a length refusal recoverable, instead of ending the turn
/// as an opaque provider failure.
pub(crate) fn anthropic_error_kind(status: u16, body: &str) -> ProviderErrorKind {
    let lowered = body.to_ascii_lowercase();
    let mentions = |needle: &str| lowered.contains(needle);
    let context_length = mentions("prompt is too long")
        || mentions("prompt too long")
        || mentions("context length")
        || mentions("context window")
        || mentions("too many tokens");
    if context_length {
        return ProviderErrorKind::ContextLength;
    }
    let transient = mentions("overloaded_error")
        || mentions("overloaded")
        || mentions("rate_limit_error")
        || status == 429
        || status >= 500;
    if transient {
        return ProviderErrorKind::ConnectionLost;
    }
    let refused = mentions("authentication_error")
        || mentions("permission_error")
        || mentions("invalid_request_error")
        || mentions("not_found_error")
        || matches!(status, 400 | 401 | 403 | 404);
    if refused {
        return ProviderErrorKind::Fatal;
    }
    ProviderErrorKind::Unknown
}

/// One model response, including opaque blocks required for continuation.
#[derive(Debug, Default)]
pub(crate) struct AnthropicStreamState {
    message: Value,
    blocks: Vec<Value>,
    input_json: HashMap<usize, String>,
    open_blocks: HashSet<usize>,
    started: bool,
    finished: bool,
    stop_reason: Option<String>,
    provider_error: Option<(String, ProviderErrorKind)>,
}

impl AnthropicStreamState {
    pub(crate) fn usage(&self, duration_ms: u64) -> ProviderCallUsage {
        let tokens = |name: &str| {
            self.message
                .get("usage")
                .and_then(|usage| usage.get(name))
                .and_then(Value::as_u64)
                .unwrap_or_default()
        };
        let input = tokens("input_tokens");
        let cached = tokens("cache_read_input_tokens");
        let written = tokens("cache_creation_input_tokens");
        let output = tokens("output_tokens");
        let all_input = input.saturating_add(cached).saturating_add(written);
        ProviderCallUsage {
            duration_ms,
            input_tokens: input,
            cached_input_tokens: cached,
            cache_creation_input_tokens: written,
            output_tokens: output,
            total_tokens: all_input.saturating_add(output),
            context_tokens: Some(all_input),
            ..Default::default()
        }
    }

    pub(crate) fn assistant_message(&self) -> ModelMessage {
        let text = self
            .blocks
            .iter()
            .filter_map(|block| {
                (block["type"] == "text")
                    .then(|| block["text"].as_str())
                    .flatten()
            })
            .collect::<String>();
        let thinking = self
            .blocks
            .iter()
            .filter_map(|block| {
                (block["type"] == "thinking")
                    .then(|| block["thinking"].as_str())
                    .flatten()
            })
            .collect::<String>();
        let tool_calls = self
            .blocks
            .iter()
            .filter(|block| block["type"] == "tool_use")
            .filter_map(|block| {
                Some(ModelToolCall::function(
                    block["id"].as_str()?.to_string(),
                    block["name"].as_str()?.to_string(),
                    block.get("input")?.to_string(),
                ))
            })
            .collect();
        ModelMessage::Assistant {
            content: (!text.is_empty()).then_some(text),
            reasoning_content: (!thinking.is_empty()).then_some(thinking),
            reasoning_details: None,
            provider_state: Some(ModelProviderState::AnthropicMessages {
                content: self.blocks.clone(),
                account_identity: None,
            }),
            tool_calls,
        }
    }

    pub(crate) fn validate_complete(&self) -> Result<(), String> {
        if let Some((message, _)) = &self.provider_error {
            return Err(message.clone());
        }
        if !self.started
            || !self.finished
            || self.stop_reason.is_none()
            || !self.open_blocks.is_empty()
        {
            return Err("Anthropic stream ended before a complete model response".to_string());
        }
        Ok(())
    }

    pub(crate) fn take_error(&mut self) -> Option<(String, ProviderErrorKind)> {
        self.provider_error.take()
    }

    pub(crate) fn finish_reason(&self) -> String {
        match self.stop_reason.as_deref() {
            Some("tool_use") => "tool_calls".to_string(),
            Some("max_tokens") => "length".to_string(),
            Some("end_turn") | None => "stop".to_string(),
            Some(other) => other.to_string(),
        }
    }

    pub(crate) fn raw_response(&self) -> Value {
        let mut response = self.message.clone();
        response["content"] = json!(self.blocks);
        response["stop_reason"] = json!(self.stop_reason);
        response
    }

    fn protocol_error(&mut self, message: &str) {
        self.provider_error.get_or_insert_with(|| {
            (
                format!("Anthropic stream protocol error: {message}"),
                ProviderErrorKind::ConnectionLost,
            )
        });
    }
}

/// Split one complete server-sent event off the front of the buffer.
fn take_sse_frame(buffer: &mut Vec<u8>) -> Option<Result<String, std::string::FromUtf8Error>> {
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
    let frame = String::from_utf8(buffer[..index].to_vec());
    buffer.drain(..index + width);
    Some(frame)
}

/// A malformed event makes continuation unsafe, even if later events arrive.
fn apply_sse_frame(
    state: &mut AnthropicStreamState,
    frame: &str,
    progress: Option<&UnboundedSender<ProviderProgress>>,
) {
    let mut event_name: Option<String> = None;
    let mut data = String::new();
    for line in frame.split("\n") {
        let line = line.trim_end_matches("\r");
        if let Some(rest) = line.strip_prefix("event:") {
            event_name = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest.trim_start());
        }
    }
    if data.trim().is_empty() || data.trim() == "[DONE]" {
        return;
    }
    let Ok(payload) = serde_json::from_str::<Value>(&data) else {
        state.protocol_error("invalid JSON in event");
        return;
    };
    let kind = event_name.unwrap_or_else(|| {
        payload
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    });
    apply_stream_event(state, &kind, &payload, progress);
}

pub(crate) fn apply_stream_event(
    state: &mut AnthropicStreamState,
    kind: &str,
    payload: &Value,
    progress: Option<&UnboundedSender<ProviderProgress>>,
) {
    if state.provider_error.is_some() {
        return;
    }
    if state.finished && kind != "ping" {
        state.protocol_error("event after message_stop");
        return;
    }
    match kind {
        "message_start" => {
            if state.started || !payload["message"].is_object() {
                state.protocol_error("invalid or duplicate message_start");
                return;
            }
            state.started = true;
            state.message = payload["message"].clone();
            state.blocks = payload["message"]["content"]
                .as_array()
                .cloned()
                .unwrap_or_default();
        }
        "content_block_start" => {
            let Some(index) = block_index(payload) else {
                state.protocol_error("missing content block index");
                return;
            };
            if !state.started
                || index != state.blocks.len()
                || index >= 16_384
                || !payload["content_block"].is_object()
            {
                state.protocol_error("invalid content block start");
                return;
            }
            let block = &payload["content_block"];
            match block["type"].as_str() {
                Some("text") => send_text(progress, block["text"].as_str().unwrap_or_default()),
                Some("thinking") => {
                    send_reasoning(progress, block["thinking"].as_str().unwrap_or_default())
                }
                Some("tool_use") => {
                    if let Some(sender) = progress {
                        let _ = sender.send(ProviderProgress::ToolCallGenerating {
                            id: block["id"].as_str().map(str::to_string),
                        });
                    }
                }
                _ => {}
            }
            state.blocks.push(block.clone());
            state.open_blocks.insert(index);
        }
        "content_block_delta" => {
            let Some(index) =
                block_index(payload).filter(|index| state.open_blocks.contains(index))
            else {
                state.protocol_error("delta for an unopened content block");
                return;
            };
            let delta = &payload["delta"];
            let block = &mut state.blocks[index];
            match delta["type"].as_str() {
                Some("text_delta" | "thinking_delta" | "signature_delta") => {
                    let field = match delta["type"].as_str() {
                        Some("text_delta") => "text",
                        Some("thinking_delta") => "thinking",
                        _ => "signature",
                    };
                    let Some(text) = delta[field].as_str() else {
                        state.protocol_error("non-string content delta");
                        return;
                    };
                    let mut value = block[field].as_str().unwrap_or_default().to_string();
                    value.push_str(text);
                    block[field] = json!(value);
                    match field {
                        "text" => send_text(progress, text),
                        "thinking" => send_reasoning(progress, text),
                        _ => {}
                    }
                }
                Some("input_json_delta") => {
                    if block["type"] != "tool_use" {
                        state.protocol_error("tool input delta for a non-tool block");
                        return;
                    }
                    let Some(partial) = delta["partial_json"].as_str() else {
                        state.protocol_error("non-string tool input delta");
                        return;
                    };
                    state.input_json.entry(index).or_default().push_str(partial);
                    if let Some(sender) = progress {
                        let _ = sender.send(ProviderProgress::ToolCallInputDelta {
                            id: block["id"].as_str().map(str::to_string),
                        });
                    }
                }
                Some("citations_delta") => {
                    let citation = &delta["citation"];
                    if !citation.is_object() {
                        state.protocol_error("invalid citation delta");
                        return;
                    }
                    if block.get("citations").is_none() {
                        block["citations"] = json!([]);
                    }
                    if let Some(citations) = block["citations"].as_array_mut() {
                        citations.push(citation.clone());
                    } else {
                        state.protocol_error("invalid citations block");
                    }
                }
                _ => state.protocol_error("unsupported content delta"),
            }
        }
        "content_block_stop" => {
            let Some(index) = block_index(payload).filter(|index| state.open_blocks.remove(index))
            else {
                state.protocol_error("stop for an unopened content block");
                return;
            };
            let block = &mut state.blocks[index];
            if let Some(partial) = state.input_json.remove(&index)
                && !partial.is_empty()
            {
                let Ok(input) = serde_json::from_str::<Value>(&partial) else {
                    state.protocol_error("incomplete tool input JSON");
                    return;
                };
                block["input"] = input;
            }
            if block["type"] == "tool_use" {
                let (Some(id), Some(name)) = (block["id"].as_str(), block["name"].as_str()) else {
                    state.protocol_error("tool call omitted identity or name");
                    return;
                };
                if !block["input"].is_object() {
                    state.protocol_error("tool call input is not an object");
                    return;
                }
                if let Some(sender) = progress {
                    let _ = sender.send(ProviderProgress::ToolCallStarted {
                        id: id.to_string(),
                        name: name.to_string(),
                        input: block["input"].clone(),
                    });
                }
            }
        }
        "message_delta" => {
            if !state.started {
                state.protocol_error("message delta before message_start");
                return;
            }
            if let Some(delta) = payload["delta"].as_object() {
                state
                    .message
                    .as_object_mut()
                    .expect("message_start validated object")
                    .extend(delta.clone());
                if let Some(reason) = delta.get("stop_reason").and_then(Value::as_str) {
                    state.stop_reason = Some(reason.to_string());
                }
            }
            if let Some(usage) = payload.get("usage") {
                absorb_usage(state, usage);
            }
            if let Some(context) = payload.get("context_management") {
                state.message["context_management"] = context.clone();
            }
        }
        "message_stop" => state.finished = true,
        "error" => {
            let message = payload
                .pointer("/error/message")
                .and_then(Value::as_str)
                .unwrap_or("stream failed");
            state.provider_error = Some((
                format!("Anthropic stream error: {message}"),
                anthropic_error_kind(200, &payload.to_string()),
            ));
        }
        _ => {}
    }
}

fn block_index(payload: &Value) -> Option<usize> {
    payload
        .get("index")
        .and_then(Value::as_u64)
        .and_then(|index| usize::try_from(index).ok())
}

fn absorb_usage(state: &mut AnthropicStreamState, usage: &Value) {
    let Some(fields) = usage.as_object() else {
        return;
    };
    if !state.message["usage"].is_object() {
        state.message["usage"] = json!({});
    }
    let target = state.message["usage"]
        .as_object_mut()
        .expect("usage object");
    // Deltas report running totals and can omit earlier breakdowns.
    for (key, value) in fields {
        if let (Some(existing), Some(incoming)) = (
            target.get_mut(key).and_then(Value::as_object_mut),
            value.as_object(),
        ) {
            existing.extend(incoming.clone());
        } else {
            target.insert(key.clone(), value.clone());
        }
    }
}

fn send_text(progress: Option<&UnboundedSender<ProviderProgress>>, text: &str) {
    if text.is_empty() {
        return;
    }
    if let Some(sender) = progress {
        let _ = sender.send(ProviderProgress::Bytes {
            stream: ProviderProgressStream::Stdout,
            chunk: text.as_bytes().to_vec(),
        });
    }
}

fn send_reasoning(progress: Option<&UnboundedSender<ProviderProgress>>, text: &str) {
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
            model: None,
            effort: None,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{ModelInputAttachment, ModelToolDefinition};

    fn request(messages: Vec<ModelMessage>, tools: Vec<ModelToolDefinition>) -> ModelTurnRequest {
        ModelTurnRequest {
            fast: false,
            request_id: None,
            session_id: None,
            prompt_cache_key: None,
            turn_routing: Default::default(),
            messages,
            tools,
            output_schema: None,
        }
    }

    /// One server-sent event, the way the service frames it.
    fn frame(event: &str, payload: Value) -> String {
        format!("event: {event}\ndata: {payload}")
    }

    #[test]
    fn a_tool_result_run_and_following_user_text_share_one_turn() {
        // The API requires strictly alternating roles, so a run of tool results,
        // the images one of them returned, and the user text that follows all
        // have to arrive as a single user turn. Sent as separate turns they are
        // rejected, which would fail every round with more than one tool call.
        let attachment = ModelInputAttachment {
            media_type: "image/png".to_string(),
            data_base64: "AAAA".to_string(),
            filename: None,
        };
        let body = messages_request_body(
            "claude-sonnet-4-5",
            None,
            &request(
                vec![
                    ModelMessage::user("run both"),
                    ModelMessage::assistant(
                        Some("calling".to_string()),
                        None,
                        None,
                        vec![
                            ModelToolCall::function(
                                "toolu_1".to_string(),
                                "read_file".to_string(),
                                "{}".to_string(),
                            ),
                            ModelToolCall::function(
                                "toolu_2".to_string(),
                                "read_file".to_string(),
                                "{}".to_string(),
                            ),
                        ],
                    ),
                    ModelMessage::tool("toolu_1", "first"),
                    ModelMessage::Tool {
                        tool_call_id: "toolu_2".to_string(),
                        content: "second".to_string(),
                        attachments: vec![attachment],
                    },
                    ModelMessage::user("and now this"),
                ],
                Vec::new(),
            ),
        );

        let messages = body["messages"].as_array().expect("messages");
        let roles = messages
            .iter()
            .map(|message| message["role"].as_str().unwrap_or_default().to_string())
            .collect::<Vec<_>>();
        assert_eq!(roles, vec!["user", "assistant", "user"]);
        assert!(
            roles.windows(2).all(|pair| pair[0] != pair[1]),
            "roles must alternate: {roles:?}"
        );

        let blocks = messages[2]["content"].as_array().expect("content");
        let kinds = blocks
            .iter()
            .map(|block| block["type"].as_str().unwrap_or_default().to_string())
            .collect::<Vec<_>>();
        assert_eq!(kinds, vec!["tool_result", "tool_result", "text"]);
        assert_eq!(blocks[0]["tool_use_id"], json!("toolu_1"));
        assert_eq!(blocks[1]["tool_use_id"], json!("toolu_2"));
        assert_eq!(blocks[1]["content"][1]["type"], "image");
        assert_eq!(blocks[2]["text"], json!("and now this"));
    }

    #[test]
    fn the_request_hoists_system_and_encodes_tools_and_tool_results() {
        let body = messages_request_body(
            "claude-sonnet-4-5",
            None,
            &request(
                vec![
                    ModelMessage::System {
                        content: "first".to_string(),
                    },
                    ModelMessage::System {
                        content: "second".to_string(),
                    },
                    ModelMessage::user("hello"),
                    ModelMessage::assistant(
                        Some("calling".to_string()),
                        Some("because".to_string()),
                        None,
                        vec![ModelToolCall::function(
                            "toolu_1".to_string(),
                            "read_file".to_string(),
                            "{\"path\":\"a\"}".to_string(),
                        )],
                    ),
                    ModelMessage::tool("toolu_1", "file body"),
                ],
                vec![
                    ModelToolDefinition::new(
                        "read_file",
                        "read one file",
                        json!({ "type": "object", "properties": {} }),
                    )
                    .expect("tool definition"),
                ],
            ),
        );

        // Every system message is hoisted into the single top-level field.
        assert_eq!(body["system"][0]["type"], json!("text"));
        assert_eq!(body["system"][0]["text"], json!("first\n\nsecond"));
        assert_eq!(
            body["system"][0]["cache_control"]["type"],
            json!("ephemeral")
        );
        assert_eq!(body["messages"][0]["role"], json!("user"));
        assert_eq!(body["messages"][0]["content"][0]["type"], json!("text"));
        // Portable reasoning without a native signature cannot be replayed.
        assert_eq!(body["messages"][1]["content"][0]["type"], json!("text"));
        assert_eq!(body["messages"][1]["content"][1]["type"], json!("tool_use"));
        assert_eq!(
            body["messages"][1]["content"][1]["input"],
            json!({ "path": "a" })
        );
        // A tool result is a user message holding one tool_result block.
        assert_eq!(body["messages"][2]["role"], json!("user"));
        assert_eq!(
            body["messages"][2]["content"][0]["type"],
            json!("tool_result")
        );
        assert_eq!(
            body["messages"][2]["content"][0]["tool_use_id"],
            json!("toolu_1")
        );
        assert_eq!(body["tools"][0]["name"], json!("read_file"));
        assert_eq!(body["tools"][0]["input_schema"]["type"], json!("object"));
        // The API requires a budget, so one is always present.
        assert_eq!(body["max_tokens"], json!(DEFAULT_MAX_OUTPUT_TOKENS));
        assert!(body.get("thinking").is_none());
    }

    #[test]
    fn the_cache_marker_budget_holds_for_every_tail_shape() {
        let text_tail = messages_request_body(
            "claude-sonnet-4-5",
            None,
            &request(
                vec![
                    ModelMessage::System {
                        content: "stable instructions".to_string(),
                    },
                    ModelMessage::user("cache me"),
                ],
                Vec::new(),
            ),
        );
        assert_eq!(
            text_tail["messages"][0]["content"][0]["cache_control"]["type"],
            json!("ephemeral")
        );
        assert_eq!(
            text_tail.to_string().matches("cache_control").count(),
            2,
            "one marker on the system block and one on the newest text: {text_tail}"
        );

        // A tail of tool results carries the tail marker too. This is where
        // most turns end, so a text-only marker would drop the marker on the
        // turns that have the most new content to cache.
        let tool_tail = messages_request_body(
            "claude-sonnet-4-5",
            None,
            &request(
                vec![
                    ModelMessage::System {
                        content: "stable instructions".to_string(),
                    },
                    ModelMessage::assistant(
                        Some("calling".to_string()),
                        None,
                        None,
                        vec![ModelToolCall::function(
                            "toolu_1".to_string(),
                            "read_file".to_string(),
                            "{}".to_string(),
                        )],
                    ),
                    ModelMessage::tool("toolu_1", "contents"),
                ],
                Vec::new(),
            ),
        );
        assert_eq!(
            tool_tail["messages"][1]["content"][0]["cache_control"]["type"],
            json!("ephemeral")
        );
        assert_eq!(
            tool_tail.to_string().matches("cache_control").count(),
            2,
            "one marker on the system block and one on the tool result: {tool_tail}"
        );

        // Tools bring a third marker, spent on the last definition so the
        // schemas stay cached when the system prompt changes.
        let with_tools = messages_request_body(
            "claude-sonnet-4-5",
            None,
            &request(
                vec![
                    ModelMessage::System {
                        content: "stable instructions".to_string(),
                    },
                    ModelMessage::user("cache me"),
                ],
                vec![
                    ModelToolDefinition::new(
                        "read_file",
                        "read one file",
                        json!({ "type": "object", "properties": {} }),
                    )
                    .expect("tool definition"),
                    ModelToolDefinition::new(
                        "write_file",
                        "write one file",
                        json!({ "type": "object", "properties": {} }),
                    )
                    .expect("tool definition"),
                ],
            ),
        );
        assert!(
            with_tools["tools"][0].get("cache_control").is_none(),
            "only the last tool definition is marked: {with_tools}"
        );
        assert_eq!(
            with_tools["tools"][1]["cache_control"]["type"],
            json!("ephemeral")
        );
        assert_eq!(
            with_tools.to_string().matches("cache_control").count(),
            3,
            "last tool, system, newest text, and no more: {with_tools}"
        );
    }

    #[test]
    fn an_attachment_becomes_an_image_block_next_to_the_text() {
        let attachment = ModelInputAttachment {
            media_type: "image/png".to_string(),
            data_base64: "AAAA".to_string(),
            filename: None,
        };
        let body = messages_request_body(
            "claude-sonnet-4-5",
            None,
            &request(
                vec![ModelMessage::user_with_attachments(
                    "look",
                    vec![attachment],
                )],
                Vec::new(),
            ),
        );
        assert_eq!(body["messages"][0]["content"][0]["type"], json!("text"));
        assert_eq!(body["messages"][0]["content"][1]["type"], json!("image"));
        assert_eq!(
            body["messages"][0]["content"][1]["source"]["media_type"],
            json!("image/png")
        );
        assert_eq!(
            body["messages"][0]["content"][1]["source"]["data"],
            json!("AAAA")
        );
    }

    #[test]
    fn large_screenshot_is_tiled_and_small_image_stays_whole() {
        use base64::Engine as _;
        let engine = base64::engine::general_purpose::STANDARD;
        let encode = |width, height| {
            let mut png = std::io::Cursor::new(Vec::new());
            image::DynamicImage::new_rgb8(width, height)
                .write_to(&mut png, image::ImageFormat::Png)
                .unwrap();
            super::super::ModelInputAttachment {
                media_type: "image/png".to_string(),
                data_base64: engine.encode(png.into_inner()),
                filename: None,
            }
        };
        let size = |block: &Value| {
            let bytes = engine
                .decode(block["source"]["data"].as_str().unwrap())
                .unwrap();
            image::load_from_memory(&bytes).unwrap().dimensions()
        };
        use image::GenericImageView as _;
        let large = encode(2560, 1440);
        let expected = crate::image_tiles::piece_count(&engine.decode(&large.data_base64).unwrap());
        assert!(expected > 1);
        let blocks = image_blocks(&[large, encode(800, 600)]);
        assert_eq!(blocks.len(), 2 * expected + 1);
        for (index, pair) in blocks[..2 * expected].chunks_exact(2).enumerate() {
            assert_eq!(pair[0]["type"], "text");
            let label = pair[0]["text"].as_str().unwrap().to_ascii_lowercase();
            assert!(label.contains(if index == 0 { "overview" } else { "tile" }));
            let (width, height) = size(&pair[1]);
            assert!(width.max(height) <= 1568);
        }
        assert_eq!(size(blocks.last().unwrap()), (800, 600));
    }

    #[test]
    fn effort_enables_thinking_and_raises_the_budget() {
        let thinking = messages_request_body(
            "claude-sonnet-4-5",
            Some("high"),
            &request(vec![ModelMessage::user("think")], Vec::new()),
        );
        assert_eq!(thinking["thinking"]["type"], json!("enabled"));
        assert_eq!(
            thinking["thinking"]["budget_tokens"],
            json!(THINKING_BUDGET_TOKENS)
        );
        assert_eq!(
            thinking["max_tokens"],
            json!(DEFAULT_MAX_OUTPUT_TOKENS + THINKING_BUDGET_TOKENS)
        );

        let off = messages_request_body(
            "claude-sonnet-4-5",
            Some("none"),
            &request(vec![ModelMessage::user("think")], Vec::new()),
        );
        assert!(off.get("thinking").is_none());
    }

    #[test]
    fn stream_events_carry_text_thinking_tools_and_usage() {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut state = AnthropicStreamState::default();
        let frames = [
            frame(
                "message_start",
                json!({
                    "type": "message_start",
                    "message": {
                        "model": "claude-sonnet-4-5",
                        "usage": {
                            "input_tokens": 10,
                            "cache_creation_input_tokens": 30,
                            "cache_creation": { "ephemeral_1h_input_tokens": 30 },
                            "cache_read_input_tokens": 1200,
                            "output_tokens": 1
                        }
                    }
                }),
            ),
            frame(
                "content_block_start",
                json!({ "type": "content_block_start", "index": 0, "content_block": { "type": "thinking" } }),
            ),
            frame(
                "content_block_delta",
                json!({ "type": "content_block_delta", "index": 0, "delta": { "type": "thinking_delta", "thinking": "weighing" } }),
            ),
            frame(
                "content_block_delta",
                json!({ "index": 0, "delta": { "type": "signature_delta", "signature": "opaque-signed-state" } }),
            ),
            frame("content_block_stop", json!({ "index": 0 })),
            frame(
                "content_block_start",
                json!({ "type": "content_block_start", "index": 1, "content_block": { "type": "text" } }),
            ),
            frame(
                "content_block_delta",
                json!({ "type": "content_block_delta", "index": 1, "delta": { "type": "text_delta", "text": "hello" } }),
            ),
            frame(
                "content_block_delta",
                json!({ "index": 1, "delta": { "type": "citations_delta", "citation": { "type": "char_location", "cited_text": "source", "document_index": 0, "start_char_index": 0, "end_char_index": 6 } } }),
            ),
            frame("content_block_stop", json!({ "index": 1 })),
            frame(
                "content_block_start",
                json!({
                    "type": "content_block_start",
                    "index": 2,
                    "content_block": { "type": "tool_use", "id": "toolu_1", "name": "read_file" }
                }),
            ),
            frame(
                "content_block_delta",
                json!({
                    "type": "content_block_delta",
                    "index": 2,
                    "delta": { "type": "input_json_delta", "partial_json": "{\"path\":" }
                }),
            ),
            frame(
                "content_block_delta",
                json!({
                    "type": "content_block_delta",
                    "index": 2,
                    "delta": { "type": "input_json_delta", "partial_json": "\"a\"}" }
                }),
            ),
            frame(
                "content_block_stop",
                json!({ "type": "content_block_stop", "index": 2 }),
            ),
            frame(
                "content_block_start",
                json!({ "index": 3, "content_block": { "type": "redacted_thinking", "data": "opaque-redacted-state" } }),
            ),
            frame("content_block_stop", json!({ "index": 3 })),
            frame(
                "message_delta",
                json!({
                    "type": "message_delta",
                    "delta": { "stop_reason": "tool_use" },
                    "usage": { "output_tokens": 5 }
                }),
            ),
            frame("message_stop", json!({ "type": "message_stop" })),
        ];
        for event in frames {
            apply_sse_frame(&mut state, &event, Some(&sender));
        }
        drop(sender);
        state.validate_complete().expect("complete response");
        let persisted = serde_json::to_vec(&state.assistant_message()).expect("persist");
        let restored = serde_json::from_slice(&persisted).expect("restore");
        let replay = messages_request_body(
            "claude-sonnet-4-5",
            Some("high"),
            &request(
                vec![
                    ModelMessage::user("read a"),
                    restored,
                    ModelMessage::tool("toolu_1", "file body"),
                ],
                Vec::new(),
            ),
        );
        assert_eq!(
            replay["messages"][1]["content"],
            state.raw_response()["content"]
        );
        assert_eq!(
            replay["messages"][1]["content"][0]["signature"],
            "opaque-signed-state"
        );
        assert_eq!(
            replay["messages"][1]["content"][3]["data"],
            "opaque-redacted-state"
        );
        assert_eq!(
            state.raw_response()["usage"]["cache_creation"]["ephemeral_1h_input_tokens"],
            30
        );

        let mut streamed_text = String::new();
        let mut streamed_reasoning = String::new();
        let mut started_tools: Vec<String> = Vec::new();
        while let Ok(event) = receiver.try_recv() {
            match event {
                ProviderProgress::Bytes { chunk, .. } => {
                    streamed_text.push_str(&String::from_utf8_lossy(&chunk));
                }
                ProviderProgress::ProviderEvent {
                    kind, content_text, ..
                } if kind == "reasoning_delta" => {
                    streamed_reasoning.push_str(content_text.as_deref().unwrap_or_default());
                }
                ProviderProgress::ToolCallStarted { name, .. } => started_tools.push(name),
                _ => {}
            }
        }
        assert_eq!(streamed_text, "hello");
        assert_eq!(streamed_reasoning, "weighing");
        assert_eq!(started_tools, vec!["read_file".to_string()]);

        let usage = state.usage(7);
        assert_eq!(usage.duration_ms, 7);
        assert_eq!(usage.input_tokens, 10);
        assert_eq!(usage.cached_input_tokens, 1200);
        assert_eq!(usage.cache_creation_input_tokens, 30);
        assert_eq!(usage.output_tokens, 5);
        assert_eq!(usage.total_tokens, 1245);

        let ModelMessage::Assistant {
            content,
            reasoning_content,
            tool_calls,
            ..
        } = state.assistant_message()
        else {
            panic!("a streamed turn is an assistant message");
        };
        assert_eq!(content.as_deref(), Some("hello"));
        assert_eq!(reasoning_content.as_deref(), Some("weighing"));
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].id, "toolu_1");
        assert_eq!(tool_calls[0].function.name, "read_file");
        assert_eq!(tool_calls[0].function.arguments, "{\"path\":\"a\"}");
        assert_eq!(state.finish_reason(), "tool_calls");
    }

    #[test]
    fn frame_splitting_survives_a_chunk_boundary() {
        let mut buffer: Vec<u8> = Vec::new();
        // The blank line that ends a frame can arrive split across two reads,
        // so a frame is not complete until its terminator is.
        let ping = format!("{}\n\n", frame("ping", json!({ "type": "ping" })));
        let (head, tail) = ping.split_at(ping.len() - 1);
        buffer.extend_from_slice(head.as_bytes());
        assert!(take_sse_frame(&mut buffer).is_none());
        buffer.extend_from_slice(tail.as_bytes());
        let first = take_sse_frame(&mut buffer)
            .expect("first frame")
            .expect("UTF-8");
        assert!(first.contains("ping"));
        assert!(take_sse_frame(&mut buffer).is_none());

        let stop = format!(
            "{}\n\n",
            frame("message_stop", json!({ "type": "message_stop" }))
        );
        buffer.extend_from_slice(stop.as_bytes());
        let second = take_sse_frame(&mut buffer)
            .expect("second frame")
            .expect("UTF-8");
        assert!(second.contains("message_stop"));
        assert!(take_sse_frame(&mut buffer).is_none());
    }

    #[test]
    fn incomplete_or_corrupt_streams_cannot_commit_an_assistant_message() {
        for corruption in ["missing_stop", "open_block", "bad_json", "bad_tool_input"] {
            let mut state = AnthropicStreamState::default();
            apply_stream_event(
                &mut state,
                "message_start",
                &json!({"message": {"content": []}}),
                None,
            );
            apply_stream_event(
                &mut state,
                "content_block_start",
                &json!({"index": 0, "content_block": {"type": "tool_use", "id": "toolu_1", "name": "read_file", "input": {}}}),
                None,
            );
            if corruption == "bad_json" {
                apply_sse_frame(
                    &mut state,
                    "event: content_block_delta\ndata: {broken",
                    None,
                );
            }
            if corruption == "bad_tool_input" {
                apply_stream_event(
                    &mut state,
                    "content_block_delta",
                    &json!({"index": 0, "delta": {"type": "input_json_delta", "partial_json": "{\"path\":"}}),
                    None,
                );
            }
            if corruption != "open_block" {
                apply_stream_event(&mut state, "content_block_stop", &json!({"index": 0}), None);
            }
            apply_stream_event(
                &mut state,
                "message_delta",
                &json!({"delta": {"stop_reason": "tool_use"}}),
                None,
            );
            if corruption != "missing_stop" {
                apply_stream_event(&mut state, "message_stop", &json!({}), None);
            }
            assert!(state.validate_complete().is_err(), "{corruption}");
        }
    }

    #[test]
    fn empty_tool_delta_preserves_the_initial_input_object() {
        let mut state = AnthropicStreamState::default();
        for (kind, payload) in [
            ("message_start", json!({"message": {"content": []}})),
            (
                "content_block_start",
                json!({"index": 0, "content_block": {"type": "tool_use", "id": "toolu_1", "name": "probe", "input": {}}}),
            ),
            (
                "content_block_delta",
                json!({"index": 0, "delta": {"type": "input_json_delta", "partial_json": ""}}),
            ),
            ("content_block_stop", json!({"index": 0})),
            (
                "message_delta",
                json!({"delta": {"stop_reason": "tool_use"}}),
            ),
            ("message_stop", json!({})),
        ] {
            apply_stream_event(&mut state, kind, &payload, None);
        }
        state
            .validate_complete()
            .expect("complete zero-argument tool call");
        assert_eq!(state.raw_response()["content"][0]["input"], json!({}));
    }

    #[test]
    fn refusals_are_classified_by_their_own_error_type() {
        let overloaded = json!({ "error": { "type": "overloaded_error" } }).to_string();
        assert_eq!(
            anthropic_error_kind(529, &overloaded),
            ProviderErrorKind::ConnectionLost
        );
        let too_long = json!({
            "error": {
                "type": "invalid_request_error",
                "message": "prompt is too long: 210000 tokens"
            }
        })
        .to_string();
        assert_eq!(
            anthropic_error_kind(400, &too_long),
            ProviderErrorKind::ContextLength
        );
        let invalid = json!({ "error": { "type": "invalid_request_error" } }).to_string();
        assert_eq!(
            anthropic_error_kind(400, &invalid),
            ProviderErrorKind::Fatal
        );
        let unauthenticated = json!({ "error": { "type": "authentication_error" } }).to_string();
        assert_eq!(
            anthropic_error_kind(401, &unauthenticated),
            ProviderErrorKind::Fatal
        );
        let unknown = json!({ "error": { "type": "mystery" } }).to_string();
        assert_eq!(
            anthropic_error_kind(200, &unknown),
            ProviderErrorKind::Unknown
        );
    }
}
