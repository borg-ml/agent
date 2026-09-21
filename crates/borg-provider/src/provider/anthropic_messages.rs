//! Native Anthropic Messages API access for the API-key lane.
//!
//! Model access only: Borg owns the agent loop, tools, persistence and policy,
//! and this route bills the Anthropic API key the user configured. It is
//! deliberately separate from the Claude subscription lane, which runs the
//! unmodified Claude Code binary. Subscription credentials are never replayed
//! here, and this key is never used to stand in for them.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use futures::StreamExt;
use serde_json::{Value, json};
use tokio::sync::mpsc::UnboundedSender;

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
        let mut stream = response.bytes_stream();
        let mut stream_failure: Option<(String, ProviderErrorKind)> = None;
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => buffer.extend_from_slice(&bytes),
                Err(error) => {
                    stream_failure = Some((
                        format!("{ANTHROPIC_LABEL} streaming response failed: {error}"),
                        ProviderErrorKind::from_transport(&error),
                    ));
                    break;
                }
            }
            while let Some(frame) = take_sse_frame(&mut buffer) {
                apply_sse_frame(&mut state, &frame, progress.as_ref());
            }
            if buffer.len() > STREAM_MAX_BYTES {
                stream_failure = Some((
                    format!("{ANTHROPIC_LABEL} stream exceeded {STREAM_MAX_BYTES} bytes"),
                    ProviderErrorKind::ConnectionLost,
                ));
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
        if let Some((message, kind)) = state.provider_error.take() {
            return Err(ProviderCallError {
                message,
                trace: Box::new(trace),
                session_id: None,
                kind,
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
/// conversation: tool results become user content blocks, and assistant
/// thinking is not replayed because the API only accepts it back with the
/// signature it issued.
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
                ..
            } => {
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
                let mut blocks = vec![json!({
                    "type": "tool_result",
                    "tool_use_id": tool_call_id,
                    "content": content,
                })];
                // The API cannot attach images to a tool result, so they ride
                // along as later blocks of the same turn rather than being
                // dropped or split into a second turn.
                blocks.extend(image_blocks(attachments));
                push_turn(&mut messages, "user", blocks);
            }
        }
    }

    let mut body = json!({
        "model": model,
        "max_tokens": max_output_tokens(effort),
        "messages": messages,
    });
    if !system_parts.is_empty() {
        body["system"] = json!(system_parts.join("\n\n"));
    }
    if !request.tools.is_empty() {
        body["tools"] = Value::Array(
            request
                .tools
                .iter()
                .map(|tool| {
                    json!({
                        "name": tool.name,
                        "description": tool.description,
                        "input_schema": tool.input_schema,
                    })
                })
                .collect(),
        );
    }
    if let Some(budget) = thinking_budget(effort) {
        body["thinking"] = json!({ "type": "enabled", "budget_tokens": budget });
    }
    body
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
        .map(|attachment| {
            json!({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": attachment.media_type,
                    "data": attachment.data_base64,
                },
            })
        })
        .collect()
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

#[derive(Debug, Clone)]
struct AnthropicToolCall {
    id: String,
    name: String,
    arguments: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AnthropicBlock {
    Text,
    Thinking,
    ToolUse(usize),
    Other,
}

/// Everything one streamed turn has produced so far.
#[derive(Debug, Default)]
pub(crate) struct AnthropicStreamState {
    text: String,
    thinking: String,
    tool_calls: Vec<AnthropicToolCall>,
    blocks: Vec<AnthropicBlock>,
    input_tokens: u64,
    cache_creation_input_tokens: u64,
    cache_read_input_tokens: u64,
    output_tokens: u64,
    stop_reason: Option<String>,
    model: Option<String>,
    provider_error: Option<(String, ProviderErrorKind)>,
}

impl AnthropicStreamState {
    fn usage(&self, duration_ms: u64) -> ProviderCallUsage {
        ProviderCallUsage {
            duration_ms,
            input_tokens: self.input_tokens,
            cached_input_tokens: self.cache_read_input_tokens,
            cache_creation_input_tokens: self.cache_creation_input_tokens,
            output_tokens: self.output_tokens,
            total_tokens: self.input_tokens
                + self.cache_creation_input_tokens
                + self.cache_read_input_tokens
                + self.output_tokens,
            context_tokens: Some(
                self.input_tokens + self.cache_creation_input_tokens + self.cache_read_input_tokens,
            ),
            ..Default::default()
        }
    }

    fn assistant_message(&self) -> ModelMessage {
        let tool_calls = self
            .tool_calls
            .iter()
            .filter(|call| !call.id.is_empty() && !call.name.is_empty())
            .map(|call| {
                ModelToolCall::function(call.id.clone(), call.name.clone(), call.arguments.clone())
            })
            .collect::<Vec<_>>();
        ModelMessage::assistant(
            (!self.text.trim().is_empty()).then(|| self.text.clone()),
            (!self.thinking.trim().is_empty()).then(|| self.thinking.clone()),
            None,
            tool_calls,
        )
    }

    /// The portable finish reason for the reported stop reason.
    fn finish_reason(&self) -> String {
        match self.stop_reason.as_deref() {
            Some("tool_use") => "tool_calls".to_string(),
            Some("max_tokens") => "length".to_string(),
            Some("end_turn") | None => "stop".to_string(),
            Some(other) => other.to_string(),
        }
    }

    fn raw_response(&self) -> Value {
        json!({
            "stop_reason": self.stop_reason,
            "model": self.model,
            "usage": {
                "input_tokens": self.input_tokens,
                "cache_creation_input_tokens": self.cache_creation_input_tokens,
                "cache_read_input_tokens": self.cache_read_input_tokens,
                "output_tokens": self.output_tokens,
            },
            "blocks": self.blocks.len(),
        })
    }
}

/// Split one complete server-sent event off the front of the buffer.
fn take_sse_frame(buffer: &mut Vec<u8>) -> Option<String> {
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
    let frame = String::from_utf8_lossy(&buffer[..index]).to_string();
    buffer.drain(..index + width);
    Some(frame)
}

/// Apply one server-sent event. Framing problems are ignored rather than
/// failing the turn: a malformed frame the adapter cannot read is not evidence
/// that the model refused, and a real failure arrives as its own error event.
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

fn apply_stream_event(
    state: &mut AnthropicStreamState,
    kind: &str,
    payload: &Value,
    progress: Option<&UnboundedSender<ProviderProgress>>,
) {
    match kind {
        "message_start" => {
            state.model = payload
                .pointer("/message/model")
                .and_then(Value::as_str)
                .map(str::to_string);
            if let Some(usage) = payload.pointer("/message/usage") {
                absorb_usage(state, usage);
            }
        }
        "content_block_start" => {
            let index = block_index(payload);
            let block = payload.get("content_block").cloned().unwrap_or(Value::Null);
            let block_kind = match block.get("type").and_then(Value::as_str) {
                Some("text") => {
                    if let Some(text) = block.get("text").and_then(Value::as_str) {
                        state.text.push_str(text);
                        send_text(progress, text);
                    }
                    AnthropicBlock::Text
                }
                Some("thinking") | Some("redacted_thinking") => AnthropicBlock::Thinking,
                Some("tool_use") => {
                    let id = block
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let name = block
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    state.tool_calls.push(AnthropicToolCall {
                        id: id.clone(),
                        name,
                        arguments: String::new(),
                    });
                    if let Some(sender) = progress {
                        let _ = sender.send(ProviderProgress::ToolCallGenerating {
                            id: (!id.is_empty()).then_some(id),
                        });
                    }
                    AnthropicBlock::ToolUse(state.tool_calls.len() - 1)
                }
                _ => AnthropicBlock::Other,
            };
            set_block(state, index, block_kind);
        }
        "content_block_delta" => {
            let index = block_index(payload);
            let delta = payload.get("delta").cloned().unwrap_or(Value::Null);
            match delta.get("type").and_then(Value::as_str) {
                Some("text_delta") => {
                    if let Some(text) = delta.get("text").and_then(Value::as_str) {
                        state.text.push_str(text);
                        send_text(progress, text);
                    }
                }
                Some("thinking_delta") => {
                    if let Some(text) = delta.get("thinking").and_then(Value::as_str) {
                        state.thinking.push_str(text);
                        send_reasoning(progress, text);
                    }
                }
                Some("input_json_delta") => {
                    let partial = delta
                        .get("partial_json")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if partial.is_empty() {
                        return;
                    }
                    let Some(AnthropicBlock::ToolUse(call)) = state.blocks.get(index).copied()
                    else {
                        return;
                    };
                    if let Some(call) = state.tool_calls.get_mut(call) {
                        call.arguments.push_str(partial);
                        let id = call.id.clone();
                        if let Some(sender) = progress {
                            let _ = sender.send(ProviderProgress::ToolCallInputDelta {
                                id: (!id.is_empty()).then_some(id),
                            });
                        }
                    }
                }
                _ => {}
            }
        }
        "content_block_stop" => {
            let index = block_index(payload);
            let Some(AnthropicBlock::ToolUse(call)) = state.blocks.get(index).copied() else {
                return;
            };
            let Some(call) = state.tool_calls.get(call) else {
                return;
            };
            if let Some(sender) = progress {
                let _ = sender.send(ProviderProgress::ToolCallStarted {
                    id: call.id.clone(),
                    name: call.name.clone(),
                    input: parse_tool_arguments(&call.arguments),
                });
            }
        }
        "message_delta" => {
            if let Some(reason) = payload
                .pointer("/delta/stop_reason")
                .and_then(Value::as_str)
            {
                state.stop_reason = Some(reason.to_string());
            }
            if let Some(usage) = payload.get("usage") {
                absorb_usage(state, usage);
            }
        }
        "error" => {
            let message = payload
                .pointer("/error/message")
                .and_then(Value::as_str)
                .unwrap_or("stream failed");
            state.provider_error = Some((
                format!("{ANTHROPIC_LABEL} stream error: {message}"),
                anthropic_error_kind(200, &payload.to_string()),
            ));
        }
        _ => {}
    }
}

fn block_index(payload: &Value) -> usize {
    payload
        .get("index")
        .and_then(Value::as_u64)
        .unwrap_or_default() as usize
}

fn set_block(state: &mut AnthropicStreamState, index: usize, kind: AnthropicBlock) {
    if state.blocks.len() <= index {
        state.blocks.resize(index + 1, AnthropicBlock::Other);
    }
    state.blocks[index] = kind;
}

fn absorb_usage(state: &mut AnthropicStreamState, usage: &Value) {
    let field = |name: &str| usage.get(name).and_then(Value::as_u64);
    // Anthropic reports running totals, so the latest value replaces the
    // earlier one rather than adding to it.
    if let Some(value) = field("input_tokens") {
        state.input_tokens = value;
    }
    if let Some(value) = field("cache_creation_input_tokens") {
        state.cache_creation_input_tokens = value;
    }
    if let Some(value) = field("cache_read_input_tokens") {
        state.cache_read_input_tokens = value;
    }
    if let Some(value) = field("output_tokens") {
        state.output_tokens = value;
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
        // Tool results first, then the image the second one carried, then text.
        assert_eq!(kinds, vec!["tool_result", "tool_result", "image", "text"]);
        assert_eq!(blocks[0]["tool_use_id"], json!("toolu_1"));
        assert_eq!(blocks[1]["tool_use_id"], json!("toolu_2"));
        assert_eq!(blocks[3]["text"], json!("and now this"));
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
        assert_eq!(body["system"], json!("first\n\nsecond"));
        assert_eq!(body["messages"][0]["role"], json!("user"));
        assert_eq!(body["messages"][0]["content"][0]["type"], json!("text"));
        // Thinking is not replayed, so the assistant turn is text plus the
        // tool call and nothing else.
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
                "content_block_start",
                json!({ "type": "content_block_start", "index": 1, "content_block": { "type": "text" } }),
            ),
            frame(
                "content_block_delta",
                json!({ "type": "content_block_delta", "index": 1, "delta": { "type": "text_delta", "text": "hello" } }),
            ),
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
                "message_delta",
                json!({
                    "type": "message_delta",
                    "delta": { "stop_reason": "tool_use" },
                    "usage": { "output_tokens": 5 }
                }),
            ),
        ];
        for event in frames {
            apply_sse_frame(&mut state, &event, Some(&sender));
        }
        drop(sender);

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
        let first = take_sse_frame(&mut buffer).expect("first frame");
        assert!(first.contains("ping"));
        assert!(take_sse_frame(&mut buffer).is_none());

        let stop = format!(
            "{}\n\n",
            frame("message_stop", json!({ "type": "message_stop" }))
        );
        buffer.extend_from_slice(stop.as_bytes());
        let second = take_sse_frame(&mut buffer).expect("second frame");
        assert!(second.contains("message_stop"));
        assert!(take_sse_frame(&mut buffer).is_none());
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
