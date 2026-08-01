use anyhow::Result;
use serde_json::Value;
use tokio::sync::mpsc;

use super::{ChatStreamEvent, extract_text_block, extract_tool_result_content};
use crate::runtime::ProviderCallUsage;

#[derive(Default)]
pub(super) struct ClaudeStreamState {
    pub(super) saw_stream_text: bool,
    pub(super) saw_stream_reasoning: bool,
    pub(super) delta_accumulator: String,
    pub(super) final_text: Option<String>,
    pub(super) final_usage: Option<ProviderCallUsage>,
    pub(super) emitted_failure: bool,
    /// Captured from the Claude Agent SDK's first "system" init
    /// message. Forwarded back to the caller on `Done` so the next
    /// turn can `resume: session_id`.
    pub(super) session_id: Option<String>,
}

impl ClaudeStreamState {
    pub(super) async fn handle_message(
        &mut self,
        value: &Value,
        tx: &mpsc::Sender<ChatStreamEvent>,
    ) -> Result<bool> {
        let message_type = value.get("type").and_then(Value::as_str).unwrap_or("");
        match message_type {
            "stream_event" => {
                let event = match value.get("event") {
                    Some(event) => event,
                    None => return Ok(false),
                };
                if event.get("type").and_then(Value::as_str) != Some("content_block_delta") {
                    return Ok(false);
                }
                let delta = match event.get("delta") {
                    Some(delta) => delta,
                    None => return Ok(false),
                };
                match delta.get("type").and_then(Value::as_str).unwrap_or("") {
                    "text_delta" => {
                        if let Some(text) = delta.get("text").and_then(Value::as_str) {
                            self.saw_stream_text = true;
                            self.delta_accumulator.push_str(text);
                            if tx
                                .send(ChatStreamEvent::Delta(text.to_string()))
                                .await
                                .is_err()
                            {
                                return Ok(true);
                            }
                        }
                    }
                    "thinking_delta" => {
                        if let Some(thinking) = delta.get("thinking").and_then(Value::as_str) {
                            self.saw_stream_reasoning = true;
                            if tx
                                .send(ChatStreamEvent::ReasoningDelta(thinking.to_string()))
                                .await
                                .is_err()
                            {
                                return Ok(true);
                            }
                        }
                    }
                    _ => {}
                }
            }
            "system" => {
                // Agent SDK emits a "system" init message at turn
                // start with session_id. Capture it; forwarded on
                // Done so the caller can `resume: session_id` next.
                if let Some(sid) = value.get("session_id").and_then(Value::as_str)
                    && !sid.is_empty()
                {
                    self.session_id = Some(sid.to_string());
                }
            }
            "assistant" => {
                let usage = super::super::extract_claude_usage(value);
                if usage.duration_ms > 0 || usage.total_tokens > 0 || usage.cost_microusd.is_some()
                {
                    self.final_usage = Some(usage);
                }
                // Some SDK versions only surface session_id on the
                // assistant message envelope.
                if self.session_id.is_none()
                    && let Some(sid) = value.get("session_id").and_then(Value::as_str)
                    && !sid.is_empty()
                {
                    self.session_id = Some(sid.to_string());
                }
                // API failures are delivered as assistant envelopes by the
                // Agent SDK. Their text is user-facing, but the structured
                // `error` field means this turn failed and must not be treated
                // as a successful answer (which would auto-continue a goal).
                if let Some(kind) = value.get("error").and_then(Value::as_str) {
                    let message = value
                        .get("message")
                        .and_then(|message| message.get("content"))
                        .and_then(Value::as_array)
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(extract_text_block)
                                .collect::<Vec<_>>()
                                .join("\n\n")
                        })
                        .filter(|text| !text.trim().is_empty())
                        .unwrap_or_else(|| "Claude API request failed".to_string());
                    let status = value
                        .get("apiErrorStatus")
                        .or_else(|| value.get("api_error_status"))
                        .and_then(Value::as_u64);
                    let mut parts = vec![
                        format!("claude SDK API error: {message}"),
                        format!(r#""kind":"{kind}""#),
                    ];
                    if let Some(status) = status {
                        parts.push(format!(r#""status":{status}"#));
                    }
                    self.emitted_failure = true;
                    let _ = tx
                        .send(ChatStreamEvent::Failed {
                            error: parts.join(" "),
                        })
                        .await;
                    // Keep draining until the SDK's result envelope so a
                    // pooled adapter remains aligned for its next turn.
                    return Ok(false);
                }
                if let Some(content) = value
                    .get("message")
                    .and_then(|message| message.get("content"))
                    .and_then(Value::as_array)
                {
                    if !self.saw_stream_reasoning {
                        for block in content {
                            if block.get("type").and_then(Value::as_str) == Some("thinking")
                                && let Some(thinking) =
                                    block.get("thinking").and_then(Value::as_str)
                                && tx
                                    .send(ChatStreamEvent::ReasoningDelta(thinking.to_string()))
                                    .await
                                    .is_err()
                            {
                                return Ok(true);
                            }
                        }
                    }
                    // Emit order matters for the consumer's bubble state:
                    // deltas first (progressive text), then the canonical
                    // Narration segment that replaces the delta
                    // accumulator, then tool calls (which flush the
                    // segment into a thinking breadcrumb). Sending
                    // Narration before the deltas doubles the segment in
                    // the live bubble.
                    if !self.saw_stream_text {
                        for block in content {
                            if block.get("type").and_then(Value::as_str) == Some("text")
                                && let Some(text) = block.get("text").and_then(Value::as_str)
                            {
                                self.delta_accumulator.push_str(text);
                                if tx
                                    .send(ChatStreamEvent::Delta(text.to_string()))
                                    .await
                                    .is_err()
                                {
                                    return Ok(true);
                                }
                            }
                        }
                    }
                    let segment = content
                        .iter()
                        .filter_map(extract_text_block)
                        .collect::<Vec<_>>()
                        .join("\n\n");
                    if !segment.trim().is_empty()
                        && tx
                            .send(ChatStreamEvent::Narration { text: segment })
                            .await
                            .is_err()
                    {
                        return Ok(true);
                    }
                    for block in content {
                        if block.get("type").and_then(Value::as_str) == Some("tool_use") {
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
                            if id.is_empty() || name.is_empty() {
                                tracing::warn!(
                                    has_id = !id.is_empty(),
                                    has_name = !name.is_empty(),
                                    "claude tool_use block missing id or name; skipping tool call"
                                );
                                continue;
                            }
                            let input = block.get("input").cloned().unwrap_or(Value::Null);
                            if tx
                                .send(ChatStreamEvent::ToolCall { id, name, input })
                                .await
                                .is_err()
                            {
                                return Ok(true);
                            }
                        }
                    }
                }
            }
            "user" => {
                if let Some(content) = value
                    .get("message")
                    .and_then(|message| message.get("content"))
                    .and_then(Value::as_array)
                {
                    for block in content {
                        if block.get("type").and_then(Value::as_str) != Some("tool_result") {
                            continue;
                        }
                        let tool_use_id = block
                            .get("tool_use_id")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        if tool_use_id.is_empty() {
                            tracing::warn!(
                                "claude tool_result block missing tool_use_id; skipping tool result"
                            );
                            continue;
                        }
                        let output = extract_tool_result_content(block.get("content"));
                        let is_error = block
                            .get("is_error")
                            .and_then(Value::as_bool)
                            .unwrap_or(false);
                        if tx
                            .send(ChatStreamEvent::ToolResult {
                                tool_use_id,
                                output,
                                is_error,
                                input: None,
                            })
                            .await
                            .is_err()
                        {
                            return Ok(true);
                        }
                    }
                }
            }
            "result" => {
                let usage = super::super::extract_claude_usage(value);
                if usage.duration_ms > 0 || usage.total_tokens > 0 || usage.cost_microusd.is_some()
                {
                    self.final_usage = Some(usage);
                }
                match value
                    .get("subtype")
                    .and_then(Value::as_str)
                    .unwrap_or("success")
                {
                    "success" => {
                        // When the caller passed `outputFormat: { type: "json_schema", ... }`,
                        // the Agent SDK validates the final assistant output against the
                        // schema and emits the parsed object on `structured_output`. That
                        // value is authoritative. The free-text `result` field may still
                        // carry the raw model output and is only a presentation transcript.
                        if let Some(structured) = value.get("structured_output")
                            && !structured.is_null()
                        {
                            self.final_text = Some(structured.to_string());
                        } else {
                            self.final_text = value
                                .get("result")
                                .and_then(Value::as_str)
                                .map(ToString::to_string)
                                .or_else(|| {
                                    value.get("content").and_then(Value::as_array).map(|items| {
                                        items.iter().filter_map(extract_text_block).collect()
                                    })
                                });
                        }
                    }
                    subtype => {
                        // Non-success SDK result subtypes: error_during_execution,
                        // error_max_turns, error_max_budget_usd, error_max_structured_output_retries.
                        // Collect whatever diagnostic strings the SDK included.
                        let explicit_errors = value
                            .get("errors")
                            .and_then(Value::as_array)
                            .map(|items| {
                                items
                                    .iter()
                                    .filter_map(Value::as_str)
                                    .collect::<Vec<_>>()
                                    .join("; ")
                            })
                            .filter(|joined| !joined.is_empty());
                        let error = value
                            .get("result")
                            .and_then(Value::as_str)
                            .map(ToString::to_string)
                            .or(explicit_errors)
                            .or_else(|| {
                                value
                                    .get("error")
                                    .and_then(Value::as_str)
                                    .map(ToString::to_string)
                            })
                            .unwrap_or_else(|| format!("claude SDK returned subtype={subtype}"));
                        self.emitted_failure = true;
                        let _ = tx
                            .send(ChatStreamEvent::Failed {
                                error: format!("claude SDK {subtype}: {error}"),
                            })
                            .await;
                        return Ok(true);
                    }
                }
            }
            "error" => {
                // Surface the structured `kind` / `status` fields the TS
                // wrapper attaches so `classify_provider_failure` can pick
                // them up without string matching.
                let message = value
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("claude SDK returned an error");
                let kind = value.get("kind").and_then(Value::as_str);
                let status = value.get("status").and_then(Value::as_u64);
                let mut parts: Vec<String> = vec![format!("claude SDK: {message}")];
                if let Some(kind) = kind {
                    parts.push(format!(r#""kind":"{kind}""#));
                }
                if let Some(status) = status {
                    parts.push(format!(r#""status":{status}"#));
                }
                self.emitted_failure = true;
                let _ = tx
                    .send(ChatStreamEvent::Failed {
                        error: parts.join(" "),
                    })
                    .await;
                return Ok(true);
            }
            _ => {}
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn emits_streamed_thinking_separately_from_assistant_text() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut state = ClaudeStreamState::default();
        state
            .handle_message(
                &json!({
                    "type": "stream_event",
                    "event": {
                        "type": "content_block_delta",
                        "index": 0,
                        "delta": {
                            "type": "thinking_delta",
                            "thinking": "checking the source"
                        }
                    }
                }),
                &tx,
            )
            .await
            .unwrap();

        assert!(state.saw_stream_reasoning);
        assert!(matches!(
            rx.try_recv(),
            Ok(ChatStreamEvent::ReasoningDelta(text)) if text == "checking the source"
        ));
    }

    #[tokio::test]
    async fn treats_assistant_api_error_as_a_terminal_failure() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut state = ClaudeStreamState::default();
        let stop_reading = state
            .handle_message(
                &json!({
                    "type": "assistant",
                    "error": "rate_limit",
                    "apiErrorStatus": 429,
                    "message": {
                        "content": [{
                            "type": "text",
                            "text": "You've hit your monthly spend limit"
                        }]
                    }
                }),
                &tx,
            )
            .await
            .unwrap();

        assert!(!stop_reading);
        assert!(state.emitted_failure);
        assert!(matches!(
            rx.try_recv(),
            Ok(ChatStreamEvent::Failed { error })
                if error.contains("monthly spend limit")
                    && error.contains(r#""kind":"rate_limit""#)
                    && error.contains(r#""status":429"#)
        ));
    }
}
