use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tokio::sync::mpsc::{Receiver, UnboundedSender};

use crate::provider::{
    ChatStreamEvent, ProviderAttemptTrace, ProviderCallError, ProviderCallResult,
    ProviderInvocation, ProviderProgress, ProviderProgressStream, parse_chat_completion_json_text,
};
use crate::runtime::{ProviderCallUsage, elapsed_millis_u64};

const ABSOLUTE_TIMEOUT_MESSAGE_TOLERANCE: Duration = Duration::from_millis(10);

pub async fn await_structured_result(
    mut rx: Receiver<ChatStreamEvent>,
    progress: Option<UnboundedSender<ProviderProgress>>,
    provider_label: &'static str,
    executable: &'static str,
    model: Option<String>,
    effort: Option<String>,
) -> std::result::Result<ProviderCallResult, ProviderCallError> {
    let started_at = Instant::now();
    let mut accumulated_text = String::new();
    let mut final_text: Option<String> = None;
    let mut usage: Option<ProviderCallUsage> = None;
    let mut session_id: Option<String> = None;
    let mut error: Option<String> = None;

    let stall_timeout = super::super::provider_stall_timeout();
    let call_timeout = super::super::provider_call_timeout();
    loop {
        let event_timeout = match bounded_event_timeout(started_at, call_timeout, stall_timeout) {
            Ok(timeout) => timeout,
            Err(message) => {
                error = Some(message);
                break;
            }
        };
        let event = if let Some(limit) = event_timeout {
            match tokio::time::timeout(limit, rx.recv()).await {
                Ok(Some(event)) => event,
                Ok(None) => break,
                Err(_) => {
                    error = Some(provider_timeout_message(
                        started_at,
                        call_timeout,
                        stall_timeout,
                        limit,
                    ));
                    break;
                }
            }
        } else {
            match rx.recv().await {
                Some(event) => event,
                None => break,
            }
        };
        match event {
            ChatStreamEvent::ProviderEvent {
                kind,
                payload,
                raw_payload,
                stream_channel,
                content_text,
                provider_item_id,
                tool_use_id,
                tool_name,
            } => forward_provider_event(
                &progress,
                model.clone(),
                effort.clone(),
                kind,
                payload,
                raw_payload,
                stream_channel,
                content_text,
                provider_item_id,
                tool_use_id,
                tool_name,
            ),
            ChatStreamEvent::Narration { text } => {
                forward_narration(&progress, &text);
            }
            ChatStreamEvent::ReasoningDelta(text) => {
                forward_narration(&progress, &text);
            }
            ChatStreamEvent::Phase { name, input } => {
                forward_phase(&progress, &name, &input);
            }
            ChatStreamEvent::Delta(chunk) => {
                accumulated_text.push_str(&chunk);
                forward_progress(&progress, ProviderProgressStream::Stdout, chunk.as_bytes());
            }
            ChatStreamEvent::ToolCall { id, name, input } => {
                forward_tool_call_started(&progress, &id, &name, &input);
            }
            ChatStreamEvent::ToolResult {
                tool_use_id,
                output,
                is_error,
                input,
            } => {
                forward_tool_call_completed(
                    &progress,
                    &tool_use_id,
                    &output,
                    is_error,
                    input.as_ref(),
                );
            }
            ChatStreamEvent::ApprovalRequested { title, .. } => {
                error = Some(format!(
                    "{title} Provider approval requires an interactive client."
                ));
                break;
            }
            ChatStreamEvent::ProviderInteractionRequested { title, .. } => {
                error = Some(format!(
                    "{title} Provider input requires an interactive client."
                ));
                break;
            }
            ChatStreamEvent::Done {
                final_text: text,
                usage: done_usage,
                session_id: done_session,
            } => {
                final_text = Some(text);
                usage = done_usage;
                session_id = done_session;
            }
            ChatStreamEvent::Failed { error: message } => {
                error = Some(message);
                break;
            }
        }
    }

    let elapsed_ms = elapsed_millis_u64(started_at);
    let trace = ProviderAttemptTrace {
        invocation: ProviderInvocation {
            provider_label: provider_label.to_string(),
            executable: executable.to_string(),
            args: Vec::new(),
            cwd: None,
            model: model.clone(),
            effort: effort.clone(),
        },
        exit_status: if error.is_none() { Some(0) } else { Some(1) },
        stdout: accumulated_text.clone(),
        stderr: error.clone().unwrap_or_default(),
    };

    if let Some(message) = error {
        return Err(ProviderCallError::with_session(message, trace, session_id));
    }

    let Some(text) = final_text else {
        return Err(ProviderCallError::with_session(
            "provider stream closed without a Done event".to_string(),
            trace,
            session_id,
        ));
    };

    let parsed: Value = parse_structured_response(&text).map_err(|parse_err| {
        ProviderCallError::with_session(
            format!(
                "provider returned invalid JSON despite schema enforcement: {parse_err} (text: {preview})",
                preview = truncate(&text, 500)
            ),
            trace.clone(),
            session_id.clone(),
        )
    })?;

    let usage = usage.unwrap_or(ProviderCallUsage {
        duration_ms: elapsed_ms,
        ..ProviderCallUsage::default()
    });

    Ok(ProviderCallResult {
        value: parsed.clone(),
        raw_response: parsed,
        usage,
        trace,
        session_id,
    })
}

pub async fn await_freeform_result(
    mut rx: Receiver<ChatStreamEvent>,
    progress: Option<UnboundedSender<ProviderProgress>>,
    provider_label: &'static str,
    executable: &'static str,
    model: Option<String>,
    effort: Option<String>,
) -> std::result::Result<ProviderCallResult, ProviderCallError> {
    let started_at = Instant::now();
    let mut accumulated_text = String::new();
    let mut final_text: Option<String> = None;
    let mut usage: Option<ProviderCallUsage> = None;
    let mut session_id: Option<String> = None;
    let mut error: Option<String> = None;

    let stall_timeout = super::super::provider_stall_timeout();
    let call_timeout = super::super::provider_call_timeout();
    loop {
        let event_timeout = match bounded_event_timeout(started_at, call_timeout, stall_timeout) {
            Ok(timeout) => timeout,
            Err(message) => {
                error = Some(message);
                break;
            }
        };
        let event = if let Some(limit) = event_timeout {
            match tokio::time::timeout(limit, rx.recv()).await {
                Ok(Some(event)) => event,
                Ok(None) => break,
                Err(_) => {
                    error = Some(provider_timeout_message(
                        started_at,
                        call_timeout,
                        stall_timeout,
                        limit,
                    ));
                    break;
                }
            }
        } else {
            match rx.recv().await {
                Some(event) => event,
                None => break,
            }
        };
        match event {
            ChatStreamEvent::ProviderEvent {
                kind,
                payload,
                raw_payload,
                stream_channel,
                content_text,
                provider_item_id,
                tool_use_id,
                tool_name,
            } => forward_provider_event(
                &progress,
                model.clone(),
                effort.clone(),
                kind,
                payload,
                raw_payload,
                stream_channel,
                content_text,
                provider_item_id,
                tool_use_id,
                tool_name,
            ),
            ChatStreamEvent::Narration { text } => {
                forward_narration(&progress, &text);
            }
            ChatStreamEvent::ReasoningDelta(text) => {
                forward_narration(&progress, &text);
            }
            ChatStreamEvent::Phase { name, input } => {
                forward_phase(&progress, &name, &input);
            }
            ChatStreamEvent::Delta(chunk) => {
                accumulated_text.push_str(&chunk);
                forward_progress(&progress, ProviderProgressStream::Stdout, chunk.as_bytes());
            }
            ChatStreamEvent::ToolCall { id, name, input } => {
                forward_tool_call_started(&progress, &id, &name, &input);
            }
            ChatStreamEvent::ToolResult {
                tool_use_id,
                output,
                is_error,
                input,
            } => {
                forward_tool_call_completed(
                    &progress,
                    &tool_use_id,
                    &output,
                    is_error,
                    input.as_ref(),
                );
            }
            ChatStreamEvent::ApprovalRequested { title, .. } => {
                error = Some(format!(
                    "{title} Provider approval requires an interactive client."
                ));
                break;
            }
            ChatStreamEvent::ProviderInteractionRequested { title, .. } => {
                error = Some(format!(
                    "{title} Provider input requires an interactive client."
                ));
                break;
            }
            ChatStreamEvent::Done {
                final_text: text,
                usage: done_usage,
                session_id: done_session,
            } => {
                final_text = Some(text);
                usage = done_usage;
                session_id = done_session;
            }
            ChatStreamEvent::Failed { error: message } => {
                error = Some(message);
                break;
            }
        }
    }

    let elapsed_ms = elapsed_millis_u64(started_at);
    let trace = ProviderAttemptTrace {
        invocation: ProviderInvocation {
            provider_label: provider_label.to_string(),
            executable: executable.to_string(),
            args: Vec::new(),
            cwd: None,
            model: model.clone(),
            effort: effort.clone(),
        },
        exit_status: if error.is_none() { Some(0) } else { Some(1) },
        stdout: accumulated_text.clone(),
        stderr: error.clone().unwrap_or_default(),
    };

    if let Some(message) = error {
        return Err(ProviderCallError::with_session(message, trace, session_id));
    }

    let Some(text) = final_text else {
        return Err(ProviderCallError::with_session(
            "provider stream closed without a Done event".to_string(),
            trace,
            session_id,
        ));
    };

    let usage = usage.unwrap_or(ProviderCallUsage {
        duration_ms: elapsed_ms,
        ..ProviderCallUsage::default()
    });

    Ok(ProviderCallResult {
        value: Value::String(text.clone()),
        raw_response: json!({ "text": text }),
        usage,
        trace,
        session_id,
    })
}

fn bounded_event_timeout(
    started_at: Instant,
    call_timeout: Option<Duration>,
    stall_timeout: Option<Duration>,
) -> Result<Option<Duration>, String> {
    let call_remaining = match call_timeout {
        Some(limit) => match limit.checked_sub(started_at.elapsed()) {
            Some(remaining) if !remaining.is_zero() => Some(remaining),
            _ => {
                return Err(format!(
                    "provider call exceeded absolute timeout of {}s; aborting stream",
                    limit.as_secs()
                ));
            }
        },
        None => None,
    };

    Ok(match (call_remaining, stall_timeout) {
        (Some(call), Some(stall)) => Some(call.min(stall)),
        (Some(call), None) => Some(call),
        (None, Some(stall)) => Some(stall),
        (None, None) => None,
    })
}

fn provider_timeout_message(
    started_at: Instant,
    call_timeout: Option<Duration>,
    stall_timeout: Option<Duration>,
    waited: Duration,
) -> String {
    if let Some(call_limit) = call_timeout
        && started_at.elapsed() >= call_limit.saturating_sub(ABSOLUTE_TIMEOUT_MESSAGE_TOLERANCE)
    {
        return format!(
            "provider call exceeded absolute timeout of {}s; aborting stream",
            call_limit.as_secs()
        );
    }
    if let Some(stall_limit) = stall_timeout
        && waited == stall_limit
    {
        return format!(
            "provider emitted no events for {}s; aborting stalled stream",
            stall_limit.as_secs()
        );
    }
    format!(
        "provider stream timed out after waiting {}s for next event",
        waited.as_secs()
    )
}

pub(super) fn forward_progress(
    sender: &Option<UnboundedSender<ProviderProgress>>,
    stream: ProviderProgressStream,
    chunk: &[u8],
) {
    if chunk.is_empty() {
        return;
    }
    let Some(sender) = sender else {
        return;
    };
    let _ = sender.send(ProviderProgress::Bytes {
        stream,
        chunk: chunk.to_vec(),
    });
}

fn forward_narration(sender: &Option<UnboundedSender<ProviderProgress>>, text: &str) {
    if text.trim().is_empty() {
        return;
    }
    let Some(sender) = sender else {
        return;
    };
    let _ = sender.send(ProviderProgress::Narration {
        text: text.to_string(),
    });
}

fn forward_phase(sender: &Option<UnboundedSender<ProviderProgress>>, name: &str, input: &Value) {
    let Some(sender) = sender else { return };
    let _ = sender.send(ProviderProgress::Phase {
        name: name.to_string(),
        input: input.clone(),
    });
}

fn forward_tool_call_started(
    sender: &Option<UnboundedSender<ProviderProgress>>,
    id: &str,
    name: &str,
    input: &Value,
) {
    let Some(sender) = sender else { return };
    let _ = sender.send(ProviderProgress::ToolCallStarted {
        id: id.to_string(),
        name: name.to_string(),
        input: input.clone(),
    });
}

fn forward_tool_call_completed(
    sender: &Option<UnboundedSender<ProviderProgress>>,
    tool_use_id: &str,
    output: &str,
    is_error: bool,
    input: Option<&Value>,
) {
    let Some(sender) = sender else { return };
    let _ = sender.send(ProviderProgress::ToolCallCompleted {
        tool_use_id: tool_use_id.to_string(),
        output: output.to_string(),
        is_error,
        input: input.cloned(),
    });
}

#[allow(clippy::too_many_arguments)]
fn forward_provider_event(
    sender: &Option<UnboundedSender<ProviderProgress>>,
    model: Option<String>,
    effort: Option<String>,
    kind: String,
    payload: Value,
    raw_payload: Option<Value>,
    stream_channel: Option<String>,
    content_text: Option<String>,
    provider_item_id: Option<String>,
    tool_use_id: Option<String>,
    tool_name: Option<String>,
) {
    let Some(sender) = sender else { return };
    let _ = sender.send(ProviderProgress::ProviderEvent {
        kind,
        payload,
        raw_payload,
        stream_channel,
        content_text,
        provider_item_id,
        tool_use_id,
        tool_name,
        model,
        effort,
    });
}

pub(super) fn truncate(text: &str, max: usize) -> String {
    let mut chars = text.chars();
    let truncated: String = chars.by_ref().take(max).collect();
    if chars.next().is_some() {
        format!("{truncated}…")
    } else {
        text.to_string()
    }
}

/// Parses the provider's final text as JSON using the shared provider recovery path.
///
/// Preferred path: the upstream mapper extracts `structured_output` (the SDK's
/// already-parsed and schema-validated JSON value serialised) and hands us a
/// valid JSON literal — the first `from_str` succeeds directly.
///
/// Returns the first error encountered so diagnostics point at the original
/// payload, not the post-extraction slice.
pub(super) fn parse_structured_response(text: &str) -> Result<Value, serde_json::Error> {
    let trimmed = text.trim();
    let first_err = match serde_json::from_str::<Value>(trimmed) {
        Ok(value) => return Ok(value),
        Err(err) => err,
    };
    parse_chat_completion_json_text(trimmed).ok_or(first_err)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{
        bounded_event_timeout, parse_structured_response, provider_timeout_message, truncate,
    };

    #[test]
    fn parses_bare_json() {
        let v = parse_structured_response(r#"{"a":1}"#).unwrap();
        assert_eq!(v["a"], 1);
    }

    #[test]
    fn parses_complete_structured_object_after_broken_prefix() {
        let v = parse_structured_response(
            "{\"arguments\":{\"facts\":[{\"text\":\"cut\n\n{\"arguments\":{\"facts\":[]}}",
        )
        .unwrap();

        assert_eq!(v["arguments"]["facts"], serde_json::json!([]));
    }

    #[test]
    fn event_timeout_uses_lower_of_stall_and_call_remaining() {
        let started = Instant::now() - Duration::from_secs(50);
        let timeout = bounded_event_timeout(
            started,
            Some(Duration::from_secs(100)),
            Some(Duration::from_secs(20)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(timeout, Duration::from_secs(20));

        let timeout = bounded_event_timeout(
            started,
            Some(Duration::from_secs(55)),
            Some(Duration::from_secs(20)),
        )
        .unwrap()
        .unwrap();
        assert!(timeout <= Duration::from_secs(5));
    }

    #[test]
    fn event_timeout_errors_after_absolute_call_deadline() {
        let started = Instant::now() - Duration::from_secs(61);
        let err = bounded_event_timeout(
            started,
            Some(Duration::from_secs(60)),
            Some(Duration::from_secs(20)),
        )
        .unwrap_err();
        assert!(err.contains("absolute timeout of 60s"));
    }

    #[test]
    fn timeout_message_distinguishes_absolute_from_idle_timeout() {
        let absolute = provider_timeout_message(
            Instant::now() - Duration::from_secs(61),
            Some(Duration::from_secs(60)),
            Some(Duration::from_secs(20)),
            Duration::from_secs(1),
        );
        assert!(absolute.contains("absolute timeout of 60s"));

        let idle = provider_timeout_message(
            Instant::now(),
            Some(Duration::from_secs(60)),
            Some(Duration::from_secs(20)),
            Duration::from_secs(20),
        );
        assert!(idle.contains("no events for 20s"));
    }

    #[test]
    fn truncate_preserves_utf8_boundaries() {
        assert_eq!(truncate("abé", 2), "ab…");
        assert_eq!(truncate("é", 1), "é");
        assert_eq!(truncate("éx", 1), "é…");
    }

    #[test]
    fn parses_json_code_fence() {
        let v = parse_structured_response("```json\n{\"a\":1}\n```").unwrap();
        assert_eq!(v["a"], 1);
    }

    #[test]
    fn parses_plain_code_fence() {
        let v = parse_structured_response("```\n{\"a\":1}\n```").unwrap();
        assert_eq!(v["a"], 1);
    }

    #[test]
    fn parses_fence_with_trailing_whitespace() {
        let v = parse_structured_response("```json\n{\"a\":1}\n```\n\n").unwrap();
        assert_eq!(v["a"], 1);
    }

    #[test]
    fn parses_prose_wrapped_json() {
        let text = "Here is the result:\n\n{\"a\": 1, \"b\": [1,2,3]}\n\nLet me know if you need anything else.";
        let v = parse_structured_response(text).unwrap();
        assert_eq!(v["a"], 1);
    }

    #[test]
    fn parses_fenced_json_with_brace_in_string() {
        let v = parse_structured_response("```json\n{\"text\": \"has } inside\"}\n```").unwrap();
        assert_eq!(v["text"], "has } inside");
    }

    #[test]
    fn parses_escaped_quotes_in_string() {
        let text = "```json\n{\"text\": \"with \\\" quote and \\\\ backslash\"}\n```";
        let v = parse_structured_response(text).unwrap();
        assert_eq!(v["text"], "with \" quote and \\ backslash");
    }

    #[test]
    fn salvages_raw_control_characters_inside_json_strings() {
        let text = "{\"text\":\"line one\nline two\u{0000}line three\"}";
        let v = parse_structured_response(text).unwrap();
        assert_eq!(v["text"], "line one line two line three");
    }

    #[test]
    fn rejects_non_json() {
        assert!(parse_structured_response("not json at all").is_err());
    }

    mod session_round_trip {
        use crate::provider::{
            ChatStreamEvent, ProviderCallError, ProviderCallResult, ProviderProgress,
        };

        use super::super::await_structured_result;

        async fn run(
            events: Vec<ChatStreamEvent>,
        ) -> std::result::Result<ProviderCallResult, ProviderCallError> {
            let (tx, rx) = tokio::sync::mpsc::channel::<ChatStreamEvent>(16);
            tokio::spawn(async move {
                for event in events {
                    let _ = tx.send(event).await;
                }
            });
            await_structured_result(rx, None, "test", "test", None, None).await
        }

        #[tokio::test]
        async fn done_session_id_propagates_to_provider_call_result() {
            let result = run(vec![ChatStreamEvent::Done {
                final_text: r#"{"ok":true}"#.to_string(),
                usage: None,
                session_id: Some("ses-from-sdk-42".to_string()),
            }])
            .await
            .expect("call should succeed");
            assert_eq!(result.session_id.as_deref(), Some("ses-from-sdk-42"));
            assert_eq!(result.value["ok"], true);
        }

        #[tokio::test]
        async fn done_without_session_id_leaves_field_none() {
            let result = run(vec![ChatStreamEvent::Done {
                final_text: r#"{"ok":true}"#.to_string(),
                usage: None,
                session_id: None,
            }])
            .await
            .expect("call should succeed");
            assert!(result.session_id.is_none());
        }

        #[tokio::test]
        async fn narration_forwards_to_provider_progress() {
            let (tx, rx) = tokio::sync::mpsc::channel::<ChatStreamEvent>(16);
            let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();
            tokio::spawn(async move {
                let _ = tx
                    .send(ChatStreamEvent::Narration {
                        text: "Reading the attached document".to_string(),
                    })
                    .await;
                let _ = tx
                    .send(ChatStreamEvent::Done {
                        final_text: r#"{"ok":true}"#.to_string(),
                        usage: None,
                        session_id: None,
                    })
                    .await;
            });

            let result = await_structured_result(rx, Some(progress_tx), "test", "test", None, None)
                .await
                .expect("call should succeed");

            assert_eq!(result.value["ok"], true);
            match progress_rx.recv().await.expect("narration progress") {
                ProviderProgress::Narration { text } => {
                    assert_eq!(text, "Reading the attached document");
                }
                other => panic!("expected narration progress, got {other:?}"),
            }
        }
    }
}
