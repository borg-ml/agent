//! xAI Grok Build adapter.
//!
//! Grok Build owns its own tool loop, like the Claude and OpenCode
//! compatibility routes, so Borg drives it through the CLI rather than its
//! native harness. The subscription is reached with the user's Grok login
//! (`grok login`, cached in `~/.grok/auth.json`) or `XAI_API_KEY`.
//!
//! Headless mode is the integration surface: `grok -p <prompt>
//! --output-format streaming-json` emits newline-delimited events
//! (`thought`, `text`, `end`), and the `end` event carries the session id that
//! makes the next Borg turn a continuation instead of a fresh conversation.

use std::time::Instant;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::sync::mpsc;

use super::{ChatStreamEvent, ChatStreamRequest, LocalAgentPermission, classify_provider_error};
use crate::runtime::ProviderCallUsage;

/// Grok Build's headless run is one prompt to completion. Subscribe before
/// spawning so a cancellation can reap the child.
pub fn run_grok_local_chat_stream(
    request: ChatStreamRequest,
    permission: LocalAgentPermission,
) -> mpsc::Receiver<ChatStreamEvent> {
    let (events, receiver) = mpsc::channel(64);
    tokio::spawn(async move {
        let result = tokio::select! {
            _ = events.closed() => return,
            result = run(request, events.clone(), permission) => result,
        };
        if let Err(error) = result {
            let _ = events
                .send(ChatStreamEvent::Failed {
                    kind: classify_provider_error(&error),
                    error: format!("{error:#}"),
                })
                .await;
        }
    });
    receiver
}

async fn run(
    request: ChatStreamRequest,
    events: mpsc::Sender<ChatStreamEvent>,
    permission: LocalAgentPermission,
) -> Result<()> {
    let started_at = Instant::now();
    let cwd = match request.working_directory.clone() {
        Some(cwd) => cwd,
        None => std::env::current_dir().context("failed to resolve Grok working directory")?,
    };
    let mut command = crate::provider_bin::command(crate::provider_bin::Runtime::Grok).await?;
    command
        .current_dir(&cwd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    // A background updater would rewrite the binary mid-turn and is pointless
    // in a supervised process.
    command.arg("--no-auto-update");
    if let Some(model) = request.model.as_deref().map(str::trim).filter(|m| !m.is_empty()) {
        command.arg("-m").arg(model);
    }
    if permission == LocalAgentPermission::FullAccess {
        // Headless Grok has no interactive approval surface; without this the
        // first tool call stalls until the turn times out.
        command.arg("--always-approve");
    }
    if let Some(session_id) = request
        .session_id
        .as_deref()
        .map(str::trim)
        .filter(|id| !id.is_empty())
    {
        command.arg("--resume").arg(session_id);
    }
    command
        .arg("-p")
        .arg(prompt_with_context(&request))
        .arg("--output-format")
        .arg("streaming-json");

    let mut child = command.spawn().context("failed to start Grok Build")?;
    let stdout = child
        .stdout
        .take()
        .context("Grok Build stdout missing")?;
    let stderr = child.stderr.take();
    // Drain stderr concurrently: a full pipe would deadlock the child.
    let stderr_task = tokio::spawn(async move {
        let mut buffer = String::new();
        if let Some(mut stderr) = stderr {
            let _ = stderr.read_to_string(&mut buffer).await;
        }
        buffer
    });
    let mut lines = BufReader::new(stdout).lines();

    let mut text = String::new();
    let mut session_id: Option<String> = None;
    let mut usage = ProviderCallUsage::default();
    let mut saw_usage = false;
    loop {
        let line = tokio::select! {
            _ = events.closed() => {
                let _ = child.kill().await;
                return Ok(());
            }
            line = lines.next_line() => line?,
        };
        let Some(line) = line else { break };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(event) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        match event.get("type").and_then(Value::as_str) {
            Some("text") => {
                if let Some(chunk) = event.get("data").and_then(Value::as_str) {
                    text.push_str(chunk);
                    if events
                        .send(ChatStreamEvent::Delta(chunk.to_string()))
                        .await
                        .is_err()
                    {
                        let _ = child.kill().await;
                        return Ok(());
                    }
                }
            }
            Some("thought") => {
                if let Some(chunk) = event.get("data").and_then(Value::as_str)
                    && events
                        .send(ChatStreamEvent::ReasoningDelta(chunk.to_string()))
                        .await
                        .is_err()
                {
                    let _ = child.kill().await;
                    return Ok(());
                }
            }
            Some("end") => {
                if let Some(id) = event.get("sessionId").and_then(Value::as_str) {
                    session_id = Some(id.to_string());
                }
                if let Some(value) = event.get("usage")
                    && apply_usage(&mut usage, value)
                {
                    saw_usage = true;
                }
            }
            _ => {}
        }
    }

    let status = child.wait().await.context("failed to wait for Grok Build")?;
    let stderr = stderr_task.await.unwrap_or_default();
    if !status.success() && text.trim().is_empty() {
        let detail = stderr.trim();
        if detail.is_empty() {
            bail!("Grok Build exited with {status}");
        }
        bail!("Grok Build exited with {status}: {detail}");
    }
    usage.duration_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
    let _ = events
        .send(ChatStreamEvent::Done {
            final_text: text,
            usage: saw_usage.then_some(usage),
            session_id,
            provider_turn_id: None,
        })
        .await;
    Ok(())
}

/// A fresh Grok process has no durable replay, so the first turn carries the
/// system prompt and any attachment paths. A resumed session already has them.
fn prompt_with_context(request: &ChatStreamRequest) -> String {
    let mut prompt = if request.session_id.is_none() && !request.system_prompt.trim().is_empty() {
        format!(
            "{}\n\nUser request:\n{}",
            request.system_prompt.trim(),
            request.prompt
        )
    } else {
        request.prompt.clone()
    };
    if !request.attachments.is_empty() {
        prompt.push_str("\n\nAttached files:\n");
        for attachment in &request.attachments {
            prompt.push_str("- ");
            prompt.push_str(&attachment.display().to_string());
            prompt.push('\n');
        }
    }
    prompt
}

/// Read the token counters a Grok `end` event reports. The field names are not
/// a published contract, so this accepts the common spellings and reports
/// nothing rather than a wrong number when none are present.
fn apply_usage(usage: &mut ProviderCallUsage, value: &Value) -> bool {
    let counter = |names: &[&str]| -> Option<u64> {
        names
            .iter()
            .find_map(|name| value.get(*name).and_then(Value::as_u64))
    };
    let mut seen = false;
    if let Some(input) = counter(&["input_tokens", "prompt_tokens"]) {
        usage.input_tokens = input;
        seen = true;
    }
    if let Some(cached) = counter(&["cached_input_tokens", "cached_tokens"]) {
        usage.cached_input_tokens = cached;
        seen = true;
    }
    if let Some(output) = counter(&["output_tokens", "completion_tokens"]) {
        usage.output_tokens = output;
        seen = true;
    }
    if let Some(total) = counter(&["total_tokens"]) {
        usage.total_tokens = total;
        seen = true;
    } else if seen {
        usage.total_tokens = usage
            .input_tokens
            .saturating_add(usage.cached_input_tokens)
            .saturating_add(usage.output_tokens);
    }
    seen
}

/// Grok's `streaming-json` events are one JSON object per line. Kept separate
/// so the parser is testable without spawning a process.
#[cfg(test)]
pub(crate) fn parse_stream_line(line: &str) -> Option<GrokEvent> {
    let event: Value = serde_json::from_str(line.trim()).ok()?;
    match event.get("type").and_then(Value::as_str)? {
        "text" => Some(GrokEvent::Text(
            event.get("data").and_then(Value::as_str)?.to_string(),
        )),
        "thought" => Some(GrokEvent::Thought(
            event.get("data").and_then(Value::as_str)?.to_string(),
        )),
        "end" => Some(GrokEvent::End {
            session_id: event
                .get("sessionId")
                .and_then(Value::as_str)
                .map(str::to_string),
            usage: event.get("usage").cloned(),
        }),
        _ => None,
    }
}

#[cfg(test)]
#[derive(Debug, PartialEq)]
pub(crate) enum GrokEvent {
    Text(String),
    Thought(String),
    End {
        session_id: Option<String>,
        usage: Option<Value>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streaming_events_map_to_text_thought_and_end() {
        assert_eq!(
            parse_stream_line(r#"{"type":"text","data":"OK"}"#),
            Some(GrokEvent::Text("OK".to_string()))
        );
        assert_eq!(
            parse_stream_line(r#"{"type":"thought","data":"hmm"}"#),
            Some(GrokEvent::Thought("hmm".to_string()))
        );
        assert_eq!(
            parse_stream_line(
                r#"{"type":"end","stopReason":"EndTurn","sessionId":"s-1"}"#
            ),
            Some(GrokEvent::End {
                session_id: Some("s-1".to_string()),
                usage: None,
            })
        );
    }

    #[test]
    fn non_event_and_unknown_lines_are_ignored() {
        assert_eq!(parse_stream_line("not json"), None);
        assert_eq!(parse_stream_line(r#"{"type":"progress"}"#), None);
        assert_eq!(parse_stream_line(r#"{"type":"text"}"#), None);
    }

    #[test]
    fn usage_accepts_common_counter_spellings_and_ignores_absent_ones() {
        let mut usage = ProviderCallUsage::default();
        assert!(apply_usage(
            &mut usage,
            &serde_json::json!({"input_tokens": 10, "output_tokens": 4, "cached_tokens": 3})
        ));
        assert_eq!(usage.input_tokens, 10);
        assert_eq!(usage.output_tokens, 4);
        assert_eq!(usage.cached_input_tokens, 3);
        assert_eq!(usage.total_tokens, 17);

        let mut empty = ProviderCallUsage::default();
        assert!(!apply_usage(&mut empty, &serde_json::json!({"note": "none"})));
    }
}
