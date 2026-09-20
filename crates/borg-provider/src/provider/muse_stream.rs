//! Meta Muse Code adapter.
//!
//! A Muse Code subscription only works through Meta's own CLI while signed in
//! with a Meta Model API account, so Borg drives it as a compatibility route
//! rather than through its native harness.
//!
//! Headless mode is the integration surface: `muse exec --json` emits one
//! journal-envelope JSON object per stdout line. The envelope carries a
//! `payload_type` discriminator; `run.output.delta` streams output text,
//! `tool.result` reports each finished tool invocation and `run.terminal.*`
//! reports the final text and terminal state.
//!
//! Multi-turn continuity comes from `--session-id`: each turn is its own
//! process, and passing the same id continues that session instead of starting
//! a new one. Borg therefore supplies the id itself (a UUID it also records as
//! the provider session id), rather than waiting for the CLI to mint one.

use anyhow::{Context, Result, bail};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::sync::mpsc;

use super::{ChatStreamEvent, ChatStreamRequest, LocalAgentPermission, classify_provider_error};

/// Muse Code's headless run is one prompt to completion. Subscribe before
/// spawning so a cancellation can reap the child.
pub fn run_muse_local_chat_stream(
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
    let cwd = match request.working_directory.clone() {
        Some(cwd) => cwd,
        None => std::env::current_dir().context("failed to resolve Muse working directory")?,
    };
    let session_id = request
        .session_id
        .as_deref()
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    let mut command = crate::provider_bin::command(crate::provider_bin::Runtime::Muse).await?;
    command
        .current_dir(&cwd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    command
        .arg("exec")
        .arg("--json")
        .arg("--session-id")
        .arg(&session_id);
    if let Some(model) = request.model.as_deref().map(str::trim).filter(|m| !m.is_empty()) {
        command.arg("--model").arg(model);
    }
    match permission {
        // A headless run has no interactive approval surface, so the policy is
        // chosen up front. Full access also drops the sandbox; every other mode
        // keeps the sandbox and only skips the unanswered prompt.
        LocalAgentPermission::FullAccess => {
            command.arg("--yolo");
        }
        LocalAgentPermission::Auto | LocalAgentPermission::Manual => {
            command.arg("--disable-approval");
        }
    }
    command.arg(prompt_with_context(&request));

    let mut child = command.spawn().context("failed to start Muse Code")?;
    let stdout = child.stdout.take().context("Muse Code stdout missing")?;
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
    let mut terminal: Option<String> = None;
    let mut reason: Option<String> = None;
    loop {
        let line = tokio::select! {
            _ = events.closed() => {
                let _ = child.kill().await;
                return Ok(());
            }
            line = lines.next_line() => line?,
        };
        let Some(line) = line else { break };
        let Some(event) = parse_muse_line(&line) else {
            continue;
        };
        match event {
            MuseEvent::Delta(chunk) => {
                text.push_str(&chunk);
                if events
                    .send(ChatStreamEvent::Delta(chunk))
                    .await
                    .is_err()
                {
                    let _ = child.kill().await;
                    return Ok(());
                }
            }
            MuseEvent::ToolResult {
                id,
                name,
                input,
                output,
                is_error,
            } => {
                if events
                    .send(ChatStreamEvent::ToolCall {
                        id: id.clone(),
                        name,
                        input: input.clone(),
                    })
                    .await
                    .is_err()
                {
                    let _ = child.kill().await;
                    return Ok(());
                }
                if events
                    .send(ChatStreamEvent::ToolResult {
                        tool_use_id: id,
                        output,
                        is_error,
                        input: Some(input),
                    })
                    .await
                    .is_err()
                {
                    let _ = child.kill().await;
                    return Ok(());
                }
            }
            MuseEvent::Terminal {
                terminal: state,
                text: final_text,
                reason: detail,
            } => {
                terminal = Some(state);
                reason = detail;
                if let Some(final_text) = final_text
                    && !final_text.is_empty()
                    && final_text != text
                {
                    // The terminal record carries the authoritative full text;
                    // emit only the part the deltas did not already deliver.
                    if let Some(suffix) = final_text.strip_prefix(text.as_str()) {
                        if !suffix.is_empty() {
                            text.push_str(suffix);
                            let _ = events
                                .send(ChatStreamEvent::Delta(suffix.to_string()))
                                .await;
                        }
                    } else {
                        text = final_text;
                    }
                }
            }
        }
    }

    let status = child.wait().await.context("failed to wait for Muse Code")?;
    let stderr = stderr_task.await.unwrap_or_default();
    let failed = terminal.as_deref().is_some_and(|state| state != "completed")
        || (!status.success() && text.trim().is_empty());
    if failed {
        let detail = reason
            .as_deref()
            .map(str::trim)
            .filter(|detail| !detail.is_empty())
            .or_else(|| {
                let stderr = stderr.trim();
                (!stderr.is_empty()).then_some(stderr)
            });
        match detail {
            Some(detail) => bail!("Muse Code run ended without completing: {detail}"),
            None => bail!("Muse Code exited with {status}"),
        }
    }
    let _ = events
        .send(ChatStreamEvent::Done {
            final_text: text,
            usage: None,
            session_id: Some(session_id),
            provider_turn_id: None,
        })
        .await;
    Ok(())
}

/// A fresh Muse process has no durable replay, so the first turn carries the
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

enum MuseEvent {
    Delta(String),
    ToolResult {
        id: String,
        name: String,
        input: Value,
        output: String,
        is_error: bool,
    },
    Terminal {
        terminal: String,
        text: Option<String>,
        reason: Option<String>,
    },
}

/// Parse one `muse exec --json` journal envelope. Unknown payload types are
/// ignored rather than failing: the journal also records approvals, edits and
/// subagent lifecycle, which Borg's compatibility route does not render.
fn parse_muse_line(line: &str) -> Option<MuseEvent> {
    let line = line.trim();
    if line.is_empty() {
        return None;
    }
    let record: Value = serde_json::from_str(line).ok()?;
    let payload_type = record.get("payload_type").and_then(Value::as_str)?;
    let payload = record.get("payload")?;
    if payload_type == "run.output.delta" {
        let text = payload.get("text").and_then(Value::as_str)?.to_string();
        return Some(MuseEvent::Delta(text));
    }
    if payload_type == "tool.result" {
        let id = payload.get("call_id").and_then(Value::as_str)?.to_string();
        let name = payload
            .pointer("/correlation_facts/tool_name")
            .and_then(Value::as_str)
            .or_else(|| payload.get("kind").and_then(Value::as_str))
            .unwrap_or("tool")
            .to_string();
        let text = payload.get("text").and_then(Value::as_str).unwrap_or_default();
        let failed = matches!(
            payload
                .pointer("/correlation_facts/outcome")
                .and_then(Value::as_str),
            Some("failure" | "failed" | "error")
        );
        let (input, output, is_error) =
            project_tool_result(text, payload.get("edit_facts"), failed);
        return Some(MuseEvent::ToolResult {
            id,
            name,
            input,
            output,
            is_error,
        });
    }
    if payload_type.starts_with("run.terminal.") {
        let terminal = payload
            .get("terminal")
            .and_then(Value::as_str)
            .unwrap_or("completed")
            .to_string();
        return Some(MuseEvent::Terminal {
            terminal,
            text: payload
                .get("text")
                .and_then(Value::as_str)
                .map(str::to_string),
            reason: payload
                .get("reason")
                .and_then(Value::as_str)
                .map(str::to_string),
        });
    }
    None
}

/// Turn one `tool.result` into the `(input, output, is_error)` a tool row needs.
///
/// The `bash`/`command` tool packs a structured `CommandResult` into `text`;
/// rendering that JSON verbatim would hide the command behind its own result,
/// so the command line and description become the row's input. Other tools
/// expose their arguments, when at all, through `edit_facts`.
fn project_tool_result(
    text: &str,
    edit_facts: Option<&Value>,
    failed: bool,
) -> (Value, String, bool) {
    if let Ok(command) = serde_json::from_str::<Value>(text)
        && command.get("command").and_then(Value::as_str).is_some()
    {
        let mut input = serde_json::Map::new();
        for key in ["command", "description"] {
            if let Some(value) = command.get(key).cloned() {
                input.insert(key.to_string(), value);
            }
        }
        let output = command
            .get("output")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let is_error = failed
            || command
                .get("exit_code")
                .and_then(Value::as_i64)
                .is_some_and(|code| code != 0);
        return (Value::Object(input), output, is_error);
    }
    let input = edit_facts
        .filter(|facts| facts.as_object().is_some_and(|map| !map.is_empty()))
        .cloned()
        .unwrap_or(Value::Null);
    (input, text.to_string(), failed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(payload_type: &str, payload: Value) -> String {
        serde_json::json!({
            "schema_version": 1,
            "id": "1",
            "stream": { "id": "s-1", "kind": "run" },
            "sequence": 1,
            "recorded_at": 0,
            "record_type": "event",
            "durability": "durable",
            "causation_id": "c",
            "payload_type": payload_type,
            "payload_schema_version": 1,
            "payload": payload,
        })
        .to_string()
    }

    #[test]
    fn output_deltas_carry_streamed_text() {
        let line = envelope("run.output.delta", serde_json::json!({"text": "hello"}));
        assert!(matches!(
            parse_muse_line(&line),
            Some(MuseEvent::Delta(text)) if text == "hello"
        ));
    }

    #[test]
    fn terminal_records_carry_state_and_final_text() {
        let line = envelope(
            "run.terminal.completed",
            serde_json::json!({"terminal": "completed", "text": "final"}),
        );
        assert!(matches!(
            parse_muse_line(&line),
            Some(MuseEvent::Terminal { terminal, text: Some(text), reason: None })
                if terminal == "completed" && text == "final"
        ));
    }

    #[test]
    fn unknown_payloads_and_junk_are_ignored() {
        assert!(parse_muse_line("not json").is_none());
        assert!(parse_muse_line("").is_none());
        let line = envelope(
            "run.model.configured",
            serde_json::json!({"model": "muse-spark-1.2"}),
        );
        assert!(parse_muse_line(&line).is_none());
    }

    #[test]
    fn tool_results_render_the_command_and_its_output() {
        let line = envelope(
            "tool.result",
            serde_json::json!({
                "kind": "tool",
                "call_id": "call_9",
                "text": "{\"chunk_id\":\"exec-1\",\"command\":\"ls -la\",\"description\":\"list files\",\"exit_code\":0,\"terminal_status\":\"completed\",\"output\":\"total 0\",\"truncated\":false}",
                "correlation_facts": {"tool_name": "bash", "outcome": "success"},
            }),
        );
        match parse_muse_line(&line) {
            Some(MuseEvent::ToolResult {
                id,
                name,
                input,
                output,
                is_error,
            }) => {
                assert_eq!(id, "call_9");
                assert_eq!(name, "bash");
                assert_eq!(input["command"], "ls -la");
                assert_eq!(output, "total 0");
                assert!(!is_error);
            }
            _ => panic!("a tool.result must parse into a tool row"),
        }
    }

    #[test]
    fn prose_tool_results_keep_their_text_and_failure() {
        let line = envelope(
            "tool.result",
            serde_json::json!({
                "kind": "tool",
                "call_id": "call_10",
                "text": "wrote 6 bytes to src/main.rs",
                "correlation_facts": {"tool_name": "write_file", "outcome": "failure"},
            }),
        );
        match parse_muse_line(&line) {
            Some(MuseEvent::ToolResult {
                name,
                output,
                is_error,
                ..
            }) => {
                assert_eq!(name, "write_file");
                assert_eq!(output, "wrote 6 bytes to src/main.rs");
                assert!(is_error);
            }
            _ => panic!("a prose tool.result must parse into a tool row"),
        }
    }
}
