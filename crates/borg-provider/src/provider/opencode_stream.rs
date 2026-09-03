use std::collections::HashSet;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

use super::{ChatStreamEvent, ChatStreamRequest, LocalAgentPermission, complete_tool_action};
use crate::runtime::ProviderCallUsage;

/// Run OpenCode as a provider subprocess while Borg remains the session and
/// event authority. OpenCode owns its provider-specific tool loop; its JSON
/// events are normalized into the same stream contract used by Codex/Claude.
pub fn run_opencode_local_chat_stream(
    request: ChatStreamRequest,
    permission: LocalAgentPermission,
) -> mpsc::Receiver<ChatStreamEvent> {
    let (events, receiver) = mpsc::channel(64);
    tokio::spawn(async move {
        if let Err(error) = run(request, events.clone(), permission).await {
            let _ = events
                .send(ChatStreamEvent::Failed {
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
    let model = match request.model.as_deref().map(str::trim) {
        Some(model) if !model.is_empty() => model.to_string(),
        _ => default_model().await?,
    };
    let cwd = match request.working_directory {
        Some(cwd) => cwd,
        None => std::env::current_dir().context("failed to resolve OpenCode working directory")?,
    };
    let mut command = crate::provider_bin::command(crate::provider_bin::Runtime::OpenCode).await?;
    command
        .args(opencode_run_args(
            request.session_id.as_deref(),
            &model,
            request.effort.as_deref(),
            permission,
        ))
        .current_dir(cwd)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    for attachment in &request.attachments {
        command.arg("--file").arg(attachment);
    }
    if !request.mcp_external_servers.is_empty() {
        let servers = request
            .mcp_external_servers
            .iter()
            .map(|server| {
                (
                    server.name.clone(),
                    serde_json::json!({
                        "type": "local",
                        "command": std::iter::once(server.command.clone())
                            .chain(server.args.iter().cloned())
                            .collect::<Vec<_>>(),
                        "environment": server.env,
                        "enabled": true,
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        command.env(
            "OPENCODE_CONFIG_CONTENT",
            serde_json::to_string(&serde_json::json!({ "mcp": servers }))?,
        );
    }

    let mut child = command.spawn().context("failed to spawn OpenCode")?;
    let mut stdin = child
        .stdin
        .take()
        .context("OpenCode stdin was not available")?;
    let prompt = if request.session_id.is_none() && !request.system_prompt.trim().is_empty() {
        format!(
            "{}\n\nUser request:\n{}",
            request.system_prompt.trim(),
            request.prompt
        )
    } else {
        request.prompt
    };
    stdin
        .write_all(prompt.as_bytes())
        .await
        .context("failed to write OpenCode prompt")?;
    stdin.shutdown().await.ok();
    drop(stdin);

    let stdout = child
        .stdout
        .take()
        .context("OpenCode stdout was not available")?;
    let stderr = child
        .stderr
        .take()
        .context("OpenCode stderr was not available")?;
    let stderr_task = tokio::spawn(async move {
        let mut stderr = stderr;
        let mut bytes = Vec::new();
        let _ = stderr.read_to_end(&mut bytes).await;
        bytes
    });
    let mut lines = BufReader::new(stdout).lines();
    let mut text = String::new();
    let mut session_id = request.session_id;
    let mut usage = ProviderCallUsage::default();
    let mut saw_usage = false;
    let mut generating_tools = HashSet::new();
    let mut described_tools = HashSet::new();
    let mut started_tools = HashSet::new();
    let mut completed_tools = HashSet::new();

    while let Some(line) = lines
        .next_line()
        .await
        .context("failed reading OpenCode output")?
    {
        let value: Value = serde_json::from_str(&line)
            .with_context(|| format!("OpenCode emitted invalid JSON: {line}"))?;
        if session_id.is_none()
            && let Some(value) = value.get("sessionID").and_then(Value::as_str)
        {
            session_id = Some(value.to_string());
        }
        let kind = value.get("type").and_then(Value::as_str).unwrap_or("event");
        events
            .send(ChatStreamEvent::ProviderEvent {
                kind: format!("opencode/{kind}"),
                payload: value.clone(),
                raw_payload: Some(value.clone()),
                stream_channel: Some(kind.to_string()),
                content_text: value
                    .pointer("/part/text")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                provider_item_id: value
                    .pointer("/part/id")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                tool_use_id: value
                    .pointer("/part/id")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                tool_name: value
                    .pointer("/part/tool")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            })
            .await
            .ok();
        match kind {
            "text" => {
                if let Some(output) = value.pointer("/part/text").and_then(Value::as_str) {
                    text.push_str(output);
                    if events
                        .send(ChatStreamEvent::Delta(output.to_string()))
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                }
            }
            "reasoning" => {
                if let Some(output) = value.pointer("/part/text").and_then(Value::as_str)
                    && events
                        .send(ChatStreamEvent::ReasoningDelta(output.to_string()))
                        .await
                        .is_err()
                {
                    return Ok(());
                }
            }
            "tool_use" => {
                emit_tool(
                    &events,
                    &value,
                    &mut generating_tools,
                    &mut described_tools,
                    &mut started_tools,
                    &mut completed_tools,
                )
                .await?
            }
            "step_finish" => {
                if let Some(step_usage) = parse_usage(&value) {
                    usage.input_tokens = usage.input_tokens.saturating_add(step_usage.input_tokens);
                    usage.cached_input_tokens = usage
                        .cached_input_tokens
                        .saturating_add(step_usage.cached_input_tokens);
                    usage.output_tokens =
                        usage.output_tokens.saturating_add(step_usage.output_tokens);
                    usage.total_tokens = usage.total_tokens.saturating_add(step_usage.total_tokens);
                    usage.cost_microusd = match (usage.cost_microusd, step_usage.cost_microusd) {
                        (Some(total), Some(step)) => Some(total.saturating_add(step)),
                        (total, None) => total,
                        (None, step) => step,
                    };
                    saw_usage = true;
                }
            }
            "error" => {
                let message = value
                    .pointer("/error/data/message")
                    .or_else(|| value.get("error"))
                    .map(Value::to_string)
                    .unwrap_or_else(|| "OpenCode turn failed".to_string());
                bail!("{message}");
            }
            _ => {}
        }
    }

    let status = child.wait().await.context("failed waiting for OpenCode")?;
    let stderr = stderr_task.await.unwrap_or_default();
    if !status.success() {
        bail!(
            "OpenCode exited with {}: {}",
            status,
            String::from_utf8_lossy(&stderr).trim()
        );
    }
    events
        .send(ChatStreamEvent::Done {
            final_text: text,
            usage: saw_usage.then(|| ProviderCallUsage {
                duration_ms: u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
                ..usage
            }),
            session_id,
            provider_turn_id: None,
        })
        .await
        .ok();
    Ok(())
}

fn opencode_run_args(
    session_id: Option<&str>,
    model: &str,
    effort: Option<&str>,
    permission: LocalAgentPermission,
) -> Vec<String> {
    let mut args = vec![
        "run".to_string(),
        "--format".to_string(),
        "json".to_string(),
    ];
    if let Some(session_id) = session_id {
        args.extend(["--session".to_string(), session_id.to_string()]);
    }
    args.extend(["--model".to_string(), model.to_string()]);
    if let Some(effort) = effort {
        args.extend(["--variant".to_string(), effort.to_string()]);
    }
    match permission {
        LocalAgentPermission::FullAccess => {
            args.push("--dangerously-skip-permissions".to_string());
        }
        LocalAgentPermission::Auto => {
            args.push("--auto".to_string());
        }
        LocalAgentPermission::Manual => {}
    }
    args
}

async fn default_model() -> Result<String> {
    let output = crate::provider_bin::command(crate::provider_bin::Runtime::OpenCode)
        .await?
        .arg("models")
        .stdin(std::process::Stdio::null())
        .output()
        .await
        .context("failed to list OpenCode models")?;
    if !output.status.success() {
        bail!("OpenCode has no configured model; choose one with --model or run `opencode` once");
    }
    let models = String::from_utf8_lossy(&output.stdout);
    let first = models
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .context("OpenCode returned no available models; configure a provider first")?;
    Ok(models
        .lines()
        .map(str::trim)
        .find(|model| *model == "opencode/big-pickle")
        .unwrap_or(first)
        .to_string())
}

async fn emit_tool(
    events: &mpsc::Sender<ChatStreamEvent>,
    value: &Value,
    generating_tools: &mut HashSet<String>,
    described_tools: &mut HashSet<String>,
    started_tools: &mut HashSet<String>,
    completed_tools: &mut HashSet<String>,
) -> Result<()> {
    let id = value
        .pointer("/part/id")
        .and_then(Value::as_str)
        .unwrap_or("opencode-tool")
        .to_string();
    let name = value
        .pointer("/part/tool")
        .and_then(Value::as_str)
        .unwrap_or("tool")
        .to_string();
    let state = value.pointer("/part/state").cloned().unwrap_or(Value::Null);
    let input = state.get("input").cloned().unwrap_or(Value::Null);
    if generating_tools.insert(id.clone())
        && events
            .send(ChatStreamEvent::ToolCallGenerating {
                id: Some(id.clone()),
            })
            .await
            .is_err()
    {
        return Ok(());
    }
    if !started_tools.contains(&id)
        && !described_tools.contains(&id)
        && let Some(action) = complete_tool_action(&input)
    {
        described_tools.insert(id.clone());
        if events
            .send(ChatStreamEvent::ToolCallAction {
                id: id.clone(),
                action,
            })
            .await
            .is_err()
        {
            return Ok(());
        }
    }
    let status = state.get("status").and_then(Value::as_str);
    if status == Some("pending") {
        return Ok(());
    }
    if started_tools.insert(id.clone())
        && events
            .send(ChatStreamEvent::ToolCall {
                id: id.clone(),
                name,
                input: input.clone(),
            })
            .await
            .is_err()
    {
        return Ok(());
    }
    if !matches!(status, Some("completed" | "error")) || !completed_tools.insert(id.clone()) {
        return Ok(());
    }
    let is_error = status == Some("error");
    let output = state
        .get(if is_error { "error" } else { "output" })
        .map(Value::to_string)
        .unwrap_or_default();
    events
        .send(ChatStreamEvent::ToolResult {
            tool_use_id: id,
            output,
            is_error,
            input: Some(input),
        })
        .await
        .ok();
    Ok(())
}

fn parse_usage(value: &Value) -> Option<ProviderCallUsage> {
    let tokens = value.pointer("/part/tokens")?;
    let input_tokens = tokens.get("input").and_then(Value::as_u64).unwrap_or(0);
    let output_tokens = tokens.get("output").and_then(Value::as_u64).unwrap_or(0);
    let cached_input_tokens = tokens
        .pointer("/cache/read")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let total_tokens = tokens
        .get("total")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| {
            input_tokens
                .saturating_add(output_tokens)
                .saturating_add(cached_input_tokens)
        });
    let cost_microusd = value
        .pointer("/part/cost")
        .and_then(Value::as_f64)
        .map(|cost| (cost.max(0.0) * 1_000_000.0).round() as u64);
    Some(ProviderCallUsage {
        input_tokens,
        cached_input_tokens,
        output_tokens,
        total_tokens,
        cost_microusd,
        ..ProviderCallUsage::default()
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use serde_json::json;
    use tokio::sync::mpsc;

    use super::{ChatStreamEvent, LocalAgentPermission, emit_tool, opencode_run_args, parse_usage};

    #[test]
    fn permission_mode_maps_to_supported_opencode_argv() {
        let full_access = opencode_run_args(
            Some("session-1"),
            "openai/gpt-5.6",
            Some("medium"),
            LocalAgentPermission::FullAccess,
        );
        assert!(
            full_access
                .iter()
                .any(|arg| arg == "--dangerously-skip-permissions")
        );

        let guarded = opencode_run_args(None, "openai/gpt-5.6", None, LocalAgentPermission::Manual);
        assert!(
            !guarded
                .iter()
                .any(|arg| arg == "--dangerously-skip-permissions")
        );

        let automatic = opencode_run_args(None, "openai/gpt-5.6", None, LocalAgentPermission::Auto);
        assert!(automatic.iter().any(|arg| arg == "--auto"));
    }

    #[test]
    fn step_finish_usage_preserves_cache_tokens_and_cost() {
        let usage = parse_usage(&json!({
            "type": "step_finish",
            "part": {
                "cost": 0.125,
                "tokens": {
                    "input": 35,
                    "output": 9,
                    "total": 13248,
                    "cache": { "read": 13184, "write": 0 }
                }
            }
        }))
        .expect("step usage");

        assert_eq!(usage.input_tokens, 35);
        assert_eq!(usage.output_tokens, 9);
        assert_eq!(usage.cached_input_tokens, 13_184);
        assert_eq!(usage.total_tokens, 13_248);
        assert_eq!(usage.cost_microusd, Some(125_000));
    }

    #[tokio::test]
    async fn pending_tool_generation_is_visible_until_opencode_starts_it() {
        let (sender, mut receiver) = mpsc::channel(8);
        let mut generating = HashSet::new();
        let mut described = HashSet::new();
        let mut started = HashSet::new();
        let mut completed = HashSet::new();
        emit_tool(
            &sender,
            &json!({
                "type": "tool_use",
                "part": {
                    "id": "tool-1",
                    "tool": "mcp__borg_agent__update_plan",
                    "state": {
                        "status": "pending",
                        "input": {"action": "edit"}
                    }
                }
            }),
            &mut generating,
            &mut described,
            &mut started,
            &mut completed,
        )
        .await
        .unwrap();

        assert!(matches!(
            receiver.recv().await,
            Some(ChatStreamEvent::ToolCallGenerating { id: Some(id) }) if id == "tool-1"
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(ChatStreamEvent::ToolCallAction { id, action })
                if id == "tool-1" && action == "edit"
        ));
        assert!(receiver.try_recv().is_err());

        emit_tool(
            &sender,
            &json!({
                "type": "tool_use",
                "part": {
                    "id": "tool-1",
                    "tool": "mcp__borg_agent__update_plan",
                    "state": {
                        "status": "running",
                        "input": {"action": "edit", "plan": []}
                    }
                }
            }),
            &mut generating,
            &mut described,
            &mut started,
            &mut completed,
        )
        .await
        .unwrap();

        assert!(matches!(
            receiver.recv().await,
            Some(ChatStreamEvent::ToolCall { id, name, .. })
                if id == "tool-1" && name == "mcp__borg_agent__update_plan"
        ));
        assert!(receiver.try_recv().is_err());
    }
}
