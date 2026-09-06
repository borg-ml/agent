use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;

use super::{ChatStreamEvent, ChatStreamRequest, LocalAgentPermission, complete_tool_action};
use crate::runtime::ProviderCallUsage;

/// OpenCode owns its tool loop in a private local server. Subscribe before
/// prompting so tool-input starts are visible before arguments are complete.
pub fn run_opencode_local_chat_stream(
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
        .current_dir(&cwd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    let mut config = serde_json::json!({});
    if permission == LocalAgentPermission::FullAccess {
        config["permission"] = serde_json::json!({"*": "allow"});
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
        config["mcp"] = Value::Object(servers);
    }
    if config.as_object().is_some_and(|config| !config.is_empty()) {
        command.env("OPENCODE_CONFIG_CONTENT", serde_json::to_string(&config)?);
    }

    let password = uuid::Uuid::new_v4().to_string();
    command
        .args(["serve", "--hostname", "127.0.0.1", "--port", "0"])
        .env("OPENCODE_SERVER_PASSWORD", &password)
        .env("OPENCODE_SERVER_USERNAME", "opencode");
    let mut server = command.spawn().context("failed to start OpenCode server")?;
    let mut server_output = BufReader::new(
        server
            .stdout
            .take()
            .context("OpenCode server stdout missing")?,
    )
    .lines();
    let server_url = tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(line) = server_output.next_line().await? {
            if let Some(url) = line.strip_prefix("opencode server listening on http://127.0.0.1:") {
                let port: u16 = url.trim().parse().context("invalid OpenCode server port")?;
                return Ok::<_, anyhow::Error>(format!("http://127.0.0.1:{port}"));
            }
        }
        bail!("OpenCode server closed before listening")
    })
    .await
    .context("OpenCode server startup timed out")??;
    let client = reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(10))
        .build()?;
    let session_id = match request.session_id.as_ref() {
        Some(id) => id.clone(),
        None => client
            .post(format!("{server_url}/session"))
            .basic_auth("opencode", Some(&password))
            .query(&[("directory", &cwd)])
            .json(&serde_json::json!({}))
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?
            .get("id")
            .and_then(Value::as_str)
            .context("OpenCode session response missing id")?
            .to_string(),
    };
    let mut server_events = client
        .get(format!("{server_url}/event"))
        .basic_auth("opencode", Some(&password))
        .query(&[("directory", &cwd)])
        .send()
        .await?
        .error_for_status()?;
    let mut server_pending = Vec::new();
    let prompt = if request.session_id.is_none() && !request.system_prompt.trim().is_empty() {
        format!(
            "{}\n\nUser request:\n{}",
            request.system_prompt.trim(),
            request.prompt
        )
    } else {
        request.prompt
    };
    let mut parts = Vec::new();
    for attachment in &request.attachments {
        let path = if attachment.is_absolute() {
            attachment.clone()
        } else {
            cwd.join(attachment)
        };
        let path = path
            .canonicalize()
            .context("OpenCode attachment not found")?;
        parts.push(serde_json::json!({
            "type": "file",
            "url": reqwest::Url::from_file_path(&path).map_err(|_| anyhow::anyhow!("invalid OpenCode attachment path"))?.as_str(),
            "filename": path.file_name().and_then(|name| name.to_str()),
            "mime": if path.is_dir() { "application/x-directory" } else { "text/plain" },
        }));
    }
    parts.push(serde_json::json!({"type": "text", "text": prompt}));
    let (provider_id, model_id) = model
        .split_once('/')
        .context("OpenCode model must be provider/model")?;
    let mut input = serde_json::json!({
        "model": {"providerID": provider_id, "modelID": model_id},
        "parts": parts,
    });
    if let Some(effort) = request.effort {
        input["variant"] = Value::String(effort);
    }
    client
        .post(format!("{server_url}/session/{session_id}/prompt_async"))
        .basic_auth("opencode", Some(&password))
        .query(&[("directory", &cwd)])
        .json(&input)
        .send()
        .await?
        .error_for_status()?;
    let mut text = String::new();
    let mut completed_parts = HashSet::new();
    let mut usage = ProviderCallUsage::default();
    let mut saw_usage = false;
    let mut generating_tools = HashSet::new();
    let mut pending_tool_snapshots = HashMap::new();
    let mut described_tools = HashSet::new();
    let mut started_tools = HashSet::new();
    let mut completed_tools = HashSet::new();

    loop {
        let event = tokio::select! {
            _ = events.closed() => return Ok(()),
            event = next_server_event(&mut server_events, &mut server_pending) => event?,
        };
        let props = &event["properties"];
        let kind = event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let value = if kind == "message.part.updated" {
            let part = &props["part"];
            if part.get("sessionID").and_then(Value::as_str) != Some(&session_id) {
                continue;
            }
            let part_kind = part.get("type").and_then(Value::as_str).unwrap_or_default();
            let kind = match part_kind {
                "tool" => "tool_use",
                "step-finish" => "step_finish",
                "text" | "reasoning" if !part.pointer("/time/end").is_none_or(Value::is_null) => {
                    part_kind
                }
                _ => continue,
            };
            if part_kind != "tool"
                && !completed_parts.insert(
                    part.get("id")
                        .and_then(Value::as_str)
                        .context("OpenCode part missing id")?
                        .to_string(),
                )
            {
                continue;
            }
            serde_json::json!({"type": kind, "part": part})
        } else {
            if props.get("sessionID").and_then(Value::as_str) != Some(&session_id) {
                continue;
            }
            match kind {
                "session.idle" => break,
                "session.status"
                    if props.pointer("/status/type").and_then(Value::as_str) == Some("idle") =>
                {
                    break;
                }
                "session.error" => serde_json::json!({"type": "error", "error": props["error"]}),
                "permission.asked" => {
                    let id = props
                        .get("id")
                        .and_then(Value::as_str)
                        .context("OpenCode permission missing id")?;
                    let reply = match permission {
                        LocalAgentPermission::FullAccess | LocalAgentPermission::Auto => "once",
                        LocalAgentPermission::Manual => "reject",
                    };
                    let response = client
                        .post(format!("{server_url}/permission/{id}/reply"))
                        .basic_auth("opencode", Some(&password))
                        .query(&[("directory", &cwd)])
                        .json(&serde_json::json!({"reply": reply}))
                        .send()
                        .await?;
                    if response.status() == reqwest::StatusCode::NOT_FOUND {
                        client
                            .post(format!(
                                "{server_url}/session/{session_id}/permissions/{id}"
                            ))
                            .basic_auth("opencode", Some(&password))
                            .query(&[("directory", &cwd)])
                            .json(&serde_json::json!({"response": reply}))
                            .send()
                            .await?
                            .error_for_status()?;
                    } else {
                        response.error_for_status()?;
                    }
                    continue;
                }
                _ => continue,
            }
        };
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
                    &mut pending_tool_snapshots,
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

    events
        .send(ChatStreamEvent::Done {
            final_text: text,
            usage: saw_usage.then(|| ProviderCallUsage {
                duration_ms: u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
                ..usage
            }),
            session_id: Some(session_id),
            provider_turn_id: None,
        })
        .await
        .ok();
    Ok(())
}

async fn next_server_event(
    response: &mut reqwest::Response,
    pending: &mut Vec<u8>,
) -> Result<Value> {
    loop {
        while let Some(end) = pending.iter().position(|byte| *byte == b'\n') {
            let line = pending.drain(..=end).collect::<Vec<_>>();
            let Some(data) = line.strip_prefix(b"data:") else {
                continue;
            };
            let event: Value =
                serde_json::from_slice(data).context("invalid OpenCode server event")?;
            return Ok(event);
        }
        let chunk = response
            .chunk()
            .await
            .context("failed reading OpenCode server events")?
            .context("OpenCode server event stream closed")?;
        anyhow::ensure!(
            pending.len().saturating_add(chunk.len()) <= 8 * 1024 * 1024,
            "OpenCode server event exceeded 8 MiB"
        );
        pending.extend_from_slice(&chunk);
    }
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

struct PendingToolInput {
    name: String,
    input: Value,
    raw: String,
    action_parser: crate::provider::StreamedToolAction,
}

async fn emit_tool(
    events: &mpsc::Sender<ChatStreamEvent>,
    value: &Value,
    generating_tools: &mut HashSet<String>,
    pending_tool_snapshots: &mut HashMap<String, PendingToolInput>,
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
    let status = state.get("status").and_then(Value::as_str);
    let raw = state
        .get("raw")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let mut snapshot = PendingToolInput {
        name: name.clone(),
        input: input.clone(),
        raw,
        action_parser: Default::default(),
    };
    let mut raw_action = None;
    if status == Some("pending")
        && !started_tools.contains(&id)
        && generating_tools.insert(id.clone())
        && events
            .send(ChatStreamEvent::ToolCallGenerating {
                id: Some(id.clone()),
            })
            .await
            .is_err()
    {
        return Ok(());
    }
    if status == Some("pending") && !started_tools.contains(&id) {
        if let Some(previous) = pending_tool_snapshots.get(&id)
            && ((!snapshot.name.is_empty() && snapshot.name != previous.name)
                || (!snapshot.input.is_null()
                    && snapshot.input != Value::Object(Default::default())
                    && snapshot.input != previous.input)
                || (!snapshot.raw.is_empty() && snapshot.raw != previous.raw))
            && events
                .send(ChatStreamEvent::ToolCallInputDelta {
                    id: Some(id.clone()),
                })
                .await
                .is_err()
        {
            return Ok(());
        }
        if let Some(previous) = pending_tool_snapshots.get_mut(&id)
            && snapshot.raw.starts_with(&previous.raw)
        {
            snapshot.action_parser = std::mem::take(&mut previous.action_parser);
        }
        raw_action = snapshot.action_parser.observe(&snapshot.raw);
        pending_tool_snapshots.insert(id.clone(), snapshot);
    }
    if status == Some("pending")
        && !started_tools.contains(&id)
        && !described_tools.contains(&id)
        && let Some(action) = complete_tool_action(&input).or(raw_action)
    {
        described_tools.insert(id.clone());
        if events
            .send(ChatStreamEvent::ToolCallAction {
                id: Some(id.clone()),
                action,
            })
            .await
            .is_err()
        {
            return Ok(());
        }
    }
    if status == Some("pending") {
        return Ok(());
    }
    if !matches!(status, Some("running" | "completed" | "error")) {
        return Ok(());
    }
    pending_tool_snapshots.remove(&id);
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
    use std::collections::{HashMap, HashSet};

    use serde_json::json;
    use tokio::sync::mpsc;

    use super::{ChatStreamEvent, emit_tool, parse_usage};

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
        let mut snapshots = HashMap::new();
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
                        "input": {},
                        "raw": "{"
                    }
                }
            }),
            &mut generating,
            &mut snapshots,
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
        assert!(receiver.try_recv().is_err());
        emit_tool(
            &sender,
            &json!({"part": {
                "id": "tool-1", "tool": "mcp__borg_agent__update_plan",
                "state": {"status": "pending", "input": {},
                    "raw": "{\"nested\":{\"action\":\"wrong\"},\"action\":\"edit\",\"plan\":["}
            }}),
            &mut generating,
            &mut snapshots,
            &mut described,
            &mut started,
            &mut completed,
        )
        .await
        .unwrap();
        assert!(matches!(
            receiver.recv().await,
            Some(ChatStreamEvent::ToolCallInputDelta { id: Some(id) }) if id == "tool-1"
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(ChatStreamEvent::ToolCallAction { id: Some(id), action })
                if id == "tool-1" && action == "edit"
        ));
        assert!(receiver.try_recv().is_err());
        emit_tool(
            &sender,
            &json!({"part": {
                "id": "tool-1", "tool": "mcp__borg_agent__update_plan",
                "state": {"status": "pending", "input": {},
                    "raw": "{\"nested\":{\"action\":\"wrong\"},\"action\":\"edit\",\"plan\":["}
            }}),
            &mut generating,
            &mut snapshots,
            &mut described,
            &mut started,
            &mut completed,
        )
        .await
        .unwrap();
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
            &mut snapshots,
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

    #[tokio::test]
    async fn late_tool_snapshots_never_invent_generation_or_reopen_it() {
        for status in ["running", "completed", "error"] {
            let (sender, mut receiver) = mpsc::channel(8);
            let mut generating = HashSet::new();
            let mut snapshots = HashMap::new();
            let mut described = HashSet::new();
            let mut started = HashSet::new();
            let mut completed = HashSet::new();
            let mut snapshot = json!({"part": {
                "id": "tool-1", "tool": "bash",
                "state": {"status": status, "input": {"action": "read file"},
                          "output": "file contents", "error": "read failed"}
            }});
            emit_tool(
                &sender,
                &snapshot,
                &mut generating,
                &mut snapshots,
                &mut described,
                &mut started,
                &mut completed,
            )
            .await
            .unwrap();
            assert!(
                matches!(receiver.try_recv().unwrap(), ChatStreamEvent::ToolCall { id, .. } if id == "tool-1")
            );
            if status != "running" {
                assert!(
                    matches!(receiver.try_recv().unwrap(), ChatStreamEvent::ToolResult { is_error, .. } if is_error == (status == "error"))
                );
            }
            assert!(receiver.try_recv().is_err());
            snapshot["part"]["state"]["status"] = json!("pending");
            emit_tool(
                &sender,
                &snapshot,
                &mut generating,
                &mut snapshots,
                &mut described,
                &mut started,
                &mut completed,
            )
            .await
            .unwrap();
            assert!(receiver.try_recv().is_err());
        }
    }
}
