use std::collections::HashSet;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;

use super::{ChatStreamEvent, ChatStreamRequest, LocalAgentPermission};
use crate::mcp::{ExternalMcpServer, normalize_mcp_tool_name};
use crate::runtime::ProviderCallUsage;

pub(super) async fn run(
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
    let mut command = Command::new("opencode");
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
        command.env(
            "OPENCODE_CONFIG_CONTENT",
            serde_json::to_string(&opencode_config(&request.mcp_external_servers))?,
        );
    }

    let mut child = command.spawn().context("failed to spawn OpenCode")?;
    let mut stdin = child
        .stdin
        .take()
        .context("OpenCode stdin was not available")?;
    stdin
        .write_all(request.prompt.as_bytes())
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
                emit_tool(&events, &value, &mut started_tools, &mut completed_tools).await?
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
        })
        .await
        .ok();
    Ok(())
}

fn opencode_config(servers: &[ExternalMcpServer]) -> Value {
    let mcp_servers = servers
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
    let mut permission = serde_json::Map::new();
    for server in servers {
        if server.allowed_tools.is_empty() {
            continue;
        }
        // OpenCode exposes MCP tools as `<server>_<tool>` and evaluates
        // wildcard permission rules in object order. serde_json's map is
        // sorted, so the `*` rule sorts before conventional MCP tool names
        // and the exact allow rules reliably win.
        permission.insert(format!("{}_{}", server.name, "*"), json!("deny"));
        for tool in &server.allowed_tools {
            let Some(tool) = normalize_mcp_tool_name(&server.name, tool) else {
                continue;
            };
            permission.insert(format!("{}_{}", server.name, tool), json!("allow"));
        }
    }
    let mut config = serde_json::Map::new();
    config.insert("mcp".to_string(), Value::Object(mcp_servers));
    if !permission.is_empty() {
        config.insert("permission".to_string(), Value::Object(permission));
    }
    Value::Object(config)
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
    if permission == LocalAgentPermission::FullAccess {
        args.push("--dangerously-skip-permissions".to_string());
    }
    args
}

async fn default_model() -> Result<String> {
    let output = Command::new("opencode")
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
    let status = state.get("status").and_then(Value::as_str);
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
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{
        ExternalMcpServer, LocalAgentPermission, opencode_config, opencode_run_args, parse_usage,
    };

    #[test]
    fn permission_mode_maps_to_supported_opencode_v2_argv() {
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
        assert!(!full_access.iter().any(|arg| arg == "--auto"));

        let guarded = opencode_run_args(None, "openai/gpt-5.6", None, LocalAgentPermission::Manual);
        assert!(
            !guarded
                .iter()
                .any(|arg| arg == "--dangerously-skip-permissions")
        );
        assert!(!guarded.iter().any(|arg| arg == "--auto"));
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

    #[test]
    fn opencode_config_denies_unlisted_mcp_tools() {
        let config = opencode_config(&[ExternalMcpServer {
            name: "docs".to_string(),
            command: "docs-mcp".to_string(),
            args: Vec::new(),
            env: BTreeMap::new(),
            allowed_tools: vec![
                "search".to_string(),
                "mcp__docs__read".to_string(),
                "mcp__other__secret".to_string(),
            ],
        }]);
        let permission = config
            .get("permission")
            .and_then(serde_json::Value::as_object)
            .expect("MCP permission policy");
        assert_eq!(
            permission.get("docs_*").and_then(|v| v.as_str()),
            Some("deny")
        );
        assert_eq!(
            permission.get("docs_search").and_then(|v| v.as_str()),
            Some("allow")
        );
        assert_eq!(
            permission.get("docs_read").and_then(|v| v.as_str()),
            Some("allow")
        );
        assert!(!permission.contains_key("docs_secret"));
    }
}
