use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;

use super::{
    ChatStreamEvent, ChatStreamRequest, LocalAgentPermission, ProviderErrorKind,
    ProviderStreamError, classify_provider_error, complete_tool_action,
};
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
                    kind: classify_provider_error(&error),
                    error: format!("{error:#}"),
                })
                .await;
        }
    });
    receiver
}

/// OpenCode's `question` tool parks the turn until a human answers it through
/// OpenCode's own UI, which Borg never renders: the call just burns the turn's
/// clock and then resolves empty. Borg already owns asking the human (steers,
/// approvals, provider interactions), so the tool is taken away rather than
/// left to time out.
///
/// OpenCode's `task` tool is its provider-native subagent launcher. Borg owns
/// delegation through `spawn_agent`, so the tool is denied here too, on both
/// levers OpenCode 1.18.31 actually honours: the per-prompt `tools` body (the
/// user message's `tools` map removes the tool and is also folded into session
/// permissions) and the config `permission` entry (the backstop for agents
/// that inherit config rather than our prompt body).
///
/// A top-level `tools` key in the config file is ignored by OpenCode, so only
/// per-prompt and per-agent overrides bite.
fn blocked_tools() -> Value {
    serde_json::json!({ "question": false, "task": false })
}

fn opencode_base_config(permission: LocalAgentPermission) -> Value {
    let mut config = serde_json::json!({});
    if permission == LocalAgentPermission::FullAccess {
        config["permission"] =
            serde_json::json!({"*": "allow", "question": "deny", "task": "deny"});
    } else {
        config["permission"] = serde_json::json!({"question": "deny", "task": "deny"});
    }
    config
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
    if model.starts_with("opencode-go/") {
        let key = crate::credentials::opencode_go_api_key()
            .context("OpenCode Go is not connected; use /login or borg login opencode --api-key")?;
        let mut auth = crate::credentials::opencode_auth_json()?;
        auth["opencode-go"] = serde_json::json!({"type": "api", "key": key});
        command.env("OPENCODE_AUTH_CONTENT", serde_json::to_string(&auth)?);
    }
    command
        .current_dir(&cwd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    let mut config = opencode_base_config(permission);
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
    let mut server_events = EventStream::connect(
        client.clone(),
        server_url.clone(),
        password.clone(),
        cwd.clone(),
        session_id.clone(),
    )
    .await?;
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
        "tools": blocked_tools(),
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
            event = server_events.next() => event?,
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

/// OpenCode's `/event` bus is a plain chunked HTTP stream with no resume
/// cursor, and it does drop mid-turn on long tool calls. The work itself lives
/// in the OpenCode server, not in this connection, so losing the socket must
/// not lose the turn: reconnect, replay the session's parts to recover what was
/// missed, and ask whether the session went idle while we were disconnected.
///
/// Replay is safe because every consumer downstream already de-duplicates by
/// part id (`completed_parts`) or tool id (`started_tools` / `completed_tools`),
/// so re-delivering parts we have already seen is a no-op.
struct EventStream {
    client: reqwest::Client,
    server_url: String,
    password: String,
    cwd: std::path::PathBuf,
    session_id: String,
    response: reqwest::Response,
    pending: Vec<u8>,
    replay: std::collections::VecDeque<Value>,
    reconnects: u32,
}

/// Enough to ride out a server hiccup without spinning forever on a server that
/// is genuinely gone.
const MAX_RECONNECTS: u32 = 64;
const RECONNECT_ATTEMPTS: u32 = 5;

impl EventStream {
    async fn connect(
        client: reqwest::Client,
        server_url: String,
        password: String,
        cwd: std::path::PathBuf,
        session_id: String,
    ) -> Result<Self> {
        let response = Self::subscribe(&client, &server_url, &password, &cwd).await?;
        Ok(Self {
            client,
            server_url,
            password,
            cwd,
            session_id,
            response,
            pending: Vec::new(),
            replay: std::collections::VecDeque::new(),
            reconnects: 0,
        })
    }

    async fn subscribe(
        client: &reqwest::Client,
        server_url: &str,
        password: &str,
        cwd: &std::path::Path,
    ) -> Result<reqwest::Response> {
        Ok(client
            .get(format!("{server_url}/event"))
            .basic_auth("opencode", Some(password))
            .query(&[("directory", &cwd)])
            .send()
            .await?
            .error_for_status()?)
    }

    async fn next(&mut self) -> Result<Value> {
        loop {
            if let Some(event) = self.replay.pop_front() {
                return Ok(event);
            }
            while let Some(end) = self.pending.iter().position(|byte| *byte == b'\n') {
                let line = self.pending.drain(..=end).collect::<Vec<_>>();
                let Some(data) = line.strip_prefix(b"data:") else {
                    continue;
                };
                let event: Value =
                    serde_json::from_slice(data).context("invalid OpenCode server event")?;
                return Ok(event);
            }
            // Both a read error and a clean end-of-stream mean the same thing
            // here: this socket is finished, but the turn may not be.
            match self.response.chunk().await {
                Ok(Some(chunk)) => {
                    anyhow::ensure!(
                        self.pending.len().saturating_add(chunk.len()) <= 8 * 1024 * 1024,
                        "OpenCode server event exceeded 8 MiB"
                    );
                    self.pending.extend_from_slice(&chunk);
                }
                Ok(None) => self.recover(None).await?,
                Err(error) => self.recover(Some(error)).await?,
            }
        }
    }

    /// Re-establish the stream, then reconcile: queue every part of this
    /// session for replay, and if the session is no longer busy queue the
    /// `session.idle` the caller is waiting for. Resubscribing *before*
    /// reconciling is deliberate — the other order can miss a session that goes
    /// idle in the window between the two calls, hanging the turn forever.
    async fn recover(&mut self, error: Option<reqwest::Error>) -> Result<()> {
        self.reconnects = self.reconnects.saturating_add(1);
        if self.reconnects > MAX_RECONNECTS {
            let detail = error
                .map(|error| format!("{error}"))
                .unwrap_or_else(|| "stream closed".to_string());
            return Err(anyhow::Error::new(ProviderStreamError {
                kind: ProviderErrorKind::ConnectionLost,
                message: format!("OpenCode server event stream kept dropping ({detail})"),
            }));
        }
        let mut last: Option<anyhow::Error> = None;
        for attempt in 0..RECONNECT_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(100 * u64::from(attempt) * 2 + 50)).await;
            match Self::subscribe(&self.client, &self.server_url, &self.password, &self.cwd).await {
                Ok(response) => {
                    self.response = response;
                    self.pending.clear();
                    return self.resync().await;
                }
                Err(error) => last = Some(error),
            }
        }
        let context = last
            .map(|error| format!("{error:#}"))
            .unwrap_or_else(|| "unknown error".to_string());
        Err(anyhow::Error::new(ProviderStreamError {
            kind: ProviderErrorKind::ConnectionLost,
            message: format!("failed reading OpenCode server events: reconnect failed ({context})"),
        }))
    }

    async fn resync(&mut self) -> Result<()> {
        let messages = self
            .client
            .get(format!(
                "{}/session/{}/message",
                self.server_url, self.session_id
            ))
            .basic_auth("opencode", Some(&self.password))
            .query(&[("directory", &self.cwd)])
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        for message in messages.as_array().into_iter().flatten() {
            for part in message
                .get("parts")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                self.replay.push_back(serde_json::json!({
                    "type": "message.part.updated",
                    "properties": {"part": part},
                }));
            }
        }
        let status = self
            .client
            .get(format!("{}/session/status", self.server_url))
            .basic_auth("opencode", Some(&self.password))
            .query(&[("directory", &self.cwd)])
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        if status.get(&self.session_id).is_none() {
            self.replay.push_back(serde_json::json!({
                "type": "session.idle",
                "properties": {"sessionID": self.session_id},
            }));
        }
        Ok(())
    }
}

async fn default_model() -> Result<String> {
    if crate::credentials::opencode_go_api_key().is_some() {
        let models = crate::refresh_opencode_go_model_catalog().await?;
        return models
            .into_iter()
            .next()
            .map(|model| model.id)
            .context("OpenCode Go returned no available models; choose one with /model");
    }
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

    use std::time::Duration;

    use serde_json::json;
    use tokio::sync::mpsc;

    use super::{
        ChatStreamEvent, EventStream, LocalAgentPermission, blocked_tools, emit_tool,
        opencode_base_config, parse_usage,
    };

    /// A stand-in OpenCode server that aborts the event stream the way the real
    /// one does: mid-chunk, so reqwest surfaces "unexpected EOF during chunk
    /// size line" rather than a clean end of body.
    async fn serve_flaky_event_stream(listener: tokio::net::TcpListener, session_id: &'static str) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut event_requests = 0;
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut request = Vec::new();
            let head = loop {
                let mut chunk = [0u8; 2048];
                let Ok(read) = socket.read(&mut chunk).await else {
                    return;
                };
                if read == 0 {
                    break None;
                }
                request.extend_from_slice(&chunk[..read]);
                if let Some(end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                    break Some(end);
                }
            };
            if head.is_none() {
                continue;
            }
            let head = String::from_utf8_lossy(&request).to_string();
            let target = head.lines().next().unwrap_or_default().to_string();

            if target.contains("/event") {
                event_requests += 1;
                let first = event_requests == 1;
                tokio::spawn(async move {
                    let event = json!({
                        "type": "message.part.updated",
                        "properties": {"part": {
                            "id": if first { "prt_first" } else { "prt_second" },
                            "sessionID": session_id,
                            "type": "text",
                            "text": "streamed",
                            "time": {"start": 0, "end": 1},
                        }},
                    })
                    .to_string();
                    let payload = format!("data: {event}\n\n");
                    let _ = socket
                        .write_all(
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{payload}\r\n",
                                payload.len()
                            )
                            .as_bytes(),
                        )
                        .await;
                    if first {
                        // Truncate in the middle of the next chunk-size line.
                        let _ = socket.write_all(b"1").await;
                        let _ = socket.shutdown().await;
                    } else {
                        // Stay open so the test observes the replayed catch-up
                        // rather than a second reconnect.
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                });
                continue;
            }

            let body = if target.contains("/message") {
                json!([{ "parts": [
                    {"id": "prt_missed", "sessionID": session_id, "type": "text",
                     "text": "missed while disconnected", "time": {"start": 0, "end": 1}},
                ]}])
                .to_string()
            } else {
                // No entry for this session: it went idle during the outage.
                json!({}).to_string()
            };
            let _ = socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await;
        }
    }

    #[tokio::test]
    async fn a_dropped_event_stream_reconnects_and_recovers_the_turn() -> anyhow::Result<()> {
        const SESSION: &str = "ses_test";
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(serve_flaky_event_stream(listener, SESSION));

        let mut stream = EventStream::connect(
            reqwest::Client::builder().no_proxy().build()?,
            format!("http://{address}"),
            "p".to_string(),
            std::env::temp_dir(),
            SESSION.to_string(),
        )
        .await?;

        let first = stream.next().await?;
        assert_eq!(first["properties"]["part"]["id"], json!("prt_first"));

        // The stream is now severed mid-chunk. Previously this returned an
        // error and killed the turn; it must instead replay what was missed...
        let recovered = tokio::time::timeout(Duration::from_secs(10), stream.next()).await??;
        assert_eq!(recovered["properties"]["part"]["id"], json!("prt_missed"));

        // ...and then report the idle the caller is blocked on, so the turn
        // ends normally instead of hanging.
        let idle = tokio::time::timeout(Duration::from_secs(10), stream.next()).await??;
        assert_eq!(idle["type"], json!("session.idle"));
        assert_eq!(idle["properties"]["sessionID"], json!(SESSION));
        Ok(())
    }

    #[test]
    fn question_tool_is_blocked_in_every_permission_mode() {
        // The prompt body is what actually removes the tool; the config only
        // backstops agents Borg does not prompt directly.
        assert_eq!(blocked_tools()["question"], json!(false));
        for permission in [
            LocalAgentPermission::Manual,
            LocalAgentPermission::Auto,
            LocalAgentPermission::FullAccess,
        ] {
            let config = opencode_base_config(permission);
            assert_eq!(config["permission"]["question"], json!("deny"));
            // A top-level `tools` key is silently ignored by OpenCode, so it
            // must not be how we claim to block anything.
            assert!(config.get("tools").is_none());
        }
        assert_eq!(
            opencode_base_config(LocalAgentPermission::FullAccess)["permission"]["*"],
            json!("allow")
        );
        assert!(
            opencode_base_config(LocalAgentPermission::Manual)["permission"]
                .get("*")
                .is_none()
        );
    }

    #[test]
    fn provider_native_subagent_tool_is_blocked_in_every_permission_mode() {
        // OpenCode's `task` tool launches provider-native subagents. It must be
        // denied on both honoured levers: the per-prompt `tools` body (which
        // removes it from the model's tool list and becomes a session
        // permission) and the config `permission` backstop for agents that
        // inherit config rather than the prompt body.
        assert_eq!(blocked_tools()["task"], json!(false));
        for permission in [
            LocalAgentPermission::Manual,
            LocalAgentPermission::Auto,
            LocalAgentPermission::FullAccess,
        ] {
            let config = opencode_base_config(permission);
            assert_eq!(config["permission"]["task"], json!("deny"));
            // Denying `task` must not disturb the existing `question` denial.
            assert_eq!(config["permission"]["question"], json!("deny"));
        }
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
