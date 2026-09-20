use std::collections::HashMap;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use borg_provider::mcp::ExternalMcpServer;
use borg_provider::provider::ModelToolDefinition;
use futures::stream::{FuturesOrdered, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

const MAX_MCP_MESSAGE_BYTES: usize = 8 * 1024 * 1024;
const MCP_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const MCP_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
// A server that closed stdout is on its way out; wait only long enough to read
// its exit status rather than stalling the turn behind a wedged child.
const EXIT_DIAGNOSTIC_TIMEOUT: Duration = Duration::from_millis(500);
const STARTUP_FAILURE_COOLDOWN: Duration = Duration::from_secs(120);
const MAX_REMEMBERED_STARTUP_FAILURES: usize = 64;
const MAX_STDERR_TAIL_LINES: usize = 10;
const MAX_STDERR_TAIL_BYTES: usize = 2048;
const CURRENT_PROTOCOL_VERSION: &str = "2026-07-28";
const LEGACY_PROTOCOL_VERSION: &str = "2024-11-05";
const PROTOCOL_VERSION_META: &str = "io.modelcontextprotocol/protocolVersion";
const CLIENT_CAPABILITIES_META: &str = "io.modelcontextprotocol/clientCapabilities";
const CLIENT_INFO_META: &str = "io.modelcontextprotocol/clientInfo";

pub(crate) struct NativeMcpRuntime {
    clients: Vec<Mutex<NativeMcpClient>>,
    servers: Vec<ExternalMcpServer>,
    tools: HashMap<String, NativeMcpTool>,
    definitions: Vec<ModelToolDefinition>,
    pub(crate) startup_failures: Vec<McpStartupFailure>,
}

/// One server that has no tools this turn.
///
/// `notify` separates a fresh launch attempt from a failure already reported:
/// the model is told about the missing tools on every turn, while the user
/// sees a warning only when something actually changed (a first failure, a
/// changed configuration, or a real retry after the cooldown).
pub(crate) struct McpStartupFailure {
    pub(crate) server: String,
    pub(crate) error: String,
    pub(crate) notify: bool,
}

#[derive(Clone)]
struct NativeMcpTool {
    client_index: usize,
    wire_name: String,
}

impl NativeMcpRuntime {
    /// `session_id` scopes remembered startup failures: one session never
    /// suppresses another session's first warning about the same server.
    pub(crate) async fn start(
        session_id: uuid::Uuid,
        servers: Vec<ExternalMcpServer>,
    ) -> Result<Self> {
        let mut clients = Vec::with_capacity(servers.len());
        let mut configured_servers = Vec::with_capacity(servers.len());
        let mut tools = HashMap::new();
        let mut definitions = Vec::new();
        let mut startup_failures = Vec::new();
        // A server that just failed to start almost always fails again on the
        // next turn for the same reason (an editor that is not running, a
        // binary that was never built). Relaunching it every turn pays the
        // full launch cost repeatedly; the remembered failure is still
        // reported every turn, so nothing is hidden.
        let mut startups = FuturesOrdered::new();
        for server in servers {
            if let Some(remembered) = recent_startup_failure(session_id, &server) {
                tracing::debug!(
                    server = %server.name,
                    "external MCP server still in its startup-failure cooldown; reusing the recorded cause"
                );
                startup_failures.push(McpStartupFailure {
                    server: server.name,
                    error: remembered,
                    notify: false,
                });
                continue;
            }
            startups.push_back(async move {
                let started = async {
                    let mut client = NativeMcpClient::start(&server).await?;
                    let listed = client.list_tools().await?;
                    Ok::<_, anyhow::Error>((client, listed))
                }
                .await;
                (server, started)
            });
        }
        while let Some((server, started)) = startups.next().await {
            let (client, listed) = match started {
                Ok(started) => started,
                Err(error) => {
                    let error = truncate(&format!("{error:#}"), 2048).to_string();
                    tracing::warn!(server = %server.name, %error, "external MCP server unavailable; continuing without its tools");
                    let notify = remember_startup_failure(session_id, &server, &error);
                    startup_failures.push(McpStartupFailure {
                        server: server.name,
                        error,
                        notify,
                    });
                    continue;
                }
            };
            let client_index = clients.len();
            for listed_tool in listed {
                let full_name = external_tool_name(&server.name, &listed_tool.name);
                if !server.allowed_tools.is_empty()
                    && !server.allowed_tools.iter().any(|allowed| {
                        allowed == &full_name
                            || allowed == &listed_tool.name
                            || external_tool_name(&server.name, allowed) == full_name
                    })
                {
                    continue;
                }
                if tools
                    .insert(
                        full_name.clone(),
                        NativeMcpTool {
                            client_index,
                            wire_name: listed_tool.name.clone(),
                        },
                    )
                    .is_some()
                {
                    bail!("duplicate native MCP tool name `{full_name}`");
                }
                definitions.push(
                    ModelToolDefinition::new(
                        full_name,
                        listed_tool.description,
                        listed_tool.input_schema,
                    )
                    .map_err(anyhow::Error::msg)?,
                );
            }
            clear_startup_failure(session_id, &server);
            clients.push(Mutex::new(client));
            configured_servers.push(server);
        }
        Ok(Self {
            clients,
            servers: configured_servers,
            tools,
            definitions,
            startup_failures,
        })
    }

    pub(crate) fn definitions(&self) -> &[ModelToolDefinition] {
        &self.definitions
    }

    pub(crate) fn contains(&self, name: &str) -> bool {
        self.tools.contains_key(name) || self.tools.contains_key(&normalize_tool_name(name))
    }

    pub(crate) async fn call(
        &self,
        name: &str,
        arguments: Value,
        cancel: Option<&CancellationToken>,
    ) -> Result<Value> {
        let canonical_name = normalize_tool_name(name);
        let tool = self
            .tools
            .get(name)
            .or_else(|| self.tools.get(&canonical_name))
            .with_context(|| format!("unknown native MCP tool `{name}`"))?;
        let mut client = self.clients[tool.client_index].lock().await;
        let result = client.call_tool(&tool.wire_name, arguments, cancel).await;
        if cancel.is_some_and(CancellationToken::is_cancelled) {
            let server = &self.servers[tool.client_index];
            tracing::debug!(server = %server.name, "restarting cancelled native MCP client");
            *client = NativeMcpClient::start(server)
                .await
                .with_context(|| format!("restart native MCP server `{}`", server.name))?;
        }
        result
    }
}

struct NativeMcpClient {
    server_name: String,
    next_id: u64,
    mode: McpMode,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    // A server that dies during initialize explains itself on stderr and in
    // its exit status. Keep both so the failure is reported with its cause
    // instead of a bare "closed its stdout".
    stderr_tail: StderrTail,
    child: Child,
}

impl Drop for NativeMcpClient {
    fn drop(&mut self) {
        // Runtime extension grants can be replaced between turns. Tokio does
        // not kill a child merely because its Child handle is dropped, so make
        // the replacement boundary terminate the old MCP process as well.
        let _ = self.child.start_kill();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum McpMode {
    Probing,
    Modern(String),
    Legacy,
}

enum ModernProbe {
    Ready(String),
    Unsupported(Vec<String>),
    Fallback,
}

struct ListedTool {
    name: String,
    description: String,
    input_schema: Value,
}

impl NativeMcpClient {
    async fn start(server: &ExternalMcpServer) -> Result<Self> {
        let mut probe = Self::spawn(server).await?;
        match probe.probe_modern().await {
            ModernProbe::Ready(version) => {
                probe.mode = McpMode::Modern(version);
                Ok(probe)
            }
            ModernProbe::Unsupported(supported) => {
                let version = select_modern_version(&supported).with_context(|| {
                    format!(
                        "MCP server `{}` supports no mutually compatible modern protocol version",
                        server.name
                    )
                })?;
                match probe.probe_modern_version(&version).await {
                    ModernProbe::Ready(version) => {
                        probe.mode = McpMode::Modern(version);
                        Ok(probe)
                    }
                    ModernProbe::Unsupported(_) | ModernProbe::Fallback => bail!(
                        "MCP server `{}` rejected the negotiated protocol version `{version}`",
                        server.name
                    ),
                }
            }
            ModernProbe::Fallback => {
                drop(probe);
                Self::start_legacy(server).await
            }
        }
    }

    async fn spawn(server: &ExternalMcpServer) -> Result<Self> {
        let mut command = Command::new(&server.command);
        crate::process_environment::configure_host_child_environment(&mut command);
        command
            .args(&server.args)
            .envs(&server.env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().with_context(|| {
            format!(
                "failed to start native MCP server `{}` with executable `{}`",
                server.name, server.command
            )
        })?;
        let stdin = child
            .stdin
            .take()
            .with_context(|| format!("MCP server `{}` has no stdin", server.name))?;
        let stdout = child
            .stdout
            .take()
            .with_context(|| format!("MCP server `{}` has no stdout", server.name))?;
        let stderr_tail = StderrTail::default();
        if let Some(stderr) = child.stderr.take() {
            let name = server.name.clone();
            let tail = stderr_tail.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let line =
                        crate::secret_scrub::scrub_secrets(truncate(&line, 4096)).into_owned();
                    tracing::debug!(
                        server = %name,
                        message = %line,
                        "native MCP server stderr"
                    );
                    tail.push(line);
                }
            });
        }
        let client = Self {
            server_name: server.name.clone(),
            next_id: 1,
            mode: McpMode::Probing,
            stdin,
            stdout: BufReader::new(stdout),
            stderr_tail,
            child,
        };
        Ok(client)
    }

    async fn start_legacy(server: &ExternalMcpServer) -> Result<Self> {
        let mut client = Self::spawn(server).await?;
        client.mode = McpMode::Legacy;
        client
            .request(
                "initialize",
                json!({
                    "protocolVersion": LEGACY_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {
                        "name": "borg-native-harness",
                        "version": env!("CARGO_PKG_VERSION"),
                    }
                }),
                None,
            )
            .await
            .with_context(|| format!("failed to initialize MCP server `{}`", server.name))?;
        client
            .notify("notifications/initialized", json!({}))
            .await?;
        Ok(client)
    }

    async fn probe_modern(&mut self) -> ModernProbe {
        self.probe_modern_version(CURRENT_PROTOCOL_VERSION).await
    }

    async fn probe_modern_version(&mut self, version: &str) -> ModernProbe {
        let id = Value::from(self.next_id);
        self.next_id = self.next_id.saturating_add(1);
        if self
            .write(&json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "server/discover",
                "params": modern_params(json!({}), version),
            }))
            .await
            .is_err()
        {
            return ModernProbe::Fallback;
        }
        let response = match tokio::time::timeout(MCP_PROBE_TIMEOUT, self.read_response(&id)).await
        {
            Ok(Ok(response)) => response,
            _ => return ModernProbe::Fallback,
        };
        if let Some(error) = response.get("error") {
            if error.get("code").and_then(Value::as_i64) == Some(-32022) {
                let supported = error
                    .get("data")
                    .and_then(|data| data.get("supported"))
                    .and_then(Value::as_array)
                    .map(|versions| {
                        versions
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_string)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                return ModernProbe::Unsupported(supported);
            }
            return ModernProbe::Fallback;
        }
        let Some(versions) = response
            .get("result")
            .and_then(|result| result.get("supportedVersions"))
            .and_then(Value::as_array)
        else {
            return ModernProbe::Fallback;
        };
        let supported = versions
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect::<Vec<_>>();
        match select_modern_version(&supported) {
            Some(version) => ModernProbe::Ready(version),
            None => ModernProbe::Unsupported(supported),
        }
    }

    async fn list_tools(&mut self) -> Result<Vec<ListedTool>> {
        let result = self.request("tools/list", json!({}), None).await?;
        result
            .get("tools")
            .and_then(Value::as_array)
            .context("MCP tools/list response is missing tools")?
            .iter()
            .map(|tool| {
                let name = tool
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|name| !name.trim().is_empty())
                    .context("MCP tool is missing a nonempty name")?;
                Ok(ListedTool {
                    name: name.to_string(),
                    description: tool
                        .get("description")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    input_schema: tool
                        .get("inputSchema")
                        .cloned()
                        .unwrap_or_else(|| json!({ "type": "object", "properties": {} })),
                })
            })
            .collect()
    }

    async fn call_tool(
        &mut self,
        name: &str,
        arguments: Value,
        cancel: Option<&CancellationToken>,
    ) -> Result<Value> {
        self.request(
            "tools/call",
            json!({
                "name": name,
                "arguments": arguments,
            }),
            cancel,
        )
        .await
    }

    async fn request(
        &mut self,
        method: &str,
        params: Value,
        cancel: Option<&CancellationToken>,
    ) -> Result<Value> {
        let id = Value::from(self.next_id);
        self.next_id = self.next_id.saturating_add(1);
        let request = self.request_inner(id.clone(), method, params);
        if let Some(cancel) = cancel {
            tokio::select! {
                _ = cancel.cancelled() => {
                    let _ = tokio::time::timeout(
                        Duration::from_secs(1),
                        self.notify(
                            "notifications/cancelled",
                            json!({
                                "requestId": id,
                                "reason": "cancelled by Borg",
                            }),
                        ),
                    )
                    .await;
                    self.terminate().await;
                    bail!("native MCP request cancelled");
                }
                result = tokio::time::timeout(MCP_REQUEST_TIMEOUT, request) => {
                    result.with_context(|| {
                        format!(
                            "MCP server `{}` timed out handling `{method}`",
                            self.server_name
                        )
                    })?
                }
            }
        } else {
            tokio::time::timeout(MCP_REQUEST_TIMEOUT, request)
                .await
                .with_context(|| {
                    format!(
                        "MCP server `{}` timed out handling `{method}`",
                        self.server_name
                    )
                })?
        }
    }

    async fn request_inner(&mut self, id: Value, method: &str, params: Value) -> Result<Value> {
        let params = match &self.mode {
            McpMode::Modern(version) => modern_params(params, version),
            McpMode::Probing | McpMode::Legacy => params,
        };
        self.write(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }))
        .await?;
        let message = self.read_response(&id).await?;
        if let Some(error) = message.get("error") {
            bail!(
                "MCP server `{}` returned an error for {method}: {}",
                self.server_name,
                truncate(&error.to_string(), 4096)
            );
        }
        message
            .get("result")
            .cloned()
            .context("MCP response is missing result")
    }

    async fn terminate(&mut self) {
        let _ = self.child.start_kill();
        let _ = tokio::time::timeout(Duration::from_secs(1), self.child.wait()).await;
    }

    async fn read_response(&mut self, id: &Value) -> Result<Value> {
        loop {
            let message = self.read_message().await?;
            if message.get("id") != Some(id) {
                if message.get("method").and_then(Value::as_str) == Some("ping")
                    && let Some(server_request_id) = message.get("id").cloned()
                {
                    self.write(&json!({
                        "jsonrpc": "2.0",
                        "id": server_request_id,
                        "result": {},
                    }))
                    .await?;
                }
                continue;
            }
            return Ok(message);
        }
    }

    async fn notify(&mut self, method: &str, params: Value) -> Result<()> {
        self.write(&json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }))
        .await
    }

    async fn write(&mut self, message: &Value) -> Result<()> {
        let mut bytes = serde_json::to_vec(message)?;
        if bytes.len() > MAX_MCP_MESSAGE_BYTES {
            bail!("outgoing MCP message exceeded {MAX_MCP_MESSAGE_BYTES} bytes");
        }
        bytes.push(b'\n');
        self.stdin
            .write_all(&bytes)
            .await
            .with_context(|| format!("failed writing to MCP server `{}`", self.server_name))?;
        self.stdin.flush().await?;
        Ok(())
    }

    /// Why the server stopped talking: its exit status when it has already
    /// finished, plus the tail of its own stderr. Optional plugins that are
    /// simply not running explain themselves here instead of surfacing as an
    /// unexplained transport failure.
    async fn exit_diagnostics(&mut self) -> String {
        let status = match self.child.try_wait() {
            Ok(Some(status)) => Some(status),
            // A server can close stdout a moment before the process is reaped.
            Ok(None) => tokio::time::timeout(EXIT_DIAGNOSTIC_TIMEOUT, self.child.wait())
                .await
                .ok()
                .and_then(Result::ok),
            Err(_) => None,
        };
        let mut diagnostics = String::new();
        match status {
            Some(status) => {
                diagnostics.push_str(&format!(" after exiting with {status}"));
            }
            None => diagnostics.push_str(" while still running"),
        }
        let stderr = self.stderr_tail.render();
        if !stderr.is_empty() {
            diagnostics.push_str(&format!("; its stderr said: {stderr}"));
        }
        diagnostics
    }

    async fn read_message(&mut self) -> Result<Value> {
        let mut line = String::new();
        let bytes =
            self.stdout.read_line(&mut line).await.with_context(|| {
                format!("failed reading from MCP server `{}`", self.server_name)
            })?;
        if bytes == 0 {
            let diagnostics = self.exit_diagnostics().await;
            bail!(
                "MCP server `{}` closed its stdout{diagnostics}",
                self.server_name
            );
        }
        if bytes > MAX_MCP_MESSAGE_BYTES {
            bail!(
                "MCP server `{}` emitted a message larger than {MAX_MCP_MESSAGE_BYTES} bytes",
                self.server_name
            );
        }
        serde_json::from_str(&line)
            .with_context(|| format!("MCP server `{}` emitted invalid JSON", self.server_name))
    }
}

fn modern_params(params: Value, version: &str) -> Value {
    let mut object = params.as_object().cloned().unwrap_or_default();
    object.insert(
        "_meta".to_string(),
        json!({
            PROTOCOL_VERSION_META: version,
            CLIENT_CAPABILITIES_META: {},
            CLIENT_INFO_META: {
                "name": "borg-native-harness",
                "version": env!("CARGO_PKG_VERSION"),
            },
        }),
    );
    Value::Object(object)
}

fn select_modern_version(supported: &[String]) -> Option<String> {
    supported
        .iter()
        .find(|version| version.as_str() == CURRENT_PROTOCOL_VERSION)
        .cloned()
}

fn external_tool_name(server_name: &str, wire_name: &str) -> String {
    let wire_name = normalize_tool_name(wire_name);
    if wire_name.starts_with("mcp__") {
        return wire_name;
    }
    let server = server_name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("mcp__{server}__{wire_name}")
}

fn normalize_tool_name(name: &str) -> String {
    name.chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '_' | '-') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

/// Remembered startup failures, keyed by a hash of the launch specification so
/// no command, argument, or environment text is retained here. A configuration
/// change produces a different key and retries immediately; an unchanged
/// configuration retries once the cooldown elapses, so a server that becomes
/// available again (its editor starts, its binary is built) recovers on its
/// own.
static STARTUP_FAILURES: std::sync::LazyLock<
    std::sync::Mutex<HashMap<u64, (std::time::Instant, String)>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

fn startup_key(session_id: uuid::Uuid, server: &ExternalMcpServer) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    session_id.hash(&mut hasher);
    server.name.hash(&mut hasher);
    server.command.hash(&mut hasher);
    server.args.hash(&mut hasher);
    for (key, value) in &server.env {
        key.hash(&mut hasher);
        value.hash(&mut hasher);
    }
    hasher.finish()
}

/// The recorded cause when this exact server failed recently, annotated so the
/// report says it is a remembered failure rather than a fresh launch attempt.
fn recent_startup_failure(session_id: uuid::Uuid, server: &ExternalMcpServer) -> Option<String> {
    let failures = STARTUP_FAILURES.lock().ok()?;
    let key = startup_key(session_id, server);
    let (recorded, error) = failures.get(&key)?;
    let age = recorded.elapsed();
    let cooldown = STARTUP_FAILURE_COOLDOWN;
    if age >= cooldown {
        return None;
    }
    let retry_in = (cooldown - age).as_secs();
    Some(format!(
        "{error} (recorded {}s ago; not relaunched again until {retry_in}s from now)",
        age.as_secs()
    ))
}

fn remember_startup_failure(
    session_id: uuid::Uuid,
    server: &ExternalMcpServer,
    error: &str,
) -> bool {
    let Ok(mut failures) = STARTUP_FAILURES.lock() else {
        return true;
    };
    let key = startup_key(session_id, server);
    let notify = failures
        .remove(&key)
        .is_none_or(|(_, previous)| previous != error);
    // Keep the previous cause across cooldowns, but bound configuration history.
    while failures.len() >= MAX_REMEMBERED_STARTUP_FAILURES {
        let Some(oldest) = failures
            .iter()
            .min_by_key(|(_, (recorded, _))| *recorded)
            .map(|(key, _)| *key)
        else {
            break;
        };
        failures.remove(&oldest);
    }
    failures.insert(key, (std::time::Instant::now(), error.to_string()));
    notify
}

fn clear_startup_failure(session_id: uuid::Uuid, server: &ExternalMcpServer) {
    if let Ok(mut failures) = STARTUP_FAILURES.lock() {
        failures.remove(&startup_key(session_id, server));
    }
}

/// A bounded, shared view of a child's stderr. The reader task owns the pipe;
/// the client reads the tail only when reporting a failure.
#[derive(Clone, Default)]
struct StderrTail(std::sync::Arc<std::sync::Mutex<std::collections::VecDeque<String>>>);

impl StderrTail {
    fn push(&self, line: String) {
        let Ok(mut lines) = self.0.lock() else {
            return;
        };
        // Server stderr can echo tokens from its own environment, and this
        // tail is reported to the model and the UI. Scrub before it is stored.
        lines.push_back(crate::secret_scrub::scrub_secrets(&line).into_owned());
        while lines.len() > MAX_STDERR_TAIL_LINES {
            lines.pop_front();
        }
    }

    fn render(&self) -> String {
        let Ok(lines) = self.0.lock() else {
            return String::new();
        };
        let joined = lines
            .iter()
            .map(|line| line.trim())
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>()
            .join("; ");
        truncate(&joined, MAX_STDERR_TAIL_BYTES).to_string()
    }
}

fn truncate(value: &str, max: usize) -> &str {
    if value.len() <= max {
        return value;
    }
    let mut boundary = max;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    &value[..boundary]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::Instant;

    #[test]
    fn external_names_are_stable_and_namespaced() {
        assert_eq!(
            external_tool_name("google-drive", "search_files"),
            "mcp__google_drive__search_files"
        );
        assert_eq!(
            external_tool_name("borg", "mcp__borg__read_document"),
            "mcp__borg__read_document"
        );
        assert_eq!(
            external_tool_name("surf-lab", "map.generate"),
            "mcp__surf_lab__map_generate"
        );
    }

    #[tokio::test]
    async fn stdio_client_initializes_filters_and_calls_namespaced_tools() {
        let script = r#"
read _initialize
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05","capabilities":{},"serverInfo":{"name":"fake","version":"1"}}}'
read _initialized
read _list
printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"map.generate","description":"Generate a map","inputSchema":{"type":"object"}},{"name":"hidden","inputSchema":{"type":"object"}}]}}'
read _call
printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"ok"}]}}'
"#;
        let runtime = NativeMcpRuntime::start(
            uuid::Uuid::new_v4(),
            vec![
                ExternalMcpServer {
                    name: "unavailable".to_string(),
                    command: "sh".to_string(),
                    args: vec!["-c".to_string(), "exit 127".to_string()],
                    ..Default::default()
                },
                ExternalMcpServer {
                    name: "fake-server".to_string(),
                    command: "sh".to_string(),
                    args: vec!["-c".to_string(), script.to_string()],
                    env: BTreeMap::new(),
                    allowed_tools: vec!["map.generate".to_string()],
                },
            ],
        )
        .await
        .unwrap();
        assert_eq!(runtime.startup_failures.len(), 1);
        assert_eq!(runtime.startup_failures[0].server, "unavailable");
        assert!(runtime.contains("mcp__fake_server__map_generate"));
        assert!(!runtime.contains("mcp__fake_server__hidden"));
        assert_eq!(runtime.definitions().len(), 1);
        let result = runtime
            .call(
                "mcp__fake_server__map.generate",
                json!({ "value": "hello" }),
                None,
            )
            .await
            .unwrap();
        assert_eq!(result["content"][0]["text"], "ok");
    }

    #[tokio::test]
    async fn startup_failure_reports_the_server_exit_status_and_stderr() {
        // A launcher that refuses to start (no built binary, editor not
        // running) explains itself on stderr. That reason has to survive into
        // the reported failure, otherwise every turn shows only an opaque
        // closed-stdout transport error.
        let runtime = NativeMcpRuntime::start(
            uuid::Uuid::new_v4(),
            vec![ExternalMcpServer {
                name: "surf-lab__lab".to_string(),
                command: "sh".to_string(),
                args: vec![
                    "-c".to_string(),
                    "echo 'no fresh surf_lab executable found' >&2; exit 127".to_string(),
                ],
                ..Default::default()
            }],
        )
        .await
        .unwrap();

        assert_eq!(runtime.definitions().len(), 0);
        let failure = runtime.startup_failures.first().expect("startup failure");
        let (name, error) = (&failure.server, &failure.error);
        assert!(failure.notify, "a fresh failure warns the user");
        assert_eq!(name, "surf-lab__lab");
        assert!(
            error.contains("no fresh surf_lab executable found"),
            "failure should quote the server's own stderr: {error}"
        );
        assert!(
            error.contains("127"),
            "failure should report the exit status: {error}"
        );
    }

    /// Counts how many times the launcher actually ran, so the memo is tested
    /// by observed launches rather than by its own bookkeeping.
    fn launcher(directory: &std::path::Path, marker: &str) -> Vec<String> {
        let log = directory.join(marker);
        vec![
            "-c".to_string(),
            format!(
                "echo launched >> {}; echo 'no fresh binary' >&2; exit 127",
                log.display()
            ),
        ]
    }

    fn launch_count(directory: &std::path::Path, marker: &str) -> usize {
        std::fs::read_to_string(directory.join(marker))
            .map(|log| log.lines().count())
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn remembered_failure_skips_relaunch_until_config_change_or_cooldown_expiry() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let server = |args: Vec<String>| ExternalMcpServer {
            name: "cooldown-server".to_string(),
            command: "sh".to_string(),
            args,
            ..Default::default()
        };
        let original = launcher(directory.path(), "launches");
        // One session: the memo is scoped per session, so all four attempts
        // below must share this id to exercise it.
        let session = uuid::Uuid::new_v4();

        // One attempt can spawn more than once (the modern probe falls back to
        // the legacy handshake), so the memo is judged by launch deltas.
        let first = NativeMcpRuntime::start(session, vec![server(original.clone())])
            .await
            .unwrap();
        let after_first = launch_count(directory.path(), "launches");
        assert!(after_first >= 1, "the first attempt launches the server");
        assert!(first.startup_failures[0].notify, "first failure warns");

        // Same configuration inside the cooldown: reported, but not relaunched
        // and not re-warned.
        let second = NativeMcpRuntime::start(session, vec![server(original.clone())])
            .await
            .unwrap();
        assert_eq!(
            launch_count(directory.path(), "launches"),
            after_first,
            "a remembered failure must not pay the launch cost again"
        );
        assert_eq!(second.startup_failures.len(), 1, "still reported");
        assert!(
            !second.startup_failures[0].notify,
            "a cached failure must not warn the user again"
        );
        assert!(
            second.startup_failures[0].error.contains("no fresh binary"),
            "the cached report keeps the real cause: {}",
            second.startup_failures[0].error
        );

        // A configuration change re-keys the memo and retries immediately.
        let mut changed = original.clone();
        changed[1].push_str(" # changed");
        let third = NativeMcpRuntime::start(session, vec![server(changed)])
            .await
            .unwrap();
        let after_change = launch_count(directory.path(), "launches");
        assert!(
            after_change > after_first,
            "a changed configuration retries at once"
        );
        assert!(third.startup_failures[0].notify, "a retry warns again");

        // An expired cooldown retries the unchanged configuration.
        STARTUP_FAILURES
            .lock()
            .unwrap()
            .get_mut(&startup_key(session, &server(original.clone())))
            .expect("the original failure remains cached")
            .0 = std::time::Instant::now() - STARTUP_FAILURE_COOLDOWN;
        let fourth = NativeMcpRuntime::start(session, vec![server(original.clone())])
            .await
            .unwrap();
        assert!(
            launch_count(directory.path(), "launches") > after_change,
            "an expired cooldown retries"
        );
        assert!(
            !fourth.startup_failures[0].notify,
            "an unchanged cause must not warn again"
        );
        let server = server(original);
        assert!(remember_startup_failure(
            session,
            &server,
            "different cause"
        ));
        assert!(!remember_startup_failure(
            session,
            &server,
            "different cause"
        ));
        clear_startup_failure(session, &server);
        assert!(remember_startup_failure(
            session,
            &server,
            "different cause"
        ));
    }

    #[tokio::test]
    async fn reported_stderr_is_scrubbed_of_secrets() {
        let runtime = NativeMcpRuntime::start(
            uuid::Uuid::new_v4(),
            vec![ExternalMcpServer {
                name: "leaky-server".to_string(),
                command: "sh".to_string(),
                args: vec![
                    "-c".to_string(),
                    "echo 'auth failed for sk-ant-abcdefghijklmnopqrstuvwxyz0123' >&2; exit 1"
                        .to_string(),
                ],
                ..Default::default()
            }],
        )
        .await
        .unwrap();

        let error = &runtime.startup_failures[0].error;
        assert!(
            !error.contains("sk-ant-abcdefghijklmnopqrstuvwxyz0123"),
            "the reported stderr must not carry a credential: {error}"
        );
        assert!(
            error.contains("[redacted:anthropic-key]"),
            "the secret is redacted in place: {error}"
        );
    }

    #[tokio::test]
    async fn stdio_client_prefers_stateless_discovery_and_per_request_metadata() {
        let script = r#"
read _discover
case "$_discover" in
  *server/discover*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"resultType":"complete","supportedVersions":["2026-07-28"],"capabilities":{"tools":{"listChanged":false}}}}' ;;
esac
read _list
case "$_list" in
  *tools/list*2026-07-28*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"resultType":"complete","tools":[{"name":"echo","description":"Echo","inputSchema":{"type":"object"}}]}}' ;;
  *) exit 3 ;;
esac
read _call
case "$_call" in
  *tools/call*2026-07-28*) printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"resultType":"complete","content":[{"type":"text","text":"ok"}]}}' ;;
  *) exit 4 ;;
esac
"#;
        let runtime = NativeMcpRuntime::start(
            uuid::Uuid::new_v4(),
            vec![ExternalMcpServer {
                name: "modern-server".to_string(),
                command: "sh".to_string(),
                args: vec!["-c".to_string(), script.to_string()],
                env: BTreeMap::new(),
                allowed_tools: vec![],
            }],
        )
        .await
        .unwrap();
        assert!(runtime.contains("mcp__modern_server__echo"));
        let result = runtime
            .call(
                "mcp__modern_server__echo",
                json!({ "value": "hello" }),
                None,
            )
            .await
            .unwrap();
        assert_eq!(result["content"][0]["text"], "ok");
    }

    #[tokio::test]
    async fn cancelled_tool_call_restarts_client_before_the_next_call() {
        let marker_root = tempfile::tempdir().unwrap();
        let marker = marker_root.path().join("first-call");
        let script = format!(
            r#"
read request
case "$request" in
  *server/discover*)
    id=$(printf '%s' "$request" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
    printf '%s\n' '{{"jsonrpc":"2.0","id":'$id',"result":{{"resultType":"complete","supportedVersions":["2026-07-28"]}}}}'
    ;;
  *) exit 2 ;;
esac
read request
if printf '%s' "$request" | grep -q 'tools/list'; then
  id=$(printf '%s' "$request" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  printf '%s\n' '{{"jsonrpc":"2.0","id":'$id',"result":{{"tools":[{{"name":"wait","inputSchema":{{"type":"object"}}}}]}}}}'
  read request
fi
case "$request" in
  *tools/call*)
    id=$(printf '%s' "$request" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
    if [ ! -e "{}" ]; then
      touch "{}"
      sleep 30
    else
      printf '%s\n' '{{"jsonrpc":"2.0","id":'$id',"result":{{"content":[{{"type":"text","text":"restarted"}}]}}}}'
    fi
    ;;
  *) exit 3 ;;
esac
"#,
            marker.display(),
            marker.display()
        );
        let runtime = NativeMcpRuntime::start(
            uuid::Uuid::new_v4(),
            vec![ExternalMcpServer {
                name: "restart-server".to_string(),
                command: "sh".to_string(),
                args: vec!["-c".to_string(), script],
                env: BTreeMap::new(),
                allowed_tools: Vec::new(),
            }],
        )
        .await
        .unwrap();
        let cancel = CancellationToken::new();
        let call = runtime.call("mcp__restart_server__wait", json!({}), Some(&cancel));
        tokio::pin!(call);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !marker.exists() {
                tokio::select! {
                    result = &mut call => panic!("MCP call ended before it could be cancelled: {result:?}"),
                    _ = tokio::time::sleep(Duration::from_millis(5)) => {}
                }
            }
        })
        .await
        .expect("MCP server should receive the cancellable call");
        cancel.cancel();
        let error = tokio::time::timeout(Duration::from_secs(2), call)
            .await
            .expect("cancelled MCP call should finish promptly")
            .unwrap_err();
        assert!(error.to_string().contains("cancelled"));
        assert!(
            marker.exists(),
            "first MCP request should have been entered"
        );

        let result = runtime
            .call("mcp__restart_server__wait", json!({}), None)
            .await
            .unwrap();
        assert_eq!(result["content"][0]["text"], "restarted");
    }

    #[tokio::test]
    async fn recognized_modern_probe_failure_does_not_downgrade_to_legacy() {
        let script = r#"
read first
case "$first" in
  *server/discover*)
    printf '%s\n' '{"jsonrpc":"2.0","id":1,"error":{"code":-32022,"message":"unsupported","data":{"supported":["2025-11-25"],"requested":"2026-07-28"}}}'
    ;;
  *initialize*)
    printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{},"serverInfo":{"name":"legacy","version":"1"}}}'
    read _initialized
    read _list
    printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}'
    ;;
  *)
    exit 3
    ;;
esac
"#;
        let error = match NativeMcpClient::start(&ExternalMcpServer {
            name: "modern-only-server".to_string(),
            command: "sh".to_string(),
            args: vec!["-c".to_string(), script.to_string()],
            env: BTreeMap::new(),
            allowed_tools: vec![],
        })
        .await
        {
            Ok(_) => panic!("recognized modern negotiation error was downgraded to legacy"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("supports no mutually compatible modern protocol version")
        );
    }

    #[tokio::test]
    #[ignore = "explicit multi-server native MCP startup performance gate"]
    async fn multi_server_startup_profile() {
        const SERVER_COUNT: usize = 4;
        let script = r#"
sleep 0.15
read _discover
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"resultType":"complete","supportedVersions":["2026-07-28"]}}'
read _list
printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"resultType":"complete","tools":[{"name":"echo","inputSchema":{"type":"object"}}]}}'
"#;
        let servers = (0..SERVER_COUNT)
            .map(|index| ExternalMcpServer {
                name: format!("profile-{index}"),
                command: "sh".to_string(),
                args: vec!["-c".to_string(), script.to_string()],
                env: BTreeMap::new(),
                allowed_tools: Vec::new(),
            })
            .collect::<Vec<_>>();

        let started = Instant::now();
        let runtime = NativeMcpRuntime::start(uuid::Uuid::new_v4(), servers)
            .await
            .unwrap();
        let elapsed = started.elapsed();
        assert_eq!(runtime.definitions().len(), SERVER_COUNT);
        assert_eq!(
            runtime
                .definitions()
                .iter()
                .map(|definition| definition.name.as_str())
                .collect::<Vec<_>>(),
            [
                "mcp__profile_0__echo",
                "mcp__profile_1__echo",
                "mcp__profile_2__echo",
                "mcp__profile_3__echo",
            ]
        );
        eprintln!("four-server native MCP startup: {elapsed:?}");
        assert!(
            elapsed < Duration::from_millis(400),
            "independent MCP servers did not initialize concurrently: {elapsed:?}"
        );
    }

    #[test]
    fn modern_params_are_inline_and_stateless() {
        let params = modern_params(json!({ "name": "echo" }), CURRENT_PROTOCOL_VERSION);
        assert_eq!(
            params["_meta"][PROTOCOL_VERSION_META],
            CURRENT_PROTOCOL_VERSION
        );
        assert!(params["_meta"][CLIENT_CAPABILITIES_META].is_object());
        assert!(params["_meta"][CLIENT_INFO_META].is_object());
    }
}
