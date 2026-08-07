use std::collections::HashMap;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use borg_provider::mcp::ExternalMcpServer;
use borg_provider::provider::ModelToolDefinition;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

const MAX_MCP_MESSAGE_BYTES: usize = 8 * 1024 * 1024;
const MCP_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const MCP_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
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
}

#[derive(Clone)]
struct NativeMcpTool {
    client_index: usize,
    wire_name: String,
}

impl NativeMcpRuntime {
    pub(crate) async fn start(servers: Vec<ExternalMcpServer>) -> Result<Self> {
        let mut clients = Vec::with_capacity(servers.len());
        let mut configured_servers = Vec::with_capacity(servers.len());
        let mut tools = HashMap::new();
        let mut definitions = Vec::new();
        for server in servers {
            let mut client = NativeMcpClient::start(&server).await?;
            let listed = client.list_tools().await?;
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
            clients.push(Mutex::new(client));
            configured_servers.push(server);
        }
        Ok(Self {
            clients,
            servers: configured_servers,
            tools,
            definitions,
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
    _child: Child,
}

impl Drop for NativeMcpClient {
    fn drop(&mut self) {
        // Runtime extension grants can be replaced between turns. Tokio does
        // not kill a child merely because its Child handle is dropped, so make
        // the replacement boundary terminate the old MCP process as well.
        let _ = self._child.start_kill();
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
        if let Some(stderr) = child.stderr.take() {
            let name = server.name.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::debug!(
                        server = %name,
                        message = %truncate(&line, 4096),
                        "native MCP server stderr"
                    );
                }
            });
        }
        let client = Self {
            server_name: server.name.clone(),
            next_id: 1,
            mode: McpMode::Probing,
            stdin,
            stdout: BufReader::new(stdout),
            _child: child,
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
        let _ = self._child.start_kill();
        let _ = tokio::time::timeout(Duration::from_secs(1), self._child.wait()).await;
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

    async fn read_message(&mut self) -> Result<Value> {
        let mut line = String::new();
        let bytes =
            self.stdout.read_line(&mut line).await.with_context(|| {
                format!("failed reading from MCP server `{}`", self.server_name)
            })?;
        if bytes == 0 {
            bail!("MCP server `{}` closed its stdout", self.server_name);
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
        let runtime = NativeMcpRuntime::start(vec![ExternalMcpServer {
            name: "fake-server".to_string(),
            command: "sh".to_string(),
            args: vec!["-c".to_string(), script.to_string()],
            env: BTreeMap::new(),
            allowed_tools: vec!["map.generate".to_string()],
        }])
        .await
        .unwrap();
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
        let runtime = NativeMcpRuntime::start(vec![ExternalMcpServer {
            name: "modern-server".to_string(),
            command: "sh".to_string(),
            args: vec!["-c".to_string(), script.to_string()],
            env: BTreeMap::new(),
            allowed_tools: vec![],
        }])
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
        let runtime = NativeMcpRuntime::start(vec![ExternalMcpServer {
            name: "restart-server".to_string(),
            command: "sh".to_string(),
            args: vec!["-c".to_string(), script],
            env: BTreeMap::new(),
            allowed_tools: Vec::new(),
        }])
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
        let error = match NativeMcpRuntime::start(vec![ExternalMcpServer {
            name: "modern-only-server".to_string(),
            command: "sh".to_string(),
            args: vec!["-c".to_string(), script.to_string()],
            env: BTreeMap::new(),
            allowed_tools: vec![],
        }])
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
