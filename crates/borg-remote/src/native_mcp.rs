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

const MAX_MCP_MESSAGE_BYTES: usize = 8 * 1024 * 1024;
const MCP_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

pub(crate) struct NativeMcpRuntime {
    clients: Vec<Mutex<NativeMcpClient>>,
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
        let mut tools = HashMap::new();
        let mut definitions = Vec::new();
        for server in servers {
            let mut client = NativeMcpClient::start(&server).await?;
            let listed = client.list_tools().await?;
            let client_index = clients.len();
            for listed_tool in listed {
                let full_name = external_tool_name(&server.name, &listed_tool.name);
                if !server.allowed_tools.is_empty()
                    && !server
                        .allowed_tools
                        .iter()
                        .any(|allowed| allowed == &full_name || allowed == &listed_tool.name)
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
        }
        Ok(Self {
            clients,
            tools,
            definitions,
        })
    }

    pub(crate) fn definitions(&self) -> &[ModelToolDefinition] {
        &self.definitions
    }

    pub(crate) fn contains(&self, name: &str) -> bool {
        self.tools.contains_key(name)
    }

    pub(crate) async fn call(&self, name: &str, arguments: Value) -> Result<Value> {
        let tool = self
            .tools
            .get(name)
            .with_context(|| format!("unknown native MCP tool `{name}`"))?;
        self.clients[tool.client_index]
            .lock()
            .await
            .call_tool(&tool.wire_name, arguments)
            .await
    }
}

struct NativeMcpClient {
    server_name: String,
    next_id: u64,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    _child: Child,
}

struct ListedTool {
    name: String,
    description: String,
    input_schema: Value,
}

impl NativeMcpClient {
    async fn start(server: &ExternalMcpServer) -> Result<Self> {
        let mut command = Command::new(&server.command);
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
        let mut client = Self {
            server_name: server.name.clone(),
            next_id: 1,
            stdin,
            stdout: BufReader::new(stdout),
            _child: child,
        };
        client
            .request(
                "initialize",
                json!({
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {
                        "name": "borg-native-harness",
                        "version": env!("CARGO_PKG_VERSION"),
                    }
                }),
            )
            .await
            .with_context(|| format!("failed to initialize MCP server `{}`", server.name))?;
        client
            .notify("notifications/initialized", json!({}))
            .await?;
        Ok(client)
    }

    async fn list_tools(&mut self) -> Result<Vec<ListedTool>> {
        let result = self.request("tools/list", json!({})).await?;
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

    async fn call_tool(&mut self, name: &str, arguments: Value) -> Result<Value> {
        self.request(
            "tools/call",
            json!({
                "name": name,
                "arguments": arguments,
            }),
        )
        .await
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        tokio::time::timeout(MCP_REQUEST_TIMEOUT, self.request_inner(method, params))
            .await
            .with_context(|| {
                format!(
                    "MCP server `{}` timed out handling `{method}`",
                    self.server_name
                )
            })?
    }

    async fn request_inner(&mut self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        self.write(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }))
        .await?;
        loop {
            let message = self.read_message().await?;
            if message.get("id").and_then(Value::as_u64) != Some(id) {
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
            if let Some(error) = message.get("error") {
                bail!(
                    "MCP server `{}` returned an error for {method}: {}",
                    self.server_name,
                    truncate(&error.to_string(), 4096)
                );
            }
            return message
                .get("result")
                .cloned()
                .context("MCP response is missing result");
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

fn external_tool_name(server_name: &str, wire_name: &str) -> String {
    if wire_name.starts_with("mcp__") {
        return wire_name.to_string();
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
    }

    #[tokio::test]
    async fn stdio_client_initializes_filters_and_calls_namespaced_tools() {
        let script = r#"
read _initialize
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05","capabilities":{},"serverInfo":{"name":"fake","version":"1"}}}'
read _initialized
read _list
printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"echo","description":"Echo","inputSchema":{"type":"object"}},{"name":"hidden","inputSchema":{"type":"object"}}]}}'
read _call
printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"ok"}]}}'
"#;
        let runtime = NativeMcpRuntime::start(vec![ExternalMcpServer {
            name: "fake-server".to_string(),
            command: "sh".to_string(),
            args: vec!["-c".to_string(), script.to_string()],
            env: BTreeMap::new(),
            allowed_tools: vec!["echo".to_string()],
        }])
        .await
        .unwrap();
        assert!(runtime.contains("mcp__fake_server__echo"));
        assert!(!runtime.contains("mcp__fake_server__hidden"));
        assert_eq!(runtime.definitions().len(), 1);
        let result = runtime
            .call("mcp__fake_server__echo", json!({ "value": "hello" }))
            .await
            .unwrap();
        assert_eq!(result["content"][0]["text"], "ok");
    }
}
