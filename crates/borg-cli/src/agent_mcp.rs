use std::collections::HashMap;
#[cfg(all(unix, test))]
use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
#[cfg(not(unix))]
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const CURRENT_PROTOCOL_VERSION: &str = "2026-07-28";
const LEGACY_PROTOCOL_VERSION: &str = "2024-11-05";
const PROTOCOL_VERSION_META: &str = "io.modelcontextprotocol/protocolVersion";
const CLIENT_CAPABILITIES_META: &str = "io.modelcontextprotocol/clientCapabilities";
const SERVER_INFO_META: &str = "io.modelcontextprotocol/serverInfo";

pub(crate) async fn run() -> Result<()> {
    let endpoint = Arc::new(AgentToolEndpoint::from_env()?);
    serve(endpoint, tokio::io::stdin(), tokio::io::stdout()).await
}

pub(crate) async fn list_tools(name: Option<&str>) -> Result<()> {
    let endpoint = AgentToolEndpoint::from_env()?;
    let tools = forward(&endpoint, "__borg_tools", json!({}), None).await?;
    let tools = tools
        .as_array()
        .context("Borg agent tool catalog was not an array")?;
    let output = if let Some(name) = name {
        tools
            .iter()
            .find(|tool| tool.get("name").and_then(Value::as_str) == Some(name))
            .cloned()
            .with_context(|| format!("unknown Borg capability `{name}`"))?
    } else {
        Value::Array(tools.clone())
    };
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

pub(crate) async fn call_tool(name: &str, arguments: Option<&str>) -> Result<()> {
    let arguments = match arguments {
        Some("-") => {
            let mut input = String::new();
            tokio::io::stdin().read_to_string(&mut input).await?;
            serde_json::from_str(&input).context("stdin did not contain valid JSON")?
        }
        Some(arguments) => {
            serde_json::from_str(arguments).context("tool arguments are not valid JSON")?
        }
        None => json!({}),
    };
    if !arguments.is_object() {
        bail!("tool arguments must be a JSON object");
    }
    let endpoint = AgentToolEndpoint::from_env()?;
    let output = forward(&endpoint, name, arguments, None).await?;
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

async fn serve<R, W>(endpoint: Arc<AgentToolEndpoint>, read: R, mut write: W) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut lines = BufReader::new(read).lines();
    let (response_tx, mut response_rx) = mpsc::channel(32);
    let mut active = HashMap::<String, CancellationToken>::new();
    loop {
        tokio::select! {
            line = lines.next_line() => {
                let Some(line) = line? else { break };
                if let Some(request_id) = cancellation_request_id(&line) {
                    if let Some(cancel) = active.remove(&request_key(&request_id)) {
                        cancel.cancel();
                    }
                    continue;
                }
                if !request_is_tool_call(&line) {
                    if let Some(response) = handle_line_with_cancel(&endpoint, &line, None).await {
                        write.write_all(response.to_string().as_bytes()).await?;
                        write.write_all(b"\n").await?;
                        write.flush().await?;
                    }
                    continue;
                }
                let request_id = request_id_from_line(&line);
                let key = request_id.as_ref().map(request_key);
                let cancel = request_id.map(|id| {
                    let cancel = CancellationToken::new();
                    active.insert(request_key(&id), cancel.clone());
                    cancel
                });
                let endpoint = Arc::clone(&endpoint);
                let response_tx = response_tx.clone();
                tokio::spawn(async move {
                    let response_cancel = cancel.clone();
                    let response = handle_line_with_cancel(&endpoint, &line, cancel).await;
                    let response = if response_cancel
                        .is_some_and(|cancel| cancel.is_cancelled())
                    {
                        None
                    } else {
                        response
                    };
                    let _ = response_tx.send((key, response)).await;
                });
            }
            response = response_rx.recv() => {
                let Some((key, response)) = response else { break };
                if let Some(key) = key {
                    active.remove(&key);
                }
                if let Some(response) = response {
                    write.write_all(response.to_string().as_bytes()).await?;
                    write.write_all(b"\n").await?;
                    write.flush().await?;
                }
            }
        }
    }
    for cancel in active.into_values() {
        cancel.cancel();
    }
    Ok(())
}

enum AgentToolEndpoint {
    #[cfg(unix)]
    Unix {
        socket: PathBuf,
        provider: borg_remote::CodingProvider,
        shared_work_enabled: bool,
        consultation_enabled: bool,
        team_policy: Option<borg_remote::TeamPolicy>,
    },
    #[cfg(not(unix))]
    Loopback {
        address: std::net::SocketAddr,
        token: String,
        provider: borg_remote::CodingProvider,
        shared_work_enabled: bool,
        consultation_enabled: bool,
        team_policy: Option<borg_remote::TeamPolicy>,
    },
}

impl AgentToolEndpoint {
    fn from_env() -> Result<Self> {
        let provider = agent_tool_provider()?;
        let team_policy = std::env::var("BORG_AGENT_TEAM_POLICY")
            .ok()
            .map(|policy| serde_json::from_str(&policy))
            .transpose()
            .context("BORG_AGENT_TEAM_POLICY must contain a valid team policy")?;
        let shared_work_enabled = std::env::var("BORG_AGENT_SHARED_WORK_ENABLED")
            .ok()
            .map(|value| value.parse::<bool>())
            .transpose()
            .context("BORG_AGENT_SHARED_WORK_ENABLED must be true or false")?
            .unwrap_or(false);
        let consultation_enabled = std::env::var("BORG_AGENT_CONSULTATION_ENABLED")
            .ok()
            .map(|value| value.parse::<bool>())
            .transpose()
            .context("BORG_AGENT_CONSULTATION_ENABLED must be true or false")?
            .unwrap_or(true);
        #[cfg(unix)]
        {
            std::env::var_os("BORG_AGENT_TOOL_SOCKET")
                .map(PathBuf::from)
                .map(|socket| Self::Unix {
                    socket,
                    provider,
                    shared_work_enabled,
                    consultation_enabled,
                    team_policy,
                })
                .context("BORG_AGENT_TOOL_SOCKET is required")
        }
        #[cfg(not(unix))]
        {
            let raw_address =
                std::env::var("BORG_AGENT_TOOL_TCP").context("BORG_AGENT_TOOL_TCP is required")?;
            let address: std::net::SocketAddr = raw_address
                .parse()
                .context("BORG_AGENT_TOOL_TCP must be a socket address")?;
            if !address.ip().is_loopback() {
                bail!("BORG_AGENT_TOOL_TCP must use a loopback address");
            }
            let token = std::env::var("BORG_AGENT_TOOL_TOKEN")
                .context("BORG_AGENT_TOOL_TOKEN is required for loopback agent tools")?;
            Ok(Self::Loopback {
                address,
                token,
                provider,
                shared_work_enabled,
                consultation_enabled,
                team_policy,
            })
        }
    }

    fn provider(&self) -> borg_remote::CodingProvider {
        match self {
            #[cfg(unix)]
            Self::Unix { provider, .. } => *provider,
            #[cfg(not(unix))]
            Self::Loopback { provider, .. } => *provider,
        }
    }

    fn team_policy(&self) -> Option<&borg_remote::TeamPolicy> {
        match self {
            #[cfg(unix)]
            Self::Unix { team_policy, .. } => team_policy.as_ref(),
            #[cfg(not(unix))]
            Self::Loopback { team_policy, .. } => team_policy.as_ref(),
        }
    }

    fn shared_work_enabled(&self) -> bool {
        match self {
            #[cfg(unix)]
            Self::Unix {
                shared_work_enabled,
                ..
            } => *shared_work_enabled,
            #[cfg(not(unix))]
            Self::Loopback {
                shared_work_enabled,
                ..
            } => *shared_work_enabled,
        }
    }

    fn consultation_enabled(&self) -> bool {
        match self {
            #[cfg(unix)]
            Self::Unix {
                consultation_enabled,
                ..
            } => *consultation_enabled,
            #[cfg(not(unix))]
            Self::Loopback {
                consultation_enabled,
                ..
            } => *consultation_enabled,
        }
    }
}

#[derive(Debug)]
enum RequestProtocol {
    Modern,
    Legacy,
}

#[derive(Debug)]
enum ProtocolError {
    Invalid(String),
    Unsupported { requested: String },
}

#[cfg(test)]
async fn handle_line(endpoint: &AgentToolEndpoint, line: &str) -> Option<Value> {
    handle_line_with_cancel(endpoint, line, None).await
}

async fn handle_line_with_cancel(
    endpoint: &AgentToolEndpoint,
    line: &str,
    cancel: Option<CancellationToken>,
) -> Option<Value> {
    let request: Value = match serde_json::from_str(line) {
        Ok(request) => request,
        Err(error) => return Some(rpc_error(Value::Null, -32700, error.to_string())),
    };
    let id = request.get("id").cloned()?;
    let method = request.get("method").and_then(Value::as_str).unwrap_or("");
    let protocol = match request_protocol(&request, method) {
        Ok(protocol) => protocol,
        Err(ProtocolError::Invalid(message)) => return Some(rpc_error(id, -32602, message)),
        Err(ProtocolError::Unsupported { requested }) => {
            return Some(rpc_error_with_data(
                id,
                -32022,
                format!("unsupported MCP protocol version `{requested}`"),
                json!({
                    "supported": [CURRENT_PROTOCOL_VERSION, LEGACY_PROTOCOL_VERSION],
                    "requested": requested,
                }),
            ));
        }
    };
    if !matches!(
        method,
        "server/discover"
            | "initialize"
            | "ping"
            | "tools/list"
            | "tools/call"
            | "resources/list"
            | "resources/templates/list"
    ) {
        return Some(rpc_error(
            id,
            -32601,
            format!("unsupported method: {method}"),
        ));
    }
    let modern = matches!(protocol, RequestProtocol::Modern);
    let result = match method {
        "server/discover" if modern => Ok(json!({
            "resultType": "complete",
            "supportedVersions": [CURRENT_PROTOCOL_VERSION, LEGACY_PROTOCOL_VERSION],
            "capabilities": {
                "tools": { "listChanged": false },
                "resources": { "listChanged": false, "subscribe": false }
            },
            "_meta": server_meta(),
        })),
        "server/discover" => Err(anyhow::anyhow!(
            "server/discover requires stateless MCP request metadata"
        )),
        "initialize" if !modern => Ok(json!({
            "protocolVersion": LEGACY_PROTOCOL_VERSION,
            "serverInfo": server_info(),
            "capabilities": { "tools": {}, "resources": {} }
        })),
        "initialize" => {
            return Some(rpc_error(
                id,
                -32601,
                "initialize is only available for legacy MCP clients".to_string(),
            ));
        }
        "ping" if modern => Ok(json!({
            "resultType": "complete",
            "_meta": server_meta(),
        })),
        "ping" => Ok(json!({})),
        "tools/list" => {
            let result = json!({
                "tools": borg_remote::agent_tool_specs_with_capabilities_and_consultation(
                    endpoint.provider(),
                    true,
                    endpoint.shared_work_enabled(),
                    endpoint.team_policy(),
                    endpoint.consultation_enabled(),
                )
            });
            Ok(if modern {
                modern_result(result)
            } else {
                result
            })
        }
        "resources/list" => {
            let result = json!({"resources": []});
            Ok(if modern {
                modern_result(result)
            } else {
                result
            })
        }
        "resources/templates/list" => {
            let result = json!({"resourceTemplates": []});
            Ok(if modern {
                modern_result(result)
            } else {
                result
            })
        }
        "tools/call" => {
            let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
            let Some(name) = params.get("name").and_then(Value::as_str) else {
                return Some(rpc_error(id, -32602, "missing tool name".to_string()));
            };
            let arguments = params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            match forward(endpoint, name, arguments, cancel).await {
                Ok(value) => Ok(if modern {
                    modern_tool_result(value)
                } else {
                    legacy_tool_result(value)
                }),
                Err(error) if modern => Ok(modern_tool_error(&error)),
                Err(error) => Err(error),
            }
        }
        _ => unreachable!("MCP method was checked above"),
    };
    Some(match result {
        Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
        Err(error) => rpc_error(id, -32000, format!("{error:#}")),
    })
}

fn request_protocol(
    request: &Value,
    method: &str,
) -> std::result::Result<RequestProtocol, ProtocolError> {
    let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
    let Some(params) = params.as_object() else {
        return Err(ProtocolError::Invalid(
            "MCP request params must be an object".to_string(),
        ));
    };
    let Some(meta) = params.get("_meta") else {
        if method == "server/discover" {
            return Err(ProtocolError::Invalid(
                "stateless MCP requests require params._meta".to_string(),
            ));
        }
        return Ok(RequestProtocol::Legacy);
    };
    let Some(meta) = meta.as_object() else {
        return Err(ProtocolError::Invalid(
            "MCP request params._meta must be an object".to_string(),
        ));
    };
    let Some(version) = meta.get(PROTOCOL_VERSION_META).and_then(Value::as_str) else {
        // Standard stateful MCP clients may attach unrelated request metadata
        // (for example a progress token) without opting into Borg's stateless
        // protocol. The Codex app-server does this during `initialize`; treat
        // it as legacy unless the stateless-only discovery method was used.
        if method != "server/discover" {
            return Ok(RequestProtocol::Legacy);
        }
        return Err(ProtocolError::Invalid(format!(
            "MCP request metadata is missing `{PROTOCOL_VERSION_META}`"
        )));
    };
    if version != CURRENT_PROTOCOL_VERSION {
        return Err(ProtocolError::Unsupported {
            requested: version.to_string(),
        });
    }
    if !meta
        .get(CLIENT_CAPABILITIES_META)
        .is_some_and(Value::is_object)
    {
        return Err(ProtocolError::Invalid(format!(
            "MCP request metadata is missing object `{CLIENT_CAPABILITIES_META}`"
        )));
    }
    Ok(RequestProtocol::Modern)
}

fn server_info() -> Value {
    json!({ "name": "borg-agent", "version": env!("CARGO_PKG_VERSION") })
}

fn server_meta() -> Value {
    json!({ SERVER_INFO_META: server_info() })
}

fn modern_result(mut result: Value) -> Value {
    let object = result
        .as_object_mut()
        .expect("MCP result helpers only receive JSON objects");
    object.insert(
        "resultType".to_string(),
        Value::String("complete".to_string()),
    );
    object.insert("_meta".to_string(), server_meta());
    result
}

fn legacy_tool_result(value: Value) -> Value {
    json!({
        "content": [{
            "type": "text",
            "text": serde_json::to_string(&value).unwrap_or_default()
        }],
        "structuredContent": value,
        "isError": false
    })
}

fn modern_tool_result(value: Value) -> Value {
    modern_result(legacy_tool_result(value))
}

fn modern_tool_error(error: &anyhow::Error) -> Value {
    modern_result(json!({
        "content": [{
            "type": "text",
            "text": format!("{error:#}")
        }],
        "isError": true
    }))
}

fn agent_tool_provider() -> Result<borg_remote::CodingProvider> {
    let provider = std::env::var("BORG_AGENT_TOOL_PROVIDER")
        .context("BORG_AGENT_TOOL_PROVIDER is required")?;
    serde_json::from_value(Value::String(provider))
        .context("BORG_AGENT_TOOL_PROVIDER is not a supported provider")
}

async fn forward(
    endpoint: &AgentToolEndpoint,
    name: &str,
    arguments: Value,
    cancel: Option<CancellationToken>,
) -> Result<Value> {
    #[cfg(unix)]
    let response = match endpoint {
        AgentToolEndpoint::Unix { socket, .. } => {
            let stream = UnixStream::connect(socket)
                .await
                .with_context(|| format!("failed to connect to {}", socket.display()))?;
            exchange(stream, name, arguments, None, cancel).await?
        }
    };
    #[cfg(not(unix))]
    let response = match endpoint {
        AgentToolEndpoint::Loopback { address, token, .. } => {
            let stream = TcpStream::connect(address).await.with_context(|| {
                format!("failed to connect to local agent tool server {address}")
            })?;
            exchange(stream, name, arguments, Some(token.as_str()), cancel).await?
        }
    };
    if let Some(error) = response.get("error").and_then(Value::as_str) {
        bail!("{error}");
    }
    response
        .get("result")
        .cloned()
        .context("Borg agent tool server returned no result")
}

async fn exchange<S>(
    stream: S,
    name: &str,
    arguments: Value,
    token: Option<&str>,
    cancel: Option<CancellationToken>,
) -> Result<Value>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (read, mut write) = tokio::io::split(stream);
    let mut request = json!({ "name": name, "arguments": arguments });
    request["workflow_approved"] = Value::Bool(
        std::env::var("BORG_AGENT_TOOL_APPROVED")
            .ok()
            .is_some_and(|value| value == "1"),
    );
    if let Some(token) = token {
        request["token"] = Value::String(token.to_string());
    }
    write.write_all(format!("{request}\n").as_bytes()).await?;
    let mut lines = BufReader::new(read).lines();
    let response = if let Some(cancel) = cancel.as_ref() {
        tokio::select! {
            _ = cancel.cancelled() => bail!("Borg agent tool call was cancelled"),
            response = lines.next_line() => response?,
        }
    } else {
        lines.next_line().await?
    }
    .context("Borg agent tool server closed without a response")?;
    serde_json::from_str(&response).context("Borg agent tool server returned invalid JSON")
}

fn request_id_from_line(line: &str) -> Option<Value> {
    serde_json::from_str::<Value>(line)
        .ok()
        .and_then(|request| request.get("id").cloned())
}

fn request_is_tool_call(line: &str) -> bool {
    serde_json::from_str::<Value>(line)
        .ok()
        .and_then(|request| {
            request
                .get("method")
                .and_then(Value::as_str)
                .map(|method| method == "tools/call")
        })
        .unwrap_or(false)
}

fn cancellation_request_id(line: &str) -> Option<Value> {
    let request = serde_json::from_str::<Value>(line).ok()?;
    if request.get("method").and_then(Value::as_str) != Some("notifications/cancelled") {
        return None;
    }
    request.get("params")?.get("requestId").cloned()
}

fn request_key(id: &Value) -> String {
    serde_json::to_string(id).unwrap_or_else(|_| "null".to_string())
}

fn rpc_error(id: Value, code: i64, message: String) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message }
    })
}

fn rpc_error_with_data(id: Value, code: i64, message: String, data: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message, "data": data }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn local_proxy_exposes_the_shared_agent_tool_catalog() {
        #[cfg(unix)]
        let endpoint = AgentToolEndpoint::Unix {
            socket: Path::new("/unused").to_path_buf(),
            provider: borg_remote::CodingProvider::Codex,
            shared_work_enabled: true,
            consultation_enabled: true,
            team_policy: None,
        };
        #[cfg(not(unix))]
        let endpoint = AgentToolEndpoint::Loopback {
            address: "127.0.0.1:1".parse().unwrap(),
            token: "unused".to_string(),
            provider: borg_remote::CodingProvider::Codex,
            shared_work_enabled: true,
            consultation_enabled: true,
            team_policy: None,
        };
        let response = handle_line(
            &endpoint,
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#,
        )
        .await
        .unwrap();
        let names = response["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str))
            .collect::<Vec<_>>();
        assert!(names.contains(&"get_goal"));
        assert!(names.contains(&"update_plan"));
        assert!(names.contains(&"spawn_agent"));
        assert!(names.contains(&"wait_agent"));
        assert!(names.contains(&"create_shared_work"));
        assert!(names.contains(&"consult_peer"));
        assert!(names.contains(&"rotate_peer"));
        assert!(names.contains(&"lsp_workspace_diagnostics"));
        assert!(names.contains(&"list_blu_workflows"));
        assert!(names.contains(&"run_blu_extension"));
    }

    #[tokio::test]
    async fn stateless_discovery_and_tool_listing_use_current_protocol_metadata() {
        #[cfg(unix)]
        let endpoint = AgentToolEndpoint::Unix {
            socket: Path::new("/unused").to_path_buf(),
            provider: borg_remote::CodingProvider::Codex,
            shared_work_enabled: false,
            consultation_enabled: true,
            team_policy: None,
        };
        #[cfg(not(unix))]
        let endpoint = AgentToolEndpoint::Loopback {
            address: "127.0.0.1:1".parse().unwrap(),
            token: "unused".to_string(),
            provider: borg_remote::CodingProvider::Codex,
            shared_work_enabled: false,
            consultation_enabled: true,
            team_policy: None,
        };
        let meta = json!({
            PROTOCOL_VERSION_META: CURRENT_PROTOCOL_VERSION,
            CLIENT_CAPABILITIES_META: {},
        });
        let discover = handle_line(
            &endpoint,
            &json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "server/discover",
                "params": { "_meta": meta },
            })
            .to_string(),
        )
        .await
        .unwrap();
        assert_eq!(discover["result"]["resultType"], "complete");
        assert_eq!(
            discover["result"]["supportedVersions"][0],
            CURRENT_PROTOCOL_VERSION
        );
        assert!(discover["result"]["_meta"][SERVER_INFO_META].is_object());
        assert_eq!(
            discover["result"]["capabilities"]["resources"]["listChanged"],
            false
        );

        let listing = handle_line(
            &endpoint,
            &json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list",
                "params": { "_meta": {
                    PROTOCOL_VERSION_META: CURRENT_PROTOCOL_VERSION,
                    CLIENT_CAPABILITIES_META: {},
                }},
            })
            .to_string(),
        )
        .await
        .unwrap();
        assert_eq!(listing["result"]["resultType"], "complete");
        assert!(listing["result"]["tools"].is_array());

        let resources = handle_line(
            &endpoint,
            &json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "resources/list",
                "params": { "_meta": {
                    PROTOCOL_VERSION_META: CURRENT_PROTOCOL_VERSION,
                    CLIENT_CAPABILITIES_META: {},
                }},
            })
            .to_string(),
        )
        .await
        .unwrap();
        assert_eq!(resources["result"]["resources"], json!([]));

        let templates = handle_line(
            &endpoint,
            &json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "resources/templates/list",
                "params": { "_meta": {
                    PROTOCOL_VERSION_META: CURRENT_PROTOCOL_VERSION,
                    CLIENT_CAPABILITIES_META: {},
                }},
            })
            .to_string(),
        )
        .await
        .unwrap();
        assert_eq!(templates["result"]["resourceTemplates"], json!([]));
    }

    #[tokio::test]
    async fn stateless_requests_require_protocol_metadata() {
        #[cfg(unix)]
        let endpoint = AgentToolEndpoint::Unix {
            socket: Path::new("/unused").to_path_buf(),
            provider: borg_remote::CodingProvider::Codex,
            shared_work_enabled: false,
            consultation_enabled: true,
            team_policy: None,
        };
        #[cfg(not(unix))]
        let endpoint = AgentToolEndpoint::Loopback {
            address: "127.0.0.1:1".parse().unwrap(),
            token: "unused".to_string(),
            provider: borg_remote::CodingProvider::Codex,
            shared_work_enabled: false,
            consultation_enabled: true,
            team_policy: None,
        };
        let response = handle_line(
            &endpoint,
            r#"{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{}}"#,
        )
        .await
        .unwrap();
        assert_eq!(response["error"]["code"], -32602);
    }

    #[tokio::test]
    async fn legacy_initialize_accepts_standard_request_metadata() {
        #[cfg(unix)]
        let endpoint = AgentToolEndpoint::Unix {
            socket: Path::new("/unused").to_path_buf(),
            provider: borg_remote::CodingProvider::Codex,
            shared_work_enabled: false,
            consultation_enabled: true,
            team_policy: None,
        };
        #[cfg(not(unix))]
        let endpoint = AgentToolEndpoint::Loopback {
            address: "127.0.0.1:1".parse().unwrap(),
            token: "unused".to_string(),
            provider: borg_remote::CodingProvider::Codex,
            shared_work_enabled: false,
            consultation_enabled: true,
            team_policy: None,
        };
        let response = handle_line(
            &endpoint,
            &json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": LEGACY_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": { "name": "codex-app-server", "version": "test" },
                    "_meta": { "progressToken": "startup" },
                },
            })
            .to_string(),
        )
        .await
        .unwrap();

        assert!(response.get("error").is_none(), "{response}");
        assert_eq!(
            response["result"]["protocolVersion"],
            LEGACY_PROTOCOL_VERSION
        );
    }

    #[tokio::test]
    async fn pipelined_handshake_is_flushed_before_input_eof() {
        #[cfg(unix)]
        let endpoint = AgentToolEndpoint::Unix {
            socket: Path::new("/unused").to_path_buf(),
            provider: borg_remote::CodingProvider::Codex,
            shared_work_enabled: false,
            consultation_enabled: true,
            team_policy: None,
        };
        #[cfg(not(unix))]
        let endpoint = AgentToolEndpoint::Loopback {
            address: "127.0.0.1:1".parse().unwrap(),
            token: "unused".to_string(),
            provider: borg_remote::CodingProvider::Codex,
            shared_work_enabled: false,
            consultation_enabled: true,
            team_policy: None,
        };
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let (read, write) = tokio::io::split(server);
        let serving = tokio::spawn(serve(Arc::new(endpoint), read, write));
        client
            .write_all(
                concat!(
                    "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}\n",
                    "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n",
                    "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{}}\n",
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        client.shutdown().await.unwrap();
        let mut output = Vec::new();
        client.read_to_end(&mut output).await.unwrap();
        serving.await.unwrap().unwrap();

        let responses = String::from_utf8(output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0]["id"], 1);
        assert_eq!(responses[1]["id"], 2);
        assert!(responses[1]["result"]["tools"].is_array());
    }
}
