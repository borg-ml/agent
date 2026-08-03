#[cfg(all(unix, test))]
use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
#[cfg(not(unix))]
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;

pub(crate) async fn run() -> Result<()> {
    let endpoint = AgentToolEndpoint::from_env()?;
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let mut stdout = tokio::io::stdout();
    while let Some(line) = lines.next_line().await? {
        if let Some(response) = handle_line(&endpoint, &line).await {
            stdout.write_all(response.to_string().as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
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

async fn handle_line(endpoint: &AgentToolEndpoint, line: &str) -> Option<Value> {
    let request: Value = match serde_json::from_str(line) {
        Ok(request) => request,
        Err(error) => return Some(rpc_error(Value::Null, -32700, error.to_string())),
    };
    let id = request.get("id").cloned()?;
    let method = request.get("method").and_then(Value::as_str).unwrap_or("");
    let result = match method {
        "initialize" => Ok(json!({
            "protocolVersion": "2024-11-05",
            "serverInfo": { "name": "borg-agent", "version": env!("CARGO_PKG_VERSION") },
            "capabilities": { "tools": {} }
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({
            "tools": borg_remote::agent_tool_specs_with_capabilities_and_consultation(
                endpoint.provider(),
                true,
                endpoint.shared_work_enabled(),
                endpoint.team_policy(),
                endpoint.consultation_enabled(),
            )
        })),
        "tools/call" => {
            let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
            let name = params
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("missing tool name"));
            match name {
                Ok(name) => {
                    let arguments = params
                        .get("arguments")
                        .cloned()
                        .unwrap_or_else(|| json!({}));
                    forward(endpoint, name, arguments).await.map(|value| {
                        json!({
                            "content": [{
                                "type": "text",
                                "text": serde_json::to_string(&value).unwrap_or_default()
                            }],
                            "structuredContent": value,
                            "isError": false
                        })
                    })
                }
                Err(error) => Err(error),
            }
        }
        _ => Err(anyhow::anyhow!("unsupported method: {method}")),
    };
    Some(match result {
        Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
        Err(error) => rpc_error(id, -32000, format!("{error:#}")),
    })
}

fn agent_tool_provider() -> Result<borg_remote::CodingProvider> {
    let provider = std::env::var("BORG_AGENT_TOOL_PROVIDER")
        .context("BORG_AGENT_TOOL_PROVIDER is required")?;
    serde_json::from_value(Value::String(provider))
        .context("BORG_AGENT_TOOL_PROVIDER is not a supported provider")
}

async fn forward(endpoint: &AgentToolEndpoint, name: &str, arguments: Value) -> Result<Value> {
    #[cfg(unix)]
    let response = match endpoint {
        AgentToolEndpoint::Unix { socket, .. } => {
            let stream = UnixStream::connect(socket)
                .await
                .with_context(|| format!("failed to connect to {}", socket.display()))?;
            exchange(stream, name, arguments, None).await?
        }
    };
    #[cfg(not(unix))]
    let response = match endpoint {
        AgentToolEndpoint::Loopback { address, token, .. } => {
            let stream = TcpStream::connect(address).await.with_context(|| {
                format!("failed to connect to local agent tool server {address}")
            })?;
            exchange(stream, name, arguments, Some(token.as_str())).await?
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

async fn exchange<S>(stream: S, name: &str, arguments: Value, token: Option<&str>) -> Result<Value>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (read, mut write) = tokio::io::split(stream);
    let mut request = json!({ "name": name, "arguments": arguments });
    if let Some(token) = token {
        request["token"] = Value::String(token.to_string());
    }
    write.write_all(format!("{request}\n").as_bytes()).await?;
    let mut lines = BufReader::new(read).lines();
    let response = lines
        .next_line()
        .await?
        .context("Borg agent tool server closed without a response")?;
    serde_json::from_str(&response).context("Borg agent tool server returned invalid JSON")
}

fn rpc_error(id: Value, code: i64, message: String) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
    }
}
