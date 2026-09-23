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
    let output = spool_result_attachments(output);
    println!("{}", serde_json::to_string(&output)?);
    // Printed first so non-image output is never lost, then failed: an image
    // that did not reach the model must not look like a success.
    if let Some(error) = first_spool_error(&output, 0) {
        bail!("{error}");
    }
    Ok(())
}

fn first_spool_error(value: &Value, depth: usize) -> Option<&str> {
    if depth > MAX_ATTACHMENT_SEARCH_DEPTH {
        return None;
    }
    let object = value.as_object()?;
    if let Some(error) = object.get(SPOOL_ERROR_KEY).and_then(Value::as_str) {
        return Some(error);
    }
    object
        .values()
        .find_map(|nested| first_spool_error(nested, depth + 1))
}

/// Number of images spooled, left in stdout in place of the base64 so the
/// printed result still says what happened.
const SPOOLED_IMAGES_KEY: &str = "spooled_images";
const ATTACHMENTS_KEY: &str = "borg_attachments";
const ATTACHMENT_SPOOL_ENV: &str = "BORG_ATTACHMENT_SPOOL";
/// Why images stayed inline, when they did. Reported rather than swallowed so
/// a degraded result is visible instead of looking like a clean one.
const SPOOL_ERROR_KEY: &str = "spool_error";
/// Tool results are shallow envelopes; this is enough to reach the images
/// without ever walking a deep structure a capability happens to return.
const MAX_ATTACHMENT_SEARCH_DEPTH: usize = 6;

/// Move a capability's images out of this command's stdout and into the
/// runtime's image spool.
///
/// `borg call` normally runs inside `exec`, which captures stdout into a
/// bounded head/tail buffer and renders it down to a token budget. A
/// screenshot printed as base64 is therefore cut in half by an
/// `… bytes omitted …` marker and reaches the model as unusable text — the
/// bytes are already destroyed by the time anything could parse them back out.
/// Writing each image to `$BORG_ATTACHMENT_SPOOL` instead keeps stdout small
/// and lets the runtime deliver real vision attachments.
///
/// With no spool set (a human running `borg call` in a terminal, or an older
/// runtime) the result is returned untouched, so this only ever adds a path.
pub(crate) fn spool_result_attachments(mut output: Value) -> Value {
    let Some(dir) = std::env::var_os(ATTACHMENT_SPOOL_ENV).map(std::path::PathBuf::from) else {
        return output;
    };
    spool_attachments_in(&mut output, &dir, 0);
    output
}

fn spool_attachments_in(value: &mut Value, dir: &std::path::Path, depth: usize) {
    if depth > MAX_ATTACHMENT_SEARCH_DEPTH {
        return;
    }
    let Some(object) = value.as_object_mut() else {
        return;
    };
    // Capabilities differ in where they put the key: beside the result, or
    // nested under `value` for a runtime script. Search rather than assume.
    if let Some(Value::Array(attachments)) = object.remove(ATTACHMENTS_KEY) {
        let mut written = Vec::new();
        let mut failure = None;
        for (index, attachment) in attachments.iter().enumerate() {
            match write_spooled_attachment(dir, attachment, index) {
                Ok(path) => written.push(path),
                Err(error) => {
                    failure = Some(format!("image #{index}: {error}"));
                    break;
                }
            }
        }
        match failure {
            None => {
                object.insert(SPOOLED_IMAGES_KEY.to_string(), json!(written.len()));
            }
            Some(error) => {
                // Never fall back to inline base64 once a spool is configured.
                // Reinlining puts the image back into stdout, where it is
                // truncated into unusable text and exposed in the transcript,
                // while the command still exits 0 and looks like it worked.
                // Partial writes are removed so nothing arrives twice, and the
                // caller turns this key into a non-zero exit.
                for path in &written {
                    let _ = std::fs::remove_file(path);
                }
                object.insert(SPOOL_ERROR_KEY.to_string(), json!(error));
            }
        }
    }
    for (_, nested) in object.iter_mut() {
        spool_attachments_in(nested, dir, depth + 1);
    }
}

/// Write one image into the spool, returning where it landed.
///
/// The runtime reads any complete file it finds, so a partly written file
/// would be read as a corrupt image. Writing to a `.tmp` name and renaming
/// into place makes the file appear atomically and whole. The timestamp prefix
/// keeps images in production order when one `exec` session makes several
/// calls, and the process id keeps concurrent commands from colliding.
///
/// A screenshot can show anything on the user's desktop, so the file is
/// owner-only, and `create_new` refuses to follow a symlink or overwrite an
/// existing file planted under the name this process is about to use.
fn write_spooled_attachment(
    dir: &std::path::Path,
    attachment: &Value,
    index: usize,
) -> Result<std::path::PathBuf> {
    use std::io::Write as _;

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    let name = format!("{nanos:039}-{}-{index:03}", std::process::id());
    let temporary = dir.join(format!("{name}.tmp"));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let write = |temporary: &std::path::Path| -> Result<()> {
        let mut file = options.open(temporary)?;
        file.write_all(&serde_json::to_vec(attachment)?)?;
        // The runtime may read this the instant it is renamed.
        file.sync_all()?;
        Ok(())
    };
    write(&temporary).with_context(|| format!("write {}", temporary.display()))?;
    let final_path = dir.join(format!("{name}.json"));
    if let Err(error) = std::fs::rename(&temporary, &final_path) {
        let _ = std::fs::remove_file(&temporary);
        return Err(anyhow::Error::new(error).context(format!("publish {}", final_path.display())));
    }
    Ok(final_path)
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
                    // Absent means a top-level session from an older server.
                    std::env::var("BORG_AGENT_DESKTOP_ENABLED")
                        .ok()
                        .is_none_or(|value| value != "false"),
                    std::env::var("BORG_AGENT_WATCHER_YIELD_ENABLED")
                        .is_ok_and(|value| value == "true"),
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
            let mut arguments = params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            strip_action_metadata(&mut arguments);
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

fn strip_action_metadata(arguments: &mut Value) {
    if let Some(arguments) = arguments.as_object_mut() {
        arguments.remove("action");
    }
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

/// Images a result may carry as MCP content items. Beyond this they are
/// dropped with a note rather than growing one tool result without bound.
const MAX_RESULT_IMAGES: usize = 8;
/// Count of images lifted out of the metadata, matching the native harness.
const ATTACHED_IMAGES_KEY: &str = "attached_images";
const DROPPED_ATTACHMENTS_KEY: &str = "dropped_attachments";
/// Original and sent size of each lifted image, with the exact scale.
const SENT_IMAGES_KEY: &str = "sent_images";
const IMAGE_COORDINATES_KEY: &str = "image_coordinates";
/// Providers downscale larger images themselves (Anthropic above a 1568 px
/// edge or about 1.15 megapixels), silently changing the coordinate space the
/// model sees. Fitting them here instead makes the scale known and reported.
const MAX_IMAGE_EDGE: u32 = 1568;
const MAX_IMAGE_PIXELS: f64 = 1_150_000.0;
/// A fitted PNG larger than this is sent as JPEG instead.
const MAX_FITTED_PNG_BYTES: usize = 1024 * 1024;

struct SentImage {
    media_type: String,
    data_base64: String,
    size: Value,
    scaled: bool,
}

/// Fit one image within the model's size limits. Returns `None` when it
/// cannot be decoded, in which case it is sent exactly as produced.
fn fit_image_for_model(media_type: &str, data_base64: &str) -> Option<SentImage> {
    use base64::Engine as _;
    let engine = base64::engine::general_purpose::STANDARD;
    let bytes = engine.decode(data_base64).ok()?;
    let (width, height) = image::ImageReader::new(std::io::Cursor::new(&bytes))
        .with_guessed_format()
        .ok()?
        .into_dimensions()
        .ok()?;
    if width == 0 || height == 0 {
        return None;
    }
    let scale = 1f64
        .min(f64::from(MAX_IMAGE_EDGE) / f64::from(width.max(height)))
        .min((MAX_IMAGE_PIXELS / (f64::from(width) * f64::from(height))).sqrt());
    let size = |sent_width: u32, sent_height: u32, media_type: &str| {
        json!({
            "width": width, "height": height,
            "sent_width": sent_width, "sent_height": sent_height,
            "scale": (f64::from(sent_width) / f64::from(width) * 1e6).round() / 1e6,
            "media_type": media_type,
        })
    };
    if scale >= 1.0 {
        return Some(SentImage {
            media_type: media_type.to_string(),
            data_base64: data_base64.to_string(),
            size: size(width, height, media_type),
            scaled: false,
        });
    }
    let sent_width = ((f64::from(width) * scale).round() as u32).max(1);
    let sent_height = ((f64::from(height) * scale).round() as u32).max(1);
    let resized = image::load_from_memory(&bytes).ok()?.resize_exact(
        sent_width,
        sent_height,
        image::imageops::FilterType::Triangle,
    );
    let mut encoded = Vec::new();
    resized
        .write_to(
            &mut std::io::Cursor::new(&mut encoded),
            image::ImageFormat::Png,
        )
        .ok()?;
    let mut sent_type = "image/png";
    if encoded.len() > MAX_FITTED_PNG_BYTES {
        encoded.clear();
        image::DynamicImage::ImageRgb8(resized.to_rgb8())
            .write_with_encoder(image::codecs::jpeg::JpegEncoder::new_with_quality(
                &mut std::io::Cursor::new(&mut encoded),
                85,
            ))
            .ok()?;
        sent_type = "image/jpeg";
    }
    Some(SentImage {
        media_type: sent_type.to_string(),
        data_base64: engine.encode(&encoded),
        size: size(sent_width, sent_height, sent_type),
        scaled: true,
    })
}

/// Move a result's `borg_attachments` images into MCP `image` content items.
///
/// MCP clients (Claude Code, Codex, OpenCode) hand `image` items to the model
/// as pixels. Base64 left inside the JSON text is only a very long string:
/// Claude Code measured a 1920x1080 screenshot at about 90k characters and
/// spooled the whole result to a file, so the model never saw the image. The
/// metadata keeps a count in place of the base64, and anything that is not a
/// usable image is dropped with a note so the model knows why it is missing.
fn lift_result_images(value: &mut Value, images: &mut Vec<Value>, depth: usize) {
    if depth > MAX_ATTACHMENT_SEARCH_DEPTH {
        return;
    }
    let Some(object) = value.as_object_mut() else {
        return;
    };
    // Beside the result, or nested under `value` for a runtime script.
    if let Some(Value::Array(attachments)) = object.remove(ATTACHMENTS_KEY) {
        let mut lifted = 0;
        let mut dropped = Vec::new();
        let mut sizes = Vec::new();
        let mut scaled = false;
        for (index, attachment) in attachments.into_iter().enumerate() {
            let media_type = attachment.get("media_type").and_then(Value::as_str);
            let data = attachment.get("data_base64").and_then(Value::as_str);
            match (media_type, data) {
                (Some(media_type), Some(data))
                    if media_type.starts_with("image/") && !data.is_empty() =>
                {
                    if images.len() == MAX_RESULT_IMAGES {
                        dropped.push(format!(
                            "#{index}: more than {MAX_RESULT_IMAGES} images per result"
                        ));
                        continue;
                    }
                    let sent = fit_image_for_model(media_type, data);
                    let (media_type, data) = match &sent {
                        Some(sent) => (sent.media_type.as_str(), sent.data_base64.as_str()),
                        None => (media_type, data),
                    };
                    images.push(json!({"type": "image", "data": data, "mimeType": media_type}));
                    if let Some(sent) = sent {
                        scaled |= sent.scaled;
                        sizes.push(sent.size);
                    }
                    lifted += 1;
                }
                (Some(media_type), Some(_)) if !media_type.starts_with("image/") => {
                    dropped.push(format!("#{index}: {media_type} is not an image"));
                }
                _ => dropped.push(format!("#{index}: not an image attachment")),
            }
        }
        object.insert(ATTACHED_IMAGES_KEY.to_string(), json!(lifted));
        if !sizes.is_empty() {
            object.insert(SENT_IMAGES_KEY.to_string(), Value::Array(sizes));
        }
        if scaled {
            object.insert(
                IMAGE_COORDINATES_KEY.to_string(),
                json!("the image was downscaled to fit the model; a point (x, y) on it is (x * width / sent_width, y * height / sent_height) in the tool's coordinates"),
            );
        }
        if !dropped.is_empty() {
            object.insert(DROPPED_ATTACHMENTS_KEY.to_string(), json!(dropped));
        }
    }
    for (_, nested) in object.iter_mut() {
        lift_result_images(nested, images, depth + 1);
    }
}

fn legacy_tool_result(mut value: Value) -> Value {
    let mut images = Vec::new();
    lift_result_images(&mut value, &mut images, 0);
    let mut content = vec![json!({
        "type": "text",
        "text": serde_json::to_string(&value).unwrap_or_default()
    })];
    content.extend(images);
    let mut result = json!({
        "content": content,
        "isError": false
    });
    // MCP requires structuredContent to be a JSON object; array results
    // (list_watchers, list_unread_team_messages) are carried by the text
    // content only, so strict clients do not reject the call.
    if value.is_object() {
        result["structuredContent"] = value;
    }
    result
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

    /// Failure mode: a screenshot reaching an MCP client (Claude Code) as
    /// ~90k characters of base64 inside the JSON text, which the client spools
    /// to a file so the model never sees the pixels.
    #[test]
    fn tool_result_images_become_mcp_image_content() {
        let image = "iVBORw0KGgoAAAANSUhEUg".repeat(4096);
        for result in [
            legacy_tool_result(json!({
                "scope": "window", "window_id": "pd:1", "width": 1920, "height": 1080,
                "borg_attachments": [{"media_type": "image/png", "data_base64": image}],
            })),
            modern_tool_result(json!({
                "scope": "window", "window_id": "pd:1", "width": 1920, "height": 1080,
                "borg_attachments": [{"media_type": "image/png", "data_base64": image}],
            })),
        ] {
            let content = result["content"].as_array().unwrap();
            assert_eq!(content.len(), 2, "{result}");
            assert_eq!(content[0]["type"], "text");
            let text = content[0]["text"].as_str().unwrap();
            assert!(!text.contains("iVBORw0KGgo"), "base64 left in the text");
            assert!(text.len() < 200, "metadata stays small: {text}");
            let metadata: Value = serde_json::from_str(text).unwrap();
            assert_eq!(metadata["window_id"], "pd:1");
            assert_eq!(metadata["width"], 1920);
            assert_eq!(metadata[ATTACHED_IMAGES_KEY], 1);
            assert!(metadata.get(ATTACHMENTS_KEY).is_none());
            assert_eq!(result["structuredContent"], metadata);
            assert_eq!(
                content[1],
                json!({"type": "image", "data": image, "mimeType": "image/png"})
            );
            assert_eq!(result["isError"], false);
        }
    }

    fn png(width: u32, height: u32) -> String {
        use base64::Engine as _;
        let mut bytes = Vec::new();
        image::DynamicImage::new_rgb8(width, height)
            .write_to(
                &mut std::io::Cursor::new(&mut bytes),
                image::ImageFormat::Png,
            )
            .unwrap();
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    fn decoded_size(data: &str) -> (u32, u32) {
        use base64::Engine as _;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(data)
            .unwrap();
        image::ImageReader::new(std::io::Cursor::new(bytes))
            .with_guessed_format()
            .unwrap()
            .into_dimensions()
            .unwrap()
    }

    /// Failure mode: the provider silently downscaling a 1920x1080 capture,
    /// so points the model reads off the image no longer match the display
    /// pixels pointer ops take.
    #[test]
    fn oversized_images_are_fitted_with_their_scale_reported() {
        let result = legacy_tool_result(json!({
            "scope": "desktop", "display": "private",
            "borg_attachments": [{"media_type": "image/png", "data_base64": png(1920, 1080)}],
        }));
        let image = &result["content"][1];
        assert_eq!(image["mimeType"], "image/png");
        let (width, height) = decoded_size(image["data"].as_str().unwrap());
        assert!(width <= MAX_IMAGE_EDGE && height <= MAX_IMAGE_EDGE);
        assert!(f64::from(width) * f64::from(height) <= MAX_IMAGE_PIXELS);
        let metadata = &result["structuredContent"];
        assert_eq!(
            metadata[SENT_IMAGES_KEY][0],
            json!({"width": 1920, "height": 1080, "sent_width": width, "sent_height": height,
                   "scale": (f64::from(width) / 1920.0 * 1e6).round() / 1e6,
                   "media_type": "image/png"})
        );
        assert!(metadata[IMAGE_COORDINATES_KEY].is_string());

        let small = png(640, 360);
        let result = legacy_tool_result(json!({
            "borg_attachments": [{"media_type": "image/png", "data_base64": small}],
        }));
        assert_eq!(
            result["content"][1]["data"], small,
            "small images pass through"
        );
        assert_eq!(
            result["structuredContent"][SENT_IMAGES_KEY][0]["scale"],
            1.0
        );
        assert!(
            result["structuredContent"]
                .get(IMAGE_COORDINATES_KEY)
                .is_none()
        );
    }

    /// Failure mode: a runtime script's nested images staying inline, or a
    /// non-image attachment silently vanishing.
    #[test]
    fn nested_and_unusable_attachments_are_reported() {
        let result = legacy_tool_result(json!({
            "ok": true,
            "value": {"borg_attachments": [
                {"media_type": "image/jpeg", "data_base64": "AAAA"},
                {"media_type": "text/plain", "data_base64": "AAAA"},
                {"media_type": "image/png", "data_base64": ""},
            ]},
        }));
        let content = result["content"].as_array().unwrap();
        assert_eq!(content.len(), 2, "{result}");
        assert_eq!(content[1]["mimeType"], "image/jpeg");
        let value = &result["structuredContent"]["value"];
        assert_eq!(value[ATTACHED_IMAGES_KEY], 1);
        assert_eq!(
            value[DROPPED_ATTACHMENTS_KEY],
            json!([
                "#1: text/plain is not an image",
                "#2: not an image attachment"
            ])
        );

        let plain = legacy_tool_result(json!({"ok": true}));
        assert_eq!(
            plain["content"],
            json!([{"type": "text", "text": "{\"ok\":true}"}])
        );
    }

    /// The reason `borg call` writes images to a spool instead of printing
    /// them: stdout is captured into a bounded buffer by `exec`, so base64
    /// left inline is truncated into unusable text. Stdout must come back
    /// carrying a count and nothing else.
    #[test]
    fn images_leave_stdout_and_land_in_the_spool_intact() {
        let spool = tempfile::tempdir().expect("spool");
        let image = "iVBORw0KGgoAAAANSUhEUg".repeat(4096);
        let mut result = json!({
            "ok": true,
            "value": {
                "note": "screenshot taken",
                "borg_attachments": [{"media_type": "image/png", "data_base64": image}],
            },
        });

        spool_attachments_in(&mut result, spool.path(), 0);

        assert!(
            !result.to_string().contains("iVBORw0KGgo"),
            "image base64 was left in stdout: {result}"
        );
        assert_eq!(result["value"][SPOOLED_IMAGES_KEY], json!(1));
        assert_eq!(result["value"]["note"], "screenshot taken");

        let spooled: Vec<_> = std::fs::read_dir(spool.path())
            .expect("read spool")
            .flatten()
            .map(|entry| entry.path())
            .collect();
        assert_eq!(spooled.len(), 1);
        // Only a renamed, complete file is ever visible to the runtime.
        assert_eq!(
            spooled[0].extension().and_then(|s| s.to_str()),
            Some("json")
        );
        let written: Value =
            serde_json::from_slice(&std::fs::read(&spooled[0]).expect("read")).expect("json");
        assert_eq!(written["media_type"], "image/png");
        assert_eq!(written["data_base64"], image);
    }

    /// The spooled file holds a full screenshot of the user's desktop.
    #[test]
    #[cfg(unix)]
    fn a_spooled_image_is_readable_only_by_its_owner() {
        use std::os::unix::fs::PermissionsExt;

        let spool = tempfile::tempdir().expect("spool");
        let mut result =
            json!({"borg_attachments": [{"media_type": "image/png", "data_base64": "AAAA"}]});

        spool_attachments_in(&mut result, spool.path(), 0);

        let written = std::fs::read_dir(spool.path())
            .expect("read spool")
            .flatten()
            .next()
            .expect("one image")
            .path();
        let mode = std::fs::metadata(&written)
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "screenshot is readable by other users");
    }

    /// A failed write must not leave half the images in the spool while the
    /// whole set also goes back into stdout: the model would be shown the
    /// same screenshot twice, once as pixels and once as text.
    #[test]
    #[cfg(unix)]
    fn a_partial_spool_failure_does_not_deliver_an_image_twice() {
        let spool = tempfile::tempdir().expect("spool");
        let image = json!({"media_type": "image/png", "data_base64": "AAAA"});
        let mut result = json!({"borg_attachments": [image.clone(), image.clone()]});
        // Make the second write fail by removing write access to the spool
        // after the first image has landed.
        let first = write_spooled_attachment(spool.path(), &image, 0).expect("seed");
        std::fs::remove_file(&first).expect("clear seed");
        let mut permissions = std::fs::metadata(spool.path())
            .expect("metadata")
            .permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(spool.path(), permissions).expect("lock spool");

        spool_attachments_in(&mut result, spool.path(), 0);

        let mut permissions = std::fs::metadata(spool.path())
            .expect("metadata")
            .permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        permissions.set_readonly(false);
        std::fs::set_permissions(spool.path(), permissions).expect("unlock spool");

        assert_eq!(
            std::fs::read_dir(spool.path()).unwrap().count(),
            0,
            "a spooled file survived the failure"
        );
        assert!(
            result.get("borg_attachments").is_none(),
            "base64 was put back into stdout: {result}"
        );
        assert!(
            result[SPOOL_ERROR_KEY].is_string(),
            "the failure was not reported: {result}"
        );
        assert!(
            first_spool_error(&result, 0).is_some(),
            "the caller cannot detect the failure and would exit 0"
        );
    }

    #[test]
    fn a_result_without_images_is_left_alone() {
        let spool = tempfile::tempdir().expect("spool");
        let mut result = json!({"ok": true, "value": {"lines": 3}});
        let before = result.clone();

        spool_attachments_in(&mut result, spool.path(), 0);

        assert_eq!(result, before);
        assert_eq!(std::fs::read_dir(spool.path()).unwrap().count(), 0);
    }

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
        let tools = response["result"]["tools"].as_array().unwrap();
        let names = tools
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
        assert!(!names.contains(&"await_watchers"));
        assert!(names.contains(&"run_blu_extension"));
        for tool in tools {
            let properties = tool["inputSchema"]["properties"]
                .as_object()
                .expect("agent tool properties");
            assert_eq!(
                properties.keys().next().map(String::as_str),
                Some("action"),
                "{} must present action first",
                tool["name"]
            );
            assert!(
                tool["inputSchema"]["required"]
                    .as_array()
                    .is_some_and(|required| required.iter().all(|field| field != "action")),
                "{} must not reject missing presentation metadata",
                tool["name"]
            );
        }
    }

    #[test]
    fn action_metadata_is_removed_before_tool_dispatch() {
        let mut arguments = json!({
            "action": "edit",
            "path": "src/main.rs",
            "payload": {"action": "domain value"}
        });

        strip_action_metadata(&mut arguments);

        assert_eq!(
            arguments,
            json!({
                "path": "src/main.rs",
                "payload": {"action": "domain value"}
            })
        );
        for metadata in [json!(null), json!(42), json!({"not": "a summary"})] {
            let mut arguments = json!({"path": "src/main.rs", "action": metadata});
            strip_action_metadata(&mut arguments);
            assert_eq!(arguments, json!({"path": "src/main.rs"}));
        }
        let mut arguments = json!({"path": "src/main.rs"});
        strip_action_metadata(&mut arguments);
        assert_eq!(arguments, json!({"path": "src/main.rs"}));
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
