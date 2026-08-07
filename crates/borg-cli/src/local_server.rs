use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use reqwest::StatusCode;
use tokio::process::{Child, Command};
use tokio::time::{Instant, sleep, timeout};
use url::Url;

use crate::agent_config::LocalProviderConfig;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 8000;
const DEFAULT_CONTEXT_TOKENS: u64 = 32_768;
const HEALTH_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);
const HEALTH_STARTUP_TIMEOUT: Duration = Duration::from_secs(90);
const HEALTH_POLL_INTERVAL: Duration = Duration::from_millis(150);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_HEALTH_BODY_BYTES: usize = 4 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommandSpec {
    pub(crate) program: PathBuf,
    pub(crate) args: Vec<OsString>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Endpoint {
    base_url: String,
    health_url: String,
    host: String,
    port: u16,
}

#[derive(Debug, Clone, Default)]
struct EnvironmentOverrides {
    base_url: Option<String>,
    server_bin: Option<PathBuf>,
    host: Option<String>,
    port: Option<u16>,
    auto_start: Option<bool>,
    ctx_size: Option<u64>,
    context_window_tokens: Option<u64>,
    model_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct ResolvedLaunch {
    endpoint: Endpoint,
    server_bin: PathBuf,
    model_path: PathBuf,
    model: String,
    context_window_tokens: u64,
    jinja: bool,
    reasoning_format: Option<String>,
}

#[derive(Debug)]
enum HealthProbe {
    Healthy,
    Loading { status: StatusCode, body: String },
    Incompatible { status: StatusCode, body: String },
    Unavailable(String),
}

/// A Borg-owned local server, or a no-op lease for a compatible server that
/// was already listening before Borg started. The latter carries no child, so
/// dropping it can never terminate an external process.
#[derive(Debug)]
pub(crate) struct LocalServerLease {
    child: Option<Child>,
    pid: Option<u32>,
    model: String,
}

impl LocalServerLease {
    pub(crate) fn model(&self) -> &str {
        &self.model
    }

    pub(crate) async fn shutdown(mut self) -> Result<()> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        let pid = self.pid.or_else(|| child.id());
        if let Some(pid) = pid {
            signal_process_group(pid, Signal::Term);
        }
        #[cfg(not(unix))]
        child
            .start_kill()
            .context("failed to terminate Borg-owned local llama-server")?;
        if child.try_wait()?.is_some() {
            if let Some(pid) = pid {
                signal_process_group(pid, Signal::Kill);
            }
            return Ok(());
        }
        match timeout(SHUTDOWN_TIMEOUT, child.wait()).await {
            Ok(status) => {
                status.context("failed to reap Borg-owned llama-server")?;
                if let Some(pid) = pid {
                    // The process-group leader can exit before one of its
                    // descendants. Do not return while a helper process can
                    // still keep the Borg-owned group alive.
                    signal_process_group(pid, Signal::Kill);
                }
            }
            Err(_) => {
                if let Some(pid) = pid {
                    signal_process_group(pid, Signal::Kill);
                }
                child
                    .wait()
                    .await
                    .context("failed to reap Borg-owned llama-server after SIGKILL")?;
            }
        }
        Ok(())
    }
}

impl Drop for LocalServerLease {
    fn drop(&mut self) {
        let Some(child) = self.child.as_mut() else {
            return;
        };
        if let Some(pid) = self.pid.or_else(|| child.id()) {
            // Drop cannot await. SIGKILL is intentional here: this path is
            // used during startup errors and panic unwinding, where leaving a
            // model process behind is worse than skipping its grace period.
            signal_process_group(pid, Signal::Kill);
        }
        #[cfg(not(unix))]
        {
            let _ = child.start_kill();
        }
    }
}

/// Ensure the configured local endpoint is healthy, starting llama-server only
/// when nothing is listening there. A healthy or loading endpoint is treated
/// as externally managed and is never killed by the returned lease.
pub(crate) async fn ensure(
    config: &LocalProviderConfig,
    cwd: &Path,
    requested_model: Option<&str>,
) -> Result<Option<LocalServerLease>> {
    let environment = read_environment()?;
    if !environment.auto_start.unwrap_or(config.auto_start) {
        return Ok(None);
    }

    let endpoint = resolve_endpoint(config, &environment)?;
    let client = reqwest::Client::builder()
        .timeout(HEALTH_REQUEST_TIMEOUT)
        .build()
        .context("failed to create the local llama-server health client")?;

    match wait_for_health(&client, &endpoint.health_url, HEALTH_STARTUP_TIMEOUT, None).await? {
        HealthProbe::Healthy => {
            set_env_if_missing("BORG_OPENAI_COMPATIBLE_BASE_URL", &endpoint.base_url);
            return Ok(Some(LocalServerLease {
                child: None,
                pid: None,
                model: requested_model.unwrap_or_default().to_string(),
            }));
        }
        HealthProbe::Unavailable(_) => {}
        probe => bail!(health_error(&endpoint.health_url, probe)),
    }

    let launch = resolve_launch(config, cwd, requested_model, environment, endpoint)?;
    let spec = command_spec(&launch);
    let mut command = Command::new(&spec.program);
    command
        .args(&spec.args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(false);
    isolate_process_group(&mut command);
    let child = command.spawn().with_context(|| {
        format!(
            "failed to start local llama-server {}; check local.server_bin and that the binary is executable",
            launch.server_bin.display()
        )
    })?;
    let pid = child.id();
    let mut child = child;
    let health = wait_for_health(
        &client,
        &launch.endpoint.health_url,
        HEALTH_STARTUP_TIMEOUT,
        Some(&mut child),
    )
    .await;
    match health {
        Ok(HealthProbe::Healthy) => {
            set_env_if_missing("BORG_OPENAI_COMPATIBLE_BASE_URL", &launch.endpoint.base_url);
            set_env_if_missing(
                "BORG_OPENAI_COMPATIBLE_CONTEXT_WINDOW_TOKENS",
                &launch.context_window_tokens.to_string(),
            );
            Ok(Some(LocalServerLease {
                child: Some(child),
                pid,
                model: launch.model,
            }))
        }
        Ok(other) => {
            terminate_child_now(&mut child, pid);
            bail!(health_error(&launch.endpoint.health_url, other));
        }
        Err(error) => {
            terminate_child_now(&mut child, pid);
            Err(error)
        }
    }
}

fn read_environment() -> Result<EnvironmentOverrides> {
    Ok(EnvironmentOverrides {
        base_url: nonempty_env("BORG_OPENAI_COMPATIBLE_BASE_URL"),
        server_bin: nonempty_env("BORG_LOCAL_SERVER_BIN").map(PathBuf::from),
        host: nonempty_env("BORG_LOCAL_HOST"),
        port: parse_env("BORG_LOCAL_PORT")?,
        auto_start: parse_bool_env("BORG_LOCAL_AUTO_START")?,
        ctx_size: parse_env("BORG_LOCAL_CTX_SIZE")?,
        context_window_tokens: parse_env("BORG_OPENAI_COMPATIBLE_CONTEXT_WINDOW_TOKENS")?,
        model_path: nonempty_env("BORG_LOCAL_MODEL_PATH").map(PathBuf::from),
    })
}

fn resolve_endpoint(
    config: &LocalProviderConfig,
    environment: &EnvironmentOverrides,
) -> Result<Endpoint> {
    let configured_base = environment
        .base_url
        .as_deref()
        .or(config.base_url.as_deref());
    let mut url = if let Some(base) = configured_base {
        Url::parse(base.trim()).with_context(|| format!("invalid local base URL {base}"))?
    } else {
        let host = environment
            .host
            .as_deref()
            .or(config.host.as_deref())
            .unwrap_or(DEFAULT_HOST);
        let port = environment.port.or(config.port).unwrap_or(DEFAULT_PORT);
        let mut url = Url::parse("http://127.0.0.1/v1")?;
        url.set_host(Some(host))
            .map_err(|_| anyhow::anyhow!("invalid local.host {host}"))?;
        url.set_port(Some(port))
            .map_err(|_| anyhow::anyhow!("invalid local.port {port}"))?;
        url
    };
    ensure!(
        matches!(url.scheme(), "http" | "https"),
        "local server URL must use http:// or https://"
    );
    let host = url
        .host_str()
        .context("local server URL must contain a host")?
        .to_string();
    let port = url
        .port_or_known_default()
        .context("local server URL must contain a port for an owned server")?;
    url.set_query(None);
    url.set_fragment(None);
    let base_url = url.as_str().trim_end_matches('/').to_string();
    let health_url = format!("{base_url}/health");
    Ok(Endpoint {
        base_url,
        health_url,
        host,
        port,
    })
}

fn resolve_launch(
    config: &LocalProviderConfig,
    cwd: &Path,
    requested_model: Option<&str>,
    environment: EnvironmentOverrides,
    endpoint: Endpoint,
) -> Result<ResolvedLaunch> {
    ensure!(
        endpoint.base_url.starts_with("http://"),
        "an owned llama-server requires an http:// local endpoint"
    );
    let model_path = environment
        .model_path
        .clone()
        .or_else(|| config.model_path.clone())
        .or_else(|| {
            requested_model
                .filter(|model| looks_like_model_path(model))
                .map(PathBuf::from)
        });
    let model_path = match model_path {
        Some(path) => resolve_path(cwd, path),
        None => discovered_model_path(config, cwd, requested_model)?.context(
            "local.auto_start is enabled but no GGUF model was selected; set local.model_path or select a discovered model",
        )?,
    };
    ensure!(
        model_path.is_file(),
        "local GGUF model does not exist: {}",
        model_path.display()
    );
    let has_gguf_suffix = model_path
        .extension()
        .and_then(OsStr::to_str)
        .is_some_and(|extension| extension.eq_ignore_ascii_case("gguf"));
    ensure!(
        has_gguf_suffix || borg_provider::parse_gguf_file(&model_path).is_ok(),
        "local model must be a GGUF file: {}",
        model_path.display()
    );

    let model = requested_model
        .filter(|model| !looks_like_model_path(model))
        .map(str::to_string)
        .or_else(|| {
            model_path
                .file_stem()
                .and_then(OsStr::to_str)
                .map(str::to_string)
        })
        .context("local GGUF model has no usable model name")?;
    let server_bin = environment
        .server_bin
        .clone()
        .or_else(|| config.server_bin.clone())
        .map(|path| {
            if path.is_absolute() || path.components().count() > 1 {
                resolve_path(cwd, path)
            } else {
                path
            }
        })
        .unwrap_or_else(default_server_bin);
    if (server_bin.is_absolute() || server_bin.components().count() > 1) && !server_bin.is_file() {
        bail!(
            "configured local.server_bin does not exist or is not a file: {}",
            server_bin.display()
        );
    }
    let context_window_tokens = resolved_context(config, &environment);
    ensure!(
        context_window_tokens > 0,
        "local context size must be positive"
    );
    Ok(ResolvedLaunch {
        endpoint,
        server_bin,
        model_path,
        model,
        context_window_tokens,
        jinja: config.jinja,
        reasoning_format: config.reasoning_format.clone(),
    })
}

fn resolved_context(config: &LocalProviderConfig, environment: &EnvironmentOverrides) -> u64 {
    environment
        .context_window_tokens
        .or(environment.ctx_size)
        .or(config.ctx_size)
        .or(config.context_window_tokens)
        .unwrap_or(DEFAULT_CONTEXT_TOKENS)
}

fn command_spec(launch: &ResolvedLaunch) -> CommandSpec {
    let mut args = vec![
        OsString::from("--host"),
        OsString::from(&launch.endpoint.host),
        OsString::from("--port"),
        OsString::from(launch.endpoint.port.to_string()),
        OsString::from("--model"),
        launch.model_path.as_os_str().to_os_string(),
        OsString::from("--alias"),
        OsString::from(&launch.model),
        OsString::from("--ctx-size"),
        OsString::from(launch.context_window_tokens.to_string()),
    ];
    if launch.jinja {
        args.push(OsString::from("--jinja"));
    } else {
        args.push(OsString::from("--no-jinja"));
    }
    if let Some(reasoning_format) = launch.reasoning_format.as_deref() {
        args.push(OsString::from("--reasoning-format"));
        args.push(OsString::from(reasoning_format));
    }
    CommandSpec {
        program: launch.server_bin.clone(),
        args,
    }
}

async fn wait_for_health(
    client: &reqwest::Client,
    health_url: &str,
    budget: Duration,
    mut child: Option<&mut Child>,
) -> Result<HealthProbe> {
    let deadline = Instant::now() + budget;
    loop {
        if let Some(child) = child.as_deref_mut()
            && let Some(status) = child.try_wait()?
        {
            bail!("local llama-server exited before /v1/health became ready (status {status})");
        }
        match probe_health(client, health_url).await {
            HealthProbe::Healthy => return Ok(HealthProbe::Healthy),
            HealthProbe::Unavailable(error) if child.is_none() => {
                return Ok(HealthProbe::Unavailable(error));
            }
            HealthProbe::Incompatible { status, body } => {
                return Ok(HealthProbe::Incompatible { status, body });
            }
            HealthProbe::Loading { status, body } => {
                if Instant::now() >= deadline {
                    return Ok(HealthProbe::Loading { status, body });
                }
            }
            HealthProbe::Unavailable(_) => {
                if Instant::now() >= deadline {
                    return Ok(HealthProbe::Unavailable(
                        "the owned server did not begin listening before the startup timeout"
                            .to_string(),
                    ));
                }
            }
        }
        sleep(HEALTH_POLL_INTERVAL).await;
    }
}

async fn probe_health(client: &reqwest::Client, health_url: &str) -> HealthProbe {
    let response = match client.get(health_url).send().await {
        Ok(response) => response,
        Err(error) => return HealthProbe::Unavailable(error.to_string()),
    };
    let status = response.status();
    let body = match response.bytes().await {
        Ok(bytes) => {
            String::from_utf8_lossy(&bytes[..bytes.len().min(MAX_HEALTH_BODY_BYTES)]).to_string()
        }
        Err(error) => format!("failed to read health response: {error}"),
    };
    if status.is_success() {
        HealthProbe::Healthy
    } else if status.is_server_error() {
        HealthProbe::Loading { status, body }
    } else {
        HealthProbe::Incompatible { status, body }
    }
}

fn health_error(health_url: &str, probe: HealthProbe) -> String {
    match probe {
        HealthProbe::Loading { status, body } => format!(
            "local llama-server at {health_url} did not become healthy before the startup timeout (HTTP {status}: {})",
            display_body(&body)
        ),
        HealthProbe::Incompatible { status, body } => format!(
            "endpoint {health_url} is listening but is not a compatible llama-server (/v1/health returned HTTP {status}: {})",
            display_body(&body)
        ),
        HealthProbe::Unavailable(error) => {
            format!("local llama-server at {health_url} did not become reachable: {error}")
        }
        HealthProbe::Healthy => "local llama-server is healthy".to_string(),
    }
}

fn display_body(body: &str) -> &str {
    let body = body.trim();
    if body.is_empty() {
        "empty response"
    } else {
        body
    }
}

fn resolve_path(cwd: &Path, path: PathBuf) -> PathBuf {
    let path = path.to_string_lossy();
    let path = if let Some(home_relative) = path.strip_prefix("~/") {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .map(|home| home.join(home_relative))
            .unwrap_or_else(|| PathBuf::from(path.as_ref()))
    } else {
        PathBuf::from(path.as_ref())
    };
    if path.is_absolute() {
        path
    } else {
        cwd.join(path)
    }
}

/// Resolve a picker id such as `dir:qwen-q4_k_m` or `ollama:qwen3.6:35b-a3b`
/// back to the GGUF path that the owned server must load. The picker is
/// intentionally allowed to expose stable ids instead of filesystem paths;
/// this is the runtime-side half of that contract.
fn discovered_model_path(
    config: &LocalProviderConfig,
    cwd: &Path,
    requested_model: Option<&str>,
) -> Result<Option<PathBuf>> {
    let Some(requested_model) = requested_model.filter(|model| !looks_like_model_path(model))
    else {
        return Ok(None);
    };
    let home = std::env::var_os("HOME").map(PathBuf::from);
    let discovery = borg_provider::LocalModelDiscoveryConfig {
        model_dirs: config
            .model_dirs
            .iter()
            .cloned()
            .map(|path| resolve_path(cwd, path))
            .collect(),
        include_ollama_store: config.include_ollama_store,
        ollama_models_dir: home.as_deref().map(|home| home.join(".ollama/models")),
        include_hf_cache: config.include_hf_cache,
        hf_cache_dir: home.map(|home| home.join(".cache/huggingface/hub")),
    };
    let models = borg_provider::discover_models(&discovery)
        .map_err(|error| anyhow::anyhow!(error))
        .context("failed to resolve the selected local model")?;
    Ok(models
        .into_iter()
        .find(|model| model.id == requested_model)
        .map(|model| model.path))
}

fn looks_like_model_path(model: &str) -> bool {
    let path = Path::new(model);
    path.extension()
        .and_then(OsStr::to_str)
        .is_some_and(|extension| extension.eq_ignore_ascii_case("gguf"))
        || path.components().count() > 1
        || path.is_absolute()
}

fn default_server_bin() -> PathBuf {
    let packaged = PathBuf::from("/usr/lib/ollama/llama-server");
    if packaged.is_file() {
        packaged
    } else {
        PathBuf::from("llama-server")
    }
}

fn nonempty_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn parse_env<T>(key: &str) -> Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let Some(value) = nonempty_env(key) else {
        return Ok(None);
    };
    value
        .parse::<T>()
        .map(Some)
        .map_err(|error| anyhow::anyhow!("{key} is invalid: {error}"))
}

fn parse_bool_env(key: &str) -> Result<Option<bool>> {
    let Some(value) = nonempty_env(key) else {
        return Ok(None);
    };
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(Some(true)),
        "0" | "false" | "no" | "off" => Ok(Some(false)),
        _ => bail!("{key} must be true/false, got {value}"),
    }
}

fn set_env_if_missing(key: &str, value: &str) {
    if std::env::var_os(key).is_none() {
        // SAFETY: local server setup runs before Borg starts its session actor
        // and provider worker tasks. Existing values are explicit overrides.
        unsafe { std::env::set_var(key, value) };
    }
}

#[derive(Clone, Copy)]
enum Signal {
    Term,
    Kill,
}

impl Signal {
    #[cfg(unix)]
    const fn number(self) -> i32 {
        match self {
            Self::Term => 15,
            Self::Kill => 9,
        }
    }
}

fn signal_process_group(pid: u32, signal: Signal) {
    #[cfg(unix)]
    {
        // SAFETY: the child is spawned as its own process-group leader, so its
        // PID is the process-group ID. A failed signal is harmless during
        // normal reaping and cannot affect an unrelated process group.
        unsafe {
            killpg(pid as i32, signal.number());
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (pid, signal);
    }
}

fn terminate_child_now(child: &mut Child, pid: Option<u32>) {
    if let Some(pid) = pid.or_else(|| child.id()) {
        signal_process_group(pid, Signal::Kill);
    }
    #[cfg(not(unix))]
    {
        let _ = child.start_kill();
    }
}

fn isolate_process_group(command: &mut Command) {
    #[cfg(unix)]
    {
        command.process_group(0);
    }
    #[cfg(not(unix))]
    {
        let _ = command;
    }
}

#[cfg(unix)]
unsafe extern "C" {
    fn killpg(pgid: i32, signal: i32) -> i32;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn launch_fixture() -> ResolvedLaunch {
        ResolvedLaunch {
            endpoint: Endpoint {
                base_url: "http://127.0.0.1:8000/v1".to_string(),
                health_url: "http://127.0.0.1:8000/v1/health".to_string(),
                host: "127.0.0.1".to_string(),
                port: 8000,
            },
            server_bin: PathBuf::from("llama-server"),
            model_path: PathBuf::from("/models/qwen.gguf"),
            model: "qwen3.6:35b-a3b".to_string(),
            context_window_tokens: 32_768,
            jinja: true,
            reasoning_format: Some("deepseek".to_string()),
        }
    }

    #[test]
    fn command_contains_openai_server_contract() {
        let spec = command_spec(&launch_fixture());
        let args = spec
            .args
            .iter()
            .map(|argument| argument.to_string_lossy())
            .collect::<Vec<_>>();
        assert!(args.windows(2).any(|pair| pair == ["--host", "127.0.0.1"]));
        assert!(args.windows(2).any(|pair| pair == ["--port", "8000"]));
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--model", "/models/qwen.gguf"])
        );
        assert!(args.windows(2).any(|pair| pair == ["--ctx-size", "32768"]));
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--reasoning-format", "deepseek"])
        );
        assert!(args.iter().any(|arg| arg == "--jinja"));
    }

    #[test]
    fn context_precedence_preserves_explicit_environment_values() {
        let config = LocalProviderConfig {
            ctx_size: Some(16_384),
            context_window_tokens: Some(8_192),
            ..LocalProviderConfig::default()
        };
        let environment = EnvironmentOverrides {
            ctx_size: Some(65_536),
            context_window_tokens: Some(32_768),
            ..EnvironmentOverrides::default()
        };
        assert_eq!(resolved_context(&config, &environment), 32_768);
    }

    #[tokio::test]
    async fn health_probe_accepts_healthy_server() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address: SocketAddr = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("connection");
            let mut request = [0_u8; 1024];
            let _ = socket.read(&mut request).await.expect("request");
            socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                .await
                .expect("response");
        });
        let client = reqwest::Client::builder()
            .timeout(HEALTH_REQUEST_TIMEOUT)
            .build()
            .expect("client");
        assert!(matches!(
            probe_health(&client, &format!("http://{address}/v1/health")).await,
            HealthProbe::Healthy
        ));
        server.await.expect("server");
    }

    #[test]
    fn incompatible_health_error_explains_ownership_boundary() {
        let message = health_error(
            "http://127.0.0.1:8000/v1/health",
            HealthProbe::Incompatible {
                status: StatusCode::NOT_FOUND,
                body: "not found".to_string(),
            },
        );
        assert!(message.contains("not a compatible llama-server"));
        assert!(message.contains("HTTP 404"));
        assert!(message.contains("not found"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn owned_lease_terminates_its_process_group() {
        let mut command = Command::new("sh");
        command.args(["-c", "sleep 30"]);
        isolate_process_group(&mut command);
        let child = command.spawn().expect("child");
        let pid = child.id().expect("child pid");
        let lease = LocalServerLease {
            child: Some(child),
            pid: Some(pid),
            model: "test".to_string(),
        };
        lease.shutdown().await.expect("shutdown");
        timeout(Duration::from_secs(1), async {
            while unsafe { killpg(pid as i32, 0) } == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("process group should be reaped after shutdown");
    }

    #[test]
    fn path_detection_distinguishes_model_ids() {
        assert!(!looks_like_model_path("qwen3.6:35b-a3b"));
        assert!(looks_like_model_path("models/qwen.gguf"));
        assert!(looks_like_model_path("/models/qwen.gguf"));
    }
}
