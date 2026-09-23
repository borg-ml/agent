//! Persistent, supervised services; independent of requesting Borg sessions.

use anyhow::{Context, bail, ensure};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    net::{IpAddr, SocketAddr},
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UnixListener, UnixStream},
    process::{Child, Command},
    sync::RwLock,
};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::lanes::{
    Access, AdmissionBudget, Holder, Hook, LaneStore, Lease, LeaseRequest, ResourceRequest,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthCheck {
    pub argv: Vec<String>,
    #[serde(default)]
    pub kind: HealthKind,
    pub interval_ms: u64,
    pub timeout_ms: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthKind {
    #[default]
    Command,
    Http,
    McpInitialize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RestartPolicy {
    pub max_restarts: u32,
    pub backoff_ms: u64,
    pub debounce_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Endpoint {
    pub listen: String,
    pub backend_ports: [u16; 2],
}

/// Client ownership policy. This does not authorize unfenced backend mutations.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ClientMode {
    /// One owner at a time; safe default for editor-like services.
    #[default]
    Exclusive,
    /// Independent owner leases up to a fixed concurrent limit.
    Shared { max_clients: usize },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceSpec {
    pub id: String,
    pub argv: Vec<String>,
    pub cwd: PathBuf,
    pub env: Vec<(String, String)>,
    pub resources: Vec<ResourceRequest>,
    pub memory_max_bytes: Option<u64>,
    pub admission: AdmissionBudget,
    pub health: HealthCheck,
    pub restart: RestartPolicy,
    pub endpoint: Option<Endpoint>,
    pub restore: Option<Hook>,
    #[serde(default)]
    pub client_mode: ClientMode,
    #[serde(default = "default_ready_timeout_ms")]
    pub readiness_timeout_ms: u64,
    #[serde(default)]
    pub graceful_stop: Option<Hook>,
    #[serde(default)]
    pub idle: Option<Hook>,
    #[serde(default)]
    pub active: Option<Hook>,
    /// Adapter explicitly enforces owner+generation for *every* mutating request.
    #[serde(default)]
    pub adapter_enforces_leases: bool,
    /// Only explicitly audited, exact GET/HEAD paths can bypass adapter fencing.
    /// No path is forwarded by default (in particular not Unreal MCP paths).
    #[serde(default)]
    pub read_only_paths: Vec<String>,
    #[serde(default = "default_idle_after_ms")]
    pub idle_after_ms: u64,
}
fn default_ready_timeout_ms() -> u64 {
    120_000
}

/// The Degraded reason when a launched backend missed its readiness window.
pub(crate) const READINESS_FAILED: &str = "backend failed readiness";

/// How long a resumed service may take to report Healthy: its own
/// `readiness_timeout_ms` plus one health interval and probe timeout, and 2 s
/// for the supervisor to launch the backend. Without a readable spec, the
/// default readiness window applies.
pub(crate) fn resume_budget(services: &Path, id: &str) -> Duration {
    #[derive(Deserialize)]
    struct Health {
        interval_ms: u64,
        timeout_ms: u64,
    }
    #[derive(Deserialize)]
    struct Budget {
        #[serde(default = "default_ready_timeout_ms")]
        readiness_timeout_ms: u64,
        health: Health,
    }
    let valid = !id.is_empty()
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    let window = valid
        .then(|| fs::read(services.join(id).join("spec.json")).ok())
        .flatten()
        .and_then(|bytes| serde_json::from_slice::<Budget>(&bytes).ok())
        .map_or(default_ready_timeout_ms(), |spec| {
            spec.readiness_timeout_ms
                .saturating_add(spec.health.interval_ms)
                .saturating_add(spec.health.timeout_ms)
        });
    Duration::from_millis(window.saturating_add(2_000))
}
fn default_idle_after_ms() -> u64 {
    60_000
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ServiceState {
    Stopped,
    Starting,
    Healthy { backend: Option<u16> },
    Degraded { reason: String },
    Yielding,
    Yielded,
    RestartPending,
    Restarting,
    Failed { reason: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientLease {
    pub id: Uuid,
    pub service_id: String,
    pub owner: Holder,
    pub expires_at_unix_ms: u64,
    #[serde(default)]
    pub purpose: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceStatus {
    pub id: String,
    pub state: ServiceState,
    pub endpoint: Option<Endpoint>,
    pub clients: Vec<ClientLease>,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub backend_pid: Option<u32>,
    #[serde(default)]
    pub supervisor_pid: Option<u32>,
    #[serde(default)]
    pub restarts: u32,
    #[serde(default)]
    pub yields: BTreeMap<String, YieldWindow>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct YieldWindow {
    pub by: String,
    pub reason: String,
    pub expires_at_unix_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ServiceRequest {
    Stop,
    Restart {
        reason: String,
        force: bool,
    },
    Yield {
        by: String,
        reason: String,
        ttl_ms: u64,
    },
    Resume {
        by: String,
    },
    Lease {
        owner: Holder,
        purpose: String,
        ttl_ms: u64,
    },
    Release {
        lease_id: Uuid,
        owner: Holder,
    },
    Touch,
}

#[async_trait]
pub trait ServiceCoordinator: Send + Sync {
    async fn start(&self, spec: ServiceSpec) -> Result<ServiceStatus>;
    async fn status(&self, id: &str) -> Result<ServiceStatus>;
    async fn acquire(&self, id: &str, owner: Holder, ttl_ms: u64) -> Result<ClientLease>;
    async fn release(&self, lease: &ClientLease) -> Result<()>;
    async fn trigger_restart(&self, id: &str, reason: &str) -> Result<()>;
    async fn yield_for(&self, id: &str, resources: &[ResourceRequest]) -> Result<()>;
    async fn resume(&self, id: &str) -> Result<ServiceStatus>;
    async fn stop(&self, id: &str) -> Result<()>;
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// The runtime root can be private to a test, user, or installation. IDs never become paths unchecked.
pub fn service_root() -> PathBuf {
    crate::lanes::LaneStore::default_root().join("services")
}

fn service_dir(root: &Path, id: &str) -> Result<PathBuf> {
    ensure!(
        !id.is_empty()
            && id.len() <= 100
            && id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "invalid service id"
    );
    let dir = root.join(id);
    fs::create_dir_all(&dir)?;
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
    Ok(dir)
}
fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let temporary = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(&serde_json::to_vec(value)?)?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    Ok(())
}
fn read_json<T: for<'a> Deserialize<'a>>(path: &Path) -> Result<T> {
    serde_json::from_slice(&fs::read(path)?)
        .with_context(|| format!("invalid JSON: {}", path.display()))
}
fn lock(path: &Path, nonblocking: bool) -> Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)?;
    let mode = libc::LOCK_EX | if nonblocking { libc::LOCK_NB } else { 0 };
    ensure!(
        unsafe { libc::flock(file.as_raw_fd(), mode) } == 0,
        "service already running"
    );
    Ok(file)
}
fn valid_spec(spec: &ServiceSpec) -> Result<()> {
    ensure!(
        !spec.id.is_empty()
            && spec.id.len() <= 100
            && spec
                .id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "invalid service id"
    );
    ensure!(
        !spec.argv.is_empty() && !spec.argv[0].is_empty() && spec.cwd.is_dir(),
        "service needs argv and existing cwd"
    );
    ensure!(
        !spec.resources.is_empty(),
        "service must bind at least one lane resource"
    );
    for resource in &spec.resources {
        resource.key.validate_canonical()?;
    }
    if let ClientMode::Shared { max_clients } = spec.client_mode {
        ensure!(
            max_clients > 0,
            "shared service max_clients must be positive"
        );
    }
    ensure!(
        spec.health.timeout_ms > 0 && spec.health.interval_ms > 0 && spec.readiness_timeout_ms > 0,
        "service health timeouts must be positive"
    );
    if let Some(endpoint) = &spec.endpoint {
        let addr: SocketAddr = endpoint
            .listen
            .parse()
            .context("invalid endpoint listen address")?;
        ensure!(
            addr.ip().is_loopback() && addr.port() != 0,
            "service front endpoint must be loopback"
        );
        ensure!(
            endpoint.backend_ports[0] != endpoint.backend_ports[1]
                && endpoint
                    .backend_ports
                    .iter()
                    .all(|p| *p != 0 && *p != addr.port()),
            "backend ports must be distinct and not front port"
        );
    } else {
        ensure!(
            matches!(spec.health.kind, HealthKind::Command),
            "HTTP/MCP probe needs endpoint"
        );
    }
    ensure!(
        spec.read_only_paths.iter().all(|path| path.starts_with('/')
            && path.len() <= 200
            && path
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"/-_.".contains(&c))),
        "read-only paths must be exact audited paths without query or fragment"
    );
    if matches!(spec.health.kind, HealthKind::Command) {
        ensure!(
            !spec.health.argv.is_empty(),
            "command health probe needs argv"
        );
    }
    Ok(())
}
fn stopped(id: &str) -> ServiceStatus {
    ServiceStatus {
        id: id.into(),
        state: ServiceState::Stopped,
        endpoint: None,
        clients: vec![],
        reason: "not started".into(),
        supervisor_pid: None,
        backend_pid: None,
        restarts: 0,
        yields: BTreeMap::new(),
    }
}
/// `dir/control.sock` addressed through the directory's descriptor, so a deep
/// lane root or long service id cannot exceed the 108-byte `sun_path` limit.
/// Keep the returned `File` open while the path is used.
fn control_socket(dir: &Path) -> Result<(File, PathBuf)> {
    let dir = File::open(dir)?;
    let path = PathBuf::from(format!("/proc/self/fd/{}/control.sock", dir.as_raw_fd()));
    Ok((dir, path))
}

fn is_running(dir: &Path) -> Result<bool> {
    use std::os::unix::fs::OpenOptionsExt;
    let fd = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(dir.join("supervisor.lock"))?;
    if unsafe { libc::flock(fd.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        return Ok(false);
    }
    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EWOULDBLOCK) => Ok(true),
        _ => Err(error.into()),
    }
}

/// Client-side command transport. Requests are atomically published and answered by the
/// supervisor; all lease/yield mutations are serial, never read/modify/write races in clients.
pub async fn service_request(
    root: &Path,
    id: &str,
    request: ServiceRequest,
    timeout: Duration,
) -> Result<ServiceStatus> {
    let dir = service_dir(root, id)?;
    ensure!(is_running(&dir)?, "service {id} is not running");
    let (_dir, socket) = control_socket(&dir)?;
    let mut stream = tokio::time::timeout(timeout, UnixStream::connect(&socket)).await??;
    let body = serde_json::to_vec(&request)?;
    ensure!(body.len() <= 8192, "service request too large");
    stream.write_all(&body).await?;
    stream.shutdown().await?;
    let mut response = Vec::new();
    tokio::time::timeout(timeout, stream.take(1_048_576).read_to_end(&mut response)).await??;
    let value: serde_json::Value =
        serde_json::from_slice(&response).context("invalid supervisor reply")?;
    if let Some(error) = value.get("error").and_then(|v| v.as_str()) {
        bail!("{error}");
    }
    serde_json::from_value(value).context("invalid service status")
}

pub struct ServiceManager {
    pub root: PathBuf,
    pub executable: PathBuf,
}
impl ServiceManager {
    pub fn new(root: PathBuf, executable: PathBuf) -> Self {
        Self { root, executable }
    }
    pub fn current() -> Result<Self> {
        Ok(Self::new(service_root(), std::env::current_exe()?))
    }
    /// The launched supervisor unit has already exited, so its service can
    /// never become ready.
    pub fn supervisor_exited(&self, id: &str) -> Result<Option<String>> {
        let dir = service_dir(&self.root, id)?;
        if is_running(&dir)? {
            return Ok(None);
        }
        let unit = format!("{}.service", read_json::<String>(&dir.join("unit.json"))?);
        let active = std::process::Command::new("systemctl")
            .args(["--user", "is-active", "--quiet", &unit])
            .status()
            .is_ok_and(|s| s.success());
        Ok((!active).then_some(unit))
    }
    pub fn read_status(&self, id: &str) -> Result<ServiceStatus> {
        let dir = service_dir(&self.root, id)?;
        let mut status: ServiceStatus =
            read_json(&dir.join("state.json")).unwrap_or_else(|_| stopped(id));
        if !is_running(&dir)? {
            status.state = ServiceState::Stopped;
            status.reason = "supervisor not running".into();
            status.backend_pid = None;
        }
        let lane = LaneStore::new(
            self.root
                .parent()
                .context("service root has no lane parent")?,
        )?;
        if let Some(waiting) = lane.service_preemption_notice(id)? {
            status.reason = format!("{}; {waiting}", status.reason);
        }
        Ok(status)
    }
    pub async fn send(
        &self,
        id: &str,
        request: ServiceRequest,
        timeout: Duration,
    ) -> Result<ServiceStatus> {
        service_request(&self.root, id, request, timeout).await
    }
    pub async fn launch(&self, spec: ServiceSpec) -> Result<ServiceStatus> {
        valid_spec(&spec)?;
        let dir = service_dir(&self.root, &spec.id)?;
        let _start_lock = lock(&dir.join("start.lock"), false)?;
        if is_running(&dir)? {
            return self.read_status(&spec.id);
        }
        use std::os::unix::fs::OpenOptionsExt;
        let _log = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(dir.join("output.log"))?;
        let args = ["lane", "service", "supervise", &spec.id];
        let has_systemd = std::process::Command::new("systemctl")
            .args(["--user", "show-environment"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success());
        if has_systemd {
            write_json(&dir.join("spec.json"), &spec)?;
            let unit = format!("borg-service-{}-{}", spec.id, Uuid::new_v4().simple());
            let mut command = std::process::Command::new("systemd-run");
            command.args([
                "--user",
                "--collect",
                "--quiet",
                &format!("--unit={unit}"),
                &format!("--working-directory={}", spec.cwd.display()),
                "-p",
                "KillMode=control-group",
                "-p",
                "Delegate=yes",
            ]);
            command.arg(format!("--setenv=BORG_SERVICE_UNIT={unit}.service"));
            if let Some(bytes) = spec.memory_max_bytes {
                command.args(["-p", &format!("MemoryMax={bytes}")]);
            }
            let lane_root = self.root.parent().context("invalid root")?.display();
            command.arg(format!("--setenv=BORG_LANE_DIR={lane_root}"));
            command.arg(format!("--setenv=BORG_LANES_ROOT={lane_root}"));
            command.arg(&self.executable).args(args);
            ensure!(
                command.status().context("systemd-run failed")?.success(),
                "could not launch service user unit"
            );
            write_json(&dir.join("unit.json"), &unit)?;
        } else {
            bail!(
                "systemd user manager is unavailable; refusing unsafe process-tree recovery without a proven scoped equivalent"
            );
        }
        self.read_status(&spec.id)
    }
}

fn expand(value: &str, port: Option<u16>, owner: Option<&str>) -> String {
    value
        .replace("{port}", &port.unwrap_or(0).to_string())
        .replace("{owner}", owner.unwrap_or(""))
}
fn command(
    argv: &[String],
    spec: &ServiceSpec,
    port: Option<u16>,
    owner: Option<&str>,
) -> Result<Command> {
    let (program, args) = argv
        .split_first()
        .context("hook/service command argv is empty")?;
    let mut cmd = Command::new(expand(program, port, owner));
    cmd.args(args.iter().map(|arg| expand(arg, port, owner)))
        .current_dir(&spec.cwd);
    for (key, value) in &spec.env {
        cmd.env(key, expand(value, port, owner));
    }
    if let Some(port) = port {
        cmd.env("BORG_SERVICE_BACKEND_PORT", port.to_string());
    }
    if let Some(owner) = owner {
        cmd.env("BORG_SERVICE_OWNER", owner);
    }
    Ok(cmd)
}
async fn hook(
    hook: &Hook,
    spec: &ServiceSpec,
    port: Option<u16>,
    owner: Option<&str>,
) -> Result<()> {
    let mut cmd = command(&hook.argv, spec, port, owner)?;
    cmd.kill_on_drop(true)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let status = tokio::time::timeout(Duration::from_millis(hook.timeout_ms.max(1)), cmd.status())
        .await
        .context("service hook timed out")??;
    ensure!(status.success(), "service hook failed: {status}");
    Ok(())
}
async fn probe(spec: &ServiceSpec, port: Option<u16>) -> bool {
    let duration = Duration::from_millis(spec.health.timeout_ms);
    tokio::time::timeout(duration, async {
        match spec.health.kind {
            HealthKind::Command => {
                let mut cmd = command(&spec.health.argv, spec, port, None)?;
                cmd.kill_on_drop(true).stdout(Stdio::null()).stderr(Stdio::null());
                Ok(cmd.status().await?.success())
            }
            HealthKind::Http | HealthKind::McpInitialize => {
                let port = port.context("missing health backend port")?;
                let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;
                let path = spec.health.argv.first().map(String::as_str).unwrap_or("/");
                ensure!(path.starts_with('/') && !path.contains(['\r', '\n']), "invalid health path");
                let mcp = matches!(spec.health.kind, HealthKind::McpInitialize);
                let body = if mcp {
                    r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"borg-services","version":"1"}}}"#
                } else { "" };
                let method = if body.is_empty() { "GET" } else { "POST" };
                stream.write_all(format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await?;
                let mut reply = HealthReply::head(&mut stream).await?;
                if !(200..300).contains(&reply.status) || !mcp {
                    return Ok((200..300).contains(&reply.status));
                }
                reply.read_body(&mut stream).await?;
                let healthy = reply.mcp_initialized();
                if healthy && let Some(session) = reply.header("mcp-session-id") {
                    end_mcp_session(port, path, session.to_owned());
                }
                Ok(healthy)
            }
        }
    }).await.is_ok_and(|result: Result<bool>| result.unwrap_or(false))
}

/// One HTTP/1.x health reply, bounded by `HEALTH_REPLY_LIMIT` in total.
struct HealthReply {
    status: u16,
    headers: Vec<(String, String)>,
    /// Bytes read past the header block; the whole body after `read_body`.
    body: Vec<u8>,
    budget: usize,
}

const HEALTH_REPLY_LIMIT: usize = 16_384;

impl HealthReply {
    async fn head(stream: &mut TcpStream) -> Result<Self> {
        let mut raw = Vec::new();
        let end = loop {
            if let Some(end) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                break end;
            }
            ensure!(
                read_some(stream, &mut raw, HEALTH_REPLY_LIMIT).await?,
                "health response ended before its headers"
            );
        };
        let head = std::str::from_utf8(&raw[..end]).context("health response head is not UTF-8")?;
        let mut lines = head.split("\r\n");
        let mut status = lines.next().unwrap_or_default().split_whitespace();
        ensure!(
            status.next().is_some_and(|v| v.starts_with("HTTP/1.")),
            "health response is not HTTP/1.x"
        );
        let status = status
            .next()
            .context("health response has no status")?
            .parse()?;
        let headers = lines
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_owned()))
            .collect();
        Ok(Self {
            status,
            headers,
            body: raw.split_off(end + 4),
            budget: HEALTH_REPLY_LIMIT - (end + 4),
        })
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    /// The complete body: chunked, Content-Length, or else until EOF. A
    /// keep-alive server that sends Content-Length never forces a timeout.
    async fn read_body(&mut self, stream: &mut TcpStream) -> Result<()> {
        let mut raw = std::mem::take(&mut self.body);
        if self
            .header("transfer-encoding")
            .is_some_and(|value| value.to_ascii_lowercase().contains("chunked"))
        {
            let mut body = Vec::new();
            let mut at = 0;
            loop {
                let line = loop {
                    if let Some(line) = raw[at..].windows(2).position(|w| w == b"\r\n") {
                        break at + line;
                    }
                    ensure!(
                        read_some(stream, &mut raw, self.budget).await?,
                        "chunked health body truncated"
                    );
                };
                let size = std::str::from_utf8(&raw[at..line])?;
                let size =
                    usize::from_str_radix(size.split(';').next().unwrap_or_default().trim(), 16)
                        .context("invalid chunk size")?;
                ensure!(size <= self.budget, "health response too large");
                at = line + 2;
                if size == 0 {
                    break;
                }
                while raw.len() < at + size + 2 {
                    ensure!(
                        read_some(stream, &mut raw, self.budget).await?,
                        "chunked health body truncated"
                    );
                }
                ensure!(
                    &raw[at + size..at + size + 2] == b"\r\n",
                    "malformed health chunk"
                );
                body.extend_from_slice(&raw[at..at + size]);
                at += size + 2;
            }
            self.body = body;
        } else if let Some(length) = self.header("content-length") {
            let length: usize = length.parse().context("invalid health Content-Length")?;
            ensure!(length <= self.budget, "health response too large");
            while raw.len() < length {
                ensure!(
                    read_some(stream, &mut raw, self.budget).await?,
                    "health body truncated"
                );
            }
            raw.truncate(length);
            self.body = raw;
        } else {
            while read_some(stream, &mut raw, self.budget).await? {}
            self.body = raw;
        }
        Ok(())
    }

    /// A JSON-RPC `error` is unhealthy; `result.protocolVersion` is healthy.
    /// JSON is one value however it is formatted; an event stream is split
    /// into events whose `data:` lines are joined before parsing.
    fn mcp_initialized(&self) -> bool {
        let text = String::from_utf8_lossy(&self.body);
        let media = self
            .header("content-type")
            .and_then(|value| value.split(';').next())
            .map(|value| value.trim().to_ascii_lowercase());
        let whole = || serde_json::from_str::<serde_json::Value>(&text).ok();
        let messages: Vec<serde_json::Value> = match media.as_deref() {
            Some("application/json") => whole().into_iter().collect(),
            Some("text/event-stream") => sse_messages(&text),
            _ => whole().map_or_else(|| sse_messages(&text), |value| vec![value]),
        };
        for message in messages {
            if message.get("error").is_some() {
                return false;
            }
            if message
                .pointer("/result/protocolVersion")
                .and_then(|v| v.as_str())
                .is_some()
            {
                return true;
            }
        }
        false
    }
}

async fn read_some(stream: &mut TcpStream, into: &mut Vec<u8>, limit: usize) -> Result<bool> {
    let mut chunk = [0; 4096];
    let count = stream.read(&mut chunk).await?;
    into.extend_from_slice(&chunk[..count]);
    ensure!(into.len() <= limit, "health response too large");
    Ok(count > 0)
}

fn sse_messages(text: &str) -> Vec<serde_json::Value> {
    text.replace("\r\n", "\n")
        .replace('\r', "\n")
        .split("\n\n")
        .filter_map(|event| {
            let data: Vec<&str> = event
                .lines()
                .filter_map(|line| line.strip_prefix("data:"))
                .map(|data| data.strip_prefix(' ').unwrap_or(data))
                .collect();
            (!data.is_empty())
                .then(|| serde_json::from_str(&data.join("\n")).ok())
                .flatten()
        })
        .collect()
}

/// Each health probe opens a fresh MCP session; end it at once so repeated
/// probes do not accumulate sessions on the backend. Best effort, detached
/// from the probe, and bounded: a failure here never changes health.
fn end_mcp_session(port: u16, path: &str, session: String) {
    if session.is_empty()
        || session.len() > 256
        || !session.bytes().all(|b| (0x21..=0x7e).contains(&b))
    {
        return;
    }
    let request = format!(
        "DELETE {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nMcp-Session-Id: {session}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(2), async {
            let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;
            stream.write_all(request.as_bytes()).await?;
            HealthReply::head(&mut stream).await
        })
        .await;
    });
}

#[derive(Clone)]
struct FrontState {
    backend: Option<u16>,
    state: ServiceState,
    reason: String,
}
async fn front_connection(
    mut client: TcpStream,
    front: Arc<RwLock<FrontState>>,
    fenced: bool,
    read_only_paths: Arc<Vec<String>>,
) {
    // A generic proxy cannot validate engine-specific mutating MCP calls. Fail closed
    // unless a trusted adapter explicitly provides owner+generation fencing upstream.
    let mut request = vec![0; 8192];
    let received =
        match tokio::time::timeout(Duration::from_secs(2), client.read(&mut request)).await {
            Ok(Ok(n)) if n > 0 => n,
            _ => return,
        };
    request.truncate(received);
    let snapshot = front.read().await.clone();
    if snapshot.backend.is_none() {
        let body = serde_json::json!({ "error":"service unavailable", "state":snapshot.state,
            "reason":snapshot.reason, "retry_after_seconds":1 })
        .to_string();
        let response = format!(
            "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: {}\r\nRetry-After: 1\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = client.write_all(response.as_bytes()).await;
        return;
    }
    if !fenced {
        // Do not forward a raw bidirectional connection: a client could pipeline or
        // subsequently write a POST after an allowed GET on the same TCP stream.
        let head_end = loop {
            if let Some(i) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                break Some(i);
            }
            if request.len() >= 8192 {
                break None;
            }
            let mut buf = [0; 1024];
            let n = match tokio::time::timeout(Duration::from_secs(2), client.read(&mut buf)).await
            {
                Ok(Ok(n)) if n > 0 => n,
                _ => break None,
            };
            request.extend_from_slice(&buf[..n]);
        };
        let allowed = head_end.and_then(|end| {
            if end + 4 != request.len() {
                return None;
            }
            let text = std::str::from_utf8(&request[..end]).ok()?;
            let mut lines = text.split("\r\n");
            let first = lines.next()?;
            let mut parts = first.split(' ');
            let method = parts.next()?;
            let path = parts.next()?;
            if !matches!(method, "GET" | "HEAD")
                || !read_only_paths.iter().any(|allowed| allowed == path)
                || !matches!(parts.next()?, "HTTP/1.0" | "HTTP/1.1")
                || parts.next().is_some()
            {
                return None;
            }
            let mut headers = Vec::new();
            for line in lines {
                let (key, value) = line.split_once(':')?;
                if !key.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-') {
                    return None;
                }
                let key_lower = key.to_ascii_lowercase();
                if key_lower == "transfer-encoding"
                    || key_lower == "upgrade"
                    || (key_lower == "content-length" && value.trim() != "0")
                {
                    return None;
                }
                if key_lower != "connection"
                    && key_lower != "proxy-connection"
                    && key_lower != "content-length"
                {
                    headers.push(line);
                }
            }
            Some(
                format!(
                    "{}\r\nConnection: close\r\n\r\n",
                    [vec![first], headers].concat().join("\r\n")
                )
                .into_bytes(),
            )
        });
        let Some(safe_request) = allowed else {
            let body =
                r#"{"error":"mutating proxy requests require adapter owner/fencing enforcement"}"#;
            let response = format!(
                "HTTP/1.1 403 Forbidden\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = client.write_all(response.as_bytes()).await;
            return;
        };
        request = safe_request;
    }
    if let Some(port) = snapshot.backend
        && let Ok(Ok(mut backend)) = tokio::time::timeout(
            Duration::from_secs(2),
            TcpStream::connect(("127.0.0.1", port)),
        )
        .await
    {
        if backend.write_all(&request).await.is_ok() {
            if fenced {
                let _ = tokio::io::copy_bidirectional(&mut client, &mut backend).await;
            } else {
                // Only the validated read request reaches the backend; no client writes
                // can turn this connection into an unfenced mutating request.
                let _ = tokio::io::copy(&mut backend, &mut client).await;
            }
        }
        return;
    }
    let body = serde_json::json!({ "error":"service unavailable", "state":snapshot.state,
        "reason":snapshot.reason, "retry_after_seconds":1 })
    .to_string();
    let response = format!(
        "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: {}\r\nRetry-After: 1\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = client.write_all(response.as_bytes()).await;
}
async fn front_server(
    listener: TcpListener,
    front: Arc<RwLock<FrontState>>,
    fenced: bool,
    read_only_paths: Vec<String>,
) {
    let read_only_paths = Arc::new(read_only_paths);
    while let Ok((client, _)) = listener.accept().await {
        tokio::spawn(front_connection(
            client,
            front.clone(),
            fenced,
            read_only_paths.clone(),
        ));
    }
}
fn publish(dir: &Path, status: &ServiceStatus) -> Result<()> {
    write_json(&dir.join("state.json"), status)
}
async fn transition(
    dir: &Path,
    status: &mut ServiceStatus,
    front: &Arc<RwLock<FrontState>>,
    state: ServiceState,
    reason: impl Into<String>,
    backend: Option<u16>,
) -> Result<()> {
    status.state = state.clone();
    status.reason = reason.into();
    {
        let mut current = front.write().await;
        current.state = state;
        current.reason = status.reason.clone();
        current.backend = backend;
    }
    publish(dir, status)
}
/// A per-generation subgroup is inside the supervisor's systemd-owned unit.
/// Killing the supervisor kills its entire cgroup; stopping one generation
/// kills just this subgroup, including descendants that used setsid().
struct BackendCgroups {
    base: Option<PathBuf>,
}
impl BackendCgroups {
    fn new() -> Result<Self> {
        if cfg!(test) {
            return Ok(Self { base: None });
        }
        let unit = std::env::var("BORG_SERVICE_UNIT")
            .context("service must run in a delegated systemd unit")?;
        ensure!(
            unit.starts_with("borg-service-") && unit.ends_with(".service"),
            "invalid service unit"
        );
        let self_group = fs::read_to_string("/proc/self/cgroup")?
            .lines()
            .find_map(|line| line.strip_prefix("0::"))
            .context("unified cgroup v2 required")?
            .to_owned();
        let output = std::process::Command::new("systemctl")
            .args([
                "--user",
                "show",
                &unit,
                "-p",
                "ControlGroup",
                "-p",
                "Delegate",
            ])
            .output()?;
        ensure!(
            output.status.success(),
            "cannot verify supervisor systemd unit"
        );
        let properties = String::from_utf8(output.stdout)?;
        ensure!(
            properties
                .lines()
                .any(|l| l == format!("ControlGroup={self_group}")),
            "supervisor not in its declared cgroup"
        );
        ensure!(
            properties.lines().any(|l| l == "Delegate=yes"),
            "supervisor unit lacks delegated cgroup"
        );
        let base = PathBuf::from("/sys/fs/cgroup").join(self_group.trim_start_matches('/'));
        ensure!(
            base.join("cgroup.kill").exists(),
            "no cgroup.kill for supervisor scope"
        );
        Ok(Self { base: Some(base) })
    }
    fn create(&self) -> Result<Option<PathBuf>> {
        let Some(base) = &self.base else {
            return Ok(None);
        };
        let scope = base.join(format!("backend-{}", Uuid::new_v4().simple()));
        fs::create_dir(&scope).context("create backend cgroup")?;
        ensure!(
            scope.join("cgroup.kill").exists(),
            "backend cgroup has no kill control"
        );
        Ok(Some(scope))
    }
}

async fn stop_child(
    child: &mut Child,
    spec: &ServiceSpec,
    port: Option<u16>,
    scope: Option<&Path>,
) -> Result<()> {
    if let Some(graceful) = &spec.graceful_stop {
        let _ = hook(graceful, spec, port, None).await;
    }
    if let Some(scope) = scope {
        // Even if the leader has exited, descendants in this cgroup must die.
        fs::write(scope.join("cgroup.kill"), "1").context("kill backend cgroup")?;
        let _ = tokio::time::timeout(Duration::from_secs(3), child.wait()).await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let events = fs::read_to_string(scope.join("cgroup.events"))?;
            if events.lines().any(|line| line == "populated 0") {
                break;
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "backend cgroup still populated"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        fs::remove_dir(scope).context("remove empty backend cgroup")?;
    } else {
        // Direct supervisor invocations are test-only; production fails closed
        // in BackendCgroups::new rather than relying on a process-group fallback.
        ensure!(cfg!(test), "unscoped backend is forbidden");
        if let Some(pid) = child.id() {
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
        }
        let _ = child.wait().await;
    }
    Ok(())
}
fn spawn_backend(
    spec: &ServiceSpec,
    port: Option<u16>,
    log: &File,
    scopes: &BackendCgroups,
) -> Result<(Child, Option<PathBuf>)> {
    use std::os::unix::process::CommandExt;
    let scope = scopes.create()?;
    let mut cmd = command(&spec.argv, spec, port, None)?;
    cmd.stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log.try_clone()?);
    cmd.as_std_mut().process_group(0);
    if let Some(ref scope_path) = scope {
        use std::os::unix::ffi::OsStrExt;
        let target =
            std::ffi::CString::new(scope_path.join("cgroup.procs").as_os_str().as_bytes())?;
        // pre_exec is child-only: libc calls without allocation/locking.
        unsafe {
            cmd.as_std_mut().pre_exec(move || {
                let fd = libc::open(target.as_ptr(), libc::O_WRONLY | libc::O_CLOEXEC);
                if fd < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                let written = libc::write(fd, b"0".as_ptr().cast(), 1);
                let error = std::io::Error::last_os_error();
                libc::close(fd);
                if written != 1 {
                    return Err(error);
                }
                Ok(())
            });
        }
    }
    let child = match cmd.spawn() {
        Ok(child) => child,
        Err(error) => {
            if let Some(scope) = &scope {
                let _ = fs::remove_dir(scope);
            }
            return Err(error).context("spawn scoped service backend");
        }
    };
    Ok((child, scope))
}

struct Backend {
    child: Child,
    scope: Option<PathBuf>,
    port: Option<u16>,
    started_ms: u64,
    last_probe_ms: u64,
    unhealthy_since_ms: Option<u64>,
}
fn next_port_after(spec: &ServiceSpec, last: Option<u16>) -> Option<u16> {
    spec.endpoint.as_ref().map(|endpoint| {
        endpoint
            .backend_ports
            .iter()
            .copied()
            .find(|port| Some(*port) != last)
            .unwrap_or(endpoint.backend_ports[0])
    })
}
async fn restore_client(spec: &ServiceSpec, lease: &ClientLease, port: Option<u16>) -> Result<()> {
    if let Some(restore) = &spec.restore {
        hook(
            restore,
            spec,
            port,
            Some(&lease.owner.participant_id.to_string()),
        )
        .await?;
    }
    Ok(())
}

/// A failed callback keeps its lease durable and names the owner and lease ID
/// so an operator can fix the hook and retry exactly that release.
async fn restore_one_client(
    spec: &ServiceSpec,
    status: &mut ServiceStatus,
    dir: &Path,
    gate: &ServiceGate,
    lease: &ClientLease,
    port: Option<u16>,
) -> Result<()> {
    if let Err(error) = restore_client(spec, lease, port).await {
        // No client-set change, so the failure notice needs no lane fence.
        status.reason = format!(
            "restore client owner={} lease_id={} failed: {error:#}",
            lease.owner.participant_id, lease.id
        );
        publish(dir, status)?;
        bail!("{}", status.reason);
    }
    gate.store
        .service_client_change(&spec.id, &gate.request.resources, false, || {
            status.clients.retain(|client| client.id != lease.id);
            if status.reason.starts_with("restore client owner=")
                && status.reason.contains(&format!("lease_id={}", lease.id))
                && matches!(status.state, ServiceState::Healthy { .. })
            {
                status.reason = "client restore recovered".to_owned();
            }
            publish(dir, status)
        })
}

/// Restore clients one at a time, persisting each successful removal. A failed
/// callback must never drop other owners from crash-recovery state.
async fn restore_clients(
    spec: &ServiceSpec,
    status: &mut ServiceStatus,
    dir: &Path,
    gate: &ServiceGate,
    port: Option<u16>,
) -> Result<()> {
    for lease in status.clients.clone() {
        restore_one_client(spec, status, dir, gate, &lease, port).await?;
    }
    Ok(())
}

/// The held lane lease spans every live backend and candidate. Admission is
/// serialized with exclusive Preparing/Granted under the lane state.lock.
struct ServiceGate {
    store: LaneStore,
    request: LeaseRequest,
    budget: AdmissionBudget,
    lease: Option<Lease>,
}
impl ServiceGate {
    fn new(root: &Path, spec: &ServiceSpec) -> Result<Self> {
        let id = Uuid::new_v5(&Uuid::NAMESPACE_OID, spec.id.as_bytes());
        Ok(Self {
            store: LaneStore::new(root.parent().context("service root has no lane parent")?)?,
            request: LeaseRequest {
                resources: spec
                    .resources
                    .iter()
                    .map(|r| ResourceRequest {
                        key: r.key.clone(),
                        access: Access::Shared { slots: 1 },
                    })
                    .collect(),
                holder: Holder {
                    participant_id: id,
                    session_id: id,
                    host_pid: Some(std::process::id()),
                    purpose: format!("service:{}", spec.id),
                },
                queue_timeout_ms: None,
            },
            budget: spec.admission.clone(),
            lease: None,
        })
    }
    fn acquire(&mut self) -> Result<bool> {
        if self.lease.is_none() {
            self.lease = self
                .store
                .try_acquire_service(self.request.clone(), &self.budget)?;
        }
        Ok(self.lease.is_some())
    }
    fn denial_reason(&self) -> Result<String> {
        let records = self.store.snapshot()?;
        Ok(records
            .iter()
            .rev()
            .find(|r| {
                r.service_lease
                    && r.request.holder.participant_id == self.request.holder.participant_id
                    && matches!(r.state, crate::lanes::TicketState::Cancelled { .. })
            })
            .and_then(|r| r.wait_reason.clone())
            .unwrap_or_else(|| "waiting for lane resource admission".into()))
    }
    fn release(&mut self) -> Result<()> {
        if let Some(lease) = &self.lease {
            self.store.release_lease(lease)?;
            self.lease = None;
        }
        Ok(())
    }
}

/// Run in the private transient user unit (or in a detached setsid process). The lock FD
/// outlives this task; a second process cannot attach a competing backend or public port.
pub async fn supervise(root: &Path, id: &str) -> Result<()> {
    let dir = service_dir(root, id)?;
    let _lock = lock(&dir.join("supervisor.lock"), true)?;
    let spec: ServiceSpec = read_json(&dir.join("spec.json"))?;
    ensure!(spec.id == id, "service spec identity mismatch");
    valid_spec(&spec)?;
    let mut gate = ServiceGate::new(root, &spec)?;
    let scopes = BackendCgroups::new()?;
    use std::os::unix::fs::OpenOptionsExt;
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(dir.join("output.log"))?;
    let mut status: ServiceStatus =
        read_json(&dir.join("state.json")).unwrap_or_else(|_| stopped(id));
    status.endpoint = spec.endpoint.clone();
    status.supervisor_pid = Some(std::process::id());
    status.backend_pid = None;
    // Crash recovery cannot assume that the previous backend still owns settings.
    if let Err(error) = restore_clients(&spec, &mut status, &dir, &gate, None).await {
        status.state = ServiceState::Failed {
            reason: error.to_string(),
        };
        status.reason = format!("restore after supervisor crash failed: {error:#}");
        publish(&dir, &status)?;
        bail!("{}", status.reason);
    }
    let socket = dir.join("control.sock");
    let _ = fs::remove_file(&socket); // Only the holder of supervisor.lock may replace a stale socket.
    let (_dir_fd, short) = control_socket(&dir)?;
    let control = UnixListener::bind(&short)?;
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&socket, fs::Permissions::from_mode(0o600))?;
    let front = Arc::new(RwLock::new(FrontState {
        backend: None,
        state: ServiceState::Starting,
        reason: "starting".into(),
    }));
    if let Some(endpoint) = &spec.endpoint {
        let addr: SocketAddr = endpoint.listen.parse()?;
        ensure!(
            matches!(addr.ip(), IpAddr::V4(_) | IpAddr::V6(_)),
            "invalid listener"
        );
        let listener = TcpListener::bind(addr)
            .await
            .with_context(|| format!("front endpoint {} busy", endpoint.listen))?;
        tokio::spawn(front_server(
            listener,
            front.clone(),
            spec.adapter_enforces_leases,
            spec.read_only_paths.clone(),
        ));
    }
    transition(
        &dir,
        &mut status,
        &front,
        ServiceState::Starting,
        "supervisor started",
        None,
    )
    .await?;
    let mut active: Option<Backend> = None;
    let mut candidate: Option<Backend> = None;
    let mut pending: Option<(u64, String)> = None;
    let mut last_port: Option<u16> = None;
    let mut last_activity = unix_ms();
    let mut idle = false;
    let mut failures = 0u32;
    let mut next_launch = 0u64;
    let mut stopping = false;
    loop {
        let now = unix_ms();
        let expired = status
            .clients
            .iter()
            .filter(|l| l.expires_at_unix_ms <= now)
            .cloned()
            .collect::<Vec<_>>();
        for lease in expired {
            if let Err(error) = restore_one_client(
                &spec,
                &mut status,
                &dir,
                &gate,
                &lease,
                active.as_ref().and_then(|b| b.port),
            )
            .await
            {
                transition(
                    &dir,
                    &mut status,
                    &front,
                    ServiceState::Failed {
                        reason: error.to_string(),
                    },
                    format!("restore on lease expiry failed: {error:#}"),
                    None,
                )
                .await?;
                stopping = true;
                break;
            }
        }
        status
            .yields
            .retain(|_, window| window.expires_at_unix_ms > now);
        if !status.yields.is_empty() && (active.is_some() || candidate.is_some()) {
            transition(
                &dir,
                &mut status,
                &front,
                ServiceState::Yielding,
                "exclusive lane requested",
                None,
            )
            .await?;
            if let Some(mut b) = candidate.take() {
                stop_child(&mut b.child, &spec, b.port, b.scope.as_deref()).await?;
            }
            if let Some(mut b) = active.take() {
                stop_child(&mut b.child, &spec, b.port, b.scope.as_deref()).await?;
            }
            status.backend_pid = None;
            gate.release()?;
            transition(
                &dir,
                &mut status,
                &front,
                ServiceState::Yielded,
                "exclusive lane holds service resource",
                None,
            )
            .await?;
        }
        // Event-driven local control socket; one serialized mutation per accepted connection.
        if let Ok(Ok((mut client, _))) =
            tokio::time::timeout(Duration::from_millis(50), control.accept()).await
        {
            let mut bytes = Vec::new();
            let response =
                match tokio::time::timeout(Duration::from_secs(5), client.read_to_end(&mut bytes))
                    .await
                {
                    Ok(Ok(_)) if bytes.len() <= 8192 => {
                        match serde_json::from_slice::<ServiceRequest>(&bytes) {
                            Ok(request) => handle_request(
                                request,
                                &spec,
                                &dir,
                                &front,
                                &mut status,
                                &mut active,
                                &mut candidate,
                                &mut pending,
                                &mut stopping,
                                &mut last_activity,
                                &mut idle,
                                &mut gate,
                            )
                            .await
                            .map(|_| serde_json::to_value(&status).unwrap_or_default()),
                            Err(error) => Err(error.into()),
                        }
                    }
                    _ => Err(anyhow::anyhow!("invalid or oversized service request")),
                };
            let value =
                response.unwrap_or_else(|error| serde_json::json!({"error":error.to_string()}));
            let _ = client.write_all(&serde_json::to_vec(&value)?).await;
            let _ = client.shutdown().await;
        }
        if stopping {
            break;
        }
        let now = unix_ms();
        if failures > spec.restart.max_restarts && active.is_none() {
            transition(
                &dir,
                &mut status,
                &front,
                ServiceState::Failed {
                    reason: "restart limit exceeded".into(),
                },
                "restart limit exceeded",
                None,
            )
            .await?;
            break;
        }
        if status.yields.is_empty() && active.is_none() && candidate.is_none() && now >= next_launch
        {
            if !gate.acquire()? {
                let reason = gate.denial_reason()?;
                if status.reason != reason {
                    transition(
                        &dir,
                        &mut status,
                        &front,
                        ServiceState::Degraded {
                            reason: reason.clone(),
                        },
                        reason,
                        None,
                    )
                    .await?;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            let port = next_port_after(&spec, last_port);
            last_port = port;
            match spawn_backend(&spec, port, &log, &scopes) {
                Ok((child, scope)) => {
                    status.backend_pid = child.id();
                    candidate = Some(Backend {
                        child,
                        scope,
                        port,
                        started_ms: now,
                        last_probe_ms: 0,
                        unhealthy_since_ms: None,
                    });
                    transition(
                        &dir,
                        &mut status,
                        &front,
                        ServiceState::Starting,
                        "waiting for health",
                        None,
                    )
                    .await?;
                }
                Err(error) => {
                    failures += 1;
                    next_launch = now + backoff(&spec, failures);
                    transition(
                        &dir,
                        &mut status,
                        &front,
                        ServiceState::Degraded {
                            reason: error.to_string(),
                        },
                        error.to_string(),
                        None,
                    )
                    .await?;
                }
            }
        }
        if let Some(b) = candidate.as_mut() {
            let gone = b.child.try_wait()?.is_some();
            let overdue = now.saturating_sub(b.started_ms) >= spec.readiness_timeout_ms;
            if gone || overdue {
                stop_child(&mut b.child, &spec, b.port, b.scope.as_deref()).await?;
                candidate = None;
                failures += 1;
                next_launch = now + backoff(&spec, failures);
                status.restarts += 1;
                if failures > spec.restart.max_restarts {
                    transition(
                        &dir,
                        &mut status,
                        &front,
                        ServiceState::Failed {
                            reason: "restart limit exceeded".into(),
                        },
                        "restart limit exceeded",
                        active.as_ref().and_then(|b| b.port),
                    )
                    .await?;
                    // Leave a healthy active backend serving even if the replacement fails.
                    if active.is_none() {
                        break;
                    }
                    pending = None;
                } else {
                    transition(
                        &dir,
                        &mut status,
                        &front,
                        ServiceState::Degraded {
                            reason: READINESS_FAILED.into(),
                        },
                        READINESS_FAILED,
                        active.as_ref().and_then(|b| b.port),
                    )
                    .await?;
                }
            } else if now.saturating_sub(b.last_probe_ms) >= spec.health.interval_ms {
                b.last_probe_ms = now;
                if probe(&spec, b.port).await {
                    let new = candidate.take().expect("candidate present");
                    let old = active.replace(new);
                    status.backend_pid = active.as_ref().and_then(|b| b.child.id());
                    let port = active.as_ref().and_then(|b| b.port);
                    transition(
                        &dir,
                        &mut status,
                        &front,
                        ServiceState::Healthy { backend: port },
                        "health check passed",
                        port,
                    )
                    .await?;
                    failures = 0;
                    pending = None;
                    last_activity = now;
                    idle = false;
                    if let Some(mut old) = old {
                        stop_child(&mut old.child, &spec, old.port, old.scope.as_deref()).await?;
                        status.restarts += 1;
                    }
                }
            }
        }
        if let Some(b) = active.as_mut() {
            if b.child.try_wait()?.is_some() {
                let mut dead = active.take().expect("active present");
                transition(
                    &dir,
                    &mut status,
                    &front,
                    ServiceState::Degraded {
                        reason: "backend crashed".into(),
                    },
                    "backend crashed",
                    None,
                )
                .await?;
                // The leader may exit leaving detached descendants. They stay
                // inside its delegated cgroup; stop/verify it before releasing
                // the lane resource or starting a replacement.
                stop_child(&mut dead.child, &spec, dead.port, dead.scope.as_deref()).await?;
                status.backend_pid = None;
                status.restarts += 1;
                failures += 1;
                next_launch = now + backoff(&spec, failures);
                if let Err(error) =
                    restore_clients(&spec, &mut status, &dir, &gate, dead.port).await
                {
                    transition(
                        &dir,
                        &mut status,
                        &front,
                        ServiceState::Failed {
                            reason: error.to_string(),
                        },
                        format!("restore after crash failed: {error:#}"),
                        None,
                    )
                    .await?;
                    stopping = true;
                }
                publish(&dir, &status)?;
            } else if now.saturating_sub(b.last_probe_ms) >= spec.health.interval_ms
                && candidate.is_none()
            {
                b.last_probe_ms = now;
                if probe(&spec, b.port).await {
                    b.unhealthy_since_ms = None;
                } else {
                    let since = *b.unhealthy_since_ms.get_or_insert(now);
                    if now.saturating_sub(since) >= spec.health.timeout_ms.saturating_mul(3) {
                        transition(
                            &dir,
                            &mut status,
                            &front,
                            ServiceState::Degraded {
                                reason: "backend hung".into(),
                            },
                            "backend hung",
                            None,
                        )
                        .await?;
                        let mut old = active.take().expect("active present");
                        stop_child(&mut old.child, &spec, old.port, old.scope.as_deref()).await?;
                        status.backend_pid = None;
                        status.restarts += 1;
                        failures += 1;
                        next_launch = now + backoff(&spec, failures);
                    }
                }
            }
        }
        if let Some((requested, reason)) = &pending
            && active.is_some()
            && candidate.is_none()
            && status.clients.is_empty()
            && now.saturating_sub(*requested) >= spec.restart.debounce_ms
            && now >= next_launch
        {
            let port = next_port_after(&spec, last_port);
            last_port = port;
            match spawn_backend(&spec, port, &log, &scopes) {
                Ok((child, scope)) => {
                    candidate = Some(Backend {
                        child,
                        scope,
                        port,
                        started_ms: now,
                        last_probe_ms: 0,
                        unhealthy_since_ms: None,
                    });
                    transition(
                        &dir,
                        &mut status,
                        &front,
                        ServiceState::Restarting,
                        reason.clone(),
                        active.as_ref().and_then(|b| b.port),
                    )
                    .await?;
                }
                Err(error) => {
                    next_launch = now + backoff(&spec, failures + 1);
                    transition(
                        &dir,
                        &mut status,
                        &front,
                        ServiceState::Degraded {
                            reason: error.to_string(),
                        },
                        format!("restart launch failed: {error}"),
                        active.as_ref().and_then(|b| b.port),
                    )
                    .await?;
                }
            }
        }
        if active.is_none() && candidate.is_none() {
            gate.release()?;
        }
        if status.yields.is_empty()
            && active.is_some()
            && status.clients.is_empty()
            && !idle
            && now.saturating_sub(last_activity) >= spec.idle_after_ms
            && let Some(h) = &spec.idle
            && hook(h, &spec, active.as_ref().and_then(|b| b.port), None)
                .await
                .is_ok()
        {
            idle = true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    transition(
        &dir,
        &mut status,
        &front,
        ServiceState::Stopped,
        "supervisor stopping",
        None,
    )
    .await?;
    if let Some(mut b) = candidate {
        stop_child(&mut b.child, &spec, b.port, b.scope.as_deref()).await?;
    }
    if let Some(mut b) = active {
        stop_child(&mut b.child, &spec, b.port, b.scope.as_deref()).await?;
    }
    restore_clients(&spec, &mut status, &dir, &gate, None).await?;
    gate.release()?;
    status.backend_pid = None;
    publish(&dir, &status)?;
    let _ = fs::remove_file(socket);
    Ok(())
}
fn backoff(spec: &ServiceSpec, failures: u32) -> u64 {
    spec.restart
        .backoff_ms
        .max(50)
        .saturating_mul(1u64 << failures.min(6))
}

#[allow(clippy::too_many_arguments)]
async fn handle_request(
    request: ServiceRequest,
    spec: &ServiceSpec,
    dir: &Path,
    front: &Arc<RwLock<FrontState>>,
    status: &mut ServiceStatus,
    active: &mut Option<Backend>,
    candidate: &mut Option<Backend>,
    pending: &mut Option<(u64, String)>,
    stopping: &mut bool,
    last_activity: &mut u64,
    idle: &mut bool,
    gate: &mut ServiceGate,
) -> Result<()> {
    let now = unix_ms();
    match request {
        ServiceRequest::Stop => {
            // Preserve the backend and lane lease if any owner's restore fails.
            restore_clients(
                spec,
                status,
                dir,
                gate,
                active.as_ref().and_then(|b| b.port),
            )
            .await?;
            transition(
                dir,
                status,
                front,
                ServiceState::Stopped,
                "stop requested",
                None,
            )
            .await?;
            if let Some(mut b) = candidate.take() {
                stop_child(&mut b.child, spec, b.port, b.scope.as_deref()).await?;
            }
            if let Some(mut b) = active.take() {
                stop_child(&mut b.child, spec, b.port, b.scope.as_deref()).await?;
            }
            status.backend_pid = None;
            gate.release()?;
            *stopping = true;
            publish(dir, status)?;
        }
        ServiceRequest::Restart { reason, force } => {
            if force {
                restore_clients(
                    spec,
                    status,
                    dir,
                    gate,
                    active.as_ref().and_then(|b| b.port),
                )
                .await?;
            }
            *pending = Some((now, reason.clone()));
            transition(
                dir,
                status,
                front,
                ServiceState::RestartPending,
                if status.clients.is_empty() {
                    reason
                } else {
                    format!("waiting for client lease: {reason}")
                },
                active.as_ref().and_then(|b| b.port),
            )
            .await?;
        }
        ServiceRequest::Yield { by, reason, ttl_ms } => {
            ensure!(
                !by.is_empty() && ttl_ms > 0 && ttl_ms <= 86_400_000,
                "invalid yield owner/duration"
            );
            // An exclusive job cannot inherit a client's editor settings. Restore
            // every leased change before acknowledging the pre-grant barrier.
            // Failure keeps the service lease/backend intact and blocks the job.
            restore_clients(
                spec,
                status,
                dir,
                gate,
                active.as_ref().and_then(|b| b.port),
            )
            .await?;
            // The caller supplies a lane job ID; no other owner can resume this yield.
            let expires = now.saturating_add(ttl_ms);
            let prior = status
                .yields
                .get(&by)
                .map(|w| w.expires_at_unix_ms)
                .unwrap_or(0);
            status.yields.insert(
                by.clone(),
                YieldWindow {
                    by,
                    reason,
                    expires_at_unix_ms: expires.max(prior),
                },
            );
            transition(
                dir,
                status,
                front,
                ServiceState::Yielding,
                "exclusive resource requested",
                None,
            )
            .await?;
            if let Some(mut b) = candidate.take() {
                stop_child(&mut b.child, spec, b.port, b.scope.as_deref()).await?;
            }
            if let Some(mut b) = active.take() {
                stop_child(&mut b.child, spec, b.port, b.scope.as_deref()).await?;
            }
            status.backend_pid = None;
            gate.release()?;
            transition(
                dir,
                status,
                front,
                ServiceState::Yielded,
                "backend stopped before exclusive admission",
                None,
            )
            .await?;
        }
        ServiceRequest::Resume { by } => {
            ensure!(status.yields.remove(&by).is_some(), "no yield held by {by}");
            if status.yields.is_empty() {
                transition(
                    dir,
                    status,
                    front,
                    ServiceState::Starting,
                    "yield released",
                    None,
                )
                .await?;
            } else {
                publish(dir, status)?;
            }
        }
        ServiceRequest::Lease {
            owner,
            purpose,
            ttl_ms,
        } => {
            ensure!(
                ttl_ms > 0 && ttl_ms <= 86_400_000,
                "lease ttl must be within 1..86400000 ms"
            );
            // The active hook must not run for a late client rejected by a
            // Preparing exclusive ticket. A failing hook restores the exact
            // pre-request lease state under the same journal lock.
            let previous_clients = status.clients.clone();
            // The lane job's Preparing decision and all client intake share
            // state.lock. Publishing the client file inside this transaction
            // makes the job's under-lock read an atomic authority check.
            gate.store
                .service_client_change(&spec.id, &gate.request.resources, true, || {
                    ensure!(
                        matches!(
                            status.state,
                            ServiceState::Healthy { .. } | ServiceState::RestartPending
                        ),
                        "service not ready for client leases"
                    );
                    ensure!(
                        status.yields.is_empty(),
                        "service yielded for exclusive work"
                    );
                    if let Some(existing) = status.clients.iter_mut().find(|lease| {
                        lease.owner.participant_id == owner.participant_id
                            && lease.owner.session_id == owner.session_id
                    }) {
                        existing.expires_at_unix_ms = now.saturating_add(ttl_ms);
                        existing.purpose = purpose;
                    } else {
                        match spec.client_mode {
                            ClientMode::Exclusive => ensure!(
                                status.clients.is_empty(),
                                "service lease held by another owner"
                            ),
                            ClientMode::Shared { max_clients } => ensure!(
                                status.clients.len() < max_clients,
                                "service client limit reached ({max_clients})"
                            ),
                        }
                        status.clients.push(ClientLease {
                            id: Uuid::new_v4(),
                            service_id: spec.id.clone(),
                            owner,
                            expires_at_unix_ms: now.saturating_add(ttl_ms),
                            purpose,
                        });
                    }
                    publish(dir, status)
                })?;
            if *idle
                && let Some(h) = &spec.active
                && let Err(error) = hook(h, spec, active.as_ref().and_then(|b| b.port), None).await
            {
                gate.store.service_client_change(
                    &spec.id,
                    &gate.request.resources,
                    false,
                    || {
                        status.clients = previous_clients;
                        publish(dir, status)
                    },
                )?;
                return Err(error);
            }
            *idle = false;
            *last_activity = now;
        }
        ServiceRequest::Release { lease_id, owner } => {
            let lease = status
                .clients
                .iter()
                .find(|l| {
                    l.id == lease_id
                        && l.owner.participant_id == owner.participant_id
                        && l.owner.session_id == owner.session_id
                })
                .context("client lease not owned by caller")?
                .clone();
            restore_one_client(
                spec,
                status,
                dir,
                gate,
                &lease,
                active.as_ref().and_then(|b| b.port),
            )
            .await?;
        }
        ServiceRequest::Touch => {
            *last_activity = now;
            if *idle {
                if let Some(h) = &spec.active {
                    hook(h, spec, active.as_ref().and_then(|b| b.port), None).await?;
                }
                *idle = false;
            }
        }
    }
    Ok(())
}

#[async_trait]
impl ServiceCoordinator for ServiceManager {
    async fn start(&self, spec: ServiceSpec) -> Result<ServiceStatus> {
        self.launch(spec).await
    }
    async fn status(&self, id: &str) -> Result<ServiceStatus> {
        self.read_status(id)
    }
    async fn acquire(&self, id: &str, owner: Holder, ttl_ms: u64) -> Result<ClientLease> {
        let status = self
            .send(
                id,
                ServiceRequest::Lease {
                    purpose: owner.purpose.clone(),
                    owner: owner.clone(),
                    ttl_ms,
                },
                Duration::from_secs(30),
            )
            .await?;
        status
            .clients
            .into_iter()
            .find(|l| {
                l.owner.participant_id == owner.participant_id
                    && l.owner.session_id == owner.session_id
            })
            .context("lease granted but missing from status")
    }
    async fn release(&self, lease: &ClientLease) -> Result<()> {
        self.send(
            &lease.service_id,
            ServiceRequest::Release {
                lease_id: lease.id,
                owner: lease.owner.clone(),
            },
            Duration::from_secs(30),
        )
        .await?;
        Ok(())
    }
    async fn trigger_restart(&self, id: &str, reason: &str) -> Result<()> {
        self.send(
            id,
            ServiceRequest::Restart {
                reason: reason.into(),
                force: false,
            },
            Duration::from_secs(30),
        )
        .await?;
        Ok(())
    }
    async fn yield_for(&self, id: &str, resources: &[ResourceRequest]) -> Result<()> {
        let spec: ServiceSpec = read_json(&service_dir(&self.root, id)?.join("spec.json"))?;
        ensure!(
            resources
                .iter()
                .any(|r| spec.resources.iter().any(|owned| owned.key == r.key)),
            "service does not declare this lane resource"
        );
        let by = resources
            .iter()
            .map(|r| r.key.name.as_str())
            .collect::<Vec<_>>()
            .join("+");
        self.send(
            id,
            ServiceRequest::Yield {
                by,
                reason: "lane exclusive".into(),
                ttl_ms: 600_000,
            },
            Duration::from_secs(120),
        )
        .await?;
        Ok(())
    }
    async fn resume(&self, id: &str) -> Result<ServiceStatus> {
        let status = self.read_status(id)?;
        ensure!(
            status.yields.len() == 1,
            "resume requires an explicit yield owner when multiple yields are active"
        );
        let by = status
            .yields
            .keys()
            .next()
            .context("no active yield")?
            .clone();
        self.send(id, ServiceRequest::Resume { by }, Duration::from_secs(30))
            .await
    }
    async fn stop(&self, id: &str) -> Result<()> {
        self.send(id, ServiceRequest::Stop, Duration::from_secs(120))
            .await?;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::lanes::{Access, ResourceKey, ResourceScope};
    use std::{net::TcpListener as StdTcpListener, os::unix::fs::PermissionsExt};

    const FAKE_HTTP: &str = r#"
import http.server, os, sys, time
port, record, flag = int(sys.argv[1]), sys.argv[2], sys.argv[3]
with open(record, 'a') as f: f.write(str(port) + '\n')
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def do_POST(self):
        if self.path == '/bad': open(os.path.join(os.path.dirname(record), 'mutated'), 'w').write('bad')
        self.rfile.read(int(self.headers.get('Content-Length', '0')))
        data = b'{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25"}}' if self.path == '/mcp' else b'{}'
        self.send_response(200); self.send_header('Content-Length', str(len(data))); self.end_headers(); self.wfile.write(data)
    def do_GET(self):
        if self.path == '/crash': os._exit(11)
        if os.path.exists(flag) and self.path == '/health': time.sleep(2)
        self.send_response(200); self.end_headers(); self.wfile.write(str(port).encode())
http.server.ThreadingHTTPServer.allow_reuse_address = True
http.server.ThreadingHTTPServer(('127.0.0.1', port), Handler).serve_forever()
"#;
    /// A free loopback port that stays free until the supervisor or backend
    /// binds it. A released `bind(0)` port is in the kernel's ephemeral range,
    /// where a concurrent test's own `bind(0)` or outgoing connection can take
    /// it first. Ports below that range are never assigned automatically, and a
    /// per-process counter (random start) never hands one out twice.
    fn port() -> u16 {
        use std::sync::atomic::{AtomicU32, Ordering};
        static NEXT: std::sync::OnceLock<(u32, u32, AtomicU32)> = std::sync::OnceLock::new();
        let (floor, span, next) = NEXT.get_or_init(|| {
            let range =
                fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range").unwrap_or_default();
            let mut bounds = range.split_whitespace().map(|v| v.parse::<u32>().ok());
            let (low, high) = match (bounds.next().flatten(), bounds.next().flatten()) {
                (Some(low), Some(high)) => (low, high),
                _ => (32_768, 60_999),
            };
            // The larger unprivileged gap outside the ephemeral range.
            let (floor, ceiling) = if low.saturating_sub(10_000) >= 65_535u32.saturating_sub(high) {
                (10_000, low.max(10_001))
            } else {
                (high + 1, 65_536)
            };
            let start = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
                ^ std::process::id();
            (floor, ceiling - floor, AtomicU32::new(start))
        });
        loop {
            let candidate = floor + next.fetch_add(1, Ordering::Relaxed) % span;
            if StdTcpListener::bind(("127.0.0.1", candidate as u16)).is_ok() {
                return candidate as u16;
            }
        }
    }
    fn spec(root: &Path) -> ServiceSpec {
        let front = port();
        let a = port();
        let b = port();
        ServiceSpec {
            id: "fake".into(),
            argv: vec![
                "python3".into(),
                "-u".into(),
                "-c".into(),
                FAKE_HTTP.into(),
                "{port}".into(),
                root.join("launches").display().to_string(),
                root.join("hang").display().to_string(),
            ],
            cwd: root.into(),
            env: vec![],
            resources: vec![ResourceRequest {
                key: ResourceKey {
                    scope: ResourceScope::Host,
                    name: "fake-exclusive".into(),
                },
                access: Access::Exclusive,
            }],
            memory_max_bytes: None,
            admission: AdmissionBudget {
                min_available_ram_bytes: 0,
                reserve_ram_bytes: 0,
                min_free_disk_bytes: 0,
                reserve_disk_bytes: 0,
                disk_path: root.into(),
            },
            health: HealthCheck {
                argv: vec!["/health".into()],
                kind: HealthKind::Http,
                interval_ms: 50,
                timeout_ms: 100,
            },
            restart: RestartPolicy {
                max_restarts: 4,
                backoff_ms: 50,
                debounce_ms: 200,
            },
            endpoint: Some(Endpoint {
                listen: format!("127.0.0.1:{front}"),
                backend_ports: [a, b],
            }),
            client_mode: ClientMode::Exclusive,
            restore: Some(Hook {
                argv: vec![
                    "python3".into(),
                    "-c".into(),
                    "import sys; open(sys.argv[1], 'a').write(sys.argv[2]+'\\n')".into(),
                    root.join("restored").display().to_string(),
                    "{owner}".into(),
                ],
                timeout_ms: 1500,
            }),
            readiness_timeout_ms: 1500,
            graceful_stop: None,
            idle: None,
            active: None,
            adapter_enforces_leases: false,
            read_only_paths: vec!["/health".into(), "/crash".into()],
            idle_after_ms: 1000,
        }
    }
    async fn state(
        manager: &ServiceManager,
        predicate: impl Fn(&ServiceStatus) -> bool,
    ) -> ServiceStatus {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
        loop {
            let current = manager.read_status("fake").unwrap();
            if predicate(&current) {
                return current;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "service timed out: {current:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
    async fn get(port: u16, path: &str) -> Result<String> {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await?;
        let mut out = Vec::new();
        tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut out)).await??;
        Ok(String::from_utf8(out)?)
    }
    async fn setup() -> (
        tempfile::TempDir,
        ServiceManager,
        u16,
        tokio::task::JoinHandle<Result<()>>,
    ) {
        setup_with(|_| {}).await
    }
    pub(crate) async fn setup_with(
        configure: impl FnOnce(&mut ServiceSpec),
    ) -> (
        tempfile::TempDir,
        ServiceManager,
        u16,
        tokio::task::JoinHandle<Result<()>>,
    ) {
        let root = tempfile::tempdir().unwrap();
        let mut spec = spec(root.path());
        configure(&mut spec);
        let services = root.path().join("services");
        let dir = service_dir(&services, "fake").unwrap();
        let manager = ServiceManager::new(services.clone(), PathBuf::new());
        // Another process can still bind a port between allocation and the
        // supervisor's bind; the supervisor then exits at once. Retry that on
        // fresh ports, and report any other early exit instead of timing out.
        for attempt in 1..=3 {
            write_json(&dir.join("spec.json"), &spec).unwrap();
            let services = services.clone();
            let mut task = tokio::spawn(async move { supervise(&services, "fake").await });
            tokio::select! {
                _ = state(&manager, |s| matches!(s.state, ServiceState::Healthy { .. })) => {
                    let front = spec.endpoint.as_ref().unwrap().listen.parse::<SocketAddr>();
                    return (root, manager, front.unwrap().port(), task);
                }
                ended = &mut task => {
                    let ended = format!("{ended:?}");
                    assert!(
                        attempt < 3 && ended.contains("busy"),
                        "supervisor exited before Healthy: {ended}"
                    );
                    spec.endpoint = Some(Endpoint {
                        listen: format!("127.0.0.1:{}", port()),
                        backend_ports: [port(), port()],
                    });
                }
            }
        }
        unreachable!("the last attempt returns or panics")
    }
    pub(crate) async fn cleanup(
        manager: &ServiceManager,
        task: tokio::task::JoinHandle<Result<()>>,
    ) {
        manager
            .send("fake", ServiceRequest::Stop, Duration::from_secs(10))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    fn owner() -> Holder {
        Holder {
            participant_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            host_pid: None,
            purpose: "test".into(),
        }
    }

    #[tokio::test]
    async fn warm_restart_debounces_and_keeps_front_serving() {
        let (root, manager, front, task) = setup().await;
        let initial = manager.read_status("fake").unwrap();
        assert!(
            get(front, "/health")
                .await
                .unwrap()
                .starts_with("HTTP/1.0 200")
        );
        let mut rejected = TcpStream::connect(("127.0.0.1", front)).await.unwrap();
        rejected
            .write_all(b"POST /mcp HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        let mut bytes = Vec::new();
        rejected.read_to_end(&mut bytes).await.unwrap();
        assert!(
            String::from_utf8(bytes).unwrap().contains("403 Forbidden"),
            "unfenced MCP must not be exposed"
        );
        assert_eq!(
            fs::metadata(manager.root.join("fake/control.sock"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(manager.root.join("fake/state.json"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        manager
            .send(
                "fake",
                ServiceRequest::Restart {
                    reason: "build".into(),
                    force: false,
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        manager
            .send(
                "fake",
                ServiceRequest::Restart {
                    reason: "second build".into(),
                    force: false,
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        let mut unavailable = 0;
        let start = tokio::time::Instant::now();
        let final_state = loop {
            if !get(front, "/health")
                .await
                .unwrap()
                .starts_with("HTTP/1.0 200")
            {
                unavailable += 1;
            }
            let current = manager.read_status("fake").unwrap();
            if matches!(current.state, ServiceState::Healthy { .. })
                && current.backend_pid != initial.backend_pid
            {
                break current;
            }
            assert!(
                start.elapsed() < Duration::from_secs(8),
                "restart stalled: {current:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(unavailable, 0, "warm restart proxy continuity");
        assert_ne!(initial.state_backend(), final_state.state_backend());
        assert_eq!(
            fs::read_to_string(root.path().join("launches"))
                .unwrap()
                .lines()
                .count(),
            2
        );
        eprintln!(
            "warm restart proxy unavailability {unavailable}, elapsed {:?}",
            start.elapsed()
        );
        cleanup(&manager, task).await;
    }

    async fn budget_contenders(disk_budget: bool) {
        let root = tempfile::tempdir().unwrap();
        let lane_root = root.path().join("lanes");
        let services = lane_root.join("services");
        let disk_a = root.path().join("disk-a");
        let disk_b = root.path().join("disk-b");
        fs::create_dir(&disk_a).unwrap();
        fs::create_dir(&disk_b).unwrap();
        let mut first = spec(root.path());
        first.id = "first".into();
        first.resources[0].key.name = "budget-first".into();
        first.admission.disk_path = disk_a;
        let mut second = spec(root.path());
        second.id = "second".into();
        second.resources[0].key.name = "budget-second".into();
        second.admission.disk_path = disk_b;
        let available = if disk_budget {
            use std::os::unix::ffi::OsStrExt;
            let path =
                std::ffi::CString::new(first.admission.disk_path.as_os_str().as_bytes()).unwrap();
            let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
            assert_eq!(
                unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) },
                0
            );
            let stat = unsafe { stat.assume_init() };
            stat.f_bavail.saturating_mul(stat.f_frsize)
        } else {
            fs::read_to_string("/proc/meminfo")
                .unwrap()
                .lines()
                .find_map(|line| line.strip_prefix("MemAvailable:"))
                .unwrap()
                .split_whitespace()
                .next()
                .unwrap()
                .parse::<u64>()
                .unwrap()
                * 1024
        };
        let reserve = available / 5 * 3; // each fits; both cannot fit together.
        assert!(reserve > 0);
        if disk_budget {
            first.admission.reserve_disk_bytes = reserve;
            second.admission.reserve_disk_bytes = reserve;
            use std::os::unix::fs::MetadataExt;
            assert_eq!(
                fs::metadata(&first.admission.disk_path).unwrap().dev(),
                fs::metadata(&second.admission.disk_path).unwrap().dev()
            );
        } else {
            first.admission.reserve_ram_bytes = reserve;
            second.admission.reserve_ram_bytes = reserve;
        }
        for s in [&first, &second] {
            let dir = service_dir(&services, &s.id).unwrap();
            write_json(&dir.join("spec.json"), s).unwrap();
        }
        let manager = ServiceManager::new(services.clone(), PathBuf::new());
        let first_root = services.clone();
        let first_task = tokio::spawn(async move { supervise(&first_root, "first").await });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !matches!(
            manager.read_status("first").unwrap().state,
            ServiceState::Healthy { .. }
        ) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "first admission stalled"
            );
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        let second_root = services.clone();
        let second_task = tokio::spawn(async move { supervise(&second_root, "second").await });
        let expected = if disk_budget {
            "disk admission queued"
        } else {
            "RAM admission queued"
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let status = manager.read_status("second").unwrap();
            if status.reason.contains(expected) {
                assert!(status.backend_pid.is_none());
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "second did not queue: {status:?}"
            );
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        // Several denied retries must coalesce to one visible admission row.
        tokio::time::sleep(Duration::from_millis(400)).await;
        let store = LaneStore::new(&lane_root).unwrap();
        let waiting = store
            .snapshot()
            .unwrap()
            .into_iter()
            .filter(|r| {
                r.service_lease
                    && r.request.holder.purpose == "service:second"
                    && matches!(r.state, crate::lanes::TicketState::Cancelled { .. })
            })
            .collect::<Vec<_>>();
        assert_eq!(waiting.len(), 1, "denial retries must not grow the journal");
        manager
            .send(
                "first",
                ServiceRequest::Yield {
                    by: "budget-release".into(),
                    reason: "handoff".into(),
                    ttl_ms: 5_000,
                },
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !matches!(
            manager.read_status("second").unwrap().state,
            ServiceState::Healthy { .. }
        ) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "second did not admit after yield"
            );
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        cleanup_service(&manager, "second", second_task).await;
        cleanup_service(&manager, "first", first_task).await;
    }
    async fn cleanup_service(
        manager: &ServiceManager,
        id: &str,
        task: tokio::task::JoinHandle<Result<()>>,
    ) {
        manager
            .send(id, ServiceRequest::Stop, Duration::from_secs(5))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(6), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    #[tokio::test]
    async fn service_disk_reservations_share_device_and_yield_releases_capacity() {
        budget_contenders(true).await;
    }
    #[tokio::test]
    async fn service_ram_reservations_queue_then_yield_releases_capacity() {
        budget_contenders(false).await;
    }

    #[tokio::test]
    async fn expired_yield_and_restart_cannot_spawn_during_exclusive_lease() {
        use crate::lanes::{LaneCoordinator, TicketState};
        let (root, manager, front, task) = setup().await;
        let store = LaneStore::new(root.path()).unwrap();
        assert!(
            store
                .snapshot()
                .unwrap()
                .iter()
                .any(|r| r.service_lease && matches!(r.state, TicketState::Granted(_)))
        );
        manager
            .send(
                "fake",
                ServiceRequest::Yield {
                    by: "job".into(),
                    reason: "exclusive".into(),
                    ttl_ms: 200,
                },
                Duration::from_secs(8),
            )
            .await
            .unwrap();
        let ticket = store
            .enqueue_lease(LeaseRequest {
                resources: vec![ResourceRequest {
                    key: ResourceKey {
                        scope: ResourceScope::Host,
                        name: "fake-exclusive".into(),
                    },
                    access: Access::Exclusive,
                }],
                holder: owner(),
                queue_timeout_ms: Some(2_000),
            })
            .unwrap();
        let exclusive = store.wait(&ticket).await.unwrap();
        tokio::time::sleep(Duration::from_millis(550)).await;
        manager
            .send(
                "fake",
                ServiceRequest::Restart {
                    reason: "during exclusive".into(),
                    force: true,
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(550)).await;
        assert!(manager.read_status("fake").unwrap().backend_pid.is_none());
        assert!(
            get(front, "/health")
                .await
                .unwrap()
                .contains("503 Service Unavailable")
        );
        store.release(&exclusive).await.unwrap();
        state(&manager, |s| {
            matches!(s.state, ServiceState::Healthy { .. })
        })
        .await;
        cleanup(&manager, task).await;
    }

    #[test]
    fn legacy_definitions_default_to_exclusive_and_shared_limit_must_be_positive() {
        let root = tempfile::tempdir().unwrap();
        let mut definition = serde_json::to_value(spec(root.path())).unwrap();
        definition.as_object_mut().unwrap().remove("client_mode");
        let mut legacy: ServiceSpec = serde_json::from_value(definition).unwrap();
        assert_eq!(legacy.client_mode, ClientMode::Exclusive);
        legacy.client_mode = ClientMode::Shared { max_clients: 0 };
        assert!(
            valid_spec(&legacy)
                .unwrap_err()
                .to_string()
                .contains("max_clients")
        );
    }

    #[tokio::test]
    async fn exclusive_client_mode_refuses_another_owner() {
        let (_root, manager, _front, task) = setup().await;
        let first = owner();
        let second = owner();
        let held = manager
            .send(
                "fake",
                ServiceRequest::Lease {
                    owner: first.clone(),
                    purpose: "first".into(),
                    ttl_ms: 5_000,
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        assert_eq!(held.clients.len(), 1);
        assert!(
            manager
                .send(
                    "fake",
                    ServiceRequest::Lease {
                        owner: second,
                        purpose: "second".into(),
                        ttl_ms: 5_000,
                    },
                    Duration::from_secs(3)
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("another owner")
        );
        assert_eq!(
            manager.read_status("fake").unwrap().clients[0].id,
            held.clients[0].id
        );
        cleanup(&manager, task).await;
    }

    #[tokio::test]
    async fn shared_clients_have_independent_limits_release_and_expiry() {
        let (root, manager, _front, task) = setup_with(|s| {
            s.client_mode = ClientMode::Shared { max_clients: 2 };
        })
        .await;
        let first = owner();
        let second = owner();
        let third = owner();
        let first_status = manager
            .send(
                "fake",
                ServiceRequest::Lease {
                    owner: first.clone(),
                    purpose: "first".into(),
                    ttl_ms: 800,
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        let first_id = first_status.clients[0].id;
        let second_status = manager
            .send(
                "fake",
                ServiceRequest::Lease {
                    owner: second.clone(),
                    purpose: "second".into(),
                    ttl_ms: 5_000,
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        assert_eq!(second_status.clients.len(), 2);
        let second_id = second_status
            .clients
            .iter()
            .find(|l| l.owner.participant_id == second.participant_id)
            .unwrap()
            .id;
        assert_ne!(first_id, second_id);
        assert!(
            manager
                .send(
                    "fake",
                    ServiceRequest::Lease {
                        owner: third.clone(),
                        purpose: "third".into(),
                        ttl_ms: 5_000,
                    },
                    Duration::from_secs(3)
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("client limit")
        );
        assert_eq!(manager.read_status("fake").unwrap().clients.len(), 2);
        let store = LaneStore::new(root.path()).unwrap();
        assert_eq!(
            store
                .snapshot()
                .unwrap()
                .iter()
                .filter(|row| row.service_lease
                    && matches!(row.state, crate::lanes::TicketState::Granted { .. }))
                .count(),
            1,
            "two clients still reserve host capacity once, per service"
        );
        let after_expiry = state(&manager, |s| {
            s.clients.len() == 1 && s.clients[0].id == second_id
        })
        .await;
        assert!(matches!(after_expiry.state, ServiceState::Healthy { .. }));
        let restored = fs::read_to_string(root.path().join("restored")).unwrap();
        assert_eq!(
            restored
                .lines()
                .filter(|line| *line == first.participant_id.to_string())
                .count(),
            1
        );
        assert!(!restored.contains(&second.participant_id.to_string()));
        let third_status = manager
            .send(
                "fake",
                ServiceRequest::Lease {
                    owner: third.clone(),
                    purpose: "third".into(),
                    ttl_ms: 5_000,
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        let third_id = third_status
            .clients
            .iter()
            .find(|l| l.owner.participant_id == third.participant_id)
            .unwrap()
            .id;
        assert!(
            manager
                .send(
                    "fake",
                    ServiceRequest::Release {
                        lease_id: second_id,
                        owner: third.clone(),
                    },
                    Duration::from_secs(3)
                )
                .await
                .is_err()
        );
        manager
            .send(
                "fake",
                ServiceRequest::Release {
                    lease_id: third_id,
                    owner: third.clone(),
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        assert_eq!(
            manager.read_status("fake").unwrap().clients[0].id,
            second_id
        );
        manager
            .send(
                "fake",
                ServiceRequest::Release {
                    lease_id: second_id,
                    owner: second.clone(),
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        let restored = fs::read_to_string(root.path().join("restored")).unwrap();
        for client in [&first, &second, &third] {
            assert_eq!(
                restored
                    .lines()
                    .filter(|line| *line == client.participant_id.to_string())
                    .count(),
                1
            );
        }
        cleanup(&manager, task).await;
    }

    #[tokio::test]
    async fn restore_failure_retains_other_clients_and_prevents_stop_release() {
        let owners = [owner(), owner()];
        let reject = owners[1].participant_id.to_string();
        let (root, manager, _front, task) = setup_with(|spec| {
            spec.client_mode = ClientMode::Shared { max_clients: 2 };
            spec.restore = Some(Hook {
                argv: vec![
                    "python3".into(),
                    "-c".into(),
                    "import pathlib,sys; marker=pathlib.Path(sys.argv[1]); owner=sys.argv[2]; "
                        .to_owned()
                        + "
if marker.exists() and owner == sys.argv[3]: sys.exit(4)"
                        + "
with open(sys.argv[4], 'a') as out: out.write(owner+chr(10))",
                    spec.cwd.join("restore-blocked").display().to_string(),
                    "{owner}".into(),
                    reject.clone(),
                    spec.cwd.join("restored").display().to_string(),
                ],
                timeout_ms: 1500,
            });
        })
        .await;
        for holder in &owners {
            manager
                .send(
                    "fake",
                    ServiceRequest::Lease {
                        owner: holder.clone(),
                        purpose: "independent".into(),
                        ttl_ms: 5_000,
                    },
                    Duration::from_secs(3),
                )
                .await
                .unwrap();
        }
        let pid = manager.read_status("fake").unwrap().backend_pid;
        fs::write(root.path().join("restore-blocked"), "hold second").unwrap();
        assert!(
            manager
                .send("fake", ServiceRequest::Stop, Duration::from_secs(4))
                .await
                .is_err()
        );
        let after = manager.read_status("fake").unwrap();
        assert!(matches!(after.state, ServiceState::Healthy { .. }));
        assert_eq!(
            after.backend_pid, pid,
            "failed restore cannot stop a shared backend"
        );
        assert_eq!(
            after.clients.len(),
            1,
            "successful first restore persists; failed second remains"
        );
        assert_eq!(
            after.clients[0].owner.participant_id,
            owners[1].participant_id
        );
        assert!(
            after
                .reason
                .contains(&format!("owner={}", owners[1].participant_id))
        );
        assert!(
            after
                .reason
                .contains(&format!("lease_id={}", after.clients[0].id))
        );
        let persisted: ServiceStatus = read_json(
            &service_dir(&manager.root, "fake")
                .unwrap()
                .join("state.json"),
        )
        .unwrap();
        assert_eq!(
            persisted.clients.len(),
            1,
            "unrestored client remains durable"
        );
        assert!(
            manager
                .send(
                    "fake",
                    ServiceRequest::Yield {
                        by: "blocked-exclusive".into(),
                        reason: "must restore both".into(),
                        ttl_ms: 5_000,
                    },
                    Duration::from_secs(4)
                )
                .await
                .is_err()
        );
        let after_yield = manager.read_status("fake").unwrap();
        assert!(after_yield.yields.is_empty());
        assert_eq!(after_yield.clients.len(), 1);
        assert_eq!(
            after_yield.backend_pid, pid,
            "failed restore cannot grant exclusivity"
        );
        assert!(
            after_yield
                .reason
                .contains(&format!("owner={}", owners[1].participant_id))
        );
        assert!(
            after_yield
                .reason
                .contains(&format!("lease_id={}", after_yield.clients[0].id))
        );
        // A successful release by someone else must not hide the stuck owner.
        let other = owner();
        let with_other = manager
            .send(
                "fake",
                ServiceRequest::Lease {
                    owner: other.clone(),
                    purpose: "other".into(),
                    ttl_ms: 5_000,
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        let other_id = with_other
            .clients
            .iter()
            .find(|l| l.owner.participant_id == other.participant_id)
            .unwrap()
            .id;
        let released_other = manager
            .send(
                "fake",
                ServiceRequest::Release {
                    lease_id: other_id,
                    owner: other.clone(),
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        assert_eq!(released_other.clients.len(), 1);
        assert!(
            released_other
                .reason
                .contains(&format!("lease_id={}", after.clients[0].id))
        );
        let store = LaneStore::new(root.path()).unwrap();
        assert!(store.snapshot().unwrap().iter().any(|row| row.service_lease
            && matches!(row.state, crate::lanes::TicketState::Granted { .. })));
        fs::remove_file(root.path().join("restore-blocked")).unwrap();
        cleanup(&manager, task).await;
        let restored = fs::read_to_string(root.path().join("restored")).unwrap();
        for holder in &owners {
            assert_eq!(
                restored
                    .lines()
                    .filter(|line| *line == holder.participant_id.to_string())
                    .count(),
                1
            );
        }
        assert_eq!(
            restored
                .lines()
                .filter(|line| *line == other.participant_id.to_string())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn yielding_shared_service_restores_every_owner_before_releasing_backend() {
        let (root, manager, front, task) = setup_with(|s| {
            s.client_mode = ClientMode::Shared { max_clients: 2 };
        })
        .await;
        let owners = [owner(), owner()];
        for holder in &owners {
            manager
                .send(
                    "fake",
                    ServiceRequest::Lease {
                        owner: holder.clone(),
                        purpose: "independent client".into(),
                        ttl_ms: 5_000,
                    },
                    Duration::from_secs(3),
                )
                .await
                .unwrap();
        }
        let yielded = manager
            .send(
                "fake",
                ServiceRequest::Yield {
                    by: "exclusive-job".into(),
                    reason: "exclusive".into(),
                    ttl_ms: 5_000,
                },
                Duration::from_secs(8),
            )
            .await
            .unwrap();
        assert!(matches!(yielded.state, ServiceState::Yielded));
        assert!(yielded.clients.is_empty());
        assert!(yielded.backend_pid.is_none());
        assert!(
            get(front, "/health")
                .await
                .unwrap()
                .contains("503 Service Unavailable")
        );
        let restored = fs::read_to_string(root.path().join("restored")).unwrap();
        for holder in &owners {
            assert_eq!(
                restored
                    .lines()
                    .filter(|line| *line == holder.participant_id.to_string())
                    .count(),
                1
            );
        }
        let store = LaneStore::new(root.path()).unwrap();
        assert!(
            !store.snapshot().unwrap().iter().any(|row| row.service_lease
                && matches!(row.state, crate::lanes::TicketState::Granted { .. }))
        );
        manager
            .send(
                "fake",
                ServiceRequest::Resume {
                    by: "exclusive-job".into(),
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        state(&manager, |s| {
            matches!(s.state, ServiceState::Healthy { .. })
        })
        .await;
        cleanup(&manager, task).await;
    }

    #[tokio::test]
    async fn active_lease_restored_before_yield_and_owner_controls_resume() {
        let (root, manager, front, task) = setup().await;
        let holder = owner();
        let lease = manager
            .send(
                "fake",
                ServiceRequest::Lease {
                    owner: holder.clone(),
                    purpose: "capture".into(),
                    ttl_ms: 250,
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap()
            .clients
            .remove(0);
        manager
            .send(
                "fake",
                ServiceRequest::Yield {
                    by: "job".into(),
                    reason: "exclusive".into(),
                    ttl_ms: 5000,
                },
                Duration::from_secs(8),
            )
            .await
            .unwrap();
        state(&manager, |s| s.clients.is_empty()).await;
        assert!(
            fs::read_to_string(root.path().join("restored"))
                .unwrap()
                .contains(&holder.participant_id.to_string())
        );
        assert_ne!(lease.id, Uuid::nil());
        let yielded = manager.read_status("fake").unwrap();
        assert!(matches!(yielded.state, ServiceState::Yielded));
        assert!(yielded.backend_pid.is_none());
        assert!(
            get(front, "/health")
                .await
                .unwrap()
                .contains("503 Service Unavailable")
        );
        let mut during_yield = TcpStream::connect(("127.0.0.1", front)).await.unwrap();
        during_yield
            .write_all(b"POST /mcp HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        during_yield.read_to_end(&mut response).await.unwrap();
        assert!(
            String::from_utf8(response)
                .unwrap()
                .contains("503 Service Unavailable")
        );
        assert!(
            manager
                .send(
                    "fake",
                    ServiceRequest::Resume {
                        by: "other-job".into()
                    },
                    Duration::from_secs(3)
                )
                .await
                .is_err()
        );
        manager
            .send(
                "fake",
                ServiceRequest::Resume { by: "job".into() },
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        state(&manager, |s| {
            matches!(s.state, ServiceState::Healthy { .. })
        })
        .await;
        let second = owner();
        let held = manager
            .send(
                "fake",
                ServiceRequest::Lease {
                    owner: second.clone(),
                    purpose: "edit".into(),
                    ttl_ms: 1000,
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap()
            .clients
            .remove(0);
        assert!(
            manager
                .send(
                    "fake",
                    ServiceRequest::Release {
                        lease_id: held.id,
                        owner: owner()
                    },
                    Duration::from_secs(3)
                )
                .await
                .is_err()
        );
        manager
            .send(
                "fake",
                ServiceRequest::Release {
                    lease_id: held.id,
                    owner: second.clone(),
                },
                Duration::from_secs(3),
            )
            .await
            .unwrap();
        assert!(
            fs::read_to_string(root.path().join("restored"))
                .unwrap()
                .contains(&second.participant_id.to_string())
        );
        cleanup(&manager, task).await;
    }

    #[tokio::test]
    async fn proxy_defaults_to_no_unfenced_backend_paths() {
        let (_root, manager, front, task) = setup_with(|s| s.read_only_paths.clear()).await;
        assert!(
            get(front, "/health")
                .await
                .unwrap()
                .starts_with("HTTP/1.1 403")
        );
        cleanup(&manager, task).await;
    }

    #[tokio::test]
    async fn unfenced_proxy_rejects_pipelined_and_late_mutations() {
        let (root, manager, port, task) = setup().await;
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        stream.write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\nPOST /bad HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n").await.unwrap();
        let mut reply = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut reply))
            .await
            .unwrap()
            .unwrap();
        assert!(reply.starts_with(b"HTTP/1.1 403"));
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        stream
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        // The subsequent POST must never be passed through the established GET tunnel.
        tokio::time::sleep(Duration::from_millis(25)).await;
        let _ = stream
            .write_all(b"POST /bad HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n")
            .await;
        let mut reply = Vec::new();
        let read = tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut reply))
            .await
            .unwrap();
        // Closing a connection with the denied POST still unread may yield
        // TCP RST after the first response; it must never forward the POST.
        if let Err(error) = read {
            assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
        }
        assert!(
            reply.starts_with(b"HTTP/1.0 200") || reply.starts_with(b"HTTP/1.1 403"),
            "reply: {:?}",
            String::from_utf8_lossy(&reply)
        );
        assert_eq!(reply.windows(7).filter(|w| *w == b"HTTP/1.").count(), 1);
        assert!(
            !root.path().join("mutated").exists(),
            "POST reached backend"
        );
        cleanup(&manager, task).await;
    }

    #[tokio::test]
    async fn mcp_probe_requires_initialize_result_not_just_http_200() {
        let (root, manager, _front, task) = setup().await;
        let mut spec: ServiceSpec = read_json(&manager.root.join("fake/spec.json")).unwrap();
        spec.health.kind = HealthKind::McpInitialize;
        spec.health.argv = vec!["/mcp".into()];
        let port = manager.read_status("fake").unwrap().state_backend();
        assert!(probe(&spec, port).await);
        spec.health.argv = vec!["/invalid".into()];
        assert!(
            !probe(&spec, port).await,
            "HTTP 200 without MCP result is unhealthy"
        );
        assert!(root.path().join("launches").exists());
        cleanup(&manager, task).await;
    }

    /// Answers the first request with `reply` (then holds the connection
    /// open when `keep_open`, as a keep-alive server may) and records the
    /// head of every request it receives.
    async fn canned_backend(
        reply: String,
        keep_open: bool,
    ) -> (u16, Arc<std::sync::Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let log = seen.clone();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut request = Vec::new();
                let mut chunk = [0; 4096];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    match stream.read(&mut chunk).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => request.extend_from_slice(&chunk[..n]),
                    }
                }
                let first = {
                    let mut log = log.lock().unwrap();
                    log.push(String::from_utf8_lossy(&request).into_owned());
                    log.len() == 1
                };
                let answer = if first {
                    reply.as_str()
                } else {
                    "HTTP/1.1 204 No Content\r\n\r\n"
                };
                let _ = stream.write_all(answer.as_bytes()).await;
                if first && keep_open {
                    held.push(stream);
                }
            }
        });
        (port, seen)
    }

    /// Failure mode: a real MCP server's valid initialize reply judged
    /// unhealthy for its framing (pretty JSON, chunks, SSE events), an error
    /// or non-2xx reply judged healthy, or every probe leaking a session.
    #[tokio::test]
    async fn mcp_probe_reads_whole_replies_and_ends_its_session() {
        let root = tempfile::tempdir().unwrap();
        let mut spec = spec(root.path());
        spec.health.kind = HealthKind::McpInitialize;
        spec.health.argv = vec!["/mcp".into()];
        spec.health.timeout_ms = 2000;
        // Shaped like the Unreal editor's reply: tab-indented, one key per line.
        let pretty = "{\n\t\"jsonrpc\": \"2.0\",\n\t\"id\": 1,\n\t\"result\":\n\t{\n\t\t\"protocolVersion\": \"2025-11-25\",\n\t\t\"capabilities\":\n\t\t{\n\t\t\t\"tools\":\n\t\t\t{\n\t\t\t\t\"listChanged\": true\n\t\t\t}\n\t\t}\n\t}\n}";
        let failed = "{\n\t\"jsonrpc\": \"2.0\",\n\t\"id\": 1,\n\t\"error\":\n\t{\n\t\t\"code\": -32600,\n\t\t\"message\": \"refused\"\n\t}\n}";
        let single = r#"{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25"}}"#;
        let sized = |status: &str, headers: &str, body: &str| {
            format!(
                "HTTP/1.1 {status}\r\n{headers}Content-Length: {}\r\n\r\n{body}",
                body.len()
            )
        };
        let chunked = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n{}0\r\n\r\n",
            pretty
                .as_bytes()
                .chunks(29)
                .map(|piece| format!(
                    "{:x}\r\n{}\r\n",
                    piece.len(),
                    String::from_utf8_lossy(piece)
                ))
                .collect::<String>()
        );
        let events = |body: &str| {
            let data: Vec<String> = body.lines().map(|line| format!("data: {line}")).collect();
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\nevent: message\r\n{}\r\n\r\n",
                data.join("\r\n")
            )
        };
        let session = "Mcp-Session-Id: 01a0cd29435d7decaf32fb7a0851b1d9\r\n";
        for (shape, reply, keep_open, healthy) in [
            (
                "pretty JSON with Content-Length, kept alive",
                sized(
                    "200",
                    &format!("content-type: application/json;charset=utf-8\r\n{session}"),
                    pretty,
                ),
                true,
                true,
            ),
            ("chunked pretty JSON", chunked, true, true),
            (
                "single-line JSON",
                sized("200 OK", "Content-Type: application/json\r\n", single),
                true,
                true,
            ),
            (
                "SSE event with multi-line data",
                events(pretty),
                false,
                true,
            ),
            ("SSE error event", events(failed), false, false),
            (
                "HTTP 200 with a JSON-RPC error",
                sized("200 OK", "Content-Type: application/json\r\n", failed),
                true,
                false,
            ),
            (
                "non-2xx with a result",
                sized(
                    "503 Service Unavailable",
                    "Content-Type: application/json\r\n",
                    single,
                ),
                true,
                false,
            ),
        ] {
            let (port, seen) = canned_backend(reply, keep_open).await;
            assert_eq!(probe(&spec, Some(port)).await, healthy, "{shape}");
            if shape.starts_with("pretty") {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
                loop {
                    let heads = seen.lock().unwrap().clone();
                    if let Some(delete) = heads.get(1) {
                        assert!(delete.starts_with("DELETE /mcp HTTP/1.1\r\n"), "{delete}");
                        assert!(delete.contains(session), "{delete}");
                        break;
                    }
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "no session DELETE: {heads:?}"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
    }

    #[tokio::test]
    async fn idle_hook_runs_once_and_touch_wakes_backend() {
        let (root, manager, _front, task) = setup_with(|spec| {
            let file = spec.cwd.join("idle-events").display().to_string();
            let hook = |name: &str| Hook {
                argv: vec![
                    "python3".into(),
                    "-c".into(),
                    "import sys; open(sys.argv[1], 'a').write(sys.argv[2]+'\\n')".into(),
                    file.clone(),
                    name.into(),
                ],
                timeout_ms: 1000,
            };
            spec.idle = Some(hook("idle"));
            spec.active = Some(hook("active"));
            spec.idle_after_ms = 200;
        })
        .await;
        let path = root.path().join("idle-events");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while !path.exists() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "idle hook never fired"
            );
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        manager
            .send("fake", ServiceRequest::Touch, Duration::from_secs(3))
            .await
            .unwrap();
        assert_eq!(
            fs::read_to_string(path)
                .unwrap()
                .lines()
                .take(2)
                .collect::<Vec<_>>(),
            vec!["idle", "active"]
        );
        cleanup(&manager, task).await;
    }

    #[tokio::test]
    async fn failed_active_hook_does_not_leave_client_lease() {
        let (root, manager, _front, task) = setup_with(|spec| {
            spec.idle_after_ms = 200;
            spec.idle = Some(Hook {
                argv: vec![
                    "python3".into(),
                    "-c".into(),
                    "import sys; open(sys.argv[1], 'w').write('idle')".into(),
                    spec.cwd.join("idle-marker").display().to_string(),
                ],
                timeout_ms: 1000,
            });
            spec.active = Some(Hook {
                argv: vec!["python3".into(), "-c".into(), "raise SystemExit(42)".into()],
                timeout_ms: 1000,
            });
        })
        .await;
        let marker = root.path().join("idle-marker");
        tokio::time::timeout(Duration::from_secs(3), async {
            while !marker.exists() {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("service never entered idle state");
        let denied = manager
            .send(
                "fake",
                ServiceRequest::Lease {
                    owner: owner(),
                    purpose: "failed activation".into(),
                    ttl_ms: 5000,
                },
                Duration::from_secs(3),
            )
            .await;
        assert!(
            denied.is_err(),
            "failing active hook granted a client lease"
        );
        assert!(
            manager.read_status("fake").unwrap().clients.is_empty(),
            "failing active hook left a ghost client lease"
        );
        cleanup(&manager, task).await;
    }

    #[tokio::test]
    async fn crash_and_hang_restart_without_losing_proxy_listener() {
        let (root, manager, front, task) = setup().await;
        let first = manager.read_status("fake").unwrap().backend_pid;
        let _ = get(front, "/crash").await;
        let after_crash = state(&manager, |s| {
            matches!(s.state, ServiceState::Healthy { .. }) && s.backend_pid != first
        })
        .await;
        assert!(after_crash.restarts >= 1);
        fs::write(root.path().join("hang"), "1").unwrap();
        let after_hang = state(&manager, |s| s.restarts > after_crash.restarts).await;
        assert!(
            after_hang.backend_pid.is_none() || after_hang.backend_pid != after_crash.backend_pid
        );
        fs::remove_file(root.path().join("hang")).unwrap();
        state(&manager, |s| {
            matches!(s.state, ServiceState::Healthy { .. })
        })
        .await;
        cleanup(&manager, task).await;
    }
    trait BackendPort {
        fn state_backend(&self) -> Option<u16>;
    }
    impl BackendPort for ServiceStatus {
        fn state_backend(&self) -> Option<u16> {
            if let ServiceState::Healthy { backend } = self.state {
                backend
            } else {
                None
            }
        }
    }
}
