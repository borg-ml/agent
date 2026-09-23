//! Persistent, supervised services; independent of requesting Borg sessions.

use std::{collections::BTreeMap, fs::{self, File, OpenOptions}, io::Write, net::{IpAddr, SocketAddr}, os::fd::AsRawFd, path::{Path, PathBuf}, process::Stdio, sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, process::{Child, Command}, sync::RwLock};
use anyhow::{bail, ensure, Context};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::lanes::{AdmissionBudget, Holder, Hook, ResourceRequest};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthCheck {
    pub argv: Vec<String>,
    #[serde(default)] pub kind: HealthKind,
    pub interval_ms: u64,
    pub timeout_ms: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthKind { #[default] Command, Http, McpInitialize }

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
    #[serde(default = "default_ready_timeout_ms")] pub readiness_timeout_ms: u64,
    #[serde(default)] pub graceful_stop: Option<Hook>,
    #[serde(default)] pub idle: Option<Hook>,
    #[serde(default)] pub active: Option<Hook>,
    #[serde(default = "default_idle_after_ms")] pub idle_after_ms: u64,
}
fn default_ready_timeout_ms() -> u64 { 120_000 }
fn default_idle_after_ms() -> u64 { 60_000 }

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
    #[serde(default)] pub purpose: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceStatus {
    pub id: String,
    pub state: ServiceState,
    pub endpoint: Option<Endpoint>,
    pub clients: Vec<ClientLease>,
    #[serde(default)] pub reason: String,
    #[serde(default)] pub backend_pid: Option<u32>,
    #[serde(default)] pub supervisor_pid: Option<u32>,
    #[serde(default)] pub restarts: u32,
    #[serde(default)] pub yields: BTreeMap<String, YieldWindow>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct YieldWindow { pub by: String, pub reason: String, pub expires_at_unix_ms: u64 }

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ServiceRequest {
    Stop,
    Restart { reason: String, force: bool },
    Yield { by: String, reason: String, ttl_ms: u64 },
    Resume { by: String },
    Lease { owner: Holder, purpose: String, ttl_ms: u64 },
    Release { lease_id: Uuid, owner: Holder },
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
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

/// The runtime root can be private to a test, user, or installation. IDs never become paths unchecked.
pub fn service_root() -> PathBuf {
    std::env::var_os("BORG_LANES_ROOT").map(PathBuf::from).unwrap_or_else(|| {
        std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("/tmp"))
            .join(format!("borg-{}", unsafe { libc::geteuid() })).join("lanes")
    }).join("services")
}
fn service_dir(root: &Path, id: &str) -> Result<PathBuf> {
    ensure!(!id.is_empty() && id.len() <= 100 && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'), "invalid service id");
    let dir = root.join(id);
    fs::create_dir_all(&dir)?;
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
    Ok(dir)
}
fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let temporary = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
    let mut file = OpenOptions::new().write(true).create_new(true).open(&temporary)?;
    file.write_all(&serde_json::to_vec(value)?)?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    Ok(())
}
fn read_json<T: for<'a> Deserialize<'a>>(path: &Path) -> Result<T> {
    serde_json::from_slice(&fs::read(path)?).with_context(|| format!("invalid JSON: {}", path.display()))
}
fn lock(path: &Path, nonblocking: bool) -> Result<File> {
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let mode = libc::LOCK_EX | if nonblocking { libc::LOCK_NB } else { 0 };
    ensure!(unsafe { libc::flock(file.as_raw_fd(), mode) } == 0, "service already running");
    Ok(file)
}
fn valid_spec(spec: &ServiceSpec) -> Result<()> {
    service_dir(&service_root(), &spec.id)?;
    ensure!(!spec.argv.is_empty() && !spec.argv[0].is_empty() && spec.cwd.is_dir(), "service needs argv and existing cwd");
    ensure!(spec.health.timeout_ms > 0 && spec.health.interval_ms > 0 && spec.readiness_timeout_ms > 0, "service health timeouts must be positive");
    if let Some(endpoint) = &spec.endpoint {
        let addr: SocketAddr = endpoint.listen.parse().context("invalid endpoint listen address")?;
        ensure!(addr.ip().is_loopback() && addr.port() != 0, "service front endpoint must be loopback");
        ensure!(endpoint.backend_ports[0] != endpoint.backend_ports[1]
            && endpoint.backend_ports.iter().all(|p| *p != 0 && *p != addr.port()), "backend ports must be distinct and not front port");
    } else { ensure!(matches!(spec.health.kind, HealthKind::Command), "HTTP/MCP probe needs endpoint"); }
    if matches!(spec.health.kind, HealthKind::Command) {
        ensure!(!spec.health.argv.is_empty(), "command health probe needs argv");
    }
    Ok(())
}
fn stopped(id: &str) -> ServiceStatus {
    ServiceStatus { id: id.into(), state: ServiceState::Stopped, endpoint: None, clients: vec![], reason: "not started".into(),
        supervisor_pid: None, backend_pid: None, restarts: 0, yields: BTreeMap::new() }
}
fn is_running(dir: &Path) -> bool {
    match lock(&dir.join("supervisor.lock"), true) { Ok(_) => false, Err(_) => true }
}

/// Client-side command transport. Requests are atomically published and answered by the
/// supervisor; all lease/yield mutations are serial, never read/modify/write races in clients.
pub async fn service_request(root: &Path, id: &str, request: ServiceRequest, timeout: Duration) -> Result<ServiceStatus> {
    let dir = service_dir(root, id)?;
    ensure!(is_running(&dir), "service {id} is not running");
    let nonce = Uuid::new_v4();
    let path = dir.join(format!("req-{nonce}.json"));
    let reply = dir.join(format!("reply-{nonce}.json"));
    write_json(&path, &request)?;
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if reply.exists() {
            let response: serde_json::Value = read_json(&reply)?;
            fs::remove_file(&reply)?;
            if let Some(error) = response.get("error").and_then(|v| v.as_str()) { bail!("{error}"); }
            return serde_json::from_value(response).context("invalid service reply");
        }
        if tokio::time::Instant::now() >= deadline {
            // The request may still execute. Do not claim cancellation on timeout.
            bail!("service {id} request timed out; inspect status before retrying");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub struct ServiceManager { pub root: PathBuf, pub executable: PathBuf }
impl ServiceManager {
    pub fn new(root: PathBuf, executable: PathBuf) -> Self { Self { root, executable } }
    pub fn current() -> Result<Self> { Ok(Self::new(service_root(), std::env::current_exe()?)) }
    pub fn read_status(&self, id: &str) -> Result<ServiceStatus> {
        let dir = service_dir(&self.root, id)?;
        let mut status: ServiceStatus = read_json(&dir.join("state.json")).unwrap_or_else(|_| stopped(id));
        if !is_running(&dir) {
            status.state = ServiceState::Stopped;
            status.reason = "supervisor not running".into();
            status.backend_pid = None;
        }
        Ok(status)
    }
    pub async fn send(&self, id: &str, request: ServiceRequest, timeout: Duration) -> Result<ServiceStatus> {
        service_request(&self.root, id, request, timeout).await
    }
    pub async fn launch(&self, spec: ServiceSpec) -> Result<ServiceStatus> {
        valid_spec(&spec)?;
        let dir = service_dir(&self.root, &spec.id)?;
        let _start_lock = lock(&dir.join("start.lock"), false)?;
        if is_running(&dir) { return self.read_status(&spec.id); }
        write_json(&dir.join("spec.json"), &spec)?;
        let log = OpenOptions::new().create(true).append(true).open(dir.join("output.log"))?;
        let args = ["lane", "service", "supervise", &spec.id];
        let has_systemd = std::process::Command::new("systemctl").args(["--user", "show-environment"])
            .stdout(Stdio::null()).stderr(Stdio::null()).status().is_ok_and(|s| s.success());
        if has_systemd {
            let unit = format!("borg-service-{}-{}", spec.id, Uuid::new_v4().simple());
            let mut command = std::process::Command::new("systemd-run");
            command.args(["--user", "--collect", "--quiet", &format!("--unit={unit}"),
                &format!("--working-directory={}", spec.cwd.display()), "-p", "KillMode=process"]);
            if let Some(bytes) = spec.memory_max_bytes { command.args(["-p", &format!("MemoryMax={bytes}")]); }
            command.arg(format!("--setenv=BORG_LANES_ROOT={}", self.root.parent().context("invalid root")?.display()));
            command.arg(&self.executable).args(args);
            ensure!(command.status().context("systemd-run failed")?.success(), "could not launch service user unit");
            write_json(&dir.join("unit.json"), &unit)?;
        } else {
            use std::os::unix::process::CommandExt;
            let mut command = std::process::Command::new(&self.executable);
            command.args(args).env("BORG_LANES_ROOT", self.root.parent().context("invalid root")?)
                .stdin(Stdio::null()).stdout(log.try_clone()?).stderr(log);
            unsafe { command.pre_exec(|| { if libc::setsid() == -1 { return Err(std::io::Error::last_os_error()); } Ok(()) }); }
            command.spawn().context("setsid service supervisor")?;
        }
        Ok(self.read_status(&spec.id)?)
    }
}

fn expand(value: &str, port: Option<u16>, owner: Option<&str>) -> String {
    value.replace("{port}", &port.unwrap_or(0).to_string()).replace("{owner}", owner.unwrap_or(""))
}
fn command(argv: &[String], spec: &ServiceSpec, port: Option<u16>, owner: Option<&str>) -> Result<Command> {
    let (program, args) = argv.split_first().context("hook/service command argv is empty")?;
    let mut cmd = Command::new(expand(program, port, owner));
    cmd.args(args.iter().map(|arg| expand(arg, port, owner))).current_dir(&spec.cwd);
    for (key, value) in &spec.env { cmd.env(key, expand(value, port, owner)); }
    if let Some(port) = port { cmd.env("BORG_SERVICE_BACKEND_PORT", port.to_string()); }
    if let Some(owner) = owner { cmd.env("BORG_SERVICE_OWNER", owner); }
    Ok(cmd)
}
async fn hook(hook: &Hook, spec: &ServiceSpec, port: Option<u16>, owner: Option<&str>) -> Result<()> {
    let mut cmd = command(&hook.argv, spec, port, owner)?;
    cmd.kill_on_drop(true).stdout(Stdio::null()).stderr(Stdio::null());
    let status = tokio::time::timeout(Duration::from_millis(hook.timeout_ms.max(1)), cmd.status()).await
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
                let body = if matches!(spec.health.kind, HealthKind::McpInitialize) {
                    r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"borg-services","version":"1"}}}"#
                } else { "" };
                let method = if body.is_empty() { "GET" } else { "POST" };
                stream.write_all(format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await?;
                let mut reply = vec![0; 4096];
                let count = stream.read(&mut reply).await?;
                let response = String::from_utf8_lossy(&reply[..count]);
                Ok(response.starts_with("HTTP/1.1 2") || response.starts_with("HTTP/1.0 2"))
            }
        }
    }).await.is_ok_and(|result: Result<bool>| result.unwrap_or(false))
}

#[derive(Clone)]
struct FrontState { backend: Option<u16>, state: ServiceState, reason: String }
async fn front_connection(mut client: TcpStream, front: Arc<RwLock<FrontState>>) {
    let snapshot = front.read().await.clone();
    if let Some(port) = snapshot.backend {
        if let Ok(Ok(mut backend)) = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(("127.0.0.1", port))).await {
            let _ = tokio::io::copy_bidirectional(&mut client, &mut backend).await;
            return;
        }
    }
    // Consume a bounded HTTP header so callers receive a clean 503, not a reset.
    let mut buf = [0; 2048];
    let _ = tokio::time::timeout(Duration::from_millis(250), client.read(&mut buf)).await;
    let body = serde_json::json!({ "error":"service unavailable", "state":snapshot.state,
        "reason":snapshot.reason, "retry_after_seconds":1 }).to_string();
    let response = format!("HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: {}\r\nRetry-After: 1\r\nConnection: close\r\n\r\n{body}", body.len());
    let _ = client.write_all(response.as_bytes()).await;
}
async fn front_server(listener: TcpListener, front: Arc<RwLock<FrontState>>) {
    while let Ok((client, _)) = listener.accept().await {
        tokio::spawn(front_connection(client, front.clone()));
    }
}
fn publish(dir: &Path, status: &ServiceStatus) -> Result<()> {
    write_json(&dir.join("state.json"), status)
}
async fn transition(dir: &Path, status: &mut ServiceStatus, front: &Arc<RwLock<FrontState>>,
    state: ServiceState, reason: impl Into<String>, backend: Option<u16>) -> Result<()> {
    status.state = state.clone(); status.reason = reason.into();
    { let mut current = front.write().await; current.state = state; current.reason = status.reason.clone(); current.backend = backend; }
    publish(dir, status)
}
async fn stop_child(child: &mut Child, spec: &ServiceSpec, port: Option<u16>) {
    let pid = child.id();
    if child.try_wait().ok().flatten().is_some() { return; }
    if let Some(hook) = &spec.graceful_stop { let _ = hook(hook, spec, port, None).await; }
    if tokio::time::timeout(Duration::from_secs(2), child.wait()).await.is_ok() { return; }
    if let Some(pid) = pid { unsafe { libc::kill(-(pid as i32), libc::SIGTERM); } }
    if tokio::time::timeout(Duration::from_secs(3), child.wait()).await.is_ok() { return; }
    if let Some(pid) = pid { unsafe { libc::kill(-(pid as i32), libc::SIGKILL); } }
    let _ = child.wait().await;
}
fn spawn_backend(spec: &ServiceSpec, port: Option<u16>, log: &File) -> Result<Child> {
    use std::os::unix::process::CommandExt;
    let mut cmd = command(&spec.argv, spec, port, None)?;
    cmd.stdin(Stdio::null()).stdout(log.try_clone()?).stderr(log.try_clone()?);
    cmd.as_std_mut().process_group(0); // Only the group's recorded PID can be signalled.
    Ok(cmd.spawn().context("spawn service backend")?)
}

struct Backend { child: Child, port: Option<u16>, started_ms: u64, last_probe_ms: u64, unhealthy_since_ms: Option<u64> }
fn next_port(spec: &ServiceSpec, active: Option<&Backend>) -> Option<u16> {
    spec.endpoint.as_ref().map(|endpoint| {
        endpoint.backend_ports.iter().copied()
            .find(|port| Some(*port) != active.and_then(|b| b.port))
            .unwrap_or(endpoint.backend_ports[0])
    })
}
async fn restore_client(spec: &ServiceSpec, lease: &ClientLease, port: Option<u16>) -> Result<()> {
    if let Some(restore) = &spec.restore {
        hook(restore, spec, port, Some(&lease.owner.participant_id.to_string())).await?;
    }
    Ok(())
}

/// Run in the private transient user unit (or in a detached setsid process). The lock FD
/// outlives this task; a second process cannot attach a competing backend or public port.
pub async fn supervise(root: &Path, id: &str) -> Result<()> {
    let dir = service_dir(root, id)?;
    let _lock = lock(&dir.join("supervisor.lock"), true)?;
    let spec: ServiceSpec = read_json(&dir.join("spec.json"))?;
    ensure!(spec.id == id, "service spec identity mismatch");
    valid_spec(&spec)?;
    let log = OpenOptions::new().create(true).append(true).open(dir.join("output.log"))?;
    let mut status: ServiceStatus = read_json(&dir.join("state.json")).unwrap_or_else(|_| stopped(id));
    status.endpoint = spec.endpoint.clone();
    status.supervisor_pid = Some(std::process::id());
    status.backend_pid = None;
    // Crash recovery cannot assume that the previous backend still owns settings.
    for lease in std::mem::take(&mut status.clients) {
        if let Err(error) = restore_client(&spec, &lease, None).await {
            status.state = ServiceState::Failed { reason: error.to_string() };
            status.reason = format!("restore after supervisor crash failed for {}: {error}", lease.owner.participant_id);
            status.clients.push(lease);
            publish(&dir, &status)?;
            bail!("{}", status.reason);
        }
    }
    let front = Arc::new(RwLock::new(FrontState { backend: None, state: ServiceState::Starting, reason: "starting".into() }));
    if let Some(endpoint) = &spec.endpoint {
        let addr: SocketAddr = endpoint.listen.parse()?;
        ensure!(matches!(addr.ip(), IpAddr::V4(_) | IpAddr::V6(_)), "invalid listener");
        let listener = TcpListener::bind(addr).await.with_context(|| format!("front endpoint {} busy", endpoint.listen))?;
        tokio::spawn(front_server(listener, front.clone()));
    }
    transition(&dir, &mut status, &front, ServiceState::Starting, "supervisor started", None).await?;
    let mut active: Option<Backend> = None;
    let mut candidate: Option<Backend> = None;
    let mut pending: Option<(u64, String)> = None;
    let mut last_activity = unix_ms();
    let mut idle = false;
    let mut failures = 0u32;
    let mut next_launch = 0u64;
    let mut stopping = false;
    loop {
        let now = unix_ms();
        let expired = status.clients.iter().filter(|l| l.expires_at_unix_ms <= now).cloned().collect::<Vec<_>>();
        for lease in expired {
            if let Err(error) = restore_client(&spec, &lease, active.as_ref().and_then(|b| b.port)).await {
                transition(&dir, &mut status, &front, ServiceState::Failed { reason: error.to_string() },
                    format!("restore on lease expiry failed: {error}"), None).await?;
                stopping = true;
                break;
            }
            status.clients.retain(|l| l.id != lease.id);
            publish(&dir, &status)?;
        }
        status.yields.retain(|_, window| window.expires_at_unix_ms > now);
        if !status.yields.is_empty() && (active.is_some() || candidate.is_some()) {
            transition(&dir, &mut status, &front, ServiceState::Yielding, "exclusive lane requested", None).await?;
            if let Some(mut b) = candidate.take() { stop_child(&mut b.child, &spec, b.port).await; }
            if let Some(mut b) = active.take() { stop_child(&mut b.child, &spec, b.port).await; }
            status.backend_pid = None;
            transition(&dir, &mut status, &front, ServiceState::Yielded, "exclusive lane holds service resource", None).await?;
        }
        // Each request is processed in one serialized turn and replied to *after* its effect.
        for entry in fs::read_dir(&dir)? {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|s| s.to_str()) else { continue };
            if !name.starts_with("req-") || !name.ends_with(".json") { continue; }
            let response = match read_json::<ServiceRequest>(&path) {
                Ok(request) => handle_request(request, &spec, &dir, &front, &mut status,
                    &mut active, &mut candidate, &mut pending, &mut stopping,
                    &mut last_activity, &mut idle).await.map(|_| serde_json::to_value(&status).unwrap_or_default()),
                Err(error) => Err(error),
            };
            let reply = dir.join(name.replacen("req-", "reply-", 1));
            match response {
                Ok(value) => write_json(&reply, &value)?,
                Err(error) => write_json(&reply, &serde_json::json!({"error":error.to_string()}))?,
            }
            fs::remove_file(&path)?;
        }
        if stopping { break; }
        let now = unix_ms();
        if status.yields.is_empty() && active.is_none() && candidate.is_none() && now >= next_launch {
            let port = next_port(&spec, active.as_ref());
            match spawn_backend(&spec, port, &log) {
                Ok(child) => {
                    status.backend_pid = child.id();
                    candidate = Some(Backend { child, port, started_ms: now, last_probe_ms: 0, unhealthy_since_ms: None });
                    transition(&dir, &mut status, &front, ServiceState::Starting, "waiting for health", None).await?;
                }
                Err(error) => { failures += 1; next_launch = now + backoff(&spec, failures);
                    transition(&dir, &mut status, &front, ServiceState::Degraded { reason: error.to_string() }, error.to_string(), None).await?; }
            }
        }
        if let Some(b) = candidate.as_mut() {
            let gone = b.child.try_wait()?.is_some();
            let overdue = now.saturating_sub(b.started_ms) >= spec.readiness_timeout_ms;
            if gone || overdue {
                stop_child(&mut b.child, &spec, b.port).await;
                candidate = None;
                failures += 1;
                next_launch = now + backoff(&spec, failures);
                status.restarts += 1;
                if failures > spec.restart.max_restarts {
                    transition(&dir, &mut status, &front, ServiceState::Failed { reason: "restart limit exceeded".into() }, "restart limit exceeded", active.as_ref().and_then(|b| b.port)).await?;
                    // Leave a healthy active backend serving even if the replacement fails.
                    if active.is_none() { break; }
                    pending = None;
                } else { transition(&dir, &mut status, &front, ServiceState::Degraded { reason: "backend failed readiness".into() }, "backend failed readiness", active.as_ref().and_then(|b| b.port)).await?; }
            } else if now.saturating_sub(b.last_probe_ms) >= spec.health.interval_ms {
                b.last_probe_ms = now;
                if probe(&spec, b.port).await {
                    let new = candidate.take().expect("candidate present");
                    let old = active.replace(new);
                    status.backend_pid = active.as_ref().and_then(|b| b.child.id());
                    let port = active.as_ref().and_then(|b| b.port);
                    transition(&dir, &mut status, &front, ServiceState::Healthy { backend: port }, "health check passed", port).await?;
                    failures = 0; pending = None; last_activity = now; idle = false;
                    if let Some(mut old) = old { stop_child(&mut old.child, &spec, old.port).await; status.restarts += 1; }
                }
            }
        }
        if let Some(b) = active.as_mut() {
            if b.child.try_wait()?.is_some() {
                let dead = active.take().expect("active present");
                transition(&dir, &mut status, &front, ServiceState::Degraded { reason: "backend crashed".into() }, "backend crashed", None).await?;
                status.backend_pid = None;
                status.restarts += 1;
                failures += 1;
                next_launch = now + backoff(&spec, failures);
                for lease in std::mem::take(&mut status.clients) {
                    if let Err(error) = restore_client(&spec, &lease, dead.port).await {
                        status.clients.push(lease);
                        transition(&dir, &mut status, &front, ServiceState::Failed { reason: error.to_string() }, "restore after crash failed", None).await?;
                        stopping = true; break;
                    }
                }
                publish(&dir, &status)?;
            } else if now.saturating_sub(b.last_probe_ms) >= spec.health.interval_ms && candidate.is_none() {
                b.last_probe_ms = now;
                if probe(&spec, b.port).await { b.unhealthy_since_ms = None; }
                else {
                    let since = *b.unhealthy_since_ms.get_or_insert(now);
                    if now.saturating_sub(since) >= spec.health.timeout_ms.saturating_mul(3) {
                        transition(&dir, &mut status, &front, ServiceState::Degraded { reason: "backend hung".into() }, "backend hung", None).await?;
                        let mut old = active.take().expect("active present");
                        stop_child(&mut old.child, &spec, old.port).await;
                        status.backend_pid = None; status.restarts += 1;
                        failures += 1; next_launch = now + backoff(&spec, failures);
                    }
                }
            }
        }
        if let Some((requested, reason)) = &pending {
            if active.is_some() && candidate.is_none() && status.clients.is_empty()
                && now.saturating_sub(*requested) >= spec.restart.debounce_ms && now >= next_launch {
                let port = next_port(&spec, active.as_ref());
                match spawn_backend(&spec, port, &log) {
                    Ok(child) => { candidate = Some(Backend { child, port, started_ms: now, last_probe_ms: 0, unhealthy_since_ms: None });
                        transition(&dir, &mut status, &front, ServiceState::Restarting, reason.clone(), active.as_ref().and_then(|b| b.port)).await?; }
                    Err(error) => { next_launch = now + backoff(&spec, failures + 1);
                        transition(&dir, &mut status, &front, ServiceState::Degraded { reason: error.to_string() }, format!("restart launch failed: {error}"), active.as_ref().and_then(|b| b.port)).await?; }
                }
            }
        }
        if status.yields.is_empty() && active.is_some() && status.clients.is_empty() && !idle
            && now.saturating_sub(last_activity) >= spec.idle_after_ms {
            if let Some(h) = &spec.idle { if hook(h, &spec, active.as_ref().and_then(|b| b.port), None).await.is_ok() { idle = true; } }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    transition(&dir, &mut status, &front, ServiceState::Stopped, "supervisor stopping", None).await?;
    if let Some(mut b) = candidate { stop_child(&mut b.child, &spec, b.port).await; }
    if let Some(mut b) = active { stop_child(&mut b.child, &spec, b.port).await; }
    for lease in status.clients.clone() { restore_client(&spec, &lease, None).await?; }
    status.clients.clear(); status.backend_pid = None;
    publish(&dir, &status)?;
    Ok(())
}
fn backoff(spec: &ServiceSpec, failures: u32) -> u64 {
    spec.restart.backoff_ms.max(50).saturating_mul(1u64 << failures.min(6))
}

#[allow(clippy::too_many_arguments)]
async fn handle_request(request: ServiceRequest, spec: &ServiceSpec, dir: &Path, front: &Arc<RwLock<FrontState>>,
    status: &mut ServiceStatus, active: &mut Option<Backend>, candidate: &mut Option<Backend>,
    pending: &mut Option<(u64, String)>, stopping: &mut bool, last_activity: &mut u64, idle: &mut bool) -> Result<()> {
    let now = unix_ms();
    match request {
        ServiceRequest::Stop => { *stopping = true; transition(dir, status, front, ServiceState::Stopped, "stop requested", None).await?; }
        ServiceRequest::Restart { reason, force } => {
            if force {
                for lease in status.clients.clone() {
                    restore_client(spec, &lease, active.as_ref().and_then(|b| b.port)).await?;
                }
                status.clients.clear();
            }
            *pending = Some((now, reason.clone()));
            transition(dir, status, front, ServiceState::RestartPending,
                if status.clients.is_empty() { reason } else { format!("waiting for client lease: {reason}") },
                active.as_ref().and_then(|b| b.port)).await?;
        }
        ServiceRequest::Yield { by, reason, ttl_ms } => {
            ensure!(!by.is_empty() && ttl_ms > 0 && ttl_ms <= 86_400_000, "invalid yield owner/duration");
            ensure!(status.clients.is_empty(), "service has an active client lease; yield refused");
            // The caller supplies a lane job ID; no other owner can resume this yield.
            let expires = now.saturating_add(ttl_ms);
            let prior = status.yields.get(&by).map(|w| w.expires_at_unix_ms).unwrap_or(0);
            status.yields.insert(by.clone(), YieldWindow { by, reason, expires_at_unix_ms: expires.max(prior) });
            transition(dir, status, front, ServiceState::Yielding, "exclusive resource requested", None).await?;
            if let Some(mut b) = candidate.take() { stop_child(&mut b.child, spec, b.port).await; }
            if let Some(mut b) = active.take() { stop_child(&mut b.child, spec, b.port).await; }
            status.backend_pid = None;
            transition(dir, status, front, ServiceState::Yielded, "backend stopped before exclusive admission", None).await?;
        }
        ServiceRequest::Resume { by } => {
            ensure!(status.yields.remove(&by).is_some(), "no yield held by {by}");
            if status.yields.is_empty() { transition(dir, status, front, ServiceState::Starting, "yield released", None).await?; }
            else { publish(dir, status)?; }
        }
        ServiceRequest::Lease { owner, purpose, ttl_ms } => {
            ensure!(ttl_ms > 0 && ttl_ms <= 86_400_000, "lease ttl must be within 1..86400000 ms");
            ensure!(matches!(status.state, ServiceState::Healthy { .. } | ServiceState::RestartPending), "service not ready for client leases");
            ensure!(status.yields.is_empty(), "service yielded for exclusive work");
            ensure!(status.clients.iter().all(|lease| lease.owner.participant_id == owner.participant_id && lease.owner.session_id == owner.session_id),
                "service lease held by another owner");
            if let Some(existing) = status.clients.iter_mut().find(|lease| lease.owner.participant_id == owner.participant_id && lease.owner.session_id == owner.session_id) {
                existing.expires_at_unix_ms = now.saturating_add(ttl_ms);
                existing.purpose = purpose;
            } else {
                status.clients.push(ClientLease { id: Uuid::new_v4(), service_id: spec.id.clone(), owner,
                    expires_at_unix_ms: now.saturating_add(ttl_ms), purpose });
            }
            *last_activity = now;
            if *idle { if let Some(h) = &spec.active { hook(h, spec, active.as_ref().and_then(|b| b.port), None).await?; } *idle = false; }
            publish(dir, status)?;
        }
        ServiceRequest::Release { lease_id, owner } => {
            let lease = status.clients.iter().find(|l| l.id == lease_id
                && l.owner.participant_id == owner.participant_id && l.owner.session_id == owner.session_id)
                .context("client lease not owned by caller")?.clone();
            restore_client(spec, &lease, active.as_ref().and_then(|b| b.port)).await?;
            status.clients.retain(|l| l.id != lease_id);
            publish(dir, status)?;
        }
        ServiceRequest::Touch => {
            *last_activity = now;
            if *idle { if let Some(h) = &spec.active { hook(h, spec, active.as_ref().and_then(|b| b.port), None).await?; } *idle = false; }
        }
    }
    Ok(())
}

#[async_trait]
impl ServiceCoordinator for ServiceManager {
    async fn start(&self, spec: ServiceSpec) -> Result<ServiceStatus> { self.launch(spec).await }
    async fn status(&self, id: &str) -> Result<ServiceStatus> { self.read_status(id) }
    async fn acquire(&self, id: &str, owner: Holder, ttl_ms: u64) -> Result<ClientLease> {
        let status = self.send(id, ServiceRequest::Lease { purpose: owner.purpose.clone(), owner: owner.clone(), ttl_ms }, Duration::from_secs(30)).await?;
        status.clients.into_iter().find(|l| l.owner.participant_id == owner.participant_id && l.owner.session_id == owner.session_id)
            .context("lease granted but missing from status")
    }
    async fn release(&self, lease: &ClientLease) -> Result<()> {
        self.send(&lease.service_id, ServiceRequest::Release { lease_id: lease.id, owner: lease.owner.clone() }, Duration::from_secs(30)).await?;
        Ok(())
    }
    async fn trigger_restart(&self, id: &str, reason: &str) -> Result<()> {
        self.send(id, ServiceRequest::Restart { reason: reason.into(), force: false }, Duration::from_secs(30)).await?;
        Ok(())
    }
    async fn yield_for(&self, id: &str, resources: &[ResourceRequest]) -> Result<()> {
        let spec: ServiceSpec = read_json(&service_dir(&self.root, id)?.join("spec.json"))?;
        ensure!(resources.iter().any(|r| spec.resources.iter().any(|owned| owned.key == r.key)), "service does not declare this lane resource");
        let by = resources.iter().map(|r| r.key.name.as_str()).collect::<Vec<_>>().join("+");
        self.send(id, ServiceRequest::Yield { by, reason: "lane exclusive".into(), ttl_ms: 600_000 }, Duration::from_secs(120)).await?;
        Ok(())
    }
    async fn resume(&self, id: &str) -> Result<ServiceStatus> {
        let status = self.read_status(id)?;
        let by = status.yields.keys().next().context("no active yield")?.clone();
        self.send(id, ServiceRequest::Resume { by }, Duration::from_secs(30)).await
    }
    async fn stop(&self, id: &str) -> Result<()> {
        self.send(id, ServiceRequest::Stop, Duration::from_secs(120)).await?;
        Ok(())
    }
}
