//! Supervised services through the real `borg lane` CLI: health, the stable
//! front port, client leases, and the exclusive-job handoff (D11) that yields
//! every bound service and resumes it afterwards. The backends, job workloads
//! and hooks are this test binary re-executed or `sh`; never a game engine.
//!
//! Gates that need real cgroup scopes are ignored by default; run them on a
//! host with a user systemd manager:
//! `cargo test -p borg --test lane_services -- --include-ignored --test-threads=1`
#![cfg(target_os = "linux")]

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::MetadataExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use borg_lanes::lanes::{
    Access, AdmissionBudget, ForeignClientGrace, Holder, Hook, JobFingerprint, JobSpec, LaneRecord,
    LeaseRequest, ResourceKey, ResourceRequest, ResourceScope, TicketState,
};
use borg_lanes::services::{
    ClientMode, Endpoint, HealthCheck, HealthKind, RestartMode, RestartPolicy, ServiceSpec,
    ServiceState, ServiceStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use support::{CANCELLED, Lane, describe, until};
use uuid::Uuid;

const BACKEND: &str = "BORG_TEST_FAKE_BACKEND";
const CHILD: &str = "BORG_TEST_FAKE_CHILD";
const ATOMIC_WORKER: &str = "BORG_TEST_ATOMIC_WORKER";
const SHARED_WORKER: &str = "BORG_TEST_SHARED_WORKER";
const HEALTH_FAIL_FILE: &str = "FAKE_HEALTH_FAIL_FILE";
const MCP_SESSION_LOG: &str = "FAKE_MCP_SESSION_LOG";
const CHILD_MARKER_DIR: &str = "FAKE_CHILD_MARKER_DIR";
const START_DELAY_FILE: &str = "FAKE_START_DELAY_FILE";
const REUSED_PORT_FILE: &str = "FAKE_REUSED_PORT_FILE";

// ---------------------------------------------------------------------------
// Re-executed roles: this test binary doubles as the fake backend, its
// detached child, and the workload that runs inside the exclusive job.

/// argv that re-runs exactly one (ignored) test of this binary.
fn self_exec(test: &str) -> Vec<String> {
    let exe = std::env::current_exe().unwrap();
    [
        exe.display().to_string().as_str(),
        "--exact",
        test,
        "--include-ignored",
        "--nocapture",
        "--quiet",
        "--test-threads=1",
    ]
    .map(String::from)
    .to_vec()
}

fn backend_argv() -> Vec<String> {
    let mut argv = self_exec("fake_backend");
    argv.push("{port}".into());
    argv
}

#[test]
#[ignore = "fake service backend; launched by the service tests"]
fn fake_backend() {
    if std::env::var_os(BACKEND).is_none() {
        return;
    }
    let port: u16 = std::env::args().next_back().unwrap().parse().unwrap();
    if let Some(dir) = std::env::var_os(CHILD_MARKER_DIR) {
        start_detached_child(Path::new(&dir));
    }
    if let Some(delay) = std::env::var_os(START_DELAY_FILE)
        && let Ok(seconds) = std::fs::read_to_string(&delay)
    {
        // Like an editor still loading: the port is not listening yet.
        std::thread::sleep(Duration::from_secs(seconds.trim().parse().unwrap_or(0)));
    }
    if let Some(marker) = std::env::var_os(REUSED_PORT_FILE) {
        let marker = Path::new(&marker);
        if std::fs::read_to_string(marker).is_ok_and(|used| used.trim() == port.to_string()) {
            // A recently used editor port can remain in TCP TIME_WAIT after
            // the old supervisor is killed. Make that window deterministic.
            std::thread::sleep(Duration::from_secs(60));
        }
        std::fs::write(marker, port.to_string()).unwrap();
    }
    let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
    for stream in listener.incoming().flatten() {
        std::thread::spawn(move || serve(stream));
    }
}

#[test]
#[ignore = "detached child of the fake backend"]
fn fake_child() {
    if std::env::var_os(CHILD).is_some() {
        loop {
            std::thread::park();
        }
    }
}

/// A child in its own process group, so only the backend's cgroup can end it.
#[expect(
    clippy::zombie_processes,
    reason = "the gate proves the backend's scope, not a wait, ends this child"
)]
fn start_detached_child(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    let mut command = Command::new(&self_exec("fake_child")[0]);
    let child = command
        .args(&self_exec("fake_child")[1..])
        .env(CHILD, "1")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()
        .unwrap();
    let cgroup = std::fs::read_to_string("/proc/self/cgroup").unwrap();
    let cgroup = cgroup.lines().next().unwrap();
    let record = serde_json::json!({
        "pid": child.id(),
        "start_ticks": start_ticks(child.id()).unwrap(),
        "cgroup": cgroup.split_once("::").map_or(cgroup, |(_, path)| path),
    });
    std::fs::write(
        dir.join(format!("{}.json", std::process::id())),
        record.to_string(),
    )
    .unwrap();
}

/// Process start time and state from /proc, so PID reuse is never mistaken
/// for the recorded child.
fn proc_stat(pid: u32) -> Option<(String, String)> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
    Some((fields[0].to_string(), fields[19].to_string()))
}

fn start_ticks(pid: u32) -> Option<String> {
    proc_stat(pid).map(|(_, ticks)| ticks)
}

fn live_child(record: &Value) -> bool {
    let pid = record["pid"].as_u64().unwrap() as u32;
    proc_stat(pid).is_some_and(|(state, ticks)| {
        state != "Z" && record["start_ticks"].as_str() == Some(ticks.as_str())
    })
}

fn serve(stream: TcpStream) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut writer = stream;
    loop {
        let mut request = String::new();
        if reader.read_line(&mut request).unwrap_or(0) == 0 {
            return;
        }
        let mut parts = request.split_whitespace();
        let method = parts.next().unwrap_or_default().to_string();
        let path = parts.next().unwrap_or_default().to_string();
        let mut length = 0;
        let mut session = None;
        loop {
            let mut header = String::new();
            if reader.read_line(&mut header).unwrap_or(0) == 0 {
                return;
            }
            let header = header.trim_end();
            if header.is_empty() {
                break;
            }
            if let Some((name, value)) = header.split_once(':') {
                match name.to_ascii_lowercase().as_str() {
                    "content-length" => length = value.trim().parse().unwrap_or(0),
                    "mcp-session-id" => session = Some(value.trim().to_string()),
                    _ => {}
                }
            }
        }
        let mut body = vec![0; length];
        if reader.read_exact(&mut body).is_err() {
            return;
        }
        let mcp = std::env::var_os(MCP_SESSION_LOG).is_some();
        let (status, headers, body, keep_open) = match (method.as_str(), path.as_str()) {
            ("GET", "/health")
                if std::env::var_os(HEALTH_FAIL_FILE).is_some_and(|f| Path::new(&f).exists()) =>
            {
                ("503 Service Unavailable", String::new(), Vec::new(), false)
            }
            ("GET", "/health") => ("200 OK", String::new(), b"ok\n".to_vec(), false),
            ("GET", "/") => (
                "200 OK",
                String::new(),
                format!("pid={}\n", std::process::id()).into_bytes(),
                false,
            ),
            // Shaped like the Unreal editor's MCP reply: pretty JSON, a
            // session header, Content-Length only and a connection left open.
            ("POST", "/mcp") if mcp => {
                let request: Value = serde_json::from_slice(&body).unwrap_or_default();
                let session = Uuid::new_v4().simple().to_string();
                let reply = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": {
                        "protocolVersion": "2025-11-25",
                        "capabilities": {"resources": {}, "tools": {"listChanged": true}},
                        "serverInfo": {"name": "", "title": "", "version": ""},
                    },
                });
                let mut pretty = Vec::new();
                let formatter = serde_json::ser::PrettyFormatter::with_indent(b"\t");
                reply
                    .serialize(&mut serde_json::Serializer::with_formatter(
                        &mut pretty,
                        formatter,
                    ))
                    .unwrap();
                log_session("open", &session);
                (
                    "200 OK",
                    format!(
                        "content-type: application/json;charset=utf-8\r\nMcp-Session-Id: {session}\r\n"
                    ),
                    pretty,
                    true,
                )
            }
            ("DELETE", "/mcp") if mcp && session.is_some() => {
                log_session("delete", session.as_deref().unwrap());
                ("204 No Content", String::new(), Vec::new(), false)
            }
            _ => ("404 Not Found", String::new(), Vec::new(), false),
        };
        let version = if keep_open { "HTTP/1.1" } else { "HTTP/1.0" };
        let head = format!(
            "{version} {status}\r\n{headers}content-length: {}\r\n\r\n",
            body.len()
        );
        if writer.write_all(head.as_bytes()).is_err()
            || writer.write_all(&body).is_err()
            || !keep_open
        {
            return;
        }
    }
}

fn log_session(event: &str, session: &str) {
    let log = std::env::var_os(MCP_SESSION_LOG).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(log)
        .unwrap();
    writeln!(file, "{event} {session}").unwrap();
}

/// Runs inside the granted exclusive job and inspects the services through
/// the CLI; a failed assertion fails the job.
#[derive(Serialize, Deserialize)]
struct AtomicWorker {
    root: PathBuf,
    degraded: bool,
    ports: Vec<u16>,
    marker: PathBuf,
    child_markers: Option<PathBuf>,
    stop_owned_service: bool,
    fail_health: bool,
}

#[test]
#[ignore = "exclusive-job workload; launched by the service tests"]
fn atomic_worker() {
    let Ok(config) = std::env::var(ATOMIC_WORKER) else {
        return;
    };
    let config: AtomicWorker = serde_json::from_str(&config).unwrap();
    let lane = Lane::attach(config.root.clone(), config.degraded);
    for (id, port) in ["bench-editor-a", "bench-editor-b"]
        .iter()
        .zip(&config.ports)
    {
        let status: ServiceStatus = lane.json(&["service", "status", id]);
        assert!(
            status.backend_pid.is_none() && matches!(status.state, ServiceState::Yielded),
            "exclusive started before {id} yielded: {status:?}"
        );
        assert!(status.clients.is_empty(), "unrestored client: {status:?}");
        assert_fenced(*port);
        // A restart may error or defer; it must not launch under the exclusive.
        let _ = lane.cli(&["service", "restart", id], None);
        let status: ServiceStatus = lane.json(&["service", "status", id]);
        assert!(status.backend_pid.is_none(), "{id} restarted: {status:?}");
    }
    if let Some(dir) = &config.child_markers {
        let records: Vec<Value> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| serde_json::from_slice(&std::fs::read(entry.unwrap().path()).unwrap()))
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(
            records.len() >= 2,
            "expected two child records: {records:?}"
        );
        for record in &records {
            assert!(!live_child(record), "detached child survived: {record}");
            let group = record["cgroup"].as_str().unwrap();
            assert!(group.contains("borg-"), "no dedicated cgroup: {group}");
            let procs = Path::new("/sys/fs/cgroup")
                .join(group.trim_start_matches('/'))
                .join("cgroup.procs");
            let procs = std::fs::read_to_string(procs).unwrap_or_default();
            assert!(procs.trim().is_empty(), "backend cgroup not empty: {procs}");
        }
    }
    if config.fail_health {
        std::fs::write(config.root.join("health-disabled"), "").unwrap();
    }
    if config.stop_owned_service {
        let _: Value = lane.json(&["service", "stop", "bench-editor-a"]);
    }
    std::thread::sleep(Duration::from_millis(300));
    std::fs::write(&config.marker, "verified").unwrap();
}

// ---------------------------------------------------------------------------
// Fixture

/// Owns one isolated lane root; on drop, cancels its unfinished jobs and stops
/// only the services it started.
struct Fixture {
    lane: Lane,
    services: std::cell::RefCell<Vec<String>>,
    jobs: std::cell::RefCell<Vec<String>>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for job in self.jobs.borrow().iter() {
            if self
                .lane
                .record(job)
                .is_some_and(|record| record.finished_ms.is_none())
            {
                let _ = self.lane.cli(&["job", "cancel", job], None);
                let _ = self.lane.cli(&["job", "wait", job], None);
            }
        }
        for id in self.services.borrow().iter().rev() {
            let out = self.lane.cli(&["service", "status", id], None);
            let running = serde_json::from_slice::<ServiceStatus>(&out.stdout)
                .is_ok_and(|status| status.supervisor_pid.is_some());
            if running {
                let _ = self.lane.cli(&["service", "stop", id], None);
            }
        }
    }
}

impl Fixture {
    fn new() -> Self {
        Self {
            lane: Lane::new(),
            services: Default::default(),
            jobs: Default::default(),
        }
    }

    /// For gates that assert on real cgroup scopes.
    fn scoped() -> Self {
        let fixture = Self::new();
        assert!(!fixture.lane.degraded, "requires a systemd user manager");
        fixture
    }

    fn root(&self) -> &Path {
        &self.lane.root
    }

    fn capacity(&self, name: &str, slots: u32) {
        let _: Value = self.lane.json(&[
            "resource",
            "set-capacity",
            "--name",
            name,
            "--slots",
            &slots.to_string(),
        ]);
    }

    fn service(
        &self,
        id: &str,
        cwd: &Path,
        ports: [u16; 3],
        resources: Vec<ResourceRequest>,
        env: Vec<(&str, PathBuf)>,
    ) -> ServiceSpec {
        let mut env: Vec<(String, String)> = env
            .into_iter()
            .map(|(name, value)| (name.to_string(), value.display().to_string()))
            .collect();
        env.push((BACKEND.into(), "1".into()));
        ServiceSpec {
            id: id.into(),
            argv: backend_argv(),
            cwd: cwd.into(),
            env,
            resources,
            memory_max_bytes: Some(128 << 20),
            memory_swap_max_bytes: None,
            admission: no_budget(cwd),
            health: HealthCheck {
                argv: vec!["/health".into()],
                kind: HealthKind::Http,
                interval_ms: 100,
                timeout_ms: 1_000,
                unhealthy_after_ms: None,
            },
            restart: RestartPolicy {
                max_restarts: 3,
                backoff_ms: 100,
                debounce_ms: 100,
                mode: Default::default(),
                transient_exit_codes: vec![],
                defer_while: vec![],
            },
            endpoint: Some(Endpoint {
                listen: format!("127.0.0.1:{}", ports[0]),
                backend_ports: [ports[1], ports[2]],
            }),
            restore: None,
            client_mode: ClientMode::Exclusive,
            readiness_timeout_ms: 120_000,
            graceful_stop: None,
            idle: None,
            active: None,
            adapter_enforces_leases: false,
            read_only_paths: vec!["/".into(), "/health".into()],
            idle_after_ms: 60_000,
        }
    }

    fn definition(&self, id: &str) -> PathBuf {
        self.root().join(format!("{id}.json"))
    }

    /// Start from a definition file; `start` returns the service status.
    fn start_output(&self, spec: &ServiceSpec, extra: &[&str]) -> Output {
        let definition = self.definition(&spec.id);
        std::fs::write(&definition, serde_json::to_vec(spec).unwrap()).unwrap();
        if !self.services.borrow().contains(&spec.id) {
            self.services.borrow_mut().push(spec.id.clone());
        }
        let definition = definition.display().to_string();
        let mut args = vec!["service", "start", &spec.id, "--definition", &definition];
        args.extend(extra);
        self.lane.cli(&args, None)
    }

    fn start(&self, spec: &ServiceSpec, extra: &[&str]) -> ServiceStatus {
        let out = self.start_output(spec, extra);
        assert!(out.status.success(), "{}", describe(&out));
        serde_json::from_slice(&out.stdout).unwrap()
    }

    fn status(&self, id: &str) -> ServiceStatus {
        self.lane.json(&["service", "status", id])
    }

    fn run(&self, args: &[&str]) -> ServiceStatus {
        let mut full = vec!["service"];
        full.extend(args);
        self.lane.json(&full)
    }

    fn lease(&self, id: &str, owner: &str, purpose: &str) -> ServiceStatus {
        self.run(&[
            "lease",
            id,
            "--owner",
            owner,
            "--ttl-seconds",
            "20",
            "--purpose",
            purpose,
        ])
    }

    fn lease_output(&self, id: &str, owner: &str, purpose: &str) -> Output {
        self.lane.cli(
            &[
                "service",
                "lease",
                id,
                "--owner",
                owner,
                "--ttl-seconds",
                "20",
                "--purpose",
                purpose,
            ],
            None,
        )
    }

    fn submit(&self, spec: &JobSpec, extra: &[&str]) -> String {
        let mut args = vec!["job", "submit", "--spec", "-"];
        args.extend(extra);
        let out = self.lane.cli(&args, Some(spec));
        assert!(out.status.success(), "{}", describe(&out));
        let id = serde_json::from_slice::<Value>(&out.stdout).unwrap()["job_id"]
            .as_str()
            .unwrap()
            .to_string();
        self.jobs.borrow_mut().push(id.clone());
        id
    }

    fn job(&self, id: &str) -> LaneRecord {
        self.lane.json(&["job", "status", id])
    }

    /// The job's terminal state, with its log when it did not exit 0.
    fn assert_job_succeeded(&self, id: &str, wait: &Output) {
        let done: Value = serde_json::from_slice(&wait.stdout).unwrap_or_default();
        if done["state"]["Finished"]["exit_code"] != 0 {
            let log = self
                .job(id)
                .job
                .and_then(|job| std::fs::read_to_string(job.log_path).ok())
                .unwrap_or_default();
            panic!("job {id} failed: {}\njob log:\n{log}", describe(wait));
        }
    }

    fn resumed(&self, id: &str) -> ServiceStatus {
        until(Duration::from_secs(10), || {
            let status = self.status(id);
            (healthy(&status) && status.backend_pid.is_some()).then_some(status)
        })
    }
}

fn healthy(status: &ServiceStatus) -> bool {
    matches!(status.state, ServiceState::Healthy { .. })
}

fn no_budget(path: &Path) -> AdmissionBudget {
    AdmissionBudget {
        min_available_ram_bytes: 0,
        reserve_ram_bytes: 0,
        min_free_disk_bytes: 0,
        reserve_disk_bytes: 0,
        disk_path: path.into(),
    }
}

fn host(name: &str) -> ResourceKey {
    ResourceKey {
        scope: ResourceScope::Host,
        name: name.into(),
    }
}

fn shared(key: ResourceKey) -> ResourceRequest {
    ResourceRequest {
        key,
        access: Access::Shared { slots: 1 },
    }
}

fn exclusive(key: ResourceKey) -> ResourceRequest {
    ResourceRequest {
        key,
        access: Access::Exclusive,
    }
}

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4().simple())
}

/// Distinct free loopback ports, all bound at once so none repeats.
fn free_ports(count: usize) -> Vec<u16> {
    let listeners: Vec<_> = (0..count)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().port())
        .collect()
}

fn get(port: u16, path: &str) -> std::io::Result<(u16, String)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(3)))?;
    // One write: `write!` on an unbuffered stream sends each piece as its
    // own segment, and a front that answers 503 after its first read closes
    // the connection, so a later piece fails with a broken pipe.
    stream.write_all(
        format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").as_bytes(),
    )?;
    let mut reader = BufReader::new(stream);
    let mut status = String::new();
    reader.read_line(&mut status)?;
    let code = status
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    let mut length = None;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header)? == 0 || header.trim().is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            length = value.trim().parse().ok();
        }
    }
    let mut body = Vec::new();
    match length {
        Some(length) => {
            body.resize(length, 0);
            reader.read_exact(&mut body)?;
        }
        None => {
            reader.read_to_end(&mut body)?;
        }
    }
    Ok((code, String::from_utf8_lossy(&body).into_owned()))
}

/// The front port forwards to a live backend.
fn serves(port: u16) -> String {
    let (code, body) = get(port, "/").unwrap_or_else(|error| panic!("front {port}: {error}"));
    assert_eq!(code, 200, "front {port} returned {code}: {body}");
    body
}

/// The front port stays up but refuses to forward.
fn assert_fenced(port: u16) {
    let (code, body) = get(port, "/").unwrap_or_else(|error| panic!("front {port}: {error}"));
    assert_eq!(code, 503, "front {port} forwarded while fenced: {body}");
}

/// Keep asserting a condition across a window, not just one snapshot.
fn holds_for(window: Duration, mut check: impl FnMut()) {
    let end = Instant::now() + window;
    while Instant::now() < end {
        check();
        std::thread::sleep(Duration::from_millis(200));
    }
}

// ---------------------------------------------------------------------------
// Service lifecycle

#[test]
fn front_port_survives_lease_restart_and_yield() {
    let f = Fixture::new();
    let ports = free_ports(3);
    let name = unique("bench-service");
    f.capacity(&name, 1);
    let spec = f.service(
        "bench-editor",
        f.root(),
        [ports[0], ports[1], ports[2]],
        vec![shared(host(&name))],
        Vec::new(),
    );
    f.start(&spec, &[]);
    let before = serves(ports[0]);
    let owner = Uuid::new_v4().to_string();
    f.lease("bench-editor", &owner, "capture");
    f.run(&["release", "bench-editor", "--owner", &owner]);
    f.run(&["restart", "bench-editor"]);
    // Restart is debounced; the front keeps serving until a new backend
    // takes over behind the same port.
    until(Duration::from_secs(5), || {
        (serves(ports[0]) != before).then_some(())
    });
    f.run(&[
        "yield",
        "bench-editor",
        "--by",
        "bench-import",
        "--for-seconds",
        "3",
    ]);
    let yielded = f.status("bench-editor");
    assert!(yielded.backend_pid.is_none(), "{yielded:?}");
    f.run(&["resume", "bench-editor", "--by", "bench-import"]);
    f.resumed("bench-editor");
    serves(ports[0]);
}

/// Start service `id` deferring restarts on `defer`, request a restart
/// whose replacement takes a minute to load (so the restart barrier on
/// `defer` is held), and SIGKILL the supervisor, as an OOM kill would; its
/// unit then kills the backends. Returns the spec, the file that sets the
/// backend's start delay, and the barrier's ticket.
fn kill_supervisor_mid_restart(
    f: &Fixture,
    id: &str,
    defer: &ResourceKey,
) -> (ServiceSpec, PathBuf, String) {
    let ports = free_ports(3);
    let slot = unique(&format!("{id}-slot"));
    f.capacity(&slot, 1);
    let slow = f.root().join(format!("{id}-start-delay"));
    let mut spec = f.service(
        id,
        f.root(),
        [ports[0], ports[1], ports[2]],
        vec![shared(host(&slot))],
        vec![(START_DELAY_FILE, slow.clone())],
    );
    spec.restart.defer_while = vec![defer.clone()];
    f.start(&spec, &[]);
    f.resumed(id);
    std::fs::write(&slow, "60").unwrap();
    f.run(&["restart", id]);
    // Restarting is published only once the replacement is spawned, so its
    // port is chosen and the first launch's barrier already released.
    until(Duration::from_secs(20), || {
        matches!(f.status(id).state, ServiceState::Restarting).then_some(())
    });
    let barrier = f
        .lane
        .records()
        .into_iter()
        .find(|r| r.restart_barrier && matches!(r.state, TicketState::Granted(_)))
        .map(|r| r.ticket.id.to_string())
        .expect("a restarting service holds its restart barrier");
    // This test started exactly this supervisor.
    let supervisor = f.status(id).supervisor_pid.unwrap().to_string();
    assert!(
        Command::new("kill")
            .args(["-KILL", &supervisor])
            .status()
            .unwrap()
            .success()
    );
    until(Duration::from_secs(20), || {
        matches!(f.status(id).state, ServiceState::Stopped).then_some(())
    });
    (spec, slow, barrier)
}

/// The barrier ended normally: Finished, not quarantined, released because
/// its owner was proven gone.
fn assert_barrier_released(f: &Fixture, barrier: &str) {
    let row = f.lane.record(barrier).unwrap();
    assert!(
        matches!(row.state, TicketState::Finished)
            && !row.quarantined
            && row
                .evidence
                .as_deref()
                .is_some_and(|evidence| evidence.contains("released: its owner is gone")),
        "{row:?}"
    );
}

/// Failure mode (F1): the editor's supervisor killed mid-restart (SIGKILL,
/// OOM, `systemctl stop`) leaves its restart barrier on the tree's build
/// key, `lane recover` quarantines it, and every later build of the tree
/// waits until a reboot.
#[test]
fn a_build_runs_after_recover_frees_a_killed_restart() {
    let f = Fixture::scoped();
    let mut build = f.lane.spec("after-kill", "true");
    build.timeout_ms = 30_000;
    let defer = build.lease.resources[0].key.clone();
    let (_, _, barrier) = kill_supervisor_mid_restart(&f, "killed-editor", &defer);
    let out = f.lane.cli(&["job", "recover"], None);
    assert!(out.status.success(), "{}", describe(&out));
    let job = f.submit(&build, &[]);
    let waited = f.lane.cli(&["job", "wait", &job, "--timeout", "60"], None);
    f.assert_job_succeeded(&job, &waited);
    assert_barrier_released(&f, &barrier);
}

/// Failure mode (F1): after its supervisor is killed mid-restart, starting
/// the service again stays stuck behind its predecessor's service lease
/// and restart barrier, and builds of the tree stay blocked.
#[test]
fn a_restarted_supervisor_frees_what_its_killed_predecessor_held() {
    let f = Fixture::scoped();
    let mut build = f.lane.spec("after-restart", "true");
    build.timeout_ms = 30_000;
    let defer = build.lease.resources[0].key.clone();
    let (spec, slow, barrier) = kill_supervisor_mid_restart(&f, "restarted-editor", &defer);
    std::fs::write(&slow, "0").unwrap();
    // No `lane recover`: the new supervisor's own claims free them.
    f.start(&spec, &[]);
    f.resumed("restarted-editor");
    assert_barrier_released(&f, &barrier);
    let job = f.submit(&build, &[]);
    let waited = f.lane.cli(&["job", "wait", &job, "--timeout", "60"], None);
    f.assert_job_succeeded(&job, &waited);
}

/// Failure mode (F2): a supervisor killed mid-restart has recorded its
/// still-loading replacement's port, so the next start reuses the port the
/// serving backend's clients just left. The editor then waits out TCP
/// TIME_WAIT (here, the fake backend's 60 s) although the other port is idle.
#[test]
fn a_killed_restart_uses_the_other_backend_port_before_the_first_is_quiet() {
    let f = Fixture::scoped();
    let defer = f.lane.spec("port-after-kill", "true").lease.resources[0]
        .key
        .clone();
    let (mut spec, slow, barrier) = kill_supervisor_mid_restart(&f, "port-editor", &defer);
    let [previous, alternate] = spec.endpoint.as_ref().unwrap().backend_ports;
    let recently_used = f.root().join("recently-used-backend-port");
    std::fs::write(&recently_used, previous.to_string()).unwrap();
    spec.env
        .push((REUSED_PORT_FILE.into(), recently_used.display().to_string()));
    std::fs::write(&slow, "0").unwrap();

    // No `lane recover`: the next supervisor releases the old claims and,
    // within `resumed`'s 10 s, serves from the port no client just left.
    f.start(&spec, &[]);
    let ready = f.resumed("port-editor");
    assert!(
        matches!(ready.state, ServiceState::Healthy { backend: Some(port) } if port == alternate),
        "expected alternate port {alternate}, got {ready:?}"
    );
    assert_barrier_released(&f, &barrier);
}

/// Failure mode: a cold restart (an editor too big to run twice) starting
/// the replacement while the old backend, or a process it detached, still
/// runs in its cgroup.
#[test]
fn cold_restart_ends_the_old_backend_and_its_children_first() {
    let f = Fixture::scoped();
    let ports = free_ports(3);
    let name = unique("cold-service");
    f.capacity(&name, 1);
    let markers = f.root().join("cold-children");
    let mut spec = f.service(
        "cold-editor",
        f.root(),
        [ports[0], ports[1], ports[2]],
        vec![shared(host(&name))],
        vec![(CHILD_MARKER_DIR, markers.clone())],
    );
    spec.restart.mode = RestartMode::Cold;
    f.start(&spec, &[]);
    let old = f.resumed("cold-editor").backend_pid.unwrap();
    let old_backend = serde_json::json!({"pid": old, "start_ticks": start_ticks(old).unwrap()});
    let old_child: Value =
        serde_json::from_slice(&std::fs::read(markers.join(format!("{old}.json"))).unwrap())
            .unwrap();
    f.run(&["restart", "cold-editor"]);
    // The replacement records its own child as it starts; by then both old
    // processes must be gone.
    let replacement = until(Duration::from_secs(20), || {
        std::fs::read_dir(&markers)
            .unwrap()
            .flatten()
            .filter_map(|entry| {
                entry
                    .file_name()
                    .to_str()?
                    .strip_suffix(".json")?
                    .parse::<u32>()
                    .ok()
            })
            .find(|pid| *pid != old)
    });
    assert!(
        !live_child(&old_backend),
        "backend {old} ran beside its replacement"
    );
    assert!(
        !live_child(&old_child),
        "the old backend's detached child survived"
    );
    let restarted = f.resumed("cold-editor");
    assert_eq!(restarted.backend_pid, Some(replacement));
    assert_eq!(restarted.restarts, 1);
    serves(ports[0]);
}

/// Failure mode: an editor service swapping the host to a halt because its
/// unit has no swap limit.
#[test]
fn service_unit_caps_swap() {
    let f = Fixture::scoped();
    let ports = free_ports(3);
    let name = unique("swap-service");
    f.capacity(&name, 1);
    let mut spec = f.service(
        "swap-editor",
        f.root(),
        [ports[0], ports[1], ports[2]],
        vec![shared(host(&name))],
        Vec::new(),
    );
    spec.memory_swap_max_bytes = Some(64 << 20);
    f.start(&spec, &[]);
    let unit_file = f.lane.state().join("services/swap-editor/unit.json");
    let unit: String = serde_json::from_slice(&std::fs::read(unit_file).unwrap()).unwrap();
    let out = Command::new("systemctl")
        .args([
            "--user",
            "show",
            &format!("{unit}.service"),
            "-p",
            "MemorySwapMax",
            "--value",
        ])
        .output()
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "67108864");
}

/// Health by MCP initialize against an editor-shaped reply that leaves the
/// connection open: the service must become Healthy, and every probe's MCP
/// session must be deleted.
#[test]
fn mcp_initialize_health_reads_whole_replies_and_ends_sessions() {
    let f = Fixture::new();
    let ports = free_ports(3);
    let sessions = f.root().join("mcp-sessions.log");
    let name = unique("bench-mcp");
    f.capacity(&name, 1);
    let mut spec = f.service(
        "bench-mcp",
        f.root(),
        [ports[0], ports[1], ports[2]],
        vec![shared(host(&name))],
        vec![(MCP_SESSION_LOG, sessions.clone())],
    );
    spec.health = HealthCheck {
        argv: vec!["/mcp".into()],
        kind: HealthKind::McpInitialize,
        interval_ms: 100,
        timeout_ms: 1_000,
        unhealthy_after_ms: None,
    };
    spec.readiness_timeout_ms = 6_000;
    spec.restart.max_restarts = 1;
    spec.read_only_paths = vec!["/".into()];
    let status = f.start(&spec, &["--wait-ready", "8"]);
    assert!(healthy(&status), "{status:?}");
    serves(ports[0]);
    let ledger = || {
        let text = std::fs::read_to_string(&sessions).unwrap_or_default();
        let mut opened = BTreeSet::new();
        let mut deleted = BTreeSet::new();
        for line in text.lines() {
            match line.split_once(' ') {
                Some(("open", id)) => opened.insert(id.to_string()),
                Some(("delete", id)) => deleted.insert(id.to_string()),
                _ => false,
            };
        }
        (opened, deleted)
    };
    let (opened, deleted) = until(Duration::from_secs(8), || {
        let (opened, deleted) = ledger();
        (opened.len() >= 5 && opened.difference(&deleted).count() <= 1).then_some((opened, deleted))
    });
    assert!(deleted.is_subset(&opened), "DELETE for unknown sessions");
    let still = f.status("bench-mcp");
    assert!(healthy(&still), "{still:?}");
    assert_eq!(still.backend_pid, status.backend_pid);
}

// ---------------------------------------------------------------------------
// D11: an exclusive job yields every service bound to its resource, runs
// only after they are fenced, then resumes them.

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Atomic {
    descendant: bool,
    post_hook: bool,
    fail_post_hook: bool,
    fail_resume: bool,
    fail_health: bool,
    foreign_lease: bool,
    late_lease: bool,
    foreign_grace: bool,
    foreign_indefinite: bool,
    fail_active_hook: bool,
    per_resource_grace: bool,
    slow_resume: bool,
    project_alias: bool,
}

fn run_atomic(o: Atomic) {
    let foreign = o.foreign_lease
        || o.late_lease
        || o.foreign_grace
        || o.foreign_indefinite
        || o.per_resource_grace;
    let scoped = o != Atomic::default();
    let f = if scoped {
        Fixture::scoped()
    } else {
        Fixture::new()
    };
    let root = f.root().to_path_buf();
    // The alias variant binds one service to a canonical Project(path).
    let single = o.project_alias;
    let ports = free_ports(if single { 3 } else { 6 });
    let offset = if single { 1 } else { 2 };
    let project = root.join("project");
    std::fs::create_dir(&project).unwrap();
    let child_markers = root.join("detached-children");
    let hook_started = root.join("post-hook-started");
    let hook_done = root.join("post-hook-done");
    let hook_fifo = root.join("post-hook-release");
    if o.post_hook {
        let fifo = std::ffi::CString::new(hook_fifo.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    }
    let name = unique("bench-exclusive");
    let resource_key = if single {
        ResourceKey {
            scope: ResourceScope::Project(project.clone()),
            name: name.clone(),
        }
    } else {
        host(&name)
    };
    let second_key = host(&format!("{name}-second"));
    if !single {
        f.capacity(&name, if o.per_resource_grace { 1 } else { 2 });
        if o.per_resource_grace {
            f.capacity(&second_key.name, 1);
        }
    }
    let idle_marker = root.join("bench-editor-b.idle");
    let active_marker = root.join("bench-editor-b.active");
    let services: &[&str] = if single {
        &["bench-editor-a"]
    } else {
        &["bench-editor-a", "bench-editor-b"]
    };
    let mut before = BTreeMap::new();
    for (i, id) in services.iter().enumerate() {
        let mut env = Vec::new();
        if o.descendant {
            env.push((CHILD_MARKER_DIR, child_markers.clone()));
        }
        if o.fail_health && i == 0 {
            env.push((HEALTH_FAIL_FILE, root.join("health-disabled")));
        }
        if o.slow_resume && i == 0 {
            env.push((START_DELAY_FILE, root.join("slow-start")));
        }
        let key = if o.per_resource_grace && i == 1 {
            second_key.clone()
        } else {
            resource_key.clone()
        };
        let mut spec = f.service(
            id,
            &project,
            [ports[i], ports[offset + i * 2], ports[offset + 1 + i * 2]],
            vec![shared(key)],
            env,
        );
        if i == 0 && (o.fail_health || o.slow_resume) {
            // The resume budget is the service's own readiness window: short
            // for the never-ready backend, a minute for the slow one.
            spec.readiness_timeout_ms = if o.fail_health { 8_000 } else { 60_000 };
        }
        if i == 1 && (o.late_lease || o.fail_active_hook) {
            spec.idle_after_ms = 100;
            spec.idle = Some(touch_hook(&idle_marker, false));
            spec.active = Some(touch_hook(&active_marker, o.fail_active_hook));
        }
        f.start(&spec, &[]);
        let status = f.status(id);
        before.insert(*id, status.backend_pid.expect("service became healthy"));
    }
    if o.slow_resume {
        // Only the backend launched by the post-job Resume starts slowly.
        std::fs::write(root.join("slow-start"), "35").unwrap();
    }
    if o.late_lease || o.fail_active_hook {
        until(Duration::from_secs(10), || {
            idle_marker.exists().then_some(())
        });
        assert!(!active_marker.exists(), "active hook ran before a client");
    }
    if o.fail_active_hook {
        let owner = Uuid::new_v4().to_string();
        let out = f.lease_output("bench-editor-b", &owner, "failing-active-hook");
        assert!(
            !out.status.success(),
            "failing active hook admitted a client"
        );
        assert!(
            describe(&out).contains("service hook failed: exit status: 42"),
            "{}",
            describe(&out)
        );
        assert!(active_marker.exists());
        let recovered = f.status("bench-editor-b");
        assert!(
            recovered.clients.is_empty()
                && recovered.backend_pid == Some(before["bench-editor-b"])
                && healthy(&recovered),
            "failing active hook left a ghost client or fenced backend: {recovered:?}"
        );
        serves(ports[1]);
    }

    // Ordinary handoffs lease the client as the exclusive holder itself;
    // foreign-client cases use a different owner.
    let holder = Uuid::new_v4();
    let mut client_owner = if foreign && !o.per_resource_grace {
        Uuid::new_v4().to_string()
    } else {
        holder.to_string()
    };
    f.lease("bench-editor-a", &client_owner, "capture");
    if o.per_resource_grace {
        client_owner = Uuid::new_v4().to_string();
        f.lease(
            "bench-editor-b",
            &client_owner,
            "different-resource-foreign-client",
        );
    }
    let marker = root.join("exclusive-verified");
    let worker = AtomicWorker {
        root: root.clone(),
        degraded: f.lane.degraded,
        ports: ports[..services.len()].to_vec(),
        marker: marker.clone(),
        child_markers: o.descendant.then(|| child_markers.clone()),
        stop_owned_service: o.fail_resume,
        fail_health: o.fail_health,
    };
    let mut resources = vec![exclusive(resource_key.clone())];
    if o.per_resource_grace {
        resources.push(exclusive(second_key.clone()));
    }
    let post_hook = o.post_hook.then(|| Hook {
        argv: vec![
            "sh".into(),
            "-c".into(),
            format!(
                "echo hook-blocked > '{}'; IFS= read -r x < '{}'; [ \"$x\" = x ] || exit 1; \
                 echo hook-completed > '{}'; exit {}",
                hook_started.display(),
                hook_fifo.display(),
                hook_done.display(),
                if o.fail_post_hook { 42 } else { 0 }
            ),
        ],
        timeout_ms: 20_000,
    });
    let mut job = JobSpec {
        foreign_client_grace_ms: 300_000,
        foreign_client_grace_by_resource: Vec::new(),
        abandon_after_ms: None,
        unit_prefix: None,
        finish_hook: None,
        memory_swap_max_bytes: None,
        fingerprint: JobFingerprint("bench-D11-exclusive".into()),
        lease: LeaseRequest {
            resources,
            holder: Holder {
                participant_id: holder,
                session_id: holder,
                host_pid: None,
                purpose: "atomic editor handoff".into(),
            },
            queue_timeout_ms: Some(if o.late_lease || o.per_resource_grace {
                40_000
            } else if foreign {
                20_000
            } else {
                10_000
            }),
        },
        argv: self_exec("atomic_worker"),
        cwd: project.clone(),
        env: vec![(
            ATOMIC_WORKER.into(),
            serde_json::to_string(&worker).unwrap(),
        )],
        memory_max_bytes: Some(128 << 20),
        admission: no_budget(&project),
        // No adapter yield/resume hook may mask a failure to discover every
        // service bound to the resource.
        pre_hook: None,
        post_hook,
        timeout_ms: if o.post_hook { 20_000 } else { 10_000 },
        stall_timeout_ms: None,
        coalesce: false,
    };
    if o.per_resource_grace {
        job.foreign_client_grace_ms = 5_000;
        job.foreign_client_grace_by_resource = vec![ForeignClientGrace {
            resource: second_key.clone(),
            grace_ms: 0,
        }];
    }

    let job_id = if o.project_alias {
        // Symlink and dot-dot aliases are refused in both filesystem scopes
        // before they can allocate a distinct lock.
        let symlink = root.join("alias-project");
        std::os::unix::fs::symlink(&project, &symlink).unwrap();
        for scope in [
            ResourceScope::Project(symlink.clone()),
            ResourceScope::Project(project.join("..").join("project")),
            ResourceScope::Worktree(project.join("..").join("project")),
            ResourceScope::Worktree(symlink),
        ] {
            let mut attempt = job.clone();
            attempt.lease.resources[0].key.scope = scope.clone();
            let out = f
                .lane
                .cli(&["job", "submit", "--spec", "-"], Some(&attempt));
            assert!(!out.status.success(), "alias {scope:?} was accepted");
        }
        f.submit(&job, &[])
    } else if foreign && !o.per_resource_grace {
        let grace = if o.foreign_indefinite {
            "0"
        } else if o.foreign_grace {
            "5"
        } else if o.late_lease {
            "30"
        } else {
            "15"
        };
        f.submit(&job, &["--foreign-lease-grace-seconds", grace])
    } else {
        f.submit(&job, &[])
    };

    if foreign {
        // A foreign client stays active: Preparing must hold the exclusive
        // before any workload or destructive service yield.
        let row = until(Duration::from_secs(5), || {
            let row = f.job(&job_id);
            assert!(
                row.started_ms.is_none() && !marker.exists(),
                "foreign client did not block the grant: {row:?}"
            );
            (matches!(row.state, TicketState::Preparing)
                && row
                    .wait_reason
                    .as_deref()
                    .is_some_and(|reason| reason.starts_with("foreign client lease:")))
            .then_some(row)
        });
        let wait_reason = row.wait_reason.unwrap();
        let (waiting, waiting_port, other, other_port) = if o.per_resource_grace {
            ("bench-editor-b", ports[1], "bench-editor-a", ports[0])
        } else {
            ("bench-editor-a", ports[0], "bench-editor-b", ports[1])
        };
        let before_release = f.status(waiting);
        assert!(
            before_release.reason.contains(&wait_reason),
            "service status omitted the foreign-client notice: {before_release:?}"
        );
        assert!(
            before_release.backend_pid == Some(before[waiting])
                && !before_release.clients.is_empty(),
            "foreign client was yielded before release: {before_release:?}"
        );
        serves(waiting_port);
        let other_before = f.status(other);
        assert!(
            other_before.backend_pid == Some(before[other]) && healthy(&other_before),
            "another bound service yielded before the foreign client ended: {other_before:?}"
        );
        serves(other_port);
        if o.per_resource_grace {
            assert!(wait_reason.contains("grace indefinite"), "{wait_reason}");
            holds_for(Duration::from_secs(7), || {
                let held = f.job(&job_id);
                let waiting_now = f.status(waiting);
                let other_now = f.status(other);
                assert!(
                    matches!(held.state, TicketState::Preparing)
                        && held.started_ms.is_none()
                        && waiting_now.backend_pid == Some(before[waiting])
                        && !waiting_now.clients.is_empty()
                        && other_now.backend_pid == Some(before[other]),
                    "zero grace on the second key released early: {held:?} {waiting_now:?} {other_now:?}"
                );
                serves(ports[0]);
                serves(ports[1]);
            });
        }
        if o.foreign_indefinite {
            holds_for(Duration::from_secs(2), || {
                let held = f.job(&job_id);
                let active = f.status("bench-editor-a");
                assert!(
                    matches!(held.state, TicketState::Preparing)
                        && held.started_ms.is_none()
                        && active.backend_pid == Some(before["bench-editor-a"])
                        && !active.clients.is_empty(),
                    "grace 0 released a foreign client early: {held:?} {active:?}"
                );
                serves(ports[0]);
            });
        }
        if o.late_lease {
            // New clients on another bound service must not slip in after the
            // exclusive entered Preparing, and must not wake it.
            let late = Uuid::new_v4().to_string();
            let out = f.lease_output("bench-editor-b", &late, "late-client");
            assert!(
                !out.status.success(),
                "late client admitted while Preparing"
            );
            assert!(f.status("bench-editor-b").clients.is_empty());
            assert!(!active_marker.exists(), "refused lease ran the active hook");
            // Renewing the waiting client would silently extend it.
            let expires = before_release.clients[0].expires_at_unix_ms;
            let out = f.lease_output("bench-editor-a", &client_owner, "late-renew");
            assert!(
                !out.status.success(),
                "late renewal accepted while Preparing"
            );
            let renewed = f.status("bench-editor-a").clients;
            assert!(
                renewed.len() == 1 && renewed[0].expires_at_unix_ms == expires,
                "late renewal changed the client: {renewed:?}"
            );
        }
        if !o.foreign_grace {
            f.run(&["release", waiting, "--owner", &client_owner]);
        }
    }

    let wait = if o.post_hook {
        let mut waiter = f.lane.command(&["job", "wait", &job_id]).spawn().unwrap();
        until(Duration::from_secs(10), || {
            hook_started.exists().then_some(())
        });
        for (id, port) in services.iter().zip(&ports) {
            let current = f.status(id);
            assert!(
                current.backend_pid.is_none() && current.yields.contains_key(&job_id),
                "{id} lost fencing before its post hook: {current:?}"
            );
            assert_fenced(*port);
        }
        std::fs::write(&hook_fifo, "x\n").unwrap();
        let out = until(Duration::from_secs(15), || waiter.try_wait().unwrap());
        let mut stdout = Vec::new();
        waiter
            .stdout
            .take()
            .unwrap()
            .read_to_end(&mut stdout)
            .unwrap();
        assert!(hook_done.exists(), "job completed before its post hook");
        Output {
            status: out,
            stdout,
            stderr: Vec::new(),
        }
    } else {
        f.lane.cli(&["job", "wait", &job_id], None)
    };
    f.assert_job_succeeded(&job_id, &wait);
    assert!(
        marker.exists(),
        "exclusive workload never verified the fence"
    );

    if o.slow_resume {
        // The resumed backend needs 35 s, past the old fixed 30 s resume check;
        // the lane itself must clear pending without recover or an error.
        let finished = Instant::now();
        until(Duration::from_secs(60), || {
            let row = f.job(&job_id);
            assert!(
                row.resume_error.is_none(),
                "slow start journalled an error: {row:?}"
            );
            row.resume_pending.is_empty().then_some(())
        });
        assert!(finished.elapsed() >= Duration::from_secs(30));
        let recovered: Value = f.lane.json(&["recover", "--wait", "5"]);
        assert!(
            recovered["resumes"].as_array().is_none_or(Vec::is_empty),
            "recover found resumes still pending: {recovered}"
        );
    }
    if o.foreign_grace {
        let waited = f.job(&job_id);
        assert!(
            waited.started_ms.unwrap() - waited.created_ms >= 3_500,
            "the exclusive did not wait for the grace: {waited:?}"
        );
    }
    if o.fail_post_hook {
        let row = f.job(&job_id);
        assert!(
            row.quarantined
                && row
                    .evidence
                    .as_deref()
                    .is_some_and(|evidence| evidence.contains("post hook failed")),
            "failed post hook was not quarantined: {row:?}"
        );
        for (id, port) in services.iter().zip(&ports) {
            let current = f.status(id);
            assert!(current.backend_pid.is_none(), "{id} restarted: {current:?}");
            assert_fenced(*port);
        }
        return;
    }
    if o.fail_health {
        let row = until(Duration::from_secs(36), || {
            let row = f.job(&job_id);
            row.resume_error.is_some().then_some(row)
        });
        let status = f.status("bench-editor-a");
        assert!(
            row.resume_pending.contains(&"bench-editor-a".to_string())
                && !status.yields.contains_key(&job_id)
                && !healthy(&status),
            "an unhealthy Resume cleared pending or kept the yield: {row:?} {status:?}"
        );
        // Re-enable health and wait for genuine Healthy; the fixture sends no
        // second Resume.
        std::fs::remove_file(root.join("health-disabled")).unwrap();
        until(Duration::from_secs(20), || {
            let status = f.status("bench-editor-a");
            if matches!(status.state, ServiceState::Stopped) {
                let definition = f.definition("bench-editor-a").display().to_string();
                let _ = f.lane.cli(
                    &[
                        "service",
                        "start",
                        "bench-editor-a",
                        "--definition",
                        &definition,
                    ],
                    None,
                );
            }
            healthy(&status).then_some(())
        });
        let recovered: Value = f.lane.json(&["job", "recover", "--wait", "20"]);
        let outcome = recovered["resumes"]
            .as_array()
            .and_then(|resumes| resumes.iter().find(|r| r["job_id"] == job_id.as_str()))
            .map(|resume| resume["outcome"].clone());
        assert_eq!(outcome, Some("resumed".into()), "{recovered}");
    }
    if o.fail_resume {
        let row = until(Duration::from_secs(6), || {
            let row = f.job(&job_id);
            row.resume_error.is_some().then_some(row)
        });
        assert!(
            row.resume_pending.contains(&"bench-editor-a".to_string()) && !row.quarantined,
            "a failed resume must stay retryable: {row:?}"
        );
        // A yielded service's foreground start blocks until Resume, so start
        // it in the background and use its status as the readiness signal.
        let definition = f.definition("bench-editor-a").display().to_string();
        let mut starter = f
            .lane
            .command(&[
                "service",
                "start",
                "bench-editor-a",
                "--definition",
                &definition,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        until(Duration::from_secs(12), || {
            let status = f.status("bench-editor-a");
            (!matches!(status.state, ServiceState::Stopped) && status.supervisor_pid.is_some())
                .then_some(())
        });
        let _: Value = f.lane.json(&["job", "recover"]);
        // Recover releases a start still waiting on the yield; otherwise end
        // only the CLI child this test started.
        let deadline = Instant::now() + Duration::from_secs(2);
        while starter.try_wait().unwrap().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = starter.kill();
        let _ = starter.wait();
    }
    if o.fail_health || o.fail_resume {
        until(Duration::from_secs(12), || {
            let row = f.job(&job_id);
            (row.resume_pending.is_empty() && row.resume_error.is_none()).then_some(())
        });
    }
    for (id, port) in services.iter().zip(&ports) {
        let after = f.resumed(id);
        assert_ne!(
            after.backend_pid,
            Some(before[id]),
            "{id} was not relaunched"
        );
        serves(*port);
    }
}

fn touch_hook(marker: &Path, fail: bool) -> Hook {
    Hook {
        argv: vec![
            "sh".into(),
            "-c".into(),
            format!(
                "echo hook-called > '{}'{}",
                marker.display(),
                if fail { "; exit 42" } else { "" }
            ),
        ],
        timeout_ms: 5_000,
    }
}

#[test]
fn exclusive_job_yields_every_bound_service_then_resumes_them() {
    run_atomic(Atomic::default());
}

#[test]
#[ignore = "requires a systemd user manager"]
fn detached_backend_children_end_with_their_scope_before_the_grant() {
    run_atomic(Atomic {
        descendant: true,
        ..Atomic::default()
    });
}

#[test]
#[ignore = "requires a systemd user manager"]
fn post_hook_completes_before_services_resume() {
    run_atomic(Atomic {
        post_hook: true,
        ..Atomic::default()
    });
}

#[test]
#[ignore = "requires a systemd user manager"]
fn failed_post_hook_quarantines_and_keeps_services_fenced() {
    run_atomic(Atomic {
        post_hook: true,
        fail_post_hook: true,
        ..Atomic::default()
    });
}

#[test]
#[ignore = "requires a systemd user manager"]
fn failed_resume_stays_pending_until_recover() {
    run_atomic(Atomic {
        fail_resume: true,
        ..Atomic::default()
    });
}

#[test]
#[ignore = "requires a systemd user manager"]
fn unhealthy_resume_journals_an_error_and_recovers_without_a_second_resume() {
    run_atomic(Atomic {
        fail_health: true,
        ..Atomic::default()
    });
}

#[test]
#[ignore = "requires a systemd user manager"]
fn foreign_client_holds_the_exclusive_in_preparing_until_released() {
    run_atomic(Atomic {
        foreign_lease: true,
        ..Atomic::default()
    });
}

#[test]
#[ignore = "requires a systemd user manager"]
fn foreign_client_is_yielded_after_its_grace() {
    run_atomic(Atomic {
        foreign_grace: true,
        ..Atomic::default()
    });
}

#[test]
#[ignore = "requires a systemd user manager"]
fn zero_grace_holds_a_foreign_client_until_it_releases() {
    run_atomic(Atomic {
        foreign_indefinite: true,
        ..Atomic::default()
    });
}

#[test]
#[ignore = "requires a systemd user manager"]
fn late_leases_and_renewals_are_refused_while_preparing() {
    run_atomic(Atomic {
        late_lease: true,
        ..Atomic::default()
    });
}

#[test]
#[ignore = "requires a systemd user manager"]
fn failing_active_hook_rolls_back_the_client() {
    run_atomic(Atomic {
        fail_active_hook: true,
        ..Atomic::default()
    });
}

#[test]
#[ignore = "requires a systemd user manager"]
fn per_resource_zero_grace_outlasts_the_job_wide_grace() {
    run_atomic(Atomic {
        per_resource_grace: true,
        ..Atomic::default()
    });
}

#[test]
#[ignore = "requires a systemd user manager; waits about 35 s"]
fn slow_resume_clears_pending_within_its_readiness_budget() {
    run_atomic(Atomic {
        slow_resume: true,
        ..Atomic::default()
    });
}

#[test]
#[ignore = "requires a systemd user manager"]
fn project_path_aliases_are_refused_and_the_canonical_handoff_runs() {
    run_atomic(Atomic {
        project_alias: true,
        ..Atomic::default()
    });
}

// ---------------------------------------------------------------------------
// Admission budgets across services

/// Two disjoint services each reserve 60% of one host's RAM or one device's
/// free space: the second queues with a reason and starts once the first
/// yields.
fn service_budget(disk: bool) {
    let f = Fixture::scoped();
    let project = f.root().join("project");
    let other = f.root().join("same-device-other-path");
    std::fs::create_dir(&project).unwrap();
    std::fs::create_dir(&other).unwrap();
    assert_eq!(
        project.metadata().unwrap().dev(),
        other.metadata().unwrap().dev()
    );
    let available = if disk {
        borg_lanes::workspace::hygiene::disk_available(&project).unwrap()
    } else {
        borg_lanes::workspace::hygiene::ram_available().unwrap()
    };
    let reserve = available / 5 * 3;
    assert!(reserve >= 1 << 30, "too little free capacity for this gate");
    let ports = free_ports(6);
    for (index, id) in ["bench-budget-a", "bench-budget-b"].iter().enumerate() {
        let mut spec = f.service(
            id,
            &project,
            [ports[index], ports[2 + index * 2], ports[3 + index * 2]],
            vec![shared(host(&format!("{id}-R")))],
            Vec::new(),
        );
        spec.admission.disk_path = if index == 0 {
            project.clone()
        } else {
            other.clone()
        };
        if disk {
            spec.admission.reserve_disk_bytes = reserve;
        } else {
            spec.admission.reserve_ram_bytes = reserve;
        }
        if index == 0 {
            let status = f.start(&spec, &["--wait-ready", "5"]);
            assert!(status.backend_pid.is_some(), "{status:?}");
        } else {
            // The CLI may give up after one second; its supervisor persists
            // and exposes the admission reason.
            f.start_output(&spec, &["--wait-ready", "1"]);
        }
    }
    let second = f.status("bench-budget-b");
    let kind = if disk { "disk" } else { "RAM" };
    assert!(
        second.backend_pid.is_none() && second.reason.contains(&format!("{kind} admission queued")),
        "combined {kind} reservation not enforced: {second:?}"
    );
    f.run(&[
        "yield",
        "bench-budget-a",
        "--by",
        "bench-budget-test",
        "--for-seconds",
        "10",
    ]);
    f.resumed("bench-budget-b");
    serves(ports[1]);
}

#[test]
#[ignore = "requires a systemd user manager; reserves 60% of free disk"]
fn service_disk_reservations_share_one_device() {
    service_budget(true);
}

#[test]
#[ignore = "requires a systemd user manager; MemAvailable moves, so informational"]
fn service_ram_reservations_cannot_overcommit_the_host() {
    service_budget(false);
}

// ---------------------------------------------------------------------------
// Shared-client services

#[derive(Serialize, Deserialize)]
struct SharedWorker {
    root: PathBuf,
    degraded: bool,
    service: String,
    front: u16,
    backend_pid: u32,
    owners: [String; 2],
}

#[test]
#[ignore = "shared-client exclusive workload; launched by the service tests"]
fn shared_worker() {
    let Ok(config) = std::env::var(SHARED_WORKER) else {
        return;
    };
    let config: SharedWorker = serde_json::from_str(&config).unwrap();
    let lane = Lane::attach(config.root.clone(), config.degraded);
    let status: ServiceStatus = lane.json(&["service", "status", &config.service]);
    assert!(
        status.clients.is_empty()
            && status.backend_pid.is_none()
            && matches!(
                status.state,
                ServiceState::Yielded | ServiceState::RestartPending
            ),
        "{status:?}"
    );
    let restored = std::fs::read_to_string(config.root.join("restored")).unwrap();
    let lines: Vec<&str> = restored.lines().collect();
    let count = |owner: &str| lines.iter().filter(|line| **line == owner).count();
    assert_eq!(
        (count(&config.owners[0]), count(&config.owners[1])),
        (2, 1),
        "{lines:?}"
    );
    assert!(
        proc_stat(config.backend_pid).is_none(),
        "backend alive at grant"
    );
    assert_fenced(config.front);
    std::fs::write(config.root.join("exclusive-verified"), "verified").unwrap();
}

/// A two-client shared service: the limit is enforced, each release runs the
/// owner's restore hook, and an exclusive job waits out its foreign clients'
/// grace, then restores both before its workload runs. With a restore that
/// fails, stop, yield and a later exclusive are all refused and name the
/// stuck client until a human releases it.
#[test]
#[ignore = "requires a systemd user manager"]
fn shared_clients_restore_before_an_exclusive_and_failed_restores_fence_it() {
    let f = Fixture::scoped();
    let root = f.root().to_path_buf();
    let service = unique("shared");
    let resource = host(&unique("shared-resource"));
    let owners: Vec<String> = (0..3).map(|_| Uuid::new_v4().to_string()).collect();
    let ports = free_ports(3);
    let restored = root.join("restored");
    let blocked_marker = root.join("restore-blocked");
    let mut spec = f.service(
        &service,
        &root,
        [ports[0], ports[1], ports[2]],
        vec![exclusive(resource.clone())],
        Vec::new(),
    );
    spec.health.interval_ms = 200;
    spec.health.timeout_ms = 500;
    spec.restart = RestartPolicy {
        max_restarts: 2,
        backoff_ms: 200,
        debounce_ms: 100,
        mode: Default::default(),
        transient_exit_codes: vec![],
        defer_while: vec![],
    };
    spec.restore = Some(Hook {
        argv: vec![
            "sh".into(),
            "-c".into(),
            r#"[ -e "$2" ] && [ "$1" = "$3" ] && exit 4; echo "$1" >> "$4""#.into(),
            "restore".into(),
            "{owner}".into(),
            blocked_marker.display().to_string(),
            owners[1].clone(),
            restored.display().to_string(),
        ],
        timeout_ms: 1_500,
    });
    spec.client_mode = ClientMode::Shared { max_clients: 2 };
    spec.read_only_paths = vec!["/health".into()];
    spec.readiness_timeout_ms = 6_000;
    let started = f.start(&spec, &["--wait-ready", "8"]);
    assert!(healthy(&started), "{started:?}");
    let pid = started.backend_pid.unwrap();

    let lease_of = |status: &ServiceStatus, owner: &str| {
        status
            .clients
            .iter()
            .find(|client| client.owner.participant_id.to_string() == owner)
            .map(|client| client.id.to_string())
    };
    let leases: Vec<String> = (0..2)
        .map(|i| {
            let status = f.lease(&service, &owners[i], &format!("db-{i}"));
            lease_of(&status, &owners[i]).unwrap()
        })
        .collect();
    assert_ne!(leases[0], leases[1]);
    let out = f.lease_output(&service, &owners[2], "overflow");
    assert!(
        !out.status.success() && describe(&out).contains("client limit reached"),
        "{}",
        describe(&out)
    );
    assert_eq!(f.status(&service).clients.len(), 2);
    let after = f.run(&[
        "release",
        &service,
        "--owner",
        &owners[0],
        "--lease-id",
        &leases[0],
    ]);
    assert_eq!(after.clients.len(), 1);
    assert_eq!(after.clients[0].id.to_string(), leases[1]);
    assert_eq!(
        std::fs::read_to_string(&restored).unwrap(),
        format!("{}\n", owners[0])
    );
    assert_eq!(f.lease(&service, &owners[0], "db-a-again").clients.len(), 2);

    let worker = SharedWorker {
        root: root.clone(),
        degraded: f.lane.degraded,
        service: service.clone(),
        front: ports[0],
        backend_pid: pid,
        owners: [owners[0].clone(), owners[1].clone()],
    };
    let job = JobSpec {
        foreign_client_grace_ms: 300_000,
        foreign_client_grace_by_resource: Vec::new(),
        abandon_after_ms: None,
        unit_prefix: None,
        finish_hook: None,
        memory_swap_max_bytes: None,
        fingerprint: JobFingerprint(unique("shared-client-exclusive")),
        lease: LeaseRequest {
            resources: vec![exclusive(resource.clone())],
            holder: Holder {
                participant_id: Uuid::new_v4(),
                session_id: Uuid::new_v4(),
                host_pid: None,
                purpose: "shared-client handoff".into(),
            },
            queue_timeout_ms: Some(20_000),
        },
        argv: self_exec("shared_worker"),
        cwd: root.clone(),
        env: vec![(
            SHARED_WORKER.into(),
            serde_json::to_string(&worker).unwrap(),
        )],
        memory_max_bytes: Some(128 << 20),
        admission: no_budget(&root),
        pre_hook: None,
        post_hook: None,
        timeout_ms: 12_000,
        stall_timeout_ms: None,
        coalesce: false,
    };
    // Both clients are foreign to this holder: Preparing holds the exclusive,
    // with clients and backend untouched, until the grace ends.
    let job_id = f.submit(&job, &["--foreign-lease-grace-seconds", "5"]);
    let row = until(Duration::from_secs(4), || {
        let row = f.job(&job_id);
        assert!(
            row.started_ms.is_none(),
            "foreign clients did not hold the exclusive: {row:?}"
        );
        (matches!(row.state, TicketState::Preparing)
            && row
                .wait_reason
                .as_deref()
                .is_some_and(|reason| reason.starts_with("foreign client lease:")))
        .then_some(row)
    });
    let waiting = f.status(&service);
    assert!(
        waiting.clients.len() == 2 && waiting.backend_pid == Some(pid) && healthy(&waiting),
        "{waiting:?}"
    );
    assert!(waiting.reason.contains(row.wait_reason.as_deref().unwrap()));
    assert_eq!(get(ports[0], "/health").unwrap().0, 200);
    let wait = f.lane.cli(&["job", "wait", &job_id], None);
    f.assert_job_succeeded(&job_id, &wait);
    assert!(root.join("exclusive-verified").exists());
    let resumed = f.resumed(&service);
    assert!(
        resumed.backend_pid != Some(pid) && resumed.clients.is_empty(),
        "{resumed:?}"
    );

    // A failing restore for the second owner fences everything.
    for (i, owner) in owners[..2].iter().enumerate() {
        f.lease(&service, owner, &format!("recovery-{i}"));
    }
    std::fs::write(&blocked_marker, "reject second owner").unwrap();
    let stop = f.lane.cli(&["service", "stop", &service], None);
    assert!(
        !stop.status.success() && describe(&stop).contains("restore client"),
        "{}",
        describe(&stop)
    );
    let blocked = f.status(&service);
    assert!(
        blocked.clients.len() == 1
            && blocked.clients[0].owner.participant_id.to_string() == owners[1]
            && blocked.backend_pid == resumed.backend_pid
            && healthy(&blocked),
        "{blocked:?}"
    );
    let stuck = blocked.clients[0].id.to_string();
    for value in [&owners[1], &stuck] {
        assert!(
            blocked.reason.contains(value.as_str()) && describe(&stop).contains(value.as_str())
        );
    }
    let failed_yield = f.lane.cli(
        &[
            "service",
            "yield",
            &service,
            "--by",
            "blocked-exclusive",
            "--for-seconds",
            "10",
        ],
        None,
    );
    assert!(!failed_yield.status.success());
    let still = f.status(&service);
    assert!(
        still.clients.len() == 1
            && still.yields.is_empty()
            && still.backend_pid == resumed.backend_pid,
        "{still:?}"
    );
    for value in [&owners[1], &stuck] {
        assert!(
            still.reason.contains(value.as_str())
                && describe(&failed_yield).contains(value.as_str())
        );
    }
    let forbidden = root.join("must-not-start");
    let mut blocked_job = job.clone();
    blocked_job.fingerprint = JobFingerprint(unique("failed-restore"));
    blocked_job.lease.queue_timeout_ms = Some(5_000);
    blocked_job.argv = vec![
        "sh".into(),
        "-c".into(),
        format!("echo unsafe > '{}'", forbidden.display()),
    ];
    blocked_job.env = Vec::new();
    // A short grace lets the yield and its failed restore decide the job.
    let denied = f.submit(&blocked_job, &["--foreign-lease-grace-seconds", "1"]);
    let wait = f.lane.cli(&["job", "wait", &denied], None);
    assert_eq!(wait.status.code(), Some(CANCELLED), "{}", describe(&wait));
    assert!(!forbidden.exists(), "exclusive ran after a failed restore");
    let evidence = f.job(&denied).evidence.unwrap_or_default();
    assert!(
        evidence.contains(owners[1].as_str()) && evidence.contains(stuck.as_str()),
        "{evidence}"
    );
    assert_eq!(f.status(&service).clients.len(), 1);
    std::fs::remove_file(&blocked_marker).unwrap();
    let released = f.run(&[
        "release",
        &service,
        "--owner",
        &owners[1],
        "--lease-id",
        &stuck,
    ]);
    assert!(
        released.clients.is_empty() && healthy(&released),
        "{released:?}"
    );
}
