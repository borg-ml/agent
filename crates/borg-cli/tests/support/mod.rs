//! Shared driver for integration tests that run the built `borg lane` CLI.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use borg_lanes::lanes::{
    Access, AdmissionBudget, Holder, JobFingerprint, JobSpec, LaneRecord, LeaseRequest,
    ResourceKey, ResourceRequest, ResourceScope,
};
use serde_json::Value;
use uuid::Uuid;

pub const BORG: &str = env!("CARGO_BIN_EXE_borg");
pub const CANCELLED: i32 = 125;

pub fn systemd_user_manager() -> bool {
    Command::new("systemctl")
        .args(["--user", "show-environment"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

pub struct Lane {
    _dir: Option<tempfile::TempDir>,
    pub root: PathBuf,
    /// Tests use explicit degraded mode only where no user systemd manager
    /// exists (CI); scope-dependent tests are ignored by default.
    pub degraded: bool,
}

impl Lane {
    pub fn new() -> Self {
        let dir = tempfile::Builder::new()
            .prefix("gd-lane-process-")
            .tempdir()
            .unwrap();
        let root = dir.path().canonicalize().unwrap();
        Self {
            _dir: Some(dir),
            root,
            degraded: !systemd_user_manager(),
        }
    }

    /// A second handle on an existing test root, e.g. from inside a job.
    pub fn attach(root: PathBuf, degraded: bool) -> Self {
        Self {
            _dir: None,
            root,
            degraded,
        }
    }

    pub fn state(&self) -> PathBuf {
        self.root.join("state")
    }

    pub fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(BORG);
        command
            .args(["lane", "--json"])
            .args(args)
            .env("BORG_LANE_DIR", self.state())
            .env("BORG_LANES_ROOT", self.state())
            .env("BORG_LANE_EXECUTABLE", BORG)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if self.degraded {
            command
                .env("BORG_LANE_SCOPE", "0")
                .env("BORG_LANE_DEGRADED", "1");
        }
        command
    }

    /// Run one CLI command; a hung command fails the test instead of hanging it.
    pub fn cli(&self, args: &[&str], stdin: Option<&JobSpec>) -> Output {
        let mut command = self.command(args);
        if stdin.is_some() {
            command.stdin(Stdio::piped());
        }
        let mut child = command.spawn().unwrap();
        if let Some(spec) = stdin {
            serde_json::to_writer(child.stdin.take().unwrap(), spec).unwrap();
        }
        let pid = child.id();
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || sender.send(child.wait_with_output().unwrap()));
        receiver
            .recv_timeout(Duration::from_secs(90))
            .unwrap_or_else(|_| {
                // Only the CLI process this helper started.
                let _ = Command::new("kill")
                    .args(["-KILL", &pid.to_string()])
                    .status();
                panic!("borg lane {args:?} did not exit within 90 s")
            })
    }

    /// Run a command that must succeed and parse its JSON output.
    pub fn json<T: serde::de::DeserializeOwned>(&self, args: &[&str]) -> T {
        let out = self.cli(args, None);
        assert!(out.status.success(), "{args:?}\n{}", describe(&out));
        serde_json::from_slice(&out.stdout)
            .unwrap_or_else(|error| panic!("{args:?}: {error}\n{}", describe(&out)))
    }

    pub fn spec(&self, name: &str, script: &str) -> JobSpec {
        self.spec_in(name, script, ResourceScope::Worktree(self.root.clone()))
    }

    pub fn spec_in(&self, name: &str, script: &str, scope: ResourceScope) -> JobSpec {
        JobSpec {
            foreign_client_grace_ms: 300_000,
            foreign_client_grace_by_resource: Vec::new(),
            abandon_after_ms: None,
            fingerprint: JobFingerprint(name.into()),
            lease: LeaseRequest {
                resources: vec![ResourceRequest {
                    key: ResourceKey {
                        scope,
                        name: "build".into(),
                    },
                    access: Access::Exclusive,
                }],
                holder: Holder {
                    participant_id: Uuid::new_v4(),
                    session_id: Uuid::new_v4(),
                    host_pid: None,
                    purpose: name.into(),
                },
                queue_timeout_ms: None,
            },
            argv: vec!["sh".into(), "-c".into(), script.into()],
            cwd: self.root.clone(),
            env: Vec::new(),
            memory_max_bytes: Some(1 << 30),
            admission: self.budget(0, 0),
            pre_hook: None,
            post_hook: None,
            timeout_ms: 10_000,
            stall_timeout_ms: None,
            scope_unit_prefix: None,
            coalesce: true,
        }
    }

    pub fn budget(&self, min_ram: u64, min_disk: u64) -> AdmissionBudget {
        AdmissionBudget {
            min_available_ram_bytes: min_ram,
            reserve_ram_bytes: 1 << 20,
            min_free_disk_bytes: min_disk,
            reserve_disk_bytes: 1 << 20,
            disk_path: self.root.clone(),
        }
    }

    pub fn submit(&self, spec: &JobSpec) -> String {
        let out = self.cli(&["job", "submit", "--spec", "-"], Some(spec));
        assert!(out.status.success(), "{}", describe(&out));
        let value: Value = serde_json::from_slice(&out.stdout).unwrap();
        value["job_id"].as_str().unwrap().to_string()
    }

    pub fn wait(&self, id: &str, code: i32) -> Value {
        let out = self.cli(&["job", "wait", id], None);
        assert_eq!(out.status.code(), Some(code), "{}", describe(&out));
        serde_json::from_slice(&out.stdout).unwrap()
    }

    pub fn records(&self) -> Vec<LaneRecord> {
        self.json(&["job", "status"])
    }

    pub fn record(&self, id: &str) -> Option<LaneRecord> {
        self.records()
            .into_iter()
            .find(|record| record.ticket.id.to_string() == id)
    }

    /// Test-only polling of fake process state; production waits use flock.
    pub fn until<T>(&self, probe: impl FnMut() -> Option<T>) -> T {
        until(Duration::from_secs(5), probe)
    }
}

pub fn until<T>(timeout: Duration, mut probe: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = probe() {
            return value;
        }
        assert!(Instant::now() < deadline, "timed out after {timeout:?}");
        std::thread::sleep(Duration::from_millis(30));
    }
}

pub fn describe(out: &Output) -> String {
    format!(
        "status {:?}\nstdout: {}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// A workload that appends `start NAME`, sleeps, then appends `end NAME`.
pub fn traced(trace: &Path, name: &str, seconds: f64) -> String {
    let trace = trace.display();
    format!("echo 'start {name}' >> '{trace}'; sleep {seconds}; echo 'end {name}' >> '{trace}'")
}

pub fn lines(path: &Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

pub fn position(lines: &[String], line: &str) -> usize {
    lines
        .iter()
        .position(|candidate| candidate == line)
        .unwrap_or_else(|| panic!("{line:?} missing from {lines:?}"))
}
