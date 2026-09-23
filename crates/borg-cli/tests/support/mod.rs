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
    _dir: tempfile::TempDir,
    pub root: PathBuf,
    /// Scheduling tests use explicit degraded mode where no user systemd
    /// manager exists (CI); the crash test requires a real scope.
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
            _dir: dir,
            root,
            degraded: !systemd_user_manager(),
        }
    }

    pub fn cli(&self, args: &[&str], stdin: Option<&JobSpec>) -> Output {
        let mut command = Command::new(BORG);
        command
            .args(["lane", "--json"])
            .args(args)
            .env("BORG_LANE_DIR", self.root.join("state"))
            .stdin(if stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if self.degraded {
            command
                .env("BORG_LANE_SCOPE", "0")
                .env("BORG_LANE_DEGRADED", "1");
        }
        let mut child = command.spawn().unwrap();
        if let Some(spec) = stdin {
            serde_json::to_writer(child.stdin.take().unwrap(), spec).unwrap();
        }
        child.wait_with_output().unwrap()
    }

    pub fn spec(&self, name: &str, script: &str) -> JobSpec {
        self.spec_in(name, script, ResourceScope::Worktree(self.root.clone()))
    }

    pub fn spec_in(&self, name: &str, script: &str, scope: ResourceScope) -> JobSpec {
        JobSpec {
            foreign_client_grace_ms: 300_000,
            foreign_client_grace_by_resource: Vec::new(),
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
        let out = self.cli(&["job", "status"], None);
        assert!(out.status.success(), "{}", describe(&out));
        serde_json::from_slice(&out.stdout).unwrap()
    }

    pub fn record(&self, id: &str) -> Option<LaneRecord> {
        self.records()
            .into_iter()
            .find(|record| record.ticket.id.to_string() == id)
    }

    /// Test-only polling of fake process state; production waits use flock.
    pub fn until<T>(&self, mut probe: impl FnMut() -> Option<T>) -> T {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(value) = probe() {
                return value;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for fake process state"
            );
            std::thread::sleep(Duration::from_millis(30));
        }
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
