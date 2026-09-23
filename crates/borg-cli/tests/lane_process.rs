//! The real `borg lane` CLI across processes, with fake shell workloads; never
//! launches an engine. Specs are built from the `borg-lanes` types so a schema
//! change breaks this test at compile time rather than at run time.
#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use borg_lanes::lanes::{
    Access, AdmissionBudget, Holder, Hook, JobFingerprint, JobSpec, LaneRecord, LeaseRequest,
    ResourceKey, ResourceRequest, ResourceScope,
};
use serde_json::Value;
use uuid::Uuid;

const BORG: &str = env!("CARGO_BIN_EXE_borg");
const CANCELLED: i32 = 125;

fn systemd_user_manager() -> bool {
    Command::new("systemctl")
        .args(["--user", "show-environment"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

struct Lane {
    _dir: tempfile::TempDir,
    root: PathBuf,
    /// Scheduling tests use explicit degraded mode where no user systemd
    /// manager exists (CI); the crash test requires a real scope.
    degraded: bool,
}

impl Lane {
    fn new() -> Self {
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

    fn cli(&self, args: &[&str], stdin: Option<&JobSpec>) -> Output {
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

    fn spec(&self, name: &str, script: &str) -> JobSpec {
        self.spec_in(name, script, ResourceScope::Worktree(self.root.clone()))
    }

    fn spec_in(&self, name: &str, script: &str, scope: ResourceScope) -> JobSpec {
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

    fn budget(&self, min_ram: u64, min_disk: u64) -> AdmissionBudget {
        AdmissionBudget {
            min_available_ram_bytes: min_ram,
            reserve_ram_bytes: 1 << 20,
            min_free_disk_bytes: min_disk,
            reserve_disk_bytes: 1 << 20,
            disk_path: self.root.clone(),
        }
    }

    fn submit(&self, spec: &JobSpec) -> String {
        let out = self.cli(&["job", "submit", "--spec", "-"], Some(spec));
        assert!(out.status.success(), "{}", describe(&out));
        let value: Value = serde_json::from_slice(&out.stdout).unwrap();
        value["job_id"].as_str().unwrap().to_string()
    }

    fn wait(&self, id: &str, code: i32) -> Value {
        let out = self.cli(&["job", "wait", id], None);
        assert_eq!(out.status.code(), Some(code), "{}", describe(&out));
        serde_json::from_slice(&out.stdout).unwrap()
    }

    fn records(&self) -> Vec<LaneRecord> {
        let out = self.cli(&["job", "status"], None);
        assert!(out.status.success(), "{}", describe(&out));
        serde_json::from_slice(&out.stdout).unwrap()
    }

    fn record(&self, id: &str) -> Option<LaneRecord> {
        self.records()
            .into_iter()
            .find(|record| record.ticket.id.to_string() == id)
    }

    /// Test-only polling of fake process state; production waits use flock.
    fn until<T>(&self, mut probe: impl FnMut() -> Option<T>) -> T {
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

fn describe(out: &Output) -> String {
    format!(
        "status {:?}\nstdout: {}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// A workload that appends `start NAME`, sleeps, then appends `end NAME`.
fn traced(trace: &Path, name: &str, seconds: f64) -> String {
    let trace = trace.display();
    format!("echo 'start {name}' >> '{trace}'; sleep {seconds}; echo 'end {name}' >> '{trace}'")
}

fn lines(path: &Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

fn position(lines: &[String], line: &str) -> usize {
    lines
        .iter()
        .position(|candidate| candidate == line)
        .unwrap_or_else(|| panic!("{line:?} missing from {lines:?}"))
}

#[test]
fn fifo_order_and_pending_identical_jobs_coalesce() {
    let lane = Lane::new();
    let trace = lane.root.join("trace");
    let first = lane.submit(&lane.spec("a", &traced(&trace, "a", 0.3)));
    lane.until(|| lines(&trace).contains(&"start a".to_string()).then_some(()));
    let second = lane.submit(&lane.spec("b", &traced(&trace, "b", 0.12)));
    let joined = lane.submit(&lane.spec("b", &traced(&trace, "b", 0.12)));
    assert_eq!(second, joined, "a pending identical job must be joined");
    let third = lane.submit(&lane.spec("c", &traced(&trace, "c", 0.05)));
    for id in [&first, &second, &third] {
        lane.wait(id, 0);
    }
    assert_eq!(
        lines(&trace),
        ["start a", "end a", "start b", "end b", "start c", "end c"]
    );
}

#[test]
fn shared_slots_overlap_and_exclusive_waits_for_them() {
    let lane = Lane::new();
    let root = lane.root.display().to_string();
    let out = lane.cli(
        &[
            "resource",
            "set-capacity",
            "--name",
            "build",
            "--slots",
            "2",
            "--scope",
            &root,
        ],
        None,
    );
    assert!(out.status.success(), "{}", describe(&out));
    let trace = lane.root.join("overlap");
    let job = |name: &str, access: Access| {
        let mut spec = lane.spec(name, &traced(&trace, name, 0.24));
        spec.lease.resources[0].access = access;
        lane.submit(&spec)
    };
    let a = job("a", Access::Shared { slots: 1 });
    let b = job("b", Access::Shared { slots: 1 });
    let x = job("x", Access::Exclusive);
    for id in [&a, &b, &x] {
        lane.wait(id, 0);
    }
    let lines = lines(&trace);
    assert!(position(&lines, "start b") < position(&lines, "end a"));
    assert!(position(&lines, "start x") > position(&lines, "end a"));
    assert!(position(&lines, "start x") > position(&lines, "end b"));
}

#[test]
fn project_and_worktree_path_aliases_never_enter_the_journal() {
    let lane = Lane::new();
    let project = lane.root.join("project");
    std::fs::create_dir(&project).unwrap();
    let link = lane.root.join("project-link");
    std::os::unix::fs::symlink(&project, &link).unwrap();
    let canonical =
        lane.submit(&lane.spec_in("canonical", "true", ResourceScope::Project(project.clone())));
    lane.wait(&canonical, 0);
    for alias in [project.join("..").join("project"), link] {
        for scope in [
            ResourceScope::Project(alias.clone()),
            ResourceScope::Worktree(alias.clone()),
        ] {
            let out = lane.cli(
                &["job", "submit", "--spec", "-"],
                Some(&lane.spec_in("alias", "true", scope)),
            );
            assert!(!out.status.success(), "{}", describe(&out));
            assert!(
                String::from_utf8_lossy(&out.stderr).contains("not canonical"),
                "{}",
                describe(&out)
            );
            // The capacity CLI canonicalizes the path before setting the same
            // key; only the store boundary rejects raw aliases.
            let alias = alias.display().to_string();
            let cap = lane.cli(
                &[
                    "resource",
                    "set-capacity",
                    "--name",
                    "build",
                    "--scope",
                    &alias,
                    "--slots",
                    "2",
                ],
                None,
            );
            assert!(cap.status.success(), "{}", describe(&cap));
        }
    }
    assert_eq!(lane.records().len(), 1);
}

#[test]
fn ram_and_disk_shortfalls_queue_with_a_reason() {
    let lane = Lane::new();
    for (name, budget, expected) in [
        ("ram", lane.budget(1 << 60, 0), "RAM"),
        ("disk", lane.budget(0, 1 << 60), "disk"),
    ] {
        let mut spec = lane.spec(name, "true");
        spec.admission = budget;
        let job = lane.submit(&spec);
        let reason = lane.until(|| lane.record(&job).and_then(|record| record.wait_reason));
        assert!(reason.contains(expected), "{reason}");
        let out = lane.cli(&["job", "cancel", &job], None);
        assert!(out.status.success(), "{}", describe(&out));
        lane.wait(&job, CANCELLED);
    }
}

#[test]
fn exclusive_pre_hook_runs_before_grant_and_its_failure_blocks_the_workload() {
    let lane = Lane::new();
    let state = lane.root.join("prestate");
    let mut spec = lane.spec("pre", "true");
    spec.pre_hook = Some(Hook {
        argv: vec![
            "sh".into(),
            "-c".into(),
            format!(
                "'{BORG}' lane --json job status \"$BORG_LANE_JOB\" > '{}'",
                state.display()
            ),
        ],
        timeout_ms: 5_000,
    });
    let job = lane.submit(&spec);
    lane.wait(&job, 0);
    let status: Value = serde_json::from_slice(&std::fs::read(&state).unwrap()).unwrap();
    assert_eq!(status["state"], "Preparing");

    let forbidden = lane.root.join("forbidden");
    let mut spec = lane.spec(
        "failed-pre",
        &format!("echo bad > '{}'", forbidden.display()),
    );
    spec.pre_hook = Some(Hook {
        argv: vec!["sh".into(), "-c".into(), "exit 7".into()],
        timeout_ms: 5_000,
    });
    let job = lane.submit(&spec);
    lane.wait(&job, CANCELLED);
    assert!(!forbidden.exists());
}

#[test]
#[ignore = "requires a systemd user manager: kills a scoped supervisor and recovers it"]
fn supervisor_crash_recovers_only_the_owned_scope() {
    let lane = Lane::new();
    assert!(!lane.degraded, "no systemd user manager");
    let mut spec = lane.spec("orphan", "sleep 60");
    spec.timeout_ms = 70_000;
    let job = lane.submit(&spec);
    let record = lane.until(|| {
        lane.record(&job).filter(|record| {
            record.scope_cgroup.is_some()
                && record.job.as_ref().is_some_and(|handle| {
                    matches!(handle.state, borg_lanes::lanes::JobState::Running { .. })
                })
        })
    });
    assert!(record.scope_cgroup.unwrap().contains("borg-lane-"));
    // This test started exactly this supervisor.
    let pid = record.supervisor_pid.unwrap().to_string();
    assert!(
        Command::new("kill")
            .args(["-KILL", &pid])
            .status()
            .unwrap()
            .success()
    );
    let out = lane.cli(&["job", "recover", "--dry-run"], None);
    assert!(out.status.success(), "{}", describe(&out));
    let planned: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert!(planned.as_array().is_some_and(|ids| !ids.is_empty()));
    let out = lane.cli(&["job", "recover"], None);
    assert!(out.status.success(), "{}", describe(&out));
    let result = lane.wait(&job, CANCELLED);
    assert_eq!(result["state"]["Finished"]["exit_code"], CANCELLED);
    assert!(!lane.record(&job).unwrap().quarantined);
}
