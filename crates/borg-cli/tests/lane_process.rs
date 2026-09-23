//! The real `borg lane` CLI across processes, with fake shell workloads; never
//! launches an engine. Specs are built from the `borg-lanes` types so a schema
//! change breaks this test at compile time rather than at run time.
#![cfg(target_os = "linux")]

mod support;

use std::process::Command;

use borg_lanes::lanes::{Access, Hook, ResourceKey, ResourceRequest, ResourceScope};
use serde_json::Value;
use support::{BORG, CANCELLED, Lane, describe, lines, position, traced};

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

/// Several agents submit a mixed workload concurrently. Judged only from the
/// CLI's own status records: jobs holding the same exclusive key never
/// overlap, a shared host capacity is never over-admitted, and every job ends.
#[test]
fn concurrent_agents_respect_exclusive_keys_and_shared_capacity() {
    const SLOTS: u32 = 10;
    let lane = Lane::new();
    let project = lane.root.join("project");
    std::fs::create_dir(&project).unwrap();
    let out = lane.cli(
        &[
            "resource",
            "set-capacity",
            "--name",
            "ram",
            "--slots",
            &SLOTS.to_string(),
        ],
        None,
    );
    assert!(out.status.success(), "{}", describe(&out));
    let ids: Vec<String> = std::thread::scope(|scope| {
        let agents: Vec<_> = (0..3u32)
            .map(|agent| {
                let lane = &lane;
                let project = &project;
                scope.spawn(move || {
                    (0..4u32)
                        .map(|index| {
                            let n = agent * 4 + index;
                            let seconds = 0.05 + f64::from(n % 4) * 0.05;
                            // The first job of every agent is an identical
                            // build, so pending duplicates may coalesce.
                            let name = if index == 0 {
                                "build:shared".to_string()
                            } else {
                                format!("job:{agent}:{index}")
                            };
                            let mut spec = lane.spec(&name, &format!("sleep {seconds}"));
                            let (scope_key, key) = match n % 3 {
                                0 => (ResourceScope::Worktree(project.clone()), "build"),
                                1 => (ResourceScope::Worktree(project.clone()), "test"),
                                _ => (ResourceScope::Project(project.clone()), "editor"),
                            };
                            spec.lease.resources[0].key.scope = scope_key;
                            spec.lease.resources[0].key.name = key.into();
                            spec.lease.resources.push(ResourceRequest {
                                key: ResourceKey {
                                    scope: ResourceScope::Host,
                                    name: "ram".into(),
                                },
                                access: Access::Shared { slots: 1 + n % 6 },
                            });
                            spec.coalesce = index == 0;
                            spec.lease.queue_timeout_ms = Some(30_000);
                            let id = lane.submit(&spec);
                            lane.wait(&id, 0);
                            id
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        agents
            .into_iter()
            .flat_map(|agent| agent.join().unwrap())
            .collect()
    });
    let records: Vec<_> = lane
        .records()
        .into_iter()
        .filter(|record| ids.contains(&record.ticket.id.to_string()))
        .collect();
    let unique: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(records.len(), unique.len());
    let intervals: Vec<_> = records
        .iter()
        .map(|record| {
            let start = record.started_ms.expect("every job started");
            let finish = record.finished_ms.expect("every job finished");
            let exclusive: Vec<_> = record
                .request
                .resources
                .iter()
                .filter(|resource| matches!(resource.access, Access::Exclusive))
                .map(|resource| resource.key.clone())
                .collect();
            let slots = record
                .request
                .resources
                .iter()
                .find_map(|resource| match resource.access {
                    Access::Shared { slots } => Some(slots),
                    Access::Exclusive => None,
                })
                .unwrap();
            (start, finish, exclusive, slots)
        })
        .collect();
    let mut overlapped = false;
    for (i, (start, finish, keys, _)) in intervals.iter().enumerate() {
        for (other_start, other_finish, other_keys, _) in &intervals[i + 1..] {
            let overlap = start < other_finish && other_start < finish;
            overlapped |= overlap;
            assert!(
                !(overlap && keys.iter().any(|key| other_keys.contains(key))),
                "conflicting jobs overlapped: {keys:?}"
            );
        }
    }
    assert!(overlapped, "disjoint keys never ran concurrently");
    let mut points: Vec<(u64, i64)> = intervals
        .iter()
        .flat_map(|(start, finish, _, slots)| {
            [(*start, i64::from(*slots)), (*finish, -i64::from(*slots))]
        })
        .collect();
    points.sort();
    let mut used = 0;
    for (_, delta) in points {
        used += delta;
        assert!(used <= i64::from(SLOTS), "admitted {used} of {SLOTS} slots");
    }
}

#[test]
fn impossible_disk_budget_times_out_without_launching() {
    let lane = Lane::new();
    let marker = lane.root.join("launched");
    let mut spec = lane.spec("impossible-disk", &format!("touch '{}'", marker.display()));
    spec.admission = lane.budget(0, u64::MAX / 2);
    spec.lease.queue_timeout_ms = Some(500);
    let job = lane.submit(&spec);
    let out = lane.cli(&["job", "wait", &job], None);
    assert!(!out.status.success(), "{}", describe(&out));
    let record = lane.record(&job).unwrap();
    assert!(record.started_ms.is_none());
    assert!(!marker.exists());
}

/// Pending identical jobs coalesce, but a running one was fingerprinted from
/// inputs that may since have changed, so an identical submit must queue.
#[test]
fn identical_submit_does_not_join_a_running_job() {
    let lane = Lane::new();
    let started = lane.root.join("started");
    let spec = lane.spec(
        "revision-a",
        &format!("touch '{}'; sleep 0.5", started.display()),
    );
    let first = lane.submit(&spec);
    lane.until(|| started.exists().then_some(()));
    let second = lane.submit(&spec);
    assert_ne!(first, second, "a running job must not be joined");
    lane.wait(&first, 0);
    lane.wait(&second, 0);
    let first = lane.record(&first).unwrap();
    let second = lane.record(&second).unwrap();
    assert!(second.started_ms.unwrap() >= first.finished_ms.unwrap());
}
