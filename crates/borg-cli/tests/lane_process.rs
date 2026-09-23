//! The real `borg lane` CLI across processes, with fake shell workloads; never
//! launches an engine. Specs are built from the `borg-lanes` types so a schema
//! change breaks this test at compile time rather than at run time.
#![cfg(target_os = "linux")]

mod support;

use std::process::Command;

use borg_lanes::lanes::{Access, Hook, JobState, ResourceKey, ResourceRequest, ResourceScope};
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
    // Recovery must verify and kill the job's own custom-named scope.
    spec.unit_prefix = Some("ab-build".into());
    spec.finish_hook = Some(reporting_hook(&lane, "orphan", "finish"));
    let job = lane.submit(&spec);
    let record = lane.until(|| {
        lane.record(&job).filter(|record| {
            record.scope_cgroup.is_some()
                && record.job.as_ref().is_some_and(|handle| {
                    matches!(handle.state, borg_lanes::lanes::JobState::Running { .. })
                })
        })
    });
    assert!(
        record
            .scope_cgroup
            .unwrap()
            .ends_with(&format!("/ab-build-{job}.scope"))
    );
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
    let recovered = lane.record(&job).unwrap();
    assert!(!recovered.quarantined);
    // Recovery names what it killed, in the evidence and recovery.jsonl.
    let workload = record.workload_pid.unwrap();
    let evidence = recovered.evidence.unwrap_or_default();
    assert!(
        evidence.contains(&format!("recover killed pids [{workload}")),
        "{evidence}"
    );
    let logged = std::fs::read_to_string(lane.state().join("recovery.jsonl")).unwrap();
    assert!(
        logged
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .any(|entry| entry["job"] == job.as_str()
                && entry["killed_pids"]
                    .as_array()
                    .is_some_and(|pids| pids.contains(&Value::from(workload)))),
        "{logged}"
    );
    // The lost supervisor's finish hook still runs, once.
    let finish = lane.root.join("orphan.finish");
    lane.until(|| (!lines(&finish).is_empty()).then_some(()));
    let reported = lines(&finish);
    assert_eq!(reported.len(), 1, "{reported:?}");
    assert!(
        reported[0].starts_with("finish finished 125 ") && reported[0].contains("lost supervisor"),
        "{reported:?}"
    );
}

/// Failure mode: Ctrl-C on the submitting terminal (a signal to its process
/// group) killing the job supervisor, which recovery then turns into a
/// killed build; and a workload that cannot name its own job.
#[test]
fn sigint_to_the_submitters_process_group_does_not_stop_the_job() {
    use std::os::unix::process::CommandExt;
    let lane = Lane::new();
    let marker = lane.root.join("finished");
    let spec = lane.spec(
        "detached",
        &format!(
            "sleep 1; echo \"$BORG_LANE_JOB $BORG_LANES_ROOT\" > '{}'",
            marker.display()
        ),
    );
    let mut submit = lane.command(&["job", "submit", "--spec", "-"]);
    submit.process_group(0).stdin(std::process::Stdio::piped());
    let mut child = submit.spawn().unwrap();
    let group = child.id();
    serde_json::to_writer(child.stdin.take().unwrap(), &spec).unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "{}", describe(&out));
    let job = serde_json::from_slice::<Value>(&out.stdout).unwrap()["job_id"]
        .as_str()
        .unwrap()
        .to_string();
    // What a terminal's Ctrl-C sends to its foreground process group. With a
    // detached supervisor the group may already be empty.
    let _ = Command::new("kill")
        .args(["-INT", "--", &format!("-{group}")])
        .status();
    lane.wait(&job, 0);
    assert_eq!(
        std::fs::read_to_string(&marker).unwrap().trim(),
        format!("{job} {}", lane.state().display())
    );
}

/// A process is gone once /proc has no live (non-zombie) entry for it.
fn gone(pid: &str) -> bool {
    std::fs::read_to_string(format!("/proc/{}/stat", pid.trim())).map_or(true, |stat| {
        stat.rsplit(')')
            .next()
            .is_some_and(|rest| rest.trim_start().starts_with('Z'))
    })
}

/// Failure mode: a running build that cannot be cancelled, or a cancel that
/// leaves the workload running, loses the reason or skips the post hook.
#[test]
fn cancelling_a_running_job_kills_it_and_reports_the_reason() {
    let lane = Lane::new();
    let pid_file = lane.root.join("workload.pid");
    let post = lane.root.join("post-phase");
    let mut spec = lane.spec(
        "long",
        &format!("echo $$ > '{}'; exec sleep 30", pid_file.display()),
    );
    spec.timeout_ms = 60_000;
    spec.post_hook = Some(Hook {
        argv: vec![
            "sh".into(),
            "-c".into(),
            format!("echo \"$BORG_LANE_PHASE\" > '{}'", post.display()),
        ],
        timeout_ms: 5_000,
    });
    let job = lane.submit(&spec);
    let pid = lane.until(|| {
        std::fs::read_to_string(&pid_file)
            .ok()
            .filter(|text| !text.trim().is_empty())
    });
    let cancelled: Value = lane.json(&["job", "cancel", &job]);
    assert_eq!(cancelled["state"], "cancel_requested");
    let out = lane.cli(&["job", "wait", &job], None);
    assert_eq!(out.status.code(), Some(CANCELLED), "{}", describe(&out));
    let waited: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(
        waited["state"]["Cancelled"]["reason"],
        "cancelled by requester"
    );
    assert!(String::from_utf8_lossy(&out.stderr).contains("cancelled by requester"));
    assert!(gone(&pid), "cancelled workload {pid} still running");
    let evidence = lane.record(&job).unwrap().evidence.unwrap_or_default();
    assert!(
        evidence.contains("cancelled: cancelled by requester"),
        "{evidence}"
    );
    if lane.degraded {
        // Unbound post hooks run in their own scope, so degraded mode only
        // records the attempt.
        assert!(evidence.contains("post hook"), "{evidence}");
    } else {
        lane.until(|| {
            std::fs::read_to_string(&post)
                .ok()
                .filter(|phase| phase.trim() == "post-exclusive")
        });
    }
}

/// Failure mode: a workload leader exits but a process it started (a
/// compiler, say) keeps running while the next job already holds the key.
#[test]
fn processes_left_by_a_workload_die_before_the_next_holder_starts() {
    let lane = Lane::new();
    let child = lane.root.join("child.pid");
    let overlap = lane.root.join("overlap");
    let first = lane.submit(&lane.spec(
        "leader",
        &format!("sleep 30 & echo $! > '{}'; exit 0", child.display()),
    ));
    let second = lane.submit(&lane.spec(
        "next",
        &format!(
            "kill -0 \"$(cat '{}')\" 2>/dev/null && echo alive > '{}'; exit 0",
            child.display(),
            overlap.display()
        ),
    ));
    lane.wait(&first, 0);
    lane.wait(&second, 0);
    assert!(
        !overlap.exists(),
        "the next holder ran beside a leftover process"
    );
    let pid = std::fs::read_to_string(&child).unwrap();
    assert!(gone(&pid), "leftover {pid} still running");
    let evidence = lane.record(&first).unwrap().evidence.unwrap_or_default();
    assert!(
        evidence.contains(&format!("killed leftover pids [{}]", pid.trim())),
        "{evidence}"
    );
}

/// Failure mode: a job whose requester went away (its `job wait` killed,
/// a shell closed) still queueing or running a build nobody wants; or a
/// job that someone does wait for being abandoned.
#[test]
fn jobs_are_abandoned_only_once_nobody_waits_for_them() {
    let lane = Lane::new();
    let state = |id: &str| {
        lane.record(id)
            .and_then(|record| record.job)
            .map(|job| job.state)
    };
    let cancelled = |id: &str| matches!(state(id), Some(JobState::Cancelled { ref reason }) if reason == "abandoned: no requester");
    let mut kept = lane.spec("kept", "sleep 1.5");
    kept.abandon_after_ms = Some(500);
    let kept = lane.submit(&kept);
    lane.wait(&kept, 0);

    let mut running = lane.spec("running", "exec sleep 30");
    running.timeout_ms = 60_000;
    running.abandon_after_ms = Some(500);
    let running = lane.submit(&running);
    let mut waiter = lane.command(&["job", "wait", &running]).spawn().unwrap();
    lane.until(|| matches!(state(&running), Some(JobState::Running { .. })).then_some(()));
    // Queued behind `running`, and nobody waits for it.
    let mut queued = lane.spec("queued", "true");
    queued.abandon_after_ms = Some(500);
    let queued = lane.submit(&queued);
    lane.until(|| cancelled(&queued).then_some(()));
    assert!(
        matches!(state(&running), Some(JobState::Running { .. })),
        "a job with a live waiter was abandoned"
    );
    waiter.kill().unwrap();
    waiter.wait().unwrap();
    lane.until(|| cancelled(&running).then_some(()));
}

/// Failure mode: a lock keeper cannot learn that its workload started, a
/// bounded wait cannot give up (or ends the job when it does), or a script
/// cannot read why a job ended or is still waiting.
#[test]
fn wait_until_started_and_timeout_leave_the_job_alone() {
    let lane = Lane::new();
    let mut long = lane.spec("long", "exec sleep 30");
    long.timeout_ms = 60_000;
    let long = lane.submit(&long);
    let started = lane.cli(&["job", "wait", &long, "--until", "started"], None);
    assert!(started.status.success(), "{}", describe(&started));
    let value: Value = serde_json::from_slice(&started.stdout).unwrap();
    assert!(value["state"]["Running"].is_object(), "{value}");

    let queued = lane.submit(&lane.spec("queued", "true"));
    let timed = lane.cli(
        &[
            "job",
            "wait",
            &queued,
            "--until",
            "started",
            "--timeout",
            "1",
        ],
        None,
    );
    assert_eq!(timed.status.code(), Some(124), "{}", describe(&timed));
    assert!(
        String::from_utf8_lossy(&timed.stderr)
            .contains(&format!("timed out waiting for job {queued}"))
    );
    let value: Value = serde_json::from_slice(&timed.stdout).unwrap();
    assert_eq!(value["timed_out"], true);
    let status: Value = lane.json(&["job", "status", &queued]);
    assert_eq!(status["state"], "Queued", "{status}");
    assert!(
        status["wait_reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("busy")),
        "{status}"
    );

    lane.json::<Value>(&["job", "cancel", &long]);
    let out = lane.cli(&["job", "wait", &long], None);
    assert_eq!(out.status.code(), Some(CANCELLED), "{}", describe(&out));
    let value: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(value["reason"], "cancelled by requester");
    assert!(value["exit_code"].is_null());
    assert!(
        value["evidence"]
            .as_str()
            .is_some_and(|e| e.contains("cancelled"))
    );
    let done = lane.wait(&queued, 0);
    assert_eq!(done["exit_code"], 0, "{done}");
    assert!(done.as_object().unwrap().contains_key("wait_reason"));
}

/// A hook that appends its phase and the job's ending to `<root>/<name>.<phase>`.
fn reporting_hook(lane: &Lane, name: &str, phase: &str) -> Hook {
    let out = lane.root.join(format!("{name}.{phase}"));
    Hook {
        argv: vec![
            "sh".into(),
            "-c".into(),
            format!(
                r#"echo "$BORG_LANE_PHASE $BORG_LANE_STATE ${{BORG_LANE_EXIT_CODE:-none}} $BORG_LANE_REASON" >> '{}'"#,
                out.display()
            ),
        ],
        timeout_ms: 10_000,
    }
}

/// Failure mode: the caller's cleanup (a finish hook) skipped on some
/// ending, run twice, or told the wrong outcome; a post hook that cannot
/// see the exit code.
#[test]
fn finish_hooks_run_once_after_every_ending() {
    let lane = Lane::new();
    let reported = |name: &str| lines(&lane.root.join(format!("{name}.finish")));
    let job = |name: &str, script: &str| {
        let mut spec = lane.spec(name, script);
        spec.finish_hook = Some(reporting_hook(&lane, name, "finish"));
        spec
    };
    let mut holder = job("holder", "exec sleep 30");
    holder.timeout_ms = 60_000;
    let holder = lane.submit(&holder);
    let started = lane.cli(&["job", "wait", &holder, "--until", "started"], None);
    assert!(started.status.success(), "{}", describe(&started));
    // Three ways to end while queued behind `holder`.
    let queued = lane.submit(&job("queued", "true"));
    lane.json::<Value>(&["job", "cancel", &queued]);
    let mut timeout = job("timeout", "true");
    timeout.lease.queue_timeout_ms = Some(300);
    let timeout = lane.submit(&timeout);
    let mut abandoned = job("abandoned", "true");
    abandoned.abandon_after_ms = Some(300);
    let abandoned = lane.submit(&abandoned);
    for id in [&timeout, &abandoned] {
        lane.until(|| {
            lane.record(id)
                .and_then(|record| record.job)
                .filter(|job| matches!(job.state, JobState::Cancelled { .. }))
        });
    }
    // A running cancel, then a job that exits 3.
    lane.json::<Value>(&["job", "cancel", &holder]);
    lane.wait(&holder, CANCELLED);
    let mut exited = job("exited", "exit 3");
    if !lane.degraded {
        // Unbound post hooks need their own scope.
        exited.post_hook = Some(reporting_hook(&lane, "exited", "post"));
    }
    let exited = lane.submit(&exited);
    lane.wait(&exited, 3);

    let expected = [
        ("holder", "finish cancelled none cancelled by requester"),
        ("queued", "finish cancelled none cancelled by requester"),
        ("timeout", "finish cancelled none queue timeout"),
        ("abandoned", "finish cancelled none abandoned: no requester"),
        ("exited", "finish finished 3 finished"),
    ];
    for (name, line) in expected {
        lane.until(|| (!reported(name).is_empty()).then_some(()));
        assert_eq!(reported(name), [line], "{name}");
    }
    for id in [&holder, &queued, &timeout, &abandoned, &exited] {
        let record = lane.until(|| lane.record(id).filter(|r| r.finish_hook_outcome.is_some()));
        assert_eq!(record.finish_hook_outcome.as_deref(), Some("exited 0"));
    }
    if !lane.degraded {
        let post = lane.root.join("exited.post");
        lane.until(|| (!lines(&post).is_empty()).then_some(()));
        assert_eq!(lines(&post), ["post-exclusive finished 3 finished"]);
    }
    // Recovery and repeated waits start none of them again.
    let out = lane.cli(&["job", "recover"], None);
    assert!(out.status.success(), "{}", describe(&out));
    lane.wait(&exited, 3);
    std::thread::sleep(std::time::Duration::from_millis(500));
    for (name, line) in expected {
        assert_eq!(reported(name), [line], "{name} ran more than once");
    }
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
