//! Exercise separate processes/journals: a per-journal mutex cannot pass this.
use borg_lanes::lanes::{
    Access, AdmissionBudget, Holder, LaneStore, LeaseRequest, ResourceKey, ResourceRequest,
    ResourceScope,
};
use std::{
    io::{BufRead, BufReader, Write},
    process::{Command, Stdio},
};
use uuid::Uuid;

#[test]
fn worker() {
    let Ok(root) = std::env::var("RAM_TEST_JOURNAL") else {
        return;
    };
    let store = LaneStore::new(&root).unwrap();
    let budget = AdmissionBudget {
        reserve_ram_bytes: std::env::var("RAM_TEST_PEAK").unwrap().parse().unwrap(),
        min_available_ram_bytes: 0,
        min_free_disk_bytes: 0,
        reserve_disk_bytes: 0,
        disk_path: root.into(),
    };
    let lease = store
        .try_acquire_service(
            LeaseRequest {
                resources: vec![ResourceRequest {
                    key: ResourceKey {
                        scope: ResourceScope::Host,
                        name: "worker".into(),
                    },
                    access: Access::Shared { slots: 1 },
                }],
                holder: Holder {
                    participant_id: Uuid::new_v4(),
                    session_id: Uuid::new_v4(),
                    host_pid: Some(std::process::id()),
                    purpose: "service:worker".into(),
                },
                queue_timeout_ms: None,
            },
            &budget,
        )
        .unwrap();
    println!("RAM_RESULT {}", lease.is_some());
    std::io::stdout().flush().unwrap();
    if let Some(lease) = lease {
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
        store.release_lease(&lease).unwrap();
    }
}

#[test]
fn independent_journals_share_host_reservations() {
    let root = tempfile::tempdir().unwrap();
    let peak = borg_lanes::workspace::hygiene::ram_available().unwrap() * 3 / 4;
    let spawn = |name: &str| {
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "worker", "--nocapture"])
            .env("BORG_HOST_ADMISSION_DIR", root.path().join("host"))
            .env("RAM_TEST_JOURNAL", root.path().join(name))
            .env("RAM_TEST_PEAK", peak.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap()
    };
    let mut first = spawn("unreal");
    let read = |child: &mut std::process::Child| {
        BufReader::new(child.stdout.as_mut().unwrap())
            .lines()
            .map(Result::unwrap)
            .find(|line| line.starts_with("RAM_RESULT "))
            .unwrap()
    };
    assert_eq!(read(&mut first), "RAM_RESULT true");
    let mut second = spawn("cargo");
    assert_eq!(read(&mut second), "RAM_RESULT false");
    assert!(second.wait().unwrap().success());
    first.stdin.take().unwrap().write_all(b"release\n").unwrap();
    assert!(first.wait().unwrap().success());
    let mut third = spawn("cargo");
    assert_eq!(read(&mut third), "RAM_RESULT true");
    third.stdin.take().unwrap().write_all(b"release\n").unwrap();
    assert!(third.wait().unwrap().success());
}
