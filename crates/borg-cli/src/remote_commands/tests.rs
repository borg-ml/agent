use super::*;
use std::time::{Duration, Instant};
use tempfile::tempdir;
#[cfg(unix)]
use tokio::io::AsyncReadExt;

#[test]
fn resume_retries_sqlite_contention_but_not_permanent_errors() {
    assert!(local_resume_error_is_retryable(&anyhow::anyhow!(
        "pool timed out while waiting for an open connection"
    )));
    assert!(local_resume_error_is_retryable(&anyhow::anyhow!(
        "database is locked"
    )));
    assert!(!local_resume_error_is_retryable(&anyhow::anyhow!(
        "recorded project directory no longer exists"
    )));
}

#[cfg(unix)]
fn short_socket_tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("borg-session-")
        .tempdir_in("/tmp")
        .expect("short Unix socket test directory")
}

#[test]
fn pending_revert_forks_after_stop_or_actor_disconnect() {
    let mut pending = Some(42);
    assert_eq!(
        take_revert_ready_to_fork(&mut pending, SessionStatus::Running, false),
        None
    );
    assert_eq!(pending, Some(42));
    assert_eq!(
        take_revert_ready_to_fork(&mut pending, SessionStatus::Stopped, false),
        Some(42)
    );
    assert_eq!(pending, None);

    let mut disconnected = Some(84);
    assert_eq!(
        take_revert_ready_to_fork(&mut disconnected, SessionStatus::Running, true),
        Some(84),
        "a lost final status event must not strand a requested revert"
    );
    assert_eq!(disconnected, None);
}

#[test]
fn active_rewind_stops_automatically_instead_of_being_rejected() {
    for status in [
        SessionStatus::Starting,
        SessionStatus::Ready,
        SessionStatus::Running,
        SessionStatus::WaitingForApproval,
        SessionStatus::Completed,
        SessionStatus::Failed,
    ] {
        assert_eq!(revert_start_mode(status), RevertStartMode::StopThenFork);
    }
    assert_eq!(
        revert_start_mode(SessionStatus::Stopped),
        RevertStartMode::ForkNow
    );
}

#[test]
fn persistent_sidecar_aliases_keep_their_durable_lane_identity() {
    assert!(matches!(
        persistent_sidecar_command("/peer claude review this"),
        Some(Ok((
            PersistentSidecar::Claude,
            PersistentSidecarIntent::Prompt(ref prompt)
        ))) if prompt == "review this"
    ));
    assert!(matches!(
        persistent_sidecar_command("/peer gpt clear"),
        Some(Ok((PersistentSidecar::Gpt, PersistentSidecarIntent::Clear)))
    ));
    assert!(matches!(
        persistent_sidecar_command("/peer claude"),
        Some(Ok((
            PersistentSidecar::Claude,
            PersistentSidecarIntent::Ensure
        )))
    ));
    assert!(persistent_sidecar_command("/peer").is_some_and(|result| result.is_err()));
    assert!(
        persistent_sidecar_command("/peer claudette review").is_some_and(|result| result.is_err())
    );
    assert!(persistent_sidecar_command("/peered review").is_none());
}

#[test]
fn director_command_extracts_text_without_matching_longer_commands() {
    assert_eq!(
        director_prompt_command("/director review the focused child").and_then(Result::ok),
        Some("review the focused child".to_string())
    );
    assert!(director_prompt_command("/director").is_some_and(|result| result.is_err()));
    assert!(director_prompt_command("/directorate review").is_none());
    assert_eq!(
        director_prompt_delivery(true, CodingProvider::Codex, true, false),
        PromptDelivery::Steer
    );
    assert_eq!(
        director_prompt_delivery(false, CodingProvider::Codex, true, false),
        PromptDelivery::Steer
    );
    assert_eq!(
        director_prompt_delivery(true, CodingProvider::Codex, true, true),
        PromptDelivery::Queue
    );
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let command = director_prompt_host_command(
        session_id,
        message_id,
        "send this to the root".to_string(),
        vec![PathBuf::from("brief.png")],
        PromptDelivery::Steer,
    );
    assert!(matches!(
        command,
        HostCommand::Prompt {
            session_id: actual_session_id,
            message_id: actual_message_id,
            text,
            attachments,
            delivery: PromptDelivery::Steer,
            ..
        } if actual_session_id == session_id
            && actual_message_id == message_id
            && text == "send this to the root"
            && attachments == vec![PathBuf::from("brief.png")]
    ));
}

#[test]
fn terminal_admission_is_released_by_a_terminal_failed_prompt() {
    let message_id = Uuid::new_v4();
    assert_eq!(
        committed_prompt_id(&SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "preserve this request".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Failed,
            delivery: Some(PromptDelivery::Queue),
        }),
        Some(message_id)
    );
}

#[test]
fn consultation_aliases_route_through_the_primary_model() {
    assert_eq!(
        normalize_consultation_command("/claude review this"),
        "/ask claude review this"
    );
    assert_eq!(
        normalize_consultation_command("/gpt compare these approaches"),
        "/ask gpt compare these approaches"
    );
    assert_eq!(
        normalize_consultation_command("/codex check the design"),
        "/ask gpt-5.6-sol@xhigh check the design"
    );
    assert_eq!(
        normalize_consultation_command("/ask claude review"),
        "/ask claude review"
    );
    assert_eq!(
        normalize_consultation_command("/peer claude review"),
        "/peer claude review"
    );
    assert_eq!(normalize_consultation_command("/claudette"), "/claudette");
    assert_eq!(
        idle_input("/claude review this").1,
        "/ask claude review this"
    );
    assert_eq!(
        running_input("/gpt compare this", CodingProvider::Claude, true, false).1,
        "/ask gpt compare this"
    );
}

#[test]
fn persistent_sidecar_prompt_is_ensured_then_sent_to_the_same_child() {
    let session_id = Uuid::new_v4();
    let commands = persistent_sidecar_commands(
        session_id,
        PersistentSidecar::Claude,
        &PersistentSidecarIntent::Prompt("review the design".to_string()),
        Uuid::nil(),
        &[],
        PromptDelivery::Steer,
    );
    assert_eq!(commands.len(), 2);
    assert!(matches!(
        &commands[0],
        HostCommand::Subagent {
            action: SubagentAction::Ensure {
                task_name,
                provider: CodingProvider::Claude,
                model: Some(model),
                effort: Some(effort),
                ..
            },
            ..
        } if task_name == "claude" && model == "claude-opus-5" && effort == "high"
    ));
    assert!(matches!(
        &commands[1],
        HostCommand::Subagent {
            action: SubagentAction::Prompt {
                target,
                text,
                delivery: PromptDelivery::Steer,
                ..
            },
            ..
        } if target == "/root/claude" && text == "review the design"
    ));
}

#[test]
fn older_history_pages_end_immediately_before_the_loaded_tail() {
    assert_eq!(older_tui_history_after(1), None);
    assert_eq!(older_tui_history_after(513), Some(0));
    assert_eq!(older_tui_history_limit(513, 0), 512);
    assert_eq!(older_tui_history_after(2_000), Some(1_487));
    assert_eq!(older_tui_history_limit(2_000, 1_487), 512);
    assert_eq!(1_487 + RICH_TUI_HISTORY_PAGE_SIZE as u64, 1_999);
    assert_eq!(older_tui_history_after(37), Some(0));
    assert_eq!(older_tui_history_limit(37, 0), 36);
}

#[test]
fn first_resume_scan_is_bounded_before_history_is_selected() {
    assert_eq!(recent_tui_history_after(10), 0);
    assert_eq!(recent_tui_history_after(100), 0);
    assert_eq!(recent_tui_history_after(60_000), 59_488);
}

#[test]
fn extensions_view_keeps_a_rejected_reload_visible() {
    let catalog = crate::extensions::ExtensionCatalog {
        revision: "0123456789abcdef".to_string(),
        load_order: Vec::new(),
        extensions: Vec::new(),
        diagnostics: Vec::new(),
    };

    let summary = live_extension_summary(
        &catalog,
        Some("1 diagnostic rejected the reload; first: unknown field"),
    );

    assert!(summary.contains("No Blu extensions installed."));
    assert!(summary.contains(
            "Last reload rejected; the running revision is unchanged: 1 diagnostic rejected the reload; first: unknown field"
        ));
    assert!(summary.contains("revision 0123456789ab"));
}

#[test]
fn first_resume_frame_keeps_real_recent_conversation_before_lazy_pages() {
    let session_id = Uuid::new_v4();
    let mut events = (1..=200)
        .map(|sequence| {
            SessionEvent::new(
                session_id,
                sequence,
                SessionEventKind::UsageUpdated {
                    provider_duration_ms: 0,
                    turn_id: None,
                    provider_context_reused: None,
                    input_tokens: 1,
                    output_tokens: 0,
                    total_tokens: 1,
                    cached_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    cost_usd: None,
                    cost_microusd: None,
                    cost_basis: String::new(),
                    context_tokens: None,
                    context_window_tokens: None,
                },
            )
        })
        .collect::<Vec<_>>();
    events[20] = SessionEvent::new(
        session_id,
        21,
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::User,
            text: "meaningful resume context".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
    );

    let selected = select_resume_bootstrap_history(events);
    assert_eq!(selected.first().unwrap().sequence, 21);
    assert_eq!(selected.len(), RICH_TUI_HISTORY_EVENT_LIMIT + 1);
    assert!(selected.iter().any(|event| matches!(
        &event.kind,
        SessionEventKind::Message { text, .. } if text == "meaningful resume context"
    )));
}

#[test]
fn first_resume_frame_drops_orphaned_output_and_queued_input() {
    let session_id = Uuid::new_v4();
    let current_user_id = Uuid::new_v4();
    let mut events = (1..=160)
        .map(|sequence| {
            SessionEvent::new(
                session_id,
                sequence,
                SessionEventKind::UsageUpdated {
                    provider_duration_ms: 0,
                    turn_id: None,
                    provider_context_reused: None,
                    input_tokens: 1,
                    output_tokens: 0,
                    total_tokens: 1,
                    cached_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    cost_usd: None,
                    cost_microusd: None,
                    cost_basis: String::new(),
                    context_tokens: None,
                    context_window_tokens: None,
                },
            )
        })
        .collect::<Vec<_>>();
    events[100] = SessionEvent::new(
        session_id,
        101,
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::Assistant,
            text: "orphaned old response".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
    );
    events[110] = SessionEvent::new(
        session_id,
        111,
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::User,
            text: "old queued input".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Queue),
        },
    );
    events[149] = SessionEvent::new(
        session_id,
        150,
        SessionEventKind::Message {
            message_id: current_user_id,
            actor: EventActor::User,
            text: "current user request".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: Some(PromptDelivery::Queue),
        },
    );
    events[150] = SessionEvent::new(
        session_id,
        151,
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::Assistant,
            text: "current response".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
    );

    let selected = select_resume_bootstrap_history(events);
    assert!(selected.iter().any(|event| matches!(
        &event.kind,
        SessionEventKind::Message { text, .. } if text == "current user request"
    )));
    assert!(selected.iter().any(|event| matches!(
        &event.kind,
        SessionEventKind::Message { text, .. } if text == "current response"
    )));
    assert!(!selected.iter().any(|event| matches!(
        &event.kind,
        SessionEventKind::Message { text, .. } if text == "orphaned old response"
    )));
    assert!(!selected.iter().any(|event| matches!(
        &event.kind,
        SessionEventKind::Message {
            status: MessageStatus::Queued,
            ..
        }
    )));
    assert_eq!(
        selected.iter().find_map(|event| match &event.kind {
            SessionEventKind::Message {
                actor: EventActor::User,
                text,
                ..
            } => Some(text.as_str()),
            _ => None,
        }),
        Some("current user request")
    );
}

#[test]
fn history_reprojection_uses_delivered_durable_and_live_projection() {
    let session_id = Uuid::new_v4();
    let ready = SessionState {
        latest_sequence: 4,
        status: Some(SessionStatus::Ready),
        ..SessionState::default()
    };
    let stale_store_snapshot = ready.clone();
    let mut delivered = DeliveredSessionProjection::new(ready);

    delivered
        .observe(&SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ContextWindowUpdated {
                context_tokens: 80,
                context_window_tokens: 100,
            },
        ))
        .unwrap();
    assert_eq!(delivered.state().latest_sequence, 4);
    assert_eq!(delivered.state().usage.context_tokens, Some(80));
    assert_eq!(delivered.state().usage.context_window_tokens, Some(100));

    delivered
        .observe(&SessionEvent::new(
            session_id,
            5,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Running,
                detail: None,
            },
        ))
        .unwrap();

    assert_eq!(stale_store_snapshot.status, Some(SessionStatus::Ready));
    assert_eq!(delivered.state().latest_sequence, 5);
    assert_eq!(delivered.state().status, Some(SessionStatus::Running));
}

#[test]
fn child_history_tail_stays_bounded_after_long_runs_and_forks() {
    assert_eq!(recent_child_history_after(0, 100), 0);
    assert_eq!(recent_child_history_after(0, 60_000), 59_872);
    assert_eq!(recent_child_history_after(59_980, 60_000), 59_980);
}

#[test]
fn initial_subagent_state_uses_only_the_loaded_root_tail() {
    let root = Uuid::new_v4();
    let child = Uuid::new_v4();
    let now = Utc::now();
    let snapshot = SubagentSnapshot {
        session_id: child,
        parent_session_id: root,
        task_name: "/root/worker".to_string(),
        status: borg_remote::SubagentStatus::Ready,
        provider: CodingProvider::Codex,
        model: Some("gpt-test".to_string()),
        effort: Some("low".to_string()),
        cwd: PathBuf::from("/workspace"),
        created_at: now,
        updated_at: now,
        detail: None,
        final_text: None,
        usage: borg_remote::SubagentUsage::default(),
    };
    let events = vec![
        SessionEvent::new(root, 1, SessionEventKind::SessionStarted),
        SessionEvent::new(
            root,
            2,
            SessionEventKind::SubagentActivity {
                activity: borg_remote::SubagentActivityKind::Completed,
                agent: snapshot,
                event: None,
            },
        ),
    ];

    let (team_history, team_snapshots) = subagent_state_from_history(&events);

    assert_eq!(team_history.len(), 1);
    assert_eq!(team_snapshots.len(), 1);
    assert_eq!(team_snapshots[0].session_id, child);
}

#[test]
fn resumed_roster_never_claims_an_unowned_child_is_running() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let child = Uuid::new_v4();
    let now = Utc::now();
    let mut snapshot = SubagentSnapshot {
        session_id: child,
        parent_session_id: root,
        task_name: "/root/worker".to_string(),
        status: SubagentStatus::Running,
        provider: CodingProvider::Codex,
        model: Some("gpt-test".to_string()),
        effort: Some("low".to_string()),
        cwd: PathBuf::from("/workspace"),
        created_at: now,
        updated_at: now,
        detail: None,
        final_text: None,
        usage: borg_remote::SubagentUsage::default(),
    };

    reconcile_dormant_subagent_snapshot(directory.path(), &mut snapshot);
    assert_eq!(snapshot.status, SubagentStatus::Ready);
    assert!(
        snapshot
            .detail
            .as_deref()
            .unwrap()
            .contains("follow up to wake")
    );

    let child_path = directory
        .path()
        .join("subagents")
        .join(format!("{child}.lock"));
    let _writer = SessionWriterLease::try_acquire(&child_path)
        .unwrap()
        .expect("test owns child");
    snapshot.status = SubagentStatus::Running;
    snapshot.detail = None;
    reconcile_dormant_subagent_snapshot(directory.path(), &mut snapshot);
    assert_eq!(snapshot.status, SubagentStatus::Running);
}

#[tokio::test]
async fn resumed_roster_prefers_the_child_terminal_ledger_over_a_stale_parent_mirror() {
    let directory = tempdir().unwrap();
    let root = Uuid::new_v4();
    let child = Uuid::new_v4();
    let now = Utc::now();
    let store = SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    store.create_session(child).await.unwrap();
    store
        .append(SessionEvent::new(
            child,
            0,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Stopped,
                detail: Some("crash cleanup completed".to_string()),
            },
        ))
        .await
        .unwrap();
    let mut snapshots = vec![SubagentSnapshot {
        session_id: child,
        parent_session_id: root,
        task_name: "/root/worker".to_string(),
        status: SubagentStatus::Running,
        provider: CodingProvider::Codex,
        model: Some("gpt-test".to_string()),
        effort: Some("low".to_string()),
        cwd: PathBuf::from("/workspace"),
        created_at: now,
        updated_at: now,
        detail: Some("turn phase: provider active".to_string()),
        final_text: None,
        usage: borg_remote::SubagentUsage::default(),
    }];

    reconcile_subagent_snapshots(&store, directory.path(), &mut snapshots).await;

    assert_eq!(snapshots[0].status, SubagentStatus::Stopped);
    assert_eq!(
        snapshots[0].detail.as_deref(),
        Some("crash cleanup completed")
    );
}

#[tokio::test]
async fn child_history_excludes_fork_inherited_director_events() {
    let directory = tempdir().expect("tempdir");
    let store = SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
        .await
        .expect("session store");
    let parent = Uuid::new_v4();
    let child = Uuid::new_v4();
    store.create_session(parent).await.expect("parent");
    store
        .append(SessionEvent::new(
            parent,
            0,
            SessionEventKind::SessionStarted,
        ))
        .await
        .expect("director event");
    store.fork_before(parent, child, 2).await.expect("fork");
    store
        .append(SessionEvent::new(
            child,
            0,
            SessionEventKind::Error {
                message: "child authored event".to_string(),
            },
        ))
        .await
        .expect("child event");

    let history = child_authored_history(&store, child)
        .await
        .expect("authored history");
    assert_eq!(history.len(), 1);
    assert!(matches!(
        history[0].kind,
        SessionEventKind::Error { ref message } if message == "child authored event"
    ));
}

#[test]
fn reinstalling_the_remote_host_restarts_it_on_the_new_binary() {
    assert_eq!(
        host_service_systemctl_commands(),
        [
            &["--user", "daemon-reload"][..],
            &["--user", "enable", "borg-remote.service"][..],
            &["--user", "restart", "borg-remote.service"][..],
        ]
    );
}

#[test]
fn isolated_remote_host_service_is_fail_closed_and_sandboxed() {
    let config = HostConfig {
        server: "https://borg.example".to_string(),
        host_id: Uuid::new_v4(),
        host_token: "secret".to_string(),
        name: "isolated".to_string(),
        roots: vec![PathBuf::from("/workspace")],
        execution_profile: HostExecutionProfile::IsolatedHosted,
        resource_limits: Default::default(),
    };
    let unit = host_service_unit(
        Path::new("/usr/local/bin/borg"),
        Path::new("/home/borg/.borg/remote/host.json"),
        &config,
        "/usr/bin:/bin",
        &["203.0.113.10/32".to_string(), "127.0.0.53".to_string()],
    )
    .expect("isolated service unit");

    for directive in [
        "NoNewPrivileges=true",
        "PrivateDevices=true",
        "ProtectSystem=strict",
        "CapabilityBoundingSet=",
        "CPUQuota=400%",
        "MemoryMax=8589934592",
        "TasksMax=512",
        "IPAddressDeny=any",
        "IPAddressAllow=203.0.113.10/32",
        "ReadWritePaths=\"/workspace\"",
        "BORG_HOST_EXECUTION_PROFILE=isolated_hosted",
        "BORG_HOST_ISOLATION_ATTESTATION=systemd-user-sandbox-v1",
    ] {
        assert!(unit.contains(directive), "missing {directive} in {unit}");
    }
}

#[test]
fn trusted_remote_host_service_keeps_legacy_unit_small() {
    let config = HostConfig {
        server: "https://borg.example".to_string(),
        host_id: Uuid::new_v4(),
        host_token: "secret".to_string(),
        name: "trusted".to_string(),
        roots: vec![PathBuf::from("/workspace")],
        execution_profile: HostExecutionProfile::TrustedUser,
        resource_limits: Default::default(),
    };
    let unit = host_service_unit(
        Path::new("/usr/local/bin/borg"),
        Path::new("/home/borg/.borg/remote/host.json"),
        &config,
        "/usr/bin:/bin",
        &[],
    )
    .expect("trusted service unit");
    assert!(!unit.contains("IPAddressDeny=any"));
    assert!(!unit.contains("NoNewPrivileges=true"));
    assert!(unit.contains("ExecStart=\"/usr/local/bin/borg\""));
}

#[test]
fn isolated_remote_host_service_requires_network_allowlist() {
    let config = HostConfig {
        server: "https://borg.example".to_string(),
        host_id: Uuid::new_v4(),
        host_token: "secret".to_string(),
        name: "isolated".to_string(),
        roots: vec![PathBuf::from("/workspace")],
        execution_profile: HostExecutionProfile::IsolatedHosted,
        resource_limits: Default::default(),
    };
    assert!(
        host_service_unit(
            Path::new("/usr/local/bin/borg"),
            Path::new("/home/borg/.borg/remote/host.json"),
            &config,
            "/usr/bin:/bin",
            &[],
        )
        .is_err()
    );
}

#[test]
fn isolated_remote_host_network_allowlist_rejects_default_routes() {
    assert!(parse_isolated_allowed_networks("0.0.0.0/0").is_err());
    assert!(parse_isolated_allowed_networks("::/0").is_err());
}

#[cfg(target_os = "linux")]
#[test]
fn isolated_remote_host_service_passes_systemd_unit_validation() {
    let config = HostConfig {
        server: "https://borg.example".to_string(),
        host_id: Uuid::new_v4(),
        host_token: "secret".to_string(),
        name: "isolated".to_string(),
        roots: vec![PathBuf::from("/workspace")],
        execution_profile: HostExecutionProfile::IsolatedHosted,
        resource_limits: Default::default(),
    };
    let executable = std::env::current_exe().expect("current test executable");
    let unit = host_service_unit(
        &executable,
        Path::new("/home/borg/.borg/remote/host.json"),
        &config,
        "/usr/bin:/bin",
        &["203.0.113.10/32".to_string(), "127.0.0.53".to_string()],
    )
    .expect("isolated service unit");
    let root = tempdir().expect("unit tempdir");
    let path = root.path().join("borg-remote.service");
    fs::write(&path, unit).expect("write unit");

    let output = std::process::Command::new("systemd-analyze")
        .arg("verify")
        .arg(&path)
        .output()
        .expect("systemd-analyze");
    assert!(
        output.status.success(),
        "systemd unit rejected: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn team_history_restores_every_agent_and_child_approval() {
    let root = Uuid::new_v4();
    let child = Uuid::new_v4();
    let now = Utc::now();
    let mut agent = SubagentSnapshot {
        session_id: child,
        parent_session_id: root,
        task_name: "/root/worker".to_string(),
        status: borg_remote::SubagentStatus::Running,
        provider: CodingProvider::Codex,
        model: Some("gpt-test".to_string()),
        effort: Some("low".to_string()),
        cwd: PathBuf::from("/workspace"),
        created_at: now,
        updated_at: now,
        detail: None,
        final_text: None,
        usage: borg_remote::SubagentUsage::default(),
    };
    let approval_id = "approval-1".to_string();
    let approval = SessionEvent::new(
        root,
        1,
        SessionEventKind::SubagentActivity {
            activity: borg_remote::SubagentActivityKind::Updated,
            agent: agent.clone(),
            event: Some(Box::new(SessionEvent::new(
                child,
                1,
                SessionEventKind::ApprovalRequested {
                    approval_id: approval_id.clone(),
                    title: "Run tests?".to_string(),
                    detail: String::new(),
                    command: None,
                },
            ))),
        },
    );
    agent.status = borg_remote::SubagentStatus::Ready;
    let completed = SessionEvent::new(
        root,
        2,
        SessionEventKind::SubagentActivity {
            activity: borg_remote::SubagentActivityKind::Completed,
            agent: agent.clone(),
            event: None,
        },
    );

    let restored = latest_subagent_snapshots(&[approval.clone(), completed]);
    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0].session_id, child);
    assert_eq!(restored[0].status, borg_remote::SubagentStatus::Ready);
    assert_eq!(
        child_pending_approval_ids(&[approval]),
        HashMap::from([(child, approval_id)])
    );
}

#[test]
fn provider_user_input_response_uses_question_ids() {
    let payload = serde_json::json!({
        "questions": [{
            "id": "scope",
            "header": "Scope",
            "question": "Which scope?",
            "options": [{"label": "Workspace", "description": "Current workspace"}]
        }]
    });

    assert_eq!(
        provider_interaction_response("user_input", &payload, "Workspace").unwrap(),
        serde_json::json!({
            "answers": {
                "scope": { "answers": ["Workspace"] }
            }
        })
    );
}

#[test]
fn provider_user_input_response_requires_all_multiple_answers() {
    let payload = serde_json::json!({
        "questions": [
            {"id": "scope", "header": "Scope", "question": "Which scope?"},
            {"id": "mode", "header": "Mode", "question": "Which mode?"}
        ]
    });

    let response = provider_interaction_response(
        "user_input",
        &payload,
        r#"{"scope":"Workspace","mode":["Fast","Safe"]}"#,
    )
    .unwrap();
    assert_eq!(
        response,
        serde_json::json!({
            "answers": {
                "scope": { "answers": ["Workspace"] },
                "mode": { "answers": ["Fast", "Safe"] }
            }
        })
    );
    assert!(
        provider_interaction_response("user_input", &payload, r#"{"scope":"Workspace"}"#)
            .unwrap_err()
            .to_string()
            .contains("mode")
    );
}

#[test]
fn provider_mcp_elicitation_accepts_structured_content_and_cancellation() {
    let payload = serde_json::json!({
        "requestedSchema": {
            "type": "object",
            "properties": {"region": {"type": "string"}}
        }
    });

    assert_eq!(
        provider_interaction_response("mcp_elicitation", &payload, r#"{"region":"eu"}"#).unwrap(),
        serde_json::json!({
            "action": "accept",
            "content": {"region": "eu"}
        })
    );
    assert_eq!(
        provider_interaction_response("mcp_elicitation", &payload, "/cancel").unwrap(),
        serde_json::json!({"action": "cancel"})
    );
}

#[test]
fn active_message_delivery_respects_provider_capability_and_explicit_override() {
    assert_eq!(
        running_input("plain", CodingProvider::Codex, true, false).0,
        PromptDelivery::Steer
    );
    assert_eq!(
        running_input("plain", CodingProvider::Codex, false, false).0,
        PromptDelivery::Queue
    );
    assert_eq!(
        running_input("/queue later", CodingProvider::Codex, true, false).0,
        PromptDelivery::Queue
    );
    assert_eq!(
        running_input("/steer now", CodingProvider::Codex, false, false).0,
        PromptDelivery::Steer
    );
    assert_eq!(
        running_input("plain", CodingProvider::OpenRouter, true, false).0,
        PromptDelivery::Steer
    );
    assert_eq!(
        running_input("/steer now", CodingProvider::OpenAiCompatible, false, false).0,
        PromptDelivery::Steer
    );
    assert_eq!(
        running_input("plain", CodingProvider::Claude, true, false).0,
        PromptDelivery::Steer
    );
    assert_eq!(
        running_input("/steer now", CodingProvider::Claude, false, false).0,
        PromptDelivery::Steer
    );
}

#[test]
fn ordinary_followups_queue_while_context_is_compacting() {
    assert_eq!(
        running_input("usage details", CodingProvider::Codex, true, true).0,
        PromptDelivery::Queue
    );
    assert_eq!(
        running_input("usage details", CodingProvider::Claude, true, true).0,
        PromptDelivery::Queue
    );
    assert_eq!(
        running_input("/steer now", CodingProvider::Codex, false, true).0,
        PromptDelivery::Steer
    );
    assert!(status_is_compacting(Some(
        "Automatically compacting context"
    )));
    assert!(!status_is_compacting(Some("turn phase: provider active")));
}

#[test]
fn session_switch_distinguishes_owner_shutdown_from_viewer_detach() {
    assert_eq!(
        LocalSessionAccess::Attached
            .switch(SessionStatus::Running)
            .unwrap(),
        SessionSwitch::DetachViewer
    );
    assert_eq!(
        LocalSessionAccess::Owned
            .switch(SessionStatus::Ready)
            .unwrap(),
        SessionSwitch::StopOwnedSession
    );
    assert!(
        LocalSessionAccess::Owned
            .switch(SessionStatus::Running)
            .unwrap_err()
            .to_string()
            .contains("Interrupt the current turn")
    );
}

#[tokio::test]
#[cfg(unix)]
async fn owner_shutdown_hands_active_turn_to_an_attached_viewer() {
    let root = short_socket_tempdir();
    let session_id = Uuid::new_v4();
    let journal_path = root.path().join(format!("{session_id}.lock"));
    let socket_path = session_control_socket_path(root.path(), session_id);
    let presence_path = borg_remote::session_control_presence_socket_path(root.path(), session_id);
    let writer = SessionWriterLease::try_acquire(&journal_path)
        .unwrap()
        .unwrap();
    let (commands, _received) = mpsc::channel(1);
    let server =
        LocalSessionControlServer::start(socket_path, session_id, &writer, commands).unwrap();

    assert!(!owner_shutdown_should_handoff_to_viewer(
        LocalSessionAccess::Owned,
        SessionStatus::Running,
        false,
        Some(&server),
    ));
    let mut presence = tokio::net::UnixStream::connect(presence_path)
        .await
        .unwrap();
    let mut acknowledgement = [0_u8; 1];
    presence.read_exact(&mut acknowledgement).await.unwrap();
    assert!(owner_shutdown_should_handoff_to_viewer(
        LocalSessionAccess::Owned,
        SessionStatus::Running,
        false,
        Some(&server),
    ));
    assert!(!owner_shutdown_should_handoff_to_viewer(
        LocalSessionAccess::Attached,
        SessionStatus::Running,
        false,
        Some(&server),
    ));
}

#[test]
fn obsolete_owner_handoff_waits_for_a_safe_turn_boundary() {
    assert!(stale_local_owner_can_handoff(Some(SessionStatus::Ready)));
    assert!(stale_local_owner_can_handoff(Some(SessionStatus::Stopped)));
    assert!(!stale_local_owner_can_handoff(Some(
        SessionStatus::Starting
    )));
    assert!(!stale_local_owner_can_handoff(Some(SessionStatus::Running)));
    assert!(!stale_local_owner_can_handoff(Some(
        SessionStatus::WaitingForApproval
    )));
    assert!(!stale_local_owner_can_handoff(None));
}

#[test]
fn owned_resume_presents_stopped_state_as_ready_while_rehydrating() {
    let state = SessionState {
        latest_sequence: 10_000,
        status: Some(SessionStatus::Stopped),
        status_detail: Some("process exited".to_string()),
        ..SessionState::default()
    };

    let displayed = resume_display_state(state.clone(), LocalSessionAccess::Owned, true);
    assert_eq!(displayed.status, Some(SessionStatus::Ready));
    assert_eq!(displayed.status_detail, None);
    assert_eq!(displayed.latest_sequence, state.latest_sequence);

    assert_eq!(
        resume_display_state(state.clone(), LocalSessionAccess::Attached, true).status,
        Some(SessionStatus::Stopped)
    );
    assert_eq!(
        resume_display_state(state, LocalSessionAccess::Owned, false).status,
        Some(SessionStatus::Stopped)
    );
}

#[tokio::test]
#[cfg(unix)]
async fn obsolete_owner_handoff_releases_and_reacquires_the_writer_lease() {
    let root = short_socket_tempdir();
    let session_id = Uuid::new_v4();
    let journal_path = root.path().join(format!("{session_id}.lock"));
    let socket_path = session_control_socket_path(root.path(), session_id);
    let writer = SessionWriterLease::try_acquire(&journal_path)
        .unwrap()
        .unwrap();
    let (commands, mut received) = mpsc::channel(1);
    let _server =
        LocalSessionControlServer::start(socket_path.clone(), session_id, &writer, commands)
            .unwrap();
    let release = tokio::spawn(async move {
        assert!(matches!(
            received.recv().await,
            Some(HostCommand::Stop { session_id: target }) if target == session_id
        ));
        drop(writer);
    });

    let replacement = stop_stale_local_owner_and_acquire(&journal_path, &socket_path, session_id)
        .await
        .unwrap();
    release.await.unwrap();
    drop(replacement);
}

#[tokio::test]
async fn obsolete_owner_handoff_uses_a_free_lease_when_its_socket_is_gone() {
    let root = tempdir().unwrap();
    let session_id = Uuid::new_v4();
    let journal_path = root.path().join(format!("{session_id}.lock"));
    let socket_path = session_control_socket_path(root.path(), session_id);
    let writer = SessionWriterLease::try_acquire(&journal_path)
        .unwrap()
        .unwrap();
    drop(writer);

    // A crashed owner can leave no usable control endpoint. The released OS
    // lock is sufficient evidence that the next resume may take ownership.
    let replacement = stop_stale_local_owner_and_acquire(&journal_path, &socket_path, session_id)
        .await
        .unwrap();
    drop(replacement);
}

#[test]
fn active_session_survives_terminal_hangup_but_idle_session_stops() {
    assert!(should_detach_on_terminal_hangup(
        "SIGHUP",
        SessionStatus::Running,
        false
    ));
    assert!(should_detach_on_terminal_hangup(
        "SIGHUP",
        SessionStatus::WaitingForApproval,
        false
    ));
    assert!(!should_detach_on_terminal_hangup(
        "SIGHUP",
        SessionStatus::Ready,
        false
    ));
    assert!(!should_detach_on_terminal_hangup(
        "SIGTERM",
        SessionStatus::Running,
        true
    ));
    assert!(should_detach_on_terminal_hangup(
        "SIGHUP",
        SessionStatus::Ready,
        true
    ));
}

#[test]
fn tui_frame_interval_preserves_supported_high_refresh_and_caps_extremes() {
    assert_eq!(
        tui_frame_interval(165),
        std::time::Duration::from_nanos(6_060_606)
    );
    assert_eq!(tui_frame_interval(1), tui_frame_interval(MIN_TUI_FPS));
    assert_eq!(tui_frame_interval(1_000), tui_frame_interval(MAX_TUI_FPS));
}

#[test]
fn expensive_draws_leave_time_for_input_and_animation_events() {
    assert_eq!(
        responsive_tui_frame_interval(165, std::time::Duration::from_millis(5), false),
        std::time::Duration::from_millis(15)
    );
    assert_eq!(
        responsive_tui_frame_interval(60, std::time::Duration::from_millis(40), false),
        ACTIVITY_FRAME_INTERVAL
    );
    assert_eq!(
        responsive_tui_frame_interval(60, std::time::Duration::ZERO, false),
        tui_frame_interval(60)
    );
    assert_eq!(
        responsive_tui_frame_interval(60, std::time::Duration::from_millis(40), true),
        std::time::Duration::from_millis(40)
    );
}

#[test]
fn active_terminal_frames_do_not_depend_on_composer_text() {
    assert!(terminal_needs_activity_tick(
        false,
        false,
        SessionStatus::Starting
    ));
    assert!(terminal_needs_activity_tick(
        false,
        false,
        SessionStatus::Running
    ));
    assert!(terminal_needs_activity_tick(
        true,
        false,
        SessionStatus::Ready
    ));
    assert!(terminal_needs_activity_tick(
        false,
        true,
        SessionStatus::Ready
    ));
    assert!(!terminal_needs_activity_tick(
        false,
        false,
        SessionStatus::Ready
    ));
}

#[tokio::test]
async fn resume_target_resolves_saved_session_and_skips_current_for_last() {
    let dir = tempdir().expect("session directory");
    let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    let current = Uuid::new_v4();
    let previous = Uuid::new_v4();
    for session_id in [previous, current] {
        let events = [
            SessionEvent::new(session_id, 1, SessionEventKind::SessionStarted),
            SessionEvent::new(
                session_id,
                2,
                SessionEventKind::SessionConfigured {
                    cwd: dir.path().to_path_buf(),
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: false,
                    response_language: ResponseLanguage::Auto,
                    permission_mode: PermissionMode::FullAccess,
                },
            ),
            SessionEvent::new(
                session_id,
                3,
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: EventActor::User,
                    text: "real resumable work".to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ),
        ];
        store.create_session(session_id).await.unwrap();
        for event in events {
            store.append(event).await.unwrap();
        }
    }

    assert_eq!(
        resolve_resume_target(dir.path(), &store, current, &previous.to_string())
            .await
            .unwrap(),
        previous
    );
    assert_eq!(
        resolve_resume_target(dir.path(), &store, current, "--last")
            .await
            .unwrap(),
        previous
    );
    assert!(
        resolve_resume_target(dir.path(), &store, current, &current.to_string())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn resume_switch_rejects_remote_owned_session_before_stopping_current() {
    let dir = tempdir().expect("session directory");
    let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    let current = Uuid::new_v4();
    let target = Uuid::new_v4();
    store.create_session(current).await.unwrap();
    store.create_session(target).await.unwrap();
    store
        .persist_host_launch_metadata(target, &serde_json::json!({ "source": "remote" }))
        .await
        .unwrap();

    let error = resolve_resume_switch(
        dir.path(),
        &store,
        current,
        &target.to_string(),
        LocalSessionAccess::Owned,
        SessionStatus::Stopped,
    )
    .await
    .expect_err("remote-owned sessions must not tear down the active TUI");
    assert!(error.to_string().contains("background Borg remote host"));
}

#[tokio::test]
async fn recent_sessions_are_ordered_by_latest_persisted_activity() {
    let dir = tempdir().expect("session directory");
    let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    let recently_active = Uuid::new_v4();
    let recent_message = Uuid::new_v4();
    let now = Utc::now();

    async fn write_events(
        store: &SqliteSessionStore,
        cwd: &Path,
        session_id: Uuid,
        events: Vec<(chrono::DateTime<Utc>, SessionEventKind)>,
    ) {
        let started_at = events.first().map_or_else(Utc::now, |(created_at, _)| {
            *created_at - chrono::TimeDelta::nanoseconds(2)
        });
        let records = [
            (started_at, SessionEventKind::SessionStarted),
            (
                started_at + chrono::TimeDelta::nanoseconds(1),
                SessionEventKind::SessionConfigured {
                    cwd: cwd.to_path_buf(),
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: false,
                    response_language: ResponseLanguage::Auto,
                    permission_mode: PermissionMode::FullAccess,
                },
            ),
        ]
        .into_iter()
        .chain(events)
        .enumerate()
        .map(|(index, (created_at, kind))| SessionEvent {
            id: Uuid::new_v4(),
            session_id,
            sequence: index as u64 + 1,
            created_at,
            kind,
        })
        .collect::<Vec<_>>();
        store.create_session(session_id).await.unwrap();
        for event in records {
            store.append(event).await.unwrap();
        }
    }
    write_events(
        &store,
        dir.path(),
        recently_active,
        vec![
            (
                now - chrono::TimeDelta::minutes(5),
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: EventActor::User,
                    text: "older prompt".to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ),
            (
                now,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Ready,
                    detail: None,
                },
            ),
        ],
    )
    .await;
    write_events(
        &store,
        dir.path(),
        recent_message,
        vec![(
            now - chrono::TimeDelta::minutes(1),
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: "newer message but older session activity".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        )],
    )
    .await;

    assert_eq!(
        recent_session_ids(dir.path(), &store).await.unwrap(),
        vec![recently_active, recent_message]
    );
}

#[tokio::test]
async fn resume_picker_titles_and_previews_sessions_from_the_latest_response() {
    let dir = tempdir().expect("session directory");
    let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    let current = Uuid::new_v4();
    let target = Uuid::new_v4();
    store.create_session(current).await.unwrap();
    store.create_session(target).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        SessionEventKind::SessionConfigured {
            cwd: dir.path().to_path_buf(),
            provider: CodingProvider::Codex,
            model: Some("gpt-resume-filter".to_string()),
            effort: Some("high".to_string()),
            fast: false,
            response_language: ResponseLanguage::Auto,
            permission_mode: PermissionMode::FullAccess,
        },
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::User,
            text: "First setup request".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::User,
            text: "Latest **formatted** request".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::Assistant,
            text: "Latest **formatted** response".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
    ] {
        store
            .append(SessionEvent::new(target, 0, kind))
            .await
            .unwrap();
    }

    let options = recent_session_options(dir.path(), &store, current, dir.path(), 8)
        .await
        .unwrap();
    let target = options
        .iter()
        .find(|option| option.id == target)
        .expect("target session should be resumable");

    assert!(target.label.contains("Latest formatted response"));
    assert!(target.label.contains("gpt-resume-filter"));
    assert!(!target.label.contains("First setup request"));
    assert!(target.preview.starts_with("Latest **formatted** response"));
    assert!(target.preview.contains("**Model:** `gpt-resume-filter`"));
    assert!(target.preview.contains("Latest prompt:"));
    assert!(target.preview.contains("Latest formatted request"));
}

#[tokio::test]
async fn resume_discovery_ignores_launch_only_probe_sessions() {
    let dir = tempdir().expect("session directory");
    let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    let probe = Uuid::new_v4();
    let real = Uuid::new_v4();
    for session_id in [probe, real] {
        store.create_session(session_id).await.unwrap();
        for kind in [
            SessionEventKind::SessionStarted,
            SessionEventKind::SessionConfigured {
                cwd: dir.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: Some("gpt-5.6-sol".to_string()),
                effort: Some("low".to_string()),
                fast: false,
                response_language: ResponseLanguage::Auto,
                permission_mode: PermissionMode::FullAccess,
            },
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                detail: None,
            },
            SessionEventKind::StatusChanged {
                status: SessionStatus::Stopped,
                detail: None,
            },
            SessionEventKind::ProviderCapabilitiesUpdated {
                providers: Vec::new(),
            },
            SessionEventKind::EffectiveCapabilitiesUpdated {
                capabilities: borg_remote::EffectiveCapabilities {
                    active: Vec::new(),
                    inactive: Vec::new(),
                },
            },
        ] {
            store
                .append(SessionEvent::new(session_id, 0, kind))
                .await
                .unwrap();
        }
    }
    store
        .append(SessionEvent::new(
            real,
            0,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: "real user work".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: Some(PromptDelivery::Queue),
            },
        ))
        .await
        .unwrap();

    let sessions = recent_session_ids(dir.path(), &store).await.unwrap();
    assert_eq!(sessions, vec![real]);
    assert!(
        store.contains_session(probe).await.unwrap(),
        "filtering the resume surface must not destructively delete stored sessions"
    );
}

#[tokio::test]
async fn continue_selects_the_latest_non_empty_session_in_the_current_directory() {
    let dir = tempdir().expect("session directory");
    let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    let local = Uuid::new_v4();
    let other = Uuid::new_v4();
    for (session_id, cwd) in [
        (local, dir.path().to_path_buf()),
        (other, dir.path().join("other-project")),
    ] {
        store.create_session(session_id).await.unwrap();
        for kind in [
            SessionEventKind::SessionStarted,
            SessionEventKind::SessionConfigured {
                cwd,
                provider: CodingProvider::Codex,
                model: None,
                effort: None,
                fast: false,
                response_language: ResponseLanguage::Auto,
                permission_mode: PermissionMode::FullAccess,
            },
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: "resumable work".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ] {
            store
                .append(SessionEvent::new(session_id, 0, kind))
                .await
                .unwrap();
        }
    }

    assert_eq!(
        latest_session_id_in_directory(dir.path(), &store, dir.path())
            .await
            .unwrap(),
        Some(local)
    );
}

#[tokio::test]
async fn resume_picker_prioritizes_current_directory_and_keeps_global_choices() {
    let dir = tempdir().expect("session directory");
    let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    let current = Uuid::new_v4();
    let local = Uuid::new_v4();
    let global = Uuid::new_v4();
    for session_id in [current, local, global] {
        store.create_session(session_id).await.unwrap();
    }
    for (session_id, cwd, prompt) in [
        (local, dir.path().to_path_buf(), "local session"),
        (global, dir.path().join("another-project"), "global session"),
    ] {
        for kind in [
            SessionEventKind::SessionStarted,
            SessionEventKind::SessionConfigured {
                cwd,
                provider: CodingProvider::Codex,
                model: Some("gpt-5.6-sol".to_string()),
                effort: Some("high".to_string()),
                fast: false,
                response_language: ResponseLanguage::Auto,
                permission_mode: PermissionMode::FullAccess,
            },
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: prompt.to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ] {
            store
                .append(SessionEvent::new(session_id, 0, kind))
                .await
                .unwrap();
        }
    }

    let options = recent_session_options(dir.path(), &store, current, dir.path(), 8)
        .await
        .unwrap();

    assert_eq!(
        options
            .iter()
            .map(|option| (option.id, option.current_directory))
            .collect::<Vec<_>>(),
        [(local, true), (global, false)]
    );
}

#[tokio::test]
async fn resume_picker_loads_older_sessions_in_recent_first_order() {
    const SESSION_COUNT: usize = 12;

    let dir = tempdir().expect("session directory");
    let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    let current = Uuid::new_v4();
    store.create_session(current).await.unwrap();
    let mut session_ids = Vec::with_capacity(SESSION_COUNT);
    for index in 0..SESSION_COUNT {
        let session_id = Uuid::new_v4();
        session_ids.push(session_id);
        store.create_session(session_id).await.unwrap();
        for kind in [
            SessionEventKind::SessionStarted,
            SessionEventKind::SessionConfigured {
                cwd: dir.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: Some(format!("picker-model-{index}")),
                effort: None,
                fast: false,
                response_language: ResponseLanguage::Auto,
                permission_mode: PermissionMode::FullAccess,
            },
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: format!("picker session {index}"),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ] {
            store
                .append(SessionEvent::new(session_id, 0, kind))
                .await
                .unwrap();
        }
    }

    let options = recent_session_options(
        dir.path(),
        &store,
        current,
        dir.path(),
        RESUME_PICKER_SESSION_LIMIT,
    )
    .await
    .unwrap();

    assert_eq!(options.len(), SESSION_COUNT);
    assert_eq!(
        options.iter().map(|option| option.id).collect::<Vec<_>>(),
        session_ids.into_iter().rev().collect::<Vec<_>>()
    );
}

#[tokio::test]
#[ignore = "explicit session-picker p95 performance gate"]
async fn recent_session_picker_p95_gate() {
    const SESSION_COUNT: usize = 109;
    const SAMPLES: usize = 100;

    let dir = tempdir().expect("session directory");
    let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    let current = Uuid::new_v4();
    store.create_session(current).await.unwrap();
    for index in 0..SESSION_COUNT {
        let session_id = Uuid::new_v4();
        store.create_session(session_id).await.unwrap();
        for kind in [
            SessionEventKind::SessionStarted,
            SessionEventKind::SessionConfigured {
                cwd: dir.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: Some("performance-fixture".to_string()),
                effort: None,
                fast: false,
                response_language: ResponseLanguage::Auto,
                permission_mode: PermissionMode::FullAccess,
            },
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: format!("performance fixture prompt {index}"),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ] {
            store
                .append(SessionEvent::new(session_id, 0, kind))
                .await
                .unwrap();
        }
    }

    assert_eq!(
        recent_session_options(
            dir.path(),
            &store,
            current,
            dir.path(),
            RESUME_PICKER_SESSION_LIMIT,
        )
        .await
        .unwrap()
        .len(),
        SESSION_COUNT
    );
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let options = recent_session_options(
            dir.path(),
            &store,
            current,
            dir.path(),
            RESUME_PICKER_SESSION_LIMIT,
        )
        .await
        .unwrap();
        assert_eq!(options.len(), SESSION_COUNT);
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    let p95 = samples[(samples.len() * 95).div_ceil(100).saturating_sub(1)];
    eprintln!("session picker p95: {p95:?}");
    assert!(
        p95 < Duration::from_millis(50),
        "session picker p95 exceeded 50 ms: {p95:?}"
    );
}

#[test]
fn resume_instructions_end_with_copyable_command() {
    let session_id = Uuid::nil();
    let instructions = resume_instructions(session_id, true);

    assert!(instructions.contains("Close the other Borg process"));
    assert_eq!(
        instructions.lines().last(),
        Some("borg resume 00000000-0000-0000-0000-000000000000")
    );
}

#[test]
fn ephemeral_exit_never_advertises_an_unresumable_session() {
    assert!(!should_print_exit_resume(true, None, true));
    assert!(should_print_exit_resume(true, None, false));
    assert!(!should_print_exit_resume(true, Some(Uuid::new_v4()), false));
    assert!(!should_print_exit_resume(false, None, false));
}

#[test]
fn tui_crash_resume_output_is_only_the_two_requested_lines() {
    let session_id = Uuid::nil();
    let instructions = resume_instructions(session_id, false);

    assert_eq!(
        instructions,
        "Copy and paste the line below to resume:\nborg resume 00000000-0000-0000-0000-000000000000"
    );
    assert_eq!(instructions.lines().count(), 2);
    assert!(!instructions.contains("BORG"));
    assert!(!instructions.contains('\x1b'));
}

#[test]
fn redirected_output_never_enters_rich_terminal_mode() {
    assert!(rich_terminal_can_prompt(true, true, false));
    assert!(!rich_terminal_can_prompt(true, false, false));
    assert!(!rich_terminal_can_prompt(true, true, true));
}
