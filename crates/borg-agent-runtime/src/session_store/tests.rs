use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::tempdir;

use super::*;
use crate::{EventActor, PromptDelivery};

async fn store() -> (tempfile::TempDir, SqliteSessionStore) {
    let directory = tempdir().unwrap();
    let store = SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    (directory, store)
}

#[tokio::test]
async fn host_launch_owner_is_atomic_immutable_and_scoped_across_reopen() {
    let (root, store) = store().await;
    let id = Uuid::new_v4();
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    let origin = "https://relay.invalid";
    let metadata = serde_json::json!({"request_id": id});
    sqlx::raw_sql("create trigger reject_owner before insert on host_launch_owners begin select raise(abort, 'injected owner write failure'); end;")
        .execute(store.pool()).await.unwrap();
    assert!(
        store
            .persist_owned_host_launch_metadata(id, &metadata, first, origin)
            .await
            .is_err()
    );
    assert!(
        store.load_host_launch_metadata(id).await.unwrap().is_none(),
        "owner and launch admission must commit together"
    );
    sqlx::query("drop trigger reject_owner")
        .execute(store.pool())
        .await
        .unwrap();
    let bound = Uuid::new_v4();
    store.create_session(bound).await.unwrap();
    let binding = store.workspace_binding(bound).await.unwrap().unwrap();
    store
        .attach_workspace(crate::SessionWorkspaceBinding {
            host_id: Some(second),
            ..binding
        })
        .await
        .unwrap();
    assert!(
        store
            .persist_owned_host_launch_metadata(bound, &metadata, first, origin)
            .await
            .is_err()
    );
    assert!(
        store
            .load_host_launch_metadata(bound)
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.host_launch_owner(bound).await.unwrap().is_none());
    let legacy = Uuid::new_v4();
    store
        .persist_host_launch_metadata(legacy, &metadata)
        .await
        .unwrap();
    store.begin_host_bootstrap(legacy).await.unwrap();
    let (a, b) = tokio::join!(
        store.persist_owned_host_launch_metadata(id, &metadata, first, origin),
        store.persist_owned_host_launch_metadata(id, &metadata, second, origin),
    );
    assert_ne!(
        a.is_ok(),
        b.is_ok(),
        "exactly one concurrent owner may admit the launch"
    );
    let owner = if a.is_ok() { first } else { second };
    let other = if a.is_ok() { second } else { first };
    store.begin_host_bootstrap(id).await.unwrap();
    assert_eq!(
        store
            .pending_host_launch_metadata_for_host(0, Some((owner, origin)), 1)
            .await
            .unwrap()[0]
            .0,
        id,
        "unverified legacy rows cannot crowd out owned recovery"
    );
    assert!(
        store
            .pending_host_launch_metadata_for_host(0, Some((other, origin)), 8)
            .await
            .unwrap()
            .iter()
            .all(|(candidate, _)| *candidate != id)
    );
    // Reopened/imported launches may share timestamps. Page order must remain
    // deterministic, with foreign owners filtered before both limit and offset.
    let mut expected = vec![id];
    for candidate_owner in [owner, other, owner] {
        let candidate = Uuid::new_v4();
        store
            .persist_owned_host_launch_metadata(candidate, &metadata, candidate_owner, origin)
            .await
            .unwrap();
        store.begin_host_bootstrap(candidate).await.unwrap();
        if candidate_owner == owner {
            expected.push(candidate);
        }
    }
    sqlx::query("update host_launches set created_at=?")
        .bind("2026-01-01T00:00:00Z")
        .execute(store.pool())
        .await
        .unwrap();
    expected.sort();
    expected.push(legacy);
    for offset in 0..=expected.len() {
        let page = store
            .pending_host_launch_metadata_for_host(offset, Some((owner, origin)), 1)
            .await
            .unwrap();
        assert_eq!(
            page.into_iter().map(|(id, _)| id).collect::<Vec<_>>(),
            expected
                .get(offset)
                .copied()
                .into_iter()
                .collect::<Vec<_>>()
        );
    }
    drop(store);
    let store = SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    assert_eq!(
        store.host_launch_owner(id).await.unwrap(),
        Some((owner, origin.to_string()))
    );
    store
        .persist_owned_host_launch_metadata(id, &metadata, owner, origin)
        .await
        .unwrap();
    assert!(
        store
            .persist_owned_host_launch_metadata(id, &metadata, other, origin)
            .await
            .is_err()
    );
    assert!(
        store
            .persist_owned_host_launch_metadata(id, &metadata, owner, "https://other.invalid")
            .await
            .is_err()
    );
    assert!(
        store
            .persist_owned_host_launch_metadata(legacy, &metadata, owner, origin)
            .await
            .is_err(),
        "an incoming launch retry alone is not legacy ownership proof"
    );
    assert!(store.host_launch_owner(legacy).await.unwrap().is_none());
    assert_eq!(
        store.load_host_launch_metadata(legacy).await.unwrap(),
        Some(metadata)
    );
    assert_eq!(
        store.host_launch_owner(id).await.unwrap(),
        Some((owner, origin.to_string()))
    );
}

#[tokio::test]
async fn terminal_host_settlement_cancels_abandoned_actions_and_fences_old_leases() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    store
        .persist_owned_host_launch_metadata(
            session_id,
            &serde_json::json!({"request_id": session_id}),
            Uuid::nil(),
            "https://relay.invalid",
        )
        .await
        .unwrap();
    store.begin_host_bootstrap(session_id).await.unwrap();
    let mut first = None;
    for _ in 0..129 {
        let id = Uuid::new_v4();
        store
            .enqueue_action(SessionAction::new(
                id,
                session_id,
                crate::SessionActionKind::Workflow,
                crate::ActionDeliveryPolicy::WhenRunIdle,
                crate::ActionWakePolicy::Immediate,
                serde_json::json!({}),
            ))
            .await
            .unwrap();
        first.get_or_insert(id);
    }
    let first = first.unwrap();
    let leased = store
        .claim_action(
            session_id,
            first,
            "abandoned-worker",
            Duration::from_secs(60),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .settle_terminal_host_session(session_id)
            .await
            .is_err()
    );
    assert_eq!(
        store.pending_actions(session_id, 256).await.unwrap().len(),
        129
    );
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Stopped,
                detail: None,
            },
        ))
        .await
        .unwrap();
    store
        .settle_terminal_host_session(session_id)
        .await
        .unwrap();
    let reopened = SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    reopened
        .settle_terminal_host_session(session_id)
        .await
        .unwrap();
    assert!(
        reopened
            .pending_actions(session_id, 256)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        reopened
            .pending_host_launch_metadata(8)
            .await
            .unwrap()
            .is_empty()
    );
    let action = reopened.action(session_id, first).await.unwrap().unwrap();
    assert_eq!(action.state, SessionActionState::Cancelled);
    assert!(action.lease_token.is_none());
    assert!(
        reopened
            .heartbeat_action(
                session_id,
                first,
                "abandoned-worker",
                leased.lease_token.unwrap(),
                Duration::from_secs(60)
            )
            .await
            .is_err()
    );
    let transitions = reopened
        .action_transitions(session_id, first)
        .await
        .unwrap();
    assert_eq!(
        transitions
            .iter()
            .filter(|t| t.to == SessionActionState::Cancelled)
            .count(),
        1
    );
    assert!(
        reopened
            .pending_host_journals(None, 8)
            .await
            .unwrap()
            .contains(&session_id)
    );
}

#[tokio::test]
async fn host_journal_cursors_preserve_late_events_live_state_and_pagination() {
    let (directory, store) = store().await;
    let first = Uuid::from_u128(1);
    let second = Uuid::from_u128(2);
    let local = Uuid::from_u128(3);
    for id in [first, second, local] {
        store.create_session(id).await.unwrap();
        store
            .append(SessionEvent::new(id, 0, SessionEventKind::SessionStarted))
            .await
            .unwrap();
        if id != local {
            store
                .persist_owned_host_launch_metadata(
                    id,
                    &serde_json::json!({"request_id": id}),
                    Uuid::nil(),
                    "https://relay.invalid",
                )
                .await
                .unwrap();
        }
    }
    assert_eq!(store.pending_host_journals(None, 1).await.unwrap(), [first]);
    assert_eq!(
        store.pending_host_journals(Some(first), 1).await.unwrap(),
        [second]
    );
    assert!(
        store
            .pending_host_journals(Some(second), 1)
            .await
            .unwrap()
            .is_empty()
    );
    store.acknowledge_host_journal(first, 1, 0).await.unwrap();
    store
        .append(SessionEvent::new(
            first,
            0,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Running,
                detail: None,
            },
        ))
        .await
        .unwrap();
    store.acknowledge_host_journal(first, 1, 0).await.unwrap();
    assert_eq!(
        store.pending_host_journals(None, 1).await.unwrap(),
        [first],
        "a stale acknowledgement cannot hide a concurrently appended event"
    );
    store.acknowledge_host_journal(first, 2, 0).await.unwrap();
    store.acknowledge_host_journal(first, 1, 0).await.unwrap();
    assert_eq!(
        store.pending_host_journals(None, 8).await.unwrap(),
        [second],
        "old acknowledgements must not rewind confirmed cursors"
    );
    store
        .append(SessionEvent::new(
            first,
            0,
            SessionEventKind::ContextWindowUpdated {
                context_tokens: 80,
                context_window_tokens: 100,
            },
        ))
        .await
        .unwrap();
    let revision = store.live_events_after(first, 0).await.unwrap()[0].revision;
    assert_eq!(store.pending_host_journals(None, 1).await.unwrap(), [first]);
    store
        .acknowledge_host_journal(first, 2, revision)
        .await
        .unwrap();
    store.acknowledge_host_journal(second, 1, 0).await.unwrap();
    drop(store);
    let store = SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    assert!(
        store
            .pending_host_journals(None, 8)
            .await
            .unwrap()
            .is_empty()
    );
    store
        .append(SessionEvent::new(
            first,
            0,
            SessionEventKind::ContextWindowUpdated {
                context_tokens: 90,
                context_window_tokens: 100,
            },
        ))
        .await
        .unwrap();
    assert_eq!(store.pending_host_journals(None, 8).await.unwrap(), [first]);
    let revision = store.live_events_after(first, 0).await.unwrap()[0].revision;
    store
        .acknowledge_host_journal(first, 2, revision)
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            first,
            0,
            SessionEventKind::ReasoningDelta {
                text: "transient output".to_string(),
            },
        ))
        .await
        .unwrap();
    assert_eq!(store.pending_host_journals(None, 8).await.unwrap(), [first]);
    let cleared = store
        .append(SessionEvent::new(
            first,
            0,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Stopped,
                detail: None,
            },
        ))
        .await
        .unwrap();
    store
        .acknowledge_host_journal(first, cleared.sequence, revision)
        .await
        .unwrap();
    assert!(
        store
            .pending_host_journals(None, 8)
            .await
            .unwrap()
            .is_empty(),
        "cleared live rows must not stay dirty merely because the revision counter is higher"
    );
    let fork_id = Uuid::from_u128(4);
    store
        .append(SessionEvent::new(
            local,
            0,
            message(Uuid::new_v4(), "inherited output"),
        ))
        .await
        .unwrap();
    let fork = store.fork_before(local, fork_id, 3).await.unwrap();
    assert!(fork.inherited_event_count > 0);
    store
        .persist_owned_host_launch_metadata(
            fork_id,
            &serde_json::json!({"request_id": fork_id}),
            Uuid::nil(),
            "https://relay.invalid",
        )
        .await
        .unwrap();
    assert_eq!(
        store.pending_host_journals(None, 8).await.unwrap(),
        [fork_id]
    );
    let events = store.events_after(fork_id, 0, 8).await.unwrap();
    assert_eq!(events.len() as u64, fork.inherited_event_count);
    assert!(events.iter().all(|event| event.session_id == fork_id));
    store
        .acknowledge_host_journal(fork_id, events.last().unwrap().sequence, 0)
        .await
        .unwrap();
    assert!(
        store
            .pending_host_journals(None, 8)
            .await
            .unwrap()
            .is_empty(),
        "inherited events are uploadable even without local event rows"
    );
}

#[tokio::test]
async fn codex_harness_route_survives_restart_clear_fork_and_child_registration() {
    let (directory, store) = store().await;
    let mut expected = Vec::new();
    for native in [true, false] {
        let parent = Uuid::new_v4();
        store.create_session(parent).await.unwrap();
        if !native {
            store
                .append(SessionEvent::new(
                    parent,
                    0,
                    SessionEventKind::SessionStarted,
                ))
                .await
                .unwrap();
        }
        let (first, second) = tokio::join!(
            store.uses_native_codex_harness(parent),
            store.uses_native_codex_harness(parent),
        );
        assert_eq!(first.unwrap(), native);
        assert_eq!(second.unwrap(), native);
        if !native {
            assert!(
                store
                    .record_model_access(parent, CodingProvider::Codex, "account-a")
                    .await
                    .is_err()
            );
        }
        store
            .append(SessionEvent::new(
                parent,
                0,
                SessionEventKind::ContextCleared,
            ))
            .await
            .unwrap();
        let fork = Uuid::new_v4();
        store.fork_before(parent, fork, 1).await.unwrap();
        let child = Uuid::new_v4();
        store.register_child_session(parent, child).await.unwrap();
        store.register_child_session(parent, child).await.unwrap();
        expected.extend([parent, fork, child].map(|id| (id, native)));
    }
    // Attaching an already routed child cannot rewrite its execution contract.
    assert!(
        store
            .register_child_session(expected[0].0, expected[3].0)
            .await
            .is_err()
    );
    store.pool.close().await;
    let reopened = SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    for (session_id, native) in expected {
        assert_eq!(
            reopened
                .uses_native_codex_harness(session_id)
                .await
                .unwrap(),
            native
        );
    }
}

#[tokio::test]
async fn model_access_changes_preserve_history_across_restart_forks_and_children() {
    let (directory, store) = store().await;
    let parent = Uuid::new_v4();
    store.create_session(parent).await.unwrap();
    // Existing native history without account tags must remain usable.
    store
        .append(SessionEvent::new(
            parent,
            0,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "native_model_message".to_string(),
                payload: serde_json::to_value(borg_provider::provider::ModelMessage::user(
                    "keep this history",
                ))
                .unwrap(),
            },
        ))
        .await
        .unwrap();
    store
        .record_model_access(parent, CodingProvider::Codex, "account-a")
        .await
        .unwrap();

    let fork = Uuid::new_v4();
    store.fork_before(parent, fork, 2).await.unwrap();
    let child = Uuid::new_v4();
    store.create_session(child).await.unwrap();
    store
        .record_model_access(child, CodingProvider::Codex, "account-b")
        .await
        .unwrap();
    store.register_child_session(parent, child).await.unwrap();

    let mut histories = Vec::new();
    for id in [parent, fork, child] {
        histories.push((
            id,
            serde_json::to_value(store.read(id).await.unwrap()).unwrap(),
        ));
    }
    store.pool.close().await;
    let reopened = SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    for (id, history) in histories {
        for identity in [
            "account-b",
            "api-sha256:first-key",
            "api-sha256:replacement-key",
            "account-a",
        ] {
            reopened
                .record_model_access(id, CodingProvider::Codex, identity)
                .await
                .unwrap();
            assert_eq!(
                serde_json::to_value(reopened.read(id).await.unwrap()).unwrap(),
                history
            );
        }
    }
    // Different selected accounts do not prevent reconnecting an existing child.
    reopened
        .record_model_access(child, CodingProvider::Codex, "account-c")
        .await
        .unwrap();
    reopened
        .register_child_session(parent, child)
        .await
        .unwrap();
}

#[tokio::test]
async fn runtime_manifest_and_checkpoint_survive_store_reopen_and_detect_worker_restart() {
    let (directory, store) = store().await;
    let path = directory.path().join("sessions.sqlite3");
    let session_id = Uuid::new_v4();
    let first_worker = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();

    let first = store
        .activate_runtime_manifest(session_id, "python", "/workspace", "python3", first_worker)
        .await
        .unwrap();
    assert!(!first.recovered_from_previous_worker);
    assert_eq!(first.manifest.status, RuntimeManifestStatus::Running);

    store
        .record_runtime_execution(session_id, first_worker, "sha256:code", false, None)
        .await
        .unwrap();
    let checkpoint = store
        .save_runtime_checkpoint(
            session_id,
            first_worker,
            "calibration-v1",
            &serde_json::json!({"angle": 12.5, "ticks": 240}),
        )
        .await
        .unwrap();
    assert!(checkpoint.content_hash.starts_with("sha256:"));

    store.pool().close().await;
    let reopened = SqliteSessionStore::open(&path).await.unwrap();
    let persisted = reopened
        .runtime_manifest(session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted.execution_count, 1);
    assert_eq!(persisted.last_code_hash.as_deref(), Some("sha256:code"));
    assert_eq!(
        reopened
            .runtime_checkpoint(session_id, Some("calibration-v1"))
            .await
            .unwrap()
            .unwrap()
            .state,
        serde_json::json!({"angle": 12.5, "ticks": 240})
    );

    let second_worker = Uuid::new_v4();
    let recovered = reopened
        .activate_runtime_manifest(session_id, "python", "/workspace", "python3", second_worker)
        .await
        .unwrap();
    assert!(recovered.recovered_from_previous_worker);
    assert_eq!(recovered.manifest.worker_id, second_worker);

    let idempotent = reopened
        .save_runtime_checkpoint(
            session_id,
            second_worker,
            "calibration-v1",
            &serde_json::json!({"angle": 12.5, "ticks": 240}),
        )
        .await
        .unwrap();
    assert_eq!(idempotent.revision, checkpoint.revision);
    assert!(
        reopened
            .save_runtime_checkpoint(
                session_id,
                second_worker,
                "calibration-v1",
                &serde_json::json!({"angle": 13.0}),
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn harness_state_is_durable_and_rolls_back_without_polluting_runtime_checkpoints() {
    let (directory, store) = store().await;
    let path = directory.path().join("sessions.sqlite3");
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();

    let first = serde_json::json!({
        "schema": 1,
        "entries": [{"id": "memory-a", "content": "first"}],
        "refinements": []
    });
    let second = serde_json::json!({
        "schema": 1,
        "entries": [{"id": "memory-a", "content": "second"}],
        "refinements": [{"id": "refine-1"}]
    });
    store.save_harness_state(session_id, &first).await.unwrap();
    store.save_harness_state(session_id, &second).await.unwrap();
    assert_eq!(
        store.load_harness_state(session_id).await.unwrap(),
        Some(second.clone())
    );

    let restored = store.rollback_harness_state(session_id, 1).await.unwrap();
    assert_eq!(restored, first);
    assert_eq!(
        store.load_harness_state(session_id).await.unwrap(),
        Some(first.clone())
    );
    assert!(store.rollback_harness_state(session_id, 1).await.is_err());
    assert!(
        store
            .runtime_checkpoint(session_id, None)
            .await
            .unwrap()
            .is_none()
    );

    store.pool().close().await;
    let reopened = SqliteSessionStore::open(path).await.unwrap();
    assert_eq!(
        reopened.load_harness_state(session_id).await.unwrap(),
        Some(first)
    );
}

#[tokio::test]
async fn durable_append_waits_through_extended_writer_contention() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();

    let blocker = store.begin_write().await.unwrap();
    let append_store = store.clone();
    let append = tokio::spawn(async move {
        append_store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Ready,
                    detail: None,
                },
            ))
            .await
    });

    tokio::time::sleep(SQLITE_BUSY_TIMEOUT + Duration::from_millis(250)).await;
    assert!(
        !append.is_finished(),
        "writer contention must wait instead of failing the session actor"
    );

    blocker.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), append)
        .await
        .expect("append did not resume after the writer lock cleared")
        .expect("append task panicked")
        .expect("append failed after the writer lock cleared");
}

#[tokio::test]
async fn session_creation_waits_through_extended_writer_contention() {
    let (_directory, store) = store().await;
    let blocker = store.begin_write().await.unwrap();
    let session_id = Uuid::new_v4();
    let create_store = store.clone();
    let create = tokio::spawn(async move { create_store.create_session(session_id).await });

    tokio::time::sleep(SQLITE_BUSY_TIMEOUT + Duration::from_millis(250)).await;
    assert!(
        !create.is_finished(),
        "session creation must wait instead of failing on the busy timeout"
    );

    blocker.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), create)
        .await
        .expect("session creation did not resume after the writer lock cleared")
        .expect("session creation task panicked")
        .expect("session creation failed after the writer lock cleared");
    assert!(store.contains_session(session_id).await.unwrap());
}

#[tokio::test]
async fn workspace_attachment_waits_through_extended_writer_contention() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let original = store.workspace_binding(session_id).await.unwrap().unwrap();
    let expected = SessionWorkspaceBinding {
        host_id: Some(Uuid::new_v4()),
        ..original.clone()
    };
    let other = SqliteSessionStore::open_interactive(directory.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    let blocker = other.begin_write().await.unwrap();
    let attach_store = store.clone();
    let binding = expected.clone();
    let attach = tokio::spawn(async move { attach_store.attach_workspace(binding).await });

    tokio::time::sleep(SQLITE_BUSY_TIMEOUT + Duration::from_millis(250)).await;
    assert!(
        !attach.is_finished(),
        "mirror attachment must wait instead of failing on the busy timeout"
    );
    assert_eq!(
        tokio::time::timeout(
            Duration::from_millis(500),
            store.workspace_binding(session_id)
        )
        .await
        .expect("binding reads must remain available during writer contention")
        .unwrap(),
        Some(original)
    );
    blocker.rollback().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), attach)
            .await
            .expect("attachment did not resume after the writer lock cleared")
            .unwrap()
            .unwrap(),
        expected
    );
    assert_eq!(
        other.workspace_binding(session_id).await.unwrap(),
        Some(expected)
    );
}

#[tokio::test]
async fn workspace_attachment_validates_identity_after_writer_admission() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let stale_binding = store.workspace_binding(session_id).await.unwrap().unwrap();
    let workspace_id = Uuid::new_v4();
    let mut blocker = store.begin_write().await.unwrap();
    // Child registration can replace the initial standalone workspace while
    // a mirror is starting. Its stale attachment must not report success.
    sqlx::query("update session_workspace_bindings set workspace_id=? where session_id=?")
        .bind(workspace_id.to_string())
        .bind(session_id.to_string())
        .execute(&mut *blocker)
        .await
        .unwrap();
    let attach_store = store.clone();
    let attach = tokio::spawn(async move { attach_store.attach_workspace(stale_binding).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    blocker.commit().await.unwrap();
    let error = tokio::time::timeout(Duration::from_secs(2), attach)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("already attached"));
    let actual = store.workspace_binding(session_id).await.unwrap().unwrap();
    assert_eq!(actual.workspace_id, workspace_id);
    assert_eq!(actual.host_id, None);
}

#[tokio::test]
async fn workspace_initialization_serializes_after_extended_writer_contention() {
    let (directory, store) = store().await;
    let other = SqliteSessionStore::open_interactive(directory.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    let blocker = store.begin_write().await.unwrap();
    let initializers = (0..4)
        .map(|index| {
            let store = if index % 2 == 0 {
                store.clone()
            } else {
                other.clone()
            };
            tokio::spawn(async move { store.workspace_store().await })
        })
        .collect::<Vec<_>>();
    tokio::time::sleep(SQLITE_BUSY_TIMEOUT + Duration::from_millis(250)).await;
    assert!(
        initializers.iter().all(|task| !task.is_finished()),
        "workspace initialization must wait instead of failing on the busy timeout"
    );
    blocker.rollback().await.unwrap();
    for initializer in initializers {
        let workspace = tokio::time::timeout(Duration::from_secs(3), initializer)
            .await
            .expect("workspace initialization did not resume")
            .unwrap()
            .expect("concurrent workspace initialization must be idempotent")
            .unwrap();
        assert!(
            workspace
                .list_workspaces_for_participant(Uuid::new_v4())
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn sqlite_full_does_not_poison_subsequent_writes() {
    let directory = tempfile::tempdir().unwrap();
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(directory.path().join("full.sqlite3"))
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal),
        )
        .await
        .unwrap();
    sqlx::query("CREATE TABLE durable (value BLOB NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO durable VALUES ('committed')")
        .execute(&pool)
        .await
        .unwrap();

    let mut transaction = SqliteSessionStore::begin_sqlite_write(&pool).await.unwrap();
    let pages: i64 = sqlx::query_scalar("PRAGMA page_count")
        .fetch_one(&mut *transaction)
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "PRAGMA max_page_count = {pages}"
    )))
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query("INSERT INTO durable VALUES ('uncommitted')")
        .execute(&mut *transaction)
        .await
        .unwrap();
    let error = sqlx::query("INSERT INTO durable VALUES (zeroblob(1048576))")
        .execute(&mut *transaction)
        .await
        .unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("13")
    );
    drop(transaction);

    let mut recovered =
        SqliteSessionStore::begin_sqlite_write_with_timeout(&pool, Duration::from_secs(2))
            .await
            .unwrap();
    sqlx::query("INSERT INTO durable VALUES ('recovered')")
        .execute(&mut *recovered)
        .await
        .unwrap();
    recovered.commit().await.unwrap();
    let mut rolled_back = SqliteSessionStore::begin_sqlite_write(&pool).await.unwrap();
    sqlx::query("INSERT INTO durable VALUES ('rolled back')")
        .execute(&mut *rolled_back)
        .await
        .unwrap();
    drop(rolled_back);
    let values: Vec<String> = sqlx::query_scalar("SELECT value FROM durable ORDER BY rowid")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(values, ["committed", "recovered"]);
    pool.close().await;
}

#[tokio::test]
async fn writer_contention_has_a_bounded_escape_hatch() {
    let (_directory, store) = store().await;
    let blocker = store.begin_write().await.unwrap();

    let result = SqliteSessionStore::begin_sqlite_write_with_timeout(
        store.pool(),
        Duration::from_millis(100),
    )
    .await;

    assert!(matches!(result, Err(sqlx::Error::PoolTimedOut)));
    blocker.rollback().await.unwrap();
}

#[tokio::test]
async fn production_writer_admission_survives_repeated_contention_timeouts() {
    let (_directory, store) = store().await;
    let blocker = store.begin_write().await.unwrap();
    let pool = store.pool().clone();
    let writer = tokio::spawn(async move {
        SqliteSessionStore::begin_sqlite_write_resilient(&pool, Duration::from_millis(100)).await
    });

    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(
        !writer.is_finished(),
        "production writer admission must not fail a session while contention persists"
    );

    blocker.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), writer)
        .await
        .expect("writer did not resume after contention cleared")
        .expect("writer task panicked")
        .expect("writer admission failed after contention cleared")
        .rollback()
        .await
        .unwrap();
}

#[tokio::test]
async fn writer_contention_does_not_exhaust_the_pool() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let blocker = store.begin_write().await.unwrap();

    let contenders = (0..8)
        .map(|_| {
            let pool = store.pool().clone();
            tokio::spawn(async move {
                SqliteSessionStore::begin_sqlite_write_with_timeout(&pool, Duration::from_secs(2))
                    .await
            })
        })
        .collect::<Vec<_>>();

    // Before the admission gate, the seven available connections all sit in
    // SQLite busy waits and the next ordinary read times out waiting for the
    // pool. The read must remain available while the journal writer is held.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(500),
            store.contains_session(session_id)
        )
        .await
        .expect("a read must not wait behind blocked writers")
        .expect("the session read must succeed")
    );

    blocker.rollback().await.unwrap();
    for contender in contenders {
        let _transaction = tokio::time::timeout(Duration::from_secs(2), contender)
            .await
            .expect("contending writer did not finish")
            .expect("contending writer task panicked")
            .expect("contending writer failed after the lock cleared");
    }
}

#[tokio::test]
async fn empty_sessions_are_discarded_but_real_sessions_are_kept() {
    let (_directory, store) = store().await;
    let empty = Uuid::new_v4();
    let real = Uuid::new_v4();
    store.create_session(empty).await.unwrap();
    store.create_session(real).await.unwrap();

    for kind in [
        SessionEventKind::SessionStarted,
        SessionEventKind::SessionConfigured {
            cwd: std::path::PathBuf::from("/tmp/borg-empty"),
            provider: CodingProvider::Codex,
            model: None,
            effort: None,
            fast: false,
            response_language: ResponseLanguage::Auto,
            permission_mode: PermissionMode::FullAccess,
        },
        SessionEventKind::ProviderCapabilitiesUpdated {
            providers: Vec::new(),
        },
        SessionEventKind::EffectiveCapabilitiesUpdated {
            capabilities: crate::EffectiveCapabilities {
                active: Vec::new(),
                inactive: Vec::new(),
            },
        },
    ] {
        store
            .append(SessionEvent::new(empty, 0, kind))
            .await
            .unwrap();
    }
    store
        .append(SessionEvent::new(
            real,
            0,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: "keep this thread".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: Some(PromptDelivery::Queue),
            },
        ))
        .await
        .unwrap();

    assert!(store.discard_empty_session(empty).await.unwrap());
    assert!(!store.contains_session(empty).await.unwrap());
    assert!(!store.discard_empty_session(real).await.unwrap());
    assert!(store.contains_session(real).await.unwrap());
}

#[tokio::test]
async fn opening_schema_v4_migrates_in_place_and_keeps_sessions() {
    let (directory, store) = store().await;
    let path = directory.path().join("sessions.sqlite3");
    let kept = Uuid::new_v4();
    store.create_session(kept).await.unwrap();
    sqlx::query("drop index if exists idx_session_actions_lease_expiry")
        .execute(store.pool())
        .await
        .unwrap();
    for column in [
        "lease_owner",
        "lease_token",
        "lease_heartbeat_at",
        "lease_expires_at",
    ] {
        // These identifiers are fixed by the schema-reset regression fixture;
        // SQLx 0.9 requires the dynamic identifier to be explicitly audited.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "alter table session_actions drop column {column}"
        )))
        .execute(store.pool())
        .await
        .unwrap();
    }
    sqlx::query("update borg_session_schema set version=4 where id=1")
        .execute(store.pool())
        .await
        .unwrap();
    store.pool().close().await;

    let reopened = SqliteSessionStore::open(path).await.unwrap();
    let columns = sqlx::query("pragma table_info(session_actions)")
        .fetch_all(reopened.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect::<HashSet<_>>();
    assert!(
        [
            "lease_owner",
            "lease_token",
            "lease_heartbeat_at",
            "lease_expires_at",
        ]
        .into_iter()
        .all(|column| columns.contains(column))
    );
    let version: i64 = sqlx::query_scalar("select version from borg_session_schema where id=1")
        .fetch_one(reopened.pool())
        .await
        .unwrap();
    assert_eq!(version, SESSION_SCHEMA_VERSION);
    let sessions: i64 = sqlx::query_scalar("select count(*) from sessions")
        .fetch_one(reopened.pool())
        .await
        .unwrap();
    assert_eq!(
        sessions, 1,
        "an additive upgrade must keep the user's sessions"
    );
    assert!(reopened.contains_session(kept).await.unwrap());
    let archive_count = std::fs::read_dir(directory.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("sessions.sqlite3.incompatible-")
        })
        .count();
    assert_eq!(archive_count, 0, "nothing was archived aside");
}

#[tokio::test]
async fn opening_a_future_schema_version_is_refused_without_touching_the_database() {
    let (directory, store) = store().await;
    let path = directory.path().join("sessions.sqlite3");
    sqlx::query("update borg_session_schema set version=? where id=1")
        .bind(SESSION_SCHEMA_VERSION + 1)
        .execute(store.pool())
        .await
        .unwrap();
    store.pool().close().await;

    let error = match SqliteSessionStore::open(&path).await {
        Ok(_) => panic!("a future schema version must be refused"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("future"), "{error}");
    assert!(path.exists());
}

#[tokio::test]
async fn opening_a_legacy_database_without_a_schema_marker_migrates_it() {
    let (directory, store) = store().await;
    let path = directory.path().join("sessions.sqlite3");
    let kept = Uuid::new_v4();
    store.create_session(kept).await.unwrap();
    sqlx::query("drop table borg_session_schema")
        .execute(store.pool())
        .await
        .unwrap();
    store.pool().close().await;

    let reopened = SqliteSessionStore::open(&path).await.unwrap();
    assert!(reopened.contains_session(kept).await.unwrap());
    let version: i64 = sqlx::query_scalar("select version from borg_session_schema where id=1")
        .fetch_one(reopened.pool())
        .await
        .unwrap();
    assert_eq!(version, SESSION_SCHEMA_VERSION);
    assert_eq!(
        std::fs::read_dir(directory.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("sessions.sqlite3.incompatible-")
            })
            .count(),
        0
    );
}

#[tokio::test]
async fn list_sessions_skips_state_for_removed_providers() {
    let (_directory, store) = store().await;
    let valid = Uuid::new_v4();
    let incompatible = Uuid::new_v4();
    store.create_session(valid).await.unwrap();
    store.create_session(incompatible).await.unwrap();
    sqlx::query("update sessions set state_json = ? where id = ?")
        .bind(r#"{"configuration":{"provider":"open_code"}}"#)
        .bind(incompatible.to_string())
        .execute(store.pool())
        .await
        .unwrap();

    let sessions = store.list_sessions(10).await.unwrap();
    assert_eq!(
        sessions
            .into_iter()
            .map(|session| session.session_id)
            .collect::<Vec<_>>(),
        vec![valid]
    );
    assert!(store.contains_session(incompatible).await.unwrap());
}

#[tokio::test]
async fn provider_capability_snapshot_is_durable_metadata_not_context() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let providers = vec![crate::ProviderCapability {
        provider: CodingProvider::Codex,
        installed: true,
        version: Some("test".to_string()),
        authenticated: true,
        auth_detail: Some("Codex subscription authenticated".to_string()),
        auth_methods: vec![crate::ProviderAuthMethod::Subscription],
        can_spawn: true,
        usage: None,
        billing: None,
    }];
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ProviderCapabilitiesUpdated {
                providers: providers.clone(),
            },
        ))
        .await
        .unwrap();

    assert_eq!(
        store.state(session_id).await.unwrap().provider_capabilities,
        providers
    );
    assert!(
        store
            .recovery(session_id)
            .await
            .unwrap()
            .context_events
            .is_empty()
    );
    assert_eq!(store.read(session_id).await.unwrap().len(), 1);
}

#[tokio::test]
async fn sessions_have_stable_workspace_bindings_and_children_inherit_the_team_workspace() {
    let (directory, store) = store().await;
    let root = Uuid::new_v4();
    store.create_session(root).await.unwrap();
    let root_binding = store.workspace_binding(root).await.unwrap().unwrap();
    assert_eq!(root_binding.workspace_id, root);
    assert_eq!(root_binding.participant_id, root);

    let host_id = Uuid::new_v4();
    let reattached = store
        .attach_workspace(SessionWorkspaceBinding {
            host_id: Some(host_id),
            attached_at: Utc::now(),
            ..root_binding.clone()
        })
        .await
        .unwrap();
    assert_eq!(reattached.host_id, Some(host_id));
    assert_eq!(
        store
            .workspace_binding(root)
            .await
            .unwrap()
            .unwrap()
            .host_id,
        Some(host_id)
    );
    assert!(
        store
            .attach_workspace(SessionWorkspaceBinding {
                workspace_id: Uuid::new_v4(),
                ..reattached
            })
            .await
            .unwrap_err()
            .to_string()
            .contains("already attached")
    );

    let child = Uuid::new_v4();
    let child_journal = directory
        .path()
        .join("subagents")
        .join(format!("{child}.lock"));
    let _writer = crate::SessionWriterLease::acquire(&child_journal).unwrap();
    store.register_child_session(root, child).await.unwrap();
    let child_binding = store.workspace_binding(child).await.unwrap().unwrap();
    assert_eq!(child_binding.workspace_id, root);
    assert_eq!(child_binding.participant_id, child);
}

#[tokio::test]
async fn new_session_can_start_in_a_selected_workspace_without_becoming_rebindable() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let binding = store
        .create_session_in_workspace(session_id, workspace_id)
        .await
        .unwrap();
    assert_eq!(binding.workspace_id, workspace_id);
    assert_eq!(binding.participant_id, session_id);
    assert_eq!(
        store.workspace_binding(session_id).await.unwrap().unwrap(),
        binding
    );
    assert!(
        store
            .attach_workspace(SessionWorkspaceBinding {
                workspace_id: Uuid::new_v4(),
                ..binding
            })
            .await
            .unwrap_err()
            .to_string()
            .contains("already attached")
    );
}

#[tokio::test]
async fn session_writes_wait_for_short_cross_connection_contention() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let blocker = store.begin_write().await.unwrap();
    let contender = store.clone();
    let append = tokio::spawn(async move {
        contender
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!append.is_finished());
    blocker.commit().await.unwrap();

    let event = tokio::time::timeout(Duration::from_secs(1), append)
        .await
        .expect("contending append should resume")
        .expect("append task should not panic")
        .expect("append should succeed after lock release");
    assert_eq!(event.sequence, 1);
}

fn configured(directory: &Path) -> SessionEventKind {
    SessionEventKind::SessionConfigured {
        cwd: directory.to_path_buf(),
        provider: CodingProvider::Codex,
        model: Some("gpt-test".to_string()),
        effort: Some("high".to_string()),
        fast: false,
        response_language: ResponseLanguage::Auto,
        permission_mode: PermissionMode::FullAccess,
    }
}

fn message(message_id: Uuid, text: &str) -> SessionEventKind {
    SessionEventKind::Message {
        message_id,
        actor: EventActor::User,
        text: text.to_string(),
        attachments: Vec::new(),
        status: MessageStatus::Complete,
        delivery: Some(PromptDelivery::Steer),
    }
}

#[tokio::test]
async fn prompt_event_boundaries_drive_one_atomic_action_lifecycle() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::SessionStarted,
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            configured(directory.path()),
        ))
        .await
        .unwrap();
    let message_id = Uuid::new_v4();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "do the work".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Queue),
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        store
            .action(session_id, message_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        SessionActionState::Admitted
    );
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::TurnStarted {
                message_id,
                provider: CodingProvider::Codex,
                model: Some("gpt-test".to_string()),
                effort: Some("high".to_string()),
                fast: false,
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        store
            .action(session_id, message_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        SessionActionState::Running
    );
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::TurnCompleted {
                message_id,
                provider_session_id: Some("provider-session".to_string()),
                final_text: "done".to_string(),
                error: None,
            },
        ))
        .await
        .unwrap();
    let completed = store.action(session_id, message_id).await.unwrap().unwrap();
    assert_eq!(completed.state, SessionActionState::Completed);
    assert!(completed.accepted_at.is_some());
    assert!(completed.delivered_at.is_some());
    assert!(completed.completed_at.is_some());
    let transitions = store
        .action_transitions(session_id, message_id)
        .await
        .unwrap();
    assert_eq!(
        transitions
            .iter()
            .map(|transition| (transition.from, transition.to))
            .collect::<Vec<_>>(),
        [
            (None, SessionActionState::Queued),
            (
                Some(SessionActionState::Queued),
                SessionActionState::Admitted
            ),
            (
                Some(SessionActionState::Admitted),
                SessionActionState::Delivered
            ),
            (
                Some(SessionActionState::Delivered),
                SessionActionState::Preparing
            ),
            (
                Some(SessionActionState::Preparing),
                SessionActionState::Committing
            ),
            (
                Some(SessionActionState::Committing),
                SessionActionState::Running
            ),
            (
                Some(SessionActionState::Running),
                SessionActionState::Completed
            ),
        ]
    );
    assert!(
        store
            .pending_actions(session_id, 10)
            .await
            .unwrap()
            .is_empty()
    );

    // Re-admission with the same id/payload is an idempotent read of the
    // durable terminal action, not a duplicate action.
    let replay = SessionAction::new(
        message_id,
        session_id,
        completed.kind,
        completed.delivery,
        completed.wake,
        completed.payload.clone(),
    );
    assert_eq!(
        store.enqueue_action(replay).await.unwrap().state,
        completed.state
    );
}

#[tokio::test]
async fn prompt_admission_is_durable_visible_and_idempotent_before_routing() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }
    let admission = || {
        SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "persist me before acknowledgement".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Steer),
            },
        )
    };

    let first = store.admit_prompt(admission()).await.unwrap();
    let duplicate = store.admit_prompt(admission()).await.unwrap();

    assert_eq!(duplicate.id, first.id);
    assert_eq!(duplicate.sequence, first.sequence);
    assert_eq!(
        store
            .state(session_id)
            .await
            .unwrap()
            .latest_prompt
            .as_deref(),
        Some("persist me before acknowledgement")
    );
    assert_eq!(
        store
            .read(session_id)
            .await
            .unwrap()
            .into_iter()
            .filter(|event| {
                matches!(
                    event.kind,
                    SessionEventKind::Message {
                        message_id: stored_id,
                        ..
                    } if stored_id == message_id
                )
            })
            .count(),
        1
    );

    let mut conflicting = admission();
    let SessionEventKind::Message { text, .. } = &mut conflicting.kind else {
        unreachable!()
    };
    *text = "different content".to_string();
    assert!(store.admit_prompt(conflicting).await.is_err());
}

#[tokio::test]
async fn stale_in_progress_message_does_not_resurrect_terminal_action() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "original prompt".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::TurnStarted {
            message_id,
            provider: CodingProvider::Codex,
            model: Some("gpt-test".to_string()),
            effort: Some("high".to_string()),
            fast: false,
        },
        SessionEventKind::TurnCompleted {
            message_id,
            provider_session_id: None,
            final_text: String::new(),
            error: Some("provider stopped".to_string()),
        },
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "coalesced stale snapshot".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: Some(PromptDelivery::Queue),
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let action = store.action(session_id, message_id).await.unwrap().unwrap();
    assert_eq!(action.state, SessionActionState::Failed);
    assert_eq!(action.payload["text"], "original prompt");
    assert!(
        !store
            .action_transitions(session_id, message_id)
            .await
            .unwrap()
            .iter()
            .any(|transition| {
                transition.from == Some(SessionActionState::Failed)
                    && transition.to == SessionActionState::Queued
            })
    );
}

#[tokio::test]
async fn accepted_steer_queue_event_reopens_a_terminal_action_projection() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "accepted steer".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Steer),
        },
        SessionEventKind::TurnStarted {
            message_id,
            provider: CodingProvider::Codex,
            model: Some("gpt-test".to_string()),
            effort: Some("high".to_string()),
            fast: false,
        },
        SessionEventKind::TurnCompleted {
            message_id,
            provider_session_id: None,
            final_text: String::new(),
            error: None,
        },
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "accepted steer".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Queue),
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let action = store.action(session_id, message_id).await.unwrap().unwrap();
    assert_eq!(action.kind, crate::SessionActionKind::Prompt);
    assert_eq!(action.state, SessionActionState::Queued);
    assert!(
        store
            .pending_actions(session_id, 10)
            .await
            .unwrap()
            .iter()
            .any(|pending| {
                pending.action_id == message_id && pending.kind == crate::SessionActionKind::Prompt
            })
    );
}

#[tokio::test]
async fn internal_messages_do_not_reuse_child_action_identity() {
    let (directory, store) = store().await;
    let child_session_id = Uuid::new_v4();
    let parent_session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    for session_id in [child_session_id, parent_session_id] {
        store.create_session(session_id).await.unwrap();
        for kind in [
            SessionEventKind::SessionStarted,
            configured(directory.path()),
        ] {
            store
                .append(SessionEvent::new(session_id, 0, kind))
                .await
                .unwrap();
        }
    }
    store
        .append(SessionEvent::new(
            child_session_id,
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "team input".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Queue),
            },
        ))
        .await
        .unwrap();

    store
        .append(SessionEvent::new(
            parent_session_id,
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::System,
                text: "internal child report".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Queue),
            },
        ))
        .await
        .unwrap();

    assert!(
        store
            .action(parent_session_id, message_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn recovered_steer_accepts_a_coalesced_queue_snapshot() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "steer while active".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Steer),
        },
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "earlier queued input\n\nsteer while active".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: Some(PromptDelivery::Queue),
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let action = store.action(session_id, message_id).await.unwrap().unwrap();
    assert_eq!(action.kind, crate::SessionActionKind::Prompt);
    assert_eq!(action.state, SessionActionState::Admitted);
    assert_eq!(
        action.payload["text"],
        "earlier queued input\n\nsteer while active"
    );
}

#[tokio::test]
async fn concurrent_claims_have_one_winner_and_same_owner_claim_is_idempotent() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let action_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    store
        .enqueue_action(SessionAction::new(
            action_id,
            session_id,
            crate::SessionActionKind::Prompt,
            crate::ActionDeliveryPolicy::NextTurnBoundary,
            crate::ActionWakePolicy::Immediate,
            serde_json::json!({"text": "claim once"}),
        ))
        .await
        .unwrap();

    let mut tasks = Vec::new();
    for worker_number in 0..8 {
        let contender = store.clone();
        tasks.push(tokio::spawn(async move {
            contender
                .claim_action(
                    session_id,
                    action_id,
                    &format!("worker-{worker_number}"),
                    Duration::from_secs(30),
                )
                .await
                .unwrap()
        }));
    }
    let mut winner = None;
    for task in tasks {
        if let Some(action) = task.await.unwrap() {
            assert!(winner.is_none(), "two workers claimed the action");
            winner = Some(action);
        }
    }
    let winner = winner.expect("one worker should claim the queued action");
    let replay = store
        .claim_action(
            session_id,
            action_id,
            winner.lease_owner.as_deref().unwrap(),
            Duration::from_secs(30),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replay.lease_token, winner.lease_token);
    assert_eq!(replay.lease_heartbeat_at, winner.lease_heartbeat_at);
    assert_eq!(
        store
            .action_transitions(session_id, action_id)
            .await
            .unwrap()
            .len(),
        1,
        "claiming must not create duplicate lifecycle transitions"
    );
}

#[tokio::test]
async fn expired_leases_requeue_once_and_fence_stale_workers() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let action_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    store
        .enqueue_action(SessionAction::new(
            action_id,
            session_id,
            crate::SessionActionKind::Prompt,
            crate::ActionDeliveryPolicy::NextTurnBoundary,
            crate::ActionWakePolicy::Immediate,
            serde_json::json!({"text": "recover me"}),
        ))
        .await
        .unwrap();
    let claimed = store
        .claim_action(session_id, action_id, "worker-a", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let token = claimed.lease_token.unwrap();
    let expired_at = claimed.lease_expires_at.unwrap() + chrono::Duration::milliseconds(1);
    store
        .transition_claimed_action(ClaimedActionTransition {
            session_id,
            action_id,
            lease_owner: "worker-a".to_string(),
            lease_token: token,
            expected: Some(SessionActionState::Queued),
            next: SessionActionState::Admitted,
            error: None,
        })
        .await
        .unwrap();
    store
        .transition_claimed_action(ClaimedActionTransition {
            session_id,
            action_id,
            lease_owner: "worker-a".to_string(),
            lease_token: token,
            expected: Some(SessionActionState::Admitted),
            next: SessionActionState::Delivered,
            error: None,
        })
        .await
        .unwrap();
    store
        .transition_claimed_action(ClaimedActionTransition {
            session_id,
            action_id,
            lease_owner: "worker-a".to_string(),
            lease_token: token,
            expected: Some(SessionActionState::Delivered),
            next: SessionActionState::Preparing,
            error: None,
        })
        .await
        .unwrap();
    store
        .transition_claimed_action(ClaimedActionTransition {
            session_id,
            action_id,
            lease_owner: "worker-a".to_string(),
            lease_token: token,
            expected: Some(SessionActionState::Preparing),
            next: SessionActionState::Committing,
            error: None,
        })
        .await
        .unwrap();
    let recovered = store
        .recover_expired_actions(session_id, expired_at, 10)
        .await
        .unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].state, SessionActionState::Queued);
    assert!(recovered[0].lease_owner.is_none());
    assert!(
        store
            .heartbeat_action(
                session_id,
                action_id,
                "worker-a",
                token,
                Duration::from_secs(30),
            )
            .await
            .is_err()
    );
    assert!(
        store
            .recover_expired_actions(session_id, Utc::now(), 10)
            .await
            .unwrap()
            .is_empty()
    );
    let reclaimed = store
        .claim_action(session_id, action_id, "worker-b", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(reclaimed.lease_token, Some(token));
    assert!(
        store
            .transition_claimed_action(ClaimedActionTransition {
                session_id,
                action_id,
                lease_owner: "worker-a".to_string(),
                lease_token: token,
                expected: Some(SessionActionState::Queued),
                next: SessionActionState::Admitted,
                error: None,
            },)
            .await
            .is_err()
    );
    let transitions = store
        .action_transitions(session_id, action_id)
        .await
        .unwrap();
    assert!(transitions.iter().any(|transition| {
        transition.from == Some(SessionActionState::Committing)
            && transition.to == SessionActionState::Queued
    }));
}

#[tokio::test]
async fn expired_action_recovery_never_moves_work_between_sessions() {
    let (_directory, store) = store().await;
    let resumed_session_id = Uuid::new_v4();
    let other_session_id = Uuid::new_v4();
    let other_action_id = Uuid::new_v4();
    store.create_session(resumed_session_id).await.unwrap();
    store.create_session(other_session_id).await.unwrap();
    store
        .enqueue_action(SessionAction::new(
            other_action_id,
            other_session_id,
            crate::SessionActionKind::Prompt,
            crate::ActionDeliveryPolicy::NextTurnBoundary,
            crate::ActionWakePolicy::Immediate,
            serde_json::json!({
                "message_id": other_action_id,
                "text": "belongs to the other session",
            }),
        ))
        .await
        .unwrap();

    let mut expected = SessionActionState::Queued;
    for next in [
        SessionActionState::Admitted,
        SessionActionState::Delivered,
        SessionActionState::Preparing,
        SessionActionState::Committing,
    ] {
        store
            .transition_action(
                other_session_id,
                other_action_id,
                Some(expected),
                next,
                None,
            )
            .await
            .unwrap();
        expected = next;
    }

    assert!(
        store
            .recover_expired_actions(resumed_session_id, Utc::now(), 10)
            .await
            .unwrap()
            .is_empty(),
        "resuming one session must not claim another session's abandoned prompt"
    );
    assert_eq!(
        store
            .action(other_session_id, other_action_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        SessionActionState::Committing
    );
    assert_eq!(
        store
            .recover_expired_actions(other_session_id, Utc::now(), 10)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn compaction_events_drive_one_replayable_action_lifecycle() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }
    let mut started = SessionEvent::new(
        session_id,
        0,
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::OpenRouter,
            kind: "context_compaction".to_string(),
            payload: serde_json::json!({"status": "started"}),
        },
    );
    started = store.append(started).await.unwrap();
    assert_eq!(
        store
            .action(session_id, started.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        SessionActionState::Running
    );
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::OpenRouter,
                kind: "context_compaction".to_string(),
                payload: serde_json::json!({
                    "status": "completed",
                    "summary": "keep the durable decisions"
                }),
            },
        ))
        .await
        .unwrap();
    let action = store.action(session_id, started.id).await.unwrap().unwrap();
    assert_eq!(action.state, SessionActionState::Completed);
    assert_eq!(
        store
            .action_transitions(session_id, started.id)
            .await
            .unwrap()
            .last()
            .unwrap()
            .to,
        SessionActionState::Completed
    );
}

#[tokio::test]
async fn sqlite_store_appends_projects_and_reads_indexed_suffixes() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        message(message_id, "hello"),
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let state = store.state(session_id).await.unwrap();
    assert_eq!(state.latest_sequence, 3);
    assert_eq!(
        state.configuration.as_ref().unwrap().model.as_deref(),
        Some("gpt-test")
    );
    assert!(
        store
            .contains_message(session_id, message_id)
            .await
            .unwrap()
    );
    let suffix = store.events_after(session_id, 1, 10).await.unwrap();
    assert_eq!(
        suffix
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        [2, 3]
    );
    let recovery = store.recovery(session_id).await.unwrap();
    assert_eq!(recovery.context_events.len(), 1);
    assert_eq!(recovery.queue_events.len(), 1);
    assert!(recovery.subagent_events.is_empty());
    assert_eq!(store.list_sessions(10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn recent_user_messages_are_bounded_ordered_and_ignore_non_recallable_prompts() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        message(Uuid::new_v4(), "first"),
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::Assistant,
            text: "assistant".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::User,
            text: "still queued".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Queue),
        },
        message(Uuid::new_v4(), "second"),
        message(Uuid::new_v4(), "third"),
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::User,
            text: "Team message from /root/worker:\n\ninternal report".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::User,
            text: "failed but recallable".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Failed,
            delivery: Some(PromptDelivery::Queue),
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let prompts = store.recent_user_messages(session_id, 2).await.unwrap();
    let texts = prompts
        .iter()
        .filter_map(|event| match &event.kind {
            SessionEventKind::Message { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(texts, vec!["third", "failed but recallable"]);
    assert!(
        store
            .recent_user_messages(session_id, 0)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn durable_store_health_requires_full_sync_wal_and_foreign_keys() {
    let (_directory, store) = store().await;
    let readiness = store.readiness().await.unwrap();
    assert_eq!(readiness.integrity, "not_checked");
    assert!(!readiness.integrity_checked);
    assert_eq!(
        readiness.journal_size_limit_bytes,
        i64::try_from(SQLITE_JOURNAL_SIZE_LIMIT_BYTES).unwrap()
    );
    assert!(readiness.is_ready());

    let health = store.health().await.unwrap();
    assert_eq!(health.integrity, "ok");
    assert!(health.integrity_checked);
    assert_eq!(health.journal_mode.to_ascii_lowercase(), "wal");
    assert!(health.synchronous >= 2);
    assert!(health.foreign_keys);
    assert!(health.is_ready());
}

#[tokio::test]
async fn context_clear_resets_provider_projection_and_recovery_prefix() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::ProviderSessionLinked {
            provider_session_id: "old-provider-thread".to_string(),
            provider_turn_id: None,
            context_contract_version: None,
        },
        message(Uuid::new_v4(), "old context"),
        SessionEventKind::ContextWindowUpdated {
            context_tokens: 40_000,
            context_window_tokens: 100_000,
        },
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::User,
            text: "queued across context clear".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::ContextCleared,
        message(Uuid::new_v4(), "new context"),
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let state = store.state(session_id).await.unwrap();
    assert!(state.provider_session_id.is_none());
    assert_eq!(state.usage.context_tokens, Some(0));
    let recovery = store.recovery(session_id).await.unwrap();
    assert_eq!(recovery.context_events.len(), 2);
    assert!(matches!(
        recovery.context_events[0].kind,
        SessionEventKind::ContextCleared
    ));
    assert!(matches!(
        &recovery.context_events[1].kind,
        SessionEventKind::Message { text, .. } if text == "new context"
    ));
    assert!(recovery.queue_events.iter().any(|event| matches!(
        &event.kind,
        SessionEventKind::Message {
            text,
            status: MessageStatus::Queued,
            ..
        } if text == "queued across context clear"
    )));
}

#[tokio::test]
async fn compacted_recovery_keeps_the_unresolved_prompt_tail() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let summarized_id = Uuid::new_v4();
    let failed_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::TurnStarted {
            message_id: summarized_id,
            provider: CodingProvider::Codex,
            model: Some("gpt-test".to_string()),
            effort: None,
            fast: false,
        },
        message(summarized_id, "summarized old context"),
        SessionEventKind::TurnCompleted {
            message_id: summarized_id,
            provider_session_id: None,
            final_text: "old result".to_string(),
            error: None,
        },
        SessionEventKind::TurnStarted {
            message_id: failed_id,
            provider: CodingProvider::Codex,
            model: Some("gpt-test".to_string()),
            effort: None,
            fast: false,
        },
        SessionEventKind::TurnCompleted {
            message_id: failed_id,
            provider_session_id: None,
            final_text: String::new(),
            error: Some("provider failed".to_string()),
        },
        SessionEventKind::Message {
            message_id: failed_id,
            actor: EventActor::User,
            text: "preserve exact failed prompt".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Failed,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "context_compaction".to_string(),
            payload: serde_json::json!({
                "status": "completed",
                "summary": "summary of the old context",
            }),
        },
        message(Uuid::new_v4(), "new context after compaction"),
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let recovery = store.recovery(session_id).await.unwrap();
    let context_text = recovery
        .context_events
        .iter()
        .filter_map(|event| match &event.kind {
            SessionEventKind::Message { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(!context_text.contains(&"summarized old context"));
    assert!(context_text.contains(&"preserve exact failed prompt"));
    assert!(context_text.contains(&"new context after compaction"));
    assert!(recovery.context_events.iter().any(|event| matches!(
        &event.kind,
        SessionEventKind::ProviderEvent { kind, payload, .. }
            if kind == "context_compaction"
                && payload.get("summary").and_then(serde_json::Value::as_str)
                    == Some("summary of the old context")
    )));
}

#[tokio::test]
async fn only_acknowledged_terminal_turns_remain_provider_resume_checkpoints() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let completed_id = Uuid::new_v4();
    let failed_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::TurnCompleted {
            message_id: completed_id,
            provider_session_id: Some("acknowledged-thread".to_string()),
            final_text: "done".to_string(),
            error: None,
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }
    assert_eq!(
        store
            .state(session_id)
            .await
            .unwrap()
            .provider_session_id
            .as_deref(),
        Some("acknowledged-thread")
    );

    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::TurnCompleted {
                message_id: failed_id,
                provider_session_id: Some("stale-thread".to_string()),
                final_text: String::new(),
                error: Some("transport closed before a terminal frame".to_string()),
            },
        ))
        .await
        .unwrap();
    assert!(
        store
            .state(session_id)
            .await
            .unwrap()
            .provider_session_id
            .is_none()
    );
}

#[tokio::test]
async fn provider_checkpoint_recovery_keeps_only_the_unacknowledged_tail() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let completed_id = Uuid::new_v4();
    let pending_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        message(completed_id, &"old user context ".repeat(20_000)),
        SessionEventKind::TurnStarted {
            message_id: completed_id,
            provider: CodingProvider::Codex,
            model: Some("gpt-test".to_string()),
            effort: Some("high".to_string()),
            fast: false,
        },
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::Assistant,
            text: "old assistant context ".repeat(20_000),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
        SessionEventKind::ProviderSessionLinked {
            provider_session_id: "durable-thread".to_string(),
            provider_turn_id: Some("durable-turn".to_string()),
            context_contract_version: Some(crate::agent::PROVIDER_CONTEXT_CONTRACT_VERSION),
        },
        SessionEventKind::TurnCompleted {
            message_id: completed_id,
            provider_session_id: Some("durable-thread".to_string()),
            final_text: "done".to_string(),
            error: None,
        },
        SessionEventKind::Message {
            message_id: pending_id,
            actor: EventActor::User,
            text: "recover this exact tail".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::TurnStarted {
            message_id: pending_id,
            provider: CodingProvider::Codex,
            model: Some("gpt-test".to_string()),
            effort: Some("high".to_string()),
            fast: false,
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let recovery = store
        .recovery_from_provider_checkpoint(session_id, "durable-thread")
        .await
        .unwrap()
        .expect("matching durable checkpoint");

    assert_eq!(recovery.queue_events.len(), 1);
    assert!(matches!(
        &recovery.queue_events[0].kind,
        SessionEventKind::Message { message_id, text, .. }
            if *message_id == pending_id && text == "recover this exact tail"
    ));
    assert!(recovery.context_events.iter().any(|event| matches!(
        event.kind,
        SessionEventKind::TurnCompleted { message_id, .. } if message_id == completed_id
    )));
    assert!(serde_json::to_vec(&recovery.context_events).unwrap().len() < 10_000);
}

#[tokio::test]
async fn state_projects_pending_approval_and_cumulative_usage() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::ApprovalRequested {
            approval_id: "approval-1".to_string(),
            title: "Run command".to_string(),
            detail: "Needs permission".to_string(),
            command: Some("cargo test".to_string()),
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }
    assert_eq!(
        store.state(session_id).await.unwrap().pending_approval_id,
        Some("approval-1".to_string())
    );
    for kind in [
        SessionEventKind::ApprovalResolved {
            approval_id: "approval-1".to_string(),
            decision: crate::ApprovalDecision::AllowOnce,
        },
        SessionEventKind::UsageUpdated {
            provider_duration_ms: 10,
            turn_id: None,
            provider_context_reused: None,
            input_tokens: 100,
            output_tokens: 20,
            cached_input_tokens: 40,
            cache_creation_input_tokens: 5,
            total_tokens: 120,
            cost_microusd: Some(100),
            cost_basis: "provider".to_string(),
            cost_usd: Some(0.0001),
            context_tokens: Some(100),
            context_window_tokens: Some(1_000),
        },
        SessionEventKind::UsageUpdated {
            provider_duration_ms: 20,
            turn_id: None,
            provider_context_reused: None,
            input_tokens: 200,
            output_tokens: 30,
            cached_input_tokens: 80,
            cache_creation_input_tokens: 7,
            total_tokens: 230,
            cost_microusd: Some(200),
            cost_basis: "provider".to_string(),
            cost_usd: Some(0.0002),
            context_tokens: Some(200),
            context_window_tokens: Some(1_000),
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let state = store.state(session_id).await.unwrap();
    assert_eq!(state.pending_approval_id, None);
    assert_eq!(state.usage.calls, 2);
    assert_eq!(state.usage.provider_duration_ms, 30);
    assert_eq!(state.usage.input_tokens, 300);
    assert_eq!(state.usage.output_tokens, 50);
    assert_eq!(state.usage.cached_input_tokens, 120);
    assert_eq!(state.usage.cache_creation_input_tokens, 12);
    assert_eq!(state.usage.total_tokens, 350);
    assert_eq!(state.usage.cost_microusd, Some(300));
    assert!((state.usage.cost_usd.unwrap() - 0.0003).abs() < f64::EPSILON);
    assert_eq!(state.usage.context_tokens, Some(200));
}

#[tokio::test]
async fn fork_records_lineage_without_copying_events() {
    let (directory, store) = store().await;
    let parent_id = Uuid::new_v4();
    let fork_id = Uuid::new_v4();
    let retained_message_id = Uuid::new_v4();
    let discarded_message_id = Uuid::new_v4();
    store.create_session(parent_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::ProviderSessionLinked {
            provider_session_id: "provider-thread".to_string(),
            provider_turn_id: None,
            context_contract_version: None,
        },
        message(retained_message_id, "keep"),
        message(discarded_message_id, "discard"),
    ] {
        store
            .append(SessionEvent::new(parent_id, 0, kind))
            .await
            .unwrap();
    }

    let fork = store.fork_before(parent_id, fork_id, 5).await.unwrap();
    assert_eq!(fork.inherited_event_count, 3);
    let copied_rows: i64 =
        sqlx::query_scalar("select count(*) from session_events where session_id = ?")
            .bind(fork_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(copied_rows, 0);

    let events = store.read(fork_id).await.unwrap();
    assert_eq!(events.len(), 3);
    assert!(events.iter().all(|event| event.session_id == fork_id));
    assert_eq!(
        events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert!(
        events
            .iter()
            .all(|event| !matches!(event.kind, SessionEventKind::ProviderSessionLinked { .. }))
    );
    assert!(
        store
            .contains_message(fork_id, retained_message_id)
            .await
            .unwrap()
    );
    assert!(
        !store
            .contains_message(fork_id, discarded_message_id)
            .await
            .unwrap()
    );
    let state = store.state(fork_id).await.unwrap();
    assert_eq!(state.latest_sequence, 3);
    assert!(state.provider_session_id.is_none());
    let recovery = store.recovery(fork_id).await.unwrap();
    assert_eq!(recovery.context_events.len(), 1);
    assert_eq!(recovery.queue_events.len(), 1);
}

#[tokio::test]
async fn fork_projection_checkpoints_bound_state_amplification() {
    const LARGE_RESPONSE_BYTES: usize = 128 * 1024;
    const EVENT_COUNT: u64 = FORK_PROJECTION_CHECKPOINT_INTERVAL + 23;

    let (directory, store) = store().await;
    let parent_id = Uuid::new_v4();
    let fork_id = Uuid::new_v4();
    let goal = SessionGoal::new("retained sparse projection goal".to_string(), None);
    let response = "r".repeat(LARGE_RESPONSE_BYTES);
    store.create_session(parent_id).await.unwrap();
    let mut transaction = store.begin_write().await.unwrap();
    for sequence in 1..=EVENT_COUNT {
        let kind = match sequence {
            1 => SessionEventKind::SessionStarted,
            2 => configured(directory.path()),
            3 => message(Uuid::new_v4(), "retained prompt"),
            4 => SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: response.clone(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
            value if value == FORK_PROJECTION_CHECKPOINT_INTERVAL + 7 => {
                SessionEventKind::GoalUpdated { goal: goal.clone() }
            }
            _ => SessionEventKind::Error {
                message: format!("sparse projection fixture {sequence}"),
            },
        };
        store
            .append_durable_in_transaction(&mut transaction, SessionEvent::new(parent_id, 0, kind))
            .await
            .unwrap();
    }
    transaction.commit().await.unwrap();

    let (checkpoint_count, checkpoint_bytes): (i64, i64) = sqlx::query_as(
        "select count(*), coalesce(sum(length(projection_json)), 0) \
         from session_events where session_id = ? and projection_json <> ''",
    )
    .bind(parent_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(checkpoint_count, 2);
    assert!(
        checkpoint_bytes < i64::try_from(LARGE_RESPONSE_BYTES * 3).unwrap(),
        "sparse fork projections retained {checkpoint_bytes} bytes"
    );
    let dense_projection_lower_bound =
        i64::try_from((EVENT_COUNT - 4) * u64::try_from(LARGE_RESPONSE_BYTES).unwrap()).unwrap();
    assert!(dense_projection_lower_bound > checkpoint_bytes * 100);
    eprintln!(
        "fork projection fixture: dense_lower_bound={dense_projection_lower_bound} bytes; sparse={checkpoint_bytes} bytes"
    );

    let fork = store
        .fork_before(parent_id, fork_id, EVENT_COUNT + 1)
        .await
        .unwrap();
    assert_eq!(fork.inherited_event_count, EVENT_COUNT);
    let state = store.state(fork_id).await.unwrap();
    assert_eq!(state.latest_sequence, EVENT_COUNT);
    assert_eq!(state.latest_response.as_deref(), Some(response.as_str()));
    assert_eq!(state.goal, Some(goal));
}

#[tokio::test]
async fn fork_starts_a_fresh_provider_context_without_losing_capacity() {
    let (directory, store) = store().await;
    let parent_id = Uuid::new_v4();
    let fork_id = Uuid::new_v4();
    store.create_session(parent_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        message(Uuid::new_v4(), "retained context"),
        SessionEventKind::UsageUpdated {
            provider_duration_ms: 1,
            turn_id: None,
            provider_context_reused: None,
            input_tokens: 1,
            output_tokens: 1,
            cached_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: 2,
            cost_microusd: None,
            cost_basis: "unknown".to_string(),
            cost_usd: None,
            context_tokens: Some(95_000),
            context_window_tokens: Some(100_000),
        },
    ] {
        store
            .append(SessionEvent::new(parent_id, 0, kind))
            .await
            .unwrap();
    }

    let parent_generation = store.state(parent_id).await.unwrap().context_generation;
    store.fork_before(parent_id, fork_id, 5).await.unwrap();

    let child = store.state(fork_id).await.unwrap();
    assert!(child.provider_session_id.is_none());
    assert_eq!(child.usage.context_tokens, Some(0));
    assert_eq!(child.usage.context_window_tokens, Some(100_000));
    assert_eq!(child.context_generation, parent_generation + 1);
    assert_eq!(
        store
            .read(fork_id)
            .await
            .unwrap()
            .iter()
            .filter_map(|event| match &event.kind {
                SessionEventKind::Message { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec!["retained context"]
    );
}

#[tokio::test]
async fn latest_completed_compaction_is_projected_through_a_fork() {
    let (directory, store) = store().await;
    let parent_id = Uuid::new_v4();
    let fork_id = Uuid::new_v4();
    store.create_session(parent_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "context_compaction".to_string(),
            payload: serde_json::json!({"status": "started"}),
        },
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "context_compaction".to_string(),
            payload: serde_json::json!({
                "status": "completed",
                "summary": "retained checkpoint"
            }),
        },
    ] {
        store
            .append(SessionEvent::new(parent_id, 0, kind))
            .await
            .unwrap();
    }
    store.fork_before(parent_id, fork_id, 5).await.unwrap();

    let checkpoint = store
        .latest_completed_context_compaction(fork_id)
        .await
        .unwrap()
        .expect("inherited checkpoint");
    assert_eq!(checkpoint.session_id, fork_id);
    assert_eq!(checkpoint.sequence, 4);
    assert!(checkpoint.kind.is_completed_context_compaction());
    assert!(matches!(
        checkpoint.kind,
        SessionEventKind::ProviderEvent { payload, .. }
            if payload.get("summary").and_then(serde_json::Value::as_str)
                == Some("retained checkpoint")
    ));
}

#[test]
fn provider_native_compaction_is_not_a_durable_replay_boundary() {
    let kind = SessionEventKind::ProviderEvent {
        provider: CodingProvider::Codex,
        kind: "context_compaction".to_string(),
        payload: serde_json::json!({
            "status": "completed",
            "provider_context_preserved": true,
        }),
    };

    assert!(!kind.is_completed_context_compaction());
    assert_eq!(kind.persistence(), EventPersistence::Durable);
    assert!(!kind.is_context_relevant());
}

#[test]
fn provider_native_recovery_checkpoint_is_context_without_rotating_generation() {
    let kind = SessionEventKind::ProviderEvent {
        provider: CodingProvider::Codex,
        kind: "context_compaction".to_string(),
        payload: serde_json::json!({
            "status": "completed",
            "summary": "provider recovery summary",
            "provider_context_preserved": true,
            "provider_recovery_checkpoint": true,
        }),
    };

    assert!(!kind.is_completed_context_compaction());
    assert!(kind.is_completed_provider_recovery_checkpoint());
    assert_eq!(kind.persistence(), EventPersistence::Durable);
    assert!(kind.is_context_relevant());
}

#[tokio::test]
async fn inherited_event_pages_match_the_full_projection_across_lineage_boundaries() {
    let (directory, store) = store().await;
    let parent_id = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    let grandchild_id = Uuid::new_v4();
    store.create_session(parent_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::ProviderSessionLinked {
            provider_session_id: "must-not-fork".to_string(),
            provider_turn_id: None,
            context_contract_version: None,
        },
        message(Uuid::new_v4(), "parent-a"),
        message(Uuid::new_v4(), "parent-b"),
    ] {
        store
            .append(SessionEvent::new(parent_id, 0, kind))
            .await
            .unwrap();
    }
    store.fork_before(parent_id, child_id, 6).await.unwrap();
    for text in ["child-a", "child-b", "child-c", "child-d"] {
        store
            .append(SessionEvent::new(
                child_id,
                0,
                message(Uuid::new_v4(), text),
            ))
            .await
            .unwrap();
    }
    store.fork_before(child_id, grandchild_id, 8).await.unwrap();
    for id in [parent_id, child_id, grandchild_id] {
        assert_eq!(store.prompt_cache_session_id(id).await.unwrap(), parent_id);
    }
    assert!(
        store
            .state(grandchild_id)
            .await
            .unwrap()
            .provider_session_id
            .is_none()
    );
    for text in ["grandchild-a", "grandchild-b"] {
        store
            .append(SessionEvent::new(
                grandchild_id,
                0,
                message(Uuid::new_v4(), text),
            ))
            .await
            .unwrap();
    }

    let full = store.read(grandchild_id).await.unwrap();
    for sequence in 0..=u64::try_from(full.len() + 1).unwrap() {
        for limit in [1, 2, 4, 100] {
            let expected = full
                .iter()
                .skip(usize::try_from(sequence).unwrap())
                .take(limit)
                .map(|event| serde_json::to_value(event).unwrap())
                .collect::<Vec<_>>();
            let actual = store
                .events_after(grandchild_id, sequence, limit)
                .await
                .unwrap()
                .iter()
                .map(|event| serde_json::to_value(event).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(actual, expected, "sequence={sequence}, limit={limit}");
        }
    }
}

/// A rewind cuts immediately before the admission of the prompt it targets.
/// That prompt's earlier queue entry sits below the cut, so inheriting it
/// would hand the fork a pending prompt and re-run exactly what the user
/// just discarded.
#[tokio::test]
async fn a_rewind_does_not_inherit_the_queue_entry_of_the_discarded_prompt() {
    let (directory, store) = store().await;
    let parent_id = Uuid::new_v4();
    let fork_id = Uuid::new_v4();
    let discarded_message_id = Uuid::new_v4();
    store.create_session(parent_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::Message {
            message_id: discarded_message_id,
            actor: EventActor::User,
            text: "discard".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Queue),
        },
        SessionEventKind::Message {
            message_id: discarded_message_id,
            actor: EventActor::User,
            text: "discard".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: Some(PromptDelivery::Queue),
        },
    ] {
        store
            .append(SessionEvent::new(parent_id, 0, kind))
            .await
            .unwrap();
    }

    // The UI rewinds to the admission at sequence 4; the queue entry it was
    // admitted from is at sequence 3, below the cut.
    store.fork_before(parent_id, fork_id, 4).await.unwrap();
    assert!(
        !store
            .contains_message(fork_id, discarded_message_id)
            .await
            .unwrap()
    );
    let recovery = store.recovery(fork_id).await.unwrap();
    assert!(
        recovery.queue_events.is_empty(),
        "the discarded prompt must not come back as pending work"
    );
    assert!(
        store
            .read(fork_id)
            .await
            .unwrap()
            .iter()
            .all(|event| !matches!(
                event.kind,
                SessionEventKind::Message {
                    status: MessageStatus::Queued,
                    ..
                }
            ))
    );
}

#[tokio::test]
async fn parallel_generation_status_survives_reconnect_and_clears_per_call() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::StatusChanged {
            status: SessionStatus::Running,
            detail: None,
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }
    for id in ["a", "b"] {
        for (kind, waiting) in [
            ("action/preparing", false),
            ("action/generation_status", true),
        ] {
            store.append(SessionEvent::new(session_id, 0, SessionEventKind::ProviderEvent {
                provider: CodingProvider::Claude, kind: kind.into(),
                payload: serde_json::json!({"tool_call_id": id, "label": "read file", "waiting": waiting}),
            })).await.unwrap();
        }
    }
    let live = store.live_events_after(session_id, 0).await.unwrap();
    assert_eq!(live.len(), 4);
    for (events, id) in live.as_chunks::<2>().0.iter().zip(["a", "b"]) {
        assert!(
            matches!(&events[0].event.kind, SessionEventKind::ProviderEvent { kind, payload, .. }
            if kind == "action/preparing" && payload["tool_call_id"] == id)
        );
        assert!(
            matches!(&events[1].event.kind, SessionEventKind::ProviderEvent { kind, payload, .. }
            if kind == "action/generation_status" && payload["tool_call_id"] == id && payload["waiting"] == true)
        );
    }
    assert_eq!(
        store.read(session_id).await.unwrap().len(),
        3,
        "live status must not grow the durable conversation"
    );
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ToolStarted {
                tool_call_id: "a".into(),
                name: "read_file".into(),
                input: serde_json::json!({}),
                input_ref: None,
            },
        ))
        .await
        .unwrap();
    let live = store.live_events_after(session_id, 0).await.unwrap();
    assert_eq!(live.len(), 2);
    assert!(live.iter().all(|event| matches!(&event.event.kind, SessionEventKind::ProviderEvent { payload, .. } if payload["tool_call_id"] == "b")));
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                detail: None,
            },
        ))
        .await
        .unwrap();
    assert!(
        store
            .live_events_after(session_id, 0)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn live_state_coalesces_without_consuming_durable_sequences() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::StatusChanged {
            status: SessionStatus::Running,
            detail: None,
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }
    for text in ["a", "a much longer snapshot"] {
        let event = store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::Message {
                    message_id,
                    actor: EventActor::Assistant,
                    text: text.to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::InProgress,
                    delivery: None,
                },
            ))
            .await
            .unwrap();
        assert_eq!(event.sequence, 0);
    }
    let first_reasoning = store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningDelta {
                text: "thinking ".to_string(),
            },
        ))
        .await
        .unwrap();
    let reasoning_id = first_reasoning.id;
    let reasoning_started_at = first_reasoning.created_at;
    assert!(matches!(
        first_reasoning.kind,
        SessionEventKind::ReasoningDelta { ref text } if text == "thinking "
    ));
    let second_reasoning = store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningDelta {
                text: "carefully".to_string(),
            },
        ))
        .await
        .unwrap();
    assert_eq!(second_reasoning.id, reasoning_id);
    assert_eq!(second_reasoning.created_at, reasoning_started_at);
    assert!(matches!(
        second_reasoning.kind,
        SessionEventKind::ReasoningDelta { ref text } if text == "thinking carefully"
    ));
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningDelta {
                text: "thinking carefully".to_string(),
            },
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "telemetry".to_string(),
                payload: serde_json::json!({"large": "discarded"}),
            },
        ))
        .await
        .unwrap();

    assert_eq!(store.read(session_id).await.unwrap().len(), 3);
    let live = store.live_events_after(session_id, 0).await.unwrap();
    assert_eq!(live.len(), 2);
    assert!(live.iter().any(|live| matches!(
        &live.event.kind,
        SessionEventKind::Message { text, .. } if text == "a much longer snapshot"
    )));
    assert!(live.iter().any(|live| matches!(
        &live.event.kind,
        SessionEventKind::ReasoningDelta { text } if text == "thinking carefully"
    )));

    let completed = store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::Assistant,
                text: "done".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ))
        .await
        .unwrap();
    assert_eq!(completed.sequence, 4);
    assert!(
        store
            .live_events_after(session_id, 0)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn reasoning_boundaries_clear_the_snapshot_before_the_next_thought() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        SessionEventKind::StatusChanged {
            status: SessionStatus::Running,
            detail: None,
        },
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningDelta {
                text: "previous thought".to_string(),
            },
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningCompleted,
        ))
        .await
        .unwrap();
    let next = store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningDelta {
                text: "next thought".to_string(),
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        next.kind,
        SessionEventKind::ReasoningDelta { ref text } if text == "next thought"
    ));

    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ToolStarted {
                tool_call_id: "tool-1".to_string(),
                name: "read_file".to_string(),
                input: serde_json::json!({"path": "src/lib.rs"}),
                input_ref: None,
            },
        ))
        .await
        .unwrap();
    let after_tool = store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningDelta {
                text: "thought after tool".to_string(),
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        after_tool.kind,
        SessionEventKind::ReasoningDelta { ref text } if text == "thought after tool"
    ));
}

#[tokio::test]
async fn terminal_boundaries_clear_all_turn_live_state_but_keep_context_window() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }

    let live_message = |message_id| SessionEventKind::Message {
        message_id,
        actor: EventActor::Assistant,
        text: "partial".to_string(),
        attachments: Vec::new(),
        status: MessageStatus::InProgress,
        delivery: None,
    };
    store
        .append(SessionEvent::new(
            session_id,
            0,
            live_message(Uuid::new_v4()),
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningDelta {
                text: "thinking".to_string(),
            },
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ContextWindowUpdated {
                context_tokens: 80,
                context_window_tokens: 100,
            },
        ))
        .await
        .unwrap();

    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::TurnCompleted {
                message_id: Uuid::new_v4(),
                provider_session_id: None,
                final_text: String::new(),
                error: Some("turn interrupted".to_string()),
            },
        ))
        .await
        .unwrap();
    let live = store.live_events_after(session_id, 0).await.unwrap();
    assert_eq!(live.len(), 1);
    assert!(matches!(
        live[0].event.kind,
        SessionEventKind::ContextWindowUpdated {
            context_tokens: 80,
            context_window_tokens: 100,
        }
    ));

    store
        .append(SessionEvent::new(
            session_id,
            0,
            live_message(Uuid::new_v4()),
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                detail: None,
            },
        ))
        .await
        .unwrap();
    let live = store.live_events_after(session_id, 0).await.unwrap();
    assert_eq!(live.len(), 1);
    assert!(matches!(
        live[0].event.kind,
        SessionEventKind::ContextWindowUpdated { .. }
    ));

    // A delayed coalesced event must not recreate turn state after the
    // session has become idle.
    store
        .append(SessionEvent::new(
            session_id,
            0,
            live_message(Uuid::new_v4()),
        ))
        .await
        .unwrap();
    let live = store.live_events_after(session_id, 0).await.unwrap();
    assert_eq!(live.len(), 1);
    assert!(matches!(
        live[0].event.kind,
        SessionEventKind::ContextWindowUpdated { .. }
    ));
}

#[tokio::test]
async fn reopening_repairs_turn_live_state_left_on_a_terminal_session() {
    let (directory, store) = store().await;
    let path = directory.path().join("sessions.sqlite3");
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::SessionStarted,
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                detail: None,
            },
        ))
        .await
        .unwrap();

    let message_id = Uuid::new_v4();
    let event = SessionEvent::new(
        session_id,
        0,
        SessionEventKind::Message {
            message_id,
            actor: EventActor::Assistant,
            text: "stale response".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        },
    );
    sqlx::query(
        "insert into session_live_state \
             (session_id, live_key, revision, event_json, updated_at) values (?, ?, ?, ?, ?)",
    )
    .bind(session_id.to_string())
    .bind(format!("message:{message_id}"))
    .bind(99_i64)
    .bind(serde_json::to_string(&event).unwrap())
    .bind(event.created_at.to_rfc3339())
    .execute(store.pool())
    .await
    .unwrap();
    drop(store);

    let reopened = SqliteSessionStore::open(&path).await.unwrap();
    assert!(
        reopened
            .live_events_after(session_id, 0)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn interactive_open_defers_terminal_live_state_repair_until_requested() {
    let (directory, store) = store().await;
    let path = directory.path().join("sessions.sqlite3");
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                detail: None,
            },
        ))
        .await
        .unwrap();

    let message_id = Uuid::new_v4();
    let event = SessionEvent::new(
        session_id,
        0,
        SessionEventKind::Message {
            message_id,
            actor: EventActor::Assistant,
            text: "stale response".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        },
    );
    sqlx::query(
        "insert into session_live_state \
         (session_id, live_key, revision, event_json, updated_at) values (?, ?, ?, ?, ?)",
    )
    .bind(session_id.to_string())
    .bind(format!("message:{message_id}"))
    .bind(99_i64)
    .bind(serde_json::to_string(&event).unwrap())
    .bind(event.created_at.to_rfc3339())
    .execute(store.pool())
    .await
    .unwrap();
    drop(store);

    let reopened = SqliteSessionStore::open_interactive(&path).await.unwrap();
    assert_eq!(
        reopened
            .live_events_after(session_id, 0)
            .await
            .unwrap()
            .len(),
        1
    );
    reopened.finish_interactive_open(session_id).await.unwrap();
    assert!(
        reopened
            .live_events_after(session_id, 0)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn interactive_open_adds_account_bindings_without_replacing_existing_sessions() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    // Reproduce the current database schema before the additive admission tables.
    sqlx::query("drop table session_model_access")
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query("drop table session_harness_routes")
        .execute(&store.pool)
        .await
        .unwrap();
    store.pool.close().await;
    let reopened = SqliteSessionStore::open_interactive(directory.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    assert!(reopened.contains_session(session_id).await.unwrap());
    assert!(
        reopened
            .uses_native_codex_harness(session_id)
            .await
            .unwrap()
    );
    reopened
        .record_model_access(session_id, CodingProvider::Codex, "account-a")
        .await
        .unwrap();
}

#[tokio::test]
async fn current_schema_interactive_open_does_not_wait_for_an_active_writer() {
    let (directory, store) = store().await;
    let path = directory.path().join("sessions.sqlite3");
    let mut writer = store.pool().acquire().await.unwrap();
    sqlx::query("begin immediate")
        .execute(&mut *writer)
        .await
        .unwrap();

    let reopened = tokio::time::timeout(
        Duration::from_millis(250),
        SqliteSessionStore::open_interactive(&path),
    )
    .await
    .expect("current-schema interactive open waited for the active writer")
    .unwrap();
    drop(reopened);

    sqlx::query("rollback").execute(&mut *writer).await.unwrap();
}

#[tokio::test]
async fn large_tool_payloads_are_loaded_only_by_reference() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let input = serde_json::json!({"text": "x".repeat(INLINE_SESSION_PAYLOAD_BYTES)});
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }
    let appended = store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ToolStarted {
                tool_call_id: "large-tool".to_string(),
                name: "large".to_string(),
                input: input.clone(),
                input_ref: None,
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        appended.kind,
        SessionEventKind::ToolStarted {
            input_ref: Some(_),
            ..
        }
    ));

    let persisted = store.events_after(session_id, 2, 1).await.unwrap();
    let SessionEventKind::ToolStarted {
        input: preview,
        input_ref: Some(payload),
        ..
    } = &persisted[0].kind
    else {
        panic!("large tool input should be stored by reference");
    };
    assert_ne!(preview, &input);
    assert_eq!(
        store.load_payload(payload).await.unwrap(),
        serde_json::to_vec(&input).unwrap()
    );
}

/// The exact text a subscription turn handed its provider is journaled as a
/// deferred payload. Without the embedded-reference extraction, history
/// expansion would silently return the preview instead of the real prompt.
#[tokio::test]
async fn large_provider_prompts_are_stored_by_reference_and_expandable() {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    let needle = "uniqueprompt8675309";
    let prompt = format!(
        "{} {needle}",
        "p".repeat(INLINE_SESSION_PAYLOAD_BYTES + 1024)
    );
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
    ] {
        store
            .append(SessionEvent::new(session_id, 0, kind))
            .await
            .unwrap();
    }
    let appended = store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Claude,
                kind: crate::PROVIDER_PROMPT_EVENT_KIND.to_string(),
                payload: serde_json::json!({
                    "prompt": prompt,
                    "search_marker": needle,
                    "provider_context_reused": false,
                }),
            },
        ))
        .await
        .unwrap();

    let SessionEventKind::ProviderEvent { payload, .. } = &appended.kind else {
        panic!("provider prompt event should round-trip");
    };
    let reference: SessionPayloadRef = serde_json::from_value(
        payload
            .get(crate::PROVIDER_PROMPT_REF_FIELD)
            .expect("deferred prompt carries a reference")
            .clone(),
    )
    .unwrap();
    assert_eq!(reference.kind, SessionPayloadKind::ProviderPrompt);
    assert_ne!(
        payload
            .get(crate::PROVIDER_PROMPT_FIELD)
            .and_then(serde_json::Value::as_str),
        Some(prompt.as_str())
    );
    assert_eq!(
        store.load_payload(&reference).await.unwrap(),
        prompt.as_bytes()
    );

    let hit = store
        .query_history(
            session_id,
            SessionHistoryQuery {
                text: Some(needle.to_string()),
                expand_payloads: true,
                max_payload_bytes: Some(INLINE_SESSION_PAYLOAD_BYTES * 2),
                ..SessionHistoryQuery::default()
            },
        )
        .await
        .unwrap();
    assert!(
        hit.hits.iter().any(|hit| hit
            .payloads
            .iter()
            .any(|payload| payload.text.contains(needle))),
        "expanded history must surface the deferred provider prompt"
    );
}

#[tokio::test]
async fn history_query_resolves_fts_regex_exact_and_full_payload_hits_to_canonical_events() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let message_event = store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: "compare the contractual remedy matrix".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ))
        .await
        .unwrap();
    let payload_needle = "uniquepayload8675309";
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ToolCompleted {
                tool_call_id: "large-history-result".to_string(),
                output: format!(
                    "{} {}",
                    "x".repeat(INLINE_SESSION_PAYLOAD_BYTES + 1024),
                    payload_needle
                ),
                output_ref: None,
                is_error: false,
                input: None,
                input_ref: None,
            },
        ))
        .await
        .unwrap();
    let other_session = Uuid::new_v4();
    store.create_session(other_session).await.unwrap();
    store
        .append(SessionEvent::new(
            other_session,
            0,
            message(
                Uuid::new_v4(),
                "contractual remedy from another tenant scope",
            ),
        ))
        .await
        .unwrap();

    let lexical = store
        .query_history(
            session_id,
            SessionHistoryQuery {
                text: Some("contractual remedy".to_string()),
                actors: vec![EventActor::User],
                ..SessionHistoryQuery::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(lexical.backend, "sqlite_fts5");
    assert_eq!(lexical.hits.len(), 1);
    assert_eq!(lexical.hits[0].event.id, message_event.id);

    let payload_hit = store
        .query_history(
            session_id,
            SessionHistoryQuery {
                text: Some(payload_needle.to_string()),
                expand_payloads: true,
                max_payload_bytes: Some(INLINE_SESSION_PAYLOAD_BYTES * 2),
                ..SessionHistoryQuery::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(payload_hit.hits.len(), 1);
    assert!(matches!(
        payload_hit.hits[0].event.kind,
        SessionEventKind::ToolCompleted {
            output_ref: Some(_),
            ..
        }
    ));
    assert!(
        payload_hit.hits[0].payloads[0]
            .text
            .ends_with(payload_needle)
    );
    assert!(!payload_hit.hits[0].payloads[0].truncated);

    let index_documents = store
        .history_index_documents_after(session_id, message_event.sequence, 10)
        .await
        .unwrap();
    assert_eq!(index_documents.len(), 1);
    assert!(index_documents[0].content.contains(payload_needle));
    assert_eq!(index_documents[0].event_id, payload_hit.hits[0].event.id);
    assert!(
        index_documents[0]
            .document_id
            .starts_with("borg-session-event:v1:")
    );

    let regex = store
        .query_history(
            session_id,
            SessionHistoryQuery {
                text: Some("PAYLOAD[0-9]{7}".to_string()),
                mode: SessionHistorySearchMode::Regex,
                case_sensitive: false,
                ..SessionHistoryQuery::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(regex.backend, "sqlite_regex");
    assert_eq!(regex.hits.len(), 1);

    let exact = store
        .query_history(
            session_id,
            SessionHistoryQuery {
                event_id: Some(message_event.id),
                ..SessionHistoryQuery::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(exact.backend, "sqlite_exact");
    assert_eq!(exact.hits.len(), 1);
    assert_eq!(exact.hits[0].event.sequence, message_event.sequence);
}

#[tokio::test]
async fn history_query_preserves_projected_ids_and_sequences_across_forks() {
    let (directory, store) = store().await;
    let parent_id = Uuid::new_v4();
    let fork_id = Uuid::new_v4();
    store.create_session(parent_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(directory.path()),
        message(Uuid::new_v4(), "inherit this distinctive finding"),
        message(Uuid::new_v4(), "discard this later finding"),
    ] {
        store
            .append(SessionEvent::new(parent_id, 0, kind))
            .await
            .unwrap();
    }
    store.fork_before(parent_id, fork_id, 4).await.unwrap();
    store
        .append(SessionEvent::new(
            fork_id,
            0,
            message(Uuid::new_v4(), "local fork conclusion"),
        ))
        .await
        .unwrap();

    let inherited = store
        .query_history(
            fork_id,
            SessionHistoryQuery {
                text: Some("distinctive finding".to_string()),
                ..SessionHistoryQuery::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(inherited.backend, "lineage_scan");
    assert_eq!(inherited.hits.len(), 1);
    assert_eq!(inherited.hits[0].event.session_id, fork_id);
    assert_eq!(inherited.hits[0].event.sequence, 3);
    assert_ne!(
        inherited.hits[0].event.id,
        store.read(parent_id).await.unwrap()[2].id
    );

    let all_messages = store
        .query_history(
            fork_id,
            SessionHistoryQuery {
                event_kinds: vec!["message".to_string()],
                ..SessionHistoryQuery::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(all_messages.hits.len(), 2);
    assert_eq!(all_messages.hits[1].event.sequence, 4);

    let index_documents = store
        .history_index_documents_after(fork_id, 0, 10)
        .await
        .unwrap();
    assert_eq!(index_documents.len(), 4);
    assert_eq!(index_documents[2].event_id, inherited.hits[0].event.id);
    assert!(index_documents[2].content.contains("distinctive finding"));
    assert!(
        index_documents
            .iter()
            .all(|document| document.session_id == fork_id)
    );
}

#[tokio::test]
async fn opening_rebuilds_missing_history_projection_from_the_lossless_journal() {
    let (directory, store) = store().await;
    let path = directory.path().join("sessions.sqlite3");
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            message(Uuid::new_v4(), "rebuildable projection evidence"),
        ))
        .await
        .unwrap();
    sqlx::query("delete from session_event_search where session_id=?")
        .bind(session_id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
    drop(store);

    let reopened = SqliteSessionStore::open(path).await.unwrap();
    let result = reopened
        .query_history(
            session_id,
            SessionHistoryQuery {
                text: Some("projection evidence".to_string()),
                ..SessionHistoryQuery::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.backend, "sqlite_fts5");
}

#[test]
fn persistence_and_fork_rules_are_typed_rust_contracts() {
    assert_eq!(
        SessionEventKind::ReasoningDelta {
            text: "working".to_string()
        }
        .persistence(),
        EventPersistence::Coalesced
    );
    assert_eq!(
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "noise".to_string(),
            payload: serde_json::Value::Null,
        }
        .persistence(),
        EventPersistence::Ephemeral
    );
    let user_in_progress = SessionEventKind::Message {
        message_id: Uuid::new_v4(),
        actor: crate::EventActor::User,
        text: "must survive a host crash".to_string(),
        attachments: Vec::new(),
        status: MessageStatus::InProgress,
        delivery: Some(crate::PromptDelivery::Queue),
    };
    assert_eq!(user_in_progress.persistence(), EventPersistence::Durable);
    // A parent journals a mirrored child event only when the child would AND
    // the parent replays something from it. See is_parent_replayable.
    let child_id = Uuid::new_v4();
    let snapshot = || crate::SubagentSnapshot {
        session_id: child_id,
        parent_session_id: Uuid::new_v4(),
        task_name: "worker".to_string(),
        status: crate::SubagentStatus::Running,
        provider: CodingProvider::Codex,
        model: None,
        effort: None,
        cwd: std::path::PathBuf::from("/tmp"),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        detail: None,
        final_text: None,
        usage: crate::SubagentUsage::default(),
    };
    let mirrored = |kind: SessionEventKind| SessionEventKind::SubagentActivity {
        activity: crate::SubagentActivityKind::Updated,
        agent: snapshot(),
        event: Some(Box::new(SessionEvent::new(child_id, 0, kind))),
    };
    assert_eq!(
        mirrored(SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "heartbeat".to_string(),
            payload: serde_json::Value::Null,
        })
        .persistence(),
        EventPersistence::Ephemeral
    );
    assert_eq!(
        mirrored(SessionEventKind::ReasoningDelta {
            text: "thinking".to_string()
        })
        .persistence(),
        EventPersistence::Ephemeral
    );
    assert_eq!(
        mirrored(SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: crate::EventActor::Assistant,
            text: "streaming".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        })
        .persistence(),
        EventPersistence::Ephemeral
    );
    assert_eq!(
        mirrored(SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: crate::EventActor::Assistant,
            text: "done".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        })
        .persistence(),
        EventPersistence::Durable
    );
    assert_eq!(
        SessionEventKind::SubagentActivity {
            activity: crate::SubagentActivityKind::Completed,
            agent: snapshot(),
            event: None,
        }
        .persistence(),
        EventPersistence::Durable
    );
    assert!(!user_in_progress.is_fork_inheritable());
    assert_eq!(
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: crate::EventActor::Assistant,
            text: "streaming".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        }
        .persistence(),
        EventPersistence::Coalesced
    );
    for kind in [
        "item/started:contextCompaction",
        "item/completed:contextCompaction",
        "item/started:context_compaction",
        "item/completed:context_compaction",
    ] {
        let notification = SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: kind.to_string(),
            payload: serde_json::Value::Null,
        };
        assert_eq!(notification.persistence(), EventPersistence::Durable);
        assert!(!notification.is_completed_context_compaction());
        assert!(!notification.is_completed_provider_recovery_checkpoint());
        assert!(!notification.is_context_relevant());
    }
    let compaction_started = SessionEventKind::ProviderEvent {
        provider: CodingProvider::Codex,
        kind: "context_compaction".to_string(),
        payload: serde_json::json!({"status": "started"}),
    };
    assert_eq!(compaction_started.persistence(), EventPersistence::Durable);
    assert!(!compaction_started.is_context_relevant());
    let compaction_completed = SessionEventKind::ProviderEvent {
        provider: CodingProvider::Codex,
        kind: "context_compaction".to_string(),
        payload: serde_json::json!({"status": "completed", "summary": "done"}),
    };
    assert_eq!(
        compaction_completed.persistence(),
        EventPersistence::Durable
    );
    assert!(compaction_completed.is_context_relevant());
    assert!(
        !SessionEventKind::ProviderSessionLinked {
            provider_session_id: "provider".to_string(),
            provider_turn_id: None,
            context_contract_version: None,
        }
        .is_fork_inheritable()
    );
}

#[test]
fn context_generation_changes_only_at_explicit_prefix_boundaries() {
    let session_id = Uuid::new_v4();
    let mut state = SessionState::default();
    state
        .apply(&SessionEvent::new(
            session_id,
            1,
            SessionEventKind::SessionStarted,
        ))
        .unwrap();
    state
        .apply(&SessionEvent::new(
            session_id,
            2,
            configured(Path::new("/tmp")),
        ))
        .unwrap();
    assert_eq!(state.context_generation, 0);
    state
        .apply(&SessionEvent::new(
            session_id,
            3,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "context_compaction".to_string(),
                payload: serde_json::json!({"status": "completed"}),
            },
        ))
        .unwrap();
    assert_eq!(state.context_generation, 1);
    state
        .apply(&SessionEvent::new(
            session_id,
            4,
            SessionEventKind::ContextCleared,
        ))
        .unwrap();
    assert_eq!(state.context_generation, 2);
    state
        .apply(&SessionEvent::new(
            session_id,
            5,
            SessionEventKind::SessionConfigured {
                cwd: PathBuf::from("/tmp"),
                provider: CodingProvider::Claude,
                model: Some("claude-test".to_string()),
                effort: Some("high".to_string()),
                fast: false,
                response_language: ResponseLanguage::Auto,
                permission_mode: PermissionMode::FullAccess,
            },
        ))
        .unwrap();
    assert_eq!(state.context_generation, 3);
}

#[test]
fn same_provider_codex_model_change_preserves_resume_checkpoint() {
    let session_id = Uuid::new_v4();
    let mut state = SessionState::default();
    state
        .apply(&SessionEvent::new(
            session_id,
            1,
            SessionEventKind::SessionStarted,
        ))
        .unwrap();
    state
        .apply(&SessionEvent::new(
            session_id,
            2,
            SessionEventKind::SessionConfigured {
                cwd: PathBuf::from("/tmp"),
                provider: CodingProvider::Codex,
                model: Some("gpt-5.6-sol".to_string()),
                effort: Some("xhigh".to_string()),
                fast: false,
                response_language: ResponseLanguage::Auto,
                permission_mode: PermissionMode::FullAccess,
            },
        ))
        .unwrap();
    state
        .apply(&SessionEvent::new(
            session_id,
            3,
            SessionEventKind::TurnCompleted {
                message_id: Uuid::new_v4(),
                provider_session_id: Some("codex-thread".to_string()),
                final_text: "done".to_string(),
                error: None,
            },
        ))
        .unwrap();
    state
        .apply(&SessionEvent::new(
            session_id,
            4,
            SessionEventKind::SessionConfigured {
                cwd: PathBuf::from("/tmp"),
                provider: CodingProvider::Codex,
                model: Some("gpt-5.6-luna".to_string()),
                effort: Some("ultra".to_string()),
                fast: false,
                response_language: ResponseLanguage::Auto,
                permission_mode: PermissionMode::FullAccess,
            },
        ))
        .unwrap();

    assert_eq!(state.context_generation, 1);
    assert_eq!(state.provider_session_id.as_deref(), Some("codex-thread"));
    assert_eq!(state.usage.context_tokens, Some(0));
}

#[tokio::test]
#[ignore = "explicit large-session p95 performance gate"]
async fn large_session_lineage_and_tail_p95_gates() {
    const EVENT_COUNT: u64 = 38_272;
    const SAMPLES: usize = 100;

    let (directory, store) = store().await;
    let parent_id = Uuid::new_v4();
    store.create_session(parent_id).await.unwrap();
    let mut state = SessionState::default();
    let mut transaction = store.pool().begin().await.unwrap();
    for sequence in 1..=EVENT_COUNT {
        let kind = match sequence {
            1 => SessionEventKind::SessionStarted,
            2 => configured(directory.path()),
            _ => SessionEventKind::Error {
                message: "bounded performance fixture".to_string(),
            },
        };
        let event = SessionEvent::new(parent_id, sequence, kind);
        state.apply(&event).unwrap();
        let projection_json = serde_json::to_string(&state).unwrap();
        sqlx::query(
            "insert into session_events \
                 (session_id, sequence, event_id, event_kind, event_json, projection_json, \
                  fork_inheritable, recovery_relevant, message_id, created_at) \
                 values (?, ?, ?, ?, ?, ?, ?, ?, null, ?)",
        )
        .bind(parent_id.to_string())
        .bind(i64::try_from(sequence).unwrap())
        .bind(event.id.to_string())
        .bind(event_kind(&event.kind).unwrap())
        .bind(serde_json::to_string(&event).unwrap())
        .bind(historical_projection_json(sequence, 0, &projection_json))
        .bind(i64::from(event.kind.is_fork_inheritable()))
        .bind(i64::from(event.kind.is_recovery_relevant()))
        .bind(event.created_at.to_rfc3339())
        .execute(&mut *transaction)
        .await
        .unwrap();
    }
    sqlx::query(
        "update sessions set next_sequence = ?, state_json = ?, updated_at = ? where id = ?",
    )
    .bind(i64::try_from(EVENT_COUNT + 1).unwrap())
    .bind(serde_json::to_string(&state).unwrap())
    .bind(Utc::now().to_rfc3339())
    .bind(parent_id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    let mut fork_samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        store
            .fork_before(parent_id, Uuid::new_v4(), EVENT_COUNT + 1)
            .await
            .unwrap();
        fork_samples.push(started.elapsed());
    }
    let mut tail_samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let tail = store
            .events_after(parent_id, EVENT_COUNT - 100, 100)
            .await
            .unwrap();
        assert_eq!(tail.len(), 100);
        tail_samples.push(started.elapsed());
    }
    let fork_p95 = duration_p95(&mut fork_samples);
    let tail_p95 = duration_p95(&mut tail_samples);
    eprintln!("lineage fork p95: {fork_p95:?}; indexed tail p95: {tail_p95:?}");
    assert!(
        fork_p95 < Duration::from_millis(200),
        "lineage fork p95 exceeded 200 ms: {fork_p95:?}"
    );
    assert!(
        tail_p95 < Duration::from_millis(50),
        "indexed tail p95 exceeded 50 ms: {tail_p95:?}"
    );
}

#[tokio::test]
#[ignore = "explicit large-session prompt-recall p95 performance gate"]
async fn large_session_recent_prompt_recall_p95_gate() {
    const EVENT_COUNT: u64 = 25_000;
    const LIMIT: usize = 100;
    const SAMPLES: usize = 100;

    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let mut state = SessionState::default();
    let mut transaction = store.pool().begin().await.unwrap();
    for sequence in 1..=EVENT_COUNT {
        let message_id = Uuid::new_v4();
        let kind = SessionEventKind::Message {
            message_id,
            actor: if sequence % 2 == 0 {
                EventActor::User
            } else {
                EventActor::Assistant
            },
            text: format!("bounded prompt recall fixture {sequence}"),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        };
        let event = SessionEvent::new(session_id, sequence, kind);
        state.apply(&event).unwrap();
        insert_performance_profile_event(&mut transaction, &event, &state).await;
    }
    sqlx::query("update sessions set next_sequence=?, state_json=?, updated_at=? where id=?")
        .bind(i64::try_from(EVENT_COUNT + 1).unwrap())
        .bind(serde_json::to_string(&state).unwrap())
        .bind(Utc::now().to_rfc3339())
        .bind(session_id.to_string())
        .execute(&mut *transaction)
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let prompts = store.recent_user_messages(session_id, LIMIT).await.unwrap();
        assert_eq!(prompts.len(), LIMIT);
        assert_eq!(prompts.last().unwrap().sequence, EVENT_COUNT);
        samples.push(started.elapsed());
    }
    let p95 = duration_p95(&mut samples);
    eprintln!("25k-message recent prompt recall p95: {p95:?}");
    assert!(
        p95 < Duration::from_millis(10),
        "recent prompt recall p95 exceeded 10 ms: {p95:?}"
    );
}

#[tokio::test]
#[ignore = "explicit sparse-message prompt-recall p95 performance gate"]
async fn sparse_recent_prompt_recall_p95_gate() {
    const EVENT_COUNT: u64 = 25_000;
    const LIMIT: usize = 100;
    const SAMPLES: usize = 100;

    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let mut state = SessionState::default();
    let mut transaction = store.pool().begin().await.unwrap();
    for sequence in 1..=EVENT_COUNT {
        let kind = if sequence % 250 == 0 {
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: format!("sparse prompt {sequence}"),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            }
        } else {
            SessionEventKind::Error {
                message: format!("non-message event {sequence}"),
            }
        };
        let event = SessionEvent::new(session_id, sequence, kind);
        state.apply(&event).unwrap();
        insert_performance_profile_event(&mut transaction, &event, &state).await;
    }
    sqlx::query("update sessions set next_sequence=?, state_json=?, updated_at=? where id=?")
        .bind(i64::try_from(EVENT_COUNT + 1).unwrap())
        .bind(serde_json::to_string(&state).unwrap())
        .bind(Utc::now().to_rfc3339())
        .bind(session_id.to_string())
        .execute(&mut *transaction)
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let prompts = store.recent_user_messages(session_id, LIMIT).await.unwrap();
        assert_eq!(prompts.len(), LIMIT);
        samples.push(started.elapsed());
    }
    let p95 = duration_p95(&mut samples);
    eprintln!("25k-event sparse prompt recall p95: {p95:?}");
    assert!(
        p95 < Duration::from_millis(10),
        "sparse recent prompt recall p95 exceeded 10 ms: {p95:?}"
    );
}

const RECOVERY_PROFILE_OBSOLETE_EVENTS: u64 = 25_000;
const RECOVERY_PROFILE_RETAINED_EVENTS: u64 = 100;

async fn recovery_profile_store(
    boundary_kind: SessionEventKind,
) -> (tempfile::TempDir, SqliteSessionStore, Uuid) {
    let (directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let mut state = SessionState::default();
    let mut transaction = store.pool().begin().await.unwrap();
    let completed_message_id = Uuid::new_v4();
    for sequence in 1..=RECOVERY_PROFILE_OBSOLETE_EVENTS {
        let kind = match sequence {
            value if value == RECOVERY_PROFILE_OBSOLETE_EVENTS - 1 => {
                SessionEventKind::TurnStarted {
                    message_id: completed_message_id,
                    provider: CodingProvider::Codex,
                    model: Some("gpt-profile".to_string()),
                    effort: None,
                    fast: false,
                }
            }
            value if value == RECOVERY_PROFILE_OBSOLETE_EVENTS => SessionEventKind::TurnCompleted {
                message_id: completed_message_id,
                provider_session_id: None,
                final_text: String::new(),
                error: None,
            },
            _ => SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: format!("obsolete recovery context {sequence}"),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        };
        let event = SessionEvent::new(session_id, sequence, kind);
        state.apply(&event).unwrap();
        insert_performance_profile_event(&mut transaction, &event, &state).await;
    }
    let boundary_sequence = RECOVERY_PROFILE_OBSOLETE_EVENTS + 1;
    let boundary = SessionEvent::new(session_id, boundary_sequence, boundary_kind);
    state.apply(&boundary).unwrap();
    insert_performance_profile_event(&mut transaction, &boundary, &state).await;
    for sequence in (boundary_sequence + 1)..=(boundary_sequence + RECOVERY_PROFILE_RETAINED_EVENTS)
    {
        let event = SessionEvent::new(
            session_id,
            sequence,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: format!("retained recovery context {sequence}"),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        );
        state.apply(&event).unwrap();
        insert_performance_profile_event(&mut transaction, &event, &state).await;
    }
    let next_sequence = boundary_sequence + RECOVERY_PROFILE_RETAINED_EVENTS + 1;
    sqlx::query("update sessions set next_sequence=?, state_json=?, updated_at=? where id=?")
        .bind(i64::try_from(next_sequence).unwrap())
        .bind(serde_json::to_string(&state).unwrap())
        .bind(Utc::now().to_rfc3339())
        .bind(session_id.to_string())
        .execute(&mut *transaction)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    (directory, store, session_id)
}

#[tokio::test]
#[ignore = "explicit cleared-context recovery p95 performance gate"]
async fn large_cleared_context_recovery_p95_gate() {
    const SAMPLES: usize = 20;
    let (_directory, store, session_id) =
        recovery_profile_store(SessionEventKind::ContextCleared).await;

    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let recovery = store.recovery(session_id).await.unwrap();
        assert_eq!(
            recovery.context_events.len(),
            usize::try_from(RECOVERY_PROFILE_RETAINED_EVENTS + 1).unwrap()
        );
        assert!(matches!(
            recovery.context_events.first().unwrap().kind,
            SessionEventKind::ContextCleared
        ));
        assert!(recovery.queue_events.is_empty());
        samples.push(started.elapsed());
    }
    let p95 = duration_p95(&mut samples);
    eprintln!("25k-event cleared-context recovery p95: {p95:?}");
    assert!(
        p95 < Duration::from_millis(10),
        "cleared-context recovery p95 exceeded 10 ms: {p95:?}"
    );
}

#[tokio::test]
#[ignore = "explicit compacted-context recovery p95 performance gate"]
async fn large_compacted_context_recovery_p95_gate() {
    const SAMPLES: usize = 20;
    let (_directory, store, session_id) = recovery_profile_store(SessionEventKind::ProviderEvent {
        provider: CodingProvider::Codex,
        kind: "context_compaction".to_string(),
        payload: serde_json::json!({
            "status": "completed",
            "summary": "retained recovery summary",
        }),
    })
    .await;

    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let recovery = store.recovery(session_id).await.unwrap();
        assert_eq!(
            recovery.context_events.len(),
            usize::try_from(RECOVERY_PROFILE_RETAINED_EVENTS + 1).unwrap()
        );
        assert!(matches!(
            recovery.context_events.first().unwrap().kind,
            SessionEventKind::ProviderEvent { ref kind, .. } if kind == "context_compaction"
        ));
        assert!(recovery.queue_events.is_empty());
        samples.push(started.elapsed());
    }
    let p95 = duration_p95(&mut samples);
    eprintln!("25k-event compacted-context recovery p95: {p95:?}");
    assert!(
        p95 < Duration::from_millis(10),
        "compacted-context recovery p95 exceeded 10 ms: {p95:?}"
    );
}

async fn insert_performance_profile_event(
    transaction: &mut Transaction<'_, Sqlite>,
    event: &SessionEvent,
    state: &SessionState,
) {
    let message_id = match &event.kind {
        SessionEventKind::Message { message_id, .. } => Some(message_id.to_string()),
        _ => None,
    };
    let projection_json = serde_json::to_string(state).unwrap();
    sqlx::query(
        "insert into session_events \
         (session_id, sequence, event_id, event_kind, event_json, projection_json, \
          fork_inheritable, recovery_relevant, message_id, created_at) \
         values (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
    )
    .bind(event.session_id.to_string())
    .bind(i64::try_from(event.sequence).unwrap())
    .bind(event.id.to_string())
    .bind(event_kind(&event.kind).unwrap())
    .bind(serde_json::to_string(event).unwrap())
    .bind(historical_projection_json(
        event.sequence,
        0,
        &projection_json,
    ))
    .bind(i64::from(event.kind.is_fork_inheritable()))
    .bind(message_id)
    .bind(event.created_at.to_rfc3339())
    .execute(&mut **transaction)
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "explicit lossless-history retrieval p95 performance gate"]
async fn large_session_history_query_p95_gate() {
    const EVENT_COUNT: u64 = 25_000;
    const SAMPLES: usize = 100;
    const NEEDLE: &str = "rare-history-needle-8675309";

    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let mut state = SessionState::default();
    let mut transaction = store.pool().begin().await.unwrap();
    for sequence in 1..=EVENT_COUNT {
        let kind = SessionEventKind::Error {
            message: if sequence == EVENT_COUNT - 17 {
                NEEDLE.to_string()
            } else {
                format!("ordinary bounded history fixture {sequence}")
            },
        };
        let event = SessionEvent::new(session_id, sequence, kind);
        state.apply(&event).unwrap();
        let event_json = serde_json::to_string(&event).unwrap();
        let stored_kind = event_kind(&event.kind).unwrap();
        let projection_json = serde_json::to_string(&state).unwrap();
        sqlx::query(
            "insert into session_events \
             (session_id, sequence, event_id, event_kind, event_json, projection_json, \
              fork_inheritable, recovery_relevant, message_id, created_at) \
             values (?, ?, ?, ?, ?, ?, ?, ?, null, ?)",
        )
        .bind(session_id.to_string())
        .bind(i64::try_from(sequence).unwrap())
        .bind(event.id.to_string())
        .bind(&stored_kind)
        .bind(&event_json)
        .bind(historical_projection_json(sequence, 0, &projection_json))
        .bind(i64::from(event.kind.is_fork_inheritable()))
        .bind(i64::from(event.kind.is_recovery_relevant()))
        .bind(event.created_at.to_rfc3339())
        .execute(&mut *transaction)
        .await
        .unwrap();
        sqlx::query(
            "insert into session_event_search \
             (session_id, sequence, event_id, event_kind, actor, body) \
             values (?, ?, ?, ?, null, ?)",
        )
        .bind(session_id.to_string())
        .bind(i64::try_from(sequence).unwrap())
        .bind(event.id.to_string())
        .bind(stored_kind)
        .bind(event_json)
        .execute(&mut *transaction)
        .await
        .unwrap();
    }
    sqlx::query("update sessions set next_sequence=?, state_json=?, updated_at=? where id=?")
        .bind(i64::try_from(EVENT_COUNT + 1).unwrap())
        .bind(serde_json::to_string(&state).unwrap())
        .bind(Utc::now().to_rfc3339())
        .bind(session_id.to_string())
        .execute(&mut *transaction)
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let query = SessionHistoryQuery {
        text: Some(NEEDLE.to_string()),
        ..SessionHistoryQuery::default()
    };
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let page = store
            .query_history(session_id, query.clone())
            .await
            .unwrap();
        assert_eq!(page.hits.len(), 1);
        samples.push(started.elapsed());
    }
    let p95 = duration_p95(&mut samples);
    eprintln!("25k-event canonical FTS query p95: {p95:?}");
    assert!(
        p95 < Duration::from_millis(10),
        "history FTS query p95 exceeded 10 ms: {p95:?}"
    );

    let regex_query = SessionHistoryQuery {
        text: Some("rare-history-needle-[0-9]+".to_string()),
        mode: SessionHistorySearchMode::Regex,
        prefilter: Some("rare history needle".to_string()),
        scan_limit: Some(EVENT_COUNT as usize),
        ..SessionHistoryQuery::default()
    };
    let mut regex_samples = Vec::with_capacity(10);
    for _ in 0..10 {
        let started = Instant::now();
        let page = store
            .query_history(session_id, regex_query.clone())
            .await
            .unwrap();
        assert_eq!(page.hits.len(), 1);
        regex_samples.push(started.elapsed());
    }
    let regex_p95 = duration_p95(&mut regex_samples);
    eprintln!("25k-event bounded regex query p95: {regex_p95:?}");
    assert!(
        regex_p95 < Duration::from_millis(10),
        "history regex query p95 exceeded 10 ms: {regex_p95:?}"
    );
}

#[tokio::test]
#[ignore = "explicit first-search projection catch-up performance gate"]
async fn first_history_search_after_sustained_appends_profile() {
    const EVENT_COUNT: u64 = 2_000;
    const NEEDLE: &str = "first-search-catchup-needle";

    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let append_started = Instant::now();
    for sequence in 1..=EVENT_COUNT {
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::Error {
                    message: if sequence == EVENT_COUNT {
                        NEEDLE.to_string()
                    } else {
                        format!("ordinary first-search fixture {sequence}")
                    },
                },
            ))
            .await
            .unwrap();
    }
    let append_elapsed = append_started.elapsed();
    let other_session = Uuid::new_v4();
    store.create_session(other_session).await.unwrap();
    let store = Arc::new(store);
    let search_store = Arc::clone(&store);
    let search = tokio::spawn(async move {
        let search_started = Instant::now();
        let page = search_store
            .query_history(
                session_id,
                SessionHistoryQuery {
                    text: Some(NEEDLE.to_string()),
                    ..SessionHistoryQuery::default()
                },
            )
            .await
            .unwrap();
        (page, search_started.elapsed())
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    let concurrent_append_started = Instant::now();
    store
        .append(SessionEvent::new(
            other_session,
            0,
            SessionEventKind::Error {
                message: "concurrent append".to_string(),
            },
        ))
        .await
        .unwrap();
    let concurrent_append_elapsed = concurrent_append_started.elapsed();
    let (page, first_search_elapsed) = search.await.unwrap();
    assert_eq!(page.hits.len(), 1);
    assert!(
        concurrent_append_elapsed < Duration::from_millis(25),
        "history projection catch-up blocked another session append for {concurrent_append_elapsed:?}"
    );
    eprintln!(
        "2k durable appends: {append_elapsed:?}; first FTS search: {first_search_elapsed:?}; concurrent append: {concurrent_append_elapsed:?}"
    );
}

fn duration_p95(samples: &mut [Duration]) -> Duration {
    samples.sort_unstable();
    samples[(samples.len() * 95).div_ceil(100).saturating_sub(1)]
}

fn subagent_activity(
    child_id: Uuid,
    parent_id: Uuid,
    task: &str,
    status: crate::SubagentStatus,
) -> SessionEventKind {
    SessionEventKind::SubagentActivity {
        activity: crate::SubagentActivityKind::Updated,
        agent: crate::SubagentSnapshot {
            session_id: child_id,
            parent_session_id: parent_id,
            task_name: task.to_string(),
            status,
            provider: crate::CodingProvider::Claude,
            model: None,
            effort: None,
            cwd: std::path::PathBuf::from("/tmp"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            detail: None,
            final_text: None,
            usage: crate::SubagentUsage::default(),
        },
        event: None,
    }
}

async fn seed_recovery_fixture(store: &SqliteSessionStore, session_id: Uuid, children: &[Uuid]) {
    for (index, child) in children.iter().enumerate() {
        for status in [
            crate::SubagentStatus::Starting,
            crate::SubagentStatus::Running,
        ] {
            store
                .append(SessionEvent::new(
                    session_id,
                    0,
                    subagent_activity(*child, session_id, &format!("task_{index}"), status),
                ))
                .await
                .unwrap();
        }
    }
    store
        .append(SessionEvent::new(
            session_id,
            0,
            message(Uuid::new_v4(), "queued prompt"),
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::PromptRecalled {
                message_id: Uuid::new_v4(),
                text: "recalled prompt".into(),
                attachments: Vec::new(),
            },
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ToolStarted {
                tool_call_id: "tool-1".into(),
                name: "read_file".into(),
                input: serde_json::json!({"path": "/tmp/a"}),
                input_ref: None,
            },
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ToolCompleted {
                tool_call_id: "tool-1".into(),
                output: "ok".into(),
                output_ref: None,
                is_error: false,
                input: None,
                input_ref: None,
            },
        ))
        .await
        .unwrap();
}

fn event_ids(events: &[SessionEvent]) -> Vec<Uuid> {
    events.iter().map(|event| event.id).collect()
}

#[tokio::test]
async fn narrowed_recovery_parts_match_the_full_projection() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let children = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    seed_recovery_fixture(&store, session_id, &children).await;

    let full = store.recovery(session_id).await.unwrap();
    assert_eq!(
        full.subagent_events.len(),
        children.len(),
        "recovery keeps only the latest activity per child"
    );
    assert!(!full.queue_events.is_empty());
    assert!(!full.context_events.is_empty());

    let queue = store
        .recovery_parts(session_id, RecoveryParts::QUEUE)
        .await
        .unwrap();
    assert_eq!(
        event_ids(&queue.queue_events),
        event_ids(&full.queue_events)
    );
    assert!(queue.context_events.is_empty());
    assert!(queue.subagent_events.is_empty());

    let subagents = store
        .recovery_parts(session_id, RecoveryParts::SUBAGENTS)
        .await
        .unwrap();
    assert_eq!(
        event_ids(&subagents.subagent_events),
        event_ids(&full.subagent_events)
    );
    assert!(subagents.context_events.is_empty());
    assert!(subagents.queue_events.is_empty());

    let all = store
        .recovery_parts(session_id, RecoveryParts::ALL)
        .await
        .unwrap();
    assert_eq!(
        event_ids(&all.context_events),
        event_ids(&full.context_events)
    );
    assert_eq!(event_ids(&all.queue_events), event_ids(&full.queue_events));
    assert_eq!(
        event_ids(&all.subagent_events),
        event_ids(&full.subagent_events)
    );
}

#[tokio::test]
async fn narrowed_recovery_preserves_a_cut_inside_inherited_history() {
    let (_directory, store) = store().await;
    let parent = Uuid::new_v4();
    store.create_session(parent).await.unwrap();
    for index in 0..2 {
        store
            .append(SessionEvent::new(
                parent,
                0,
                SessionEventKind::ToolCompleted {
                    tool_call_id: format!("tool-{index}"),
                    output: "result".into(),
                    output_ref: None,
                    is_error: false,
                    input: None,
                    input_ref: None,
                },
            ))
            .await
            .unwrap();
        let mut prompt = message(Uuid::new_v4(), "completed prompt");
        if let SessionEventKind::Message { status, .. } = &mut prompt {
            *status = crate::MessageStatus::Complete;
        }
        store
            .append(SessionEvent::new(parent, 0, prompt))
            .await
            .unwrap();
    }
    let child = Uuid::new_v4();
    store.fork_before(parent, child, 5).await.unwrap();
    let grandchild = Uuid::new_v4();
    store.fork_before(child, grandchild, 3).await.unwrap();
    let full = store.recovery(grandchild).await.unwrap();
    let queue = store
        .recovery_parts(grandchild, RecoveryParts::QUEUE)
        .await
        .unwrap();
    assert_eq!(full.queue_events.len(), 1);
    assert_eq!(
        event_ids(&queue.queue_events),
        event_ids(&full.queue_events)
    );
}

#[tokio::test]
async fn narrowed_recovery_parts_match_the_full_projection_across_a_context_boundary() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let children = [Uuid::new_v4(), Uuid::new_v4()];
    seed_recovery_fixture(&store, session_id, &children).await;
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ContextCleared,
        ))
        .await
        .unwrap();
    seed_recovery_fixture(&store, session_id, &children).await;

    let full = store.recovery(session_id).await.unwrap();
    let queue = store
        .recovery_parts(session_id, RecoveryParts::QUEUE)
        .await
        .unwrap();
    let subagents = store
        .recovery_parts(session_id, RecoveryParts::SUBAGENTS)
        .await
        .unwrap();
    assert_eq!(
        event_ids(&queue.queue_events),
        event_ids(&full.queue_events)
    );
    assert!(queue.context_events.is_empty());
    assert_eq!(
        event_ids(&subagents.subagent_events),
        event_ids(&full.subagent_events)
    );
    assert!(subagents.context_events.is_empty());
    assert!(subagents.queue_events.is_empty());
}

#[tokio::test]
#[ignore = "explicit recovery performance benchmark"]
async fn narrowed_recovery_skips_the_context_payloads_a_resume_never_reads() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    let children = (0..20).map(|_| Uuid::new_v4()).collect::<Vec<_>>();
    seed_recovery_fixture(&store, session_id, &children).await;
    // A long session is mostly tool traffic. Recovery has to match all of it
    // for provider replay, which is exactly the cost resume should not pay to
    // seed a roster or a prompt queue.
    let payload = "x".repeat(4_096);
    for index in 0..2_000 {
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ToolCompleted {
                    tool_call_id: format!("tool-{index}"),
                    output: payload.clone(),
                    output_ref: None,
                    is_error: false,
                    input: None,
                    input_ref: None,
                },
            ))
            .await
            .unwrap();
    }

    let full_started = Instant::now();
    let full = store.recovery(session_id).await.unwrap();
    let full_elapsed = full_started.elapsed();
    let subagents_started = Instant::now();
    let subagents = store
        .recovery_parts(session_id, RecoveryParts::SUBAGENTS)
        .await
        .unwrap();
    let subagents_elapsed = subagents_started.elapsed();
    let queue_started = Instant::now();
    let queue = store
        .recovery_parts(session_id, RecoveryParts::QUEUE)
        .await
        .unwrap();
    let queue_elapsed = queue_started.elapsed();

    assert!(full.context_events.len() > 2_000);
    assert_eq!(
        event_ids(&subagents.subagent_events),
        event_ids(&full.subagent_events)
    );
    assert_eq!(
        event_ids(&queue.queue_events),
        event_ids(&full.queue_events)
    );
    eprintln!(
        "recovery over {} context events: full {full_elapsed:?}; roster-only {subagents_elapsed:?}; queue-only {queue_elapsed:?}",
        full.context_events.len()
    );
}

#[tokio::test]
async fn compact_removes_legacy_mirrored_rows_the_journal_no_longer_persists() {
    let (_directory, store) = store().await;
    let parent = Uuid::new_v4();
    let child = Uuid::new_v4();
    store.create_session(parent).await.unwrap();
    let snapshot = crate::SubagentSnapshot {
        session_id: child,
        parent_session_id: parent,
        task_name: "worker".to_string(),
        status: crate::SubagentStatus::Running,
        provider: CodingProvider::Codex,
        model: None,
        effort: None,
        cwd: std::path::PathBuf::from("/tmp"),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        detail: None,
        final_text: None,
        usage: crate::SubagentUsage::default(),
    };
    let mirrored = |kind: SessionEventKind| SessionEventKind::SubagentActivity {
        activity: crate::SubagentActivityKind::Updated,
        agent: snapshot.clone(),
        event: Some(Box::new(SessionEvent::new(child, 7, kind))),
    };
    let heartbeat = || SessionEventKind::ProviderEvent {
        provider: CodingProvider::Codex,
        kind: "heartbeat".to_string(),
        payload: serde_json::json!({ "noise": true }),
    };
    store
        .append(SessionEvent::new(
            parent,
            0,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: "orchestrate".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: Some(PromptDelivery::Queue),
            },
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            parent,
            0,
            mirrored(SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: "child finished".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            }),
        ))
        .await
        .unwrap();
    // A row an earlier release journaled: the child's provider heartbeat,
    // mirrored as a durable parent event.
    let legacy = SessionEvent::new(parent, 3, mirrored(heartbeat()));
    sqlx::query(
        "insert into session_events \
         (session_id, sequence, event_id, event_kind, event_json, projection_json, \
          fork_inheritable, recovery_relevant, message_id, created_at) \
         values (?, ?, ?, 'subagent_activity', ?, '{}', 0, 1, null, ?)",
    )
    .bind(parent.to_string())
    .bind(3_i64)
    .bind(legacy.id.to_string())
    .bind(serde_json::to_string(&legacy).unwrap())
    .bind(legacy.created_at.to_rfc3339())
    .execute(store.pool())
    .await
    .unwrap();
    let count = |store: &SqliteSessionStore| {
        let pool = store.pool().clone();
        let parent = parent.to_string();
        async move {
            sqlx::query_scalar::<_, i64>("select count(*) from session_events where session_id = ?")
                .bind(parent)
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };
    assert_eq!(count(&store).await, 3);

    let outcome = store.compact(false).await.unwrap();
    assert_eq!(outcome.deleted_events, 1);
    assert!(!outcome.vacuumed);
    assert_eq!(count(&store).await, 2);
    let kinds: Vec<String> = sqlx::query_scalar(
        "select event_kind from session_events where session_id = ? order by sequence",
    )
    .bind(parent.to_string())
    .fetch_all(store.pool())
    .await
    .unwrap();
    assert_eq!(kinds, vec!["message", "subagent_activity"]);

    // The live journal no longer writes that row in the first place.
    let live = store
        .append(SessionEvent::new(parent, 0, mirrored(heartbeat())))
        .await
        .unwrap();
    assert_eq!(live.sequence, 0);
    assert_eq!(count(&store).await, 2);

    let vacuumed = store.compact(true).await.unwrap();
    assert!(vacuumed.vacuumed);
    assert_eq!(vacuumed.deleted_events, 0);
    // The sizes are reported from the page count; a tiny store may not shrink
    // (a VACUUM can even add a page), so the contract is only that both are
    // measured, not that the file always gets smaller.
    assert!(vacuumed.bytes_before > 0 && vacuumed.bytes_after > 0);
}

fn opencode_configured(model: &str) -> SessionEventKind {
    SessionEventKind::SessionConfigured {
        cwd: std::path::PathBuf::from("/tmp"),
        provider: CodingProvider::OpenCode,
        model: Some(model.to_string()),
        effort: None,
        fast: false,
        response_language: crate::ResponseLanguage::default(),
        permission_mode: crate::PermissionMode::Auto,
    }
}

/// Only the Go aliases have an API Borg can call directly, so the route is
/// decided by the model. Getting this wrong either strands a working route on
/// the CLI or, worse, points a non-Go model at the Go allowance.
#[tokio::test]
async fn opencode_route_is_native_only_for_go_models() {
    let (_directory, store) = store().await;
    for (model, native) in [
        ("opencode-go/kimi-k2.7-code", true),
        ("opencode-go/glm-5.3", true),
        ("opencode/kimi-k2.7-code", false),
        ("opencode-go/", false),
        ("claude-opus-5", false),
    ] {
        let session_id = Uuid::new_v4();
        store.create_session(session_id).await.unwrap();
        assert_eq!(
            store
                .uses_native_opencode_harness(session_id, Some(model))
                .await
                .unwrap(),
            native,
            "{model}"
        );
    }
}

/// The route is pinned on first resolution. A session that has been answering
/// through the `opencode` CLI keeps a transcript only that CLI can replay, so
/// switching to a Go model must not move the conversation onto Borg's harness
/// — and a session Borg already owns must not be handed back.
#[tokio::test]
async fn a_pinned_opencode_route_survives_model_switches_and_restart() {
    let (directory, store) = store().await;

    // Pinned native, then switched to a route with no Borg-reachable API.
    let native_session = Uuid::new_v4();
    store.create_session(native_session).await.unwrap();
    assert!(
        store
            .uses_native_opencode_harness(native_session, Some("opencode-go/kimi-k2.7-code"))
            .await
            .unwrap()
    );
    assert!(
        store
            .uses_native_opencode_harness(native_session, Some("opencode/kimi-k2.7-code"))
            .await
            .unwrap(),
        "a pinned native session keeps its Borg-owned history across a model switch"
    );

    // A CLI-owned conversation that later selects a Go model.
    let legacy_session = Uuid::new_v4();
    store.create_session(legacy_session).await.unwrap();
    store
        .append(SessionEvent::new(
            legacy_session,
            0,
            opencode_configured("opencode/kimi-k2.7-code"),
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            legacy_session,
            0,
            SessionEventKind::ProviderSessionLinked {
                provider_session_id: "opencode-cli-thread".to_string(),
                provider_turn_id: None,
                context_contract_version: None,
            },
        ))
        .await
        .unwrap();
    assert!(
        !store
            .uses_native_opencode_harness(legacy_session, Some("opencode-go/kimi-k2.7-code"))
            .await
            .unwrap(),
        "legacy OpenCode history must not be adopted by Borg's harness"
    );

    store.pool.close().await;
    let reopened = SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
        .await
        .unwrap();
    assert!(
        reopened
            .uses_native_opencode_harness(native_session, None)
            .await
            .unwrap()
    );
    assert!(
        !reopened
            .uses_native_opencode_harness(legacy_session, None)
            .await
            .unwrap()
    );
}

/// A fork or child shares its owner's transcript, so it must share whichever
/// harness owns it; otherwise one conversation would have two owners.
#[tokio::test]
async fn opencode_route_is_inherited_by_forks_and_children() {
    let (_directory, store) = store().await;
    for (model, native) in [
        ("opencode-go/kimi-k2.7-code", true),
        ("opencode/glm", false),
    ] {
        let parent = Uuid::new_v4();
        store.create_session(parent).await.unwrap();
        assert_eq!(
            store
                .uses_native_opencode_harness(parent, Some(model))
                .await
                .unwrap(),
            native
        );
        store
            .append(SessionEvent::new(parent, 0, opencode_configured(model)))
            .await
            .unwrap();

        let fork = Uuid::new_v4();
        store.fork_before(parent, fork, 1).await.unwrap();
        let child = Uuid::new_v4();
        store.register_child_session(parent, child).await.unwrap();
        for inheritor in [fork, child] {
            assert_eq!(
                store
                    .uses_native_opencode_harness(inheritor, None)
                    .await
                    .unwrap(),
                native,
                "{model}"
            );
            // Re-resolving under the opposite model cannot rewrite the route.
            assert_eq!(
                store
                    .uses_native_opencode_harness(inheritor, Some("opencode-go/other"))
                    .await
                    .unwrap(),
                native
            );
        }
    }
}

/// A fresh session with no model yet must stay undecided. Pinning it here
/// would strand it on the compatibility route before its model was ever known.
#[tokio::test]
async fn an_unknown_opencode_model_does_not_pin_the_route() {
    let (_directory, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    assert!(
        !store
            .uses_native_opencode_harness(session_id, None)
            .await
            .unwrap()
    );
    assert!(
        store
            .uses_native_opencode_harness(session_id, Some("opencode-go/kimi-k2.7-code"))
            .await
            .unwrap(),
        "the route is still open once the model is known"
    );
}

/// The parent mirrors a child's whole event stream so the UI can follow a child
/// live, but a child's provider audit trail is already durable in the child's
/// own journal and nothing renders or replays it from the parent. Measured on
/// one orchestration session, mirrored child `native_model_message` rows alone
/// were 424 MB of 1,132 MB. Ordered subagent replay of the child's transcript
/// events must be unaffected.
#[test]
fn mirrored_child_provider_audit_events_are_live_only() {
    let child_id = Uuid::new_v4();
    let snapshot = || crate::SubagentSnapshot {
        session_id: child_id,
        parent_session_id: Uuid::new_v4(),
        task_name: "worker".to_string(),
        status: crate::SubagentStatus::Running,
        provider: CodingProvider::Codex,
        model: None,
        effort: None,
        cwd: std::path::PathBuf::from("/tmp"),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        detail: None,
        final_text: None,
        usage: crate::SubagentUsage::default(),
    };
    let mirrored = |kind: SessionEventKind| SessionEventKind::SubagentActivity {
        activity: crate::SubagentActivityKind::Updated,
        agent: snapshot(),
        event: Some(Box::new(SessionEvent::new(child_id, 0, kind))),
    };

    // Durable in the child, but provider audit records the parent never reads.
    for kind in [
        "native_model_message",
        "native_tool_round_completed",
        "context_compaction",
    ] {
        let child = SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: kind.to_string(),
            payload: serde_json::json!({"content": "a full model message"}),
        };
        assert_eq!(
            child.persistence(),
            EventPersistence::Durable,
            "fixture must be durable in the child: {kind}"
        );
        assert_eq!(
            mirrored(child).persistence(),
            EventPersistence::Ephemeral,
            "the parent must not journal a child's provider audit trail: {kind}"
        );
    }

    // A child's own session metadata describes the child's session, not its
    // transcript. The parent renders and replays none of it.
    for kind in [
        SessionEventKind::ProviderCapabilitiesUpdated {
            providers: Vec::new(),
        },
        SessionEventKind::UserStopChanged { engaged: true },
    ] {
        assert_eq!(
            kind.persistence(),
            EventPersistence::Durable,
            "fixture must be durable in the child: {kind:?}"
        );
        assert_eq!(
            mirrored(kind.clone()).persistence(),
            EventPersistence::Ephemeral,
            "the parent must not journal a child's own session metadata: {kind:?}"
        );
    }

    // The child's transcript events stay durable in the parent: commit 84b03b9
    // requires them to remain replayable after the live projection disconnects.
    let replayable = [
        SessionEventKind::ToolStarted {
            tool_call_id: "call-1".to_string(),
            name: "exec".to_string(),
            input: serde_json::json!({"cmd": "cargo test"}),
            input_ref: None,
        },
        SessionEventKind::ToolCompleted {
            tool_call_id: "call-1".to_string(),
            output: "ok".to_string(),
            output_ref: None,
            is_error: false,
            input: None,
            input_ref: None,
        },
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: crate::EventActor::Assistant,
            text: "report".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
        SessionEventKind::StatusChanged {
            status: crate::SessionStatus::Ready,
            detail: None,
        },
    ];
    for kind in replayable {
        assert_eq!(
            mirrored(kind.clone()).persistence(),
            EventPersistence::Durable,
            "ordered subagent replay must keep this durable: {kind:?}"
        );
    }

    // A child's live stream stays live-only, and snapshot-only activity is
    // always durable.
    assert_eq!(
        mirrored(SessionEventKind::ReasoningDelta {
            text: "thinking".to_string()
        })
        .persistence(),
        EventPersistence::Ephemeral
    );
    assert_eq!(
        SessionEventKind::SubagentActivity {
            activity: crate::SubagentActivityKind::Completed,
            agent: snapshot(),
            event: None,
        }
        .persistence(),
        EventPersistence::Durable
    );
}

/// Journal rows written by older builds must still load. Two shapes matter and
/// both are taken verbatim from the live 59 GB journal:
///
/// * a July `subagent_activity` whose agent snapshot predates the `usage`
///   field entirely, and
/// * a row whose mirrored child event is a kind current code no longer
///   journals - dropping the *write* must never drop the *read*, or 3.4 GB of
///   existing history stops rendering.
#[test]
fn historical_subagent_activity_rows_still_deserialise() {
    // Verbatim from the journal: session 479bc6e5, 2026-07-26, before the agent
    // snapshot carried `usage`.
    let legacy_july = r#"{"id":"4d85aa4b-e17d-413b-a94c-eb050d98a783","session_id":"479bc6e5-9272-4efa-b2a3-641adf31c379","sequence":33,"created_at":"2026-07-26T01:01:45.889433205Z","kind":{"type":"subagent_activity","activity":"started","agent":{"session_id":"1e293f97-280c-40b8-bade-9dc7fc5da93c","parent_session_id":"479bc6e5-9272-4efa-b2a3-641adf31c379","task_name":"/root/smoke_child","status":"starting","provider":"codex","model":"gpt-5.6-sol","effort":"medium","cwd":"/home/shulgin/borg","created_at":"2026-07-26T01:01:45.861616732Z","updated_at":"2026-07-26T01:01:45.861616732Z","detail":null,"final_text":null},"event":null}}"#;

    let event: SessionEvent =
        serde_json::from_str(legacy_july).expect("a pre-`usage` agent snapshot must still load");
    let SessionEventKind::SubagentActivity {
        activity,
        agent,
        event: child,
    } = &event.kind
    else {
        panic!("expected subagent_activity, got {:?}", event.kind);
    };
    assert_eq!(*activity, crate::SubagentActivityKind::Started);
    assert_eq!(agent.task_name, "/root/smoke_child");
    assert_eq!(agent.status, crate::SubagentStatus::Starting);
    assert!(child.is_none());
    // The missing field defaults rather than failing the whole row.
    assert_eq!(agent.usage.total_tokens, 0);

    // A row whose child event is a kind we now keep live-only. Current code
    // will not write this again, but 3.4 GB of journal already contains it and
    // it must still load and still render the same agent state.
    let mirrored_provider_audit = r#"{"id":"0fe32fee-495d-4a9e-8271-6246f98ca113","session_id":"bd254d05-e129-4703-af86-68e9aafc3223","sequence":293527,"created_at":"2026-09-02T16:57:19.433426094Z","kind":{"type":"subagent_activity","activity":"updated","agent":{"session_id":"eee819c7-ae35-45c9-af95-cf3a3efc33b7","parent_session_id":"bd254d05-e129-4703-af86-68e9aafc3223","task_name":"/root/worker","status":"running","provider":"codex","model":"gpt-5.6-luna","effort":"max","cwd":"/home/shulgin/free-radicals","created_at":"2026-09-02T10:03:11.028415785Z","updated_at":"2026-09-02T16:57:19.423451668Z","detail":"turn phase: provider active","final_text":"a report","usage":{"input_tokens":1158104,"output_tokens":171134,"total_tokens":28811094,"context_tokens":124146,"cost_microusd":null,"cost_basis":"unavailable"}},"event":{"id":"42169e93-e4f8-4674-bb39-2e26ebdadb84","session_id":"eee819c7-ae35-45c9-af95-cf3a3efc33b7","sequence":0,"created_at":"2026-09-02T16:57:19.423451668Z","kind":{"type":"provider_event","provider":"codex","kind":"native_model_message","payload":{"content":"anything"}}}}}"#;

    let event: SessionEvent = serde_json::from_str(mirrored_provider_audit)
        .expect("an already-journaled provider-audit row must still load");
    let SessionEventKind::SubagentActivity {
        agent,
        event: Some(child),
        ..
    } = &event.kind
    else {
        panic!("expected a mirrored child event, got {:?}", event.kind);
    };
    assert_eq!(agent.usage.total_tokens, 28_811_094);
    assert_eq!(agent.detail.as_deref(), Some("turn phase: provider active"));
    assert!(matches!(
        child.kind,
        SessionEventKind::ProviderEvent { ref kind, .. } if kind == "native_model_message"
    ));
    // Reading it back is unaffected by the write rule; only new writes stop.
    assert_eq!(event.kind.persistence(), EventPersistence::Ephemeral);
}

/// Keeping a mirrored child event out of the journal must not change what the
/// UI reconstructs from history. borg-ui's `rebuild_agents` walks the durable
/// history and lets the LAST `subagent_activity` per child win, so the risk is
/// that the dropped row was the last one and carried fresher agent state than
/// the row before it.
///
/// It does not, and the reason is structural: the agent snapshot only moves
/// when the child does transcript work, and those rows stay durable. Verified
/// against the live journal too - across the 25 most recent sessions, all 23
/// agents reconstruct a byte-identical final snapshot with and without the
/// dropped rows, even though 16 of them end on a row that is now dropped.
#[test]
fn dropping_live_only_child_rows_does_not_change_the_reconstructed_roster() {
    let child_id = Uuid::new_v4();
    let parent_id = Uuid::new_v4();
    let snapshot = |detail: &str, total_tokens: u64| crate::SubagentSnapshot {
        session_id: child_id,
        parent_session_id: parent_id,
        task_name: "worker".to_string(),
        status: crate::SubagentStatus::Running,
        provider: CodingProvider::Codex,
        model: None,
        effort: None,
        cwd: std::path::PathBuf::from("/tmp"),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        detail: Some(detail.to_string()),
        final_text: None,
        usage: crate::SubagentUsage {
            total_tokens,
            ..Default::default()
        },
    };
    let row = |agent: crate::SubagentSnapshot, child: Option<SessionEventKind>| {
        SessionEventKind::SubagentActivity {
            activity: crate::SubagentActivityKind::Updated,
            agent,
            event: child.map(|kind| Box::new(SessionEvent::new(child_id, 0, kind))),
        }
    };

    // The agent snapshot advances on transcript work, then the child emits
    // provider audit and capability rows that carry the SAME snapshot - which
    // is what the live journal actually looks like, and why the last row being
    // dropped is harmless.
    let settled = snapshot("ran a tool", 2_048);
    let history = vec![
        row(
            snapshot("starting", 0),
            Some(SessionEventKind::StatusChanged {
                status: crate::SessionStatus::Running,
                detail: None,
            }),
        ),
        row(
            settled.clone(),
            Some(SessionEventKind::ToolCompleted {
                tool_call_id: "call-1".to_string(),
                output: "ok".to_string(),
                output_ref: None,
                is_error: false,
                input: None,
                input_ref: None,
            }),
        ),
        // Both of these are now live-only, and both are LAST.
        row(
            settled.clone(),
            Some(SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "native_model_message".to_string(),
                payload: serde_json::json!({"content": "big"}),
            }),
        ),
        row(
            settled.clone(),
            Some(SessionEventKind::ProviderCapabilitiesUpdated {
                providers: Vec::new(),
            }),
        ),
    ];

    // Mirrors borg-ui rebuild_agents: last activity per child wins.
    let latest_agent = |events: &[SessionEventKind]| {
        events
            .iter()
            .filter_map(|kind| match kind {
                SessionEventKind::SubagentActivity { agent, .. } => Some(agent.clone()),
                _ => None,
            })
            .next_back()
            .expect("at least one activity")
    };

    let durable: Vec<SessionEventKind> = history
        .iter()
        .filter(|kind| kind.persistence() == EventPersistence::Durable)
        .cloned()
        .collect();

    assert_eq!(
        durable.len(),
        2,
        "the provider-audit and capability rows must be live-only"
    );
    let from_everything = latest_agent(&history);
    let from_journal = latest_agent(&durable);
    assert_eq!(from_journal.detail, from_everything.detail);
    assert_eq!(
        from_journal.usage.total_tokens,
        from_everything.usage.total_tokens
    );
    assert_eq!(from_journal.status, from_everything.status);
    assert_eq!(from_journal.session_id, from_everything.session_id);
}

/// What the two backends actually cost on disk for the same history.
///
/// The migration's storage claim is not "Postgres is smaller" -- a row store
/// with per-row headers, a uuid primary key and several indexes starts out
/// LARGER than SQLite for the same events. The claim is that the cold tier pays
/// that back: once a thread has gone quiet, its bodies are dictionary-compressed
/// and the footprint drops below the SQLite baseline. Reporting only the hot
/// number, or only the cold one, would each be a half-truth, so this prints all
/// three and the ratios between them.
///
/// Both backends are written through `append`, not raw inserts, so each pays
/// its real per-event cost including projections and indexes.
#[tokio::test]
#[ignore = "explicit storage footprint comparison against the SQLite baseline"]
async fn storage_footprint_profile() {
    const SESSIONS: usize = 12;
    const EVENTS_PER_SESSION: u64 = 150;

    let Some(url) = crate::session_store::postgres::testing::test_url() else {
        eprintln!("storage footprint: skipping, BORG_TEST_SESSIONS_URL is not set");
        return;
    };

    // Bodies shaped like real agent traffic: repeated structure with varying
    // detail. Uniform filler would flatter the dictionary; wholly random text
    // would defeat it. Neither would say anything about production.
    fn event_for(session_id: Uuid, sequence: u64) -> SessionEvent {
        let kind = match sequence % 5 {
            1 => SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: format!(
                    "run the failing integration test for module {} and report the first error",
                    sequence
                ),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: Some(PromptDelivery::Queue),
            },
            2 => SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: format!(
                    "I ran the suite for module {sequence}. 3 tests failed, all in the \
                     serialisation path; the first is `decode_rejects_truncated_body`, which \
                     expects an error and receives Ok(()). I will look at the decoder next."
                ),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
            _ => SessionEventKind::Error {
                message: format!(
                    "tool `cargo test -p module-{sequence}` exited with status 101: \
                     thread 'decode_rejects_truncated_body' panicked at src/decode.rs:214: \
                     assertion failed: result.is_err()"
                ),
            },
        };
        SessionEvent::new(session_id, sequence, kind)
    }

    async fn fill(store: &dyn SessionStore) {
        for _ in 0..SESSIONS {
            let session_id = Uuid::new_v4();
            store.create_session(session_id).await.expect("create");
            store
                .append(SessionEvent::new(
                    session_id,
                    0,
                    SessionEventKind::SessionStarted,
                ))
                .await
                .expect("started");
            for sequence in 2..=EVENTS_PER_SESSION {
                store
                    .append(event_for(session_id, sequence))
                    .await
                    .expect("append");
            }
        }
    }

    let total_events = SESSIONS as u64 * EVENTS_PER_SESSION;

    let directory = tempdir().expect("temp dir");
    let path = directory.path().join("sessions.sqlite3");
    let sqlite = SqliteSessionStore::open(&path).await.expect("sqlite");
    fill(&sqlite).await;
    // Vacuum both engines before measuring. Comparing a compacted database to
    // an uncompacted one measures when each was last tidied, not what the data
    // costs -- an earlier version of this benchmark did exactly that and
    // reported a negative footprint.
    sqlite.compact(true).await.expect("sqlite vacuum");
    let sqlite_bytes = std::fs::metadata(&path).expect("sqlite metadata").len() as i64;

    let scratch = crate::session_store::postgres::testing::ScratchDatabase::create(&url).await;
    let postgres = crate::session_store::postgres::PostgresSessionStore::connect_with_pool_size(
        &scratch.url,
        4,
    )
    .await
    .expect("postgres");
    fill(&postgres).await;

    /// Bytes held by the session tables and their indexes.
    ///
    /// Summed per relation rather than taken from `pg_database_size`, which
    /// also counts the system catalogs every database carries whether or not it
    /// holds a single event.
    async fn session_bytes(store: &crate::session_store::postgres::PostgresSessionStore) -> i64 {
        sqlx::query_scalar(
            "select coalesce(sum(pg_total_relation_size(c.oid)), 0)::bigint \
             from pg_class c join pg_namespace n on n.oid = c.relnamespace \
             where n.nspname = 'public' and c.relkind = 'r' \
               and c.relname in ('sessions', 'session_events', 'session_payloads', \
                                 'session_actions', 'session_action_transitions', \
                                 'session_event_dicts')",
        )
        .fetch_one(store.pool())
        .await
        .expect("session bytes")
    }

    sqlx::query("vacuum full")
        .execute(postgres.pool())
        .await
        .ok();
    let hot = session_bytes(&postgres).await;

    // Force the cold tier: a cutoff in the future ages every session, which is
    // what a real journal reaches seven days after a thread goes quiet.
    let aged = postgres
        .age_cold_sessions(Utc::now() + chrono::Duration::days(1), SESSIONS * 2)
        .await
        .expect("age");
    sqlx::query("vacuum full")
        .execute(postgres.pool())
        .await
        .ok();
    let cold = session_bytes(&postgres).await;

    let per_event = |bytes: i64| bytes as f64 / total_events as f64;
    eprintln!("storage footprint over {total_events} events across {SESSIONS} sessions");
    eprintln!(
        "  sqlite         {sqlite_bytes:>10} bytes  ({:6.1} B/event)",
        per_event(sqlite_bytes)
    );
    eprintln!(
        "  postgres hot   {hot:>10} bytes  ({:6.1} B/event)  {:.2}x sqlite",
        per_event(hot),
        hot as f64 / sqlite_bytes as f64
    );
    eprintln!(
        "  postgres cold  {cold:>10} bytes  ({:6.1} B/event)  {:.2}x sqlite, {:.2}x hot",
        per_event(cold),
        cold as f64 / sqlite_bytes as f64,
        cold as f64 / hot as f64
    );
    eprintln!(
        "  bodies: {} -> {} bytes ({:.2}x) over {} events in {} sessions",
        aged.bytes_before,
        aged.bytes_after,
        aged.bytes_before as f64 / aged.bytes_after.max(1) as f64,
        aged.events_compressed,
        aged.sessions_aged
    );

    assert!(
        cold < hot,
        "the cold tier must shrink the footprint: hot {hot}, cold {cold}"
    );

    scratch.discard().await;
}

/// Does adding writers add throughput? This is the migration's central claim.
///
/// SQLite permits one writer per FILE, so concurrent agents appending to
/// DIFFERENT sessions still queue behind each other -- throughput is flat no
/// matter how many are added. Postgres serialises per session ROW, via
/// `select ... for update` in the sequence allocator, so those same writers
/// proceed in parallel.
///
/// Each writer owns its own session on purpose. Writers sharing one session
/// SHOULD serialise on both backends; that is the sequence contract working,
/// not contention, and measuring it would prove nothing about the file lock.
#[tokio::test]
#[ignore = "explicit concurrent-writer scaling comparison"]
async fn concurrent_writer_scaling_profile() {
    const APPENDS_PER_WRITER: u64 = 60;
    const WRITER_COUNTS: [usize; 5] = [1, 4, 8, 16, 32];

    async fn throughput(store: Arc<dyn SessionStore>, writers: usize) -> f64 {
        let mut sessions = Vec::with_capacity(writers);
        for _ in 0..writers {
            let session_id = Uuid::new_v4();
            store.create_session(session_id).await.expect("create");
            store
                .append(SessionEvent::new(
                    session_id,
                    0,
                    SessionEventKind::SessionStarted,
                ))
                .await
                .expect("started");
            sessions.push(session_id);
        }

        let started = Instant::now();
        let mut tasks = Vec::with_capacity(writers);
        for session_id in sessions {
            let store = Arc::clone(&store);
            tasks.push(tokio::spawn(async move {
                for sequence in 2..=(APPENDS_PER_WRITER + 1) {
                    store
                        .append(SessionEvent::new(
                            session_id,
                            sequence,
                            SessionEventKind::Error {
                                message: "concurrent writer fixture".to_string(),
                            },
                        ))
                        .await
                        .expect("append");
                }
            }));
        }
        for task in tasks {
            task.await.expect("writer");
        }
        let elapsed = started.elapsed();
        (writers as u64 * APPENDS_PER_WRITER) as f64 / elapsed.as_secs_f64()
    }

    let mut report: Vec<(&str, Vec<f64>)> = Vec::new();

    let directory = tempdir().expect("temp dir");
    let sqlite: Arc<dyn SessionStore> = Arc::new(
        SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .expect("sqlite"),
    );
    let mut sqlite_rates = Vec::new();
    for writers in WRITER_COUNTS {
        sqlite_rates.push(throughput(Arc::clone(&sqlite), writers).await);
    }
    report.push(("sqlite", sqlite_rates));

    let scratch = match crate::session_store::postgres::testing::test_url() {
        Some(url) => {
            Some(crate::session_store::postgres::testing::ScratchDatabase::create(&url).await)
        }
        None => {
            eprintln!("concurrent writers: skipping postgres, BORG_TEST_SESSIONS_URL is not set");
            None
        }
    };
    if let Some(scratch) = &scratch {
        // One connection per writer, or the pool becomes the bottleneck being
        // measured instead of the lock.
        let postgres: Arc<dyn SessionStore> = Arc::new(
            crate::session_store::postgres::PostgresSessionStore::connect_with_pool_size(
                &scratch.url,
                40,
            )
            .await
            .expect("postgres"),
        );
        let mut rates = Vec::new();
        for writers in WRITER_COUNTS {
            rates.push(throughput(Arc::clone(&postgres), writers).await);
        }
        report.push(("postgres", rates));
    }

    eprintln!("concurrent writer scaling, {APPENDS_PER_WRITER} appends per writer:");
    for (name, rates) in &report {
        let baseline = rates[0];
        let cells: Vec<String> = WRITER_COUNTS
            .iter()
            .zip(rates)
            .map(|(writers, rate)| format!("{writers}w {rate:8.1}/s ({:.2}x)", rate / baseline))
            .collect();
        eprintln!("  {name:<9} {}", cells.join("   "));
    }

    if let Some(scratch) = scratch {
        let postgres_rates = &report[1].1;
        let scaling = postgres_rates[postgres_rates.len() - 1] / postgres_rates[0];
        assert!(
            scaling > 1.5,
            "postgres must gain throughput from concurrent writers on distinct \
             sessions, got {scaling:.2}x at {} writers",
            WRITER_COUNTS[WRITER_COUNTS.len() - 1]
        );
        scratch.discard().await;
    }
}
