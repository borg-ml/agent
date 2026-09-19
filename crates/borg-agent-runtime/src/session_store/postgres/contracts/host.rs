//! What the journal owes a host: launch ownership, settlement, and cursors.
//!
//! These contracts are about the boundary between a host and the journal it
//! uploads -- who is allowed to admit a launch, what a terminal session leaves
//! behind, and which events a cursor may still hide. They are written against
//! `SessionStore`, not against the SQL underneath it, so they keep holding when
//! the queries change.
//!
//! The runtime-manifest and harness-state cases live here too, because what
//! they actually assert is the same thing: state written by one worker has to
//! be there for the next one, and a restart has to be visible as a restart.
//!
//! Where the originals reopened a SQLite file, these open a second store on the
//! same scratch database. That is the closer equivalent than it looks: a new
//! pool and a new set of prepared statements, carrying nothing of the first
//! store's in-memory state.

use std::time::Duration;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::support::{message, store};
use crate::session_store::RuntimeManifestStatus;
use crate::session_store::postgres::PostgresSessionStore;
use crate::session_store::postgres::testing::ScratchDatabase;
use crate::{
    SessionAction, SessionActionState, SessionEvent, SessionEventKind, SessionStatus, SessionStore,
    SessionWorkspaceBinding,
};

/// Reopen the journal as a fresh store on the same database.
///
/// The pool size matches the one the test harness uses: the suite runs many
/// stores against one server, and a reopen is a second store, not a second
/// server's worth of connections.
async fn reopen(scratch: &ScratchDatabase) -> PostgresSessionStore {
    PostgresSessionStore::connect_with_pool_size(&scratch.url, 4)
        .await
        .unwrap()
}

#[tokio::test]
async fn host_launch_owner_is_atomic_immutable_and_scoped_across_reopen() {
    let (scratch, store) = store().await;
    let id = Uuid::new_v4();
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    let origin = "https://relay.invalid";
    let metadata = serde_json::json!({"request_id": id});
    // SQLite injected this failure with `raise(abort, ...)` in a trigger body.
    // Postgres needs the raise to live in a function; the effect on the caller
    // is the same -- the owner insert fails and takes the launch with it.
    sqlx::raw_sql(
        "create function reject_owner() returns trigger language plpgsql as $$ \
         begin raise exception 'injected owner write failure'; end; $$; \
         create trigger reject_owner before insert on host_launch_owners \
         for each row execute function reject_owner();",
    )
    .execute(store.pool())
    .await
    .unwrap();
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
    sqlx::raw_sql("drop trigger reject_owner on host_launch_owners; drop function reject_owner();")
        .execute(store.pool())
        .await
        .unwrap();
    let bound = Uuid::new_v4();
    store.create_session(bound).await.unwrap();
    let binding = store.workspace_binding(bound).await.unwrap().unwrap();
    store
        .attach_workspace(SessionWorkspaceBinding {
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
    sqlx::query("update host_launches set created_at = $1")
        .bind("2026-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap())
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
    let store = reopen(&scratch).await;
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
    scratch.discard().await;
}

#[tokio::test]
async fn terminal_host_settlement_cancels_abandoned_actions_and_fences_old_leases() {
    let (scratch, store) = store().await;
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
    let reopened = reopen(&scratch).await;
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
    scratch.discard().await;
}

#[tokio::test]
async fn host_journal_cursors_preserve_late_events_live_state_and_pagination() {
    let (scratch, store) = store().await;
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
    let store = reopen(&scratch).await;
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
    scratch.discard().await;
}

#[tokio::test]
async fn runtime_manifest_and_checkpoint_survive_store_reopen_and_detect_worker_restart() {
    let (scratch, store) = store().await;
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
    let reopened = reopen(&scratch).await;
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
    scratch.discard().await;
}

#[tokio::test]
async fn harness_state_is_durable_and_rolls_back_without_polluting_runtime_checkpoints() {
    let (scratch, store) = store().await;
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
    let reopened = reopen(&scratch).await;
    assert_eq!(
        reopened.load_harness_state(session_id).await.unwrap(),
        Some(first)
    );
    scratch.discard().await;
}
