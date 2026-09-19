//! The suite written against the `SessionStore` trait rather than a store.
//!
//! These tests exercise the trait's contract, not one implementation's
//! internals, so they stay honest about what a store must do rather than what
//! the current one happens to do. They are kept in the looping shape they were
//! written in: adding a second store means adding it to `harnesses`, and every
//! test then covers it without being rewritten.
//!
//! A server is required. See `postgres::testing` for why these do not skip.

use std::sync::Arc;

use uuid::Uuid;

use super::postgres::testing::{self, ScratchDatabase};
use crate::session_action::{
    ActionDeliveryPolicy, ActionWakePolicy, SessionAction, SessionActionKind, SessionActionState,
};
use crate::session_store::{RecoveryParts, SessionStore};
use crate::{
    EventActor, MessageStatus, PromptDelivery, SessionEvent, SessionEventKind, SessionStatus,
};

/// One backend under test, with whatever it needs to clean up afterwards.
struct Harness {
    name: &'static str,
    store: Arc<dyn SessionStore>,
    scratch: ScratchDatabase,
}

impl Harness {
    async fn discard(self) {
        self.scratch.discard().await;
    }
}

/// Every backend under test.
async fn harnesses() -> Vec<Harness> {
    let (scratch, postgres) = testing::session_store().await;
    vec![Harness {
        name: "postgres",
        store: Arc::new(postgres),
        scratch,
    }]
}

async fn started_session(store: &dyn SessionStore) -> Uuid {
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
    session_id
}

fn user_prompt(session_id: Uuid, message_id: Uuid, text: &str) -> SessionEvent {
    SessionEvent::new(
        session_id,
        0,
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: text.to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Queue),
        },
    )
}

fn assistant_reply(session_id: Uuid, text: &str) -> SessionEvent {
    SessionEvent::new(
        session_id,
        0,
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::Assistant,
            text: text.to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
    )
}

fn message_ids(events: &[SessionEvent]) -> Vec<Uuid> {
    events
        .iter()
        .filter_map(|event| match &event.kind {
            SessionEventKind::Message { message_id, .. } => Some(*message_id),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn sequences_are_allocated_contiguously_from_one() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let name = harness.name;
        let session_id = started_session(store.as_ref()).await;
        let first = store
            .append(assistant_reply(session_id, "one"))
            .await
            .expect("append");
        let second = store
            .append(assistant_reply(session_id, "two"))
            .await
            .expect("append");
        assert_eq!((first.sequence, second.sequence), (2, 3), "[{name}]");

        let events = store.read(session_id).await.expect("read");
        assert_eq!(events.len(), 3, "[{name}]");
        assert_eq!(
            events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3],
            "[{name}]"
        );
        assert_eq!(
            store
                .events_after(session_id, 1, 10)
                .await
                .expect("after")
                .len(),
            2,
            "[{name}]"
        );

        // A caller-supplied sequence must match the allocator exactly.
        let mut stale = assistant_reply(session_id, "from the past");
        stale.sequence = 2;
        assert!(store.append(stale).await.is_err(), "[{name}]");
        harness.discard().await;
    }
}

#[tokio::test]
async fn persistence_classes_agree_across_backends() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let name = harness.name;
        let session_id = started_session(store.as_ref()).await;
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Running,
                    detail: None,
                },
            ))
            .await
            .expect("running");

        // Coalesced live frames collapse into one row and never take a
        // sequence; durable events do.
        for text in ["think", "thinking", "thinking hard"] {
            store
                .append(SessionEvent::new(
                    session_id,
                    0,
                    SessionEventKind::ReasoningDelta {
                        text: text.to_string(),
                    },
                ))
                .await
                .expect("live");
        }
        let live = store
            .live_events_after(session_id, 0)
            .await
            .expect("live events");
        assert_eq!(live.len(), 1, "[{name}] live frames must coalesce");
        let SessionEventKind::ReasoningDelta { text } = &live[0].event.kind else {
            panic!("[{name}] reasoning delta expected");
        };
        assert_eq!(text, "thinking hard", "[{name}]");
        assert_eq!(
            store.read(session_id).await.expect("read").len(),
            2,
            "[{name}] live frames must not enter the durable sequence"
        );
        assert!(
            store
                .live_events_after(session_id, live[0].revision)
                .await
                .expect("live")
                .is_empty(),
            "[{name}] a cursor at the newest revision sees nothing new"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn a_durable_prompt_becomes_an_action_and_completes_with_its_turn() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let name = harness.name;
        let session_id = started_session(store.as_ref()).await;
        let message_id = Uuid::new_v4();
        store
            .append(user_prompt(session_id, message_id, "do the thing"))
            .await
            .expect("prompt");

        let action = store
            .action(session_id, message_id)
            .await
            .expect("action")
            .unwrap_or_else(|| panic!("[{name}] a durable prompt must create its action"));
        assert_eq!(action.state, SessionActionState::Admitted, "[{name}]");
        assert_eq!(action.kind, SessionActionKind::Prompt, "[{name}]");
        assert!(
            store
                .contains_message(session_id, message_id)
                .await
                .unwrap()
        );

        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::TurnCompleted {
                    message_id,
                    provider_session_id: Some("provider-1".to_string()),
                    final_text: "done".to_string(),
                    error: None,
                },
            ))
            .await
            .expect("turn completed");
        assert_eq!(
            store
                .action(session_id, message_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            SessionActionState::Completed,
            "[{name}]"
        );
        assert!(
            store
                .pending_actions(session_id, 10)
                .await
                .unwrap()
                .is_empty(),
            "[{name}] completed work is not pending"
        );

        // Every intermediate boundary is audited, not skipped.
        let states: Vec<SessionActionState> = store
            .action_transitions(session_id, message_id)
            .await
            .expect("audit")
            .into_iter()
            .map(|transition| transition.to)
            .collect();
        assert_eq!(
            states,
            vec![
                SessionActionState::Queued,
                SessionActionState::Admitted,
                SessionActionState::Delivered,
                SessionActionState::Preparing,
                SessionActionState::Committing,
                SessionActionState::Running,
                SessionActionState::Completed,
            ],
            "[{name}]"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn the_action_lifecycle_refuses_illegal_edges_on_both_backends() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let name = harness.name;
        let session_id = started_session(store.as_ref()).await;
        let action = SessionAction::new(
            Uuid::new_v4(),
            session_id,
            SessionActionKind::Prompt,
            ActionDeliveryPolicy::NextTurnBoundary,
            ActionWakePolicy::OnLowerBoundary,
            serde_json::json!({"text": "queued work"}),
        );
        let action_id = action.action_id;
        let stored = store.enqueue_action(action.clone()).await.expect("enqueue");
        assert_eq!(stored.state, SessionActionState::Queued, "[{name}]");

        // Enqueue is idempotent by id, but immutable fields may not change.
        assert_eq!(
            store
                .enqueue_action(action.clone())
                .await
                .expect("re-enqueue")
                .action_id,
            action_id,
            "[{name}]"
        );
        let mut mutated = action;
        mutated.payload = serde_json::json!({"text": "different work"});
        assert!(store.enqueue_action(mutated).await.is_err(), "[{name}]");

        // Queued -> Completed is not a legal edge.
        assert!(
            store
                .transition_action(
                    session_id,
                    action_id,
                    Some(SessionActionState::Queued),
                    SessionActionState::Completed,
                    None
                )
                .await
                .is_err(),
            "[{name}] a lifecycle jump must be refused"
        );
        store
            .transition_action(
                session_id,
                action_id,
                None,
                SessionActionState::Admitted,
                None,
            )
            .await
            .expect("admit");
        // A stale expected-state must not apply.
        assert!(
            store
                .transition_action(
                    session_id,
                    action_id,
                    Some(SessionActionState::Queued),
                    SessionActionState::Delivered,
                    None
                )
                .await
                .is_err(),
            "[{name}]"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn a_lease_is_exclusive_and_token_fenced() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let name = harness.name;
        let session_id = started_session(store.as_ref()).await;
        let action_id = store
            .enqueue_action(SessionAction::new(
                Uuid::new_v4(),
                session_id,
                SessionActionKind::Prompt,
                ActionDeliveryPolicy::NextTurnBoundary,
                ActionWakePolicy::OnLowerBoundary,
                serde_json::json!({"text": "leased"}),
            ))
            .await
            .expect("enqueue")
            .action_id;

        let lease = std::time::Duration::from_secs(3_600);
        let claimed = store
            .claim_action(session_id, action_id, "worker-a", lease)
            .await
            .expect("claim")
            .unwrap_or_else(|| panic!("[{name}] first claim must succeed"));
        assert!(
            store
                .claim_action(session_id, action_id, "worker-b", lease)
                .await
                .expect("claim")
                .is_none(),
            "[{name}] a live lease excludes other workers"
        );
        // Re-claiming your own live lease is a no-op, not an error.
        assert_eq!(
            store
                .claim_action(session_id, action_id, "worker-a", lease)
                .await
                .expect("claim")
                .expect("own lease")
                .lease_token,
            claimed.lease_token,
            "[{name}]"
        );
        // A heartbeat with the wrong token must not extend the lease.
        assert!(
            store
                .heartbeat_action(session_id, action_id, "worker-a", Uuid::new_v4(), lease)
                .await
                .is_err(),
            "[{name}]"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn clearing_context_is_a_replay_boundary_that_spares_pending_work() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let name = harness.name;
        let session_id = started_session(store.as_ref()).await;

        let answered = Uuid::new_v4();
        store
            .append(user_prompt(session_id, answered, "old question"))
            .await
            .expect("prompt");
        store
            .append(assistant_reply(session_id, "ancient history"))
            .await
            .expect("reply");
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::TurnCompleted {
                    message_id: answered,
                    provider_session_id: Some("provider-1".to_string()),
                    final_text: "ok".to_string(),
                    error: None,
                },
            ))
            .await
            .expect("turn completed");
        let unresolved = Uuid::new_v4();
        store
            .append(user_prompt(session_id, unresolved, "never ran"))
            .await
            .expect("prompt");
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ContextCleared,
            ))
            .await
            .expect("context cleared");

        let recovery = store.recovery(session_id).await.expect("recovery");
        let context_text: Vec<String> = recovery
            .context_events
            .iter()
            .filter_map(|event| match &event.kind {
                SessionEventKind::Message { text, .. } => Some(text.clone()),
                _ => None,
            })
            .collect();
        assert!(
            !context_text.iter().any(|text| text == "ancient history"),
            "[{name}] a boundary must drop superseded context: {context_text:?}"
        );
        let queued = message_ids(&recovery.queue_events);
        assert!(
            queued.contains(&unresolved),
            "[{name}] an unresolved prompt must survive the boundary"
        );
        assert!(
            !queued.contains(&answered),
            "[{name}] a completed prompt must not be replayed"
        );

        // Narrowing changes which slices are populated, never their contents.
        let queue_only = store
            .recovery_parts(session_id, RecoveryParts::QUEUE)
            .await
            .expect("queue recovery");
        assert!(queue_only.context_events.is_empty(), "[{name}]");
        assert_eq!(
            message_ids(&queue_only.queue_events),
            queued,
            "[{name}] a narrowed slice must match the full projection"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn a_fork_inherits_renumbers_and_diverges_identically() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let name = harness.name;
        let parent = started_session(store.as_ref()).await;
        for index in 0..4 {
            store
                .append(assistant_reply(parent, &format!("reply {index}")))
                .await
                .expect("reply");
        }
        let parent_events = store.read(parent).await.expect("read");
        let cut_at = parent_events.last().expect("events").sequence;

        let child = Uuid::new_v4();
        let fork = store
            .fork_before(parent, child, cut_at)
            .await
            .expect("fork");
        assert_eq!(fork.parent_cut_sequence, cut_at - 1, "[{name}]");

        let inherited = store.read(child).await.expect("read fork");
        let texts: Vec<String> = inherited
            .iter()
            .filter_map(|event| match &event.kind {
                SessionEventKind::Message { text, .. } => Some(text.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(texts, vec!["reply 0", "reply 1", "reply 2"], "[{name}]");
        assert_eq!(
            inherited
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            (1..=inherited.len() as u64).collect::<Vec<_>>(),
            "[{name}] an inherited prefix must be contiguous"
        );
        assert!(
            inherited.iter().all(|event| event.session_id == child),
            "[{name}] inherited events belong to the child"
        );
        let parent_ids: Vec<Uuid> = parent_events.iter().map(|event| event.id).collect();
        assert!(
            inherited
                .iter()
                .all(|event| !parent_ids.contains(&event.id)),
            "[{name}] an inherited event must not reuse the parent's id"
        );

        store
            .append(assistant_reply(child, "a different path"))
            .await
            .expect("append to fork");
        // The parent keeps the event the fork cut away: a fork is a branch,
        // not a rewrite.
        let parent_texts: Vec<String> = store
            .read(parent)
            .await
            .expect("read parent")
            .iter()
            .filter_map(|event| match &event.kind {
                SessionEventKind::Message { text, .. } => Some(text.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            parent_texts,
            vec!["reply 0", "reply 1", "reply 2", "reply 3"],
            "[{name}]"
        );
        assert_eq!(
            store.inherited_event_count(child).await.unwrap(),
            fork.inherited_event_count,
            "[{name}]"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn an_oversized_tool_output_is_deferred_and_reloadable() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let name = harness.name;
        let session_id = started_session(store.as_ref()).await;
        let output = "x".repeat(super::INLINE_SESSION_PAYLOAD_BYTES + 4_096);
        let stored = store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ToolCompleted {
                    tool_call_id: "call-1".to_string(),
                    output: output.clone(),
                    output_ref: None,
                    is_error: false,
                    input: None,
                    input_ref: None,
                },
            ))
            .await
            .expect("tool completed");

        let SessionEventKind::ToolCompleted {
            output: stored_output,
            output_ref,
            ..
        } = &stored.kind
        else {
            panic!("[{name}] tool completion expected");
        };
        let reference = output_ref
            .clone()
            .unwrap_or_else(|| panic!("[{name}] an oversized output must be offloaded"));
        assert!(
            stored_output.len() < output.len(),
            "[{name}] the inline body must shrink"
        );
        assert_eq!(reference.byte_len, output.len() as u64, "[{name}]");

        let loaded = store.load_payload(&reference).await.expect("load payload");
        assert_eq!(
            String::from_utf8(loaded).expect("utf8"),
            output,
            "[{name}] a deferred payload must reload byte for byte"
        );
        harness.discard().await;
    }
}

#[tokio::test]
async fn sessions_are_listed_newest_first_without_children() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let name = harness.name;
        let first = started_session(store.as_ref()).await;
        let second = started_session(store.as_ref()).await;
        let child = Uuid::new_v4();
        store
            .register_child_session(second, child)
            .await
            .expect("register child");

        let listed: Vec<Uuid> = store
            .list_sessions(50)
            .await
            .expect("list")
            .into_iter()
            .map(|summary| summary.session_id)
            .collect();
        assert!(
            listed.contains(&first) && listed.contains(&second),
            "[{name}]"
        );
        assert!(
            !listed.contains(&child),
            "[{name}] a child session is not a root session"
        );
        harness.discard().await;
    }
}

/// A manifest is claimable, and a second worker taking it over is reported.
///
/// Takeover is how a session recovers from a worker that died without stopping
/// cleanly, so the flag has to be true for a NEW worker and false for the same
/// one reconnecting -- a backend that always reported takeover would make every
/// reconnect look like a crash.
#[tokio::test]
async fn runtime_manifest_activation_reports_worker_takeover() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let session_id = started_session(store.as_ref()).await;
        let first_worker = Uuid::new_v4();

        let activation = store
            .activate_runtime_manifest(session_id, "bun", "/tmp/root", "bun run x", first_worker)
            .await
            .unwrap_or_else(|error| panic!("{}: activate: {error:#}", harness.name));
        assert!(
            !activation.recovered_from_previous_worker,
            "{}: a first activation is not a takeover",
            harness.name
        );
        assert_eq!(
            activation.manifest.worker_id, first_worker,
            "{}",
            harness.name
        );

        let same_again = store
            .activate_runtime_manifest(session_id, "bun", "/tmp/root", "bun run x", first_worker)
            .await
            .unwrap_or_else(|error| panic!("{}: reactivate: {error:#}", harness.name));
        assert!(
            !same_again.recovered_from_previous_worker,
            "{}: the same worker reconnecting is not a takeover",
            harness.name
        );

        let second_worker = Uuid::new_v4();
        let taken_over = store
            .activate_runtime_manifest(session_id, "bun", "/tmp/root", "bun run y", second_worker)
            .await
            .unwrap_or_else(|error| panic!("{}: takeover: {error:#}", harness.name));
        assert!(
            taken_over.recovered_from_previous_worker,
            "{}: a new worker claiming the manifest is a takeover",
            harness.name
        );

        // The manifest is bound to its runtime and root for life; rebinding
        // would aim a live session's checkpoints at a different tree.
        let wrong_runtime = store
            .activate_runtime_manifest(session_id, "python", "/tmp/root", "x", second_worker)
            .await;
        assert!(
            wrong_runtime.is_err(),
            "{}: a manifest must not change runtime",
            harness.name
        );
        let wrong_root = store
            .activate_runtime_manifest(session_id, "bun", "/other", "x", second_worker)
            .await;
        assert!(
            wrong_root.is_err(),
            "{}: a manifest must not change root",
            harness.name
        );

        harness.discard().await;
    }
}

/// A superseded worker cannot keep writing to a manifest it no longer owns.
///
/// This is the fence that makes takeover safe. Without it a process that was
/// replaced -- but has not noticed yet -- would keep recording executions and
/// could stop the runtime its successor is driving.
#[tokio::test]
async fn a_superseded_worker_is_fenced_out_of_its_manifest() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let session_id = started_session(store.as_ref()).await;
        let old_worker = Uuid::new_v4();
        let new_worker = Uuid::new_v4();
        store
            .activate_runtime_manifest(session_id, "bun", "/tmp/root", "run", old_worker)
            .await
            .unwrap_or_else(|error| panic!("{}: activate: {error:#}", harness.name));

        store
            .record_runtime_execution(session_id, old_worker, "sha256:aaa", false, None)
            .await
            .unwrap_or_else(|error| panic!("{}: owned execution: {error:#}", harness.name));

        store
            .activate_runtime_manifest(session_id, "bun", "/tmp/root", "run", new_worker)
            .await
            .unwrap_or_else(|error| panic!("{}: takeover: {error:#}", harness.name));

        assert!(
            store
                .record_runtime_execution(session_id, old_worker, "sha256:bbb", false, None)
                .await
                .is_err(),
            "{}: a superseded worker must not record executions",
            harness.name
        );
        assert!(
            store
                .stop_runtime_manifest(session_id, old_worker)
                .await
                .is_err(),
            "{}: a superseded worker must not stop the runtime",
            harness.name
        );

        let manifest = store
            .runtime_manifest(session_id)
            .await
            .unwrap_or_else(|error| panic!("{}: read: {error:#}", harness.name))
            .unwrap_or_else(|| panic!("{}: manifest must exist", harness.name));
        assert_eq!(manifest.worker_id, new_worker, "{}", harness.name);
        assert_eq!(
            manifest.execution_count, 1,
            "{}: only the owned execution counted",
            harness.name
        );

        harness.discard().await;
    }
}

/// A checkpoint key names one immutable state.
///
/// Re-saving identical content returns the existing row; re-saving DIFFERENT
/// content under the same key is an error rather than an overwrite, because a
/// replay must reproduce the runtime that was actually recorded.
#[tokio::test]
async fn runtime_checkpoints_are_immutable_per_key() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let session_id = started_session(store.as_ref()).await;
        let worker_id = Uuid::new_v4();
        store
            .activate_runtime_manifest(session_id, "bun", "/tmp/root", "run", worker_id)
            .await
            .unwrap_or_else(|error| panic!("{}: activate: {error:#}", harness.name));

        let state = serde_json::json!({"step": 1, "cursor": "abc"});
        let saved = store
            .save_runtime_checkpoint(session_id, worker_id, "step-1", &state)
            .await
            .unwrap_or_else(|error| panic!("{}: save: {error:#}", harness.name));
        assert_eq!(saved.state, state, "{}", harness.name);

        let again = store
            .save_runtime_checkpoint(session_id, worker_id, "step-1", &state)
            .await
            .unwrap_or_else(|error| panic!("{}: re-save: {error:#}", harness.name));
        assert_eq!(
            again.revision, saved.revision,
            "{}: an identical re-save is idempotent",
            harness.name
        );
        assert_eq!(again.content_hash, saved.content_hash, "{}", harness.name);

        assert!(
            store
                .save_runtime_checkpoint(
                    session_id,
                    worker_id,
                    "step-1",
                    &serde_json::json!({"step": 2})
                )
                .await
                .is_err(),
            "{}: a key must not silently change content",
            harness.name
        );

        // A non-owner cannot write checkpoints at all.
        assert!(
            store
                .save_runtime_checkpoint(session_id, Uuid::new_v4(), "step-2", &state)
                .await
                .is_err(),
            "{}: only the owning worker may checkpoint",
            harness.name
        );

        let fetched = store
            .runtime_checkpoint(session_id, Some("step-1"))
            .await
            .unwrap_or_else(|error| panic!("{}: fetch: {error:#}", harness.name))
            .unwrap_or_else(|| panic!("{}: checkpoint must exist", harness.name));
        assert_eq!(fetched.state, state, "{}", harness.name);

        harness.discard().await;
    }
}

/// Harness state is a bounded history that rollback walks backwards.
///
/// The last assertion is the one that matters: rolling back must not merely
/// uncover an older revision, it must re-append it as the newest, so that the
/// rollback is itself recorded and reversible.
#[tokio::test]
async fn harness_state_rolls_back_through_its_history() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let session_id = started_session(store.as_ref()).await;

        assert!(
            store
                .load_harness_state(session_id)
                .await
                .unwrap_or_else(|error| panic!("{}: empty load: {error:#}", harness.name))
                .is_none(),
            "{}: a fresh session has no harness state",
            harness.name
        );

        for step in 1..=3 {
            store
                .save_harness_state(session_id, &serde_json::json!({"step": step}))
                .await
                .unwrap_or_else(|error| panic!("{}: save {step}: {error:#}", harness.name));
        }
        let latest = store
            .load_harness_state(session_id)
            .await
            .unwrap_or_else(|error| panic!("{}: load: {error:#}", harness.name))
            .unwrap_or_else(|| panic!("{}: state must exist", harness.name));
        assert_eq!(latest, serde_json::json!({"step": 3}), "{}", harness.name);

        let rolled = store
            .rollback_harness_state(session_id, 1)
            .await
            .unwrap_or_else(|error| panic!("{}: rollback: {error:#}", harness.name));
        assert_eq!(rolled, serde_json::json!({"step": 2}), "{}", harness.name);
        let after = store
            .load_harness_state(session_id)
            .await
            .unwrap_or_else(|error| panic!("{}: reload: {error:#}", harness.name))
            .unwrap_or_else(|| panic!("{}: state must exist", harness.name));
        assert_eq!(
            after,
            serde_json::json!({"step": 2}),
            "{}: rollback re-appends the target as the newest state",
            harness.name
        );

        assert!(
            store.rollback_harness_state(session_id, 0).await.is_err(),
            "{}: rollback of zero steps is rejected",
            harness.name
        );
        assert!(
            store.rollback_harness_state(session_id, 13).await.is_err(),
            "{}: rollback beyond the retained history is rejected",
            harness.name
        );

        harness.discard().await;
    }
}

/// Harness state and runtime checkpoints share a table but not a namespace.
///
/// They are stored together and both allocate from one revision counter, so the
/// only thing separating them is a reserved key prefix. If a checkpoint listing
/// leaked harness revisions, every session would appear to have checkpoints it
/// never saved.
#[tokio::test]
async fn harness_state_is_invisible_to_checkpoint_listings() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let session_id = started_session(store.as_ref()).await;
        let worker_id = Uuid::new_v4();
        store
            .activate_runtime_manifest(session_id, "bun", "/tmp/root", "run", worker_id)
            .await
            .unwrap_or_else(|error| panic!("{}: activate: {error:#}", harness.name));
        store
            .save_harness_state(session_id, &serde_json::json!({"harness": true}))
            .await
            .unwrap_or_else(|error| panic!("{}: harness save: {error:#}", harness.name));
        store
            .save_runtime_checkpoint(
                session_id,
                worker_id,
                "only-one",
                &serde_json::json!({"n": 1}),
            )
            .await
            .unwrap_or_else(|error| panic!("{}: checkpoint: {error:#}", harness.name));

        let listed = store
            .list_runtime_checkpoints(session_id, 50)
            .await
            .unwrap_or_else(|error| panic!("{}: list: {error:#}", harness.name));
        let keys: Vec<&str> = listed.iter().map(|entry| entry.key.as_str()).collect();
        assert_eq!(keys, vec!["only-one"], "{}: {keys:?}", harness.name);

        let newest = store
            .runtime_checkpoint(session_id, None)
            .await
            .unwrap_or_else(|error| panic!("{}: newest: {error:#}", harness.name))
            .unwrap_or_else(|| panic!("{}: a checkpoint must exist", harness.name));
        assert_eq!(newest.key, "only-one", "{}", harness.name);

        // The reserved prefix is not a writable checkpoint key.
        assert!(
            store
                .save_runtime_checkpoint(
                    session_id,
                    worker_id,
                    &format!("{}x", crate::session_store::HARNESS_CHECKPOINT_PREFIX),
                    &serde_json::json!({"n": 2})
                )
                .await
                .is_err(),
            "{}: the harness prefix is reserved",
            harness.name
        );

        harness.discard().await;
    }
}

/// A workflow id admits exactly once, and its action is created already
/// running.
///
/// Both halves matter. Double admission would make replay show one workflow
/// starting twice; an action left `Queued` would be picked up by the
/// pending-action sweep as undelivered work and woken a second time.
#[tokio::test]
async fn a_workflow_admits_once_and_starts_running() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let session_id = started_session(store.as_ref()).await;
        let workflow_id = Uuid::new_v4();
        // The action payload must mirror the Started event's fields: appending
        // that event derives an action from them, and a mismatch is rejected as
        // an id reused with different immutable content. The two are one fact
        // recorded twice, so the suite states the shape rather than inventing
        // an arbitrary one that would only pass in isolation.
        let payload = serde_json::json!({
            "workflow_id": workflow_id,
            "source_hash": "sha256:probe",
            "name": "probe",
        });

        let action = store
            .ensure_workflow_action(session_id, workflow_id, &payload)
            .await
            .unwrap_or_else(|error| panic!("{}: action: {error:#}", harness.name));
        assert_eq!(action.action_id, workflow_id, "{}", harness.name);
        assert_eq!(
            action.state,
            crate::SessionActionState::Running,
            "{}: a workflow action is created already running",
            harness.name
        );

        // Idempotent for an identical request...
        let again = store
            .ensure_workflow_action(session_id, workflow_id, &payload)
            .await
            .unwrap_or_else(|error| panic!("{}: re-action: {error:#}", harness.name));
        assert_eq!(again.action_id, action.action_id, "{}", harness.name);
        // ...and an error when the durable metadata would change.
        assert!(
            store
                .ensure_workflow_action(
                    session_id,
                    workflow_id,
                    &serde_json::json!({"name": "different"})
                )
                .await
                .is_err(),
            "{}: conflicting workflow metadata must be refused",
            harness.name
        );

        let started = |name: &str| {
            SessionEvent::new(
                session_id,
                0,
                SessionEventKind::BluWorkflowStarted {
                    workflow_id,
                    source_hash: "sha256:probe".to_string(),
                    name: name.to_string(),
                },
            )
        };
        let first = store
            .ensure_workflow_started(started("probe"), workflow_id)
            .await
            .unwrap_or_else(|error| panic!("{}: admit: {error:#}", harness.name));
        let second = store
            .ensure_workflow_started(started("probe"), workflow_id)
            .await
            .unwrap_or_else(|error| panic!("{}: re-admit: {error:#}", harness.name));
        assert_eq!(
            first.sequence, second.sequence,
            "{}: a workflow id admits exactly once",
            harness.name
        );

        // A different workflow id is genuinely a second admission.
        let other_id = Uuid::new_v4();
        let other = store
            .ensure_workflow_started(
                SessionEvent::new(
                    session_id,
                    0,
                    SessionEventKind::BluWorkflowStarted {
                        workflow_id: other_id,
                        source_hash: "sha256:other".to_string(),
                        name: "other".to_string(),
                    },
                ),
                other_id,
            )
            .await
            .unwrap_or_else(|error| panic!("{}: other admit: {error:#}", harness.name));
        assert_ne!(other.sequence, first.sequence, "{}", harness.name);

        // A non-Started event is not an admission record at all.
        assert!(
            store
                .ensure_workflow_started(
                    user_prompt(session_id, Uuid::new_v4(), "not a workflow"),
                    workflow_id
                )
                .await
                .is_err(),
            "{}: admission requires a Started event",
            harness.name
        );

        harness.discard().await;
    }
}

/// A superseded worker cannot publish over its replacement.
///
/// `append_with_action_lease` validates the lease under the action's row lock
/// in the same transaction that appends, so there is no window between "I still
/// own this" and "my event is committed". Without the fence, a worker paused
/// long enough to lose its lease could wake and journal a terminal event over
/// work its replacement had already begun.
#[tokio::test]
async fn a_workflow_event_is_fenced_on_its_action_lease() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let session_id = started_session(store.as_ref()).await;
        let workflow_id = Uuid::new_v4();
        store
            .ensure_workflow_action(
                session_id,
                workflow_id,
                &serde_json::json!({
                    "workflow_id": workflow_id,
                    "source_hash": "sha256:probe",
                    "name": "probe",
                }),
            )
            .await
            .unwrap_or_else(|error| panic!("{}: action: {error:#}", harness.name));

        let claimed = store
            .claim_action(
                session_id,
                workflow_id,
                "worker-a",
                std::time::Duration::from_secs(60),
            )
            .await
            .unwrap_or_else(|error| panic!("{}: claim: {error:#}", harness.name))
            .unwrap_or_else(|| panic!("{}: claim must succeed", harness.name));
        let lease_token = claimed
            .lease_token
            .unwrap_or_else(|| panic!("{}: a claimed action carries a fence token", harness.name));

        let event = || {
            SessionEvent::new(
                session_id,
                0,
                SessionEventKind::BluWorkflowStarted {
                    workflow_id,
                    source_hash: "sha256:probe".to_string(),
                    name: "probe".to_string(),
                },
            )
        };
        store
            .append_with_action_lease(event(), workflow_id, "worker-a", lease_token)
            .await
            .unwrap_or_else(|error| panic!("{}: owned append: {error:#}", harness.name));

        // Wrong owner and wrong token are both refused.
        assert!(
            store
                .append_with_action_lease(event(), workflow_id, "worker-b", lease_token)
                .await
                .is_err(),
            "{}: another owner must not append under this lease",
            harness.name
        );
        assert!(
            store
                .append_with_action_lease(event(), workflow_id, "worker-a", Uuid::new_v4())
                .await
                .is_err(),
            "{}: a stale fence token must not append",
            harness.name
        );

        harness.discard().await;
    }
}

/// The relay's pending-message sweep, which spans BOTH tiers.
///
/// This query joins the journal tier (host launches, workspace bindings,
/// cursors) to the workspace tier (members, events). In Postgres the journal
/// types ids as `uuid` while the satellite tables keep them `text`, so every
/// cross-tier predicate needs an explicit cast -- and a missing one is a
/// runtime error that no amount of compiling catches, because the SQL is not
/// checked until it runs. Driving it on both backends is the only thing that
/// proves the translation is faithful rather than merely syntactic.
#[tokio::test]
async fn the_relay_sweep_spans_the_journal_and_workspace_tiers() {
    use crate::workspace::{
        Participant, ParticipantKind, Workspace, WorkspaceMembership, WorkspaceRole,
    };

    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let name = harness.name;
        let workspace = store
            .workspace_store()
            .await
            .unwrap_or_else(|error| panic!("{name}: workspace tier: {error:#}"))
            .unwrap_or_else(|| panic!("{name}: workspace tier must exist"));

        let session_id = started_session(store.as_ref()).await;
        let host_id = Uuid::new_v4();
        // A session is already bound to its own workspace when it starts, so
        // the sweep must be exercised against THAT binding rather than a second
        // one -- re-attaching to a different workspace is refused, and rightly.
        // Only the owning host is added here.
        let existing = store
            .workspace_binding(session_id)
            .await
            .unwrap_or_else(|error| panic!("{name}: binding: {error:#}"))
            .unwrap_or_else(|| panic!("{name}: a started session is bound to a workspace"));
        let author = existing.participant_id;
        let workspace_id = existing.workspace_id;

        // A message needs a recipient other than its author, so the workspace
        // gets a second member. The sweep still keys off the BOUND participant:
        // it looks for messages this session's own participant authored.
        let listener = Uuid::new_v4();
        for (id, display) in [(author, "author"), (listener, "listener")] {
            workspace
                .create_participant(Participant {
                    id,
                    display_name: display.to_string(),
                    kind: ParticipantKind::Agent,
                    created_at: chrono::Utc::now(),
                })
                .await
                .unwrap_or_else(|error| panic!("{name}: participant: {error:#}"));
        }
        workspace
            .create_workspace(Workspace {
                id: workspace_id,
                name: "relay-sweep".to_string(),
                created_at: chrono::Utc::now(),
            })
            .await
            .unwrap_or_else(|error| panic!("{name}: workspace: {error:#}"));
        for id in [author, listener] {
            workspace
                .add_member(WorkspaceMembership {
                    workspace_id,
                    participant_id: id,
                    role: WorkspaceRole::Editor,
                    joined_at: chrono::Utc::now(),
                })
                .await
                .unwrap_or_else(|error| panic!("{name}: member: {error:#}"));
        }

        // The journal side: a launch this host owns, bound to that workspace.
        store
            .persist_host_launch_metadata(session_id, &serde_json::json!({"probe": true}))
            .await
            .unwrap_or_else(|error| panic!("{name}: launch: {error:#}"));
        store
            .attach_workspace(crate::SessionWorkspaceBinding {
                session_id,
                workspace_id,
                participant_id: author,
                host_id: Some(host_id),
                attached_at: chrono::Utc::now(),
            })
            .await
            .unwrap_or_else(|error| panic!("{name}: attach: {error:#}"));

        // Nothing has been said yet, so there is nothing to relay.
        let before = store
            .pending_host_workspace_messages(host_id, None, 16)
            .await
            .unwrap_or_else(|error| panic!("{name}: empty sweep: {error:#}"));
        assert!(before.is_empty(), "{name}: {before:?}");

        let receipt = workspace
            .append_message(crate::workspace::NewWorkspaceMessage {
                workspace_id,
                author_id: author,
                text: "relay me".to_string(),
                mentions: Vec::new(),
                audience: crate::workspace::Audience::Workspace,
                mode: crate::workspace::DeliveryMode::Notify,
                thread_id: None,
                reply_to_message_id: None,
                idempotency_key: "relay-sweep-1".to_string(),
            })
            .await
            .unwrap_or_else(|error| panic!("{name}: message: {error:#}"));

        let pending = store
            .pending_host_workspace_messages(host_id, None, 16)
            .await
            .unwrap_or_else(|error| panic!("{name}: sweep: {error:#}"));
        assert_eq!(pending, vec![session_id], "{name}: {pending:?}");

        // Another host sees nothing: the sweep is scoped by ownership.
        let other = store
            .pending_host_workspace_messages(Uuid::new_v4(), None, 16)
            .await
            .unwrap_or_else(|error| panic!("{name}: other host: {error:#}"));
        assert!(other.is_empty(), "{name}: {other:?}");

        // Acknowledging past the message clears it.
        store
            .acknowledge_host_workspaces(
                host_id,
                session_id,
                &std::collections::HashMap::from([(workspace_id, receipt.sequence)]),
            )
            .await
            .unwrap_or_else(|error| panic!("{name}: acknowledge: {error:#}"));
        let after = store
            .pending_host_workspace_messages(host_id, None, 16)
            .await
            .unwrap_or_else(|error| panic!("{name}: post-ack sweep: {error:#}"));
        assert!(
            after.is_empty(),
            "{name}: an acknowledged message is no longer pending, got {after:?}"
        );

        harness.discard().await;
    }
}

/// Build a session whose messages are the given (actor, text) pairs.
async fn session_with_messages(store: &dyn SessionStore, texts: &[(EventActor, &str)]) -> Uuid {
    let session_id = started_session(store).await;
    for (actor, text) in texts {
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: *actor,
                    text: (*text).to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ))
            .await
            .expect("message");
    }
    session_id
}

/// The message texts a page returned, in page order.
fn hit_texts(page: &crate::session_store::SessionHistoryPage) -> Vec<String> {
    page.hits
        .iter()
        .filter_map(|hit| match &hit.event.kind {
            SessionEventKind::Message { text, .. } => Some(text.clone()),
            _ => None,
        })
        .collect()
}

fn lexical(text: &str) -> crate::session_store::SessionHistoryQuery {
    crate::session_store::SessionHistoryQuery {
        text: Some(text.to_string()),
        ..Default::default()
    }
}

/// Lexical search selects events by what they contain, not by how they rank.
///
/// WHAT IS AND IS NOT COMPARED: relevance scores and snippet boundaries belong
/// to the search engine and are deliberately not asserted here, because pinning
/// them would freeze an implementation detail. What MUST hold is which events a
/// query selects; a caller that gets different evidence from a store is told a
/// different history, which is the failure this whole suite exists to catch.
#[tokio::test]
async fn lexical_search_selects_the_same_events_on_both_backends() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let name = harness.name;
        let session_id = session_with_messages(
            store.as_ref(),
            &[
                (EventActor::User, "please migrate the journal to postgres"),
                (EventActor::Assistant, "the postgres work is under way"),
                (EventActor::Assistant, "unrelated chatter about lunch"),
            ],
        )
        .await;

        let page = store
            .query_history(session_id, lexical("postgres"))
            .await
            .unwrap_or_else(|error| panic!("{name}: search: {error:#}"));
        let mut texts = hit_texts(&page);
        texts.sort();
        assert_eq!(
            texts,
            vec![
                "please migrate the journal to postgres".to_string(),
                "the postgres work is under way".to_string(),
            ],
            "{name}: a term selects exactly the events containing it"
        );
        // Both engines must produce SOME snippet for a text match, even though
        // the snippets themselves differ.
        assert!(
            page.hits.iter().all(|hit| hit.snippet.is_some()),
            "{name}: a lexical hit carries a snippet"
        );

        let absent = store
            .query_history(session_id, lexical("kubernetes"))
            .await
            .unwrap_or_else(|error| panic!("{name}: absent term: {error:#}"));
        assert!(
            absent.hits.is_empty(),
            "{name}: a term nobody said matches nothing, got {:?}",
            hit_texts(&absent)
        );

        harness.discard().await;
    }
}

/// Filters narrow a search identically on both engines.
///
/// These predicates are applied outside the text index -- by actor, by event
/// kind, by sequence range, by limit -- so unlike ranking they have one correct
/// answer and the backends must give it.
#[tokio::test]
async fn history_filters_narrow_identically_on_both_backends() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let name = harness.name;
        let session_id = session_with_messages(
            store.as_ref(),
            &[
                (EventActor::User, "deploy the shared cache"),
                (EventActor::Assistant, "deploy finished for the cache"),
                (EventActor::User, "roll back the cache"),
            ],
        )
        .await;

        // By actor.
        let page = store
            .query_history(
                session_id,
                crate::session_store::SessionHistoryQuery {
                    actors: vec![EventActor::User],
                    ..lexical("cache")
                },
            )
            .await
            .unwrap_or_else(|error| panic!("{name}: actor filter: {error:#}"));
        let mut texts = hit_texts(&page);
        texts.sort();
        assert_eq!(
            texts,
            vec![
                "deploy the shared cache".to_string(),
                "roll back the cache".to_string()
            ],
            "{name}: an actor filter excludes the other actor's events"
        );

        // By limit, newest first: the same bound must select the same event.
        let newest = store
            .query_history(
                session_id,
                crate::session_store::SessionHistoryQuery {
                    newest_first: true,
                    limit: Some(1),
                    ..lexical("cache")
                },
            )
            .await
            .unwrap_or_else(|error| panic!("{name}: limit: {error:#}"));
        assert_eq!(
            hit_texts(&newest),
            vec!["roll back the cache".to_string()],
            "{name}: newest_first with limit 1 returns the latest match"
        );

        // By event kind: no message survives a filter for a kind they are not.
        let wrong_kind = store
            .query_history(
                session_id,
                crate::session_store::SessionHistoryQuery {
                    event_kinds: vec!["session_started".to_string()],
                    ..lexical("cache")
                },
            )
            .await
            .unwrap_or_else(|error| panic!("{name}: kind filter: {error:#}"));
        assert!(
            wrong_kind.hits.is_empty(),
            "{name}: a kind filter excludes other kinds, got {:?}",
            hit_texts(&wrong_kind)
        );

        harness.discard().await;
    }
}

/// Regex search agrees exactly, including case sensitivity.
///
/// Unlike lexical search this has no engine-specific index behind it: both
/// backends narrow candidates and then run the SAME Rust regex over the event
/// body. Any disagreement here is a bug in candidate selection, not a
/// difference of opinion between two search engines, so this is asserted
/// exactly.
#[tokio::test]
async fn regex_search_agrees_exactly_including_case() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let name = harness.name;
        let session_id = session_with_messages(
            store.as_ref(),
            &[
                (EventActor::User, "failure code E4041 in the gateway"),
                (EventActor::Assistant, "retrying after code e4041"),
                (EventActor::Assistant, "no codes here"),
            ],
        )
        .await;

        let regex = |case_sensitive: bool| crate::session_store::SessionHistoryQuery {
            text: Some(r"code E\d{4}".to_string()),
            mode: crate::session_store::SessionHistorySearchMode::Regex,
            case_sensitive,
            ..Default::default()
        };

        let sensitive = store
            .query_history(session_id, regex(true))
            .await
            .unwrap_or_else(|error| panic!("{name}: regex: {error:#}"));
        assert_eq!(
            hit_texts(&sensitive),
            vec!["failure code E4041 in the gateway".to_string()],
            "{name}: a case-sensitive regex matches only the exact case"
        );

        let insensitive = store
            .query_history(session_id, regex(false))
            .await
            .unwrap_or_else(|error| panic!("{name}: regex insensitive: {error:#}"));
        let mut texts = hit_texts(&insensitive);
        texts.sort();
        assert_eq!(
            texts,
            vec![
                "failure code E4041 in the gateway".to_string(),
                "retrying after code e4041".to_string(),
            ],
            "{name}: a case-insensitive regex matches both cases"
        );

        harness.discard().await;
    }
}

/// An `event_id` lookup is exact, and an empty query is a plain range read.
///
/// Neither path consults the text index, so both are pure agreement checks:
/// resolving a hit back to canonical evidence must return that one event, and
/// an untexted query must return history rather than nothing.
#[tokio::test]
async fn event_lookup_and_untexted_reads_agree() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let name = harness.name;
        let session_id = session_with_messages(
            store.as_ref(),
            &[
                (EventActor::User, "first thing"),
                (EventActor::Assistant, "second thing"),
            ],
        )
        .await;

        let all = store
            .query_history(
                session_id,
                crate::session_store::SessionHistoryQuery::default(),
            )
            .await
            .unwrap_or_else(|error| panic!("{name}: untexted: {error:#}"));
        assert_eq!(
            hit_texts(&all),
            vec!["first thing".to_string(), "second thing".to_string()],
            "{name}: an untexted query reads history in order"
        );

        let target = all
            .hits
            .iter()
            .find(|hit| matches!(&hit.event.kind, SessionEventKind::Message { text, .. } if text == "second thing"))
            .unwrap_or_else(|| panic!("{name}: fixture event must exist"))
            .event
            .clone();

        let resolved = store
            .query_history(
                session_id,
                crate::session_store::SessionHistoryQuery {
                    event_id: Some(target.id),
                    ..Default::default()
                },
            )
            .await
            .unwrap_or_else(|error| panic!("{name}: by id: {error:#}"));
        assert_eq!(
            resolved.hits.len(),
            1,
            "{name}: an event id resolves to exactly one event"
        );
        assert_eq!(resolved.hits[0].event.id, target.id, "{name}");
        assert_eq!(
            resolved.hits[0].event.sequence, target.sequence,
            "{name}: the resolved event is the canonical one"
        );

        harness.discard().await;
    }
}

/// Neither engine stems, so a word variant matches on both or on neither.
///
/// Postgres would stem by default: `to_tsvector('english', ...)` maps
/// "migration" and "migrate" to one lexeme, so a search for either would find
/// both, and a search for an exact term would return events that never contain
/// it. The schema therefore pins the `'simple'` configuration, which does not
/// stem, and this test is what keeps it pinned -- switching to `'english'` for
/// "better" search would silently give callers
/// different answers to the same question.
#[tokio::test]
async fn neither_backend_stems_word_variants() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let name = harness.name;
        let session_id = session_with_messages(
            store.as_ref(),
            &[
                (EventActor::User, "we are running the migration now"),
                (EventActor::Assistant, "the caches were flushed"),
            ],
        )
        .await;

        // The exact words said are found.
        for term in ["migration", "caches"] {
            let page = store
                .query_history(session_id, lexical(term))
                .await
                .unwrap_or_else(|error| panic!("{name}: {term}: {error:#}"));
            assert_eq!(
                page.hits.len(),
                1,
                "{name}: `{term}` occurs verbatim and must be found"
            );
        }

        // Variants of them are not. If a backend ever starts stemming, this is
        // the assertion that fails, and it fails on that backend alone.
        for term in ["migrations", "migrate", "migrating", "cache", "flush"] {
            let page = store
                .query_history(session_id, lexical(term))
                .await
                .unwrap_or_else(|error| panic!("{name}: {term}: {error:#}"));
            assert!(
                page.hits.is_empty(),
                "{name}: `{term}` is a variant, not a word that was said; \
                 matching it means this backend is stemming and the two \
                 backends no longer agree"
            );
        }

        harness.discard().await;
    }
}

/// Concurrent appends to ONE session allocate a gapless, duplicate-free
/// sequence on both backends.
///
/// This is the correctness half of the migration's central claim. The
/// throughput half is measured by `concurrent_writer_scaling_profile`; scaling
/// would be worthless if the allocator raced. Postgres serialises these writers
/// on the session's own row (`select ... for update` in the sequence
/// allocator). The result must be exactly
/// one event per append, numbered contiguously from 1, because replay walks
/// that sequence and a gap or a repeat silently truncates or duplicates
/// history.
#[tokio::test]
async fn concurrent_appends_to_one_session_stay_gapless() {
    const WRITERS: usize = 8;
    const APPENDS: usize = 12;

    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let name = harness.name;
        let session_id = started_session(store.as_ref()).await;

        let mut tasks = Vec::with_capacity(WRITERS);
        for writer in 0..WRITERS {
            let store = Arc::clone(&store);
            tasks.push(tokio::spawn(async move {
                for attempt in 0..APPENDS {
                    // Sequence 0 asks the store to allocate; that allocator is
                    // exactly what is under test.
                    store
                        .append(SessionEvent::new(
                            session_id,
                            0,
                            SessionEventKind::Error {
                                message: format!("writer {writer} append {attempt}"),
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

        let events = store
            .read(session_id)
            .await
            .unwrap_or_else(|error| panic!("{name}: read: {error:#}"));
        let mut sequences: Vec<u64> = events.iter().map(|event| event.sequence).collect();
        sequences.sort_unstable();

        // SessionStarted is sequence 1, so the appends occupy 2..=n.
        let expected: Vec<u64> = (1..=(WRITERS * APPENDS + 1) as u64).collect();
        assert_eq!(
            sequences, expected,
            "{name}: {WRITERS} concurrent writers must produce a contiguous \
             sequence with no gaps and no duplicates"
        );

        // Every append survived, and each one exactly once.
        let mut bodies: Vec<String> = events
            .iter()
            .filter_map(|event| match &event.kind {
                SessionEventKind::Error { message } => Some(message.clone()),
                _ => None,
            })
            .collect();
        bodies.sort();
        bodies.dedup();
        assert_eq!(
            bodies.len(),
            WRITERS * APPENDS,
            "{name}: every concurrent append is journaled exactly once"
        );

        harness.discard().await;
    }
}

/// An event body containing a NUL survives on both backends.
///
/// Found by replaying a real journal, not by construction. Postgres `jsonb`
/// parses to a tree whose strings are `text`, and `text` cannot hold a NUL, so
/// the server rejects the entire insert with "unsupported Unicode escape
/// sequence", so an event carrying one would fail the whole append rather than
/// be stored lossily. 246 events of 2.5 million in the journal this was
/// measured against carry one, nearly all of them tool output that captured a
/// binary byte.
///
/// This is not a migration concern: the same append path runs live, so before
/// the fix a single NUL in a tool's output would have failed the turn.
#[tokio::test]
async fn an_event_body_containing_a_nul_round_trips_on_both_backends() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let name = harness.name;
        let session_id = started_session(store.as_ref()).await;
        // A NUL in the middle of otherwise ordinary output, as a captured
        // binary byte would appear.
        let output = "header\u{0000}trailer";

        let stored = store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ToolCompleted {
                    tool_call_id: "nul-call".to_string(),
                    output: output.to_string(),
                    output_ref: None,
                    is_error: false,
                    input: None,
                    input_ref: None,
                },
            ))
            .await
            .unwrap_or_else(|error| panic!("{name}: a NUL must not fail the append: {error:#}"));
        assert_eq!(stored.sequence, 2, "{name}");

        // Read back through the ordinary path: the byte must survive exactly,
        // not be stripped or replaced.
        let events = store
            .read(session_id)
            .await
            .unwrap_or_else(|error| panic!("{name}: read: {error:#}"));
        let replayed = events
            .iter()
            .find_map(|event| match &event.kind {
                SessionEventKind::ToolCompleted { output, .. } => Some(output.clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{name}: the event must be readable"));
        assert_eq!(
            replayed, output,
            "{name}: a NUL must survive the round trip byte for byte"
        );

        // And the session still projects, so the event did not merely land in
        // a row nothing can replay.
        let state = store
            .state(session_id)
            .await
            .unwrap_or_else(|error| panic!("{name}: state: {error:#}"));
        assert_eq!(state.latest_sequence, 2, "{name}");

        harness.discard().await;
    }
}

/// Prompt admission is idempotent by message id on both backends.
///
/// This was a REAL GAP found by auditing which trait methods Postgres left to
/// their defaults: `admit_prompt`'s default bails outright, so every
/// interactive prompt and every relayed prompt would have failed on Postgres
/// while the one-shot path kept working. A method with a default is not a
/// method that is implemented.
#[tokio::test]
async fn prompt_admission_is_idempotent_on_both_backends() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let name = harness.name;
        let session_id = started_session(store.as_ref()).await;
        // Admission waits for the session to have journaled its configuration,
        // because a prompt admitted earlier would replay against a session that
        // does not yet know its provider.
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::SessionConfigured {
                    cwd: std::path::PathBuf::from("/tmp"),
                    provider: crate::CodingProvider::Claude,
                    model: None,
                    effort: None,
                    fast: false,
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: crate::PermissionMode::FullAccess,
                },
            ))
            .await
            .unwrap_or_else(|error| panic!("{name}: configure: {error:#}"));

        let message_id = Uuid::new_v4();
        let admission = || user_prompt(session_id, message_id, "admit me once");

        let first = store
            .admit_prompt(admission())
            .await
            .unwrap_or_else(|error| panic!("{name}: admit: {error:#}"));
        let repeat = store
            .admit_prompt(admission())
            .await
            .unwrap_or_else(|error| panic!("{name}: re-admit: {error:#}"));
        assert_eq!(
            first.sequence, repeat.sequence,
            "{name}: a retried prompt returns the event already journaled"
        );
        assert_eq!(first.id, repeat.id, "{name}");

        // A DIFFERENT prompt reusing the id is refused rather than silently
        // replacing the admitted one.
        assert!(
            store
                .admit_prompt(user_prompt(session_id, message_id, "a different prompt"))
                .await
                .is_err(),
            "{name}: reusing a message id for different text must be refused"
        );

        harness.discard().await;
    }
}

/// Recent-message recall returns the same tail on both backends.
///
/// Also a gap found by the same audit: Postgres was falling back to the trait
/// default, which reads the ENTIRE session to find the last few messages. On a
/// 133,512-event session -- one exists in the journal this was built against --
/// that is hundreds of megabytes to answer a question the message index can
/// answer directly.
#[tokio::test]
async fn recent_message_recall_agrees_on_both_backends() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let name = harness.name;
        let session_id = started_session(store.as_ref()).await;

        // Completed, not queued: recall is about turns that HAPPENED, and a
        // queued prompt has not happened yet -- which is why the filter
        // excludes it and why the fixture must not use `user_prompt`.
        let completed_user = |text: &str| {
            SessionEvent::new(
                session_id,
                0,
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: EventActor::User,
                    text: text.to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            )
        };
        for index in 0..6 {
            store
                .append(completed_user(&format!("user {index}")))
                .await
                .unwrap_or_else(|error| panic!("{name}: user append: {error:#}"));
            store
                .append(SessionEvent::new(
                    session_id,
                    0,
                    SessionEventKind::Message {
                        message_id: Uuid::new_v4(),
                        actor: EventActor::Assistant,
                        text: format!("assistant {index}"),
                        attachments: Vec::new(),
                        status: MessageStatus::Complete,
                        delivery: None,
                    },
                ))
                .await
                .unwrap_or_else(|error| panic!("{name}: assistant append: {error:#}"));
        }

        let text_of = |events: &[SessionEvent]| -> Vec<String> {
            events
                .iter()
                .filter_map(|event| match &event.kind {
                    SessionEventKind::Message { text, .. } => Some(text.clone()),
                    _ => None,
                })
                .collect()
        };

        // Both actors, oldest-first within the tail.
        let recent = store
            .recent_messages(session_id, 4)
            .await
            .unwrap_or_else(|error| panic!("{name}: recent_messages: {error:#}"));
        assert_eq!(
            text_of(&recent),
            vec![
                "user 4".to_string(),
                "assistant 4".to_string(),
                "user 5".to_string(),
                "assistant 5".to_string(),
            ],
            "{name}: the newest four messages, in order"
        );

        // User prompts only.
        let prompts = store
            .recent_user_messages(session_id, 3)
            .await
            .unwrap_or_else(|error| panic!("{name}: recent_user_messages: {error:#}"));
        assert_eq!(
            text_of(&prompts),
            vec![
                "user 3".to_string(),
                "user 4".to_string(),
                "user 5".to_string()
            ],
            "{name}: recall excludes assistant turns"
        );

        // A zero limit is not a full read.
        assert!(
            store
                .recent_messages(session_id, 0)
                .await
                .unwrap_or_else(|error| panic!("{name}: zero: {error:#}"))
                .is_empty(),
            "{name}: a zero limit returns nothing"
        );

        harness.discard().await;
    }
}

/// Cross-session search spans sessions on both backends.
///
/// Until this test it was unreachable: the implementation existed but was on no
/// trait and had no caller, so nothing in the runtime could invoke it. A
/// capability nothing can call is not delivered.
#[tokio::test]
async fn cross_session_search_spans_sessions() {
    for harness in harnesses().await {
        let store = Arc::clone(&harness.store);
        let name = harness.name;

        let first = session_with_messages(
            store.as_ref(),
            &[(EventActor::User, "the gateway returned a teapot error")],
        )
        .await;
        let second = session_with_messages(
            store.as_ref(),
            &[(
                EventActor::Assistant,
                "a teapot error again, in another thread",
            )],
        )
        .await;
        let unrelated = session_with_messages(
            store.as_ref(),
            &[(EventActor::User, "lunch plans for thursday")],
        )
        .await;

        let page = store
            .search_all_sessions(lexical("teapot"))
            .await
            .unwrap_or_else(|error| panic!("{name}: cross-session search: {error:#}"));

        let sessions: std::collections::BTreeSet<Uuid> =
            page.hits.iter().map(|hit| hit.event.session_id).collect();
        assert!(
            sessions.contains(&first) && sessions.contains(&second),
            "{name}: a term said in two sessions must be found in both, got {sessions:?}"
        );
        assert!(
            !sessions.contains(&unrelated),
            "{name}: a session that never said it must not match"
        );

        // Per-session search remains scoped: the cross-session query is a
        // different question, not a widening of the old one.
        let scoped = store
            .query_history(first, lexical("teapot"))
            .await
            .unwrap_or_else(|error| panic!("{name}: scoped: {error:#}"));
        let scoped_sessions: std::collections::BTreeSet<Uuid> =
            scoped.hits.iter().map(|hit| hit.event.session_id).collect();
        assert_eq!(
            scoped_sessions,
            std::collections::BTreeSet::from([first]),
            "{name}: a per-session search stays inside its session"
        );

        // Regex is refused rather than silently scanning every body in the
        // journal, which is a different and far more expensive promise.
        assert!(
            store
                .search_all_sessions(crate::session_store::SessionHistoryQuery {
                    text: Some("teapot".to_string()),
                    mode: crate::session_store::SessionHistorySearchMode::Regex,
                    ..Default::default()
                })
                .await
                .is_err(),
            "{name}: cross-session regex must be refused, not attempted"
        );

        harness.discard().await;
    }
}
