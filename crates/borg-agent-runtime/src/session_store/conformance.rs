//! One suite, both backends.
//!
//! These tests are written against the `SessionStore` trait and run twice: once
//! on SQLite and once on PostgreSQL. Anywhere the two backends disagree, a test
//! here fails -- which is the only real proof that swapping the journal engine
//! does not quietly change behaviour.
//!
//! The Postgres half is skipped when `BORG_TEST_SESSIONS_URL` is unset, so the
//! suite still runs on a machine with no database; the SQLite half always runs.

use std::sync::Arc;

use uuid::Uuid;

use super::postgres::PostgresSessionStore;
use super::postgres::testing::{ScratchDatabase, test_url};
use crate::session_action::{
    ActionDeliveryPolicy, ActionWakePolicy, SessionAction, SessionActionKind, SessionActionState,
};
use crate::session_store::{RecoveryParts, SessionStore, SqliteSessionStore};
use crate::{
    EventActor, MessageStatus, PromptDelivery, SessionEvent, SessionEventKind, SessionStatus,
};

/// One backend under test, with whatever it needs to clean up afterwards.
struct Harness {
    name: &'static str,
    store: Arc<dyn SessionStore>,
    _directory: Option<tempfile::TempDir>,
    scratch: Option<ScratchDatabase>,
}

impl Harness {
    async fn discard(self) {
        if let Some(scratch) = self.scratch {
            scratch.discard().await;
        }
    }
}

/// Every backend available on this machine.
async fn harnesses() -> Vec<Harness> {
    let mut harnesses = Vec::new();
    let directory = tempfile::tempdir().expect("temp dir");
    let sqlite = SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
        .await
        .expect("open sqlite store");
    harnesses.push(Harness {
        name: "sqlite",
        store: Arc::new(sqlite),
        _directory: Some(directory),
        scratch: None,
    });

    if let Some(url) = test_url() {
        let scratch = ScratchDatabase::create(&url).await;
        let postgres = PostgresSessionStore::connect(&scratch.url)
            .await
            .expect("connect postgres store");
        harnesses.push(Harness {
            name: "postgres",
            store: Arc::new(postgres),
            _directory: None,
            scratch: Some(scratch),
        });
    } else {
        eprintln!("conformance: skipping postgres, BORG_TEST_SESSIONS_URL is not set");
    }
    harnesses
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
            events.iter().map(|event| event.sequence).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "[{name}]"
        );
        assert_eq!(
            store.events_after(session_id, 1, 10).await.expect("after").len(),
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
        assert!(store.contains_message(session_id, message_id).await.unwrap());

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
            store.action(session_id, message_id).await.unwrap().unwrap().state,
            SessionActionState::Completed,
            "[{name}]"
        );
        assert!(
            store.pending_actions(session_id, 10).await.unwrap().is_empty(),
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
            store.enqueue_action(action.clone()).await.expect("re-enqueue").action_id,
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
            .transition_action(session_id, action_id, None, SessionActionState::Admitted, None)
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
        let fork = store.fork_before(parent, child, cut_at).await.expect("fork");
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
            inherited.iter().map(|event| event.sequence).collect::<Vec<_>>(),
            (1..=inherited.len() as u64).collect::<Vec<_>>(),
            "[{name}] an inherited prefix must be contiguous"
        );
        assert!(
            inherited.iter().all(|event| event.session_id == child),
            "[{name}] inherited events belong to the child"
        );
        let parent_ids: Vec<Uuid> = parent_events.iter().map(|event| event.id).collect();
        assert!(
            inherited.iter().all(|event| !parent_ids.contains(&event.id)),
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
        assert!(listed.contains(&first) && listed.contains(&second), "[{name}]");
        assert!(
            !listed.contains(&child),
            "[{name}] a child session is not a root session"
        );
        harness.discard().await;
    }
}

/// Guard against the suite quietly becoming single-backend.
///
/// Every test above loops over whatever `harnesses()` returns, so a
/// misconfigured URL or a broken connection would turn the whole conformance
/// suite green while only exercising SQLite. This asserts the loop really did
/// cover Postgres whenever the environment says it should.
#[tokio::test]
async fn both_backends_are_exercised_when_postgres_is_configured() {
    let configured = test_url().is_some();
    let harnesses = harnesses().await;
    let names: Vec<&str> = harnesses.iter().map(|harness| harness.name).collect();
    assert!(names.contains(&"sqlite"), "sqlite must always be covered");
    assert_eq!(
        names.contains(&"postgres"),
        configured,
        "postgres coverage must follow BORG_TEST_SESSIONS_URL, got {names:?}"
    );
    for harness in harnesses {
        harness.discard().await;
    }
}
