//! Live turn state: what a streaming turn may publish, and when it is erased.
//!
//! Live state is the one part of the journal that is deliberately NOT durable.
//! A streaming turn produces a frame per token, and writing each one as an
//! event would make the durable sequence a function of network chatter rather
//! than of what happened. So these snapshots coalesce into one row per live
//! key, carry no sequence, and are erased at the boundaries that end the thing
//! they were describing.
//!
//! The contracts below pin the two halves of that bargain: coalescing must not
//! consume durable sequences or leak into `read`, and every boundary -- a tool
//! starting, a thought completing, a turn ending, a session going idle -- must
//! clear the state it invalidates while leaving the context window, which
//! describes the session rather than the turn, in place.

use uuid::Uuid;

use super::support::{configured, store};
use crate::{
    EventActor, MessageStatus, SessionEvent, SessionEventKind, SessionStatus, SessionStore,
};

#[tokio::test]
async fn parallel_generation_status_survives_reconnect_and_clears_per_call() {
    let (scratch, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(std::path::Path::new("/tmp")),
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
                provider: crate::CodingProvider::Claude, kind: kind.into(),
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
                parent_tool_call_id: None,
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
    scratch.discard().await;
}

#[tokio::test]
async fn live_state_coalesces_without_consuming_durable_sequences() {
    let (scratch, store) = store().await;
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(std::path::Path::new("/tmp")),
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
                provider: crate::CodingProvider::Codex,
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
    scratch.discard().await;
}

#[tokio::test]
async fn reasoning_boundaries_clear_the_snapshot_before_the_next_thought() {
    let (scratch, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(std::path::Path::new("/tmp")),
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
                parent_tool_call_id: None,
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
    scratch.discard().await;
}

#[tokio::test]
async fn terminal_boundaries_clear_all_turn_live_state_but_keep_context_window() {
    let (scratch, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(std::path::Path::new("/tmp")),
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
    scratch.discard().await;
}

/// Stranded turn live state is erased when the store is opened again.
///
/// This is the crash case, and it is the reason the repair exists at all. Live
/// state is not written as events, so nothing replays it into a correct shape:
/// a process that dies mid-turn simply leaves its last frames behind. The
/// session is terminal by the time anyone looks, and serving those frames makes
/// a finished session look like it is still answering.
///
/// The row is inserted directly rather than through `append`, because
/// `append_live` refuses to write turn live state to a session that is not
/// running -- which is exactly the invariant the repair restores. Reaching
/// around the writer is the only way to produce the state a crash produces.
#[tokio::test]
async fn reopening_repairs_turn_live_state_left_on_a_terminal_session() {
    let (scratch, store) = store().await;
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
         (session_id, live_key, revision, event_json, updated_at) \
         values ($1, $2, $3, $4, $5)",
    )
    .bind(session_id)
    .bind(format!("message:{message_id}"))
    .bind(99_i64)
    .bind(serde_json::to_value(&event).unwrap())
    .bind(event.created_at)
    .execute(store.pool())
    .await
    .unwrap();
    assert!(
        !store
            .live_events_after(session_id, 0)
            .await
            .unwrap()
            .is_empty(),
        "the fixture must actually strand a row, or this proves nothing"
    );

    let reopened = crate::session_store::postgres::PostgresSessionStore::connect_with_pool_size(
        &scratch.url,
        4,
    )
    .await
    .unwrap();
    assert!(
        reopened
            .live_events_after(session_id, 0)
            .await
            .unwrap()
            .is_empty(),
        "reopening must not serve the frames of a turn that died"
    );

    scratch.discard().await;
}

/// The repair must not touch a session that is actually running.
///
/// Without this the test above is satisfied by a repair that deletes every live
/// row on open, which would erase the in-flight turn of a second process that
/// happens to be mid-stream while this one starts. The whole difficulty of the
/// repair is telling those two cases apart, so both halves have to be pinned.
#[tokio::test]
async fn reopening_leaves_the_live_state_of_a_running_session_alone() {
    let (scratch, store) = store().await;
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
                status: SessionStatus::Running,
                detail: None,
            },
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
                actor: EventActor::Assistant,
                text: "still streaming".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::InProgress,
                delivery: None,
            },
        ))
        .await
        .unwrap();
    let before = store.live_events_after(session_id, 0).await.unwrap();
    assert!(
        !before.is_empty(),
        "a running turn must publish live state for this to mean anything"
    );

    let reopened = crate::session_store::postgres::PostgresSessionStore::connect_with_pool_size(
        &scratch.url,
        4,
    )
    .await
    .unwrap();
    let after = reopened.live_events_after(session_id, 0).await.unwrap();
    assert_eq!(
        after.len(),
        before.len(),
        "opening a second store must not disturb a turn that is still running"
    );

    scratch.discard().await;
}
