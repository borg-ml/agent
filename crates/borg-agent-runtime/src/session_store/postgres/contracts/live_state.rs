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
