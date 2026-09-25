//! What a resume is allowed to see after the session has moved on.
//!
//! Recovery is the journal's answer to "what does the provider need in order to
//! carry on?", and every contract here pins one way that answer must stay
//! narrow. A context boundary has to reset the provider projection rather than
//! merely hide history; a compaction has to keep the unresolved prompt tail it
//! summarised around; a checkpoint resume has to replay only the turns the
//! provider never acknowledged; and a narrowed projection has to return exactly
//! the events the full projection would have, without dragging the context
//! payloads a queue-only or roster-only resume never reads.
//!
//! These were ported from the retired SQLite suite deliberately unchanged in
//! name and intent: they describe the store's behaviour, not one engine's SQL.

use std::path::Path;
use std::time::Instant;

use uuid::Uuid;

use super::support::{configured, event_ids, message, seed_recovery_fixture, store};
use crate::session_store::RecoveryParts;
use crate::{
    CodingProvider, EventActor, MessageStatus, PromptDelivery, SessionEvent, SessionEventKind,
    SessionStore,
};

#[tokio::test]
async fn context_clear_resets_provider_projection_and_recovery_prefix() {
    let (scratch, store) = store().await;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(Path::new("/tmp")),
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
    scratch.discard().await;
}

#[tokio::test]
async fn compacted_recovery_keeps_the_unresolved_prompt_tail() {
    let (scratch, store) = store().await;
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
    scratch.discard().await;
}

#[tokio::test]
async fn only_acknowledged_terminal_turns_remain_provider_resume_checkpoints() {
    let (scratch, store) = store().await;
    let session_id = Uuid::new_v4();
    let completed_id = Uuid::new_v4();
    let failed_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(Path::new("/tmp")),
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
    scratch.discard().await;
}

#[tokio::test]
async fn provider_checkpoint_recovery_keeps_only_the_unacknowledged_tail() {
    let (scratch, store) = store().await;
    let session_id = Uuid::new_v4();
    let completed_id = Uuid::new_v4();
    let pending_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(Path::new("/tmp")),
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
    scratch.discard().await;
}

#[tokio::test]
async fn narrowed_recovery_parts_match_the_full_projection() {
    let (scratch, store) = store().await;
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
    scratch.discard().await;
}

#[tokio::test]
async fn narrowed_recovery_preserves_a_cut_inside_inherited_history() {
    let (scratch, store) = store().await;
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
                    parent_tool_call_id: None,
                },
            ))
            .await
            .unwrap();
        let mut prompt = message(Uuid::new_v4(), "completed prompt");
        if let SessionEventKind::Message { status, .. } = &mut prompt {
            *status = MessageStatus::Complete;
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
    scratch.discard().await;
}

#[tokio::test]
async fn narrowed_recovery_parts_match_the_full_projection_across_a_context_boundary() {
    let (scratch, store) = store().await;
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
    scratch.discard().await;
}

#[tokio::test]
#[ignore = "explicit recovery performance benchmark"]
async fn narrowed_recovery_skips_the_context_payloads_a_resume_never_reads() {
    let (scratch, store) = store().await;
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
                    parent_tool_call_id: None,
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
    scratch.discard().await;
}
