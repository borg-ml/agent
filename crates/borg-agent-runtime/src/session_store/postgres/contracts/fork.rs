//! What a fork owes its parent, and what it must refuse to inherit.
//!
//! A fork records a cut rather than copying events, so every guarantee here is
//! really a guarantee about composition on read: the child's history, its
//! projected state, its paged reads and its recovery set all have to agree with
//! what a full copy would have produced, while the parent's rows stay where
//! they are. These contracts pin both halves -- that the cheap representation
//! is still correct, and that the things a fork must NOT carry across (the
//! provider thread, the consumed context, the queue entry of the prompt the
//! user just discarded) genuinely stop at the cut.

use uuid::Uuid;

use super::support::{configured, message};
use crate::session_store::FORK_PROJECTION_CHECKPOINT_INTERVAL;
use crate::{
    CodingProvider, EventActor, EventPersistence, MessageStatus, PromptDelivery, SessionEvent,
    SessionEventKind, SessionGoal, SessionStore,
};

#[tokio::test]
async fn request_usage_survives_reload_without_double_counting_or_fork_inheritance() {
    let (scratch, store) = super::support::store().await;
    let parent = Uuid::new_v4();
    store.create_session(parent).await.unwrap();
    let raw = serde_json::json!({
        "input_tokens": 12, "input_tokens_details": {"cached_tokens": 4, "cache_write_tokens": 2},
        "output_tokens": 3, "output_tokens_details": {"reasoning_tokens": 2},
    });
    for kind in [
        SessionEventKind::SessionStarted,
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "native_model_request".into(),
            payload: serde_json::json!({"request_id":"request-1"}),
        },
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "native_model_usage".into(),
            payload: serde_json::json!({"request_id":"request-1","complete":false,"usage":{"input_tokens":12}}),
        },
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "native_model_usage".into(),
            payload: serde_json::json!({"request_id":"request-1","complete":true,"usage":raw}),
        },
        SessionEventKind::UsageUpdated {
            provider_duration_ms: 1,
            turn_id: None,
            provider_context_reused: None,
            input_tokens: 6,
            output_tokens: 3,
            cached_input_tokens: 4,
            cache_creation_input_tokens: 2,
            total_tokens: 15,
            cost_microusd: None,
            cost_basis: "subscription_equivalent".into(),
            cost_usd: None,
            context_tokens: Some(12),
            context_window_tokens: None,
        },
    ] {
        store
            .append(SessionEvent::new(parent, 0, kind))
            .await
            .unwrap();
    }
    drop(store);
    let store = crate::PostgresSessionStore::connect_with_pool_size(&scratch.url, 2)
        .await
        .unwrap();
    let events = store.read(parent).await.unwrap();
    assert_eq!(events.len(), 5);
    assert!(
        matches!(&events[3].kind, SessionEventKind::ProviderEvent { payload, .. }
        if payload["complete"] == true && payload["usage"] == raw)
    );
    let usage = store.state(parent).await.unwrap().usage;
    assert_eq!(
        (
            usage.calls,
            usage.input_tokens,
            usage.cached_input_tokens,
            usage.cache_creation_input_tokens,
            usage.output_tokens,
            usage.total_tokens
        ),
        (1, 6, 4, 2, 3, 15)
    );
    let child = Uuid::new_v4();
    store.fork_before(parent, child, 6).await.unwrap();
    assert!(store.read(child).await.unwrap().iter().all(|event| !matches!(&event.kind,
        SessionEventKind::ProviderEvent { kind, .. } if matches!(kind.as_str(), "native_model_request" | "native_model_usage"))));
    scratch.discard().await;
}

#[tokio::test]
async fn cache_projection_survives_reload_and_fork_without_erasing_the_transcript() {
    use borg_provider::provider::{ModelMessage, ModelToolCall};
    let (scratch, store) = super::support::store().await;
    let parent = Uuid::new_v4();
    store.create_session(parent).await.unwrap();
    let prefix = crate::NativeRequestPrefix {
        provider: CodingProvider::Claude,
        model: "test-model".into(),
        system_prompt: "stable system".into(),
        tools: vec![],
        prompt_cache_key: "parent-key".into(),
    };
    let original = "complete tool evidence".repeat(500);
    for message in [
        ModelMessage::user("inspect"),
        ModelMessage::assistant(
            None,
            None,
            None,
            vec![ModelToolCall::function(
                "call-1".into(),
                "read".into(),
                "{}".into(),
            )],
        ),
        ModelMessage::tool("call-1", &original),
    ] {
        store
            .append(SessionEvent::new(
                parent,
                0,
                SessionEventKind::ProviderEvent {
                    provider: CodingProvider::Claude,
                    kind: "native_model_message".into(),
                    payload: serde_json::to_value(message).unwrap(),
                },
            ))
            .await
            .unwrap();
    }
    for (kind, payload) in [
        ("native_tool_round_completed", serde_json::json!({})),
        (
            "native_request_prefix",
            serde_json::to_value(&prefix).unwrap(),
        ),
        (
            "context_microcompaction",
            serde_json::json!({"status":"completed","cleared_tool_call_ids":["call-1"]}),
        ),
    ] {
        store
            .append(SessionEvent::new(
                parent,
                0,
                SessionEventKind::ProviderEvent {
                    provider: CodingProvider::Claude,
                    kind: kind.into(),
                    payload,
                },
            ))
            .await
            .unwrap();
    }
    drop(store);
    let store = crate::PostgresSessionStore::connect_with_pool_size(&scratch.url, 2)
        .await
        .unwrap();
    let child = Uuid::new_v4();
    store.fork_before(parent, child, 7).await.unwrap();
    for session in [parent, child] {
        let context = store.recovery(session).await.unwrap().context_events;
        assert_eq!(
            crate::session::native_request_prefix(&context),
            Some(prefix.clone())
        );
        let messages =
            crate::session::native_conversation(&context, CodingProvider::Claude).unwrap();
        assert!(messages.iter().any(
            |message| matches!(message, ModelMessage::Tool { content, .. }
            if content == crate::native_harness::MICROCOMPACT_CLEARED_TOOL_RESULT)
        ));
        assert!(store.read(session).await.unwrap().iter().any(|event| matches!(&event.kind,
            SessionEventKind::ProviderEvent { kind, payload, .. } if kind == "native_model_message" && payload["content"] == original)));
    }
    store
        .append(SessionEvent::new(
            child,
            0,
            SessionEventKind::ContextCleared,
        ))
        .await
        .unwrap();
    assert!(
        crate::session::native_request_prefix(&store.recovery(child).await.unwrap().context_events)
            .is_none()
    );
    scratch.discard().await;
}

#[tokio::test]
async fn fork_records_lineage_without_copying_events() {
    let (scratch, store) = super::support::store().await;
    let parent_id = Uuid::new_v4();
    let fork_id = Uuid::new_v4();
    let retained_message_id = Uuid::new_v4();
    let discarded_message_id = Uuid::new_v4();
    store.create_session(parent_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(std::path::Path::new("/tmp")),
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
        sqlx::query_scalar("select count(*) from session_events where session_id = $1")
            .bind(fork_id)
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
    scratch.discard().await;
}

#[tokio::test]
async fn fork_projection_checkpoints_bound_state_amplification() {
    const LARGE_RESPONSE_BYTES: usize = 128 * 1024;
    const EVENT_COUNT: u64 = FORK_PROJECTION_CHECKPOINT_INTERVAL + 23;

    let (scratch, store) = super::support::store().await;
    let parent_id = Uuid::new_v4();
    let fork_id = Uuid::new_v4();
    let goal = SessionGoal::new("retained sparse projection goal".to_string(), None);
    let response = "r".repeat(LARGE_RESPONSE_BYTES);
    store.create_session(parent_id).await.unwrap();
    let mut transaction = store.pool().begin().await.unwrap();
    for sequence in 1..=EVENT_COUNT {
        let kind = match sequence {
            1 => SessionEventKind::SessionStarted,
            2 => configured(std::path::Path::new("/tmp")),
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
         from session_events where session_id = $1 and projection_json <> ''",
    )
    .bind(parent_id)
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
    scratch.discard().await;
}

#[tokio::test]
async fn fork_starts_a_fresh_provider_context_without_losing_capacity() {
    let (scratch, store) = super::support::store().await;
    let parent_id = Uuid::new_v4();
    let fork_id = Uuid::new_v4();
    store.create_session(parent_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(std::path::Path::new("/tmp")),
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
    scratch.discard().await;
}

#[tokio::test]
async fn latest_completed_compaction_is_projected_through_a_fork() {
    let (scratch, store) = super::support::store().await;
    let parent_id = Uuid::new_v4();
    let fork_id = Uuid::new_v4();
    store.create_session(parent_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(std::path::Path::new("/tmp")),
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
    scratch.discard().await;
}

#[tokio::test]
async fn provider_native_compaction_is_not_a_durable_replay_boundary() {
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

#[tokio::test]
async fn provider_native_recovery_checkpoint_is_context_without_rotating_generation() {
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
    let (scratch, store) = super::support::store().await;
    let parent_id = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    let grandchild_id = Uuid::new_v4();
    store.create_session(parent_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(std::path::Path::new("/tmp")),
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
    scratch.discard().await;
}

/// A rewind cuts immediately before the admission of the prompt it targets.
/// That prompt's earlier queue entry sits below the cut, so inheriting it
/// would hand the fork a pending prompt and re-run exactly what the user
/// just discarded.
#[tokio::test]
async fn a_rewind_does_not_inherit_the_queue_entry_of_the_discarded_prompt() {
    let (scratch, store) = super::support::store().await;
    let parent_id = Uuid::new_v4();
    let fork_id = Uuid::new_v4();
    let discarded_message_id = Uuid::new_v4();
    store.create_session(parent_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(std::path::Path::new("/tmp")),
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
    scratch.discard().await;
}

/// A fork of a fork, cut between two checkpoints, sees the EARLIER one.
///
/// WHY THIS IS NOT COVERED BY THE SINGLE-FORK TEST ABOVE: that fork inherits
/// its whole visible prefix, so asking its parent for the latest checkpoint up
/// to the cut and asking for the latest checkpoint the fork can see are the
/// same question. They stop being the same question the moment a cut lands
/// INSIDE an inherited prefix, which needs two generations to construct.
///
/// The parent numbers its own events and each fork renumbers what it inherited,
/// so the bound "up to sequence 4 of mine" cannot be handed to the parent as a
/// sequence at all. A lookup that hands over the full cut instead gets the
/// parent's LATEST checkpoint, finds it lies beyond the bound, and reports no
/// checkpoint -- discarding an older one that is genuinely visible. The
/// consequence is not a missing field: a resume with no boundary replays a
/// conversation that was already compacted.
#[tokio::test]
async fn a_fork_cut_between_two_checkpoints_inherits_the_earlier_one() {
    let (scratch, store) = super::support::store().await;
    let root = Uuid::new_v4();
    store.create_session(root).await.unwrap();

    let compaction = |summary: &str| SessionEventKind::ProviderEvent {
        provider: CodingProvider::Codex,
        kind: "context_compaction".to_string(),
        payload: serde_json::json!({"status": "completed", "summary": summary}),
    };
    for kind in [
        SessionEventKind::SessionStarted,
        configured(std::path::Path::new("/tmp")),
        compaction("earlier checkpoint"),
        message(Uuid::new_v4(), "after the earlier checkpoint"),
        compaction("later checkpoint"),
        message(Uuid::new_v4(), "after the later checkpoint"),
    ] {
        store
            .append(SessionEvent::new(root, 0, kind))
            .await
            .unwrap();
    }

    // The first fork inherits everything, so both checkpoints are visible to it
    // at its own sequences 3 and 5.
    let middle = Uuid::new_v4();
    store.fork_before(root, middle, 7).await.unwrap();
    let inherited = store
        .latest_completed_context_compaction(middle)
        .await
        .unwrap()
        .expect("a fork inheriting both checkpoints sees the later one");
    assert!(matches!(
        &inherited.kind,
        SessionEventKind::ProviderEvent { payload, .. }
            if payload.get("summary").and_then(serde_json::Value::as_str)
                == Some("later checkpoint")
    ));

    // The second fork cuts between them: it inherits the earlier checkpoint and
    // not the later one.
    let leaf = Uuid::new_v4();
    store.fork_before(middle, leaf, 5).await.unwrap();
    let checkpoint = store
        .latest_completed_context_compaction(leaf)
        .await
        .unwrap()
        .expect("the earlier checkpoint is inside the cut and must not be hidden");
    assert!(
        matches!(
            &checkpoint.kind,
            SessionEventKind::ProviderEvent { payload, .. }
                if payload.get("summary").and_then(serde_json::Value::as_str)
                    == Some("earlier checkpoint")
        ),
        "expected the earlier checkpoint, got {:?}",
        checkpoint.kind
    );
    assert_eq!(
        checkpoint.session_id, leaf,
        "an inherited checkpoint is reported in the reading session's identity"
    );
    assert!(
        checkpoint.sequence <= 4,
        "the checkpoint must sit inside the cut, got sequence {}",
        checkpoint.sequence
    );

    scratch.discard().await;
}
