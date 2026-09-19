//! What the journal owes an action once a prompt has been accepted.
//!
//! A durable action is the promise the journal makes to a user who pressed
//! enter: the work is recorded before it is acknowledged, it runs at most once,
//! it belongs to exactly one session, and every boundary it crosses leaves a
//! record that survives a crash. These contracts pin that promise down at the
//! seams where it is easiest to break -- a coalesced event arriving after the
//! turn already ended, two workers racing for the same lease, a lease expiring
//! under a worker that later wakes up and tries to finish, a sweep that could
//! reach into a neighbouring session's queue.
//!
//! They are worth a test because none of it is visible in a single query. Each
//! contract only fails across a sequence, and the failure mode is silent: work
//! that is quietly dropped, quietly duplicated, or quietly completed by a
//! worker that no longer owns it.

use std::time::Duration;

use chrono::Utc;
use uuid::Uuid;

use super::support::configured;
use crate::{
    ClaimedActionTransition, CodingProvider, EventActor, MessageStatus, PromptDelivery,
    SessionAction, SessionActionState, SessionEvent, SessionEventKind, SessionStore,
};

#[tokio::test]
async fn prompt_event_boundaries_drive_one_atomic_action_lifecycle() {
    let (scratch, store) = super::support::store().await;
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
            configured(std::path::Path::new("/tmp")),
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
    scratch.discard().await;
}

#[tokio::test]
async fn prompt_admission_is_durable_visible_and_idempotent_before_routing() {
    let (scratch, store) = super::support::store().await;
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
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
    scratch.discard().await;
}

#[tokio::test]
async fn stale_in_progress_message_does_not_resurrect_terminal_action() {
    let (scratch, store) = super::support::store().await;
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(std::path::Path::new("/tmp")),
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
    scratch.discard().await;
}

#[tokio::test]
async fn accepted_steer_queue_event_reopens_a_terminal_action_projection() {
    let (scratch, store) = super::support::store().await;
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(std::path::Path::new("/tmp")),
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
    scratch.discard().await;
}

#[tokio::test]
async fn internal_messages_do_not_reuse_child_action_identity() {
    let (scratch, store) = super::support::store().await;
    let child_session_id = Uuid::new_v4();
    let parent_session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    for session_id in [child_session_id, parent_session_id] {
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
    scratch.discard().await;
}

#[tokio::test]
async fn recovered_steer_accepts_a_coalesced_queue_snapshot() {
    let (scratch, store) = super::support::store().await;
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    store.create_session(session_id).await.unwrap();
    for kind in [
        SessionEventKind::SessionStarted,
        configured(std::path::Path::new("/tmp")),
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
    scratch.discard().await;
}

#[tokio::test]
async fn concurrent_claims_have_one_winner_and_same_owner_claim_is_idempotent() {
    let (scratch, store) = super::support::store().await;
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
    // Postgres `timestamptz` keeps microseconds, so the heartbeat read back off
    // the row is a truncation of the one the winning claim returned from memory.
    // The contract is that a replayed claim does not move the lease, so it is
    // compared at the precision the durable row actually preserves.
    assert_eq!(
        replay.lease_heartbeat_at.map(|at| at.timestamp_micros()),
        winner.lease_heartbeat_at.map(|at| at.timestamp_micros())
    );
    assert_eq!(
        store
            .action_transitions(session_id, action_id)
            .await
            .unwrap()
            .len(),
        1,
        "claiming must not create duplicate lifecycle transitions"
    );
    scratch.discard().await;
}

#[tokio::test]
async fn expired_leases_requeue_once_and_fence_stale_workers() {
    let (scratch, store) = super::support::store().await;
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
    scratch.discard().await;
}

#[tokio::test]
async fn expired_action_recovery_never_moves_work_between_sessions() {
    let (scratch, store) = super::support::store().await;
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
    scratch.discard().await;
}

#[tokio::test]
async fn compaction_events_drive_one_replayable_action_lifecycle() {
    let (scratch, store) = super::support::store().await;
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
    scratch.discard().await;
}
