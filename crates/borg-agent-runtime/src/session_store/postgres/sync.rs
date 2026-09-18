//! Keeping the durable action queue in step with the event journal.
//!
//! An appended event and the action row it implies must commit together or not
//! at all: a prompt recorded without its action would never execute, and an
//! action completed without its event would look done while its work was lost.
//! Every function here therefore runs inside the caller's append transaction.
//!
//! This is a faithful port of the SQLite `sync_session_action`, including its
//! awkward branches -- steer/prompt promotion, queue coalescing, retry
//! re-admission. Those exist because real sessions produce them, so
//! "simplifying" them here would change behaviour rather than tidy it.

use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::actions::{
    ACTION_COLUMNS, append_action_transition, decode_action, insert_action_row,
    insert_initial_action_transitions, update_action_row,
};
use crate::session_action::{SessionAction, SessionActionKind, SessionActionState};
use crate::session_store::{enum_text, same_prompt_payload_ignoring_delivery};
use crate::{MessageStatus, SessionEvent, SessionEventKind};

/// Walk an action forward to `target`, one legal edge at a time.
///
/// Callers name an outcome ("this turn completed"), not a path. The lifecycle
/// forbids jumps, so the intermediate boundaries are synthesised here and each
/// one is audited, which is what makes a crash mid-turn reconstructable.
/// A terminal action is left alone: a late or replayed event must never
/// resurrect finished work.
async fn advance_action(
    transaction: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
    action_id: Uuid,
    target: SessionActionState,
    error: Option<String>,
) -> Result<()> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "select {ACTION_COLUMNS} from session_actions \
         where action_id = $1 and session_id = $2 for update"
    )))
    .bind(action_id)
    .bind(session_id)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(row) = row else { return Ok(()) };
    let mut action = decode_action(&row)?;
    if action.state.is_terminal() {
        return Ok(());
    }
    while action.state != target {
        let from_state = action.state;
        let next = match action.state {
            SessionActionState::Queued => SessionActionState::Admitted,
            SessionActionState::Admitted => {
                if target == SessionActionState::Failed {
                    SessionActionState::Failed
                } else {
                    SessionActionState::Delivered
                }
            }
            SessionActionState::Delivered => {
                if target == SessionActionState::Failed {
                    SessionActionState::Failed
                } else {
                    SessionActionState::Preparing
                }
            }
            SessionActionState::Preparing => {
                if target == SessionActionState::Failed {
                    SessionActionState::Failed
                } else {
                    SessionActionState::Committing
                }
            }
            SessionActionState::Committing => SessionActionState::Running,
            SessionActionState::Running => target,
            SessionActionState::Completed
            | SessionActionState::Failed
            | SessionActionState::Cancelled => break,
        };
        action.transition(Some(action.state), next, error.clone())?;
        append_action_transition(transaction, &action, from_state, error.clone()).await?;
    }
    update_action_row(transaction, &action).await
}

/// The newest non-terminal action of one kind in a session.
///
/// Compaction events do not carry the action id they belong to, so the open
/// action of that kind is the only way to attribute a completion to its start.
async fn latest_action_id(
    transaction: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
    kind: SessionActionKind,
) -> Result<Option<Uuid>> {
    Ok(sqlx::query_scalar(
        "select action_id from session_actions \
         where session_id = $1 and action_kind = $2 \
           and state not in ('completed', 'failed', 'cancelled') \
         order by created_at desc, action_id desc limit 1",
    )
    .bind(session_id)
    .bind(enum_text(&kind)?)
    .fetch_optional(&mut **transaction)
    .await?)
}

/// A retry is another durable queued message carrying the same action id, so
/// re-admission must move a failed projection back into the executable state
/// machine rather than treat the duplicate as a no-op.
async fn requeue_failed_action(
    transaction: &mut Transaction<'_, Postgres>,
    mut action: SessionAction,
) -> Result<()> {
    if action.state != SessionActionState::Failed {
        return Ok(());
    }
    let from_state = action.state;
    action.transition(Some(from_state), SessionActionState::Queued, None)?;
    append_action_transition(transaction, &action, from_state, None).await?;
    update_action_row(transaction, &action).await
}

/// A legacy accepted steer can hold a terminal projection before its
/// interruption requeues the same message into the FIFO. A durable queue event
/// is an explicit retry boundary, so both terminal states reopen here; ordinary
/// prompt duplicates still use `requeue_failed_action`.
async fn requeue_terminal_action(
    transaction: &mut Transaction<'_, Postgres>,
    mut action: SessionAction,
) -> Result<()> {
    if !matches!(
        action.state,
        SessionActionState::Completed | SessionActionState::Failed
    ) {
        return Ok(());
    }
    let from_state = action.state;
    action.transition(Some(from_state), SessionActionState::Queued, None)?;
    append_action_transition(transaction, &action, from_state, None).await?;
    update_action_row(transaction, &action).await
}

async fn rewrite_action_projection(
    transaction: &mut Transaction<'_, Postgres>,
    action: &SessionAction,
) -> Result<()> {
    sqlx::query(
        "update session_actions set action_kind=$1, delivery_policy=$2, wake_policy=$3, \
         payload_json=$4, updated_at=$5 where action_id=$6 and session_id=$7",
    )
    .bind(enum_text(&action.kind)?)
    .bind(enum_text(&action.delivery)?)
    .bind(enum_text(&action.wake)?)
    .bind(&action.payload)
    .bind(action.updated_at)
    .bind(action.action_id)
    .bind(action.session_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

/// Insert an action implied by an event, reconciling it with any existing row.
///
/// `allow_in_progress_payload_rewrite` marks a durable in-progress snapshot,
/// which is permitted to rewrite content that a queued event may not.
async fn upsert_event_action(
    transaction: &mut Transaction<'_, Postgres>,
    action: &SessionAction,
    allow_in_progress_payload_rewrite: bool,
) -> Result<()> {
    let existing = sqlx::query(sqlx::AssertSqlSafe(format!(
        "select {ACTION_COLUMNS} from session_actions where action_id = $1 for update"
    )))
    .bind(action.action_id)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(row) = existing else {
        insert_action_row(transaction, action).await?;
        insert_initial_action_transitions(transaction, action).await?;
        return Ok(());
    };

    let existing = decode_action(&row)?;
    anyhow::ensure!(
        existing.session_id == action.session_id,
        "action {} was reused by a different session",
        action.action_id
    );

    if allow_in_progress_payload_rewrite && existing.state.is_terminal() {
        // Recovery replays durable in-progress snapshots. One can legitimately
        // arrive after its terminal event (an interrupted coalesced queue, for
        // example) but must not resurrect or rewrite completed work. A later
        // queued event is the explicit retry boundary.
        tracing::debug!(
            action_id = %action.action_id,
            state = ?existing.state,
            "ignoring stale in-progress snapshot for terminal action"
        );
        return Ok(());
    }

    if existing.kind == SessionActionKind::Steering
        && action.kind == SessionActionKind::Prompt
        && (same_prompt_payload_ignoring_delivery(&existing.payload, &action.payload)
            || (allow_in_progress_payload_rewrite
                && existing.payload.get("message_id") == action.payload.get("message_id")))
    {
        // A rejected or interrupted active-turn steer is deliberately promoted
        // into the next-turn FIFO. Its in-progress snapshot may also carry a
        // coalesced queue payload, so that snapshot may rewrite content while
        // preserving the durable message identity.
        rewrite_action_projection(transaction, action).await?;
        return requeue_terminal_action(transaction, existing).await;
    }

    if allow_in_progress_payload_rewrite
        && existing.kind == SessionActionKind::Prompt
        && action.kind == SessionActionKind::Steering
        && same_prompt_payload_ignoring_delivery(&existing.payload, &action.payload)
    {
        // Escape can promote a queued prompt into the active provider turn.
        // The provider's admission event is the durable routing boundary, so
        // identity and state are preserved and only the delivery class moves.
        anyhow::ensure!(
            !existing.state.is_terminal(),
            "action {} completed before its queued input was flushed",
            action.action_id
        );
        return rewrite_action_projection(transaction, action).await;
    }

    if allow_in_progress_payload_rewrite && existing.payload != action.payload {
        // Queue coalescing combines several durable queue entries under the
        // last message id immediately before execution. The in-progress event
        // is the authoritative executed payload; update the projection without
        // letting arbitrary queued-message identity reuse bypass the
        // immutability check below.
        anyhow::ensure!(
            existing.kind == action.kind,
            "action {} changed kind before its in-progress payload changed",
            action.action_id
        );
        anyhow::ensure!(
            !existing.state.is_terminal(),
            "action {} was completed before its in-progress payload changed",
            action.action_id
        );
        rewrite_action_projection(transaction, action).await?;
        let mut rewritten = existing;
        rewritten.kind = action.kind;
        rewritten.delivery = action.delivery;
        rewritten.wake = action.wake;
        rewritten.payload = action.payload.clone();
        return requeue_failed_action(transaction, rewritten).await;
    }

    anyhow::ensure!(
        existing.kind == action.kind && existing.payload == action.payload,
        "action {} was reused with a different immutable payload",
        action.action_id
    );
    requeue_failed_action(transaction, existing).await
}

async fn create_action_and_advance(
    transaction: &mut Transaction<'_, Postgres>,
    action: SessionAction,
    target: SessionActionState,
    error: Option<String>,
) -> Result<()> {
    let action_id = action.action_id;
    let session_id = action.session_id;
    upsert_event_action(transaction, &action, false).await?;
    advance_action(transaction, session_id, action_id, target, error).await
}

fn stamped(mut action: SessionAction, created_at: DateTime<Utc>) -> SessionAction {
    action.created_at = created_at;
    action.updated_at = created_at;
    action
}

/// Project one appended event onto the durable action queue.
pub(super) async fn sync_session_action(
    transaction: &mut Transaction<'_, Postgres>,
    event: &SessionEvent,
) -> Result<()> {
    match &event.kind {
        SessionEventKind::SessionConfigured { .. } => {
            let action = SessionAction::new(
                event.id,
                event.session_id,
                SessionActionKind::ProviderChange,
                crate::ActionDeliveryPolicy::WhenRunIdle,
                crate::ActionWakePolicy::Immediate,
                serde_json::to_value(&event.kind)?,
            );
            create_action_and_advance(transaction, action, SessionActionState::Completed, None)
                .await?;
        }
        SessionEventKind::Message {
            message_id,
            actor: crate::EventActor::User,
            text,
            attachments,
            status: status @ (MessageStatus::Queued | MessageStatus::InProgress),
            delivery,
        } => {
            let delivery = delivery.unwrap_or(crate::PromptDelivery::Queue);
            let (kind, delivery_policy, wake_policy) = match delivery {
                crate::PromptDelivery::Steer => (
                    SessionActionKind::Steering,
                    crate::ActionDeliveryPolicy::WhenRunIdle,
                    crate::ActionWakePolicy::Immediate,
                ),
                crate::PromptDelivery::Queue => (
                    SessionActionKind::Prompt,
                    crate::ActionDeliveryPolicy::NextTurnBoundary,
                    crate::ActionWakePolicy::OnLowerBoundary,
                ),
            };
            let mut action = stamped(
                SessionAction::new(
                    *message_id,
                    event.session_id,
                    kind,
                    delivery_policy,
                    wake_policy,
                    serde_json::json!({
                        "message_id": message_id,
                        "text": text,
                        "attachments": attachments,
                        "delivery": delivery,
                    }),
                ),
                event.created_at,
            );
            // A durably recorded prompt is already admitted: the journal entry
            // IS the admission, so the row starts past queued.
            action.transition(
                Some(SessionActionState::Queued),
                SessionActionState::Admitted,
                None,
            )?;
            action.created_at = event.created_at;
            upsert_event_action(
                transaction,
                &action,
                *status == MessageStatus::InProgress,
            )
            .await?;
        }
        SessionEventKind::Message {
            message_id,
            actor: crate::EventActor::User,
            status: status @ (MessageStatus::Complete | MessageStatus::Failed),
            ..
        } => {
            advance_action(
                transaction,
                event.session_id,
                *message_id,
                if *status == MessageStatus::Complete {
                    SessionActionState::Completed
                } else {
                    SessionActionState::Failed
                },
                None,
            )
            .await?;
        }
        SessionEventKind::ProviderEvent { kind, payload, .. } if kind == "network_retry" => {
            // Retry admission is explicit: an ordinary in-progress replay must
            // still never resurrect a terminal action.
            if let Some(ids) = payload
                .get("message_ids")
                .and_then(serde_json::Value::as_array)
            {
                for id in ids {
                    let id: Uuid = serde_json::from_value(id.clone())?;
                    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
                        "select {ACTION_COLUMNS} from session_actions \
                         where action_id = $1 and session_id = $2 for update"
                    )))
                    .bind(id)
                    .bind(event.session_id)
                    .fetch_optional(&mut **transaction)
                    .await?;
                    if let Some(row) = row {
                        requeue_failed_action(transaction, decode_action(&row)?).await?;
                    }
                }
            }
        }
        SessionEventKind::TurnStarted { message_id, .. } => {
            advance_action(
                transaction,
                event.session_id,
                *message_id,
                SessionActionState::Running,
                None,
            )
            .await?;
        }
        SessionEventKind::TurnCompleted {
            message_id, error, ..
        } => {
            let target = if error.is_some() {
                SessionActionState::Failed
            } else {
                SessionActionState::Completed
            };
            advance_action(
                transaction,
                event.session_id,
                *message_id,
                target,
                error.clone(),
            )
            .await?;
        }
        SessionEventKind::PromptRecalled { message_id, .. } => {
            let row = sqlx::query(sqlx::AssertSqlSafe(format!(
                "select {ACTION_COLUMNS} from session_actions where action_id = $1 for update"
            )))
            .bind(message_id)
            .fetch_optional(&mut **transaction)
            .await?;
            let Some(row) = row else { return Ok(()) };
            let mut action = decode_action(&row)?;
            if !action.state.is_terminal() {
                let current = action.state;
                action.transition(
                    Some(current),
                    SessionActionState::Cancelled,
                    Some("recalled before execution".to_string()),
                )?;
                append_action_transition(transaction, &action, current, action.error.clone())
                    .await?;
                update_action_row(transaction, &action).await?;
            }
        }
        SessionEventKind::ProviderEvent { kind, payload, .. } if kind == "context_compaction" => {
            match payload.get("status").and_then(serde_json::Value::as_str) {
                Some("started") => {
                    let action = SessionAction::new(
                        event.id,
                        event.session_id,
                        SessionActionKind::Compaction,
                        crate::ActionDeliveryPolicy::WhenRunIdle,
                        crate::ActionWakePolicy::Immediate,
                        payload.clone(),
                    );
                    create_action_and_advance(
                        transaction,
                        action,
                        SessionActionState::Running,
                        None,
                    )
                    .await?;
                }
                Some("completed") => {
                    if let Some(action_id) =
                        latest_action_id(transaction, event.session_id, SessionActionKind::Compaction)
                            .await?
                    {
                        advance_action(
                            transaction,
                            event.session_id,
                            action_id,
                            SessionActionState::Completed,
                            None,
                        )
                        .await?;
                    }
                }
                _ => {}
            }
        }
        SessionEventKind::ProviderEvent { kind, payload, .. }
            if kind == "context_compaction_failed" =>
        {
            if let Some(action_id) =
                latest_action_id(transaction, event.session_id, SessionActionKind::Compaction)
                    .await?
            {
                advance_action(
                    transaction,
                    event.session_id,
                    action_id,
                    SessionActionState::Failed,
                    payload
                        .get("error")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string),
                )
                .await?;
            }
        }
        SessionEventKind::BluWorkflowStarted {
            workflow_id,
            source_hash,
            name,
        } => {
            let action = stamped(
                SessionAction::new(
                    *workflow_id,
                    event.session_id,
                    SessionActionKind::Workflow,
                    crate::ActionDeliveryPolicy::WhenRunIdle,
                    crate::ActionWakePolicy::Immediate,
                    serde_json::json!({
                        "workflow_id": workflow_id,
                        "source_hash": source_hash,
                        "name": name,
                    }),
                ),
                event.created_at,
            );
            create_action_and_advance(transaction, action, SessionActionState::Running, None)
                .await?;
        }
        SessionEventKind::BluWorkflowCompleted {
            workflow_id,
            success,
            error,
            ..
        } => {
            advance_action(
                transaction,
                event.session_id,
                *workflow_id,
                if *success {
                    SessionActionState::Completed
                } else {
                    SessionActionState::Failed
                },
                error.clone(),
            )
            .await?;
        }
        SessionEventKind::RuntimeWorkflowStarted {
            workflow_id,
            runtime,
            artifact_hash,
            name,
        } => {
            let action = stamped(
                SessionAction::new(
                    *workflow_id,
                    event.session_id,
                    SessionActionKind::Workflow,
                    crate::ActionDeliveryPolicy::WhenRunIdle,
                    crate::ActionWakePolicy::Immediate,
                    serde_json::json!({
                        "workflow_id": workflow_id,
                        "runtime": runtime,
                        "artifact_hash": artifact_hash,
                        "name": name,
                    }),
                ),
                event.created_at,
            );
            create_action_and_advance(transaction, action, SessionActionState::Running, None)
                .await?;
        }
        SessionEventKind::RuntimeWorkflowCompleted {
            workflow_id,
            success,
            error,
            ..
        } => {
            advance_action(
                transaction,
                event.session_id,
                *workflow_id,
                if *success {
                    SessionActionState::Completed
                } else {
                    SessionActionState::Failed
                },
                error.clone(),
            )
            .await?;
        }
        SessionEventKind::ContextCleared
        | SessionEventKind::GoalUpdated { .. }
        | SessionEventKind::GoalCleared { .. }
        | SessionEventKind::PlanUpdated { .. }
        | SessionEventKind::SubagentControl { .. } => {
            let kind = if matches!(event.kind, SessionEventKind::ContextCleared) {
                SessionActionKind::Revert
            } else {
                SessionActionKind::Command
            };
            let action = SessionAction::new(
                event.id,
                event.session_id,
                kind,
                crate::ActionDeliveryPolicy::WhenRunIdle,
                crate::ActionWakePolicy::Immediate,
                serde_json::to_value(&event.kind)?,
            );
            create_action_and_advance(transaction, action, SessionActionState::Completed, None)
                .await?;
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::PostgresSessionStore;
    use super::super::testing::{ScratchDatabase, test_url};
    use super::*;
    use crate::session_store::SessionStore;
    use crate::{EventActor, PromptDelivery};
    use crate::CodingProvider;

    async fn running_session(url: &str) -> (ScratchDatabase, PostgresSessionStore, Uuid) {
        let scratch = ScratchDatabase::create(url).await;
        let store = PostgresSessionStore::connect(&scratch.url)
            .await
            .expect("connect");
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
        (scratch, store, session_id)
    }

    fn prompt(session_id: Uuid, message_id: Uuid, status: MessageStatus) -> SessionEvent {
        SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "run the migration".to_string(),
                attachments: Vec::new(),
                status,
                delivery: Some(PromptDelivery::Queue),
            },
        )
    }

    fn turn_started(session_id: Uuid, message_id: Uuid) -> SessionEvent {
        SessionEvent::new(
            session_id,
            0,
            SessionEventKind::TurnStarted {
                message_id,
                provider: CodingProvider::Claude,
                model: None,
                effort: None,
                fast: false,
            },
        )
    }

    fn turn_completed(session_id: Uuid, message_id: Uuid, error: Option<&str>) -> SessionEvent {
        SessionEvent::new(
            session_id,
            0,
            SessionEventKind::TurnCompleted {
                message_id,
                provider_session_id: None,
                final_text: "done".to_string(),
                error: error.map(str::to_string),
            },
        )
    }

    #[tokio::test]
    async fn a_durable_prompt_creates_its_action_and_the_turn_drives_it_to_completion() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, session_id) = running_session(&url).await;
        let message_id = Uuid::new_v4();

        store
            .append(prompt(session_id, message_id, MessageStatus::Queued))
            .await
            .expect("prompt");
        // The journal entry IS the admission, so the action starts past queued.
        let action = store
            .action(session_id, message_id)
            .await
            .expect("action")
            .expect("a durable prompt must create its action");
        assert_eq!(action.state, SessionActionState::Admitted);
        assert_eq!(action.kind, SessionActionKind::Prompt);

        store
            .append(turn_started(session_id, message_id))
            .await
            .expect("turn started");
        assert_eq!(
            store.action(session_id, message_id).await.unwrap().unwrap().state,
            SessionActionState::Running
        );

        store
            .append(turn_completed(session_id, message_id, None))
            .await
            .expect("turn completed");
        assert_eq!(
            store.action(session_id, message_id).await.unwrap().unwrap().state,
            SessionActionState::Completed
        );

        // Intermediate boundaries are synthesised, not skipped: that audit
        // trail is what makes a crash mid-turn reconstructable.
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
            ]
        );
        assert!(store.pending_actions(session_id, 10).await.unwrap().is_empty());
        scratch.discard().await;
    }

    #[tokio::test]
    async fn a_failed_turn_fails_its_action_and_a_retry_requeues_the_same_row() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, session_id) = running_session(&url).await;
        let message_id = Uuid::new_v4();
        store
            .append(prompt(session_id, message_id, MessageStatus::Queued))
            .await
            .expect("prompt");
        store
            .append(turn_started(session_id, message_id))
            .await
            .expect("turn started");
        store
            .append(turn_completed(session_id, message_id, Some("connection reset")))
            .await
            .expect("turn failed");
        let failed = store.action(session_id, message_id).await.unwrap().unwrap();
        assert_eq!(failed.state, SessionActionState::Failed);
        assert_eq!(failed.error.as_deref(), Some("connection reset"));

        // A network retry is an explicit re-admission boundary: the failed row
        // returns to the executable state machine rather than being abandoned.
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ProviderEvent {
                    provider: CodingProvider::Claude,
                    kind: "network_retry".to_string(),
                    payload: serde_json::json!({"message_ids": [message_id]}),
                },
            ))
            .await
            .expect("network retry");
        assert_eq!(
            store.action(session_id, message_id).await.unwrap().unwrap().state,
            SessionActionState::Queued,
            "a retry must requeue the existing action, not strand it"
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn recalling_a_prompt_cancels_its_pending_work() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, session_id) = running_session(&url).await;
        let message_id = Uuid::new_v4();
        store
            .append(prompt(session_id, message_id, MessageStatus::Queued))
            .await
            .expect("prompt");
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::PromptRecalled {
                    message_id,
                    text: "run the migration".to_string(),
                    attachments: Vec::new(),
                },
            ))
            .await
            .expect("recall");
        let action = store.action(session_id, message_id).await.unwrap().unwrap();
        assert_eq!(action.state, SessionActionState::Cancelled);
        assert_eq!(action.error.as_deref(), Some("recalled before execution"));
        assert!(store.pending_actions(session_id, 10).await.unwrap().is_empty());
        scratch.discard().await;
    }

    #[tokio::test]
    async fn a_late_turn_event_cannot_resurrect_finished_work() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, session_id) = running_session(&url).await;
        let message_id = Uuid::new_v4();
        store
            .append(prompt(session_id, message_id, MessageStatus::Queued))
            .await
            .expect("prompt");
        store
            .append(turn_completed(session_id, message_id, None))
            .await
            .expect("turn completed");
        let transitions_before = store
            .action_transitions(session_id, message_id)
            .await
            .unwrap()
            .len();

        // A replayed or delayed start arriving after the terminal boundary must
        // be ignored rather than reopening the action.
        store
            .append(turn_started(session_id, message_id))
            .await
            .expect("late turn started");
        let action = store.action(session_id, message_id).await.unwrap().unwrap();
        assert_eq!(action.state, SessionActionState::Completed);
        assert_eq!(
            store
                .action_transitions(session_id, message_id)
                .await
                .unwrap()
                .len(),
            transitions_before,
            "a late event must not add audit records to terminal work"
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn a_compaction_pairs_its_start_and_completion_without_an_action_id() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, session_id) = running_session(&url).await;
        let started = store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ProviderEvent {
                    provider: CodingProvider::Claude,
                    kind: "context_compaction".to_string(),
                    payload: serde_json::json!({"status": "started"}),
                },
            ))
            .await
            .expect("compaction started");
        let action = store
            .action(session_id, started.id)
            .await
            .expect("action")
            .expect("compaction must open an action");
        assert_eq!(action.state, SessionActionState::Running);
        assert_eq!(action.kind, SessionActionKind::Compaction);

        // The completion event carries no action id, so it is attributed to the
        // session's open compaction.
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ProviderEvent {
                    provider: CodingProvider::Claude,
                    kind: "context_compaction".to_string(),
                    payload: serde_json::json!({"status": "completed"}),
                },
            ))
            .await
            .expect("compaction completed");
        assert_eq!(
            store.action(session_id, started.id).await.unwrap().unwrap().state,
            SessionActionState::Completed
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn a_steer_and_a_queued_prompt_are_distinct_action_classes() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, session_id) = running_session(&url).await;
        let steer_id = Uuid::new_v4();
        let mut steer = prompt(session_id, steer_id, MessageStatus::Queued);
        if let SessionEventKind::Message { delivery, .. } = &mut steer.kind {
            *delivery = Some(PromptDelivery::Steer);
        }
        store.append(steer).await.expect("steer");
        let action = store.action(session_id, steer_id).await.unwrap().unwrap();
        assert_eq!(action.kind, SessionActionKind::Steering);
        assert_eq!(action.delivery, crate::ActionDeliveryPolicy::WhenRunIdle);

        let queued_id = Uuid::new_v4();
        store
            .append(prompt(session_id, queued_id, MessageStatus::Queued))
            .await
            .expect("prompt");
        let action = store.action(session_id, queued_id).await.unwrap().unwrap();
        assert_eq!(action.kind, SessionActionKind::Prompt);
        assert_eq!(action.delivery, crate::ActionDeliveryPolicy::NextTurnBoundary);
        assert_eq!(store.pending_actions(session_id, 10).await.unwrap().len(), 2);
        scratch.discard().await;
    }
}
