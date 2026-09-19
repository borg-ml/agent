//! Workflow admission and the fenced commit point.
//!
//! WHY THIS EXISTS: a workflow is a long-running unit of work identified by a
//! caller-chosen id, executed by a worker that can die and be replaced. Three
//! things therefore have to be true, and all three are properties of a
//! transaction rather than of application code:
//!
//! * A workflow id admits EXACTLY ONCE. Two callers racing to start the same
//!   workflow must not both journal a Started event, or replay would show one
//!   workflow beginning twice.
//! * A workflow's durable action is created and driven to `Running` in one
//!   step, so no observer ever sees a workflow that exists but has no state.
//! * A worker that has been superseded must not be able to publish a terminal
//!   event. `append_with_action_lease` is the fence: the lease is validated
//!   under the action's row lock in the SAME transaction that appends the
//!   event, so a stale worker cannot pass the check and then win the write.
//!
//! On SQLite all three came free from the single file writer. Here they are
//! bought explicitly with `for update` row locks, which is what lets two
//! workflows on two sessions admit and commit concurrently instead of queueing.

use anyhow::{Result, ensure};
use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

use super::PostgresSessionStore;
use super::actions::{ACTION_COLUMNS, decode_action, load_action_for_update, validate_live_lease};
use crate::session_store::{
    EventPersistence, SessionAction, SessionActionState, SessionEvent, SessionEventKind,
    event_kind, workflow_event_id,
};

impl PostgresSessionStore {
    /// Return this workflow's durable action, creating it if it is new.
    ///
    /// Idempotent by workflow id, but only for an IDENTICAL request: a repeat
    /// with different metadata is an error rather than an overwrite, because
    /// the action is what a resuming worker replays from. The terminal check is
    /// the other half of that -- a finished action means the workflow already
    /// ended, so handing it back as if it were live would run it twice.
    pub(crate) async fn ensure_workflow_action(
        &self,
        session_id: Uuid,
        workflow_id: Uuid,
        payload: &serde_json::Value,
    ) -> Result<SessionAction> {
        let mut transaction = self.pool().begin().await?;
        let existing = sqlx::query(sqlx::AssertSqlSafe(format!(
            "select {ACTION_COLUMNS} from session_actions \
             where action_id = $1 and session_id = $2 for update"
        )))
        .bind(workflow_id)
        .bind(session_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(row) = existing {
            let action = decode_action(&row)?;
            ensure!(
                action.kind == crate::SessionActionKind::Workflow && action.payload == *payload,
                "workflow action {workflow_id} has conflicting durable metadata"
            );
            ensure!(
                !action.state.is_terminal(),
                "workflow action {workflow_id} is terminal without a workflow completion record"
            );
            transaction.commit().await?;
            return Ok(action);
        }
        let exists: bool =
            sqlx::query_scalar("select exists(select 1 from sessions where id = $1)")
                .bind(session_id)
                .fetch_one(&mut *transaction)
                .await?;
        ensure!(exists, "session {session_id} does not exist");
        let action = SessionAction::new(
            workflow_id,
            session_id,
            crate::SessionActionKind::Workflow,
            crate::ActionDeliveryPolicy::WhenRunIdle,
            crate::ActionWakePolicy::Immediate,
            payload.clone(),
        );
        super::actions::insert_action_row(&mut transaction, &action).await?;
        super::actions::insert_initial_action_transitions(&mut transaction, &action).await?;
        // Driven to Running here rather than by the caller: a workflow action
        // that existed in `Queued` would be picked up by the pending-action
        // sweep as undelivered work and woken a second time.
        super::sync::advance_action(
            &mut transaction,
            session_id,
            workflow_id,
            SessionActionState::Running,
            None,
        )
        .await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "select {ACTION_COLUMNS} from session_actions \
             where action_id = $1 and session_id = $2"
        )))
        .bind(workflow_id)
        .bind(session_id)
        .fetch_one(&mut *transaction)
        .await?;
        let action = decode_action(&row)?;
        transaction.commit().await?;
        Ok(action)
    }

    /// Admit a workflow start exactly once.
    ///
    /// The scan and the append share one transaction, holding the session's
    /// row lock through both, so two callers cannot each conclude "not yet
    /// started" and both publish. The scan is over already-journaled Started
    /// events of the same kind rather than a uniqueness constraint, because
    /// the workflow id lives inside the event body.
    pub(crate) async fn ensure_workflow_started(
        &self,
        event: SessionEvent,
        workflow_id: Uuid,
    ) -> Result<SessionEvent> {
        ensure!(
            matches!(
                &event.kind,
                SessionEventKind::BluWorkflowStarted { .. }
                    | SessionEventKind::RuntimeWorkflowStarted { .. }
            ),
            "workflow admission requires a Started event"
        );
        let kind = event_kind(&event.kind)?;
        let session_id = event.session_id;
        let mut transaction = self.pool().begin().await?;
        // Take the session row lock FIRST. The append below needs it anyway,
        // and acquiring it before the scan is what makes "not started" a
        // conclusion no concurrent writer can invalidate.
        sqlx::query("select 1 from sessions where id = $1 for update")
            .bind(session_id)
            .fetch_optional(&mut *transaction)
            .await?;
        let rows = sqlx::query(
            "select event_json, event_body, dict_id from session_events \
             where session_id = $1 and event_kind = $2 order by sequence",
        )
        .bind(session_id)
        .bind(kind)
        .fetch_all(&mut *transaction)
        .await?;
        for row in &rows {
            // A cold row is decoded rather than skipped: treating a compressed
            // Started event as absent would admit the same workflow twice.
            let json: Option<serde_json::Value> = row.try_get("event_json")?;
            let value = match json {
                Some(value) => value,
                None => {
                    let bytes: Option<Vec<u8>> = row.try_get("event_body")?;
                    let dict_id: Option<i32> = row.try_get("dict_id")?;
                    let dictionary = match dict_id {
                        Some(dict_id) => Some(super::body::EventDictionary {
                            dict_id,
                            bytes: sqlx::query_scalar(
                                "select dict_bytes from session_event_dicts where dict_id = $1",
                            )
                            .bind(dict_id)
                            .fetch_one(&mut *transaction)
                            .await?,
                        }),
                        None => None,
                    };
                    serde_json::from_slice(&super::body::decompress(
                        &bytes.unwrap_or_default(),
                        dictionary.as_ref(),
                    )?)?
                }
            };
            let existing: SessionEvent = serde_json::from_value(value)?;
            if workflow_event_id(&existing.kind) == Some(workflow_id) {
                transaction.commit().await?;
                return Ok(existing);
            }
        }
        let admitted = self
            .append_durable_in_transaction(&mut transaction, event)
            .await?;
        transaction.commit().await?;
        Ok(admitted)
    }

    /// Append a durable event only while the caller still owns the lease.
    ///
    /// The lease check and the append are one transaction over the action's
    /// locked row, so there is no window between "I am still the owner" and
    /// "my event is committed". Without that, a worker paused long enough to
    /// lose its lease could wake and publish a terminal event over the work
    /// its replacement had already begun.
    pub(crate) async fn append_with_action_lease(
        &self,
        event: SessionEvent,
        action_id: Uuid,
        lease_owner: &str,
        lease_token: Uuid,
    ) -> Result<SessionEvent> {
        ensure!(
            event.kind.persistence() == EventPersistence::Durable,
            "leased workflow events must be durable"
        );
        let mut transaction = self.pool().begin().await?;
        let action = load_action_for_update(&mut transaction, event.session_id, action_id).await?;
        validate_live_lease(&action, lease_owner, lease_token, Utc::now())?;
        let event = self
            .append_durable_in_transaction(&mut transaction, event)
            .await?;
        transaction.commit().await?;
        Ok(event)
    }
}
