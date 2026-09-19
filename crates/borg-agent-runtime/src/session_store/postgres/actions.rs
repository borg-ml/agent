//! The durable action queue and its leases, on PostgreSQL.
//!
//! One semantic difference from SQLite matters for correctness. SQLite held a
//! database-wide write lock for the whole transaction, so a plain `select`
//! followed by an `update` could not interleave with another writer. Postgres
//! has no such lock, so every read-modify-write here takes `for update` on the
//! action row. Without it two workers could both read `running`, both apply a
//! transition, and one update would be silently lost -- exactly the class of
//! bug the durable lifecycle exists to prevent.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::postgres::PgRow;
use sqlx::{Postgres, Row, Transaction};
use std::time::Duration;
use uuid::Uuid;

use super::PostgresSessionStore;
use crate::session_action::{SessionAction, SessionActionState, SessionActionTransition};
use crate::session_store::{ClaimedActionTransition, enum_text, parse_enum};

/// Every column of `session_actions`, in one place, so a read and a write
/// cannot drift apart silently.
pub(super) const ACTION_COLUMNS: &str = "action_id, session_id, action_kind, state, delivery_policy, \
     wake_policy, payload_json, attempt, error, created_at, updated_at, accepted_at, \
     delivered_at, completed_at, lease_owner, lease_token, lease_heartbeat_at, lease_expires_at";

pub(super) fn decode_action(row: &PgRow) -> Result<SessionAction> {
    Ok(SessionAction {
        action_id: row.try_get("action_id")?,
        session_id: row.try_get("session_id")?,
        kind: parse_enum(row.try_get("action_kind")?)?,
        state: parse_enum(row.try_get("state")?)?,
        delivery: parse_enum(row.try_get("delivery_policy")?)?,
        wake: parse_enum(row.try_get("wake_policy")?)?,
        payload: row.try_get("payload_json")?,
        attempt: u32::try_from(row.try_get::<i64, _>("attempt")?)
            .context("negative session action attempt")?,
        error: row.try_get("error")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        accepted_at: row.try_get("accepted_at")?,
        delivered_at: row.try_get("delivered_at")?,
        completed_at: row.try_get("completed_at")?,
        lease_owner: row.try_get("lease_owner")?,
        lease_token: row.try_get("lease_token")?,
        lease_heartbeat_at: row.try_get("lease_heartbeat_at")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
    })
}

fn decode_action_transition(row: &PgRow) -> Result<SessionActionTransition> {
    Ok(SessionActionTransition {
        action_id: row.try_get("action_id")?,
        session_id: row.try_get("session_id")?,
        transition_no: u64::try_from(row.try_get::<i64, _>("transition_no")?)
            .context("negative action transition number")?,
        from: row
            .try_get::<Option<String>, _>("from_state")?
            .as_deref()
            .map(parse_enum)
            .transpose()?,
        to: parse_enum(row.try_get("to_state")?)?,
        error: row.try_get("error")?,
        created_at: row.try_get("created_at")?,
    })
}

pub(super) async fn insert_action_row(
    transaction: &mut Transaction<'_, Postgres>,
    action: &SessionAction,
) -> Result<()> {
    sqlx::query(
        "insert into session_actions \
         (action_id, session_id, action_kind, state, delivery_policy, wake_policy, \
          payload_json, attempt, error, created_at, updated_at, accepted_at, delivered_at, \
          completed_at, lease_owner, lease_token, lease_heartbeat_at, lease_expires_at) \
         values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)",
    )
    .bind(action.action_id)
    .bind(action.session_id)
    .bind(enum_text(&action.kind)?)
    .bind(enum_text(&action.state)?)
    .bind(enum_text(&action.delivery)?)
    .bind(enum_text(&action.wake)?)
    .bind(&action.payload)
    .bind(i64::from(action.attempt))
    .bind(action.error.as_deref())
    .bind(action.created_at)
    .bind(action.updated_at)
    .bind(action.accepted_at)
    .bind(action.delivered_at)
    .bind(action.completed_at)
    .bind(action.lease_owner.as_deref())
    .bind(action.lease_token)
    .bind(action.lease_heartbeat_at)
    .bind(action.lease_expires_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub(super) async fn update_action_row(
    transaction: &mut Transaction<'_, Postgres>,
    action: &SessionAction,
) -> Result<()> {
    sqlx::query(
        "update session_actions set state=$1, attempt=$2, error=$3, updated_at=$4, \
         accepted_at=$5, delivered_at=$6, completed_at=$7, lease_owner=$8, lease_token=$9, \
         lease_heartbeat_at=$10, lease_expires_at=$11 where action_id=$12 and session_id=$13",
    )
    .bind(enum_text(&action.state)?)
    .bind(i64::from(action.attempt))
    .bind(action.error.as_deref())
    .bind(action.updated_at)
    .bind(action.accepted_at)
    .bind(action.delivered_at)
    .bind(action.completed_at)
    .bind(action.lease_owner.as_deref())
    .bind(action.lease_token)
    .bind(action.lease_heartbeat_at)
    .bind(action.lease_expires_at)
    .bind(action.action_id)
    .bind(action.session_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

/// Load one action and hold its row until the transaction ends.
///
/// `for update` is the Postgres replacement for SQLite's database-wide write
/// lock; every caller that then writes the row depends on it.
pub(super) async fn load_action_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
    action_id: Uuid,
) -> Result<SessionAction> {
    let sql = format!(
        "select {ACTION_COLUMNS} from session_actions \
         where action_id = $1 and session_id = $2 for update"
    );
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(action_id)
        .bind(session_id)
        .fetch_optional(&mut **transaction)
        .await?
        .with_context(|| format!("action {action_id} does not exist in session {session_id}"))
        .and_then(|row| decode_action(&row))
}

async fn insert_action_transition(
    transaction: &mut Transaction<'_, Postgres>,
    action: &SessionAction,
    from_state: Option<SessionActionState>,
    to_state: SessionActionState,
    error: Option<String>,
    created_at: DateTime<Utc>,
    transition_no: i64,
) -> Result<()> {
    sqlx::query(
        "insert into session_action_transitions \
         (action_id, session_id, transition_no, from_state, to_state, error, created_at) \
         values ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(action.action_id)
    .bind(action.session_id)
    .bind(transition_no)
    .bind(from_state.map(|value| enum_text(&value)).transpose()?)
    .bind(enum_text(&to_state)?)
    .bind(error.as_deref())
    .bind(created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

/// The audit trail a new action starts with: always a `queued` origin, plus the
/// transition into its initial state when it was not enqueued queued.
pub(super) async fn insert_initial_action_transitions(
    transaction: &mut Transaction<'_, Postgres>,
    action: &SessionAction,
) -> Result<()> {
    let queued = SessionActionState::Queued;
    insert_action_transition(
        transaction,
        action,
        None,
        queued,
        None,
        action.created_at,
        0,
    )
    .await?;
    if action.state != queued {
        insert_action_transition(
            transaction,
            action,
            Some(queued),
            action.state,
            action.error.clone(),
            action.accepted_at.unwrap_or(action.updated_at),
            1,
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn append_action_transition(
    transaction: &mut Transaction<'_, Postgres>,
    action: &SessionAction,
    from_state: SessionActionState,
    error: Option<String>,
) -> Result<()> {
    let next_no: i64 = sqlx::query_scalar(
        "select coalesce(max(transition_no) + 1, 0) \
         from session_action_transitions where action_id=$1 and session_id=$2",
    )
    .bind(action.action_id)
    .bind(action.session_id)
    .fetch_one(&mut **transaction)
    .await?;
    insert_action_transition(
        transaction,
        action,
        Some(from_state),
        action.state,
        error,
        action.updated_at,
        next_no,
    )
    .await
}

pub(super) fn validate_live_lease(
    action: &SessionAction,
    lease_owner: &str,
    lease_token: Uuid,
    now: DateTime<Utc>,
) -> Result<()> {
    anyhow::ensure!(
        action.lease_owner.as_deref() == Some(lease_owner)
            && action.lease_token == Some(lease_token),
        "action {} lease is not owned by {lease_owner}",
        action.action_id
    );
    anyhow::ensure!(
        !action.lease_expired_at(now),
        "action {} lease has expired",
        action.action_id
    );
    anyhow::ensure!(
        !action.state.is_terminal(),
        "action {} is already terminal",
        action.action_id
    );
    Ok(())
}

/// Apply a transition to an action inside an existing transaction.
///
/// Separate from the trait method because some callers -- settling a terminal
/// session, for instance -- must cancel work as part of a larger atomic change.
pub(super) async fn transition_action_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
    action_id: Uuid,
    expected: Option<SessionActionState>,
    next: SessionActionState,
    error: Option<String>,
) -> Result<SessionAction> {
    let mut action = load_action_for_update(transaction, session_id, action_id).await?;
    let from_state = action.state;
    action.transition(expected, next, error)?;
    append_action_transition(transaction, &action, from_state, action.error.clone()).await?;
    update_action_row(transaction, &action).await?;
    Ok(action)
}

impl PostgresSessionStore {
    pub(super) async fn pg_enqueue_action(&self, action: SessionAction) -> Result<SessionAction> {
        anyhow::ensure!(
            action.state == SessionActionState::Queued,
            "newly enqueued action {} must start queued",
            action.action_id
        );
        let mut transaction = self.pool().begin().await?;
        let existing = sqlx::query(sqlx::AssertSqlSafe(format!(
            "select {ACTION_COLUMNS} from session_actions where action_id = $1 for update"
        )))
        .bind(action.action_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(row) = existing {
            // Enqueue is idempotent by action id, but the immutable fields must
            // match: reusing an id with different content would silently
            // replace durable work with different work.
            let existing = decode_action(&row)?;
            anyhow::ensure!(
                existing.session_id == action.session_id
                    && existing.kind == action.kind
                    && existing.delivery == action.delivery
                    && existing.wake == action.wake
                    && existing.payload == action.payload,
                "action {} was reused with different immutable payload",
                action.action_id
            );
            transaction.commit().await?;
            return Ok(existing);
        }
        let exists: bool =
            sqlx::query_scalar("select exists(select 1 from sessions where id = $1)")
                .bind(action.session_id)
                .fetch_one(&mut *transaction)
                .await?;
        anyhow::ensure!(exists, "session {} does not exist", action.session_id);
        insert_action_row(&mut transaction, &action).await?;
        insert_initial_action_transitions(&mut transaction, &action).await?;
        transaction.commit().await?;
        Ok(action)
    }

    pub(super) async fn pg_transition_action(
        &self,
        session_id: Uuid,
        action_id: Uuid,
        expected: Option<SessionActionState>,
        next: SessionActionState,
        error: Option<String>,
    ) -> Result<SessionAction> {
        let mut transaction = self.pool().begin().await?;
        let mut action = load_action_for_update(&mut transaction, session_id, action_id).await?;
        let from_state = action.state;
        action.transition(expected, next, error)?;
        append_action_transition(&mut transaction, &action, from_state, action.error.clone())
            .await?;
        update_action_row(&mut transaction, &action).await?;
        transaction.commit().await?;
        Ok(action)
    }

    pub(super) async fn pg_claim_action(
        &self,
        session_id: Uuid,
        action_id: Uuid,
        lease_owner: &str,
        lease_duration: Duration,
    ) -> Result<Option<SessionAction>> {
        anyhow::ensure!(
            !lease_owner.trim().is_empty(),
            "action lease owner is empty"
        );
        anyhow::ensure!(!lease_duration.is_zero(), "action lease duration is zero");
        let mut transaction = self.pool().begin().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "select {ACTION_COLUMNS} from session_actions \
             where action_id = $1 and session_id = $2 for update"
        )))
        .bind(action_id)
        .bind(session_id)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(row) = row else {
            transaction.commit().await?;
            return Ok(None);
        };
        let mut action = decode_action(&row)?;
        let now = Utc::now();
        if action.state.is_terminal() {
            transaction.commit().await?;
            return Ok(None);
        }
        // Re-claiming your own live lease is a no-op rather than an error, so a
        // worker that retries after a transient failure is not fenced out of
        // work it already owns.
        if action.lease_owner.as_deref() == Some(lease_owner) && !action.lease_expired_at(now) {
            transaction.commit().await?;
            return Ok(Some(action));
        }
        if !action.lease_expired_at(now) {
            transaction.commit().await?;
            return Ok(None);
        }
        action.claim(lease_owner.to_string(), Uuid::new_v4(), now, lease_duration)?;
        update_action_row(&mut transaction, &action).await?;
        transaction.commit().await?;
        Ok(Some(action))
    }

    pub(super) async fn pg_heartbeat_action(
        &self,
        session_id: Uuid,
        action_id: Uuid,
        lease_owner: &str,
        lease_token: Uuid,
        lease_duration: Duration,
    ) -> Result<SessionAction> {
        let mut transaction = self.pool().begin().await?;
        let mut action = load_action_for_update(&mut transaction, session_id, action_id).await?;
        action.heartbeat(lease_owner, lease_token, Utc::now(), lease_duration)?;
        update_action_row(&mut transaction, &action).await?;
        transaction.commit().await?;
        Ok(action)
    }

    pub(super) async fn pg_transition_claimed_action(
        &self,
        transition: ClaimedActionTransition,
    ) -> Result<SessionAction> {
        let ClaimedActionTransition {
            session_id,
            action_id,
            lease_owner,
            lease_token,
            expected,
            next,
            error,
        } = transition;
        let mut transaction = self.pool().begin().await?;
        let mut action = load_action_for_update(&mut transaction, session_id, action_id).await?;
        // The token fences a worker that was paused past its expiry and resumed
        // after another worker legitimately reclaimed the action.
        validate_live_lease(&action, &lease_owner, lease_token, Utc::now())?;
        let from_state = action.state;
        action.transition(expected, next, error)?;
        append_action_transition(&mut transaction, &action, from_state, action.error.clone())
            .await?;
        update_action_row(&mut transaction, &action).await?;
        transaction.commit().await?;
        Ok(action)
    }

    pub(super) async fn pg_recover_expired_actions(
        &self,
        session_id: Uuid,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<SessionAction>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut transaction = self.pool().begin().await?;
        // `skip locked` is a genuine improvement over the SQLite original:
        // two hosts sweeping at once take disjoint work instead of one waiting
        // on the other's transaction.
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "select {ACTION_COLUMNS} from session_actions \
             where session_id = $1 \
               and state in ('running', 'committing') \
               and (lease_expires_at is null or lease_expires_at <= $2) \
             order by lease_expires_at, created_at, action_id limit $3 \
             for update skip locked"
        )))
        .bind(session_id)
        .bind(now)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&mut *transaction)
        .await?;
        let mut recovered = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut action = decode_action(row)?;
            if !action.lease_expired_at(now) {
                continue;
            }
            let from_state = action.state;
            action.transition(
                Some(from_state),
                SessionActionState::Queued,
                Some("action lease expired; requeued for recovery".to_string()),
            )?;
            append_action_transition(&mut transaction, &action, from_state, action.error.clone())
                .await?;
            update_action_row(&mut transaction, &action).await?;
            recovered.push(action);
        }
        transaction.commit().await?;
        Ok(recovered)
    }

    pub(super) async fn pg_action(
        &self,
        session_id: Uuid,
        action_id: Uuid,
    ) -> Result<Option<SessionAction>> {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "select {ACTION_COLUMNS} from session_actions \
             where action_id = $1 and session_id = $2"
        )))
        .bind(action_id)
        .bind(session_id)
        .fetch_optional(self.pool())
        .await?
        .as_ref()
        .map(decode_action)
        .transpose()
    }

    pub(super) async fn pg_action_transitions(
        &self,
        session_id: Uuid,
        action_id: Uuid,
    ) -> Result<Vec<SessionActionTransition>> {
        let rows = sqlx::query(
            "select action_id, session_id, transition_no, from_state, to_state, error, created_at \
             from session_action_transitions \
             where session_id = $1 and action_id = $2 order by transition_no",
        )
        .bind(session_id)
        .bind(action_id)
        .fetch_all(self.pool())
        .await?;
        rows.iter().map(decode_action_transition).collect()
    }

    pub(super) async fn pg_pending_actions(
        &self,
        session_id: Uuid,
        limit: usize,
    ) -> Result<Vec<SessionAction>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "select {ACTION_COLUMNS} from session_actions where session_id = $1 \
             and state not in ('completed', 'failed', 'cancelled') \
             order by created_at, action_id limit $2"
        )))
        .bind(session_id)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(self.pool())
        .await?;
        rows.iter().map(decode_action).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::super::testing::{ScratchDatabase, test_url};
    use super::*;
    use crate::session_action::{ActionDeliveryPolicy, ActionWakePolicy, SessionActionKind};
    use crate::session_store::SessionStore;
    use std::sync::Arc;

    fn action(session_id: Uuid) -> SessionAction {
        SessionAction::new(
            Uuid::new_v4(),
            session_id,
            SessionActionKind::Prompt,
            ActionDeliveryPolicy::NextTurnBoundary,
            ActionWakePolicy::OnLowerBoundary,
            serde_json::json!({"text": "do the thing"}),
        )
    }

    async fn session(url: &str) -> (ScratchDatabase, PostgresSessionStore, Uuid) {
        let scratch = ScratchDatabase::create(url).await;
        let store = PostgresSessionStore::connect_with_pool_size(&scratch.url, 4)
            .await
            .expect("connect");
        let session_id = Uuid::new_v4();
        store.create_session(session_id).await.expect("create");
        (scratch, store, session_id)
    }

    #[tokio::test]
    async fn enqueue_is_idempotent_and_rejects_a_reused_id_with_new_content() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, session_id) = session(&url).await;
        let queued = action(session_id);
        let stored = store.enqueue_action(queued.clone()).await.expect("enqueue");
        assert_eq!(stored.state, SessionActionState::Queued);

        // A retried enqueue must return the same durable row, not a second one.
        let again = store.enqueue_action(queued.clone()).await.expect("enqueue");
        assert_eq!(again.action_id, stored.action_id);
        assert_eq!(
            store.pending_actions(session_id, 10).await.unwrap().len(),
            1
        );

        // Reusing the id for different work would silently replace durable
        // work with other work, so it is refused.
        let mut mutated = queued.clone();
        mutated.payload = serde_json::json!({"text": "something else entirely"});
        assert!(store.enqueue_action(mutated).await.is_err());

        // An action for an unknown session has nothing to attach to.
        assert!(store.enqueue_action(action(Uuid::new_v4())).await.is_err());
        scratch.discard().await;
    }

    #[tokio::test]
    async fn the_lifecycle_is_enforced_and_every_step_is_audited() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, session_id) = session(&url).await;
        let queued = store
            .enqueue_action(action(session_id))
            .await
            .expect("enqueue");
        let id = queued.action_id;

        store
            .transition_action(
                session_id,
                id,
                Some(SessionActionState::Queued),
                SessionActionState::Admitted,
                None,
            )
            .await
            .expect("admit");
        // Queued -> Completed is not a legal edge; a crash must not be able to
        // complete work that never ran.
        assert!(
            store
                .transition_action(
                    session_id,
                    id,
                    Some(SessionActionState::Queued),
                    SessionActionState::Completed,
                    None
                )
                .await
                .is_err(),
            "a stale expected-state must not transition the row"
        );
        // Admitted -> Running is not a legal edge either: work must be
        // delivered before it can run.
        assert!(
            store
                .transition_action(
                    session_id,
                    id,
                    Some(SessionActionState::Admitted),
                    SessionActionState::Running,
                    None
                )
                .await
                .is_err()
        );
        store
            .transition_action(
                session_id,
                id,
                Some(SessionActionState::Admitted),
                SessionActionState::Delivered,
                None,
            )
            .await
            .expect("deliver");
        store
            .transition_action(
                session_id,
                id,
                Some(SessionActionState::Delivered),
                SessionActionState::Running,
                None,
            )
            .await
            .expect("run");
        let done = store
            .transition_action(
                session_id,
                id,
                Some(SessionActionState::Running),
                SessionActionState::Completed,
                None,
            )
            .await
            .expect("complete");
        assert_eq!(done.state, SessionActionState::Completed);

        let transitions = store
            .action_transitions(session_id, id)
            .await
            .expect("audit");
        let states: Vec<SessionActionState> = transitions.iter().map(|t| t.to).collect();
        assert_eq!(
            states,
            vec![
                SessionActionState::Queued,
                SessionActionState::Admitted,
                SessionActionState::Delivered,
                SessionActionState::Running,
                SessionActionState::Completed,
            ],
            "the audit trail must record every boundary exactly once"
        );
        let numbers: Vec<u64> = transitions.iter().map(|t| t.transition_no).collect();
        assert_eq!(numbers, vec![0, 1, 2, 3, 4]);

        // A terminal action is no longer pending work.
        assert!(
            store
                .pending_actions(session_id, 10)
                .await
                .unwrap()
                .is_empty()
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn a_lease_excludes_other_workers_until_it_expires() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, session_id) = session(&url).await;
        let id = store
            .enqueue_action(action(session_id))
            .await
            .expect("enqueue")
            .action_id;

        let lease = Duration::from_secs(3_600);
        let claimed = store
            .claim_action(session_id, id, "worker-a", lease)
            .await
            .expect("claim")
            .expect("claimable");
        assert_eq!(claimed.lease_owner.as_deref(), Some("worker-a"));

        // Another worker must not take live work.
        assert!(
            store
                .claim_action(session_id, id, "worker-b", lease)
                .await
                .expect("claim")
                .is_none()
        );
        // The owner re-claiming its own live lease is a no-op, so a retry after
        // a transient failure is not fenced out of work it already owns.
        let again = store
            .claim_action(session_id, id, "worker-a", lease)
            .await
            .expect("claim")
            .expect("own lease");
        assert_eq!(again.lease_token, claimed.lease_token);

        // A heartbeat from the wrong token must not extend the lease.
        assert!(
            store
                .heartbeat_action(session_id, id, "worker-a", Uuid::new_v4(), lease)
                .await
                .is_err()
        );

        // Expire the lease by ageing the durable row rather than by sleeping:
        // claim_action reads the wall clock internally, so a timing-based test
        // would race the scheduler under load instead of testing the boundary.
        sqlx::query(
            "update session_actions set lease_expires_at = now() - interval '1 hour' \
             where action_id = $1",
        )
        .bind(id)
        .execute(store.pool())
        .await
        .expect("age the lease");

        let stolen = store
            .claim_action(session_id, id, "worker-b", lease)
            .await
            .expect("claim")
            .expect("expired lease is claimable");
        assert_eq!(stolen.lease_owner.as_deref(), Some("worker-b"));
        assert_ne!(stolen.lease_token, claimed.lease_token);

        // The original owner is now fenced: its token is stale.
        assert!(
            store
                .transition_claimed_action(ClaimedActionTransition {
                    session_id,
                    action_id: id,
                    lease_owner: "worker-a".to_string(),
                    lease_token: claimed.lease_token.expect("token"),
                    expected: None,
                    next: SessionActionState::Completed,
                    error: None,
                })
                .await
                .is_err(),
            "a resumed worker must not complete work another worker now owns"
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn expired_in_flight_work_is_requeued_for_recovery() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, session_id) = session(&url).await;
        let id = store
            .enqueue_action(action(session_id))
            .await
            .expect("enqueue")
            .action_id;
        store
            .transition_action(session_id, id, None, SessionActionState::Admitted, None)
            .await
            .expect("admit");
        store
            .transition_action(session_id, id, None, SessionActionState::Delivered, None)
            .await
            .expect("deliver");
        store
            .transition_action(session_id, id, None, SessionActionState::Running, None)
            .await
            .expect("run");
        store
            .claim_action(session_id, id, "crashed-worker", Duration::from_secs(3_600))
            .await
            .expect("claim")
            .expect("claimable");

        // Expiry is driven by the caller's clock rather than by sleeping, so
        // this asserts the real boundary instead of racing a timer under load.
        assert!(
            store
                .recover_expired_actions(session_id, Utc::now(), 10)
                .await
                .expect("recover")
                .is_empty(),
            "work whose lease is still live must not be requeued"
        );

        let after_expiry = Utc::now() + chrono::Duration::hours(2);
        let recovered = store
            .recover_expired_actions(session_id, after_expiry, 10)
            .await
            .expect("recover");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].state, SessionActionState::Queued);
        assert!(
            recovered[0]
                .error
                .as_deref()
                .is_some_and(|error| error.contains("lease expired")),
            "recovery must record why the work was requeued"
        );
        let reloaded = store
            .action(session_id, id)
            .await
            .expect("action")
            .expect("present");
        assert_eq!(reloaded.state, SessionActionState::Queued);
        scratch.discard().await;
    }

    /// The race `for update` exists to prevent. SQLite could rely on its
    /// database-wide write lock; here two workers can genuinely interleave a
    /// read and a write, and exactly one must win.
    #[tokio::test]
    async fn only_one_of_many_racing_workers_claims_an_action() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, session_id) = session(&url).await;
        let store = Arc::new(store);
        let id = store
            .enqueue_action(action(session_id))
            .await
            .expect("enqueue")
            .action_id;

        let mut workers = tokio::task::JoinSet::new();
        for worker in 0..8 {
            let store = Arc::clone(&store);
            workers.spawn(async move {
                store
                    .claim_action(
                        session_id,
                        id,
                        &format!("worker-{worker}"),
                        Duration::from_secs(30),
                    )
                    .await
                    .expect("claim")
                    .map(|action| action.lease_token.expect("token"))
            });
        }
        let mut winners = Vec::new();
        while let Some(joined) = workers.join_next().await {
            if let Some(token) = joined.expect("worker panicked") {
                winners.push(token);
            }
        }
        assert_eq!(
            winners.len(),
            1,
            "exactly one worker may hold a lease, got {winners:?}"
        );

        // And the durable row agrees with the single winner.
        let stored = store.action(session_id, id).await.unwrap().unwrap();
        assert_eq!(stored.lease_token, Some(winners[0]));
        scratch.discard().await;
    }

    /// Racing transitions must not lose an update: without a row lock two
    /// workers could both read `admitted` and both write, and one transition
    /// would vanish from the audit trail.
    #[tokio::test]
    async fn racing_transitions_never_lose_an_audit_record() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, session_id) = session(&url).await;
        let store = Arc::new(store);
        let id = store
            .enqueue_action(action(session_id))
            .await
            .expect("enqueue")
            .action_id;

        let mut workers = tokio::task::JoinSet::new();
        for next in [
            SessionActionState::Admitted,
            SessionActionState::Cancelled,
            SessionActionState::Admitted,
        ] {
            let store = Arc::clone(&store);
            workers.spawn(async move {
                store
                    .transition_action(session_id, id, None, next, None)
                    .await
                    .is_ok()
            });
        }
        let mut accepted = 0;
        while let Some(joined) = workers.join_next().await {
            if joined.expect("worker panicked") {
                accepted += 1;
            }
        }

        let transitions = store
            .action_transitions(session_id, id)
            .await
            .expect("audit");
        // One origin record plus exactly one record per accepted transition,
        // numbered without gaps or collisions.
        assert_eq!(transitions.len(), accepted + 1);
        let numbers: Vec<u64> = transitions.iter().map(|t| t.transition_no).collect();
        assert_eq!(numbers, (0..=accepted as u64).collect::<Vec<_>>());
        scratch.discard().await;
    }
}
