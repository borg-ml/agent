//! The autonomous job queue, on PostgreSQL.
//!
//! This tier is a lease-fenced work queue, so it gets the same treatment as the
//! session action queue: every read-modify-write takes a row lock, and the
//! scheduler's claim sweep uses `for update skip locked`. That last part is a
//! real improvement on the original rather than a translation -- SQLite's
//! version selects candidates, then discovers it lost a race by checking
//! `rows_affected`, because its database-wide lock made anything finer
//! pointless. Here two schedulers take disjoint work on the first try.
//!
//! Validation, hashing and lease rules are imported from `autonomy`, not
//! reimplemented: they decide whether work is admitted, retried or abandoned,
//! and two backends disagreeing about that would lose or duplicate real work.

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::postgres::{PgPool, PgRow};
use sqlx::{Postgres, Row, Transaction};
use std::time::Duration;
use uuid::Uuid;

use crate::autonomy::{
    AutonomyCheckpoint, AutonomyJob, AutonomyJobState, AutonomyJobTransition, AutonomyLease,
    AutonomyTransition, EnqueueAutonomyJob, SaveAutonomyCheckpoint, add_duration, checkpoint_hash,
    from_millis, from_optional_millis, parse_optional_uuid, parse_uuid, to_millis, to_u32, to_u64,
    validate_batch_size, validate_checkpoint, validate_enqueue, validate_lease_for_transition,
    validate_optional_text, validate_owner,
};

/// Matches the SQLite store's bounds.
const MAX_PAYLOAD_BYTES: usize = 256 * 1024;
const MAX_ERROR_BYTES: usize = 8 * 1024;
const MAX_CHECKPOINTS_PER_LIST: i64 = 512;

/// The durable autonomous job journal, backed by PostgreSQL.
#[derive(Debug, Clone)]
pub struct PostgresAutonomyStore {
    pool: PgPool,
}

fn decode_job(row: &PgRow) -> Result<AutonomyJob> {
    Ok(AutonomyJob {
        job_id: parse_uuid(row.try_get("job_id")?, "job_id")?,
        idempotency_key: row.try_get("idempotency_key")?,
        kind: row.try_get("kind")?,
        payload: serde_json::from_str(row.try_get::<String, _>("payload_json")?.as_str())
            .context("decode autonomy job payload")?,
        state: AutonomyJobState::parse(row.try_get::<String, _>("state")?.as_str())?,
        due_at: from_millis(row.try_get("due_at_ms")?, "due_at_ms")?,
        attempt: to_u32(row.try_get("attempt")?, "attempt")?,
        max_attempts: to_u32(row.try_get("max_attempts")?, "max_attempts")?,
        lease_owner: row.try_get("lease_owner")?,
        lease_token: parse_optional_uuid(row.try_get("lease_token")?, "lease_token")?,
        lease_heartbeat_at: from_optional_millis(
            row.try_get("lease_heartbeat_at_ms")?,
            "lease_heartbeat_at_ms",
        )?,
        lease_expires_at: from_optional_millis(
            row.try_get("lease_expires_at_ms")?,
            "lease_expires_at_ms",
        )?,
        session_id: parse_optional_uuid(row.try_get("session_id")?, "session_id")?,
        goal_id: parse_optional_uuid(row.try_get("goal_id")?, "goal_id")?,
        result: row
            .try_get::<Option<String>, _>("result_json")?
            .map(|value| serde_json::from_str(&value).context("decode autonomy job result"))
            .transpose()?,
        last_error: row.try_get("last_error")?,
        created_at: from_millis(row.try_get("created_at_ms")?, "created_at_ms")?,
        updated_at: from_millis(row.try_get("updated_at_ms")?, "updated_at_ms")?,
    })
}

fn decode_transition(row: &PgRow) -> Result<AutonomyJobTransition> {
    Ok(AutonomyJobTransition {
        job_id: parse_uuid(row.try_get("job_id")?, "job_id")?,
        sequence: to_u64(row.try_get("sequence")?, "sequence")?,
        from: row
            .try_get::<Option<String>, _>("from_state")?
            .as_deref()
            .map(AutonomyJobState::parse)
            .transpose()?,
        to: AutonomyJobState::parse(row.try_get::<String, _>("to_state")?.as_str())?,
        attempt: to_u32(row.try_get("attempt")?, "attempt")?,
        reason: row.try_get("reason")?,
        lease_owner: row.try_get("lease_owner")?,
        occurred_at: from_millis(row.try_get("occurred_at_ms")?, "occurred_at_ms")?,
    })
}

fn decode_checkpoint(row: &PgRow) -> Result<AutonomyCheckpoint> {
    Ok(AutonomyCheckpoint {
        checkpoint_id: parse_uuid(row.try_get("checkpoint_id")?, "checkpoint_id")?,
        job_id: parse_uuid(row.try_get("job_id")?, "job_id")?,
        checkpoint_key: row.try_get("checkpoint_key")?,
        session_id: parse_optional_uuid(row.try_get("session_id")?, "session_id")?,
        goal_id: parse_optional_uuid(row.try_get("goal_id")?, "goal_id")?,
        kind: row.try_get("kind")?,
        state: serde_json::from_str(row.try_get::<String, _>("state_json")?.as_str())
            .context("decode autonomy checkpoint state")?,
        evidence: serde_json::from_str(row.try_get::<String, _>("evidence_json")?.as_str())
            .context("decode autonomy checkpoint evidence")?,
        content_hash: row.try_get("content_hash")?,
        created_at: from_millis(row.try_get("created_at_ms")?, "created_at_ms")?,
    })
}

const JOB_COLUMNS: &str = "job_id, idempotency_key, kind, payload_json, state, due_at_ms, \
     attempt, max_attempts, lease_owner, lease_token, lease_heartbeat_at_ms, \
     lease_expires_at_ms, session_id, goal_id, result_json, last_error, created_at_ms, \
     updated_at_ms";

async fn load_job(
    transaction: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
) -> Result<AutonomyJob> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "select {JOB_COLUMNS} from autonomy_jobs where job_id = $1 for update"
    )))
    .bind(job_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .with_context(|| format!("autonomy job {job_id} does not exist"))?;
    decode_job(&row)
}

async fn append_transition(
    transaction: &mut Transaction<'_, Postgres>,
    transition: AutonomyTransition,
) -> Result<()> {
    let AutonomyTransition {
        job_id,
        from,
        to,
        attempt,
        reason,
        lease_owner,
        occurred_at,
    } = transition;
    let sequence: i64 = sqlx::query_scalar(
        "select coalesce(max(sequence) + 1, 0) from autonomy_job_transitions where job_id = $1",
    )
    .bind(job_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    sqlx::query(
        "insert into autonomy_job_transitions \
         (job_id, sequence, from_state, to_state, attempt, reason, lease_owner, occurred_at_ms) \
         values ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(job_id.to_string())
    .bind(sequence)
    .bind(from.map(|state| state.as_str().to_string()))
    .bind(to.as_str())
    .bind(i64::from(attempt))
    .bind(reason)
    .bind(lease_owner)
    .bind(to_millis(occurred_at))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

impl PostgresAutonomyStore {
    /// Adopt a pool whose satellite schema is already applied by the session
    /// store's single bootstrap path.
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn enqueue(&self, input: EnqueueAutonomyJob) -> Result<AutonomyJob> {
        validate_enqueue(&input)?;
        let payload_json = serde_json::to_string(&input.payload)?;
        let now = Utc::now();
        let job_id = input.job_id.unwrap_or_else(Uuid::new_v4);
        let mut transaction = self.pool.begin().await?;

        // Enqueue is idempotent by key: a retried submission returns the job
        // that already exists rather than queueing the same work twice.
        let existing = sqlx::query(sqlx::AssertSqlSafe(format!(
            "select {JOB_COLUMNS} from autonomy_jobs where idempotency_key = $1 for update"
        )))
        .bind(&input.idempotency_key)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(row) = existing {
            let job = decode_job(&row)?;
            // A retry must describe the same work. Returning the existing job
            // for a different request would silently drop the new one.
            // Stored at millisecond precision; compare at that precision or a
            // caller with a finer instant can never retry its own request.
            let due_at = from_millis(to_millis(input.due_at), "due_at_ms")?;
            ensure!(
                job.kind == input.kind
                    && job.payload == input.payload
                    && job.due_at == due_at
                    && job.max_attempts == input.max_attempts
                    && job.session_id == input.session_id
                    && job.goal_id == input.goal_id,
                "idempotency key already names a different autonomy job",
            );
            transaction.commit().await?;
            return Ok(job);
        }

        let due_at = input.due_at;
        sqlx::query(
            "insert into autonomy_jobs \
             (job_id, idempotency_key, kind, payload_json, state, due_at_ms, attempt, \
              max_attempts, session_id, goal_id, created_at_ms, updated_at_ms) \
             values ($1, $2, $3, $4, 'queued', $5, 0, $6, $7, $8, $9, $9)",
        )
        .bind(job_id.to_string())
        .bind(&input.idempotency_key)
        .bind(&input.kind)
        .bind(&payload_json)
        .bind(to_millis(due_at))
        .bind(i64::from(input.max_attempts))
        .bind(input.session_id.map(|id| id.to_string()))
        .bind(input.goal_id.map(|id| id.to_string()))
        .bind(to_millis(now))
        .execute(&mut *transaction)
        .await?;
        let job = load_job(&mut transaction, job_id).await?;
        append_transition(
            &mut transaction,
            AutonomyTransition {
                job_id,
                from: None,
                to: AutonomyJobState::Queued,
                attempt: 0,
                reason: None,
                lease_owner: None,
                occurred_at: now,
            },
        )
        .await?;
        transaction.commit().await?;
        Ok(job)
    }

    pub async fn get(&self, job_id: Uuid) -> Result<Option<AutonomyJob>> {
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "select {JOB_COLUMNS} from autonomy_jobs where job_id = $1"
        )))
        .bind(job_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(decode_job).transpose()
    }

    pub async fn claim_due(
        &self,
        now: DateTime<Utc>,
        lease_owner: &str,
        lease_duration: Duration,
        limit: u32,
    ) -> Result<Vec<AutonomyJob>> {
        self.claim_due_filtered(now, lease_owner, lease_duration, limit, None)
            .await
    }

    pub async fn claim_due_for_session(
        &self,
        now: DateTime<Utc>,
        lease_owner: &str,
        lease_duration: Duration,
        limit: u32,
        session_id: Uuid,
    ) -> Result<Vec<AutonomyJob>> {
        self.claim_due_filtered(now, lease_owner, lease_duration, limit, Some(session_id))
            .await
    }

    async fn claim_due_filtered(
        &self,
        now: DateTime<Utc>,
        lease_owner: &str,
        lease_duration: Duration,
        limit: u32,
        session_id: Option<Uuid>,
    ) -> Result<Vec<AutonomyJob>> {
        validate_owner(lease_owner)?;
        validate_batch_size(limit)?;
        ensure!(!lease_duration.is_zero(), "lease duration must be non-zero");
        let lease_expires_at = add_duration(now, lease_duration)?;
        let mut transaction = self.pool.begin().await?;

        // `skip locked` is the whole point: a second scheduler sweeping at the
        // same moment takes different jobs instead of contending for these.
        let rows = sqlx::query(
            "select job_id from autonomy_jobs \
             where state = 'queued' and due_at_ms <= $1 and attempt < max_attempts \
               and ($2::text is null or session_id = $2::text) \
             order by due_at_ms asc, created_at_ms asc, job_id asc limit $3 \
             for update skip locked",
        )
        .bind(to_millis(now))
        .bind(session_id.map(|id| id.to_string()))
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await?;

        let mut claimed = Vec::with_capacity(rows.len());
        for row in &rows {
            let job_id = parse_uuid(row.try_get::<String, _>("job_id")?, "job_id")?;
            let token = Uuid::new_v4();
            sqlx::query(
                "update autonomy_jobs set state = 'claimed', lease_owner = $1, \
                 lease_token = $2, lease_heartbeat_at_ms = $3, lease_expires_at_ms = $4, \
                 updated_at_ms = $3 where job_id = $5",
            )
            .bind(lease_owner)
            .bind(token.to_string())
            .bind(to_millis(now))
            .bind(to_millis(lease_expires_at))
            .bind(job_id.to_string())
            .execute(&mut *transaction)
            .await?;
            let job = load_job(&mut transaction, job_id).await?;
            append_transition(
                &mut transaction,
                AutonomyTransition {
                    job_id,
                    from: Some(AutonomyJobState::Queued),
                    to: AutonomyJobState::Claimed,
                    attempt: job.attempt,
                    reason: None,
                    lease_owner: Some(lease_owner.to_owned()),
                    occurred_at: now,
                },
            )
            .await?;
            claimed.push(job);
        }
        transaction.commit().await?;
        Ok(claimed)
    }

    pub async fn heartbeat(
        &self,
        job_id: Uuid,
        lease: &AutonomyLease,
        now: DateTime<Utc>,
        lease_duration: Duration,
    ) -> Result<AutonomyJob> {
        validate_owner(&lease.owner)?;
        ensure!(!lease_duration.is_zero(), "lease duration must be non-zero");
        let lease_expires_at = add_duration(now, lease_duration)?;
        let mut transaction = self.pool.begin().await?;
        let current = load_job(&mut transaction, job_id).await?;
        // A heartbeat proves the worker still holds the lease; a stale token
        // must not extend work another worker has taken over.
        ensure!(
            current.lease_owner.as_deref() == Some(lease.owner.as_str())
                && current.lease_token == Some(lease.token),
            "job {job_id} lease is not owned by {}",
            lease.owner
        );
        ensure!(
            matches!(
                current.state,
                AutonomyJobState::Claimed | AutonomyJobState::Running
            ),
            "job {job_id} is not leased"
        );
        sqlx::query(
            "update autonomy_jobs set lease_heartbeat_at_ms = $1, lease_expires_at_ms = $2, \
             updated_at_ms = $1 where job_id = $3",
        )
        .bind(to_millis(now))
        .bind(to_millis(lease_expires_at))
        .bind(job_id.to_string())
        .execute(&mut *transaction)
        .await?;
        let job = load_job(&mut transaction, job_id).await?;
        transaction.commit().await?;
        Ok(job)
    }

    pub async fn transition(
        &self,
        job_id: Uuid,
        expected: AutonomyJobState,
        next: AutonomyJobState,
        lease: Option<&AutonomyLease>,
        reason: Option<String>,
        now: DateTime<Utc>,
    ) -> Result<AutonomyJob> {
        if let Some(reason) = reason.as_deref() {
            validate_optional_text(reason, MAX_ERROR_BYTES, "transition reason")?;
        }
        ensure!(expected != next, "autonomy transition must change state");
        ensure!(
            expected.can_transition_to(next),
            "invalid autonomy transition {expected:?} -> {next:?}"
        );
        let mut transaction = self.pool.begin().await?;
        let current = load_job(&mut transaction, job_id).await?;
        ensure!(
            current.state == expected,
            "job {job_id} expected state {:?}, found {:?}",
            expected,
            current.state
        );
        validate_lease_for_transition(&current, lease, now)?;
        if next == AutonomyJobState::Queued {
            ensure!(
                current.attempt < current.max_attempts,
                "job {job_id} has exhausted its retry-attempt budget"
            );
        }
        // An attempt is consumed when the job starts executing, not when it is
        // claimed: a worker that claims and dies before running must not burn
        // the budget of a job that never ran.
        let next_attempt = if next == AutonomyJobState::Running {
            ensure!(
                current.attempt < current.max_attempts,
                "job {job_id} has exhausted its execution-attempt budget"
            );
            current.attempt + 1
        } else {
            current.attempt
        };
        let clear_lease = matches!(
            next,
            AutonomyJobState::Queued
                | AutonomyJobState::Completed
                | AutonomyJobState::Failed
                | AutonomyJobState::Cancelled
        );
        sqlx::query(
            "update autonomy_jobs set state = $1, attempt = $2, due_at_ms = $3, \
             lease_owner = $4, lease_token = $5, lease_heartbeat_at_ms = $6, \
             lease_expires_at_ms = $7, last_error = $8, updated_at_ms = $9 \
             where job_id = $10 and state = $11",
        )
        .bind(next.as_str())
        .bind(i64::from(next_attempt))
        .bind(to_millis(if next == AutonomyJobState::Queued {
            now
        } else {
            current.due_at
        }))
        .bind(if clear_lease {
            None
        } else {
            current.lease_owner.clone()
        })
        .bind(if clear_lease {
            None
        } else {
            current.lease_token.map(|token| token.to_string())
        })
        .bind(if clear_lease {
            None
        } else {
            current.lease_heartbeat_at.map(to_millis)
        })
        .bind(if clear_lease {
            None
        } else {
            current.lease_expires_at.map(to_millis)
        })
        .bind(reason.clone())
        .bind(to_millis(now))
        .bind(job_id.to_string())
        .bind(expected.as_str())
        .execute(&mut *transaction)
        .await?;
        let job = load_job(&mut transaction, job_id).await?;
        append_transition(
            &mut transaction,
            AutonomyTransition {
                job_id,
                from: Some(expected),
                to: next,
                attempt: next_attempt,
                reason,
                lease_owner: lease.map(|lease| lease.owner.clone()),
                occurred_at: now,
            },
        )
        .await?;
        transaction.commit().await?;
        Ok(job)
    }

    pub async fn complete(
        &self,
        job_id: Uuid,
        lease: &AutonomyLease,
        result: Value,
        now: DateTime<Utc>,
    ) -> Result<AutonomyJob> {
        let result_json = serde_json::to_string(&result)?;
        ensure!(
            result_json.len() <= MAX_PAYLOAD_BYTES,
            "autonomy job result exceeds {MAX_PAYLOAD_BYTES} bytes"
        );
        let mut transaction = self.pool.begin().await?;
        let current = load_job(&mut transaction, job_id).await?;
        ensure!(
            current.state == AutonomyJobState::Running,
            "job {job_id} is not running"
        );
        validate_lease_for_transition(&current, Some(lease), now)?;
        sqlx::query(
            "update autonomy_jobs set state = 'completed', result_json = $1, last_error = null, \
             lease_owner = null, lease_token = null, lease_heartbeat_at_ms = null, \
             lease_expires_at_ms = null, updated_at_ms = $2 \
             where job_id = $3 and state = 'running'",
        )
        .bind(&result_json)
        .bind(to_millis(now))
        .bind(job_id.to_string())
        .execute(&mut *transaction)
        .await?;
        let job = load_job(&mut transaction, job_id).await?;
        append_transition(
            &mut transaction,
            AutonomyTransition {
                job_id,
                from: Some(AutonomyJobState::Running),
                to: AutonomyJobState::Completed,
                attempt: job.attempt,
                reason: None,
                lease_owner: Some(lease.owner.clone()),
                occurred_at: now,
            },
        )
        .await?;
        transaction.commit().await?;
        Ok(job)
    }

    pub async fn recover_expired(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<AutonomyJob>> {
        self.recover_expired_filtered(now, limit, None).await
    }

    pub async fn recover_expired_for_session(
        &self,
        now: DateTime<Utc>,
        limit: u32,
        session_id: Uuid,
    ) -> Result<Vec<AutonomyJob>> {
        self.recover_expired_filtered(now, limit, Some(session_id))
            .await
    }

    async fn recover_expired_filtered(
        &self,
        now: DateTime<Utc>,
        limit: u32,
        session_id: Option<Uuid>,
    ) -> Result<Vec<AutonomyJob>> {
        validate_batch_size(limit)?;
        let mut transaction = self.pool.begin().await?;
        let rows = sqlx::query(
            "select job_id from autonomy_jobs \
             where state in ('claimed', 'running') and lease_expires_at_ms <= $1 \
               and ($2::text is null or session_id = $2::text) \
             order by lease_expires_at_ms asc, updated_at_ms asc, job_id asc limit $3 \
             for update skip locked",
        )
        .bind(to_millis(now))
        .bind(session_id.map(|id| id.to_string()))
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await?;

        let mut recovered = Vec::with_capacity(rows.len());
        for row in &rows {
            let job_id = parse_uuid(row.try_get::<String, _>("job_id")?, "job_id")?;
            let current = load_job(&mut transaction, job_id).await?;
            if !matches!(
                current.state,
                AutonomyJobState::Claimed | AutonomyJobState::Running
            ) || current
                .lease_expires_at
                .is_none_or(|expires_at| expires_at > now)
            {
                continue;
            }
            // Out of attempts means the work is abandoned, not retried forever.
            let next = if current.attempt < current.max_attempts {
                AutonomyJobState::Queued
            } else {
                AutonomyJobState::Failed
            };
            let reason = if next == AutonomyJobState::Queued {
                "lease expired; job requeued for retry"
            } else {
                "lease expired; retry-attempt budget exhausted"
            };
            sqlx::query(
                "update autonomy_jobs set state = $1, due_at_ms = $2, lease_owner = null, \
                 lease_token = null, lease_heartbeat_at_ms = null, lease_expires_at_ms = null, \
                 last_error = $3, updated_at_ms = $2 where job_id = $4 and state = $5",
            )
            .bind(next.as_str())
            .bind(to_millis(now))
            .bind(reason)
            .bind(job_id.to_string())
            .bind(current.state.as_str())
            .execute(&mut *transaction)
            .await?;
            let job = load_job(&mut transaction, job_id).await?;
            append_transition(
                &mut transaction,
                AutonomyTransition {
                    job_id,
                    from: Some(current.state),
                    to: next,
                    attempt: job.attempt,
                    reason: Some(reason.to_string()),
                    lease_owner: current.lease_owner.clone(),
                    occurred_at: now,
                },
            )
            .await?;
            recovered.push(job);
        }
        transaction.commit().await?;
        Ok(recovered)
    }

    pub async fn save_checkpoint(
        &self,
        input: SaveAutonomyCheckpoint,
    ) -> Result<AutonomyCheckpoint> {
        validate_checkpoint(&input)?;
        let state_json = serde_json::to_string(&input.state)?;
        let evidence_json = serde_json::to_string(&input.evidence)?;
        let content_hash = checkpoint_hash(&state_json, &evidence_json);
        let checkpoint_id = input.checkpoint_id.unwrap_or_else(Uuid::new_v4);
        let mut transaction = self.pool.begin().await?;
        let job = load_job(&mut transaction, input.job_id).await?;
        if let Some(session_id) = input.session_id {
            ensure!(
                job.session_id.is_none_or(|owner| owner == session_id),
                "checkpoint session does not match its job"
            );
        }

        // A checkpoint key is write-once per job: the same key with different
        // content would silently rewrite recorded evidence.
        let existing = sqlx::query(
            "select checkpoint_id, job_id, checkpoint_key, session_id, goal_id, kind, \
                    state_json, evidence_json, content_hash, created_at_ms \
             from autonomy_checkpoints where job_id = $1 and checkpoint_key = $2",
        )
        .bind(input.job_id.to_string())
        .bind(&input.checkpoint_key)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(row) = existing {
            let existing = decode_checkpoint(&row)?;
            ensure!(
                existing.content_hash == content_hash,
                "checkpoint {} already exists with different content",
                input.checkpoint_key
            );
            transaction.commit().await?;
            return Ok(existing);
        }

        sqlx::query(
            "insert into autonomy_checkpoints \
             (checkpoint_id, job_id, checkpoint_key, session_id, goal_id, kind, state_json, \
              evidence_json, content_hash, created_at_ms) \
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(checkpoint_id.to_string())
        .bind(input.job_id.to_string())
        .bind(&input.checkpoint_key)
        .bind(input.session_id.map(|id| id.to_string()))
        .bind(input.goal_id.map(|id| id.to_string()))
        .bind(&input.kind)
        .bind(&state_json)
        .bind(&evidence_json)
        .bind(&content_hash)
        .bind(to_millis(input.created_at))
        .execute(&mut *transaction)
        .await?;
        let row = sqlx::query(
            "select checkpoint_id, job_id, checkpoint_key, session_id, goal_id, kind, \
                    state_json, evidence_json, content_hash, created_at_ms \
             from autonomy_checkpoints where checkpoint_id = $1",
        )
        .bind(checkpoint_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        let checkpoint = decode_checkpoint(&row)?;
        transaction.commit().await?;
        Ok(checkpoint)
    }

    pub async fn list_checkpoints(&self, job_id: Uuid) -> Result<Vec<AutonomyCheckpoint>> {
        let rows = sqlx::query(
            "select checkpoint_id, job_id, checkpoint_key, session_id, goal_id, kind, \
                    state_json, evidence_json, content_hash, created_at_ms \
             from autonomy_checkpoints where job_id = $1 \
             order by created_at_ms asc, checkpoint_id asc limit $2",
        )
        .bind(job_id.to_string())
        .bind(MAX_CHECKPOINTS_PER_LIST)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(decode_checkpoint).collect()
    }

    /// The append-only transition audit for one job.
    pub async fn list_transitions(&self, job_id: Uuid) -> Result<Vec<AutonomyJobTransition>> {
        let rows = sqlx::query(
            "select job_id, sequence, from_state, to_state, attempt, reason, lease_owner, \
                    occurred_at_ms from autonomy_job_transitions \
             where job_id = $1 order by sequence asc",
        )
        .bind(job_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(decode_transition).collect()
    }
}

#[async_trait::async_trait]
impl crate::autonomy::AutonomyStore for PostgresAutonomyStore {
    async fn enqueue(&self, input: EnqueueAutonomyJob) -> Result<AutonomyJob> {
        Self::enqueue(self, input).await
    }

    async fn get(&self, job_id: Uuid) -> Result<Option<AutonomyJob>> {
        Self::get(self, job_id).await
    }

    async fn claim_due(
        &self,
        now: DateTime<Utc>,
        lease_owner: &str,
        lease_duration: Duration,
        limit: u32,
    ) -> Result<Vec<AutonomyJob>> {
        Self::claim_due(self, now, lease_owner, lease_duration, limit).await
    }

    async fn claim_due_for_session(
        &self,
        now: DateTime<Utc>,
        lease_owner: &str,
        lease_duration: Duration,
        limit: u32,
        session_id: Uuid,
    ) -> Result<Vec<AutonomyJob>> {
        Self::claim_due_for_session(self, now, lease_owner, lease_duration, limit, session_id).await
    }

    async fn heartbeat(
        &self,
        job_id: Uuid,
        lease: &AutonomyLease,
        now: DateTime<Utc>,
        lease_duration: Duration,
    ) -> Result<AutonomyJob> {
        Self::heartbeat(self, job_id, lease, now, lease_duration).await
    }

    async fn transition(
        &self,
        job_id: Uuid,
        expected: AutonomyJobState,
        next: AutonomyJobState,
        lease: Option<&AutonomyLease>,
        reason: Option<String>,
        now: DateTime<Utc>,
    ) -> Result<AutonomyJob> {
        Self::transition(self, job_id, expected, next, lease, reason, now).await
    }

    async fn complete(
        &self,
        job_id: Uuid,
        lease: &AutonomyLease,
        result: Value,
        now: DateTime<Utc>,
    ) -> Result<AutonomyJob> {
        Self::complete(self, job_id, lease, result, now).await
    }

    async fn recover_expired(&self, now: DateTime<Utc>, limit: u32) -> Result<Vec<AutonomyJob>> {
        Self::recover_expired(self, now, limit).await
    }

    async fn recover_expired_for_session(
        &self,
        now: DateTime<Utc>,
        limit: u32,
        session_id: Uuid,
    ) -> Result<Vec<AutonomyJob>> {
        Self::recover_expired_for_session(self, now, limit, session_id).await
    }

    async fn save_checkpoint(&self, input: SaveAutonomyCheckpoint) -> Result<AutonomyCheckpoint> {
        Self::save_checkpoint(self, input).await
    }

    async fn list_checkpoints(&self, job_id: Uuid) -> Result<Vec<AutonomyCheckpoint>> {
        Self::list_checkpoints(self, job_id).await
    }

    async fn list_transitions(&self, job_id: Uuid) -> Result<Vec<AutonomyJobTransition>> {
        Self::list_transitions(self, job_id).await
    }
}
