//! Provider-neutral durable runtime jobs backed by SQLite.
//!
//! This module intentionally owns only the runtime-job tables.  It does not
//! model teams, sessions, or provider-specific execution.  A later caller can
//! register the module and connect its optional session/goal identifiers to
//! the surrounding application without changing this store's state machine.

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow, ensure};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{Row, Sqlite, SqlitePool, Transaction};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const MAX_BATCH_SIZE: u32 = 256;
const MAX_CHECKPOINTS_PER_LIST: u32 = 512;
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 256;
const MAX_KIND_BYTES: usize = 128;
const MAX_OWNER_BYTES: usize = 256;
const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
const MAX_ERROR_BYTES: usize = 16 * 1024;
const MAX_CHECKPOINT_KEY_BYTES: usize = 256;
const MAX_CHECKPOINT_KIND_BYTES: usize = 128;
const MAX_CHECKPOINT_JSON_BYTES: usize = 2 * 1024 * 1024;

const SQLITE_WRITE_TRANSACTION: &str = "BEGIN IMMEDIATE";
const AUTONOMY_SCHEMA_VERSION: i64 = 2;

/// The durable lifecycle of one programmatic runtime job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutonomyJobState {
    Queued,
    Claimed,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl AutonomyJobState {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Claimed => "claimed",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "queued" => Ok(Self::Queued),
            "claimed" => Ok(Self::Claimed),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(anyhow!("unknown autonomy job state {value:?}")),
        }
    }

    const fn can_transition_to(self, next: Self) -> bool {
        match self {
            Self::Queued => matches!(next, Self::Claimed | Self::Cancelled),
            Self::Claimed => matches!(
                next,
                Self::Running | Self::Queued | Self::Failed | Self::Cancelled
            ),
            Self::Running => matches!(
                next,
                Self::Completed | Self::Queued | Self::Failed | Self::Cancelled
            ),
            Self::Failed => matches!(next, Self::Queued),
            Self::Completed | Self::Cancelled => false,
        }
    }
}

/// A lease fence returned by [`SqliteAutonomyStore::claim_due`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutonomyLease {
    pub owner: String,
    pub token: Uuid,
}

/// Input for an idempotent runtime-job enqueue.
#[derive(Debug, Clone)]
pub struct EnqueueAutonomyJob {
    pub job_id: Option<Uuid>,
    pub idempotency_key: String,
    pub kind: String,
    pub payload: Value,
    pub due_at: DateTime<Utc>,
    pub max_attempts: u32,
    pub session_id: Option<Uuid>,
    pub goal_id: Option<Uuid>,
}

/// A durable runtime job.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AutonomyJob {
    pub job_id: Uuid,
    pub idempotency_key: String,
    pub kind: String,
    pub payload: Value,
    pub state: AutonomyJobState,
    pub due_at: DateTime<Utc>,
    pub attempt: u32,
    pub max_attempts: u32,
    pub lease_owner: Option<String>,
    pub lease_token: Option<Uuid>,
    pub lease_heartbeat_at: Option<DateTime<Utc>>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub session_id: Option<Uuid>,
    pub goal_id: Option<Uuid>,
    pub result: Option<Value>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Provider- and tool-neutral execution hook for durable jobs. The handler is
/// deliberately outside the store: SQLite owns admission, leases, retries,
/// and results, while the session/runtime owns what a job means.
#[async_trait]
pub trait AutonomyJobHandler: Send + Sync {
    async fn execute(&self, job: AutonomyJob) -> Result<Value>;
}

/// A bounded, lease-aware worker for the autonomous runtime journal.
///
/// A worker may be started in a resident session process or in a host
/// supervisor. Claiming and completion are fenced in SQLite, so a crashed
/// worker can be replaced without duplicating a completed job.
#[derive(Clone)]
pub struct SqliteAutonomySupervisor {
    store: SqliteAutonomyStore,
    handler: Arc<dyn AutonomyJobHandler>,
    owner: String,
    session_id: Option<Uuid>,
    lease_duration: Duration,
    poll_interval: Duration,
    batch_size: u32,
}

impl SqliteAutonomySupervisor {
    pub fn new(
        store: SqliteAutonomyStore,
        handler: Arc<dyn AutonomyJobHandler>,
        owner: impl Into<String>,
    ) -> Result<Self> {
        let owner = owner.into();
        validate_owner(&owner)?;
        Ok(Self {
            store,
            handler,
            owner,
            session_id: None,
            lease_duration: Duration::from_secs(60),
            poll_interval: Duration::from_millis(250),
            batch_size: 8,
        })
    }

    pub fn with_limits(
        mut self,
        lease_duration: Duration,
        poll_interval: Duration,
        batch_size: u32,
    ) -> Result<Self> {
        ensure!(!lease_duration.is_zero(), "autonomy lease duration is zero");
        ensure!(!poll_interval.is_zero(), "autonomy poll interval is zero");
        validate_batch_size(batch_size)?;
        self.lease_duration = lease_duration;
        self.poll_interval = poll_interval;
        self.batch_size = batch_size;
        Ok(self)
    }

    /// Restrict this worker to jobs owned by one session. A shared SQLite
    /// authority may host many sessions, so a per-session supervisor must
    /// never claim or recover another session's work.
    pub fn for_session(mut self, session_id: Uuid) -> Self {
        self.session_id = Some(session_id);
        self
    }

    /// Recover abandoned claims and execute one bounded batch.
    pub async fn run_once(&self, now: DateTime<Utc>) -> Result<u32> {
        if let Some(session_id) = self.session_id {
            self.store
                .recover_expired_for_session(now, self.batch_size, session_id)
                .await?;
        } else {
            self.store.recover_expired(now, self.batch_size).await?;
        }
        let claimed = if let Some(session_id) = self.session_id {
            self.store
                .claim_due_for_session(
                    now,
                    &self.owner,
                    self.lease_duration,
                    self.batch_size,
                    session_id,
                )
                .await?
        } else {
            self.store
                .claim_due(now, &self.owner, self.lease_duration, self.batch_size)
                .await?
        };
        let mut completed = 0;
        for job in claimed {
            let Some(lease) = job.lease() else {
                continue;
            };
            let running = match self
                .store
                .transition(
                    job.job_id,
                    AutonomyJobState::Claimed,
                    AutonomyJobState::Running,
                    Some(&lease),
                    None,
                    Utc::now(),
                )
                .await
            {
                Ok(job) => job,
                Err(error) => {
                    tracing::warn!(job_id = %job.job_id, %error, "autonomy job could not enter running state");
                    continue;
                }
            };

            let execution = self.handler.execute(running.clone());
            tokio::pin!(execution);
            let mut heartbeat = tokio::time::interval(self.lease_duration / 3);
            heartbeat.tick().await;
            let result = loop {
                tokio::select! {
                    result = &mut execution => break result,
                    _ = heartbeat.tick() => {
                        if let Err(error) = self.store.heartbeat(
                            running.job_id,
                            &lease,
                            Utc::now(),
                            self.lease_duration,
                        ).await {
                            break Err(error).context("autonomy job lease was lost while executing");
                        }
                    }
                }
            };

            match result {
                Ok(value) => {
                    self.store
                        .complete(running.job_id, &lease, value, Utc::now())
                        .await?;
                    completed += 1;
                }
                Err(error) => {
                    let now = Utc::now();
                    let next = if running.attempt < running.max_attempts {
                        AutonomyJobState::Queued
                    } else {
                        AutonomyJobState::Failed
                    };
                    self.store
                        .transition(
                            running.job_id,
                            AutonomyJobState::Running,
                            next,
                            Some(&lease),
                            Some(format!("{error:#}")),
                            now,
                        )
                        .await?;
                }
            }
        }
        Ok(completed)
    }

    /// Keep the worker alive until its cancellation token is closed. A poll
    /// error does not kill the supervisor; the next iteration can recover a
    /// transient SQLite/provider failure while the lease fence prevents dupes.
    pub async fn run_until_cancelled(&self, cancel: CancellationToken) {
        let mut tick = tokio::time::interval(self.poll_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tick.tick() => {
                    if let Err(error) = self.run_once(Utc::now()).await {
                        tracing::warn!(owner = %self.owner, %error, "autonomy supervisor iteration failed");
                    }
                }
            }
        }
    }
}

impl AutonomyJob {
    pub fn lease(&self) -> Option<AutonomyLease> {
        match (self.lease_owner.clone(), self.lease_token) {
            (Some(owner), Some(token)) => Some(AutonomyLease { owner, token }),
            _ => None,
        }
    }
}

/// One append-only lifecycle transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutonomyJobTransition {
    pub job_id: Uuid,
    pub sequence: u64,
    pub from: Option<AutonomyJobState>,
    pub to: AutonomyJobState,
    pub attempt: u32,
    pub reason: Option<String>,
    pub lease_owner: Option<String>,
    pub occurred_at: DateTime<Utc>,
}

/// Input for a reproducible checkpoint/evidence row.
#[derive(Debug, Clone)]
pub struct SaveAutonomyCheckpoint {
    pub checkpoint_id: Option<Uuid>,
    pub job_id: Uuid,
    pub checkpoint_key: String,
    pub session_id: Option<Uuid>,
    pub goal_id: Option<Uuid>,
    pub kind: String,
    pub state: Value,
    pub evidence: Value,
    pub created_at: DateTime<Utc>,
}

/// A persisted checkpoint with its exact content hash.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AutonomyCheckpoint {
    pub checkpoint_id: Uuid,
    pub job_id: Uuid,
    pub checkpoint_key: String,
    pub session_id: Option<Uuid>,
    pub goal_id: Option<Uuid>,
    pub kind: String,
    pub state: Value,
    pub evidence: Value,
    pub content_hash: String,
    pub created_at: DateTime<Utc>,
}

/// A bounded durable job store using a caller-supplied SQLite pool.
#[derive(Debug, Clone)]
pub struct SqliteAutonomyStore {
    pool: SqlitePool,
}

impl SqliteAutonomyStore {
    /// Create the autonomy tables and return a store over `pool`.
    pub async fn open(pool: SqlitePool) -> Result<Self> {
        let store = Self { pool };
        store.ensure_schema().await?;
        Ok(store)
    }

    /// Reuse the canonical session journal on the same SQLite pool. Native
    /// runtime records must not create a second database authority.
    pub(crate) fn session_store(&self) -> crate::SqliteSessionStore {
        crate::SqliteSessionStore::from_pool(self.pool.clone())
    }

    /// Enqueue a job, returning the existing row for a repeated idempotency
    /// key when its immutable request fields match exactly.
    pub async fn enqueue(&self, input: EnqueueAutonomyJob) -> Result<AutonomyJob> {
        validate_enqueue(&input)?;
        let payload_json = serde_json::to_string(&input.payload)?;
        let now = Utc::now();
        let job_id = input.job_id.unwrap_or_else(Uuid::new_v4);
        let mut tx = self.pool.begin_with(SQLITE_WRITE_TRANSACTION).await?;

        if let Some(row) = sqlx::query("select * from autonomy_jobs where idempotency_key=?")
            .bind(&input.idempotency_key)
            .fetch_optional(&mut *tx)
            .await?
        {
            let existing = decode_job(&row)?;
            ensure!(
                existing.kind == input.kind
                    && existing.payload == input.payload
                    && existing.due_at == input.due_at
                    && existing.max_attempts == input.max_attempts
                    && existing.session_id == input.session_id
                    && existing.goal_id == input.goal_id,
                "idempotency key already names a different autonomy job",
            );
            tx.commit().await?;
            return Ok(existing);
        }

        sqlx::query(
            "insert into autonomy_jobs \
             (job_id, idempotency_key, kind, payload_json, state, due_at_ms, attempt, \
              max_attempts, session_id, goal_id, created_at_ms, updated_at_ms) \
             values (?, ?, ?, ?, 'queued', ?, 0, ?, ?, ?, ?, ?)",
        )
        .bind(job_id.to_string())
        .bind(&input.idempotency_key)
        .bind(&input.kind)
        .bind(&payload_json)
        .bind(to_millis(input.due_at))
        .bind(i64::from(input.max_attempts))
        .bind(input.session_id.map(|value| value.to_string()))
        .bind(input.goal_id.map(|value| value.to_string()))
        .bind(to_millis(now))
        .bind(to_millis(now))
        .execute(&mut *tx)
        .await
        .with_context(|| format!("insert autonomy job {job_id}"))?;

        append_transition(
            &mut tx,
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

        let job = load_job(&mut tx, job_id).await?;
        tx.commit().await?;
        Ok(job)
    }

    /// Read one job for inspection or an authorized runtime worker.
    pub async fn get(&self, job_id: Uuid) -> Result<Option<AutonomyJob>> {
        let row = sqlx::query("select * from autonomy_jobs where job_id=?")
            .bind(job_id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(decode_job).transpose()
    }

    /// Claim up to `limit` due jobs and fence each claim with a fresh token.
    pub async fn claim_due(
        &self,
        now: DateTime<Utc>,
        lease_owner: impl AsRef<str>,
        lease_duration: Duration,
        limit: u32,
    ) -> Result<Vec<AutonomyJob>> {
        self.claim_due_filtered(now, lease_owner, lease_duration, limit, None)
            .await
    }

    /// Claim only jobs owned by `session_id` on a shared authority.
    pub async fn claim_due_for_session(
        &self,
        now: DateTime<Utc>,
        lease_owner: impl AsRef<str>,
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
        lease_owner: impl AsRef<str>,
        lease_duration: Duration,
        limit: u32,
        session_id: Option<Uuid>,
    ) -> Result<Vec<AutonomyJob>> {
        let lease_owner = lease_owner.as_ref();
        validate_owner(lease_owner)?;
        validate_batch_size(limit)?;
        ensure!(!lease_duration.is_zero(), "lease duration must be non-zero");
        let lease_expires_at = add_duration(now, lease_duration)?;
        let mut tx = self.pool.begin_with(SQLITE_WRITE_TRANSACTION).await?;
        let rows = sqlx::query(
            "select job_id from autonomy_jobs \
             where state='queued' and due_at_ms <= ? and attempt < max_attempts \
             and (? is null or session_id = ?) \
             order by due_at_ms asc, created_at_ms asc, job_id asc limit ?",
        )
        .bind(to_millis(now))
        .bind(session_id.map(|value| value.to_string()))
        .bind(session_id.map(|value| value.to_string()))
        .bind(i64::from(limit))
        .fetch_all(&mut *tx)
        .await?;
        let mut claimed = Vec::with_capacity(rows.len());

        for row in rows {
            let job_id = parse_uuid(row.try_get::<String, _>("job_id")?, "job_id")?;
            let token = Uuid::new_v4();
            let updated = sqlx::query(
                "update autonomy_jobs set state='claimed', attempt=attempt+1, \
                 lease_owner=?, lease_token=?, lease_heartbeat_at_ms=?, lease_expires_at_ms=?, \
                 updated_at_ms=? where job_id=? and state='queued' and due_at_ms <= ? \
                 and attempt < max_attempts and (? is null or session_id = ?)",
            )
            .bind(lease_owner)
            .bind(token.to_string())
            .bind(to_millis(now))
            .bind(to_millis(lease_expires_at))
            .bind(to_millis(now))
            .bind(job_id.to_string())
            .bind(to_millis(now))
            .bind(session_id.map(|value| value.to_string()))
            .bind(session_id.map(|value| value.to_string()))
            .execute(&mut *tx)
            .await?;
            if updated.rows_affected() != 1 {
                continue;
            }
            let job = load_job(&mut tx, job_id).await?;
            append_transition(
                &mut tx,
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

        tx.commit().await?;
        Ok(claimed)
    }

    /// Extend a live claim without changing its lifecycle state.
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
        let mut tx = self.pool.begin_with(SQLITE_WRITE_TRANSACTION).await?;
        let current = load_job(&mut tx, job_id).await?;
        ensure!(
            matches!(
                current.state,
                AutonomyJobState::Claimed | AutonomyJobState::Running
            ),
            "job {job_id} is not leaseable in state {:?}",
            current.state
        );
        ensure!(
            current.lease_owner.as_deref() == Some(lease.owner.as_str())
                && current.lease_token == Some(lease.token)
                && current
                    .lease_expires_at
                    .is_some_and(|expires_at| expires_at > now),
            "lease for job {job_id} is missing, fenced, or expired"
        );
        sqlx::query(
            "update autonomy_jobs set lease_heartbeat_at_ms=?, lease_expires_at_ms=?, \
             updated_at_ms=? where job_id=?",
        )
        .bind(to_millis(now))
        .bind(to_millis(lease_expires_at))
        .bind(to_millis(now))
        .bind(job_id.to_string())
        .execute(&mut *tx)
        .await?;
        let updated = load_job(&mut tx, job_id).await?;
        tx.commit().await?;
        Ok(updated)
    }

    /// Apply a validated lifecycle transition under the current lease fence.
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
            "invalid autonomy transition {:?} -> {:?}",
            expected,
            next
        );
        let mut tx = self.pool.begin_with(SQLITE_WRITE_TRANSACTION).await?;
        let current = load_job(&mut tx, job_id).await?;
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
        let lease_owner = lease.map(|value| value.owner.clone());
        let clear_lease = matches!(
            next,
            AutonomyJobState::Queued
                | AutonomyJobState::Completed
                | AutonomyJobState::Failed
                | AutonomyJobState::Cancelled
        );
        sqlx::query(
            "update autonomy_jobs set state=?, due_at_ms=?, lease_owner=?, lease_token=?, \
             lease_heartbeat_at_ms=?, lease_expires_at_ms=?, last_error=?, updated_at_ms=? \
             where job_id=? and state=?",
        )
        .bind(next.as_str())
        .bind(to_millis(if next == AutonomyJobState::Queued {
            now
        } else {
            current.due_at
        }))
        .bind(if clear_lease {
            None::<String>
        } else {
            current.lease_owner.clone()
        })
        .bind(if clear_lease {
            None::<String>
        } else {
            current.lease_token.map(|value| value.to_string())
        })
        .bind(if clear_lease {
            None::<i64>
        } else {
            current.lease_heartbeat_at.map(to_millis)
        })
        .bind(if clear_lease {
            None::<i64>
        } else {
            current.lease_expires_at.map(to_millis)
        })
        .bind(reason.as_deref())
        .bind(to_millis(now))
        .bind(job_id.to_string())
        .bind(expected.as_str())
        .execute(&mut *tx)
        .await?
        .rows_affected()
        .eq(&1)
        .then_some(())
        .context("autonomy transition lost its state fence")?;
        let updated = load_job(&mut tx, job_id).await?;
        append_transition(
            &mut tx,
            AutonomyTransition {
                job_id,
                from: Some(expected),
                to: next,
                attempt: updated.attempt,
                reason,
                lease_owner,
                occurred_at: now,
            },
        )
        .await?;
        tx.commit().await?;
        Ok(updated)
    }

    /// Complete a running job and persist its result atomically with the
    /// terminal transition and its audit record.
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
        let mut tx = self.pool.begin_with(SQLITE_WRITE_TRANSACTION).await?;
        let current = load_job(&mut tx, job_id).await?;
        ensure!(
            current.state == AutonomyJobState::Running,
            "job {job_id} is not running"
        );
        validate_lease_for_transition(&current, Some(lease), now)?;
        sqlx::query(
            "update autonomy_jobs set state='completed', result_json=?, last_error=null, \
             lease_owner=null, lease_token=null, lease_heartbeat_at_ms=null, \
             lease_expires_at_ms=null, updated_at_ms=? where job_id=? and state='running'",
        )
        .bind(&result_json)
        .bind(to_millis(now))
        .bind(job_id.to_string())
        .execute(&mut *tx)
        .await?
        .rows_affected()
        .eq(&1)
        .then_some(())
        .context("autonomy completion lost its state fence")?;
        let updated = load_job(&mut tx, job_id).await?;
        append_transition(
            &mut tx,
            AutonomyTransition {
                job_id,
                from: Some(AutonomyJobState::Running),
                to: AutonomyJobState::Completed,
                attempt: updated.attempt,
                reason: None,
                lease_owner: Some(lease.owner.clone()),
                occurred_at: now,
            },
        )
        .await?;
        tx.commit().await?;
        Ok(updated)
    }

    /// Recover expired claims. Jobs below their retry budget are requeued;
    /// exhausted jobs become failed, both with an audited transition.
    pub async fn recover_expired(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<AutonomyJob>> {
        self.recover_expired_filtered(now, limit, None).await
    }

    /// Recover only jobs owned by `session_id` on a shared authority.
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
        let mut tx = self.pool.begin_with(SQLITE_WRITE_TRANSACTION).await?;
        let rows = sqlx::query(
            "select job_id from autonomy_jobs \
             where state in ('claimed', 'running') and lease_expires_at_ms <= ? \
             and (? is null or session_id = ?) \
             order by lease_expires_at_ms asc, updated_at_ms asc, job_id asc limit ?",
        )
        .bind(to_millis(now))
        .bind(session_id.map(|value| value.to_string()))
        .bind(session_id.map(|value| value.to_string()))
        .bind(i64::from(limit))
        .fetch_all(&mut *tx)
        .await?;
        let mut recovered = Vec::with_capacity(rows.len());

        for row in rows {
            let job_id = parse_uuid(row.try_get::<String, _>("job_id")?, "job_id")?;
            let current = load_job(&mut tx, job_id).await?;
            if !matches!(
                current.state,
                AutonomyJobState::Claimed | AutonomyJobState::Running
            ) || current
                .lease_expires_at
                .is_none_or(|expires_at| expires_at > now)
            {
                continue;
            }
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
                "update autonomy_jobs set state=?, due_at_ms=?, lease_owner=null, lease_token=null, \
                 lease_heartbeat_at_ms=null, lease_expires_at_ms=null, last_error=?, updated_at_ms=? \
                 where job_id=? and state=?",
            )
            .bind(next.as_str())
            .bind(to_millis(now))
            .bind(reason)
            .bind(to_millis(now))
            .bind(job_id.to_string())
            .bind(current.state.as_str())
            .execute(&mut *tx)
            .await?
            .rows_affected()
            .eq(&1)
            .then_some(())
            .context("expired autonomy job changed while recovering")?;
            append_transition(
                &mut tx,
                AutonomyTransition {
                    job_id,
                    from: Some(current.state),
                    to: next,
                    attempt: current.attempt,
                    reason: Some(reason.to_owned()),
                    lease_owner: current.lease_owner,
                    occurred_at: now,
                },
            )
            .await?;
            recovered.push(load_job(&mut tx, job_id).await?);
        }

        tx.commit().await?;
        Ok(recovered)
    }

    /// Save a checkpoint/evidence row, idempotent by `(job_id, checkpoint_key)`.
    pub async fn save_checkpoint(
        &self,
        input: SaveAutonomyCheckpoint,
    ) -> Result<AutonomyCheckpoint> {
        validate_checkpoint(&input)?;
        let state_json = serde_json::to_string(&input.state)?;
        let evidence_json = serde_json::to_string(&input.evidence)?;
        let content_hash = checkpoint_hash(&state_json, &evidence_json);
        let checkpoint_id = input.checkpoint_id.unwrap_or_else(Uuid::new_v4);
        let mut tx = self.pool.begin_with(SQLITE_WRITE_TRANSACTION).await?;
        let job = load_job(&mut tx, input.job_id).await?;
        if let Some(session_id) = input.session_id {
            ensure!(
                job.session_id == Some(session_id),
                "checkpoint session does not own the autonomy job"
            );
        }
        if let Some(goal_id) = input.goal_id {
            ensure!(
                job.goal_id == Some(goal_id),
                "checkpoint goal does not own the autonomy job"
            );
        }

        if let Some(row) =
            sqlx::query("select * from autonomy_checkpoints where job_id=? and checkpoint_key=?")
                .bind(input.job_id.to_string())
                .bind(&input.checkpoint_key)
                .fetch_optional(&mut *tx)
                .await?
        {
            let existing = decode_checkpoint(&row)?;
            ensure!(
                existing.content_hash == content_hash
                    && existing.session_id == input.session_id
                    && existing.goal_id == input.goal_id
                    && existing.kind == input.kind,
                "checkpoint key already names different checkpoint content"
            );
            tx.commit().await?;
            return Ok(existing);
        }

        sqlx::query(
            "insert into autonomy_checkpoints \
             (checkpoint_id, job_id, checkpoint_key, session_id, goal_id, kind, state_json, \
              evidence_json, content_hash, created_at_ms) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(checkpoint_id.to_string())
        .bind(input.job_id.to_string())
        .bind(&input.checkpoint_key)
        .bind(input.session_id.map(|value| value.to_string()))
        .bind(input.goal_id.map(|value| value.to_string()))
        .bind(&input.kind)
        .bind(&state_json)
        .bind(&evidence_json)
        .bind(&content_hash)
        .bind(to_millis(input.created_at))
        .execute(&mut *tx)
        .await?;
        let checkpoint = load_checkpoint(&mut tx, checkpoint_id).await?;
        tx.commit().await?;
        Ok(checkpoint)
    }

    /// List checkpoints in insertion order with a fixed safety bound.
    pub async fn list_checkpoints(&self, job_id: Uuid) -> Result<Vec<AutonomyCheckpoint>> {
        let rows = sqlx::query(
            "select * from autonomy_checkpoints where job_id=? \
             order by created_at_ms asc, checkpoint_id asc limit ?",
        )
        .bind(job_id.to_string())
        .bind(i64::from(MAX_CHECKPOINTS_PER_LIST))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(decode_checkpoint).collect()
    }

    /// Read the append-only transition audit for one job.
    pub async fn list_transitions(&self, job_id: Uuid) -> Result<Vec<AutonomyJobTransition>> {
        let rows = sqlx::query(
            "select * from autonomy_job_transitions where job_id=? order by sequence asc",
        )
        .bind(job_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(decode_transition).collect()
    }

    async fn ensure_schema(&self) -> Result<()> {
        sqlx::raw_sql(
            r#"
            create table if not exists autonomy_jobs (
                job_id text primary key,
                idempotency_key text not null unique,
                kind text not null,
                payload_json text not null,
                state text not null,
                due_at_ms integer not null,
                attempt integer not null default 0,
                max_attempts integer not null,
                lease_owner text,
                lease_token text,
                lease_heartbeat_at_ms integer,
                lease_expires_at_ms integer,
                session_id text,
                goal_id text,
                result_json text,
                last_error text,
                created_at_ms integer not null,
                updated_at_ms integer not null,
                check (state in ('queued', 'claimed', 'running', 'completed', 'failed', 'cancelled')),
                check (attempt >= 0 and max_attempts > 0 and attempt <= max_attempts)
            );

            create index if not exists idx_autonomy_jobs_due
                on autonomy_jobs (state, due_at_ms, created_at_ms, job_id);
            create index if not exists idx_autonomy_jobs_lease_expiry
                on autonomy_jobs (state, lease_expires_at_ms, updated_at_ms, job_id);

            create table if not exists autonomy_job_transitions (
                job_id text not null references autonomy_jobs(job_id) on delete cascade,
                sequence integer not null,
                from_state text,
                to_state text not null,
                attempt integer not null,
                reason text,
                lease_owner text,
                occurred_at_ms integer not null,
                primary key (job_id, sequence),
                check (to_state in ('queued', 'claimed', 'running', 'completed', 'failed', 'cancelled')),
                check (from_state is null or from_state in
                    ('queued', 'claimed', 'running', 'completed', 'failed', 'cancelled'))
            );

            create table if not exists autonomy_checkpoints (
                checkpoint_id text primary key,
                job_id text not null references autonomy_jobs(job_id) on delete cascade,
                checkpoint_key text not null,
                session_id text,
                goal_id text,
                kind text not null,
                state_json text not null,
                evidence_json text not null,
                content_hash text not null,
                created_at_ms integer not null,
                unique (job_id, checkpoint_key)
            );
            create index if not exists idx_autonomy_checkpoints_job
                on autonomy_checkpoints (job_id, created_at_ms, checkpoint_id);

            create table if not exists borg_autonomy_schema (
                id integer primary key check(id=1),
                version integer not null
            );
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create autonomy SQLite schema")?;
        let has_result: i64 = sqlx::query_scalar(
            "select exists(select 1 from pragma_table_info('autonomy_jobs') where name='result_json')",
        )
        .fetch_one(&self.pool)
        .await?;
        ensure!(
            has_result != 0,
            "autonomy database is stale: autonomy_jobs.result_json is missing; recreate or explicitly export/import this database"
        );
        let version: Option<i64> =
            sqlx::query_scalar("select version from borg_autonomy_schema where id=1")
                .fetch_optional(&self.pool)
                .await?;
        match version {
            Some(version) => ensure!(
                version == AUTONOMY_SCHEMA_VERSION,
                "autonomy database schema version {version} is unsupported; expected {AUTONOMY_SCHEMA_VERSION}"
            ),
            None => {
                sqlx::query("insert into borg_autonomy_schema(id,version) values(1,?)")
                    .bind(AUTONOMY_SCHEMA_VERSION)
                    .execute(&self.pool)
                    .await?;
            }
        }
        Ok(())
    }
}

struct AutonomyTransition {
    job_id: Uuid,
    from: Option<AutonomyJobState>,
    to: AutonomyJobState,
    attempt: u32,
    reason: Option<String>,
    lease_owner: Option<String>,
    occurred_at: DateTime<Utc>,
}

async fn append_transition(
    tx: &mut Transaction<'_, Sqlite>,
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
        "select coalesce(max(sequence) + 1, 0) from autonomy_job_transitions where job_id=?",
    )
    .bind(job_id.to_string())
    .fetch_one(&mut **tx)
    .await?;
    sqlx::query(
        "insert into autonomy_job_transitions \
         (job_id, sequence, from_state, to_state, attempt, reason, lease_owner, occurred_at_ms) \
         values (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(job_id.to_string())
    .bind(sequence)
    .bind(from.map(AutonomyJobState::as_str))
    .bind(to.as_str())
    .bind(i64::from(attempt))
    .bind(reason)
    .bind(lease_owner)
    .bind(to_millis(occurred_at))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn load_job(tx: &mut Transaction<'_, Sqlite>, job_id: Uuid) -> Result<AutonomyJob> {
    let row = sqlx::query("select * from autonomy_jobs where job_id=?")
        .bind(job_id.to_string())
        .fetch_optional(&mut **tx)
        .await?
        .with_context(|| format!("autonomy job {job_id} does not exist"))?;
    decode_job(&row)
}

async fn load_checkpoint(
    tx: &mut Transaction<'_, Sqlite>,
    checkpoint_id: Uuid,
) -> Result<AutonomyCheckpoint> {
    let row = sqlx::query("select * from autonomy_checkpoints where checkpoint_id=?")
        .bind(checkpoint_id.to_string())
        .fetch_one(&mut **tx)
        .await?;
    decode_checkpoint(&row)
}

fn decode_job(row: &sqlx::sqlite::SqliteRow) -> Result<AutonomyJob> {
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

fn decode_transition(row: &sqlx::sqlite::SqliteRow) -> Result<AutonomyJobTransition> {
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

fn decode_checkpoint(row: &sqlx::sqlite::SqliteRow) -> Result<AutonomyCheckpoint> {
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

fn validate_enqueue(input: &EnqueueAutonomyJob) -> Result<()> {
    validate_optional_text(
        &input.idempotency_key,
        MAX_IDEMPOTENCY_KEY_BYTES,
        "idempotency key",
    )?;
    validate_optional_text(&input.kind, MAX_KIND_BYTES, "job kind")?;
    ensure!(
        input.max_attempts > 0,
        "max_attempts must be greater than zero"
    );
    let payload_bytes = serde_json::to_vec(&input.payload)?.len();
    ensure!(
        payload_bytes <= MAX_PAYLOAD_BYTES,
        "job payload exceeds {MAX_PAYLOAD_BYTES} bytes"
    );
    Ok(())
}

fn validate_checkpoint(input: &SaveAutonomyCheckpoint) -> Result<()> {
    validate_optional_text(
        &input.checkpoint_key,
        MAX_CHECKPOINT_KEY_BYTES,
        "checkpoint key",
    )?;
    validate_optional_text(&input.kind, MAX_CHECKPOINT_KIND_BYTES, "checkpoint kind")?;
    let json_bytes = serde_json::to_vec(&(&input.state, &input.evidence))?.len();
    ensure!(
        json_bytes <= MAX_CHECKPOINT_JSON_BYTES,
        "checkpoint JSON exceeds {MAX_CHECKPOINT_JSON_BYTES} bytes"
    );
    Ok(())
}

fn validate_owner(owner: &str) -> Result<()> {
    validate_optional_text(owner, MAX_OWNER_BYTES, "lease owner")
}

fn validate_optional_text(value: &str, max_bytes: usize, label: &str) -> Result<()> {
    ensure!(!value.trim().is_empty(), "{label} must not be empty");
    ensure!(
        value.len() <= max_bytes,
        "{label} exceeds {max_bytes} bytes"
    );
    Ok(())
}

fn validate_batch_size(limit: u32) -> Result<()> {
    ensure!(
        limit <= MAX_BATCH_SIZE,
        "batch limit exceeds {MAX_BATCH_SIZE}"
    );
    Ok(())
}

fn validate_lease_for_transition(
    job: &AutonomyJob,
    lease: Option<&AutonomyLease>,
    now: DateTime<Utc>,
) -> Result<()> {
    match job.state {
        AutonomyJobState::Claimed | AutonomyJobState::Running => {
            let lease = lease.context("a live job transition requires its lease")?;
            ensure!(
                job.lease_owner.as_deref() == Some(lease.owner.as_str())
                    && job.lease_token == Some(lease.token)
                    && job
                        .lease_expires_at
                        .is_some_and(|expires_at| expires_at > now),
                "lease is missing, fenced, or expired"
            );
        }
        AutonomyJobState::Queued | AutonomyJobState::Failed => {
            ensure!(
                lease.is_none(),
                "queued or failed job must not carry a lease"
            );
        }
        AutonomyJobState::Completed | AutonomyJobState::Cancelled => {
            unreachable!("terminal states cannot transition")
        }
    }
    Ok(())
}

fn checkpoint_hash(state_json: &str, evidence_json: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"borg-autonomy-checkpoint-v1\0");
    hasher.update(state_json.as_bytes());
    hasher.update([0]);
    hasher.update(evidence_json.as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

fn add_duration(now: DateTime<Utc>, duration: Duration) -> Result<DateTime<Utc>> {
    let duration = ChronoDuration::from_std(duration).context("lease duration is out of range")?;
    now.checked_add_signed(duration)
        .context("lease expiration is out of range")
}

fn to_millis(value: DateTime<Utc>) -> i64 {
    value.timestamp_millis()
}

fn from_millis(value: i64, field: &str) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_millis(value)
        .with_context(|| format!("invalid {field} millisecond timestamp {value}"))
}

fn from_optional_millis(value: Option<i64>, field: &str) -> Result<Option<DateTime<Utc>>> {
    value.map(|value| from_millis(value, field)).transpose()
}

fn parse_uuid(value: String, field: &str) -> Result<Uuid> {
    Uuid::parse_str(&value).with_context(|| format!("invalid {field} UUID {value:?}"))
}

fn parse_optional_uuid(value: Option<String>, field: &str) -> Result<Option<Uuid>> {
    value.map(|value| parse_uuid(value, field)).transpose()
}

fn to_u32(value: i64, field: &str) -> Result<u32> {
    u32::try_from(value).with_context(|| format!("invalid {field} value {value}"))
}

fn to_u64(value: i64, field: &str) -> Result<u64> {
    u64::try_from(value).with_context(|| format!("invalid {field} value {value}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn store() -> SqliteAutonomyStore {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory SQLite pool");
        SqliteAutonomyStore::open(pool)
            .await
            .expect("autonomy schema")
    }

    fn at(second: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(second, 0).expect("test timestamp")
    }

    fn enqueue(key: &str, due_at: DateTime<Utc>, max_attempts: u32) -> EnqueueAutonomyJob {
        EnqueueAutonomyJob {
            job_id: None,
            idempotency_key: key.to_owned(),
            kind: "test.runtime".to_owned(),
            payload: serde_json::json!({"key": key, "version": 1}),
            due_at,
            max_attempts,
            session_id: Some(Uuid::from_u128(11)),
            goal_id: Some(Uuid::from_u128(22)),
        }
    }

    struct EchoHandler;

    #[async_trait]
    impl AutonomyJobHandler for EchoHandler {
        async fn execute(&self, job: AutonomyJob) -> Result<Value> {
            Ok(serde_json::json!({"job_id": job.job_id, "kind": job.kind}))
        }
    }

    #[tokio::test]
    async fn enqueue_is_idempotent_and_initial_transition_is_durable() {
        let store = store().await;
        let input = enqueue("same-request", at(1_700_000_000), 2);
        let first = store.enqueue(input.clone()).await.expect("enqueue");
        let second = store.enqueue(input).await.expect("idempotent enqueue");
        assert_eq!(first.job_id, second.job_id);
        assert_eq!(first.state, AutonomyJobState::Queued);

        let transitions = store
            .list_transitions(first.job_id)
            .await
            .expect("transition audit");
        assert_eq!(transitions.len(), 1);
        assert_eq!(transitions[0].from, None);
        assert_eq!(transitions[0].to, AutonomyJobState::Queued);

        let mut changed = enqueue("same-request", at(1_700_000_000), 2);
        changed.payload = serde_json::json!({"changed": true});
        assert!(store.enqueue(changed).await.is_err());
    }

    #[tokio::test]
    async fn claim_heartbeat_and_transition_are_fenced_and_audited() {
        let store = store().await;
        let job = store
            .enqueue(enqueue("lease", at(1_700_000_000), 2))
            .await
            .expect("enqueue");
        let claimed = store
            .claim_due(at(1_700_000_001), "worker-a", Duration::from_secs(30), 1)
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].attempt, 1);
        let lease = claimed[0].lease().expect("claim lease");

        let wrong = AutonomyLease {
            owner: "worker-b".to_owned(),
            token: lease.token,
        };
        assert!(
            store
                .heartbeat(
                    job.job_id,
                    &wrong,
                    at(1_700_000_002),
                    Duration::from_secs(30)
                )
                .await
                .is_err()
        );
        let heartbeated = store
            .heartbeat(
                job.job_id,
                &lease,
                at(1_700_000_002),
                Duration::from_secs(30),
            )
            .await
            .expect("heartbeat");
        assert_eq!(heartbeated.state, AutonomyJobState::Claimed);

        assert!(
            store
                .transition(
                    job.job_id,
                    AutonomyJobState::Claimed,
                    AutonomyJobState::Completed,
                    Some(&wrong),
                    None,
                    at(1_700_000_003),
                )
                .await
                .is_err()
        );
        let running = store
            .transition(
                job.job_id,
                AutonomyJobState::Claimed,
                AutonomyJobState::Running,
                Some(&lease),
                None,
                at(1_700_000_003),
            )
            .await
            .expect("running transition");
        let completed = store
            .transition(
                job.job_id,
                AutonomyJobState::Running,
                AutonomyJobState::Completed,
                running.lease().as_ref(),
                Some("done".to_owned()),
                at(1_700_000_004),
            )
            .await
            .expect("completed transition");
        assert!(completed.state.is_terminal());

        let transitions = store
            .list_transitions(job.job_id)
            .await
            .expect("transition audit");
        assert_eq!(
            transitions
                .iter()
                .map(|transition| transition.to)
                .collect::<Vec<_>>(),
            vec![
                AutonomyJobState::Queued,
                AutonomyJobState::Claimed,
                AutonomyJobState::Running,
                AutonomyJobState::Completed,
            ]
        );
    }

    #[tokio::test]
    async fn session_scoped_claims_do_not_cross_session_boundaries() {
        let store = store().await;
        let first_session = Uuid::from_u128(11);
        let second_session = Uuid::from_u128(12);
        let first = store
            .enqueue(enqueue("first", at(1), 2))
            .await
            .expect("first job");
        let second = store
            .enqueue(EnqueueAutonomyJob {
                job_id: None,
                idempotency_key: "second".to_string(),
                kind: "test.runtime".to_string(),
                payload: serde_json::json!({"key": "second"}),
                due_at: at(1),
                max_attempts: 2,
                session_id: Some(second_session),
                goal_id: None,
            })
            .await
            .expect("second job");

        let claimed = store
            .claim_due_for_session(
                at(2),
                "session-11",
                Duration::from_secs(30),
                8,
                first_session,
            )
            .await
            .expect("scoped claim");
        assert_eq!(
            claimed.iter().map(|job| job.job_id).collect::<Vec<_>>(),
            vec![first.job_id]
        );
        assert_eq!(
            store
                .get(second.job_id)
                .await
                .expect("second lookup")
                .expect("second row")
                .state,
            AutonomyJobState::Queued
        );
    }

    #[tokio::test]
    async fn invalid_transition_rolls_back_job_and_audit_atomically() {
        let store = store().await;
        let job = store
            .enqueue(enqueue("acid", at(1_700_000_000), 1))
            .await
            .expect("enqueue");
        let claimed = store
            .claim_due(at(1_700_000_001), "worker", Duration::from_secs(10), 1)
            .await
            .expect("claim")
            .pop()
            .expect("claimed job");
        let lease = claimed.lease().expect("lease");
        assert!(
            store
                .transition(
                    job.job_id,
                    AutonomyJobState::Claimed,
                    AutonomyJobState::Completed,
                    None,
                    None,
                    at(1_700_000_002),
                )
                .await
                .is_err()
        );
        let transitions = store
            .list_transitions(job.job_id)
            .await
            .expect("audit after rollback");
        assert_eq!(transitions.len(), 2);
        let current = store
            .heartbeat(
                job.job_id,
                &lease,
                at(1_700_000_002),
                Duration::from_secs(10),
            )
            .await
            .expect("claim remained live");
        assert_eq!(current.state, AutonomyJobState::Claimed);
    }

    #[tokio::test]
    async fn expired_claims_requeue_then_fail_at_attempt_budget() {
        let store = store().await;
        let job = store
            .enqueue(enqueue("recovery", at(1_700_000_000), 2))
            .await
            .expect("enqueue");
        let first = store
            .claim_due(at(1_700_000_001), "worker-a", Duration::from_secs(5), 1)
            .await
            .expect("first claim");
        assert_eq!(first[0].attempt, 1);
        let requeued = store
            .recover_expired(at(1_700_000_007), 1)
            .await
            .expect("recovery");
        assert_eq!(requeued[0].state, AutonomyJobState::Queued);
        assert_eq!(requeued[0].attempt, 1);

        let second = store
            .claim_due(at(1_700_000_008), "worker-b", Duration::from_secs(5), 1)
            .await
            .expect("second claim");
        assert_eq!(second[0].attempt, 2);
        let failed = store
            .recover_expired(at(1_700_000_014), 1)
            .await
            .expect("terminal recovery");
        assert_eq!(failed[0].state, AutonomyJobState::Failed);
        assert!(
            failed[0]
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("exhausted"))
        );
        assert_eq!(
            store
                .claim_due(at(1_700_000_015), "worker-c", Duration::from_secs(5), 1)
                .await
                .expect("terminal job is not claimable")
                .len(),
            0
        );
        assert_eq!(store.list_transitions(job.job_id).await.unwrap().len(), 5);
    }

    #[tokio::test]
    async fn checkpoints_are_idempotent_hashed_and_linked() {
        let store = store().await;
        let job = store
            .enqueue(enqueue("checkpoint", at(1_700_000_000), 1))
            .await
            .expect("enqueue");
        let input = SaveAutonomyCheckpoint {
            checkpoint_id: None,
            job_id: job.job_id,
            checkpoint_key: "step-1".to_owned(),
            session_id: job.session_id,
            goal_id: job.goal_id,
            kind: "tool-result".to_owned(),
            state: serde_json::json!({"step": 1}),
            evidence: serde_json::json!({"stdout": "stable"}),
            created_at: at(1_700_000_010),
        };
        let first = store
            .save_checkpoint(input.clone())
            .await
            .expect("checkpoint");
        let second = store
            .save_checkpoint(input)
            .await
            .expect("idempotent checkpoint");
        assert_eq!(first, second);
        assert!(first.content_hash.starts_with("sha256:"));
        assert_eq!(first.session_id, job.session_id);
        assert_eq!(first.goal_id, job.goal_id);
        assert_eq!(
            store.list_checkpoints(job.job_id).await.unwrap(),
            vec![first]
        );

        let changed = SaveAutonomyCheckpoint {
            checkpoint_id: None,
            job_id: job.job_id,
            checkpoint_key: "step-1".to_owned(),
            session_id: job.session_id,
            goal_id: job.goal_id,
            kind: "tool-result".to_owned(),
            state: serde_json::json!({"step": 2}),
            evidence: serde_json::json!({"stdout": "stable"}),
            created_at: at(1_700_000_010),
        };
        assert!(store.save_checkpoint(changed).await.is_err());
    }

    #[tokio::test]
    async fn supervisor_executes_and_persists_a_fenced_result() {
        let store = store().await;
        let job = store
            .enqueue(enqueue("supervisor", at(1_700_000_000), 2))
            .await
            .expect("enqueue");
        let supervisor =
            SqliteAutonomySupervisor::new(store.clone(), Arc::new(EchoHandler), "test-supervisor")
                .expect("supervisor")
                .with_limits(Duration::from_secs(30), Duration::from_secs(1), 1)
                .expect("limits");

        assert_eq!(supervisor.run_once(Utc::now()).await.unwrap(), 1);
        let completed = store.get(job.job_id).await.unwrap().unwrap();
        assert_eq!(completed.state, AutonomyJobState::Completed);
        assert_eq!(
            completed.result,
            Some(serde_json::json!({"job_id": job.job_id, "kind": "test.runtime"}))
        );
        assert_eq!(store.list_transitions(job.job_id).await.unwrap().len(), 4);
    }
}
