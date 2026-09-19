//! Provider-neutral durable runtime jobs backed by PostgreSQL.
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

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Claimed => "claimed",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self> {
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

    pub(crate) const fn can_transition_to(self, next: Self) -> bool {
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

/// A lease fence returned by [`AutonomyStore::claim_due`].
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

/// The durable runtime-job journal, independent of engine.
///
/// WHY A TRAIT: admission, leases, retries and checkpoints are a state machine,
/// and the whole point of running it on Postgres is that two supervisors can
/// sweep for due work concurrently instead of queueing behind one file writer.
/// Both backends implement this identically -- the conformance suite drives
/// this trait against each -- so a caller holding `dyn AutonomyStore` cannot
/// come to depend on either engine's incidental behaviour.
///
/// `lease_owner` is `&str` rather than `impl AsRef<str>` because the trait must
/// be object-safe; that is the only signature that differs from the original
/// inherent methods.
#[async_trait]
pub trait AutonomyStore: Send + Sync {
    /// Enqueue a job, returning the existing row for a repeated idempotency
    /// key when its immutable request fields match exactly.
    async fn enqueue(&self, input: EnqueueAutonomyJob) -> Result<AutonomyJob>;

    /// One job by id.
    async fn get(&self, job_id: Uuid) -> Result<Option<AutonomyJob>>;

    /// Claim up to `limit` due jobs for `lease_owner`.
    async fn claim_due(
        &self,
        now: DateTime<Utc>,
        lease_owner: &str,
        lease_duration: Duration,
        limit: u32,
    ) -> Result<Vec<AutonomyJob>>;

    /// Claim due jobs belonging to one session only.
    async fn claim_due_for_session(
        &self,
        now: DateTime<Utc>,
        lease_owner: &str,
        lease_duration: Duration,
        limit: u32,
        session_id: Uuid,
    ) -> Result<Vec<AutonomyJob>>;

    /// Extend a live lease. Fails if the fence no longer matches, which is how
    /// a superseded worker learns it has been replaced.
    async fn heartbeat(
        &self,
        job_id: Uuid,
        lease: &AutonomyLease,
        now: DateTime<Utc>,
        lease_duration: Duration,
    ) -> Result<AutonomyJob>;

    /// Move a job between states, refusing illegal edges.
    async fn transition(
        &self,
        job_id: Uuid,
        expected: AutonomyJobState,
        next: AutonomyJobState,
        lease: Option<&AutonomyLease>,
        reason: Option<String>,
        now: DateTime<Utc>,
    ) -> Result<AutonomyJob>;

    /// Record a terminal result under a live lease.
    async fn complete(
        &self,
        job_id: Uuid,
        lease: &AutonomyLease,
        result: Value,
        now: DateTime<Utc>,
    ) -> Result<AutonomyJob>;

    /// Return jobs whose leases expired to the queue, so a crashed worker's
    /// work is retried rather than stranded.
    async fn recover_expired(&self, now: DateTime<Utc>, limit: u32) -> Result<Vec<AutonomyJob>>;

    /// Recover expired leases for one session only.
    async fn recover_expired_for_session(
        &self,
        now: DateTime<Utc>,
        limit: u32,
        session_id: Uuid,
    ) -> Result<Vec<AutonomyJob>>;

    /// Save a reproducible checkpoint for a job.
    async fn save_checkpoint(&self, input: SaveAutonomyCheckpoint) -> Result<AutonomyCheckpoint>;

    /// Every checkpoint recorded for a job, oldest first.
    async fn list_checkpoints(&self, job_id: Uuid) -> Result<Vec<AutonomyCheckpoint>>;

    /// The audited state-transition history for a job.
    async fn list_transitions(&self, job_id: Uuid) -> Result<Vec<AutonomyJobTransition>>;
}

/// Provider- and tool-neutral execution hook for durable jobs. The handler is
/// deliberately outside the store: the store owns admission, leases, retries,
/// and results, while the session/runtime owns what a job means.
#[async_trait]
pub trait AutonomyJobHandler: Send + Sync {
    async fn execute(&self, job: AutonomyJob) -> Result<Value>;
}

/// A bounded, lease-aware worker for the autonomous runtime journal.
///
/// A worker may be started in a resident session process or in a host
/// supervisor. Claiming and completion are fenced in the store, so a crashed
/// worker can be replaced without duplicating a completed job.
#[derive(Clone)]
pub struct AutonomySupervisor {
    store: Arc<dyn AutonomyStore>,
    handler: Arc<dyn AutonomyJobHandler>,
    owner: String,
    session_id: Option<Uuid>,
    lease_duration: Duration,
    poll_interval: Duration,
    batch_size: u32,
}

impl AutonomySupervisor {
    pub fn new(
        store: Arc<dyn AutonomyStore>,
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

    /// Restrict this worker to jobs owned by one session. A shared
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
    /// transient store/provider failure while the lease fence prevents dupes.
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

pub(crate) struct AutonomyTransition {
    pub(crate) job_id: Uuid,
    pub(crate) from: Option<AutonomyJobState>,
    pub(crate) to: AutonomyJobState,
    pub(crate) attempt: u32,
    pub(crate) reason: Option<String>,
    pub(crate) lease_owner: Option<String>,
    pub(crate) occurred_at: DateTime<Utc>,
}

pub(crate) fn validate_enqueue(input: &EnqueueAutonomyJob) -> Result<()> {
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

pub(crate) fn validate_checkpoint(input: &SaveAutonomyCheckpoint) -> Result<()> {
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

pub(crate) fn validate_owner(owner: &str) -> Result<()> {
    validate_optional_text(owner, MAX_OWNER_BYTES, "lease owner")
}

pub(crate) fn validate_optional_text(value: &str, max_bytes: usize, label: &str) -> Result<()> {
    ensure!(!value.trim().is_empty(), "{label} must not be empty");
    ensure!(
        value.len() <= max_bytes,
        "{label} exceeds {max_bytes} bytes"
    );
    Ok(())
}

pub(crate) fn validate_batch_size(limit: u32) -> Result<()> {
    ensure!(
        limit <= MAX_BATCH_SIZE,
        "batch limit exceeds {MAX_BATCH_SIZE}"
    );
    Ok(())
}

pub(crate) fn validate_lease_for_transition(
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

pub(crate) fn checkpoint_hash(state_json: &str, evidence_json: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"borg-autonomy-checkpoint-v1\0");
    hasher.update(state_json.as_bytes());
    hasher.update([0]);
    hasher.update(evidence_json.as_bytes());
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

pub(crate) fn add_duration(now: DateTime<Utc>, duration: Duration) -> Result<DateTime<Utc>> {
    let duration = ChronoDuration::from_std(duration).context("lease duration is out of range")?;
    now.checked_add_signed(duration)
        .context("lease expiration is out of range")
}

pub(crate) fn to_millis(value: DateTime<Utc>) -> i64 {
    value.timestamp_millis()
}

pub(crate) fn from_millis(value: i64, field: &str) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_millis(value)
        .with_context(|| format!("invalid {field} millisecond timestamp {value}"))
}

pub(crate) fn from_optional_millis(
    value: Option<i64>,
    field: &str,
) -> Result<Option<DateTime<Utc>>> {
    value.map(|value| from_millis(value, field)).transpose()
}

pub(crate) fn parse_uuid(value: String, field: &str) -> Result<Uuid> {
    Uuid::parse_str(&value).with_context(|| format!("invalid {field} UUID {value:?}"))
}

pub(crate) fn parse_optional_uuid(value: Option<String>, field: &str) -> Result<Option<Uuid>> {
    value.map(|value| parse_uuid(value, field)).transpose()
}

pub(crate) fn to_u32(value: i64, field: &str) -> Result<u32> {
    u32::try_from(value).with_context(|| format!("invalid {field} value {value}"))
}

pub(crate) fn to_u64(value: i64, field: &str) -> Result<u64> {
    u64::try_from(value).with_context(|| format!("invalid {field} value {value}"))
}
