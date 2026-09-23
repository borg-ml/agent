//! Atomic resource admission, persistent FIFO tickets and isolated job execution.

use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Host identity is implicit: keys never coordinate resources across machines.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum ResourceScope {
    Host,
    /// Canonicalized absolute path to the project identity file or root.
    Project(PathBuf),
    /// Canonicalized absolute worktree root; distinct trees can build concurrently.
    Worktree(PathBuf),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ResourceKey {
    pub scope: ResourceScope,
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Capacity {
    pub key: ResourceKey,
    pub slots: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Access {
    Shared { slots: u32 },
    Exclusive,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceRequest {
    pub key: ResourceKey,
    pub access: Access,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Holder {
    pub participant_id: Uuid,
    pub session_id: Uuid,
    pub host_pid: Option<u32>,
    pub purpose: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaseRequest {
    pub resources: Vec<ResourceRequest>,
    pub holder: Holder,
    pub queue_timeout_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ticket {
    pub id: Uuid,
    /// Monotonically increasing admission order on this host.
    pub sequence: u64,
}

/// Descriptive handle only: possession never substitutes for the supervisor's kernel lock.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Lease {
    pub id: Uuid,
    pub ticket: Ticket,
    pub resources: Vec<ResourceRequest>,
    pub holder: Holder,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TicketState {
    Queued,
    Granted(Lease),
    Finished,
    Cancelled { reason: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdmissionBudget {
    pub min_available_ram_bytes: u64,
    pub reserve_ram_bytes: u64,
    pub min_free_disk_bytes: u64,
    pub reserve_disk_bytes: u64,
    pub disk_path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Hook {
    pub argv: Vec<String>,
    pub timeout_ms: u64,
}

/// Fingerprint includes canonical tree, inputs, target, args, toolchain and output policy.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct JobFingerprint(pub String);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobSpec {
    pub fingerprint: JobFingerprint,
    pub lease: LeaseRequest,
    pub argv: Vec<String>,
    pub cwd: PathBuf,
    pub env: Vec<(String, String)>,
    pub memory_max_bytes: Option<u64>,
    pub admission: AdmissionBudget,
    pub pre_hook: Option<Hook>,
    pub post_hook: Option<Hook>,
    pub timeout_ms: u64,
    pub stall_timeout_ms: Option<u64>,
    pub coalesce: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum JobState {
    Queued,
    Running { scope: String },
    Finished { exit_code: i32 },
    Cancelled { reason: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobHandle {
    pub id: Uuid,
    pub ticket: Ticket,
    pub fingerprint: JobFingerprint,
    pub log_path: PathBuf,
    pub state: JobState,
}

/// Subscribe to a ticket/job's terminal event, not a path that could outlive its producer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LaneEvent {
    Ticket { ticket: Ticket, state: TicketState },
    Job { job: JobHandle },
}

/// Implement in the host-local supervisor; implementations must journal transitions.
#[async_trait]
pub trait LaneCoordinator: Send + Sync {
    async fn enqueue(&self, request: LeaseRequest) -> Result<Ticket>;
    async fn wait(&self, ticket: &Ticket) -> Result<Lease>;
    async fn release(&self, lease: &Lease) -> Result<()>;
    async fn ticket_status(&self, ticket: &Ticket) -> Result<TicketState>;
    async fn recover(&self, dry_run: bool) -> Result<Vec<String>>;
}

#[async_trait]
pub trait JobCoordinator: Send + Sync {
    /// Identical pending jobs attach to one job; never reuse stale running inputs.
    async fn submit(&self, spec: JobSpec) -> Result<JobHandle>;
    async fn wait(&self, id: Uuid) -> Result<JobHandle>;
    async fn status(&self, id: Uuid) -> Result<JobHandle>;
    async fn cancel(&self, id: Uuid) -> Result<()>;
}
