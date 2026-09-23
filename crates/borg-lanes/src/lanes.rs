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

impl ResourceKey {
    /// Reject aliases before any ticket, capacity or service binding can enter
    /// the journal. Only already-canonical, existing absolute roots are valid.
    pub fn validate_canonical(&self) -> Result<()> {
        if let ResourceScope::Project(path) | ResourceScope::Worktree(path) = &self.scope {
            ensure!(
                path.is_absolute(),
                "lane resource path must be absolute: {}",
                path.display()
            );
            let resolved = fs::canonicalize(path)
                .with_context(|| format!("lane resource path must exist: {}", path.display()))?;
            ensure!(
                &resolved == path,
                "lane resource path is not canonical: {} (expected {})",
                path.display(),
                resolved.display()
            );
        }
        Ok(())
    }
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
    /// Monotone fencing token (global ticket sequence, hence per-key monotone).
    #[serde(default)]
    pub generation: u64,
    pub ticket: Ticket,
    pub resources: Vec<ResourceRequest>,
    pub holder: Holder,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TicketState {
    Queued,
    /// FIFO reservation; pre-exclusive yield has not completed, so no lease is granted.
    Preparing,
    Granted(Lease),
    Finished,
    Cancelled {
        reason: String,
    },
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

fn default_foreign_client_grace_ms() -> u64 {
    300_000
}

/// Override a job's foreign-client wait for one exclusive resource key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ForeignClientGrace {
    pub resource: ResourceKey,
    /// Zero means wait indefinitely for clients on this resource.
    pub grace_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobSpec {
    /// Foreign client leases delay preemption for this many milliseconds.
    /// Zero waits indefinitely; default is five minutes.
    #[serde(default = "default_foreign_client_grace_ms")]
    pub foreign_client_grace_ms: u64,
    /// Per-exclusive-resource grace; unspecified resources use the job-wide default.
    #[serde(default)]
    pub foreign_client_grace_by_resource: Vec<ForeignClientGrace>,
    /// Cancel (queued) or kill (running) the job once no `job wait` has held
    /// it for this long since submit; None keeps it regardless.
    #[serde(default)]
    pub abandon_after_ms: Option<u64>,
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

// The journal is a single atomically replaced snapshot protected by a stable
// kernel lock. Scope and lease tokens describe ownership; open lock FDs prove it.
use anyhow::{Context, bail, ensure};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LaneRecord {
    pub ticket: Ticket,
    pub request: LeaseRequest,
    pub state: TicketState,
    pub job: Option<JobHandle>,
    pub spec: Option<JobSpec>,
    pub created_ms: u64,
    pub started_ms: Option<u64>,
    pub finished_ms: Option<u64>,
    pub supervisor_pid: Option<u32>,
    pub wait_reason: Option<String>,
    pub progress: Option<String>,
    pub parallelism_hint: Option<u32>,
    pub cpu_seconds: Option<f64>,
    #[serde(default)]
    pub workload_pid: Option<u32>,
    #[serde(default)]
    pub workload_start_ticks: Option<u64>,
    #[serde(default)]
    pub scope_cgroup: Option<String>,
    #[serde(default)]
    pub post_scope: Option<String>,
    #[serde(default)]
    pub quarantined: bool,
    #[serde(default)]
    pub service_lease: bool,
    /// Only a Granted row reserves service RAM and filesystem space.
    #[serde(default)]
    pub service_admission: Option<AdmissionBudget>,
    /// Set once when the exclusive first enters Preparing (not reset by retries).
    #[serde(default)]
    pub preparing_since_ms: Option<u64>,
    /// Active service IDs captured atomically when this exclusive enters Preparing.
    #[serde(default)]
    pub yield_services: Vec<String>,
    #[serde(default)]
    pub resume_pending: Vec<String>,
    #[serde(default)]
    pub resume_error: Option<String>,
    /// Finished resume-controller attempts, so `recover --wait` can tell a
    /// fresh outcome from an earlier error.
    #[serde(default)]
    pub resume_attempts: u64,
    /// A cancel request for a job past Queued. Its supervisor kills the
    /// workload and ends the record Cancelled with this reason.
    #[serde(default)]
    pub cancel_requested: Option<String>,
    pub evidence: Option<String>,
}

#[derive(Default, Clone, Serialize, Deserialize)]
struct Journal {
    sequence: u64,
    records: Vec<LaneRecord>,
    capacities: Vec<Capacity>,
    /// Client lease mutations also wake journal waiters.
    #[serde(default)]
    client_revision: u64,
}

/// One host-local lane store. Clones share held in-process lease FDs; cross-
/// process jobs are owned by a detached supervisor, not the submitter.
#[derive(Clone)]
pub struct LaneStore {
    root: PathBuf,
    held: Arc<Mutex<HashMap<Uuid, File>>>,
}

fn milliseconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn stable_file(path: &Path) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .with_context(|| format!("opening stable lane lock {}", path.display()))
}

impl LaneStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(root.join("locks"))?;
        fs::create_dir_all(root.join("jobs"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for path in [&root, &root.join("locks"), &root.join("jobs")] {
                fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
            }
        }
        Ok(Self {
            root,
            held: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn default_root() -> PathBuf {
        std::env::var_os("BORG_LANE_DIR")
            .or_else(|| std::env::var_os("BORG_LANES_ROOT"))
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("XDG_RUNTIME_DIR").map(|p| PathBuf::from(p).join("borg/lanes"))
            })
            .unwrap_or_else(|| {
                std::env::temp_dir().join(format!("borg-lanes-{}", unsafe { libc::geteuid() }))
            })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn locked<T>(&self, f: impl FnOnce(&mut Journal) -> Result<T>) -> Result<T> {
        let guard = stable_file(&self.root.join("state.lock"))?;
        guard.lock()?;
        let state = self.root.join("state.json");
        let mut journal: Journal = if state.exists() {
            serde_json::from_slice(&fs::read(&state)?)?
        } else {
            Journal::default()
        };
        let before = serde_json::to_vec(&journal)?;
        let result = f(&mut journal);
        if result.is_ok() && serde_json::to_vec(&journal)? != before {
            let tmp = self.root.join(format!("state.{}.tmp", Uuid::new_v4()));
            let mut out = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600))?;
            }
            out.write_all(&serde_json::to_vec(&journal)?)?;
            out.sync_all()?;
            fs::rename(&tmp, &state)?;
            File::open(&self.root)?.sync_all()?;
        }
        result
    }

    /// The service supervisor serializes every client grant, renewal and
    /// release with exclusive Preparing decisions. The callback publishes its
    /// own service state atomically *while* this metadata lock is held; jobs
    /// read that file under the same lock. Never call lane APIs in `change`.
    pub(crate) fn service_client_change<T>(
        &self,
        service_id: &str,
        resources: &[ResourceRequest],
        acquiring: bool,
        change: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        self.locked(|journal| {
            if acquiring {
                let tag = format!("service:{service_id}");
                ensure!(
                    journal.records.iter().any(|r| {
                        r.service_lease
                            && r.request.holder.purpose == tag
                            && matches!(r.state, TicketState::Granted(_))
                    }),
                    "service {service_id} holds no active lane lease for client grant"
                );
                if let Some(exclusive) = journal.records.iter().find(|r| {
                    matches!(r.state, TicketState::Preparing | TicketState::Granted(_))
                        && !r.service_lease
                        && r.request.resources.iter().any(|claim| {
                            matches!(claim.access, Access::Exclusive)
                                && resources.iter().any(|bound| bound.key == claim.key)
                        })
                }) {
                    bail!(
                        "exclusive lane ticket {} is preparing or active; new client lease denied",
                        exclusive.ticket.id
                    );
                }
            }
            let value = change()?;
            journal.client_revision = journal.client_revision.saturating_add(1);
            Ok(value)
        })
    }

    /// Advisory for the service's public status; the journal lock and client
    /// transaction, not this display, decide admission.
    pub(crate) fn service_preemption_notice(&self, service_id: &str) -> Result<Option<String>> {
        self.reading(|journal| {
            Ok(journal.records.iter().find_map(|row| {
                (matches!(row.state, TicketState::Preparing)
                    && row.yield_services.iter().any(|id| id == service_id))
                .then_some(row.wait_reason.as_deref())
                .flatten()
                .filter(|reason| reason.starts_with("foreign client lease:"))
                .map(str::to_owned)
            }))
        })
    }

    /// Global capacities are host-scoped keys; missing keys default to one.
    pub fn set_capacity(&self, capacity: Capacity) -> Result<()> {
        ensure!(capacity.slots > 0, "capacity must be positive");
        capacity.key.validate_canonical()?;
        self.locked(|state| {
            if let Some(existing) = state.capacities.iter_mut().find(|c| c.key == capacity.key) {
                *existing = capacity;
            } else {
                state.capacities.push(capacity);
            }
            Ok(())
        })
    }

    fn reading<T>(&self, f: impl FnOnce(&Journal) -> Result<T>) -> Result<T> {
        let lock = stable_file(&self.root.join("state.lock"))?;
        lock.lock_shared()?;
        let path = self.root.join("state.json");
        if !path.exists() {
            return f(&Journal::default());
        }
        f(&serde_json::from_slice(&fs::read(path)?)?)
    }

    /// Nonblocking shared claim by the long-lived service supervisor. The
    /// returned lease is valid only while this LaneStore instance retains its
    /// kernel FD; a service restart must acquire a NEW token before backend start.
    pub fn try_acquire_service(
        &self,
        request: LeaseRequest,
        budget: &AdmissionBudget,
    ) -> Result<Option<Lease>> {
        ensure!(
            request
                .resources
                .iter()
                .all(|r| matches!(r.access, Access::Shared { .. })),
            "service resource claims must be shared"
        );
        let service_id = request
            .holder
            .purpose
            .strip_prefix("service:")
            .context("service holder must have service:<id> purpose")?;
        ensure!(
            !service_id.is_empty()
                && service_id.len() <= 100
                && service_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "invalid service holder id"
        );
        let service_id = service_id.to_owned();
        let mut held: Option<File> = None;
        let lease = self.locked(|state| {
            // Preview a fresh position WITHOUT creating a new lock inode for
            // each denied retry. Only a successful attempt gets a new ticket.
            if let Some(index) = state.records.iter().position(|r| {
                r.service_lease
                    && r.request.holder.purpose == format!("service:{service_id}")
                    && matches!(r.state, TicketState::Cancelled { .. })
            }) {
                let mut preview = state.clone();
                let candidate = &mut preview.records[index];
                candidate.ticket.sequence = state.sequence.saturating_add(1);
                candidate.state = TicketState::Queued;
                let id = candidate.ticket.id;
                let reason = dispatch_reason(&preview, id)?;
                let reason = if reason.is_some() {
                    reason
                } else {
                    budget_reason(state, budget).unwrap_or_else(|error| {
                        Some(format!("budget inspection unavailable: {error:#}"))
                    })
                };
                if let Some(reason) = reason {
                    state.records[index].wait_reason = Some(reason.clone());
                    state.records[index].state = TicketState::Cancelled { reason };
                    return Ok(None);
                }
                state.records.remove(index);
            }
            let (ticket, _) = self.enqueue_record(state, request, None)?;
            let lock = stable_file(&self.ticket_path(ticket.id))?;
            lock.lock()?;
            held = Some(lock);
            let reason = dispatch_reason(state, ticket.id)?.or(budget_reason(state, budget)
                .unwrap_or_else(|error| Some(format!("budget inspection unavailable: {error:#}"))));
            let entry = state
                .records
                .iter_mut()
                .find(|r| r.ticket.id == ticket.id)
                .context("service ticket missing")?;
            entry.service_lease = true;
            entry.service_admission = Some(budget.clone());
            if let Some(reason) = reason {
                entry.wait_reason = Some(reason.clone());
                entry.state = TicketState::Cancelled { reason };
                entry.finished_ms = Some(milliseconds());
                return Ok(None);
            }
            Ok(Some(grant_entry(entry)))
        })?;
        if let Some(lease) = &lease {
            self.held
                .lock()
                .unwrap()
                .insert(lease.ticket.id, held.context("service FD missing")?);
        }
        Ok(lease)
    }

    /// Latest nonblocking refusal for a named service; a rejected attempt has
    /// no FIFO position but the supervisor can report its actual wait reason.
    pub fn service_admission_reason(&self, service_id: &str) -> Result<Option<String>> {
        self.reading(|state| {
            Ok(state
                .records
                .iter()
                .filter(|r| {
                    r.service_lease
                        && r.request.holder.purpose == format!("service:{service_id}")
                        && matches!(r.state, TicketState::Cancelled { .. })
                })
                .max_by_key(|r| r.ticket.sequence)
                .and_then(|r| r.wait_reason.clone()))
        })
    }

    /// Reject a stale token before dropping the owner FD. Expired/restarted
    /// services cannot release a newer generation.
    pub fn release_lease(&self, lease: &Lease) -> Result<()> {
        ensure!(
            self.held.lock().unwrap().contains_key(&lease.ticket.id),
            "lease is not held by this process"
        );
        self.locked(|state| {
            let record = state
                .records
                .iter_mut()
                .find(|r| r.ticket.id == lease.ticket.id)
                .context("lease vanished")?;
            ensure!(
                matches!(&record.state, TicketState::Granted(current)
                if current.id == lease.id && current.generation == lease.generation),
                "lease token mismatch"
            );
            record.state = TicketState::Finished;
            record.finished_ms = Some(milliseconds());
            Ok(())
        })?;
        self.held.lock().unwrap().remove(&lease.ticket.id);
        Ok(())
    }

    pub fn snapshot(&self) -> Result<Vec<LaneRecord>> {
        self.reading(|state| Ok(state.records.clone()))
    }

    fn ticket_path(&self, id: Uuid) -> PathBuf {
        self.root.join("locks").join(format!("{id}.flock"))
    }
    fn job_dir(&self, id: Uuid) -> PathBuf {
        self.root.join("jobs").join(id.to_string())
    }

    fn validate(request: &LeaseRequest, capacities: &[Capacity]) -> Result<()> {
        ensure!(
            !request.resources.is_empty(),
            "at least one resource required"
        );
        let mut seen = HashSet::new();
        for r in &request.resources {
            r.key.validate_canonical()?;
            ensure!(!r.key.name.trim().is_empty(), "empty resource name");
            ensure!(
                seen.insert(&r.key),
                "duplicate resource key: {}",
                r.key.name
            );
            let cap = capacity(capacities, &r.key);
            if let Access::Shared { slots } = r.access {
                ensure!(
                    slots > 0 && slots <= cap,
                    "shared slots must fit capacity of {} ({cap})",
                    r.key.name
                );
            }
        }
        Ok(())
    }

    fn enqueue_record(
        &self,
        state: &mut Journal,
        request: LeaseRequest,
        spec: Option<JobSpec>,
    ) -> Result<(Ticket, Option<JobHandle>)> {
        Self::validate(&request, &state.capacities)?;
        if let Some(ref spec) = spec {
            let mut grace_keys = HashSet::new();
            for override_ in &spec.foreign_client_grace_by_resource {
                ensure!(
                    spec.lease.resources.iter().any(|resource| {
                        resource.key == override_.resource
                            && matches!(resource.access, Access::Exclusive)
                    }),
                    "foreign-client grace override requires a requested exclusive resource: {}",
                    override_.resource.name
                );
                ensure!(
                    grace_keys.insert(&override_.resource),
                    "duplicate foreign-client grace override: {}",
                    override_.resource.name
                );
            }
            ensure!(!spec.argv.is_empty(), "a job needs a command");
            ensure!(spec.cwd.is_absolute(), "job cwd must be absolute");
            ensure!(
                spec.cwd.is_dir(),
                "job cwd does not exist: {}",
                spec.cwd.display()
            );
            if spec.coalesce
                && let Some(existing) = state.records.iter().find(|r| {
                    let Some(other) = r.spec.as_ref() else {
                        return false;
                    };
                    matches!(r.state, TicketState::Queued)
                        && r.job
                            .as_ref()
                            .is_some_and(|j| matches!(j.state, JobState::Queued))
                        && spec.fingerprint == other.fingerprint
                        && spec.argv == other.argv
                        && spec.cwd == other.cwd
                        && spec.env == other.env
                        && serde_json::to_value(&spec.lease.resources).ok()
                            == serde_json::to_value(&other.lease.resources).ok()
                        && serde_json::to_value(&spec.admission).ok()
                            == serde_json::to_value(&other.admission).ok()
                        && serde_json::to_value(&spec.pre_hook).ok()
                            == serde_json::to_value(&other.pre_hook).ok()
                        && serde_json::to_value(&spec.post_hook).ok()
                            == serde_json::to_value(&other.post_hook).ok()
                        && spec.memory_max_bytes == other.memory_max_bytes
                        && spec.foreign_client_grace_ms == other.foreign_client_grace_ms
                        && spec.foreign_client_grace_by_resource
                            == other.foreign_client_grace_by_resource
                        && spec.abandon_after_ms == other.abandon_after_ms
                        && spec.timeout_ms == other.timeout_ms
                        && spec.stall_timeout_ms == other.stall_timeout_ms
                        // Only the existing ticket's supervisor enforces a
                        // queue timeout, so a joiner is bound by that ticket's
                        // limit, not its own. Join only an equal limit: the
                        // ticket was queued first, so it expires no later than
                        // the joiner's own deadline would. A differing limit
                        // (including None against Some) gets its own ticket.
                        && spec.lease.queue_timeout_ms == other.lease.queue_timeout_ms
                })
            {
                return Ok((existing.ticket.clone(), existing.job.clone()));
            }
        }
        state.sequence += 1;
        let ticket = Ticket {
            id: Uuid::now_v7(),
            sequence: state.sequence,
        };
        let job = spec.as_ref().map(|s| JobHandle {
            id: ticket.id,
            ticket: ticket.clone(),
            fingerprint: s.fingerprint.clone(),
            log_path: self.job_dir(ticket.id).join("output.log"),
            state: JobState::Queued,
        });
        if job.is_some() {
            fs::create_dir_all(self.job_dir(ticket.id))?;
        }
        // Never unlink a lock inode: waiters and a crashed process may still hold it.
        stable_file(&self.ticket_path(ticket.id))?;
        state.records.push(LaneRecord {
            ticket: ticket.clone(),
            request,
            state: TicketState::Queued,
            job: job.clone(),
            spec,
            created_ms: milliseconds(),
            started_ms: None,
            finished_ms: None,
            supervisor_pid: None,
            wait_reason: None,
            progress: None,
            parallelism_hint: None,
            cpu_seconds: None,
            workload_pid: None,
            workload_start_ticks: None,
            scope_cgroup: None,
            post_scope: None,
            quarantined: false,
            service_lease: false,
            service_admission: None,
            preparing_since_ms: None,
            yield_services: vec![],
            resume_pending: vec![],
            resume_error: None,
            resume_attempts: 0,
            cancel_requested: None,
            evidence: None,
        });
        Ok((ticket, job))
    }

    pub fn enqueue_lease(&self, request: LeaseRequest) -> Result<Ticket> {
        self.locked(|state| {
            self.enqueue_record(state, request, None)
                .map(|(ticket, _)| ticket)
        })
    }

    pub fn enqueue_job(&self, spec: JobSpec) -> Result<JobHandle> {
        // A submitter holds the completion lock while publishing and spawning,
        // so a waiter never sees a queued job without a live owner.
        let mut owner: Option<File> = None;
        let (job, newly_created) = self.locked(|state| {
            let old = state.sequence;
            let (_, job) = self.enqueue_record(state, spec.lease.clone(), Some(spec))?;
            let job = job.context("job submission yielded no handle")?;
            if state.sequence != old {
                let lock = stable_file(&self.ticket_path(job.id))?;
                lock.lock()?;
                owner = Some(lock);
            }
            Ok((job, state.sequence != old))
        })?;
        if newly_created {
            let lock = owner.context("new job has no lock")?;
            let executable = std::env::var_os("BORG_LANE_EXECUTABLE")
                .map(PathBuf::from)
                .unwrap_or(std::env::current_exe()?);
            // The lock FD crosses exec to the supervisor, not to the workload.
            use std::os::fd::AsRawFd;
            let fd = lock.as_raw_fd();
            unsafe {
                libc::fcntl(fd, libc::F_SETFD, 0);
            }
            // Detach the supervisor from its submitter: a session of its own
            // (no terminal Ctrl-C or process-group kill) and, with a user
            // manager, a scope of its own (it outlives the caller's cgroup).
            // `systemd-run --scope` execs in place, keeping the lock FD.
            let mut command = if workload_scoped() {
                let mut command = Command::new("systemd-run");
                command
                    .args([
                        "--user",
                        "--scope",
                        "--quiet",
                        "--collect",
                        "--expand-environment=no",
                    ])
                    .arg(format!("--unit=borg-lane-sup-{}", job.id))
                    .arg("--")
                    .arg(&executable);
                command
            } else {
                Command::new(&executable)
            };
            command
                .args(["lane", "__supervise", "--state-dir"])
                .arg(&self.root)
                .arg(job.id.to_string())
                .env("BORG_LANE_LOCK_FD", fd.to_string())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            unsafe {
                use std::os::unix::process::CommandExt;
                command.pre_exec(|| {
                    if libc::setsid() < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            let spawned = command.spawn();
            unsafe {
                libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
            }
            if let Err(error) = spawned {
                self.finish(job.id, 125, &format!("cannot spawn supervisor: {error}"))?;
            }
            drop(lock);
        }
        self.job_status(job.id)
    }

    fn record(&self, id: Uuid) -> Result<LaneRecord> {
        self.reading(|state| {
            state
                .records
                .iter()
                .find(|r| r.ticket.id == id)
                .cloned()
                .with_context(|| format!("unknown lane ticket/job {id}"))
        })
    }
    pub fn job_status(&self, id: Uuid) -> Result<JobHandle> {
        self.record(id)?.job.context("ticket has no job")
    }
    pub fn ticket_state(&self, ticket: &Ticket) -> Result<TicketState> {
        Ok(self.record(ticket.id)?.state)
    }

    /// A `job wait` holds this shared lock for as long as it waits, which is
    /// what keeps a job with `abandon_after_ms` alive.
    fn hold_as_requester(&self, id: Uuid) -> Result<File> {
        let file = stable_file(&self.job_dir(id).join("waiters.lock"))?;
        file.lock_shared()?;
        Ok(file)
    }

    fn has_requester(&self, id: Uuid) -> bool {
        let Ok(file) = stable_file(&self.job_dir(id).join("waiters.lock")) else {
            return true;
        };
        match file.try_lock() {
            Ok(()) => {
                let _ = file.unlock();
                false
            }
            Err(_) => true,
        }
    }

    /// Whether the job has had no requester for its abandon window; while a
    /// requester is seen, `last_seen_ms` moves forward.
    fn abandoned(&self, spec: &JobSpec, id: Uuid, last_seen_ms: &mut u64) -> bool {
        let Some(after) = spec.abandon_after_ms else {
            return false;
        };
        let now = milliseconds();
        if self.has_requester(id) {
            *last_seen_ms = now;
            return false;
        }
        now.saturating_sub(*last_seen_ms) >= after
    }

    /// Block, as a requester, until the workload has started (Running) or the
    /// job ended. A free ticket lock means its supervisor died: recover first.
    pub fn wait_started(&self, id: Uuid) -> Result<JobHandle> {
        let _requester = self.hold_as_requester(id)?;
        let events = StateEvents::new(&self.root)?;
        loop {
            let job = self.job_status(id)?;
            if !matches!(job.state, JobState::Queued) {
                return Ok(job);
            }
            let lock = stable_file(&self.ticket_path(id))?;
            if lock.try_lock_shared().is_ok() {
                drop(lock);
                self.recover(false)?;
                return self.job_status(id);
            }
            events.wait(Duration::from_secs(2))?;
        }
    }

    /// Block on the kernel-owned completion lock; an exited supervisor wakes
    /// every waiter immediately even if it never wrote a terminal event.
    pub fn wait_job(&self, id: Uuid) -> Result<JobHandle> {
        let _requester = self.hold_as_requester(id)?;
        let initial = self.job_status(id)?;
        if matches!(
            initial.state,
            JobState::Finished { .. } | JobState::Cancelled { .. }
        ) {
            return Ok(initial);
        }
        let lock = stable_file(&self.ticket_path(id))?;
        lock.lock_shared()?;
        drop(lock);
        self.recover(false)?;
        self.job_status(id)
    }

    pub fn finish(&self, id: Uuid, exit_code: i32, evidence: &str) -> Result<()> {
        self.conclude(id, JobEnd::Exited(exit_code), evidence)
    }

    /// The one terminal transition of a job: Finished with its exit code or
    /// Cancelled with a reason. An already terminal record is left as it is.
    /// Yielded services are resumed either way.
    fn conclude(&self, id: Uuid, end: JobEnd, evidence: &str) -> Result<()> {
        let services = self.locked(|state| {
            let record = state
                .records
                .iter_mut()
                .find(|r| r.ticket.id == id)
                .context("unknown job")?;
            if matches!(
                record.state,
                TicketState::Finished | TicketState::Cancelled { .. }
            ) {
                return Ok(Vec::new());
            }
            record.finished_ms = Some(milliseconds());
            let earlier = record.evidence.take();
            record.evidence =
                Some(earlier.map_or_else(|| evidence.to_owned(), |e| format!("{e}; {evidence}")));
            match end {
                JobEnd::Exited(exit_code) => {
                    record.state = TicketState::Finished;
                    if let Some(job) = record.job.as_mut() {
                        job.state = JobState::Finished { exit_code };
                    }
                }
                JobEnd::Cancelled(reason) => {
                    record.state = TicketState::Cancelled {
                        reason: reason.clone(),
                    };
                    if let Some(job) = record.job.as_mut() {
                        job.state = JobState::Cancelled { reason };
                    }
                }
            }
            if !record.quarantined {
                record.resume_pending = record.yield_services.clone();
            }
            Ok(record.resume_pending.clone())
        })?;
        if !services.is_empty() && !cfg!(test) {
            // Recovery also invokes finish; resume is a detached control task
            // so it cannot change job exit status or hold the completion FD.
            let executable = std::env::var_os("BORG_LANE_EXECUTABLE")
                .map(PathBuf::from)
                .unwrap_or(std::env::current_exe()?);
            Command::new(executable)
                .args(["lane", "--state-dir"])
                .arg(&self.root)
                .args(["__resume_services"])
                .arg(id.to_string())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .context("starting service resume control")?;
        }
        Ok(())
    }

    /// The detached control process is idempotent and holds a per-job lock.
    /// Remaining failures are journalled so recover can retry and users can
    /// inspect them; successful resume cannot hide workload exit status.
    pub fn resume_services(&self, id: Uuid) -> Result<()> {
        let gate = stable_file(&self.root.join("locks").join(format!("resume-{id}.flock")))?;
        match gate.try_lock() {
            Ok(()) => (),
            Err(std::fs::TryLockError::WouldBlock) => return Ok(()),
            Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
        }
        let record = self.record(id)?;
        ensure!(
            matches!(record.state, TicketState::Finished),
            "cannot resume service before job finishes"
        );
        ensure!(
            !record.quarantined,
            "resource quarantined: automatic service resume refused"
        );
        let outcome = (|| -> Result<()> {
            for service in &record.resume_pending {
                #[cfg(test)]
                let result: Result<()> =
                    if service == "test" && self.root.join("fake-resume-ok").exists() {
                        Ok(())
                    } else {
                        self.complete_service_resume(service, id)
                    };
                #[cfg(not(test))]
                let result = self.complete_service_resume(service, id);
                self.locked(|state| {
                    let row = state
                        .records
                        .iter_mut()
                        .find(|r| r.ticket.id == id)
                        .context("job disappeared")?;
                    match &result {
                        Ok(()) => {
                            row.resume_pending.retain(|s| s != service);
                            row.resume_error = None;
                        }
                        Err(error) => {
                            row.resume_error =
                                Some(format!("service {service} resume pending: {error:#}"));
                        }
                    }
                    Ok(())
                })?;
                result?;
            }
            Ok(())
        })();
        self.locked(|state| {
            if let Some(row) = state.records.iter_mut().find(|r| r.ticket.id == id) {
                row.resume_attempts = row.resume_attempts.saturating_add(1);
            }
            Ok(())
        })?;
        outcome
    }

    /// A Resume RPC only removes the yield token; startup and readiness are
    /// asynchronous and can be slow (a real editor takes about a minute).
    /// Keep journal recovery pending until the backend is actually Healthy,
    /// and never send Resume twice after its token is gone. Wait on the
    /// service's status events within its resume budget; a missed readiness
    /// window, Failed or a stopped supervisor ends the wait early.
    fn complete_service_resume(&self, service: &str, id: Uuid) -> Result<()> {
        use crate::services::ServiceState;
        let status = self.service_status(service)?;
        if status.yields.contains_key(&id.to_string()) {
            self.service_control(service, id, "resume", 0)?;
        }
        let services = self.root.join("services");
        let budget = crate::services::resume_budget(&services, service);
        let events = StateEvents::new(&services.join(service)).ok();
        let started = Instant::now();
        let mut backoff = Duration::from_millis(250);
        loop {
            let status = self.service_status(service)?;
            if status.yields.is_empty() && matches!(status.state, ServiceState::Healthy { .. }) {
                return Ok(());
            }
            let failed = match &status.state {
                ServiceState::Stopped | ServiceState::Failed { .. } => true,
                ServiceState::Degraded { reason } => reason == crate::services::READINESS_FAILED,
                _ => false,
            };
            let elapsed = started.elapsed();
            if failed || elapsed >= budget {
                anyhow::bail!(
                    "service {service} not healthy after resume ({} of {} s budget): {:?}: {}",
                    elapsed.as_secs(),
                    budget.as_secs(),
                    status.state,
                    status.reason
                );
            }
            // Events wake this at once; the capped backoff bounds a missed one.
            let wait = backoff.min(budget - elapsed);
            match &events {
                Some(events) => events.wait(wait)?,
                None => std::thread::sleep(wait),
            }
            backoff = (backoff * 2).min(Duration::from_secs(2));
        }
    }

    fn service_status(&self, service_id: &str) -> Result<crate::services::ServiceStatus> {
        #[cfg(test)]
        if service_id == "test" && self.root.join("fake-service-status.json").exists() {
            return Ok(serde_json::from_slice(&fs::read(
                self.root.join("fake-service-status.json"),
            )?)?);
        }
        let executable = std::env::var_os("BORG_LANE_EXECUTABLE")
            .map(PathBuf::from)
            .unwrap_or(std::env::current_exe()?);
        let output = Command::new(executable)
            .args(["lane", "--json", "--state-dir"])
            .arg(&self.root)
            .args(["service", "status", service_id])
            .output()
            .with_context(|| format!("reading service {service_id} status"))?;
        ensure!(
            output.status.success(),
            "service {service_id} status failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&output.stdout)?)
    }

    /// Cancel a queued ticket at once. A job past Queued (Preparing or
    /// running) gets a cancel request that its supervisor carries out: it
    /// kills the workload, runs the post hook and ends the job Cancelled.
    pub fn cancel_ticket(&self, id: Uuid, reason: &str) -> Result<()> {
        self.locked(|state| {
            let record = state
                .records
                .iter_mut()
                .find(|r| r.ticket.id == id)
                .context("unknown ticket")?;
            match record.state {
                TicketState::Queued => {
                    record.state = TicketState::Cancelled {
                        reason: reason.to_owned(),
                    };
                    if let Some(job) = record.job.as_mut() {
                        job.state = JobState::Cancelled {
                            reason: reason.to_owned(),
                        };
                    }
                    record.finished_ms = Some(milliseconds());
                }
                TicketState::Preparing | TicketState::Granted(_) if record.job.is_some() => {
                    record
                        .cancel_requested
                        .get_or_insert_with(|| reason.to_owned());
                }
                _ => bail!("cannot cancel a completed ticket or a service lease"),
            }
            Ok(())
        })
    }
}

/// The cancel reason when no requester is left (`abandon_after_ms`).
const ABANDONED: &str = "abandoned: no requester";

/// How a supervised job ended.
enum JobEnd {
    Exited(i32),
    Cancelled(String),
}

/// Where one finished job's service resume stands after `recover --wait`.
#[derive(Clone, Debug, Serialize)]
pub struct ResumeOutcome {
    pub job_id: Uuid,
    /// Services still waiting to resume; empty once resumed.
    pub pending: Vec<String>,
    /// `resumed`, `failed` (a controller attempt ended in an error) or
    /// `pending` (no attempt finished before the wait timed out).
    pub outcome: &'static str,
    pub error: Option<String>,
}

fn resume_is_pending(row: &LaneRecord) -> bool {
    matches!(row.state, TicketState::Finished) && !row.resume_pending.is_empty() && !row.quarantined
}

fn capacity(capacities: &[Capacity], key: &ResourceKey) -> u32 {
    capacities
        .iter()
        .find(|c| &c.key == key)
        .map_or(1, |c| c.slots)
}

fn conflicts(left: &ResourceRequest, right: &ResourceRequest) -> bool {
    left.key == right.key
}

/// Pure FIFO decision: an earlier conflicting ticket is never overtaken,
/// while disjoint keys can run concurrently. All keys grant atomically.
fn dispatch_reason(state: &Journal, id: Uuid) -> Result<Option<String>> {
    let me = state
        .records
        .iter()
        .find(|r| r.ticket.id == id)
        .context("unknown ticket")?;
    if !matches!(me.state, TicketState::Queued) {
        bail!("ticket is not queued");
    }
    for quarantined in state.records.iter().filter(|r| r.quarantined) {
        if quarantined
            .request
            .resources
            .iter()
            .any(|a| me.request.resources.iter().any(|b| conflicts(a, b)))
        {
            return Ok(Some(format!(
                "resource quarantined after unverified orphan {}",
                quarantined.ticket.id
            )));
        }
    }
    for earlier in state
        .records
        .iter()
        .filter(|r| r.ticket.sequence < me.ticket.sequence)
    {
        if matches!(earlier.state, TicketState::Queued | TicketState::Preparing)
            && earlier
                .request
                .resources
                .iter()
                .any(|a| me.request.resources.iter().any(|b| conflicts(a, b)))
        {
            return Ok(Some(format!("FIFO ticket {} ahead", earlier.ticket.id)));
        }
    }
    for requested in &me.request.resources {
        let used = state
            .records
            .iter()
            .filter(|r| matches!(r.state, TicketState::Granted(_) | TicketState::Preparing))
            .flat_map(|r| &r.request.resources)
            .filter(|r| r.key == requested.key)
            .fold(0_u32, |n, r| {
                n.saturating_add(match r.access {
                    Access::Exclusive => capacity(&state.capacities, &r.key),
                    Access::Shared { slots } => slots,
                })
            });
        let preparable_service_holder = me.spec.is_some()
            && matches!(requested.access, Access::Exclusive)
            && state
                .records
                .iter()
                .filter(|r| matches!(r.state, TicketState::Granted(_)))
                .filter(|r| r.request.resources.iter().any(|r| r.key == requested.key))
                .all(|r| r.service_lease);
        if preparable_service_holder {
            continue;
        }
        if used > 0 && matches!(requested.access, Access::Exclusive) {
            return Ok(Some(format!(
                "exclusive resource {} busy",
                requested.key.name
            )));
        }
        if used.saturating_add(match requested.access {
            Access::Shared { slots } => slots,
            Access::Exclusive => capacity(&state.capacities, &requested.key),
        }) > capacity(&state.capacities, &requested.key)
        {
            return Ok(Some(format!(
                "resource {} capacity {}/{}",
                requested.key.name,
                used,
                capacity(&state.capacities, &requested.key)
            )));
        }
    }
    Ok(None)
}

fn grant_entry(entry: &mut LaneRecord) -> Lease {
    let lease = Lease {
        id: Uuid::new_v4(),
        generation: entry.ticket.sequence,
        ticket: entry.ticket.clone(),
        resources: entry.request.resources.clone(),
        holder: entry.request.holder.clone(),
    };
    entry.state = TicketState::Granted(lease.clone());
    entry.started_ms = Some(milliseconds());
    entry.wait_reason = None;
    if let Some(spec) = entry.spec.as_ref() {
        let available = mem_available().unwrap_or(0);
        let free = available.saturating_sub(spec.admission.reserve_ram_bytes);
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        entry.parallelism_hint =
            Some(((free / (1024 * 1024 * 1024)) as usize).clamp(1, cores) as u32);
    }
    if let Some(job) = entry.job.as_mut() {
        job.state = JobState::Running {
            scope: String::new(),
        };
    }
    lease
}

fn mem_available() -> Option<u64> {
    fs::read_to_string("/proc/meminfo")
        .ok()?
        .lines()
        .find(|s| s.starts_with("MemAvailable:"))?
        .split_whitespace()
        .nth(1)?
        .parse::<u64>()
        .ok()
        .map(|kb| kb * 1024)
}

fn same_filesystem(left: &Path, right: &Path) -> Result<bool> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let a = fs::metadata(left)
            .with_context(|| format!("inspect reserved disk {}", left.display()))?;
        let b = fs::metadata(right)
            .with_context(|| format!("inspect requested disk {}", right.display()))?;
        Ok(a.dev() == b.dev())
    }
    #[cfg(not(unix))]
    {
        Ok(left == right)
    }
}

fn budget_reason(state: &Journal, budget: &AdmissionBudget) -> Result<Option<String>> {
    let reserved_ram: u64 = state
        .records
        .iter()
        .filter(|r| matches!(r.state, TicketState::Granted(_) | TicketState::Preparing))
        .filter_map(|r| {
            r.spec
                .as_ref()
                .map(|s| &s.admission)
                .or(r.service_admission.as_ref())
        })
        .map(|b| b.reserve_ram_bytes)
        .fold(0_u64, u64::saturating_add);
    let reserved_disk: u64 = state
        .records
        .iter()
        .filter(|r| matches!(r.state, TicketState::Granted(_) | TicketState::Preparing))
        .filter_map(|r| {
            r.spec
                .as_ref()
                .map(|s| &s.admission)
                .or(r.service_admission.as_ref())
        })
        .try_fold(0_u64, |used, other| {
            Ok::<u64, anyhow::Error>(used.saturating_add(
                if same_filesystem(&other.disk_path, &budget.disk_path)? {
                    other.reserve_disk_bytes
                } else {
                    0
                },
            ))
        })?;
    match crate::workspace::assess_budget(budget, reserved_ram, reserved_disk) {
        Ok(admission) => Ok((!admission.admitted).then_some(admission.reason)),
        Err(error) => Ok(Some(format!("budget inspection unavailable: {error:#}"))),
    }
}

fn proc_start_ticks(pid: u32) -> Option<u64> {
    let text = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    text.rsplit_once(") ")?
        .1
        .split_whitespace()
        .nth(19)?
        .parse()
        .ok()
}

fn systemd_available() -> bool {
    Command::new("systemd-run")
        .args(["--user", "--scope", "--quiet", "--collect", "true"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// The cgroup of `pid` when it is the named systemd scope.
fn process_scope(pid: u32, unit: &str) -> Option<String> {
    let text = fs::read_to_string(format!("/proc/{pid}/cgroup")).ok()?;
    let path = text.lines().find_map(|line| line.strip_prefix("0::"))?;
    path.ends_with(&format!("/{unit}")).then(|| path.to_owned())
}

/// Processes in a cgroup v2 group (empty once the group is gone).
fn cgroup_pids(path: &str) -> Vec<u32> {
    fs::read_to_string(
        Path::new("/sys/fs/cgroup")
            .join(path.trim_start_matches('/'))
            .join("cgroup.procs"),
    )
    .unwrap_or_default()
    .lines()
    .filter_map(|line| line.trim().parse().ok())
    .collect()
}

/// Live processes whose process group is `group`.
fn process_group_pids(group: u32) -> Vec<u32> {
    let Ok(entries) = fs::read_dir("/proc") else {
        return Vec::new();
    };
    entries
        .filter_map(|entry| entry.ok()?.file_name().to_str()?.parse::<u32>().ok())
        .filter(|pid| {
            fs::read_to_string(format!("/proc/{pid}/stat"))
                .ok()
                .and_then(|stat| {
                    // Fields after the parenthesised command: state ppid pgrp ...
                    let mut fields = stat[stat.rfind(')')? + 2..].split_whitespace();
                    let state = fields.next()?;
                    let pgrp = fields.nth(1)?.parse::<u32>().ok()?;
                    // A zombie is already dead; only its parent can reap it.
                    (state != "Z").then_some(pgrp)
                })
                == Some(group)
        })
        .collect()
}

/// Workloads and supervisors run in systemd user scopes unless the caller
/// opts out with `BORG_LANE_SCOPE=0` or no user manager exists.
fn workload_scoped() -> bool {
    std::env::var_os("BORG_LANE_SCOPE").as_deref() != Some(std::ffi::OsStr::new("0"))
        && systemd_available()
}

fn scope_control_group(unit: &str) -> Option<String> {
    let output = Command::new("systemctl")
        .args(["--user", "show", unit, "-p", "ControlGroup", "--value"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let path = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    if path.is_empty() { None } else { Some(path) }
}

fn proc_cpu(pid: u32) -> Option<f64> {
    let text = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let tail = text
        .rsplit_once(") ")?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    let ticks: u64 = tail
        .get(11)?
        .parse::<u64>()
        .ok()?
        .saturating_add(tail.get(12)?.parse().ok()?);
    Some(ticks as f64 / unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.max(1) as f64)
}

impl LaneStore {
    /// Dequeued admission is journalled while the metadata lock is held, so
    /// even two independent supervisors on different worktrees cannot race.
    fn try_grant(&self, id: Uuid) -> Result<Option<Lease>> {
        self.locked(|state| {
            let reason = dispatch_reason(state, id)?;
            let reason = if reason.is_some() {
                reason
            } else {
                match state
                    .records
                    .iter()
                    .find(|r| r.ticket.id == id)
                    .and_then(|r| r.spec.as_ref())
                {
                    Some(spec) => budget_reason(state, &spec.admission).unwrap_or_else(|error| {
                        Some(format!("budget inspection unavailable: {error:#}"))
                    }),
                    None => None,
                }
            };
            let request = state
                .records
                .iter()
                .find(|r| r.ticket.id == id)
                .context("ticket disappeared")?
                .request
                .resources
                .clone();
            let mut services = state
                .records
                .iter()
                .filter(|r| r.service_lease && matches!(r.state, TicketState::Granted(_)))
                .filter(|r| {
                    r.request.resources.iter().any(|resource| {
                        request.iter().any(|requested| {
                            matches!(requested.access, Access::Exclusive)
                                && requested.key == resource.key
                        })
                    })
                })
                .filter_map(|r| {
                    r.request
                        .holder
                        .purpose
                        .strip_prefix("service:")
                        .map(str::to_owned)
                })
                .collect::<Vec<_>>();
            services.sort();
            services.dedup();
            let entry = state
                .records
                .iter_mut()
                .find(|r| r.ticket.id == id)
                .context("ticket disappeared")?;
            if let Some(reason) = reason {
                entry.wait_reason = Some(reason);
                return Ok(None);
            }
            if (entry
                .spec
                .as_ref()
                .is_some_and(|spec| spec.pre_hook.is_some())
                || !services.is_empty())
                && entry
                    .request
                    .resources
                    .iter()
                    .any(|r| matches!(r.access, Access::Exclusive))
            {
                entry.yield_services = services;
                entry.state = TicketState::Preparing;
                entry.preparing_since_ms = Some(milliseconds());
                entry.wait_reason = Some("waiting for synchronous pre-exclusive yield".to_owned());
                return Ok(None);
            }
            Ok(Some(grant_entry(entry)))
        })
    }

    /// Called only while holding state.lock. Service client mutation publishes
    /// this file under the same lock, closing late-grant/read races.
    fn foreign_client_wait(&self, state: &Journal, id: Uuid) -> Result<Option<String>> {
        let row = state
            .records
            .iter()
            .find(|row| row.ticket.id == id)
            .context("preparing job disappeared")?;
        let spec = row.spec.as_ref().context("preparing ticket has no job")?;
        let now = milliseconds();
        let since = row.preparing_since_ms.unwrap_or(now);
        for service_id in &row.yield_services {
            // Already stopped/yielded services cannot have a running editor
            // holding new client settings; avoid waiting on stale status.
            let tag = format!("service:{service_id}");
            let Some(bound) = state.records.iter().find(|holder| {
                holder.service_lease
                    && holder.request.holder.purpose == tag
                    && matches!(holder.state, TicketState::Granted(_))
            }) else {
                continue;
            };
            // Yielding a service bound to several exclusive keys affects all
            // of them. Honor the longest relevant grace (zero is indefinite).
            let mut grace_ms: Option<u64> = None;
            for bound_resource in &bound.request.resources {
                if !spec.lease.resources.iter().any(|request| {
                    request.key == bound_resource.key && matches!(request.access, Access::Exclusive)
                }) {
                    continue;
                }
                let grace = spec
                    .foreign_client_grace_by_resource
                    .iter()
                    .find(|override_| override_.resource == bound_resource.key)
                    .map(|override_| override_.grace_ms)
                    .unwrap_or(spec.foreign_client_grace_ms);
                grace_ms = Some(match grace_ms {
                    Some(0) => 0,
                    Some(_) if grace == 0 => 0,
                    Some(previous) => previous.max(grace),
                    None => grace,
                });
            }
            let Some(grace_ms) = grace_ms else { continue };
            if grace_ms != 0 && now.saturating_sub(since) >= grace_ms {
                continue;
            }
            ensure!(
                service_id
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'),
                "invalid bound service id"
            );
            let path = self
                .root
                .join("services")
                .join(service_id)
                .join("state.json");
            ensure!(
                fs::metadata(&path)?.len() <= 1_048_576,
                "service status too large"
            );
            let status: crate::services::ServiceStatus = serde_json::from_slice(&fs::read(path)?)?;
            ensure!(status.id == *service_id, "service status identity mismatch");
            for client in &status.clients {
                if client.expires_at_unix_ms > now
                    && (client.owner.participant_id != row.request.holder.participant_id
                        || client.owner.session_id != row.request.holder.session_id)
                {
                    let grace = if grace_ms == 0 {
                        "indefinite".to_owned()
                    } else {
                        since.saturating_add(grace_ms).to_string()
                    };
                    return Ok(Some(format!(
                        "foreign client lease: service {service_id} holder {}/{} until {} or grace {grace}",
                        client.owner.participant_id,
                        client.owner.session_id,
                        client.expires_at_unix_ms
                    )));
                }
            }
        }
        Ok(None)
    }

    /// `last_requester_ms` carries when a `job wait` was last seen into the
    /// running phase, so abandonment counts from then, not from the grant.
    fn wait_grant(&self, id: Uuid, last_requester_ms: &mut u64) -> Result<Lease> {
        let entered = std::time::Instant::now();
        let event = StateEvents::new(&self.root)?;
        loop {
            self.recover(false)?;
            let current = self.record(id)?;
            if let Some(spec) = &current.spec
                && current.cancel_requested.is_none()
                && self.abandoned(spec, id, last_requester_ms)
            {
                self.cancel_ticket(id, ABANDONED)?;
                bail!("{ABANDONED}");
            }
            if let Some(reason) = &current.cancel_requested {
                bail!("cancel requested: {reason}");
            }
            if let TicketState::Cancelled { reason } = &current.state {
                bail!("cancelled before grant: {reason}");
            }
            if !matches!(current.state, TicketState::Preparing)
                && let Some(lease) = self.try_grant(id)?
            {
                return Ok(lease);
            }
            let record = self.record(id)?;
            if matches!(record.state, TicketState::Preparing) {
                let waiting = self.locked(|state| {
                    let reason = self.foreign_client_wait(state, id).unwrap_or_else(|error| {
                        Some(format!("service client state unavailable: {error:#}"))
                    });
                    let row = state
                        .records
                        .iter_mut()
                        .find(|row| row.ticket.id == id)
                        .context("preparing job disappeared")?;
                    row.wait_reason = reason.clone();
                    Ok(reason)
                })?;
                if waiting.is_some() {
                    if let Some(limit) = record.request.queue_timeout_ms
                        && entered.elapsed().as_millis() >= u128::from(limit)
                    {
                        self.cancel_ticket(id, "queue timeout waiting for foreign client lease")?;
                        bail!("queue timeout for ticket {id}");
                    }
                    event.wait(Duration::from_secs(2))?;
                    continue;
                }
                let spec = record.spec.context("preparing ticket has no job")?;
                // All active service bindings are journalled as shared leases.
                // Request EVERY bound service to yield, regardless of optional
                // adapter hooks; no backend may restart past Preparing/Granted.
                for service_id in &record.yield_services {
                    self.service_control(service_id, id, "yield", spec.timeout_ms)?;
                }
                if let Some(hook) = &spec.pre_hook
                    && let Err(error) = self.run_hook(hook, &spec, id, "pre-exclusive", true)
                {
                    // Yield may have partially succeeded. Resume the service
                    // through the post hook even though no lease was granted.
                    if let Some(post) = &spec.post_hook {
                        let _ = self.run_hook(post, &spec, id, "post-exclusive", false);
                    }
                    return Err(error);
                }
                // The hook requesting yield is not proof of release. Recheck
                // under the SAME journal lock used for service startup.
                let granted = self.locked(|state| {
                    let mut check = state.clone();
                    let pending = check
                        .records
                        .iter_mut()
                        .find(|r| r.ticket.id == id)
                        .context("ticket disappeared")?;
                    ensure!(
                        matches!(pending.state, TicketState::Preparing),
                        "ticket lost pre-grant reservation"
                    );
                    pending.state = TicketState::Queued;
                    // Disable the service-holder exception: actual release is
                    // required before the exclusive claim is granted.
                    pending.spec = None;
                    ensure!(
                        dispatch_reason(&check, id)?.is_none(),
                        "service did not release resource before exclusive grant"
                    );
                    let entry = state
                        .records
                        .iter_mut()
                        .find(|r| r.ticket.id == id)
                        .context("ticket disappeared")?;
                    Ok(grant_entry(entry))
                });
                if granted.is_err()
                    && let Some(post) = &spec.post_hook
                {
                    let _ = self.run_hook(post, &spec, id, "post-exclusive", false);
                }
                return granted;
            }
            if let Some(limit) = record.request.queue_timeout_ms
                && entered.elapsed().as_millis() >= u128::from(limit)
            {
                self.cancel_ticket(id, "queue timeout")?;
                bail!("queue timeout for ticket {id}");
            }
            // Snapshot followed by a subscribed notification (armed before
            // the snapshot) cannot miss an already-committed transition.
            event.wait(Duration::from_secs(2))?;
        }
    }

    /// The service CLI sends an authenticated local supervisor request and
    /// returns only after the backend stopped, its proxy went 503, and its
    /// shared lease was released. Never interpret a hook's exit as this ack.
    fn service_control(
        &self,
        service_id: &str,
        id: Uuid,
        verb: &str,
        timeout_ms: u64,
    ) -> Result<()> {
        #[cfg(test)]
        if service_id == "test" {
            return Ok(());
        }
        let executable = std::env::var_os("BORG_LANE_EXECUTABLE")
            .map(PathBuf::from)
            .unwrap_or(std::env::current_exe()?);
        let seconds = (timeout_ms / 1000).saturating_add(600).clamp(600, 86_400);
        let mut cmd = Command::new(executable);
        cmd.args(["lane", "--json", "--state-dir"])
            .arg(&self.root)
            .args(["service", verb, service_id, "--by"])
            .arg(id.to_string());
        if verb == "yield" {
            cmd.args(["--for-seconds", &seconds.to_string()]);
        }
        let output = cmd
            .output()
            .with_context(|| format!("requesting {verb} from service {service_id}"))?;
        ensure!(
            output.status.success(),
            "service {service_id} {verb} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        if verb == "yield" {
            let status: crate::services::ServiceStatus = serde_json::from_slice(&output.stdout)?;
            ensure!(
                matches!(status.state, crate::services::ServiceState::Yielded),
                "service {service_id} did not acknowledge a stopped backend"
            );
        }
        Ok(())
    }

    fn run_hook(
        &self,
        hook: &Hook,
        spec: &JobSpec,
        id: Uuid,
        phase: &str,
        sync: bool,
    ) -> Result<()> {
        ensure!(!hook.argv.is_empty(), "empty {phase} hook");
        let mut command = Command::new(&hook.argv[0]);
        command
            .args(&hook.argv[1..])
            .current_dir(&spec.cwd)
            .env("BORG_LANE_JOB", id.to_string())
            .env("BORG_LANE_PHASE", phase)
            .env(
                "BORG_LANE_RESOURCE",
                spec.lease.resources.first().map_or("", |r| &r.key.name),
            )
            .env("BORG_LANE_LOG", self.job_dir(id).join("output.log"))
            .env("BORG_LANES_ROOT", &self.root);
        let scoped_post = phase.starts_with("post") && systemd_available();
        if phase.starts_with("post") && !scoped_post {
            ensure!(
                std::env::var("BORG_LANE_DEGRADED").as_deref() == Ok("1"),
                "post hook requires a systemd user scope outside explicit degraded mode"
            );
        }
        if !sync || scoped_post {
            let unit = format!("borg-lane-hook-{id}-{phase}.scope");
            self.locked(|state| {
                let entry = state
                    .records
                    .iter_mut()
                    .find(|r| r.ticket.id == id)
                    .context("post hook job missing")?;
                entry.post_scope = Some(unit.clone());
                Ok(())
            })?;
            if !systemd_available() {
                bail!("asynchronous post hook requires a systemd user scope");
            }
            let mut scoped = Command::new("systemd-run");
            scoped
                .args([
                    "--user",
                    "--scope",
                    "--quiet",
                    "--collect",
                    "--expand-environment=no",
                ])
                .arg(format!("--unit={unit}"))
                .args(["-p", "MemoryMax=2G", "--"])
                .arg(&hook.argv[0])
                .args(&hook.argv[1..]);
            command = scoped;
            command
                .current_dir(&spec.cwd)
                .env("BORG_LANES_ROOT", &self.root)
                .env("BORG_LANE_JOB", id.to_string())
                .env("BORG_LANE_PHASE", phase)
                .env(
                    "BORG_LANE_RESOURCE",
                    spec.lease.resources.first().map_or("", |r| &r.key.name),
                )
                .env("BORG_LANE_LOG", self.job_dir(id).join("output.log"));
        }
        let mut child = command.stdin(Stdio::null()).spawn()?;
        if !sync {
            return Ok(());
        }
        let start = std::time::Instant::now();
        loop {
            if let Some(status) = child.try_wait()? {
                ensure!(status.success(), "{phase} hook exited with {status}");
                return Ok(());
            }
            if start.elapsed() > Duration::from_millis(hook.timeout_ms) {
                let _ = child.kill();
                let _ = child.wait();
                bail!("{phase} hook timed out after {}ms", hook.timeout_ms);
            }
            std::thread::sleep(Duration::from_millis(30));
        }
    }

    /// Called only from `borg lane __supervise` with the inherited done-lock FD.
    /// Workload FDs use CLOEXEC: neither a compiler nor a hook can hold the
    /// completion lock after its supervisor has died.
    pub fn supervise(&self, id: Uuid) -> Result<i32> {
        use std::os::fd::FromRawFd;
        let lock = if let Ok(fd) = std::env::var("BORG_LANE_LOCK_FD") {
            let fd: i32 = fd.parse()?;
            let inherited = unsafe { File::from_raw_fd(fd) };
            unsafe {
                libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
            }
            inherited
        } else {
            let lock = stable_file(&self.ticket_path(id))?;
            lock.lock()?;
            lock
        };
        self.locked(|state| {
            let record = state
                .records
                .iter_mut()
                .find(|r| r.ticket.id == id)
                .context("unknown supervisor job")?;
            record.supervisor_pid = Some(std::process::id());
            Ok(())
        })?;
        let result = self.supervise_job(id);
        let requested = self.record(id)?.cancel_requested;
        let (end, evidence) = match (result, requested) {
            (Ok(JobEnd::Exited(code)), _) => (JobEnd::Exited(code), "finished".to_owned()),
            (Ok(JobEnd::Cancelled(reason)), _) => {
                let evidence = format!("cancelled: {reason}");
                (JobEnd::Cancelled(reason), evidence)
            }
            (Err(error), Some(reason)) => (
                JobEnd::Cancelled(reason),
                format!("cancelled before start: {error:#}"),
            ),
            (Err(error), None) => (JobEnd::Exited(125), format!("supervisor: {error:#}")),
        };
        let code = match end {
            JobEnd::Exited(code) => code,
            JobEnd::Cancelled(_) => 125,
        };
        self.conclude(id, end, &evidence)?;
        drop(lock);
        Ok(code)
    }

    fn supervise_job(&self, id: Uuid) -> Result<JobEnd> {
        let scoped = workload_scoped();
        ensure!(
            scoped || std::env::var("BORG_LANE_DEGRADED").as_deref() == Ok("1"),
            "lane requires a systemd user manager (set BORG_LANE_DEGRADED=1 only for explicit unscoped testing)"
        );
        let mut last_requester_ms = self.record(id)?.created_ms;
        let lease = self.wait_grant(id, &mut last_requester_ms)?;
        let spec = self.record(id)?.spec.context("job spec missing")?;
        let exclusive = spec
            .lease
            .resources
            .iter()
            .any(|r| matches!(r.access, Access::Exclusive));
        if let Some(pre) = &spec.pre_hook
            && !exclusive
        {
            // Exclusive hooks already ran before the grant as a FIFO barrier.
            self.run_hook(pre, &spec, id, "pre", true)?;
        }
        if let Some(reason) = self.record(id)?.cancel_requested {
            self.run_post_for_job(&spec, id, exclusive)?;
            return Ok(JobEnd::Cancelled(reason));
        }
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.job_dir(id).join("output.log"))?;
        let stderr = log.try_clone()?;
        let unit = format!("borg-lane-{id}.scope");
        let mut command = if scoped {
            let mut command = Command::new("systemd-run");
            command
                .args([
                    "--user",
                    "--scope",
                    "--quiet",
                    "--collect",
                    "--expand-environment=no",
                ])
                .arg(format!("--unit={unit}"));
            if let Some(max) = spec.memory_max_bytes {
                command.args(["-p", &format!("MemoryMax={max}")]);
            }
            command.arg("--").arg(&spec.argv[0]).args(&spec.argv[1..]);
            command
        } else {
            let mut command = Command::new(&spec.argv[0]);
            command.args(&spec.argv[1..]);
            #[cfg(unix)]
            {
                use std::os::unix::process::CommandExt;
                command.process_group(0);
            }
            command
        };
        command
            .current_dir(&spec.cwd)
            .envs(spec.env.clone())
            .env("BORG_LANE_JOB", id.to_string())
            .env("BORG_LANES_ROOT", &self.root)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(stderr));
        let mut child = command
            .spawn()
            .with_context(|| format!("starting job {id}"))?;
        // `systemd-run` joins the scope before it execs the workload, so the
        // workload's own /proc entry names the scope without a manager call.
        let mut group = None;
        if scoped {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while group.is_none()
                && child.try_wait()?.is_none()
                && std::time::Instant::now() < deadline
            {
                group = process_scope(child.id(), &unit);
                if group.is_none() {
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
            group = group.or_else(|| scope_control_group(&unit));
        }
        self.locked(|state| {
            let record = state
                .records
                .iter_mut()
                .find(|r| r.ticket.id == id)
                .context("job vanished")?;
            record.workload_pid = Some(child.id());
            record.workload_start_ticks = proc_start_ticks(child.id());
            if scoped {
                record.scope_cgroup = group;
            }
            if let Some(job) = record.job.as_mut() {
                job.state = JobState::Running {
                    scope: if scoped { unit.clone() } else { String::new() },
                };
            }
            Ok(())
        })?;
        let started = std::time::Instant::now();
        let mut last_progress = started;
        let mut last_size = 0;
        let mut last_cpu = 0.0;
        let leader = child.id();
        let end = loop {
            if let Some(status) = child.try_wait()? {
                break JobEnd::Exited(status.code().unwrap_or(128 + status.signal().unwrap_or(9)));
            }
            let size = fs::metadata(self.job_dir(id).join("output.log")).map_or(0, |m| m.len());
            let cpu = proc_cpu(leader).unwrap_or(0.0);
            if size != last_size || cpu > last_cpu + 0.05 {
                last_progress = std::time::Instant::now();
            }
            last_size = size;
            last_cpu = cpu;
            if self.abandoned(&spec, id, &mut last_requester_ms) {
                self.cancel_ticket(id, ABANDONED)?;
            }
            let cancel = self.locked(|state| {
                let record = state
                    .records
                    .iter_mut()
                    .find(|r| r.ticket.id == id)
                    .context("job vanished")?;
                record.progress = Some(format!("{} bytes", size));
                record.cpu_seconds = Some(cpu);
                Ok(record.cancel_requested.clone())
            })?;
            let timed_out = started.elapsed() > Duration::from_millis(spec.timeout_ms);
            let stalled = spec
                .stall_timeout_ms
                .is_some_and(|ms| last_progress.elapsed() > Duration::from_millis(ms));
            if cancel.is_some() || timed_out || stalled {
                let reason = match &cancel {
                    Some(reason) => format!("cancelled: {reason}"),
                    None if timed_out => "timeout".to_owned(),
                    None => "stall: no CPU or log growth".to_owned(),
                };
                self.log_recovery(id, &reason)?;
                self.kill_workload(id, &unit, scoped, leader, "killed pids")?;
                let _ = child.kill();
                let _ = child.wait();
                break match cancel {
                    Some(reason) => JobEnd::Cancelled(reason),
                    None if timed_out => JobEnd::Exited(124),
                    None => JobEnd::Exited(125),
                };
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        // The leader is gone; no process of this workload may outlive it into
        // the next holder's lease (a compiler it started, for example).
        self.kill_workload(id, &unit, scoped, leader, "killed leftover pids")?;
        self.run_post_for_job(&spec, id, exclusive)?;
        let _ = lease;
        Ok(end)
    }

    /// SIGKILL every process left in the job's scope (degraded: its own
    /// process group, whose ID the kernel will not reuse while a member
    /// lives) and wait until none remain. The PIDs go to the evidence and
    /// recovery log; a group that will not empty quarantines the resource
    /// rather than release it to the next job.
    fn kill_workload(
        &self,
        id: Uuid,
        unit: &str,
        scoped: bool,
        group: u32,
        label: &str,
    ) -> Result<Vec<u32>> {
        let cgroup = scoped
            .then(|| {
                self.record(id)
                    .ok()
                    .and_then(|r| r.scope_cgroup)
                    .or_else(|| scope_control_group(unit))
            })
            .flatten();
        let members = || match &cgroup {
            Some(path) => cgroup_pids(path),
            None if scoped => Vec::new(),
            None => process_group_pids(group),
        };
        let killed = members();
        if killed.is_empty() {
            return Ok(killed);
        }
        if scoped {
            let _ = Command::new("systemctl")
                .args(["--user", "kill", "--signal=SIGKILL", unit])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        } else {
            unsafe {
                libc::kill(-(group as i32), libc::SIGKILL);
            }
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut left = members();
        while !left.is_empty() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
            left = members();
        }
        let note = if left.is_empty() {
            format!("{label} {killed:?}")
        } else {
            format!("{label} {killed:?}; still running {left:?}, resource quarantined")
        };
        self.locked(|state| {
            let row = state
                .records
                .iter_mut()
                .find(|r| r.ticket.id == id)
                .context("job vanished")?;
            row.evidence = Some(match row.evidence.take() {
                Some(earlier) => format!("{earlier}; {note}"),
                None => note.clone(),
            });
            if !left.is_empty() {
                row.quarantined = true;
            }
            Ok(())
        })?;
        self.log_killed(id, &note, &killed)?;
        Ok(killed)
    }

    fn run_post_for_job(&self, spec: &JobSpec, id: Uuid, exclusive: bool) -> Result<()> {
        // Bound services must remain stopped until the post hook completes.
        // Unbound jobs keep their independently scoped asynchronous hook.
        if let Some(post) = &spec.post_hook {
            let bound = !self.record(id)?.yield_services.is_empty();
            if let Err(error) = self.run_hook(
                post,
                spec,
                id,
                if exclusive { "post-exclusive" } else { "post" },
                bound,
            ) {
                self.locked(|state| {
                    let row = state
                        .records
                        .iter_mut()
                        .find(|r| r.ticket.id == id)
                        .context("job vanished")?;
                    row.evidence = Some(format!("post hook failed: {error:#}"));
                    if bound {
                        row.quarantined = true;
                    }
                    Ok(())
                })?;
            }
        }
        Ok(())
    }

    fn log_killed(&self, id: Uuid, reason: &str, killed: &[u32]) -> Result<()> {
        let record = serde_json::json!({"time_ms": milliseconds(), "job": id, "reason": reason,
            "killed_pids": killed});
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(self.root.join("recovery.jsonl"))?;
        writeln!(file, "{record}")?;
        Ok(())
    }

    fn log_recovery(&self, id: Uuid, reason: &str) -> Result<()> {
        let record = serde_json::json!({"time_ms": milliseconds(), "job": id, "reason": reason,
            "evidence": self.record(id)?});
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(self.root.join("recovery.jsonl"))?;
        writeln!(file, "{record}")?;
        Ok(())
    }

    /// `recover`, then block on journal events until every service resume it
    /// started has cleared or finished a failed attempt, or `timeout` passes.
    pub fn recover_wait(&self, timeout: Duration) -> Result<(Vec<String>, Vec<ResumeOutcome>)> {
        let events = StateEvents::new(&self.root)?;
        let tracked: Vec<(Uuid, u64)> = self
            .snapshot()?
            .iter()
            .filter(|row| resume_is_pending(row))
            .map(|row| (row.ticket.id, row.resume_attempts))
            .collect();
        let actions = self.recover(false)?;
        let deadline = Instant::now() + timeout;
        loop {
            let rows = self.snapshot()?;
            let outcomes: Vec<ResumeOutcome> = tracked
                .iter()
                .filter_map(|(id, attempts)| {
                    let row = rows.iter().find(|row| row.ticket.id == *id)?;
                    let outcome = if row.resume_pending.is_empty() {
                        "resumed"
                    } else if row.resume_attempts > *attempts {
                        "failed"
                    } else {
                        "pending"
                    };
                    Some(ResumeOutcome {
                        job_id: *id,
                        pending: row.resume_pending.clone(),
                        outcome,
                        error: row.resume_error.clone().filter(|_| outcome != "resumed"),
                    })
                })
                .collect();
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || outcomes.iter().all(|o| o.outcome != "pending") {
                return Ok((actions, outcomes));
            }
            events.wait(remaining.min(Duration::from_secs(2)))?;
        }
    }

    pub fn recover(&self, dry_run: bool) -> Result<Vec<String>> {
        let mut actions = Vec::new();
        for row in self.snapshot()?.into_iter().filter(resume_is_pending) {
            let services = row.resume_pending.join(", ");
            if dry_run {
                actions.push(format!("job {}: would resume {services}", row.ticket.id));
                continue;
            }
            actions.push(format!(
                "job {}: resuming {services} (detached; `recover --wait` reports the outcome)",
                row.ticket.id
            ));
            #[cfg(test)]
            {
                let store = self.clone();
                let id = row.ticket.id;
                std::thread::spawn(move || store.resume_services(id));
            }
            #[cfg(not(test))]
            {
                let executable = std::env::var_os("BORG_LANE_EXECUTABLE")
                    .map(PathBuf::from)
                    .unwrap_or(std::env::current_exe()?);
                let _ = Command::new(executable)
                    .args(["lane", "--state-dir"])
                    .arg(&self.root)
                    .args(["__resume_services"])
                    .arg(row.ticket.id.to_string())
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn();
            }
        }
        // Avoid nested metadata locks: test each kernel lock before journalling.
        for record in self.snapshot()? {
            if !matches!(
                record.state,
                TicketState::Granted(_) | TicketState::Queued | TicketState::Preparing
            ) || (record.job.is_none() && !record.service_lease)
            {
                continue;
            }
            let lock = stable_file(&self.ticket_path(record.ticket.id))?;
            match lock.try_lock_shared() {
                Ok(()) => (),
                Err(std::fs::TryLockError::WouldBlock) => continue,
                Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
            }
            // Hold the shared probe lock through recovery. Owner journals
            // Finished BEFORE unlocking its exclusive FD; a fresh metadata
            // check cannot race its release. Never quarantine or finish
            // that already-released row based on stale evidence.
            let active = self.reading(|state| {
                Ok(state.records.iter().any(|current| {
                    current.ticket.id == record.ticket.id
                        && matches!(
                            current.state,
                            TicketState::Granted(_) | TicketState::Queued | TicketState::Preparing
                        )
                }))
            })?;
            if !active {
                continue;
            }
            let note = format!(
                "job {} lost supervisor pid {:?} while {:?}",
                record.ticket.id, record.supervisor_pid, record.state
            );
            actions.push(note.clone());
            if dry_run {
                continue;
            }
            let mut verified = !record.service_lease;
            if let Some(JobHandle {
                state: JobState::Running { scope },
                ..
            }) = &record.job
            {
                if !scope.is_empty() {
                    let expected = format!("borg-lane-{}.scope", record.ticket.id);
                    let current = scope_control_group(scope);
                    if scope != &expected
                        || !current
                            .as_ref()
                            .is_some_and(|c| c.ends_with(&format!("/{expected}")))
                        || record
                            .scope_cgroup
                            .as_ref()
                            .is_some_and(|recorded| Some(recorded) != current.as_ref())
                    {
                        verified = false;
                    } else {
                        verified = Command::new("systemctl")
                            .args(["--user", "kill", "--signal=SIGKILL", scope])
                            .stdout(Stdio::null())
                            .stderr(Stdio::null())
                            .status()
                            .is_ok_and(|s| s.success());
                    }
                } else if let (Some(pid), Some(ticks)) =
                    (record.workload_pid, record.workload_start_ticks)
                {
                    if proc_start_ticks(pid) == Some(ticks) {
                        verified = unsafe { libc::kill(-(pid as i32), libc::SIGKILL) } == 0;
                    } else {
                        verified = false;
                    }
                } else {
                    verified = false;
                }
            }
            if !verified {
                self.locked(|state| {
                    let entry = state
                        .records
                        .iter_mut()
                        .find(|r| r.ticket.id == record.ticket.id)
                        .context("recovery job missing")?;
                    entry.quarantined = true;
                    entry.evidence =
                        Some(format!("unverified owner; resource quarantined: {note}"));
                    Ok(())
                })?;
            }
            self.log_recovery(record.ticket.id, &note)?;
            self.finish(record.ticket.id, 125, &note)?;
        }
        Ok(actions)
    }
}

#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;

#[async_trait]
impl LaneCoordinator for LaneStore {
    async fn enqueue(&self, request: LeaseRequest) -> Result<Ticket> {
        self.enqueue_lease(request)
    }
    async fn wait(&self, ticket: &Ticket) -> Result<Lease> {
        let store = self.clone();
        let id = ticket.id;
        tokio::task::spawn_blocking(move || {
            let lock = stable_file(&store.ticket_path(id))?;
            lock.lock()?;
            let granted = store.wait_grant(id, &mut 0);
            if granted.is_ok() {
                store.held.lock().unwrap().insert(id, lock);
            }
            granted
        })
        .await?
    }
    async fn release(&self, lease: &Lease) -> Result<()> {
        self.release_lease(lease)
    }
    async fn ticket_status(&self, ticket: &Ticket) -> Result<TicketState> {
        self.ticket_state(ticket)
    }
    async fn recover(&self, dry_run: bool) -> Result<Vec<String>> {
        LaneStore::recover(self, dry_run)
    }
}

#[async_trait]
impl JobCoordinator for LaneStore {
    async fn submit(&self, spec: JobSpec) -> Result<JobHandle> {
        self.enqueue_job(spec)
    }
    async fn wait(&self, id: Uuid) -> Result<JobHandle> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.wait_job(id)).await?
    }
    async fn status(&self, id: Uuid) -> Result<JobHandle> {
        self.job_status(id)
    }
    async fn cancel(&self, id: Uuid) -> Result<()> {
        self.cancel_ticket(id, "cancelled by requester")
    }
}

/// Cross-process journal change notification. Install the watch before taking
/// the first snapshot, then drain after each wake. The timeout only rechecks
/// external RAM/disk changes that have no journal event.
struct StateEvents {
    #[cfg(target_os = "linux")]
    fd: std::os::fd::OwnedFd,
}
impl StateEvents {
    fn new(path: &Path) -> Result<Self> {
        #[cfg(target_os = "linux")]
        {
            use std::ffi::CString;
            use std::os::fd::FromRawFd;
            use std::os::unix::ffi::OsStrExt;
            let fd = unsafe { libc::inotify_init1(libc::IN_CLOEXEC | libc::IN_NONBLOCK) };
            if fd < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            let fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) };
            let name = CString::new(path.as_os_str().as_bytes())?;
            let watch = unsafe {
                libc::inotify_add_watch(
                    std::os::fd::AsRawFd::as_raw_fd(&fd),
                    name.as_ptr(),
                    libc::IN_MOVED_TO,
                )
            };
            if watch < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(Self { fd })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = path;
            Ok(Self {})
        }
    }
    fn wait(&self, timeout: Duration) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            let mut poll = libc::pollfd {
                fd: self.fd.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            let rc = unsafe {
                libc::poll(
                    &mut poll,
                    1,
                    timeout.as_millis().min(i32::MAX as u128) as i32,
                )
            };
            if rc < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            if rc > 0 {
                let mut bytes = [0u8; 4096];
                while unsafe {
                    libc::read(self.fd.as_raw_fd(), bytes.as_mut_ptr().cast(), bytes.len())
                } > 0
                {}
            }
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            std::thread::sleep(timeout);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &str) -> ResourceKey {
        ResourceKey {
            scope: ResourceScope::Host,
            name: name.into(),
        }
    }
    fn resource(name: &str, access: Access) -> ResourceRequest {
        ResourceRequest {
            key: key(name),
            access,
        }
    }
    fn holder() -> Holder {
        Holder {
            participant_id: Uuid::nil(),
            session_id: Uuid::nil(),
            host_pid: None,
            purpose: "test".into(),
        }
    }
    fn test_budget(path: &Path) -> AdmissionBudget {
        AdmissionBudget {
            min_available_ram_bytes: 0,
            reserve_ram_bytes: 0,
            min_free_disk_bytes: 0,
            reserve_disk_bytes: 0,
            disk_path: path.into(),
        }
    }
    fn service_holder() -> Holder {
        Holder {
            purpose: "service:test".into(),
            ..holder()
        }
    }
    fn record(seq: u64, state: TicketState, resources: Vec<ResourceRequest>) -> LaneRecord {
        LaneRecord {
            ticket: Ticket {
                id: Uuid::from_u128(u128::from(seq)),
                sequence: seq,
            },
            request: LeaseRequest {
                resources,
                holder: holder(),
                queue_timeout_ms: None,
            },
            state,
            job: None,
            spec: None,
            created_ms: 0,
            started_ms: None,
            finished_ms: None,
            supervisor_pid: None,
            wait_reason: None,
            progress: None,
            parallelism_hint: None,
            cpu_seconds: None,
            workload_pid: None,
            workload_start_ticks: None,
            scope_cgroup: None,
            post_scope: None,
            quarantined: false,
            service_lease: false,
            service_admission: None,
            preparing_since_ms: None,
            yield_services: vec![],
            resume_pending: vec![],
            resume_error: None,
            resume_attempts: 0,
            cancel_requested: None,
            evidence: None,
        }
    }
    fn granted(seq: u64, resources: Vec<ResourceRequest>) -> LaneRecord {
        let mut row = record(seq, TicketState::Queued, resources);
        row.state = TicketState::Granted(Lease {
            id: Uuid::new_v4(),
            generation: seq,
            ticket: row.ticket.clone(),
            resources: row.request.resources.clone(),
            holder: holder(),
        });
        row
    }
    #[test]
    fn aliases_and_symlinks_never_create_distinct_project_or_worktree_locks() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("project");
        fs::create_dir(&project).unwrap();
        let alias = dir.path().join("project").join("..").join("project");
        let symlink = dir.path().join("project-link");
        std::os::unix::fs::symlink(&project, &symlink).unwrap();
        let store = LaneStore::new(dir.path().join("state")).unwrap();
        for scope in [ResourceScope::Project, ResourceScope::Worktree] {
            let canonical = ResourceKey {
                scope: scope(project.clone()),
                name: "editor".into(),
            };
            canonical.validate_canonical().unwrap();
            store
                .set_capacity(Capacity {
                    key: canonical.clone(),
                    slots: 2,
                })
                .unwrap();
            for path in [&alias, &symlink] {
                let key = ResourceKey {
                    scope: scope(path.clone()),
                    name: "editor".into(),
                };
                assert!(key.validate_canonical().is_err());
                assert!(
                    store
                        .set_capacity(Capacity {
                            key: key.clone(),
                            slots: 2
                        })
                        .is_err()
                );
                assert!(
                    store
                        .enqueue_lease(LeaseRequest {
                            resources: vec![ResourceRequest {
                                key,
                                access: Access::Exclusive
                            }],
                            holder: holder(),
                            queue_timeout_ms: None
                        })
                        .is_err()
                );
            }
        }
    }
    #[test]
    fn fifo_exclusive_not_overtaken_by_shared() {
        // A running shared reader must not allow a later reader to starve a queued writer.
        let state = Journal {
            client_revision: 0,
            sequence: 3,
            capacities: vec![Capacity {
                key: key("gpu"),
                slots: 2,
            }],
            records: vec![
                granted(1, vec![resource("gpu", Access::Shared { slots: 1 })]),
                record(
                    2,
                    TicketState::Queued,
                    vec![resource("gpu", Access::Exclusive)],
                ),
                record(
                    3,
                    TicketState::Queued,
                    vec![resource("gpu", Access::Shared { slots: 1 })],
                ),
            ],
        };
        assert!(
            dispatch_reason(&state, Uuid::from_u128(3))
                .unwrap()
                .unwrap()
                .contains("FIFO")
        );
        assert!(
            dispatch_reason(&state, Uuid::from_u128(2))
                .unwrap()
                .unwrap()
                .contains("exclusive")
        );
    }
    #[test]
    fn disjoint_jobs_admit_without_waiting_for_blocked_tree() {
        let state = Journal {
            client_revision: 0,
            sequence: 3,
            capacities: vec![],
            records: vec![
                granted(1, vec![resource("a", Access::Exclusive)]),
                record(
                    2,
                    TicketState::Queued,
                    vec![resource("a", Access::Exclusive)],
                ),
                record(
                    3,
                    TicketState::Queued,
                    vec![resource("b", Access::Exclusive)],
                ),
            ],
        };
        assert_eq!(dispatch_reason(&state, Uuid::from_u128(3)).unwrap(), None);
    }
    #[test]
    fn capacity_respects_weights_and_atomic_multi_key_requests() {
        let state = Journal {
            client_revision: 0,
            sequence: 2,
            capacities: vec![Capacity {
                key: key("ram"),
                slots: 4,
            }],
            records: vec![
                granted(1, vec![resource("ram", Access::Shared { slots: 3 })]),
                record(
                    2,
                    TicketState::Queued,
                    vec![
                        resource("cpu", Access::Shared { slots: 1 }),
                        resource("ram", Access::Shared { slots: 2 }),
                    ],
                ),
            ],
        };
        assert!(
            dispatch_reason(&state, Uuid::from_u128(2))
                .unwrap()
                .unwrap()
                .contains("ram")
        );
    }
    #[test]
    fn failing_bound_post_quarantines_without_changing_workload_exit() {
        let dir = tempfile::tempdir().unwrap();
        let store = LaneStore::new(dir.path()).unwrap();
        let spec = JobSpec {
            foreign_client_grace_ms: default_foreign_client_grace_ms(),
            foreign_client_grace_by_resource: vec![],
            abandon_after_ms: None,
            fingerprint: JobFingerprint("post-fail".into()),
            lease: LeaseRequest {
                resources: vec![resource("project", Access::Exclusive)],
                holder: holder(),
                queue_timeout_ms: None,
            },
            argv: vec!["true".into()],
            cwd: dir.path().into(),
            env: vec![],
            memory_max_bytes: None,
            admission: test_budget(dir.path()),
            pre_hook: None,
            post_hook: Some(Hook {
                argv: vec!["false".into()],
                timeout_ms: 1000,
            }),
            timeout_ms: 2000,
            stall_timeout_ms: None,
            coalesce: false,
        };
        let id = store
            .locked(|state| {
                let (ticket, _) =
                    store.enqueue_record(state, spec.lease.clone(), Some(spec.clone()))?;
                let row = state
                    .records
                    .iter_mut()
                    .find(|r| r.ticket.id == ticket.id)
                    .unwrap();
                row.yield_services.push("test".into());
                grant_entry(row);
                Ok(ticket.id)
            })
            .unwrap();
        store.run_post_for_job(&spec, id, true).unwrap();
        store.finish(id, 0, "workload succeeded").unwrap();
        let row = store.record(id).unwrap();
        assert!(row.quarantined);
        assert!(
            row.evidence
                .as_deref()
                .unwrap()
                .contains("post hook failed")
        );
        assert!(matches!(
            row.job.unwrap().state,
            JobState::Finished { exit_code: 0 }
        ));
        assert!(row.resume_pending.is_empty());
        // A late service restart must not pass the quarantine.
        assert!(
            store
                .try_acquire_service(
                    LeaseRequest {
                        resources: vec![resource("project", Access::Shared { slots: 1 })],
                        holder: service_holder(),
                        queue_timeout_ms: None,
                    },
                    &test_budget(dir.path())
                )
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn failed_resume_remains_visible_and_retry_clears_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = LaneStore::new(dir.path()).unwrap();
        let id = Uuid::new_v4();
        store
            .locked(|state| {
                let mut row = record(
                    1,
                    TicketState::Finished,
                    vec![resource("project", Access::Exclusive)],
                );
                row.ticket.id = id;
                row.yield_services.push("test".into());
                row.resume_pending.push("test".into());
                state.records.push(row);
                Ok(())
            })
            .unwrap();
        assert!(store.resume_services(id).is_err());
        let failed = store.record(id).unwrap();
        assert_eq!(failed.resume_pending, vec!["test"]);
        assert!(
            failed
                .resume_error
                .unwrap()
                .contains("service test resume pending")
        );
        fs::write(dir.path().join("fake-resume-ok"), b"ok").unwrap();
        store.resume_services(id).unwrap();
        assert!(store.record(id).unwrap().resume_pending.is_empty());
        assert!(store.record(id).unwrap().resume_error.is_none());
    }

    #[test]
    fn acknowledged_resume_waits_for_health_then_recover_clears_pending() {
        let dir = tempfile::tempdir().unwrap();
        let store = LaneStore::new(dir.path()).unwrap();
        let id = Uuid::new_v4();
        store
            .locked(|state| {
                let mut row = record(
                    1,
                    TicketState::Finished,
                    vec![resource("project", Access::Exclusive)],
                );
                row.ticket.id = id;
                row.yield_services.push("test".into());
                row.resume_pending.push("test".into());
                state.records.push(row);
                Ok(())
            })
            .unwrap();
        let status_file = dir.path().join("fake-service-status.json");
        // The service ACKed Resume (yield token gone), then failed readiness.
        fs::write(
            &status_file,
            serde_json::to_vec(&serde_json::json!({
                "id":"test", "state":{"Failed":{"reason":"health probe failed"}},
                "endpoint":null, "clients":[], "reason":"health probe failed", "yields":{}
            }))
            .unwrap(),
        )
        .unwrap();
        assert!(store.resume_services(id).is_err());
        let row = store.record(id).unwrap();
        assert_eq!(row.resume_pending, ["test"]);
        assert!(
            row.resume_error
                .unwrap()
                .contains("not healthy after resume")
        );
        // A later healthy status lets recover clear the row without sending
        // Resume a second time (its yield token is already gone).
        fs::write(
            &status_file,
            serde_json::to_vec(&serde_json::json!({
                "id":"test", "state":{"Healthy":{"backend":null}},
                "endpoint":null, "clients":[], "reason":"ready", "yields":{}
            }))
            .unwrap(),
        )
        .unwrap();
        // Parallel tests may fork while the previous resume controller owns
        // the flock; inherited descriptors can keep its gate busy briefly.
        // WouldBlock leaves the journal pending, so retry until a deadline.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            store.resume_services(id).unwrap();
            if store.record(id).unwrap().resume_pending.is_empty() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "resume gate stayed busy"
            );
            std::thread::sleep(Duration::from_millis(25));
        }
        let row = store.record(id).unwrap();
        assert!(row.resume_pending.is_empty());
        assert!(row.resume_error.is_none());
    }

    /// A finished job whose "test" service resume is pending.
    fn pending_resume(store: &LaneStore) -> Uuid {
        let id = Uuid::new_v4();
        store
            .locked(|state| {
                let mut row = record(
                    1,
                    TicketState::Finished,
                    vec![resource("project", Access::Exclusive)],
                );
                row.ticket.id = id;
                row.yield_services.push("test".into());
                row.resume_pending.push("test".into());
                state.records.push(row);
                Ok(())
            })
            .unwrap();
        id
    }

    /// Publish the "test" service's status atomically, as a supervisor does.
    fn fake_service_state(root: &Path, state: serde_json::Value) {
        let status = serde_json::json!({"id": "test", "state": state, "endpoint": null,
            "clients": [], "reason": "test", "yields": {}});
        let temporary = root.join("fake-service-status.json.tmp");
        fs::write(&temporary, serde_json::to_vec(&status).unwrap()).unwrap();
        fs::rename(temporary, root.join("fake-service-status.json")).unwrap();
    }

    /// A 2.4 s resume budget: 300 ms readiness + 50 + 50 ms probe + 2 s launch.
    fn short_resume_budget(root: &Path) -> Duration {
        let spec = root.join("services").join("test");
        fs::create_dir_all(&spec).unwrap();
        fs::write(
            spec.join("spec.json"),
            r#"{"readiness_timeout_ms": 300, "health": {"interval_ms": 50, "timeout_ms": 50}}"#,
        )
        .unwrap();
        crate::services::resume_budget(&root.join("services"), "test")
    }

    /// Failure mode: a slow-starting service (a real editor needs about a
    /// minute) leaving its resume pending until an operator retries.
    #[test]
    fn slow_start_resume_clears_pending_without_a_manual_step() {
        let dir = tempfile::tempdir().unwrap();
        let store = LaneStore::new(dir.path()).unwrap();
        let id = pending_resume(&store);
        fake_service_state(dir.path(), serde_json::json!("Starting"));
        let root = dir.path().to_path_buf();
        let ready = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(1500));
            fake_service_state(&root, serde_json::json!({"Healthy": {"backend": null}}));
        });
        store.resume_services(id).unwrap();
        ready.join().unwrap();
        let row = store.record(id).unwrap();
        assert!(row.resume_pending.is_empty(), "{row:?}");
        assert!(row.resume_error.is_none());
        assert_eq!(row.resume_attempts, 1);
    }

    /// Failure mode: a resumed service that never becomes Healthy waiting
    /// forever, or giving up before its own readiness budget.
    #[test]
    fn never_healthy_resume_journals_error_after_its_budget() {
        let dir = tempfile::tempdir().unwrap();
        let store = LaneStore::new(dir.path()).unwrap();
        let id = pending_resume(&store);
        let budget = short_resume_budget(dir.path());
        assert_eq!(budget, Duration::from_millis(2_400));
        fake_service_state(dir.path(), serde_json::json!("Starting"));
        let started = std::time::Instant::now();
        assert!(store.resume_services(id).is_err());
        assert!(started.elapsed() >= budget, "gave up early");
        let row = store.record(id).unwrap();
        assert_eq!(row.resume_pending, ["test"]);
        assert!(
            row.resume_error
                .unwrap()
                .contains("not healthy after resume")
        );
        // A missed readiness window ends the wait at once. Retry past a gate
        // briefly held by a descriptor that a parallel test's fork inherited.
        fake_service_state(
            dir.path(),
            serde_json::json!({"Degraded": {"reason": crate::services::READINESS_FAILED}}),
        );
        let started = std::time::Instant::now();
        while store.record(id).unwrap().resume_attempts < 2 {
            let _ = store.resume_services(id);
        }
        assert!(
            started.elapsed() < budget,
            "readiness failure waited out the budget"
        );
        assert_eq!(store.record(id).unwrap().resume_pending, ["test"]);
    }

    /// Failure mode: `recover` returning before the resume it started has an
    /// outcome, so people and scripts fall back to a hidden command.
    #[test]
    fn recover_wait_reports_each_resume_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let store = LaneStore::new(dir.path()).unwrap();
        let id = pending_resume(&store);
        fake_service_state(dir.path(), serde_json::json!("Starting"));
        let root = dir.path().to_path_buf();
        let ready = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(1000));
            fake_service_state(&root, serde_json::json!({"Healthy": {"backend": null}}));
        });
        let (actions, resumes) = store.recover_wait(Duration::from_secs(20)).unwrap();
        ready.join().unwrap();
        assert!(
            actions
                .iter()
                .any(|action| action.starts_with(&format!("job {id}: resuming test"))),
            "{actions:?}"
        );
        assert_eq!(resumes.len(), 1);
        assert_eq!(resumes[0].outcome, "resumed", "{resumes:?}");

        let dir = tempfile::tempdir().unwrap();
        let store = LaneStore::new(dir.path()).unwrap();
        pending_resume(&store);
        short_resume_budget(dir.path());
        fake_service_state(dir.path(), serde_json::json!("Starting"));
        let (_, resumes) = store.recover_wait(Duration::from_secs(20)).unwrap();
        assert_eq!(resumes[0].outcome, "failed", "{resumes:?}");
        assert!(
            resumes[0]
                .error
                .as_deref()
                .is_some_and(|error| error.contains("not healthy after resume"))
        );
    }

    #[test]
    fn service_disk_reservation_is_atomic_and_shared_with_jobs() {
        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a");
        let b_path = dir.path().join("b");
        fs::create_dir(&a_path).unwrap();
        fs::create_dir(&b_path).unwrap();
        let available = crate::workspace::hygiene::disk_available(&a_path).unwrap();
        let reserve = available.saturating_mul(3) / 5;
        let store = LaneStore::new(dir.path().join("state")).unwrap();
        let budget_a = AdmissionBudget {
            reserve_disk_bytes: reserve,
            ..test_budget(&a_path)
        };
        let budget_b = AdmissionBudget {
            reserve_disk_bytes: reserve,
            ..test_budget(&b_path)
        };
        let request = |id: &str| LeaseRequest {
            resources: vec![resource(id, Access::Shared { slots: 1 })],
            holder: Holder {
                purpose: format!("service:{id}"),
                ..holder()
            },
            queue_timeout_ms: None,
        };
        let first = store
            .try_acquire_service(request("first"), &budget_a)
            .unwrap()
            .unwrap();
        let job_budget = AdmissionBudget {
            min_free_disk_bytes: available / 2,
            ..test_budget(&b_path)
        };
        let job_reason = store
            .reading(|state| budget_reason(state, &job_budget))
            .unwrap()
            .unwrap();
        assert!(job_reason.contains("disk"), "{job_reason}");
        assert!(
            store
                .try_acquire_service(request("second"), &budget_b)
                .unwrap()
                .is_none()
        );
        let deferred = store
            .snapshot()
            .unwrap()
            .into_iter()
            .find(|r| r.request.holder.purpose == "service:second")
            .unwrap();
        assert!(matches!(deferred.state, TicketState::Cancelled { .. }));
        assert!(deferred.wait_reason.unwrap().contains("disk"));
        assert!(
            store
                .try_acquire_service(request("second"), &budget_b)
                .unwrap()
                .is_none()
        );
        let retries = store
            .snapshot()
            .unwrap()
            .into_iter()
            .filter(|r| r.request.holder.purpose == "service:second")
            .collect::<Vec<_>>();
        assert_eq!(
            retries.len(),
            1,
            "retries must not create unbounded tickets"
        );
        assert_eq!(retries[0].ticket.id, deferred.ticket.id);
        store.release_lease(&first).unwrap();
        let second = store
            .try_acquire_service(request("second"), &budget_b)
            .unwrap()
            .unwrap();
        assert_eq!(second.generation, first.generation + 2);
        store.release_lease(&second).unwrap();
        assert!(
            store
                .reading(|state| budget_reason(state, &job_budget))
                .unwrap()
                .is_none()
        );
    }
    #[test]
    fn missing_reserved_disk_path_cannot_bypass_reservation() {
        let dir = tempfile::tempdir().unwrap();
        let active = dir.path().join("active");
        fs::create_dir(&active).unwrap();
        let other = dir.path().join("other");
        fs::create_dir(&other).unwrap();
        let store = LaneStore::new(dir.path().join("lanes")).unwrap();
        let budget = AdmissionBudget {
            reserve_disk_bytes: 1,
            ..test_budget(&active)
        };
        let lease = store
            .try_acquire_service(
                LeaseRequest {
                    resources: vec![resource("service", Access::Shared { slots: 1 })],
                    holder: service_holder(),
                    queue_timeout_ms: None,
                },
                &budget,
            )
            .unwrap()
            .unwrap();
        fs::remove_dir(&active).unwrap();
        let error = store
            .reading(|state| budget_reason(state, &test_budget(&other)))
            .unwrap_err()
            .to_string();
        assert!(error.contains("reserved disk"), "{error}");
        assert!(
            store
                .try_acquire_service(
                    LeaseRequest {
                        resources: vec![resource("unrelated", Access::Shared { slots: 1 })],
                        holder: Holder {
                            purpose: "service:unrelated".into(),
                            ..holder()
                        },
                        queue_timeout_ms: None,
                    },
                    &test_budget(&other)
                )
                .unwrap()
                .is_none()
        );
        let reason = store
            .service_admission_reason("unrelated")
            .unwrap()
            .unwrap();
        assert!(reason.contains("budget inspection unavailable"), "{reason}");
        store.release_lease(&lease).unwrap();
    }
    fn test_service_status(root: &Path, clients: &[crate::services::ClientLease]) {
        let dir = root.join("services/test");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("state.json"),
            serde_json::to_vec(&serde_json::json!({
                "id": "test", "state": {"Healthy": {"backend": null}},
                "endpoint": null, "clients": clients, "reason": "ready", "yields": {}
            }))
            .unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn foreign_client_preparing_waits_for_release_while_own_client_does_not() {
        use crate::services::ClientLease;
        let dir = tempfile::tempdir().unwrap();
        let store = LaneStore::new(dir.path()).unwrap();
        let bound = vec![resource("project", Access::Shared { slots: 1 })];
        let service = store
            .try_acquire_service(
                LeaseRequest {
                    resources: bound.clone(),
                    holder: service_holder(),
                    queue_timeout_ms: None,
                },
                &test_budget(dir.path()),
            )
            .unwrap()
            .unwrap();
        let mut foreign_owner = holder();
        foreign_owner.participant_id = Uuid::new_v4();
        foreign_owner.session_id = Uuid::new_v4();
        let foreign = ClientLease {
            id: Uuid::new_v4(),
            service_id: "test".into(),
            owner: foreign_owner,
            expires_at_unix_ms: milliseconds() + 60_000,
            purpose: "editor work".into(),
        };
        test_service_status(dir.path(), std::slice::from_ref(&foreign));
        let spec = JobSpec {
            foreign_client_grace_ms: 0, // Forever until explicit release/expiry.
            foreign_client_grace_by_resource: vec![],
            abandon_after_ms: None,
            fingerprint: JobFingerprint("foreign-client".into()),
            lease: LeaseRequest {
                resources: vec![resource("project", Access::Exclusive)],
                holder: holder(),
                queue_timeout_ms: Some(5000),
            },
            argv: vec!["true".into()],
            cwd: dir.path().into(),
            env: vec![],
            memory_max_bytes: None,
            admission: test_budget(dir.path()),
            // The test service yield stub ACKs immediately. A bounded hook
            // models the real RPC barrier while the test releases its lease.
            pre_hook: Some(Hook {
                argv: vec!["sh".into(), "-c".into(), "sleep 0.4".into()],
                timeout_ms: 2000,
            }),
            post_hook: None,
            timeout_ms: 2000,
            stall_timeout_ms: None,
            coalesce: false,
        };
        let job = store
            .locked(|state| {
                Ok(store
                    .enqueue_record(state, spec.lease.clone(), Some(spec))?
                    .1
                    .unwrap())
            })
            .unwrap();
        let lock = stable_file(&store.ticket_path(job.id)).unwrap();
        lock.lock().unwrap();
        assert!(store.try_grant(job.id).unwrap().is_none());
        assert!(matches!(
            store.record(job.id).unwrap().state,
            TicketState::Preparing
        ));
        store
            .locked(|state| {
                let reason = store.foreign_client_wait(state, job.id)?.unwrap();
                assert!(
                    reason.starts_with("foreign client lease: service test"),
                    "{reason}"
                );
                Ok(())
            })
            .unwrap();
        // A late client grant/renew loses to the Preparing reservation.
        let error = store
            .service_client_change("test", &bound, true, || Ok(()))
            .unwrap_err();
        assert!(
            error.to_string().contains("new client lease denied"),
            "{error}"
        );
        let waiter = store.clone();
        let task = std::thread::spawn(move || waiter.wait_grant(job.id, &mut 0));
        let entered = std::time::Instant::now();
        loop {
            let row = store.record(job.id).unwrap();
            if row
                .wait_reason
                .as_deref()
                .is_some_and(|r| r.starts_with("foreign client lease:"))
            {
                break;
            }
            assert!(
                entered.elapsed() < Duration::from_secs(2),
                "foreign wait not reported"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(matches!(
            store.record(job.id).unwrap().state,
            TicketState::Preparing
        ));
        assert!(
            store
                .service_preemption_notice("test")
                .unwrap()
                .unwrap()
                .contains("foreign client lease:")
        );
        let visible = crate::services::ServiceManager::new(
            dir.path().join("services"),
            std::path::PathBuf::new(),
        )
        .read_status("test")
        .unwrap();
        assert!(visible.reason.contains("foreign client lease:"));
        // The requester's own lease must not delay the same job.
        let own = ClientLease {
            owner: holder(),
            ..foreign.clone()
        };
        store
            .service_client_change("test", &bound, false, || {
                test_service_status(dir.path(), std::slice::from_ref(&own));
                Ok(())
            })
            .unwrap();
        store
            .locked(|state| {
                assert!(store.foreign_client_wait(state, job.id)?.is_none());
                Ok(())
            })
            .unwrap();
        // The fake service relinquishes its shared backend lease after yield.
        store.release_lease(&service).unwrap();
        let exclusive = task.join().unwrap().unwrap();
        assert_eq!(exclusive.ticket.id, job.id);
        store.finish(job.id, 0, "test complete").unwrap();
    }

    /// Failure mode: a Shared-mode service admits several owners, so the
    /// client fence must still hold an exclusive on ANY foreign owner beside
    /// the requester's own lease, refuse new owners and renewals while
    /// Preparing, and lift only on a fenced release or grace expiry.
    #[tokio::test]
    async fn shared_service_foreign_client_fences_preparing_exclusive() {
        use crate::services::tests::{cleanup, setup_with};
        use crate::services::{ClientMode, ServiceRequest};
        let (root, manager, _front, task) = setup_with(|spec| {
            spec.client_mode = ClientMode::Shared { max_clients: 3 };
        })
        .await;
        let store = LaneStore::new(root.path()).unwrap();
        let someone = || Holder {
            participant_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            ..holder()
        };
        let (own, foreign) = (someone(), someone());
        let lease = |owner: Holder| ServiceRequest::Lease {
            owner,
            purpose: "shared".into(),
            ttl_ms: 60_000,
        };
        let rpc = Duration::from_secs(3);
        manager.send("fake", lease(own.clone()), rpc).await.unwrap();
        manager
            .send("fake", lease(foreign.clone()), rpc)
            .await
            .unwrap();
        let prepare = |grace_ms: u64| {
            let spec = JobSpec {
                foreign_client_grace_ms: grace_ms,
                foreign_client_grace_by_resource: vec![],
                abandon_after_ms: None,
                fingerprint: JobFingerprint("shared-foreign".into()),
                lease: LeaseRequest {
                    resources: vec![resource("fake-exclusive", Access::Exclusive)],
                    holder: own.clone(),
                    queue_timeout_ms: None,
                },
                argv: vec!["true".into()],
                cwd: root.path().into(),
                env: vec![],
                memory_max_bytes: None,
                admission: test_budget(root.path()),
                pre_hook: None,
                post_hook: None,
                timeout_ms: 2000,
                stall_timeout_ms: None,
                coalesce: false,
            };
            let job = store
                .locked(|state| {
                    Ok(store
                        .enqueue_record(state, spec.lease.clone(), Some(spec))?
                        .1
                        .unwrap())
                })
                .unwrap();
            let lock = stable_file(&store.ticket_path(job.id)).unwrap();
            lock.lock().unwrap();
            assert!(store.try_grant(job.id).unwrap().is_none());
            assert!(matches!(
                store.record(job.id).unwrap().state,
                TicketState::Preparing
            ));
            (job.id, lock)
        };
        let waiting = |id| {
            store
                .locked(|state| store.foreign_client_wait(state, id))
                .unwrap()
        };
        let clients = || {
            let mut rows = manager
                .read_status("fake")
                .unwrap()
                .clients
                .into_iter()
                .map(|c| (c.owner.participant_id, c.id, c.expires_at_unix_ms))
                .collect::<Vec<_>>();
            rows.sort();
            rows
        };

        let (job, _held) = prepare(0);
        let reason = waiting(job).expect("foreign shared client must hold the exclusive");
        assert!(
            reason.contains(&foreign.participant_id.to_string()),
            "{reason}"
        );
        let before = clients();
        assert_eq!(before.len(), 2);
        for late in [someone(), own.clone()] {
            let error = manager.send("fake", lease(late), rpc).await.unwrap_err();
            assert!(
                error.to_string().contains("new client lease denied"),
                "{error}"
            );
        }
        assert_eq!(clients(), before, "late owner or renewal changed clients");
        let foreign_id = before
            .iter()
            .find(|(owner, ..)| *owner == foreign.participant_id)
            .unwrap()
            .1;
        manager
            .send(
                "fake",
                ServiceRequest::Release {
                    lease_id: foreign_id,
                    owner: foreign.clone(),
                },
                rpc,
            )
            .await
            .unwrap();
        assert!(waiting(job).is_none(), "own shared lease blocked its job");
        store.finish(job, 125, "test ends before yield").unwrap();

        manager
            .send("fake", lease(foreign.clone()), rpc)
            .await
            .unwrap();
        let (job, _held) = prepare(300);
        assert!(waiting(job).is_some());
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert!(waiting(job).is_none(), "grace expiry did not lift the wait");
        assert_eq!(clients().len(), 2);
        store.finish(job, 125, "test ends before yield").unwrap();
        cleanup(&manager, task).await;
    }

    #[test]
    fn foreign_grace_expiry_does_not_reset_after_recovery() {
        use crate::services::ClientLease;
        let dir = tempfile::tempdir().unwrap();
        let store = LaneStore::new(dir.path()).unwrap();
        let service = store
            .try_acquire_service(
                LeaseRequest {
                    resources: vec![
                        resource("project", Access::Shared { slots: 1 }),
                        resource("editor", Access::Shared { slots: 1 }),
                    ],
                    holder: service_holder(),
                    queue_timeout_ms: None,
                },
                &test_budget(dir.path()),
            )
            .unwrap()
            .unwrap();
        let foreign = ClientLease {
            id: Uuid::new_v4(),
            service_id: "test".into(),
            owner: Holder {
                participant_id: Uuid::new_v4(),
                session_id: Uuid::new_v4(),
                ..holder()
            },
            expires_at_unix_ms: milliseconds() + 60_000,
            purpose: "editor work".into(),
        };
        test_service_status(dir.path(), &[foreign]);
        let spec = JobSpec {
            foreign_client_grace_ms: 500,
            abandon_after_ms: None,
            foreign_client_grace_by_resource: vec![ForeignClientGrace {
                resource: resource("editor", Access::Exclusive).key,
                grace_ms: 0,
            }],
            fingerprint: JobFingerprint("grace-client".into()),
            lease: LeaseRequest {
                resources: vec![
                    resource("project", Access::Exclusive),
                    resource("editor", Access::Exclusive),
                ],
                holder: holder(),
                queue_timeout_ms: None,
            },
            argv: vec!["true".into()],
            cwd: dir.path().into(),
            env: vec![],
            memory_max_bytes: None,
            admission: test_budget(dir.path()),
            pre_hook: None,
            post_hook: None,
            timeout_ms: 2000,
            stall_timeout_ms: None,
            coalesce: false,
        };
        let mut invalid = spec.clone();
        invalid
            .foreign_client_grace_by_resource
            .push(ForeignClientGrace {
                resource: resource("editor", Access::Exclusive).key,
                grace_ms: 5,
            });
        assert!(
            store
                .locked(|state| {
                    store.enqueue_record(state, invalid.lease.clone(), Some(invalid.clone()))
                })
                .is_err(),
            "duplicate resource grace override was accepted"
        );
        let job = store
            .locked(|state| {
                Ok(store
                    .enqueue_record(state, spec.lease.clone(), Some(spec))?
                    .1
                    .unwrap())
            })
            .unwrap();
        assert!(store.try_grant(job.id).unwrap().is_none());
        store
            .locked(|state| {
                let row = state
                    .records
                    .iter_mut()
                    .find(|r| r.ticket.id == job.id)
                    .unwrap();
                row.preparing_since_ms = Some(milliseconds() - 1000);
                Ok(())
            })
            .unwrap();
        // A new LaneStore (e.g. after supervisor recovery) sees the
        // indefinite editor grace even though project grace has expired.
        let recovered = LaneStore::new(dir.path()).unwrap();
        recovered
            .locked(|state| {
                assert!(
                    recovered
                        .foreign_client_wait(state, job.id)?
                        .unwrap()
                        .contains("indefinite")
                );
                state
                    .records
                    .iter_mut()
                    .find(|r| r.ticket.id == job.id)
                    .unwrap()
                    .spec
                    .as_mut()
                    .unwrap()
                    .foreign_client_grace_by_resource[0]
                    .grace_ms = 500;
                assert!(recovered.foreign_client_wait(state, job.id)?.is_none());
                Ok(())
            })
            .unwrap();
        store.release_lease(&service).unwrap();
    }

    #[test]
    fn service_lease_yields_before_exclusive_grant_and_blocks_restart() {
        let dir = tempfile::tempdir().unwrap();
        let store = LaneStore::new(dir.path()).unwrap();
        let service_request = LeaseRequest {
            resources: vec![resource("project", Access::Shared { slots: 1 })],
            holder: service_holder(),
            queue_timeout_ms: None,
        };
        let service = store
            .try_acquire_service(service_request.clone(), &test_budget(dir.path()))
            .unwrap()
            .unwrap();
        test_service_status(dir.path(), &[]);
        let spec = JobSpec {
            foreign_client_grace_ms: default_foreign_client_grace_ms(),
            foreign_client_grace_by_resource: vec![],
            abandon_after_ms: None,
            fingerprint: JobFingerprint("yield-r1".into()),
            lease: LeaseRequest {
                resources: vec![resource("project", Access::Exclusive)],
                holder: holder(),
                queue_timeout_ms: None,
            },
            argv: vec!["true".into()],
            cwd: dir.path().into(),
            env: vec![],
            memory_max_bytes: None,
            admission: AdmissionBudget {
                min_available_ram_bytes: 0,
                reserve_ram_bytes: 0,
                min_free_disk_bytes: 0,
                reserve_disk_bytes: 0,
                disk_path: dir.path().into(),
            },
            pre_hook: Some(Hook {
                argv: vec!["sh".into(), "-c".into(), "sleep 0.3".into()],
                timeout_ms: 2000,
            }),
            post_hook: None,
            timeout_ms: 2000,
            stall_timeout_ms: None,
            coalesce: false,
        };
        let job = store
            .locked(|state| {
                Ok(store
                    .enqueue_record(state, spec.lease.clone(), Some(spec))?
                    .1
                    .unwrap())
            })
            .unwrap();
        let owner_lock = stable_file(&store.ticket_path(job.id)).unwrap();
        owner_lock.lock().unwrap();
        let waiting = store.clone();
        let task = std::thread::spawn(move || waiting.wait_grant(job.id, &mut 0));
        let started = std::time::Instant::now();
        loop {
            if matches!(
                store.ticket_state(&job.ticket).unwrap(),
                TicketState::Preparing
            ) {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "exclusive did not prepare"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        // Preparing blocks a new backend even before the old backend yields.
        assert!(
            store
                .try_acquire_service(service_request.clone(), &test_budget(dir.path()))
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            store.ticket_state(&job.ticket).unwrap(),
            TicketState::Preparing
        ));
        store.release_lease(&service).unwrap();
        let exclusive = task.join().unwrap().unwrap();
        assert!(matches!(
            store.ticket_state(&job.ticket).unwrap(),
            TicketState::Granted(_)
        ));
        // Timed resume/crash restart must fail until this exclusive is finished.
        assert!(
            store
                .try_acquire_service(service_request.clone(), &test_budget(dir.path()))
                .unwrap()
                .is_none()
        );
        store
            .finish(exclusive.ticket.id, 0, "test complete")
            .unwrap();
        let resumed = store
            .try_acquire_service(service_request, &test_budget(dir.path()))
            .unwrap()
            .unwrap();
        assert!(resumed.generation > service.generation);
        store.release_lease(&resumed).unwrap();
    }

    #[test]
    fn exclusive_discovers_all_service_bindings_without_adapter_hook() {
        let dir = tempfile::tempdir().unwrap();
        let store = LaneStore::new(dir.path()).unwrap();
        let held = store
            .try_acquire_service(
                LeaseRequest {
                    resources: vec![resource("project", Access::Shared { slots: 1 })],
                    holder: service_holder(),
                    queue_timeout_ms: None,
                },
                &test_budget(dir.path()),
            )
            .unwrap()
            .unwrap();
        let spec = JobSpec {
            foreign_client_grace_ms: default_foreign_client_grace_ms(),
            foreign_client_grace_by_resource: vec![],
            abandon_after_ms: None,
            fingerprint: JobFingerprint("auto-bound".into()),
            lease: LeaseRequest {
                resources: vec![resource("project", Access::Exclusive)],
                holder: holder(),
                queue_timeout_ms: None,
            },
            argv: vec!["true".into()],
            cwd: dir.path().into(),
            env: vec![],
            memory_max_bytes: None,
            admission: AdmissionBudget {
                min_available_ram_bytes: 0,
                reserve_ram_bytes: 0,
                min_free_disk_bytes: 0,
                reserve_disk_bytes: 0,
                disk_path: dir.path().into(),
            },
            pre_hook: None,
            post_hook: None,
            timeout_ms: 2000,
            stall_timeout_ms: None,
            coalesce: false,
        };
        let id = store
            .locked(|state| {
                Ok(store
                    .enqueue_record(state, spec.lease.clone(), Some(spec))?
                    .0
                    .id)
            })
            .unwrap();
        assert!(store.try_grant(id).unwrap().is_none());
        let row = store.record(id).unwrap();
        assert!(matches!(row.state, TicketState::Preparing));
        assert_eq!(row.yield_services, vec!["test"]);
        store.finish(id, 125, "test").unwrap();
        store.release_lease(&held).unwrap();
    }
    #[test]
    fn pre_hook_success_without_yield_does_not_grant() {
        let dir = tempfile::tempdir().unwrap();
        let store = LaneStore::new(dir.path()).unwrap();
        let held = store
            .try_acquire_service(
                LeaseRequest {
                    resources: vec![resource("project", Access::Shared { slots: 1 })],
                    holder: service_holder(),
                    queue_timeout_ms: None,
                },
                &test_budget(dir.path()),
            )
            .unwrap()
            .unwrap();
        test_service_status(dir.path(), &[]);
        let spec = JobSpec {
            foreign_client_grace_ms: default_foreign_client_grace_ms(),
            foreign_client_grace_by_resource: vec![],
            abandon_after_ms: None,
            fingerprint: JobFingerprint("no-yield".into()),
            lease: LeaseRequest {
                resources: vec![resource("project", Access::Exclusive)],
                holder: holder(),
                queue_timeout_ms: None,
            },
            argv: vec!["true".into()],
            cwd: dir.path().into(),
            env: vec![],
            memory_max_bytes: None,
            admission: AdmissionBudget {
                min_available_ram_bytes: 0,
                reserve_ram_bytes: 0,
                min_free_disk_bytes: 0,
                reserve_disk_bytes: 0,
                disk_path: dir.path().into(),
            },
            pre_hook: Some(Hook {
                argv: vec!["true".into()],
                timeout_ms: 1000,
            }),
            post_hook: None,
            timeout_ms: 2000,
            stall_timeout_ms: None,
            coalesce: false,
        };
        let job = store
            .locked(|state| {
                Ok(store
                    .enqueue_record(state, spec.lease.clone(), Some(spec))?
                    .1
                    .unwrap())
            })
            .unwrap();
        let owner_lock = stable_file(&store.ticket_path(job.id)).unwrap();
        owner_lock.lock().unwrap();
        assert!(
            store
                .wait_grant(job.id, &mut 0)
                .unwrap_err()
                .to_string()
                .contains("did not release")
        );
        assert!(!matches!(
            store.ticket_state(&job.ticket).unwrap(),
            TicketState::Granted(_)
        ));
        store.finish(job.id, 125, "failed yield").unwrap();
        store.release_lease(&held).unwrap();
    }
    fn coalescing_spec(dir: &Path, queue_timeout_ms: Option<u64>) -> JobSpec {
        JobSpec {
            foreign_client_grace_ms: default_foreign_client_grace_ms(),
            foreign_client_grace_by_resource: vec![],
            abandon_after_ms: None,
            fingerprint: JobFingerprint("source-r1".into()),
            lease: LeaseRequest {
                resources: vec![resource("build", Access::Exclusive)],
                holder: holder(),
                queue_timeout_ms,
            },
            argv: vec!["true".into()],
            cwd: dir.to_path_buf(),
            env: vec![],
            memory_max_bytes: Some(2_000_000_000),
            admission: AdmissionBudget {
                min_available_ram_bytes: 0,
                reserve_ram_bytes: 1,
                min_free_disk_bytes: 0,
                reserve_disk_bytes: 1,
                disk_path: dir.into(),
            },
            pre_hook: None,
            post_hook: None,
            timeout_ms: 5000,
            stall_timeout_ms: None,
            coalesce: true,
        }
    }

    /// Failure mode: a running job that cannot be cancelled, or a late
    /// supervisor exit rewriting a cancelled job as Finished.
    #[test]
    fn cancels_reach_jobs_past_queued_and_terminal_records_stay_put() {
        let dir = tempfile::tempdir().unwrap();
        let store = LaneStore::new(dir.path()).unwrap();
        let spec = coalescing_spec(dir.path(), None);
        let (queued, running) = store
            .locked(|state| {
                let mut job = |argv: &str| {
                    let spec = JobSpec {
                        argv: vec![argv.into()],
                        ..spec.clone()
                    };
                    store
                        .enqueue_record(state, spec.lease.clone(), Some(spec))
                        .map(|(_, job)| job.unwrap().id)
                };
                let (running, queued) = (job("first")?, job("second")?);
                grant_entry(
                    state
                        .records
                        .iter_mut()
                        .find(|r| r.ticket.id == running)
                        .unwrap(),
                );
                Ok((queued, running))
            })
            .unwrap();
        store.cancel_ticket(queued, "stop").unwrap();
        store.finish(queued, 125, "supervisor exit").unwrap();
        assert!(matches!(
            store.job_status(queued).unwrap().state,
            JobState::Cancelled { .. }
        ));
        store.cancel_ticket(running, "stop").unwrap();
        let row = store.record(running).unwrap();
        assert!(matches!(row.state, TicketState::Granted(_)));
        assert_eq!(row.cancel_requested.as_deref(), Some("stop"));
        store
            .conclude(running, JobEnd::Cancelled("stop".into()), "cancelled: stop")
            .unwrap();
        assert!(matches!(
            store.job_status(running).unwrap().state,
            JobState::Cancelled { ref reason } if reason == "stop"
        ));
        assert!(store.cancel_ticket(running, "again").is_err());
    }

    /// Failure mode: callers that name job directories and scopes by job id,
    /// and pick "the newest job" by id, getting an arbitrary order.
    #[test]
    fn ticket_ids_are_time_ordered() {
        let dir = tempfile::tempdir().unwrap();
        let store = LaneStore::new(dir.path()).unwrap();
        let tickets: Vec<Ticket> = (0..20)
            .map(|_| {
                store
                    .enqueue_lease(LeaseRequest {
                        resources: vec![resource("build", Access::Shared { slots: 1 })],
                        holder: holder(),
                        queue_timeout_ms: None,
                    })
                    .unwrap()
            })
            .collect();
        assert!(tickets.iter().all(|t| t.id.get_version_num() == 7));
        assert!(
            tickets
                .windows(2)
                .all(|pair| pair[0].id < pair[1].id && pair[0].sequence < pair[1].sequence)
        );
    }

    #[test]
    fn pending_coalesces_only_matching_workload_and_options() {
        let dir = tempfile::tempdir().unwrap();
        let store = LaneStore::new(dir.path()).unwrap();
        let spec = coalescing_spec(dir.path(), None);
        store
            .locked(|state| {
                let first = store
                    .enqueue_record(state, spec.lease.clone(), Some(spec.clone()))?
                    .0;
                let joined = store
                    .enqueue_record(state, spec.lease.clone(), Some(spec.clone()))?
                    .0;
                assert_eq!(first.id, joined.id);
                let mut changed_grace = spec.clone();
                changed_grace.foreign_client_grace_by_resource = vec![ForeignClientGrace {
                    resource: resource("build", Access::Exclusive).key,
                    grace_ms: 0,
                }];
                assert_ne!(
                    first.id,
                    store
                        .enqueue_record(
                            state,
                            changed_grace.lease.clone(),
                            Some(changed_grace.clone()),
                        )?
                        .0
                        .id
                );
                let changed = JobSpec {
                    argv: vec!["false".into()],
                    ..spec.clone()
                };
                assert_ne!(
                    first.id,
                    store
                        .enqueue_record(state, changed.lease.clone(), Some(changed))?
                        .0
                        .id
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn coalescing_never_binds_a_joiner_to_a_longer_queue_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let store = LaneStore::new(dir.path()).unwrap();
        let unbounded = coalescing_spec(dir.path(), None);
        let short = coalescing_spec(dir.path(), Some(1_000));
        let long = coalescing_spec(dir.path(), Some(60_000));
        store
            .locked(|state| {
                let mut submit = |spec: &JobSpec| {
                    store
                        .enqueue_record(state, spec.lease.clone(), Some(spec.clone()))
                        .map(|(ticket, _)| ticket.id)
                };
                let open = submit(&unbounded)?;
                // A 1 s joiner must not ride a ticket that never times out.
                let bounded = submit(&short)?;
                assert_ne!(open, bounded);
                // Nor a 60 s ticket onto a 1 s one, or back.
                let longer = submit(&long)?;
                assert_ne!(longer, bounded);
                assert_ne!(longer, open);
                // Equal limits still coalesce, bounded and unbounded alike.
                assert_eq!(submit(&short)?, bounded);
                assert_eq!(submit(&long)?, longer);
                assert_eq!(submit(&unbounded)?, open);
                Ok(())
            })
            .unwrap();
    }
}
