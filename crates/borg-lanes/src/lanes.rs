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
    /// Scope unit prefix: the workload runs as `<prefix>-<id>.scope`.
    /// Must match `[a-z][a-z0-9-]{0,23}`; None means `borg-lane`.
    #[serde(default)]
    pub unit_prefix: Option<String>,
    /// Runs once, detached and bounded by its timeout, after the job ended
    /// on any path: finished, cancelled queued or running, queue timeout,
    /// abandoned, or recovered from a lost supervisor.
    #[serde(default)]
    pub finish_hook: Option<Hook>,
    /// Swap limit of the workload scope (systemd MemorySwapMax); None
    /// leaves the default.
    #[serde(default)]
    pub memory_swap_max_bytes: Option<u64>,
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

impl JobSpec {
    /// The systemd scope unit its workload runs in.
    pub fn scope_unit(&self, id: Uuid) -> String {
        format!(
            "{}-{id}.scope",
            self.unit_prefix.as_deref().unwrap_or("borg-lane")
        )
    }
}

fn valid_unit_prefix(prefix: &str) -> bool {
    prefix.len() <= 24
        && prefix.starts_with(|c: char| c.is_ascii_lowercase())
        && prefix
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
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
    /// A service restart's transient exclusive claim on its `defer_while`
    /// keys (`LaneStore::try_acquire_restart_barrier`); never a yielding
    /// service lease.
    #[serde(default)]
    pub restart_barrier: bool,
    /// A service lease's or restart barrier's owning supervisor: its start
    /// time (with `supervisor_pid`) and the service unit cgroup it and its
    /// backends run in, so recovery can prove the owner gone and release it.
    #[serde(default)]
    pub owner_start_ticks: Option<u64>,
    #[serde(default)]
    pub owner_cgroup: Option<String>,
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
    /// When the finish hook was claimed; journalled before it is spawned,
    /// so it never runs twice.
    #[serde(default)]
    pub finish_hook_started_ms: Option<u64>,
    /// How the finish hook ended (`exited 0`, `timed out after …`, …).
    #[serde(default)]
    pub finish_hook_outcome: Option<String>,
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
            // A predecessor killed with its backends (SIGKILL, OOM) must not
            // hold this service's resources, or its restart barrier, forever.
            self.release_dead_service_claims(state, false)?;
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
            entry.supervisor_pid = entry.request.holder.host_pid;
            entry.owner_start_ticks = entry.supervisor_pid.and_then(proc_start_ticks);
            entry.owner_cgroup = entry.supervisor_pid.and_then(service_unit_cgroup);
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

    /// Release every service lease and restart barrier whose owning service
    /// supervisor is provably gone (see `claim_owner_gone`): a Granted one
    /// whose lock is free ends Finished, and one that recovery quarantined
    /// (it could not prove the owner gone then) is un-quarantined. A claim
    /// whose owner may still run is left as it is (fail closed). With
    /// `dry_run` only the report is returned. Runs under the journal lock.
    fn release_dead_service_claims(
        &self,
        state: &mut Journal,
        dry_run: bool,
    ) -> Result<Vec<(Uuid, String)>> {
        let mut actions = Vec::new();
        for row in state
            .records
            .iter_mut()
            .filter(|r| r.restart_barrier || r.service_lease)
        {
            let granted = matches!(row.state, TicketState::Granted(_));
            let quarantined = row.quarantined && matches!(row.state, TicketState::Finished);
            if !granted && !quarantined {
                continue;
            }
            if granted {
                // A live owner holds this lock (nonblocking probe; the
                // journal lock is a different file).
                let lock = stable_file(&self.ticket_path(row.ticket.id))?;
                match lock.try_lock_shared() {
                    Ok(()) => (),
                    Err(std::fs::TryLockError::WouldBlock) => continue,
                    Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
                }
            }
            let Some(proof) = claim_owner_gone(row) else {
                continue;
            };
            let kind = if row.restart_barrier {
                "restart barrier"
            } else {
                "service lease"
            };
            let note = format!(
                "{kind} {} released: its owner is gone ({proof})",
                row.ticket.id
            );
            if dry_run {
                actions.push((
                    row.ticket.id,
                    format!(
                        "{kind} {}: would release, its owner is gone ({proof})",
                        row.ticket.id
                    ),
                ));
                continue;
            }
            actions.push((row.ticket.id, note.clone()));
            if granted {
                row.state = TicketState::Finished;
                row.finished_ms = Some(milliseconds());
            }
            row.quarantined = false;
            row.evidence = Some(match row.evidence.take() {
                Some(earlier) => format!("{earlier}; {note}"),
                None => note.clone(),
            });
            let entry = serde_json::json!({"time_ms": milliseconds(), "job": row.ticket.id,
                "reason": note, "evidence": &*row});
            let mut file = OpenOptions::new()
                .append(true)
                .create(true)
                .open(self.root.join("recovery.jsonl"))?;
            writeln!(file, "{entry}")?;
        }
        Ok(actions)
    }

    /// Nonblocking, all-or-nothing exclusive claim on a service restart's
    /// `defer_while` keys, decided under the same metadata lock that admits
    /// builds: no build is admitted between this check and the restart, nor
    /// until the barrier is released. A ticket that holds (Granted or
    /// Preparing), waits for (Queued: builds already in line keep their FIFO
    /// turn) or quarantines one of the keys defers the restart; a refused
    /// attempt journals nothing and creates no lock inode. The barrier is not
    /// a service lease, so a job can never prepare past it by having a
    /// service yield.
    pub(crate) fn try_acquire_restart_barrier(
        &self,
        request: LeaseRequest,
    ) -> Result<RestartBarrier> {
        ensure!(
            request
                .resources
                .iter()
                .all(|r| matches!(r.access, Access::Exclusive)),
            "restart barrier keys must be exclusive"
        );
        ensure!(
            request.holder.purpose.starts_with("restart:"),
            "restart barrier holder must name its service"
        );
        let mut held: Option<File> = None;
        let outcome = self.locked(|state| {
            Self::validate(&request, &state.capacities)?;
            // A barrier left by a supervisor that is provably gone (killed
            // mid-restart) must not defer the next one forever.
            self.release_dead_service_claims(state, false)?;
            let blocker = state.records.iter().find_map(|record| {
                let how = if record.quarantined {
                    "quarantined by"
                } else {
                    match record.state {
                        TicketState::Granted(_) | TicketState::Preparing => "held by",
                        TicketState::Queued => "queued for",
                        _ => return None,
                    }
                };
                let claim = record.request.resources.iter().find(|claim| {
                    request
                        .resources
                        .iter()
                        .any(|wanted| conflicts(claim, wanted))
                })?;
                Some(format!(
                    "{} {how} ticket {}",
                    key_label(&claim.key),
                    record.ticket.id
                ))
            });
            if let Some(reason) = blocker {
                return Ok(RestartBarrier::Deferred(reason));
            }
            let (ticket, _) = self.enqueue_record(state, request, None)?;
            let lock = stable_file(&self.ticket_path(ticket.id))?;
            lock.lock()?;
            held = Some(lock);
            let entry = state
                .records
                .iter_mut()
                .find(|r| r.ticket.id == ticket.id)
                .context("restart barrier ticket missing")?;
            entry.restart_barrier = true;
            entry.supervisor_pid = entry.request.holder.host_pid;
            entry.owner_start_ticks = entry.supervisor_pid.and_then(proc_start_ticks);
            entry.owner_cgroup = entry.supervisor_pid.and_then(service_unit_cgroup);
            Ok(RestartBarrier::Held(grant_entry(entry)))
        })?;
        if let RestartBarrier::Held(lease) = &outcome {
            self.held.lock().unwrap().insert(
                lease.ticket.id,
                held.context("restart barrier lock FD missing")?,
            );
        }
        Ok(outcome)
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

    /// Drop expired terminal records (see `expired_records`) with their job
    /// directories and lock files. Runs whenever a ticket is created, which
    /// is the only way the journal grows.
    fn prune(&self, state: &mut Journal) {
        let max_age_ms = std::env::var("BORG_LANE_RETAIN_SECONDS")
            .ok()
            .and_then(|seconds| seconds.parse::<u64>().ok())
            .unwrap_or(RETAIN_SECONDS)
            .saturating_mul(1000);
        let expired = expired_records(state, milliseconds(), max_age_ms, RETAIN_RECORDS);
        if expired.is_empty() {
            return;
        }
        state.records.retain(|r| !expired.contains(&r.ticket.id));
        for id in expired {
            let _ = fs::remove_dir_all(self.job_dir(id));
            let _ = fs::remove_file(self.ticket_path(id));
            let _ = fs::remove_file(self.root.join("locks").join(format!("resume-{id}.flock")));
        }
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
        self.prune(state);
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
            if let Some(prefix) = &spec.unit_prefix {
                ensure!(
                    valid_unit_prefix(prefix),
                    "unit_prefix must match [a-z][a-z0-9-]{{0,23}}: {prefix:?}"
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
                        && serde_json::to_value(&spec.finish_hook).ok()
                            == serde_json::to_value(&other.finish_hook).ok()
                        && spec.memory_max_bytes == other.memory_max_bytes
                        && spec.memory_swap_max_bytes == other.memory_swap_max_bytes
                        && spec.foreign_client_grace_ms == other.foreign_client_grace_ms
                        && spec.foreign_client_grace_by_resource
                            == other.foreign_client_grace_by_resource
                        && spec.abandon_after_ms == other.abandon_after_ms
                        && spec.unit_prefix == other.unit_prefix
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
            restart_barrier: false,
            owner_start_ticks: None,
            owner_cgroup: None,
            service_admission: None,
            preparing_since_ms: None,
            yield_services: vec![],
            resume_pending: vec![],
            resume_error: None,
            resume_attempts: 0,
            cancel_requested: None,
            finish_hook_started_ms: None,
            finish_hook_outcome: None,
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

    /// Block, as a requester, until the workload has spawned or the job
    /// ended. A granted job is Running before its (non-exclusive) pre hook
    /// and spawn, so this waits for the recorded workload PID. A free ticket
    /// lock means its supervisor died: recover first.
    pub fn wait_started(&self, id: Uuid) -> Result<JobHandle> {
        let _requester = self.hold_as_requester(id)?;
        let events = StateEvents::new(&self.root)?;
        loop {
            let record = self.record(id)?;
            let job = record.job.context("ticket has no job")?;
            if record.workload_pid.is_some()
                || matches!(
                    job.state,
                    JobState::Finished { .. } | JobState::Cancelled { .. }
                )
            {
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
                return Ok(None);
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
            Ok(Some(record.resume_pending.clone()))
        })?;
        let Some(services) = services else {
            return Ok(());
        };
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
        self.start_finish_hook(id)
    }

    /// Claim an ended job's finish hook and start its detached runner. The
    /// claim is journalled first, so racing callers (the supervisor, a
    /// canceller, recovery) start it at most once; a job without a finish
    /// hook, or whose hook was already claimed, is left alone.
    fn start_finish_hook(&self, id: Uuid) -> Result<()> {
        let hook = self.locked(|state| {
            let Some(row) = state.records.iter_mut().find(|r| r.ticket.id == id) else {
                return Ok(None);
            };
            let ended = matches!(
                row.state,
                TicketState::Finished | TicketState::Cancelled { .. }
            );
            let Some(hook) = row.spec.as_ref().and_then(|spec| spec.finish_hook.clone()) else {
                return Ok(None);
            };
            if !ended || row.finish_hook_started_ms.is_some() {
                return Ok(None);
            }
            row.finish_hook_started_ms = Some(milliseconds());
            Ok(Some(hook))
        })?;
        let Some(hook) = hook else {
            return Ok(());
        };
        if let Err(error) = self.spawn_finish_hook(id, &hook) {
            self.set_finish_hook_outcome(id, format!("not started: {error:#}"))?;
        }
        Ok(())
    }

    /// The runner is `borg lane __finish_hook ID` in a session of its own
    /// and, with a user manager, a scope that systemd stops at the hook's
    /// deadline even if the runner itself is killed.
    fn spawn_finish_hook(&self, id: Uuid, hook: &Hook) -> Result<()> {
        if cfg!(test) {
            let store = self.clone();
            std::thread::spawn(move || store.run_finish_hook(id));
            return Ok(());
        }
        let executable = std::env::var_os("BORG_LANE_EXECUTABLE")
            .map(PathBuf::from)
            .unwrap_or(std::env::current_exe()?);
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
                .arg(format!("--unit=borg-lane-hook-{id}-finish.scope"))
                .args([
                    "-p",
                    &format!(
                        "RuntimeMaxSec={}ms",
                        hook.timeout_ms.saturating_add(FINISH_HOOK_GRACE_MS)
                    ),
                    "--",
                ])
                .arg(&executable);
            command
        } else {
            Command::new(&executable)
        };
        command
            .args(["lane", "--state-dir"])
            .arg(&self.root)
            .arg("__finish_hook")
            .arg(id.to_string())
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
        let mut child = command.spawn().context("starting the finish hook runner")?;
        // Reap it without making a long-lived caller wait for the hook.
        std::thread::spawn(move || child.wait());
        Ok(())
    }

    /// Run an ended job's claimed finish hook, bounded by its timeout, and
    /// journal how it ended. Nothing it started outlives it.
    pub fn run_finish_hook(&self, id: Uuid) -> Result<()> {
        let record = self.record(id)?;
        let spec = record.spec.as_ref().context("job spec missing")?;
        let hook = spec
            .finish_hook
            .as_ref()
            .context("job has no finish hook")?;
        ensure!(
            record.finish_hook_started_ms.is_some() && record.finish_hook_outcome.is_none(),
            "finish hook of job {id} is not claimed or already ran"
        );
        let end = match record.job.as_ref().map(|job| &job.state) {
            Some(JobState::Finished { exit_code }) => JobEnd::Exited(*exit_code),
            Some(JobState::Cancelled { reason }) => JobEnd::Cancelled(reason.clone()),
            _ => bail!("job {id} has not ended"),
        };
        let outcome = self
            .run_bounded_hook(
                hook,
                spec,
                id,
                &end.hook_env(record.evidence.as_deref().unwrap_or_default()),
            )
            .unwrap_or_else(|error| format!("failed: {error:#}"));
        self.set_finish_hook_outcome(id, outcome)
    }

    fn run_bounded_hook(
        &self,
        hook: &Hook,
        spec: &JobSpec,
        id: Uuid,
        end: &[(&str, String)],
    ) -> Result<String> {
        ensure!(!hook.argv.is_empty(), "empty finish hook");
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.job_dir(id).join("finish-hook.log"))?;
        let mut command = Command::new(&hook.argv[0]);
        command.args(&hook.argv[1..]);
        self.hook_env(&mut command, spec, id, "finish", end);
        use std::os::unix::process::CommandExt;
        let mut child = command
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?;
        let group = child.id();
        let started = Instant::now();
        let outcome = loop {
            if let Some(status) = child.try_wait()? {
                break match (status.code(), status.signal()) {
                    (Some(code), _) => format!("exited {code}"),
                    (None, signal) => format!("killed by signal {}", signal.unwrap_or(0)),
                };
            }
            if started.elapsed() > Duration::from_millis(hook.timeout_ms) {
                unsafe {
                    libc::kill(-(group as i32), libc::SIGKILL);
                }
                let _ = child.wait();
                break format!("timed out after {}ms", hook.timeout_ms);
            }
            std::thread::sleep(Duration::from_millis(30));
        };
        if !process_group_pids(group).is_empty() {
            unsafe {
                libc::kill(-(group as i32), libc::SIGKILL);
            }
        }
        Ok(outcome)
    }

    fn set_finish_hook_outcome(&self, id: Uuid, outcome: String) -> Result<()> {
        self.locked(|state| {
            if let Some(row) = state.records.iter_mut().find(|r| r.ticket.id == id) {
                row.finish_hook_outcome = Some(outcome);
            }
            Ok(())
        })
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
            matches!(
                record.state,
                TicketState::Finished | TicketState::Cancelled { .. }
            ),
            "cannot resume service before job ends"
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
        let ended = self.locked(|state| {
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
                    Ok(true)
                }
                TicketState::Preparing | TicketState::Granted(_) if record.job.is_some() => {
                    if record.cancel_requested.is_none() {
                        record.cancel_requested = Some(reason.to_owned());
                        // A running supervisor watches this file, not the journal.
                        fs::write(self.job_dir(id).join(CANCEL_MARKER), reason)?;
                    }
                    Ok(false)
                }
                _ => bail!("cannot cancel a completed ticket or a service lease"),
            }
        })?;
        if ended {
            self.start_finish_hook(id)?;
        }
        Ok(())
    }
}

/// The file in a job's directory whose presence (holding the reason) asks
/// its running supervisor to cancel it.
const CANCEL_MARKER: &str = "cancel";

/// Finished and cancelled records are kept for this long by default
/// (`BORG_LANE_RETAIN_SECONDS`)...
const RETAIN_SECONDS: u64 = 86_400;
/// ...and at most this many of the newest are kept regardless of age.
const RETAIN_RECORDS: usize = 2000;

/// The evidence label for the processes `recover` killed.
const RECOVER_KILLED: &str = "recover killed pids";

/// The cancel reason when no requester is left (`abandon_after_ms`).
const ABANDONED: &str = "abandoned: no requester";

/// How a supervised job ended.
enum JobEnd {
    Exited(i32),
    Cancelled(String),
}

impl JobEnd {
    /// What post and finish hooks learn about the ending: its state, the
    /// exit code of a finished job and the reason (a cancelled job's
    /// reason, otherwise the supervisor's note).
    fn hook_env(&self, note: &str) -> Vec<(&'static str, String)> {
        match self {
            JobEnd::Exited(code) => vec![
                ("BORG_LANE_STATE", "finished".to_owned()),
                ("BORG_LANE_EXIT_CODE", code.to_string()),
                ("BORG_LANE_REASON", note.to_owned()),
            ],
            JobEnd::Cancelled(reason) => vec![
                ("BORG_LANE_STATE", "cancelled".to_owned()),
                ("BORG_LANE_REASON", reason.clone()),
            ],
        }
    }
}

/// How long a finish hook's scope may outlive its timeout before systemd
/// stops it, in case its runner was killed.
const FINISH_HOOK_GRACE_MS: u64 = 30_000;

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

/// Finished or cancelled records past retention: older than `max_age_ms`
/// or beyond the newest `keep` terminal records. Quarantined records, those
/// with a service resume pending and those whose finish hook is still due
/// or running are kept.
fn expired_records(state: &Journal, now_ms: u64, max_age_ms: u64, keep: usize) -> HashSet<Uuid> {
    let ended_ms = |r: &LaneRecord| r.finished_ms.unwrap_or(r.created_ms);
    let mut terminal: Vec<&LaneRecord> = state
        .records
        .iter()
        .filter(|r| {
            matches!(
                r.state,
                TicketState::Finished | TicketState::Cancelled { .. }
            )
        })
        .collect();
    terminal.sort_by_key(|r| std::cmp::Reverse((ended_ms(r), r.ticket.sequence)));
    terminal
        .into_iter()
        .enumerate()
        .filter(|(rank, r)| *rank >= keep || now_ms.saturating_sub(ended_ms(r)) > max_age_ms)
        .filter(|(_, r)| {
            !r.quarantined
                && r.resume_pending.is_empty()
                && !finish_hook_unclaimed(r)
                && !finish_hook_running(r, now_ms)
        })
        .map(|(_, r)| r.ticket.id)
        .collect()
}

/// A claimed finish hook with no outcome yet, within its time limit.
fn finish_hook_running(row: &LaneRecord, now_ms: u64) -> bool {
    let (Some(started), None, Some(hook)) = (
        row.finish_hook_started_ms,
        &row.finish_hook_outcome,
        row.spec.as_ref().and_then(|spec| spec.finish_hook.as_ref()),
    ) else {
        return false;
    };
    now_ms.saturating_sub(started) < hook.timeout_ms.saturating_add(FINISH_HOOK_GRACE_MS)
}

/// An ended job whose finish hook was never claimed.
fn finish_hook_unclaimed(row: &LaneRecord) -> bool {
    matches!(
        row.state,
        TicketState::Finished | TicketState::Cancelled { .. }
    ) && row.finish_hook_started_ms.is_none()
        && row
            .spec
            .as_ref()
            .is_some_and(|spec| spec.finish_hook.is_some())
}

/// A job that ended either way (Finished or Cancelled) returns the services
/// that yielded to it.
fn resume_is_pending(row: &LaneRecord) -> bool {
    matches!(
        row.state,
        TicketState::Finished | TicketState::Cancelled { .. }
    ) && !row.resume_pending.is_empty()
        && !row.quarantined
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

/// A resource key for status reasons: its name and scope.
pub(crate) fn key_label(key: &ResourceKey) -> String {
    match &key.scope {
        ResourceScope::Host => format!("{} (host)", key.name),
        ResourceScope::Project(path) => format!("{} (project {})", key.name, path.display()),
        ResourceScope::Worktree(path) => format!("{} (worktree {})", key.name, path.display()),
    }
}

/// Why the service supervisor owning a service lease or restart barrier,
/// and every backend it started, are certainly gone; None while they might
/// still run. With a recorded service unit cgroup (the supervisor and its
/// backends all run in it), the cgroup must be gone or empty. Without one,
/// the supervisor pid must be gone or name another process: a different
/// start time, or, for a claim recorded without one (an older binary), a
/// process that is not a supervisor of the claim's service.
fn claim_owner_gone(row: &LaneRecord) -> Option<String> {
    if let Some(cgroup) = &row.owner_cgroup {
        return (!cgroup_populated(cgroup)).then(|| format!("unit cgroup {cgroup} is empty"));
    }
    let pid = row.supervisor_pid.or(row.request.holder.host_pid)?;
    let Some(ticks) = proc_start_ticks(pid) else {
        return Some(format!("supervisor pid {pid} has exited"));
    };
    match row.owner_start_ticks {
        Some(recorded) => (recorded != ticks).then(|| format!("pid {pid} is now another process")),
        None => {
            let purpose = &row.request.holder.purpose;
            let service = purpose
                .strip_prefix("restart:")
                .or_else(|| purpose.strip_prefix("service:"))
                .unwrap_or_default();
            let cmdline = fs::read(format!("/proc/{pid}/cmdline")).unwrap_or_default();
            let args: Vec<&[u8]> = cmdline.split(|byte| *byte == 0).collect();
            let supervisor =
                args.contains(&b"supervise".as_slice()) && args.contains(&service.as_bytes());
            (!supervisor).then(|| format!("pid {pid} is not a supervisor of {service}"))
        }
    }
}

/// The cgroup of `pid` when it is a service supervisor's own systemd unit
/// (`borg-service-*.service`); None for any other process.
fn service_unit_cgroup(pid: u32) -> Option<String> {
    let text = fs::read_to_string(format!("/proc/{pid}/cgroup")).ok()?;
    let path = text.lines().find_map(|line| line.strip_prefix("0::"))?;
    let unit = path.rsplit('/').next()?;
    (unit.starts_with("borg-service-") && unit.ends_with(".service")).then(|| path.to_owned())
}

/// Whether a cgroup v2 group, or any group below it, still has a process.
/// A group that no longer exists is empty; one that cannot be read counts
/// as populated.
fn cgroup_populated(path: &str) -> bool {
    let events = Path::new("/sys/fs/cgroup")
        .join(path.trim_start_matches('/'))
        .join("cgroup.events");
    match fs::read_to_string(events) {
        Ok(text) => !text.lines().any(|line| line.trim() == "populated 0"),
        Err(error) => error.kind() != std::io::ErrorKind::NotFound,
    }
}

/// The outcome of `LaneStore::try_acquire_restart_barrier`.
#[derive(Debug)]
pub(crate) enum RestartBarrier {
    /// Every key is reserved until this lease is released.
    Held(Lease),
    /// Why not: `<key> (<scope>) held by|queued for|quarantined by ticket <id>`.
    Deferred(String),
}

/// FIFO decision. A later ticket never overtakes an earlier conflicting
/// one, unless that earlier ticket is itself held back by a key the later
/// one does not request (busy, full or quarantined), so a build queued
/// behind its own busy tree does not hold another tree's build off a free
/// slot. An earlier ticket that could run keeps its turn. Disjoint keys
/// run concurrently, and all keys grant atomically.
fn dispatch_reason(state: &Journal, id: Uuid) -> Result<Option<String>> {
    let me = state
        .records
        .iter()
        .find(|r| r.ticket.id == id)
        .context("unknown ticket")?;
    if !matches!(me.state, TicketState::Queued) {
        bail!("ticket is not queued");
    }
    if let Some(quarantined) = me
        .request
        .resources
        .iter()
        .find_map(|requested| quarantined_by(state, requested))
    {
        return Ok(Some(format!(
            "resource quarantined after unverified orphan {}",
            quarantined.ticket.id
        )));
    }
    for earlier in state
        .records
        .iter()
        .filter(|r| r.ticket.sequence < me.ticket.sequence)
    {
        if !matches!(earlier.state, TicketState::Queued | TicketState::Preparing)
            || !earlier
                .request
                .resources
                .iter()
                .any(|a| me.request.resources.iter().any(|b| conflicts(a, b)))
        {
            continue;
        }
        let held_back = earlier
            .request
            .resources
            .iter()
            .filter(|theirs| {
                !me.request
                    .resources
                    .iter()
                    .any(|mine| conflicts(theirs, mine))
            })
            .any(|theirs| {
                quarantined_by(state, theirs).is_some()
                    || key_reason(state, earlier, theirs).is_some()
            });
        if !held_back {
            return Ok(Some(format!("FIFO ticket {} ahead", earlier.ticket.id)));
        }
    }
    Ok(me
        .request
        .resources
        .iter()
        .find_map(|requested| key_reason(state, me, requested)))
}

/// The quarantined record that holds `requested`'s key, if any.
fn quarantined_by<'a>(state: &'a Journal, requested: &ResourceRequest) -> Option<&'a LaneRecord> {
    state.records.iter().find(|r| {
        r.quarantined
            && r.request
                .resources
                .iter()
                .any(|held| conflicts(held, requested))
    })
}

/// Why `ticket` cannot take `requested` beside the current Granted and
/// Preparing holders (other than itself); None when it fits. A job may
/// claim an exclusive key held only by granted service leases: it enters
/// Preparing and has them yield.
fn key_reason(state: &Journal, ticket: &LaneRecord, requested: &ResourceRequest) -> Option<String> {
    let holders: Vec<&LaneRecord> = state
        .records
        .iter()
        .filter(|r| r.ticket.id != ticket.ticket.id)
        .filter(|r| matches!(r.state, TicketState::Granted(_) | TicketState::Preparing))
        .filter(|r| {
            r.request
                .resources
                .iter()
                .any(|held| conflicts(held, requested))
        })
        .collect();
    let preparable_service_holder = ticket.spec.is_some()
        && matches!(requested.access, Access::Exclusive)
        && holders
            .iter()
            .all(|r| r.service_lease && matches!(r.state, TicketState::Granted(_)));
    if preparable_service_holder {
        return None;
    }
    let slots = capacity(&state.capacities, &requested.key);
    let used = holders
        .iter()
        .flat_map(|r| &r.request.resources)
        .filter(|held| conflicts(held, requested))
        .fold(0_u32, |n, held| {
            n.saturating_add(match held.access {
                Access::Exclusive => slots,
                Access::Shared { slots } => slots,
            })
        });
    if used > 0 && matches!(requested.access, Access::Exclusive) {
        return Some(format!("exclusive resource {} busy", requested.key.name));
    }
    let wanted = match requested.access {
        Access::Shared { slots } => slots,
        Access::Exclusive => slots,
    };
    (used.saturating_add(wanted) > slots)
        .then(|| format!("resource {} capacity {used}/{slots}", requested.key.name))
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
    let active: Vec<_> = state
        .records
        .iter()
        .filter(|r| matches!(r.state, TicketState::Granted(_) | TicketState::Preparing))
        .filter_map(|r| {
            r.spec
                .as_ref()
                .map(|s| &s.admission)
                .or(r.service_admission.as_ref())
                .map(|b| (r, b))
        })
        .collect();
    let reserved_ram = active
        .iter()
        .map(|(row, b)| {
            // MemAvailable already excludes resident memory. Reserve only future
            // growth, and never credit a shared/ancestor cgroup to two owners.
            let scope = row.scope_cgroup.as_ref().or(row.owner_cgroup.as_ref());
            let exclusive = scope.filter(|scope| {
                !active.iter().any(|(other, _)| {
                    other.ticket.id != row.ticket.id
                        && other
                            .scope_cgroup
                            .as_ref()
                            .or(other.owner_cgroup.as_ref())
                            .is_some_and(|s| {
                                s == *scope
                                    || s.starts_with(&format!("{scope}/"))
                                    || scope.starts_with(&format!("{s}/"))
                            })
                })
            });
            let resident = exclusive
                .and_then(|scope| {
                    fs::read_to_string(
                        Path::new("/sys/fs/cgroup")
                            .join(scope.trim_start_matches('/'))
                            .join("memory.stat"),
                    )
                    .ok()?
                    .lines()
                    // Reclaimable file cache can still be in MemAvailable;
                    // crediting memory.current would count those bytes twice.
                    .filter_map(|line| {
                        let (name, bytes) = line.split_once(' ')?;
                        matches!(name, "anon" | "shmem")
                            .then(|| bytes.parse::<u64>().ok())
                            .flatten()
                    })
                    .reduce(u64::saturating_add)
                })
                .unwrap_or(0);
            b.reserve_ram_bytes.saturating_sub(resident)
        })
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

/// CPU seconds of `pid`, plus those of the children it has reaped when
/// `with_children`.
fn proc_cpu(pid: u32, with_children: bool) -> Option<f64> {
    let text = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let tail = text
        .rsplit_once(") ")?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    // utime, stime, cutime, cstime
    let fields = if with_children { 11..15 } else { 11..13 };
    let mut ticks = 0_u64;
    for field in fields {
        ticks = ticks.saturating_add(tail.get(field)?.parse().ok()?);
    }
    Some(ticks as f64 / unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.max(1) as f64)
}

/// CPU seconds a job's workload has used so far. Scoped: its scope's
/// cgroup, which counts every process it started, exited ones included.
/// Unscoped: the live members of its own process group, each with the
/// children it has reaped. A scoped job whose cgroup is unknown falls
/// back to its leader alone.
fn workload_cpu(cgroup: Option<&str>, scoped: bool, leader: u32) -> f64 {
    if let Some(usec) = cgroup.and_then(cgroup_cpu_usec) {
        return usec as f64 / 1_000_000.0;
    }
    if scoped {
        return proc_cpu(leader, false).unwrap_or(0.0);
    }
    process_group_pids(leader)
        .into_iter()
        .filter_map(|pid| proc_cpu(pid, true))
        .sum()
}

/// `usage_usec` from a cgroup v2 group's cpu.stat.
fn cgroup_cpu_usec(path: &str) -> Option<u64> {
    fs::read_to_string(
        Path::new("/sys/fs/cgroup")
            .join(path.trim_start_matches('/'))
            .join("cpu.stat"),
    )
    .ok()?
    .lines()
    .find_map(|line| line.strip_prefix("usage_usec "))?
    .trim()
    .parse()
    .ok()
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
                    && let Err(error) = self.run_hook(hook, &spec, id, "pre-exclusive", true, &[])
                {
                    // Yield may have partially succeeded. Resume the service
                    // through the post hook even though no lease was granted.
                    if let Some(post) = &spec.post_hook {
                        let end = self.failed_end(id)?;
                        let note = format!("pre hook failed: {error:#}");
                        let _ = self.run_hook(
                            post,
                            &spec,
                            id,
                            "post-exclusive",
                            false,
                            &end.hook_env(&note),
                        );
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
                if let Err(error) = &granted
                    && let Some(post) = &spec.post_hook
                {
                    let end = self.failed_end(id)?;
                    let _ = self.run_hook(
                        post,
                        &spec,
                        id,
                        "post-exclusive",
                        false,
                        &end.hook_env(&format!("{error:#}")),
                    );
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

    /// The environment every hook gets, plus how the job ended (`end`) for
    /// post and finish hooks.
    fn hook_env(
        &self,
        command: &mut Command,
        spec: &JobSpec,
        id: Uuid,
        phase: &str,
        end: &[(&str, String)],
    ) {
        command
            .current_dir(&spec.cwd)
            .env("BORG_LANE_JOB", id.to_string())
            .env("BORG_LANE_PHASE", phase)
            .env(
                "BORG_LANE_RESOURCE",
                spec.lease.resources.first().map_or("", |r| &r.key.name),
            )
            .env("BORG_LANE_LOG", self.job_dir(id).join("output.log"))
            .env("BORG_LANES_ROOT", &self.root)
            .envs(end.iter().map(|(key, value)| (*key, value)));
    }

    fn run_hook(
        &self,
        hook: &Hook,
        spec: &JobSpec,
        id: Uuid,
        phase: &str,
        sync: bool,
        end: &[(&str, String)],
    ) -> Result<()> {
        ensure!(!hook.argv.is_empty(), "empty {phase} hook");
        let mut command = Command::new(&hook.argv[0]);
        command.args(&hook.argv[1..]);
        self.hook_env(&mut command, spec, id, phase, end);
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
            self.hook_env(&mut command, spec, id, phase, end);
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
            (Ok((JobEnd::Exited(code), note)), _) => (JobEnd::Exited(code), note),
            (Ok((JobEnd::Cancelled(reason), _)), _) => {
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

    /// The job's end and the supervisor's note on it: `finished`, `timeout`
    /// or the stall.
    fn supervise_job(&self, id: Uuid) -> Result<(JobEnd, String)> {
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
            self.run_hook(pre, &spec, id, "pre", true, &[])?;
        }
        if let Some(reason) = self.record(id)?.cancel_requested {
            let end = JobEnd::Cancelled(reason);
            let note = "cancelled before start".to_owned();
            self.run_post_for_job(&spec, id, exclusive, &end, &note)?;
            return Ok((end, note));
        }
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.job_dir(id).join("output.log"))?;
        let stderr = log.try_clone()?;
        let unit = spec.scope_unit(id);
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
            if let Some(max) = spec.memory_swap_max_bytes {
                command.args(["-p", &format!("MemorySwapMax={max}")]);
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
        let cpu_cgroup = group.clone();
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
        // CPU at the last progress mark: a stall is no log growth and under
        // 50 ms of CPU across the whole workload since then.
        let mut progress_cpu = 0.0;
        // Progress reaches the journal at most once a second and only when
        // it changed; otherwise the loop neither locks nor reads the journal
        // and learns of a cancel from its marker file.
        let mut journalled: Option<(u64, f64)> = None;
        let mut journalled_at: Option<Instant> = None;
        let mut latest = (0, 0.0);
        let cancel_marker = self.job_dir(id).join(CANCEL_MARKER);
        let leader = child.id();
        let (end, note) = loop {
            if let Some(status) = child.try_wait()? {
                let code = status.code().unwrap_or(128 + status.signal().unwrap_or(9));
                break (JobEnd::Exited(code), "finished".to_owned());
            }
            let size = fs::metadata(self.job_dir(id).join("output.log")).map_or(0, |m| m.len());
            let cpu = workload_cpu(cpu_cgroup.as_deref(), scoped, leader);
            if size != last_size || cpu > progress_cpu + 0.05 {
                last_progress = std::time::Instant::now();
                progress_cpu = cpu;
            }
            // Exited unreaped processes drop out of an unscoped sum.
            progress_cpu = progress_cpu.min(cpu);
            last_size = size;
            if self.abandoned(&spec, id, &mut last_requester_ms) {
                self.cancel_ticket(id, ABANDONED)?;
            }
            let due = journalled_at.is_none_or(|at| at.elapsed() >= Duration::from_secs(1));
            // Journalled at the 10 ms resolution of /proc CPU ticks, so the
            // microsecond cgroup counter of an idle job does not rewrite it.
            let cpu = (cpu * 100.0).round() / 100.0;
            latest = (size, cpu);
            if due && journalled != Some(latest) {
                self.locked(|state| {
                    let record = state
                        .records
                        .iter_mut()
                        .find(|r| r.ticket.id == id)
                        .context("job vanished")?;
                    record.progress = Some(format!("{size} bytes"));
                    record.cpu_seconds = Some(cpu);
                    Ok(())
                })?;
                journalled = Some((size, cpu));
                journalled_at = Some(Instant::now());
            }
            let cancel = fs::read_to_string(&cancel_marker).ok();
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
                    Some(cancelled) => (JobEnd::Cancelled(cancelled), reason),
                    None if timed_out => (JobEnd::Exited(124), reason),
                    None => (JobEnd::Exited(125), reason),
                };
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        // The last reading while the workload lived (an unscoped group is
        // gone once its leader is reaped).
        if journalled != Some(latest) {
            self.locked(|state| {
                if let Some(record) = state.records.iter_mut().find(|r| r.ticket.id == id) {
                    record.progress = Some(format!("{} bytes", latest.0));
                    record.cpu_seconds = Some(latest.1);
                }
                Ok(())
            })?;
        }
        // The leader is gone; no process of this workload may outlive it into
        // the next holder's lease (a compiler it started, for example).
        self.kill_workload(id, &unit, scoped, leader, "killed leftover pids")?;
        self.run_post_for_job(&spec, id, exclusive, &end, &note)?;
        let _ = lease;
        Ok((end, note))
    }

    /// How a job that failed before its workload ran will end: cancelled
    /// when that was requested, otherwise finished 125.
    fn failed_end(&self, id: Uuid) -> Result<JobEnd> {
        Ok(match self.record(id)?.cancel_requested {
            Some(reason) => JobEnd::Cancelled(reason),
            None => JobEnd::Exited(125),
        })
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

    fn run_post_for_job(
        &self,
        spec: &JobSpec,
        id: Uuid,
        exclusive: bool,
        end: &JobEnd,
        note: &str,
    ) -> Result<()> {
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
                &end.hook_env(note),
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
        let released = if dry_run {
            self.reading(|state| self.release_dead_service_claims(&mut state.clone(), true))?
        } else {
            self.locked(|state| self.release_dead_service_claims(state, false))?
        };
        let releasable: Vec<Uuid> = released.iter().map(|(id, _)| *id).collect();
        actions.extend(released.into_iter().map(|(_, action)| action));
        // Avoid nested metadata locks: test each kernel lock before journalling.
        for record in self.snapshot()? {
            // A job that ended without starting its finish hook (its ender
            // died in between) gets it now; the claim keeps it to once.
            if finish_hook_unclaimed(&record) {
                let id = record.ticket.id;
                if dry_run {
                    actions.push(format!("job {id}: would start its finish hook"));
                } else {
                    actions.push(format!("job {id}: starting its finish hook"));
                    self.start_finish_hook(id)?;
                }
                continue;
            }
            if !matches!(
                record.state,
                TicketState::Granted(_) | TicketState::Queued | TicketState::Preparing
            ) || (record.job.is_none() && !record.service_lease && !record.restart_barrier)
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
            if releasable.contains(&record.ticket.id) {
                continue; // A dry run: the release above would end it.
            }
            let note = format!(
                "{} {} lost supervisor pid {:?} while {:?}",
                if record.restart_barrier {
                    "restart barrier"
                } else {
                    "job"
                },
                record.ticket.id,
                record.supervisor_pid,
                record.state
            );
            actions.push(note.clone());
            if dry_run {
                continue;
            }
            // A service claim whose owner is not provably gone (those were
            // released above) may still have a backend running with nobody
            // to stop it: quarantine its keys until a later recover, or the
            // next claim, can prove the owner gone and lift it.
            let mut verified = !record.service_lease && !record.restart_barrier;
            if let Some(JobHandle {
                state: JobState::Running { scope },
                ..
            }) = &record.job
            {
                if !scope.is_empty() {
                    // The unit this job's own spec names, not any lookalike.
                    let expected = record.spec.as_ref().map_or_else(
                        || format!("borg-lane-{}.scope", record.ticket.id),
                        |spec| spec.scope_unit(record.ticket.id),
                    );
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
                        // Kills the verified scope, waits for it to empty
                        // and records the killed PIDs (quarantine if not).
                        self.kill_workload(record.ticket.id, scope, true, 0, RECOVER_KILLED)?;
                    }
                } else if let (Some(pid), Some(ticks)) =
                    (record.workload_pid, record.workload_start_ticks)
                {
                    if proc_start_ticks(pid) == Some(ticks) {
                        self.kill_workload(record.ticket.id, "", false, pid, RECOVER_KILLED)?;
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
            restart_barrier: false,
            owner_start_ticks: None,
            owner_cgroup: None,
            service_admission: None,
            preparing_since_ms: None,
            yield_services: vec![],
            resume_pending: vec![],
            resume_error: None,
            resume_attempts: 0,
            cancel_requested: None,
            finish_hook_started_ms: None,
            finish_hook_outcome: None,
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
    /// Failure mode: a build queued behind its busy tree holding back every
    /// later build on another tree that fits a free slot; or that relief
    /// letting a later build take the slot an earlier eligible one is due,
    /// or two exclusive claims on one key.
    #[test]
    fn fifo_blocked_tickets_do_not_hold_back_later_tickets_on_free_keys() {
        use TicketState::{Preparing, Queued};
        let build = |tree: &str| {
            vec![
                resource(tree, Access::Exclusive),
                resource("slots", Access::Shared { slots: 1 }),
            ]
        };
        let journal = |records| Journal {
            client_revision: 0,
            sequence: 9,
            capacities: vec![Capacity {
                key: key("slots"),
                slots: 2,
            }],
            records,
        };
        let reason = |state: &Journal, seq: u128| {
            dispatch_reason(state, Uuid::from_u128(seq))
                .unwrap()
                .unwrap_or_default()
        };
        // running(A), pending(A), pending(C): C takes the free slot.
        let state = journal(vec![
            granted(1, build("a")),
            record(2, Queued, build("a")),
            record(3, Queued, build("c")),
        ]);
        assert!(reason(&state, 2).contains("busy"));
        assert_eq!(reason(&state, 3), "");
        // running(A), pending(B), pending(C): B is due the free slot first.
        let state = journal(vec![
            granted(1, build("a")),
            record(2, Queued, build("b")),
            record(3, Queued, build("c")),
        ]);
        assert_eq!(reason(&state, 2), "");
        assert!(reason(&state, 3).contains("FIFO"));
        // running(A), pending(A), pending(A): the third waits.
        let state = journal(vec![
            granted(1, build("a")),
            record(2, Queued, build("a")),
            record(3, Queued, build("a")),
        ]);
        assert!(reason(&state, 3).contains("FIFO"));
        // A tree quarantined by an unverified orphan relieves the same way.
        let mut orphan = record(
            1,
            TicketState::Finished,
            vec![resource("a", Access::Exclusive)],
        );
        orphan.quarantined = true;
        let state = journal(vec![
            orphan,
            record(2, Queued, build("a")),
            record(3, Queued, build("c")),
        ]);
        assert!(reason(&state, 2).contains("quarantined"));
        assert_eq!(reason(&state, 3), "");
        // Once its tree is free, the overtaken build is due the next slot.
        let state = journal(vec![
            granted(3, build("c")),
            record(2, Queued, build("a")),
            record(4, Queued, build("d")),
        ]);
        assert_eq!(reason(&state, 2), "");
        assert!(reason(&state, 4).contains("FIFO"));
        // A key both request never relieves: a writer still is not starved.
        let state = journal(vec![
            granted(1, vec![resource("slots", Access::Shared { slots: 1 })]),
            record(2, Queued, vec![resource("slots", Access::Exclusive)]),
            record(
                3,
                Queued,
                vec![resource("slots", Access::Shared { slots: 1 })],
            ),
        ]);
        assert!(reason(&state, 3).contains("FIFO"));
        // A later job that overtook on the editor and is still Preparing (its
        // services yielding) keeps the earlier job off that exclusive key.
        let job = |seq: u64, state: TicketState, tree: &str| {
            let mut row = record(
                seq,
                state,
                vec![
                    resource("editor", Access::Exclusive),
                    resource(tree, Access::Exclusive),
                ],
            );
            row.spec = Some(coalescing_spec(Path::new("/"), None));
            row
        };
        let state = journal(vec![job(2, Queued, "a"), job(3, Preparing, "b")]);
        assert!(reason(&state, 2).contains("editor busy"));
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
            unit_prefix: None,
            finish_hook: None,
            memory_swap_max_bytes: None,
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
        store
            .run_post_for_job(&spec, id, true, &JobEnd::Exited(0), "finished")
            .unwrap();
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

    /// Failure mode: a journal that grows forever, or retention that drops a
    /// record still needed (a quarantine, a pending service resume, a
    /// finish hook not yet run, a live ticket).
    #[test]
    fn retention_expires_only_settled_terminal_records() {
        let day = 86_400_000;
        let now = 10 * day;
        let ended = |seq: u64, age: u64| {
            let mut row = record(seq, TicketState::Finished, vec![]);
            row.finished_ms = Some(now - age);
            row
        };
        let with_hook = |mut row: LaneRecord| {
            row.spec = Some(JobSpec {
                finish_hook: Some(Hook {
                    argv: vec!["true".into()],
                    timeout_ms: 1000,
                }),
                ..coalescing_spec(Path::new("/"), None)
            });
            row
        };
        let mut quarantined = ended(2, 2 * day);
        quarantined.quarantined = true;
        let mut resuming = ended(3, 2 * day);
        resuming.resume_pending.push("editor".into());
        let unclaimed = with_hook(ended(4, 2 * day));
        let mut hook_running = with_hook(ended(5, 2 * day));
        hook_running.finish_hook_started_ms = Some(now - 500);
        let mut hook_lost = with_hook(ended(6, 2 * day));
        hook_lost.finish_hook_started_ms = Some(now - day);
        let mut hook_done = with_hook(ended(7, 2 * day));
        hook_done.finish_hook_started_ms = Some(now - day);
        hook_done.finish_hook_outcome = Some("exited 0".into());
        let mut cancelled = ended(8, 2 * day);
        cancelled.state = TicketState::Cancelled {
            reason: "stop".into(),
        };
        let mut queued = record(9, TicketState::Queued, vec![]);
        queued.created_ms = now - 2 * day;
        let state = Journal {
            records: vec![
                ended(1, 2 * day),
                quarantined,
                resuming,
                unclaimed,
                hook_running,
                hook_lost,
                hook_done,
                cancelled,
                queued,
                ended(10, 1000),
            ],
            ..Journal::default()
        };
        let ids =
            |seqs: &[u128]| -> HashSet<Uuid> { seqs.iter().map(|s| Uuid::from_u128(*s)).collect() };
        assert_eq!(expired_records(&state, now, day, 2000), ids(&[1, 6, 7, 8]));
        // Past the newest `keep` terminal records, age no longer matters.
        let recent = Journal {
            records: (1..=5).map(|seq| ended(seq, 1000 * (10 - seq))).collect(),
            ..Journal::default()
        };
        assert_eq!(expired_records(&recent, now, day, 3), ids(&[1, 2]));
        assert!(expired_records(&recent, now, day, 5).is_empty());
    }

    /// Failure mode: a finish hook (the caller's cleanup) skipped on some
    /// ending (a queued cancel, a recovery), run twice when two callers end
    /// the same job, told the wrong outcome, or left running past its limit.
    #[test]
    fn finish_hook_runs_once_whichever_way_the_job_ends() {
        let dir = tempfile::tempdir().unwrap();
        let store = LaneStore::new(dir.path()).unwrap();
        let submit = |name: &str, script: &str, timeout_ms: u64| {
            let out = dir.path().join(format!("{name}.out"));
            let spec = JobSpec {
                argv: vec![format!("run-{name}")],
                finish_hook: Some(Hook {
                    argv: vec![
                        "sh".into(),
                        "-c".into(),
                        format!("{{ {script}; }} >> '{}'", out.display()),
                    ],
                    timeout_ms,
                }),
                ..coalescing_spec(dir.path(), None)
            };
            let id = store
                .locked(|state| {
                    Ok(store
                        .enqueue_record(state, spec.lease.clone(), Some(spec))?
                        .0
                        .id)
                })
                .unwrap();
            (id, out)
        };
        let outcome = |id: Uuid| {
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                if let Some(outcome) = store.record(id).unwrap().finish_hook_outcome {
                    return outcome;
                }
                assert!(Instant::now() < deadline, "finish hook of {id} never ended");
                std::thread::sleep(Duration::from_millis(20));
            }
        };
        let lines = |path: &Path| -> Vec<String> {
            fs::read_to_string(path)
                .unwrap_or_default()
                .lines()
                .map(str::to_owned)
                .collect()
        };
        let report = r#"echo "$BORG_LANE_PHASE $BORG_LANE_STATE ${BORG_LANE_EXIT_CODE:-none} $BORG_LANE_REASON""#;

        // A queued cancel; queue timeouts and abandonment end the same way.
        let (queued, queued_out) = submit("queued", report, 5_000);
        store.cancel_ticket(queued, "stop").unwrap();
        assert_eq!(outcome(queued), "exited 0");

        // The supervisor's ending, then a late second ender.
        let (ended, ended_out) = submit("ended", report, 5_000);
        store
            .locked(|state| {
                grant_entry(
                    state
                        .records
                        .iter_mut()
                        .find(|r| r.ticket.id == ended)
                        .unwrap(),
                );
                Ok(())
            })
            .unwrap();
        store
            .conclude(ended, JobEnd::Exited(3), "finished")
            .unwrap();
        store.finish(ended, 125, "late").unwrap();
        assert_eq!(outcome(ended), "exited 0");

        // Its ender died before starting the hook: recovery starts it.
        let (orphan, orphan_out) = submit("orphan", report, 5_000);
        store
            .locked(|state| {
                let row = state
                    .records
                    .iter_mut()
                    .find(|r| r.ticket.id == orphan)
                    .unwrap();
                row.state = TicketState::Finished;
                row.job.as_mut().unwrap().state = JobState::Finished { exit_code: 0 };
                row.evidence = Some("finished".into());
                Ok(())
            })
            .unwrap();
        let planned = store.recover(true).unwrap();
        assert!(
            planned
                .iter()
                .any(|action| action.contains("would start its finish hook")),
            "{planned:?}"
        );
        assert!(
            store
                .record(orphan)
                .unwrap()
                .finish_hook_started_ms
                .is_none()
        );
        store.recover(false).unwrap();
        assert_eq!(outcome(orphan), "exited 0");

        // Enders racing while the hook still runs start it once.
        let (raced, raced_out) = submit("raced", "echo run; sleep 0.3", 5_000);
        store.cancel_ticket(raced, "stop").unwrap();
        store.start_finish_hook(raced).unwrap();
        store.recover(false).unwrap();
        assert_eq!(outcome(raced), "exited 0");
        std::thread::sleep(Duration::from_millis(400));
        assert_eq!(lines(&raced_out), ["run"]);

        // A hook past its timeout dies with what it started.
        let child = dir.path().join("child.pid");
        let (slow, _) = submit(
            "slow",
            &format!("sleep 30 & echo $! > '{}'; wait", child.display()),
            300,
        );
        let started = Instant::now();
        store.cancel_ticket(slow, "stop").unwrap();
        assert_eq!(outcome(slow), "timed out after 300ms");
        assert!(started.elapsed() < Duration::from_secs(5));
        let pid = fs::read_to_string(&child).unwrap().trim().to_owned();
        let deadline = Instant::now() + Duration::from_secs(5);
        while fs::read_to_string(format!("/proc/{pid}/stat"))
            .is_ok_and(|stat| !stat.contains(") Z "))
        {
            assert!(
                Instant::now() < deadline,
                "finish hook child {pid} outlived it"
            );
            std::thread::sleep(Duration::from_millis(20));
        }

        // Nothing starts any of them again, not even a racing ender.
        for id in [queued, ended, orphan] {
            store.start_finish_hook(id).unwrap();
        }
        store.recover(false).unwrap();
        assert!(store.cancel_ticket(queued, "again").is_err());
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(lines(&queued_out), ["finish cancelled none stop"]);
        assert_eq!(lines(&ended_out), ["finish finished 3 finished"]);
        assert_eq!(lines(&orphan_out), ["finish finished 0 finished"]);
    }

    /// Failure mode: services that yielded to a job stay stopped when the
    /// job ends Cancelled (a running cancel, or a queue timeout waiting for
    /// a foreign client) because only Finished jobs resumed them.
    #[test]
    fn services_yielded_to_a_cancelled_job_resume() {
        let dir = tempfile::tempdir().unwrap();
        let store = LaneStore::new(dir.path()).unwrap();
        let spec = coalescing_spec(dir.path(), None);
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
        store.cancel_ticket(id, "stop").unwrap();
        store
            .conclude(id, JobEnd::Cancelled("stop".into()), "cancelled: stop")
            .unwrap();
        assert_eq!(store.record(id).unwrap().resume_pending, ["test"]);
        fs::write(dir.path().join("fake-resume-ok"), b"ok").unwrap();
        let (_, outcomes) = store.recover_wait(Duration::from_secs(10)).unwrap();
        assert_eq!(outcomes.len(), 1, "cancelled job's resume was not started");
        assert_eq!(outcomes[0].outcome, "resumed");
        assert!(store.record(id).unwrap().resume_pending.is_empty());
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
    fn concurrent_workers_cannot_reserve_the_same_ram() {
        let dir = tempfile::tempdir().unwrap();
        let reserve = crate::workspace::hygiene::ram_available().unwrap() * 3 / 4;
        let budget = AdmissionBudget {
            reserve_ram_bytes: reserve,
            ..test_budget(dir.path())
        };
        let store = LaneStore::new(dir.path().join("state")).unwrap();
        let barrier = std::sync::Barrier::new(2);
        let admitted = std::thread::scope(|scope| {
            let workers: Vec<_> = ["unreal", "cargo"]
                .into_iter()
                .map(|id| {
                    let store = &store;
                    let budget = &budget;
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        store
                            .try_acquire_service(
                                LeaseRequest {
                                    resources: vec![resource(id, Access::Shared { slots: 1 })],
                                    holder: Holder {
                                        purpose: format!("service:{id}"),
                                        ..holder()
                                    },
                                    queue_timeout_ms: None,
                                },
                                budget,
                            )
                            .unwrap()
                    })
                })
                .collect();
            workers
                .into_iter()
                .filter_map(|w| w.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(
            admitted.len(),
            1,
            "independent resource keys still share host RAM"
        );
        store.release_lease(&admitted[0]).unwrap();
        assert!(
            store
                .reading(|s| budget_reason(s, &budget))
                .unwrap()
                .is_none()
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
            unit_prefix: None,
            finish_hook: None,
            memory_swap_max_bytes: None,
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
                unit_prefix: None,
                finish_hook: None,
                memory_swap_max_bytes: None,
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
            unit_prefix: None,
            finish_hook: None,
            memory_swap_max_bytes: None,
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
            unit_prefix: None,
            finish_hook: None,
            memory_swap_max_bytes: None,
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
            unit_prefix: None,
            finish_hook: None,
            memory_swap_max_bytes: None,
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
            unit_prefix: None,
            finish_hook: None,
            memory_swap_max_bytes: None,
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
            unit_prefix: None,
            finish_hook: None,
            memory_swap_max_bytes: None,
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

    /// Failure mode: a scope unit named from arbitrary input, or a custom
    /// prefix that recovery would not recognise as the job's own unit.
    #[test]
    fn unit_prefixes_are_validated_and_name_the_scope() {
        let dir = tempfile::tempdir().unwrap();
        let store = LaneStore::new(dir.path()).unwrap();
        let spec = coalescing_spec(dir.path(), None);
        let id = Uuid::nil();
        assert_eq!(spec.scope_unit(id), format!("borg-lane-{id}.scope"));
        for (prefix, valid) in [
            ("ab-build", true),
            ("a", true),
            ("a23456789012345678901234", true),
            ("a234567890123456789012345", false),
            ("Ab", false),
            ("1ab", false),
            ("ab_build", false),
            ("ab/build", false),
            ("", false),
        ] {
            let spec = JobSpec {
                unit_prefix: Some(prefix.into()),
                argv: vec![format!("run-{prefix}")],
                ..spec.clone()
            };
            let submitted = store.locked(|state| {
                store.enqueue_record(state, spec.lease.clone(), Some(spec.clone()))
            });
            assert_eq!(submitted.is_ok(), valid, "{prefix:?}");
        }
        let prefixed = JobSpec {
            unit_prefix: Some("ab-build".into()),
            ..spec
        };
        assert_eq!(prefixed.scope_unit(id), format!("ab-build-{id}.scope"));
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

    fn barrier_request(names: &[&str]) -> LeaseRequest {
        LeaseRequest {
            resources: names
                .iter()
                .map(|name| resource(name, Access::Exclusive))
                .collect(),
            holder: Holder {
                purpose: "restart:editor".into(),
                ..holder()
            },
            queue_timeout_ms: None,
        }
    }
    fn exclusive_lease(names: &[&str]) -> LeaseRequest {
        LeaseRequest {
            holder: holder(),
            ..barrier_request(names)
        }
    }
    fn held_barrier(outcome: RestartBarrier) -> Lease {
        match outcome {
            RestartBarrier::Held(lease) => lease,
            RestartBarrier::Deferred(reason) => panic!("restart barrier deferred: {reason}"),
        }
    }
    fn deferred_barrier(outcome: RestartBarrier) -> String {
        match outcome {
            RestartBarrier::Deferred(reason) => reason,
            RestartBarrier::Held(lease) => panic!("restart barrier granted: {lease:?}"),
        }
    }
    /// A queued job (it may prepare past a service lease) on `names`.
    fn queue_job(store: &LaneStore, dir: &Path, names: &[&str]) -> Uuid {
        let spec = JobSpec {
            foreign_client_grace_ms: default_foreign_client_grace_ms(),
            foreign_client_grace_by_resource: vec![],
            abandon_after_ms: None,
            unit_prefix: None,
            finish_hook: None,
            memory_swap_max_bytes: None,
            fingerprint: JobFingerprint(names.join("+")),
            lease: exclusive_lease(names),
            argv: vec!["true".into()],
            cwd: dir.into(),
            env: vec![],
            memory_max_bytes: None,
            admission: test_budget(dir),
            pre_hook: None,
            post_hook: None,
            timeout_ms: 2000,
            stall_timeout_ms: None,
            coalesce: false,
        };
        store
            .locked(|state| {
                Ok(store
                    .enqueue_record(state, spec.lease.clone(), Some(spec))?
                    .0
                    .id)
            })
            .unwrap()
    }

    /// Failure mode (C10): a service restart and a build of its tree both
    /// admitted, because the restart checked the build keys in one step and
    /// reserved nothing, or a queued build lost its turn to it.
    #[tokio::test]
    async fn restart_barrier_and_build_admission_never_interleave() {
        let dir = tempfile::tempdir().unwrap();
        let store = LaneStore::new(dir.path()).unwrap();
        let keys = ["main-build", "start-lock"];
        let journal = || {
            (
                store.snapshot().unwrap().len(),
                fs::read_dir(dir.path().join("locks")).unwrap().count(),
            )
        };

        // Before: a build queued ahead keeps its FIFO turn, then holds the
        // key. A refused attempt journals no record and makes no lock inode.
        let build = store.enqueue_lease(exclusive_lease(&keys[..1])).unwrap();
        let before = journal();
        let reason = deferred_barrier(
            store
                .try_acquire_restart_barrier(barrier_request(&keys))
                .unwrap(),
        );
        assert_eq!(
            reason,
            format!("main-build (host) queued for ticket {}", build.id)
        );
        assert_eq!(journal(), before, "a refused barrier left journal churn");
        let lease = LaneCoordinator::wait(&store, &build).await.unwrap();
        let reason = deferred_barrier(
            store
                .try_acquire_restart_barrier(barrier_request(&keys))
                .unwrap(),
        );
        assert_eq!(
            reason,
            format!("main-build (host) held by ticket {}", build.id)
        );
        store.release_lease(&lease).unwrap();
        // All keys or none: a build on the second key alone refuses it, and
        // the first key is not reserved meanwhile.
        let other = store.enqueue_lease(exclusive_lease(&keys[1..])).unwrap();
        let other = LaneCoordinator::wait(&store, &other).await.unwrap();
        deferred_barrier(
            store
                .try_acquire_restart_barrier(barrier_request(&keys))
                .unwrap(),
        );
        assert!(!store.snapshot().unwrap().iter().any(|r| r.restart_barrier));
        store.release_lease(&other).unwrap();

        // During: no ticket needing a barrier key is admitted. Not a plain
        // lease, not a job that could prepare past a service lease by having
        // the service yield, and not another service's shared lease.
        let barrier = held_barrier(
            store
                .try_acquire_restart_barrier(barrier_request(&keys))
                .unwrap(),
        );
        let service = store
            .try_acquire_service(
                LeaseRequest {
                    resources: vec![resource("editor", Access::Shared { slots: 1 })],
                    holder: service_holder(),
                    queue_timeout_ms: None,
                },
                &test_budget(dir.path()),
            )
            .unwrap()
            .expect("the editor's own key is not in the barrier");
        let other_service = LeaseRequest {
            resources: vec![resource("main-build", Access::Shared { slots: 1 })],
            holder: Holder {
                purpose: "service:other".into(),
                ..holder()
            },
            queue_timeout_ms: None,
        };
        assert!(
            store
                .try_acquire_service(other_service, &test_budget(dir.path()))
                .unwrap()
                .is_none()
        );
        let plain = store.enqueue_lease(exclusive_lease(&keys[1..])).unwrap();
        let job = queue_job(&store, dir.path(), &["editor", "main-build"]);
        assert!(store.try_grant(plain.id).unwrap().is_none());
        assert!(store.try_grant(job).unwrap().is_none());
        let row = store.record(job).unwrap();
        assert!(
            matches!(row.state, TicketState::Queued) && row.yield_services.is_empty(),
            "a job prepared past the restart barrier: {row:?}"
        );
        assert_eq!(
            row.wait_reason.as_deref(),
            Some("exclusive resource main-build busy")
        );

        // After: the queued tickets are admitted in turn (the job by having
        // the editor yield), and a second restart waits for them.
        store.release_lease(&barrier).unwrap();
        assert!(store.try_grant(plain.id).unwrap().is_some());
        store.finish(plain.id, 0, "test").unwrap();
        assert!(store.try_grant(job).unwrap().is_none());
        let row = store.record(job).unwrap();
        assert!(matches!(row.state, TicketState::Preparing));
        assert_eq!(row.yield_services, vec!["test"]);
        let reason = deferred_barrier(
            store
                .try_acquire_restart_barrier(barrier_request(&keys))
                .unwrap(),
        );
        assert_eq!(
            reason,
            format!("main-build (host) held by ticket {job}"),
            "{reason}"
        );
        store.finish(job, 0, "test").unwrap();
        store.release_lease(&service).unwrap();
        let again = held_barrier(
            store
                .try_acquire_restart_barrier(barrier_request(&keys))
                .unwrap(),
        );
        store.release_lease(&again).unwrap();
    }

    /// Two threads race a restart barrier against build admission on the
    /// same key; neither may ever see the other granted beside it.
    #[test]
    fn restart_barrier_races_build_admission_without_overlap() {
        // Grants per side. With the barrier's check and grant split over two
        // lock acquisitions this failed 10/10 runs (3/10 at 10 rounds).
        const ROUNDS: usize = 40;
        let dir = tempfile::tempdir().unwrap();
        let store = LaneStore::new(dir.path()).unwrap();
        let overlap = |store: &LaneStore| {
            let rows = store.snapshot().unwrap();
            let granted = |barrier: bool| {
                rows.iter().any(|r| {
                    r.restart_barrier == barrier && matches!(r.state, TicketState::Granted(_))
                })
            };
            granted(true) && granted(false)
        };
        let restarts = {
            let store = store.clone();
            std::thread::spawn(move || {
                let mut held = 0;
                for _ in 0..20 * ROUNDS {
                    if held == ROUNDS {
                        break;
                    }
                    if let RestartBarrier::Held(lease) = store
                        .try_acquire_restart_barrier(barrier_request(&["main-build"]))
                        .unwrap()
                    {
                        held += 1;
                        // Release before asserting, so a failure cannot
                        // leave the other thread spinning behind a barrier.
                        let clash = overlap(&store);
                        store.release_lease(&lease).unwrap();
                        assert!(!clash, "a build was granted beside the barrier");
                    }
                }
                held
            })
        };
        let mut builds = 0;
        for _ in 0..20 * ROUNDS {
            if builds == ROUNDS {
                break;
            }
            let ticket = store
                .enqueue_lease(exclusive_lease(&["main-build"]))
                .unwrap();
            let granted = store.try_grant(ticket.id).unwrap().is_some();
            let clash = granted && overlap(&store);
            store.finish(ticket.id, 0, "test").unwrap();
            assert!(!clash, "the barrier was granted beside a build");
            builds += usize::from(granted);
        }
        let held = restarts.join().unwrap();
        assert_eq!(
            (held, builds),
            (ROUNDS, ROUNDS),
            "barriers and builds granted"
        );
    }

    /// Failure mode: a service supervisor lost mid-restart (SIGKILL, OOM,
    /// `systemctl stop`) leaving its restart barrier, or its own service
    /// lease, quarantined until a reboot; or either released while the
    /// owner, or a backend it started, may still run.
    #[test]
    fn orphaned_service_claims_are_released_once_their_owner_is_gone() {
        let dir = tempfile::tempdir().unwrap();
        let set_owner = |store: &LaneStore, id: Uuid, pid: u32, ticks, cgroup| {
            store
                .locked(|state| {
                    let row = state
                        .records
                        .iter_mut()
                        .find(|r| r.ticket.id == id)
                        .unwrap();
                    row.supervisor_pid = Some(pid);
                    row.owner_start_ticks = ticks;
                    row.owner_cgroup = cgroup;
                    Ok(())
                })
                .unwrap();
        };
        let exited = || {
            let mut child = Command::new("true").spawn().unwrap();
            let pid = child.id();
            child.wait().unwrap();
            pid
        };
        let me = std::process::id();
        let my_ticks = proc_start_ticks(me).unwrap();
        let my_cgroup = fs::read_to_string("/proc/self/cgroup")
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix("0::"))
            .unwrap()
            .to_owned();
        let has = |actions: &[String], prefix: &str| {
            actions.iter().any(|action| action.starts_with(prefix))
        };

        let store = LaneStore::new(dir.path()).unwrap();
        let barrier = held_barrier(
            store
                .try_acquire_restart_barrier(barrier_request(&["main-build"]))
                .unwrap(),
        );
        let id = barrier.ticket.id;
        set_owner(&store, id, me, Some(my_ticks), None);
        // While its owner holds the lock, recovery leaves it alone.
        assert!(store.recover(false).unwrap().is_empty());
        let build = store
            .enqueue_lease(exclusive_lease(&["main-build"]))
            .unwrap();
        // The lock closes without a journal release but the owner process
        // lives: nothing proves its backend gone, so the keys are
        // quarantined.
        drop(store);
        let store = LaneStore::new(dir.path()).unwrap();
        let actions = store.recover(false).unwrap();
        assert!(
            has(&actions, &format!("restart barrier {id} lost supervisor")),
            "{actions:?}"
        );
        assert!(store.record(id).unwrap().quarantined);
        assert!(store.try_grant(build.id).unwrap().is_none());
        // The supervisor is gone but its unit cgroup still has a process (a
        // backend it started): still quarantined.
        set_owner(&store, id, exited(), None, Some(my_cgroup));
        store.recover(false).unwrap();
        assert!(store.record(id).unwrap().quarantined);
        assert!(store.try_grant(build.id).unwrap().is_none());
        // Provably gone (its pid exited, no cgroup recorded): a dry run
        // reports it, recover lifts the quarantine and the build is admitted.
        set_owner(&store, id, exited(), Some(my_ticks), None);
        let planned = store.recover(true).unwrap();
        assert!(
            has(&planned, &format!("restart barrier {id}: would release")),
            "{planned:?}"
        );
        assert!(store.record(id).unwrap().quarantined);
        let actions = store.recover(false).unwrap();
        assert!(
            has(&actions, &format!("restart barrier {id} released")),
            "{actions:?}"
        );
        let row = store.record(id).unwrap();
        assert!(!row.quarantined && row.evidence.unwrap().contains("has exited"));
        assert!(store.try_grant(build.id).unwrap().is_some());
        store.finish(build.id, 0, "test").unwrap();

        // The next barrier claim (the next supervisor start) releases a
        // Granted orphan itself: here its pid now names another process.
        let orphan = held_barrier(
            store
                .try_acquire_restart_barrier(barrier_request(&["main-build"]))
                .unwrap(),
        );
        set_owner(&store, orphan.ticket.id, me, Some(my_ticks + 1), None);
        drop(store);
        let store = LaneStore::new(dir.path()).unwrap();
        let next = held_barrier(
            store
                .try_acquire_restart_barrier(barrier_request(&["main-build"]))
                .unwrap(),
        );
        let row = store.record(orphan.ticket.id).unwrap();
        assert!(
            matches!(row.state, TicketState::Finished)
                && row.evidence.unwrap().contains("another process")
        );
        store.release_lease(&next).unwrap();

        // A dead supervisor's own service lease is released when the next
        // supervisor claims it; without that, one slot stays taken forever.
        let request = LeaseRequest {
            resources: vec![resource("project", Access::Shared { slots: 1 })],
            holder: service_holder(),
            queue_timeout_ms: None,
        };
        let budget = test_budget(dir.path());
        let lease = store
            .try_acquire_service(request.clone(), &budget)
            .unwrap()
            .unwrap();
        // Recorded by a binary without start times: the pid (this test)
        // is not a supervisor of the service.
        set_owner(&store, lease.ticket.id, me, None, None);
        drop(store);
        let store = LaneStore::new(dir.path()).unwrap();
        let successor = store.try_acquire_service(request, &budget).unwrap();
        assert!(
            successor.is_some(),
            "the dead supervisor's lease blocked it"
        );
        let row = store.record(lease.ticket.id).unwrap();
        assert!(
            matches!(row.state, TicketState::Finished)
                && row.evidence.unwrap().contains("not a supervisor of test"),
        );
        store.release_lease(&successor.unwrap()).unwrap();
    }
}
