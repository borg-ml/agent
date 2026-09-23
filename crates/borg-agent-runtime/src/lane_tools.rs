//! Model-facing bridge to host-local lanes (jobs) and supervised services.
//!
//! Identity is the calling session's: the lease holder is built from the
//! dispatcher's actor, never from tool arguments, so one session (or a
//! sub-agent) cannot act as another. Jobs come only from registered
//! templates; destructive and unfenced service control stays CLI-only.
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use borg_lanes::adapter::JobTemplate;
use borg_lanes::lanes::{
    Access, Holder, JobHandle, JobSpec, JobState, LaneRecord, LaneStore, ResourceKey, TicketState,
};
use borg_lanes::services::{ServiceManager, ServiceRequest, ServiceSpec, ServiceStatus};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

/// Identity and authority fields a model might try to supply. The holder is
/// the session's; fences, lease ids and approvals are never model input.
const IDENTITY_FIELDS: &[&str] = &[
    "owner",
    "holder",
    "participant_id",
    "session_id",
    "by",
    "confirmed",
    "fence",
    "generation",
    "lease_id",
    "argv",
    "force",
];

const MAX_WAIT_SECONDS: u64 = 1800;
const DEFAULT_WAIT_SECONDS: u64 = 600;
const MAX_LEASE_SECONDS: u64 = 86_400;
const MAX_READ_BYTES: usize = 4 * 1024 * 1024;

/// The calling session, as the dispatcher knows it.
#[derive(Clone, Debug)]
pub(crate) struct Caller {
    pub(crate) participant_id: Uuid,
    pub(crate) session_id: Uuid,
}

impl Caller {
    fn holder(&self, purpose: &str) -> Holder {
        Holder {
            participant_id: self.participant_id,
            session_id: self.session_id,
            host_pid: Some(std::process::id()),
            purpose: purpose.to_string(),
        }
    }

    fn owns(&self, holder: &Holder) -> bool {
        holder.participant_id == self.participant_id && holder.session_id == self.session_id
    }
}

/// Where lanes state and the template policy live, and which `borg` runs
/// `lane job wait`.
#[derive(Clone, Debug)]
pub(crate) struct LaneTools {
    pub(crate) root: PathBuf,
    /// Human-curated template policy, kept apart from the supervisor-writable
    /// lanes state. Same-UID shells can still edit it: the permission mode,
    /// not this directory, is the sandbox.
    pub(crate) templates: PathBuf,
    pub(crate) executable: Option<PathBuf>,
    /// (session, job) pairs submit handed out. A coalesced pending job keeps
    /// its first submitter as holder, yet the later submitter may wait on it.
    submitted: Arc<Mutex<HashSet<(Uuid, Uuid)>>>,
}

impl Default for LaneTools {
    fn default() -> Self {
        let templates = std::env::var_os("BORG_LANE_TEMPLATES")
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("XDG_CONFIG_HOME")
                    .map(PathBuf::from)
                    .or_else(|| dirs::home_dir().map(|home| home.join(".config")))
                    .map(|config| config.join("borg/lane-templates"))
            })
            .unwrap_or_default();
        Self::new(
            LaneStore::default_root(),
            templates,
            std::env::var_os("BORG_LANE_EXECUTABLE")
                .map(PathBuf::from)
                .or_else(|| std::env::current_exe().ok()),
        )
    }
}

/// What the dispatcher must do after a submit asked for a watcher.
pub(crate) struct WatchRequest {
    pub(crate) command: String,
    pub(crate) label: String,
}

fn refuse_identity_fields(arguments: &Value) -> Result<()> {
    if let Some(object) = arguments.as_object() {
        for field in IDENTITY_FIELDS {
            ensure!(
                !object.contains_key(*field),
                "`{field}` cannot be supplied: lanes and services act as your own session, and fences, lease ids and approvals are never tool input"
            );
        }
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum JobOp {
    Submit,
    Wait,
    Status,
    List,
    Cancel,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JobArgs {
    op: JobOp,
    adapter: Option<String>,
    template: Option<String>,
    job_id: Option<Uuid>,
    timeout_seconds: Option<u64>,
    #[serde(default)]
    watch: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ServiceOp {
    Status,
    Lease,
    Release,
    Restart,
    Read,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ServiceArgs {
    op: ServiceOp,
    id: String,
    purpose: Option<String>,
    ttl_seconds: Option<u64>,
    reason: Option<String>,
    path: Option<String>,
}

fn safe_name(kind: &str, value: Option<&str>) -> Result<String> {
    let value = value.with_context(|| format!("{kind} is required"))?;
    ensure!(
        !value.is_empty()
            && value.len() <= 64
            && value
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_')),
        "{kind} must be 1-64 letters, digits, '-' or '_'"
    );
    Ok(value.to_string())
}

fn job_value(job: &JobHandle) -> Value {
    json!({
        "job_id": job.id,
        "ticket": job.ticket.sequence,
        "state": job.state,
        "log_path": job.log_path,
    })
}

fn terminal(job: &JobHandle) -> bool {
    matches!(
        job.state,
        JobState::Finished { .. } | JobState::Cancelled { .. }
    )
}

impl LaneTools {
    pub(crate) fn new(root: PathBuf, templates: PathBuf, executable: Option<PathBuf>) -> Self {
        Self {
            root,
            templates,
            executable,
            submitted: Arc::default(),
        }
    }

    fn store(&self) -> Result<LaneStore> {
        LaneStore::new(&self.root)
    }

    fn services(&self) -> ServiceManager {
        ServiceManager::new(
            self.root.join("services"),
            self.executable.clone().unwrap_or_default(),
        )
    }

    /// A registered template, `<templates>/<adapter>/<template>.json`. Every
    /// component must be a real directory or file owned by this user and
    /// writable by no one else; the file is opened without following links
    /// and must be the same inode that was checked. The model only names it.
    fn template(&self, adapter: &str, template: &str) -> Result<JobTemplate> {
        use std::io::Read;
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        let unregistered = || format!("job template {adapter}/{template} is not registered");
        let directory = self.templates.join(adapter);
        let path = directory.join(format!("{template}.json"));
        for component in [&self.templates, &directory, &path] {
            trusted_by_user(component).with_context(unregistered)?;
        }
        let checked = std::fs::symlink_metadata(&path).with_context(unregistered)?;
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)
            .with_context(unregistered)?;
        let opened = file.metadata()?;
        ensure!(
            opened.is_file()
                && opened.dev() == checked.dev()
                && opened.ino() == checked.ino()
                && opened.uid() == unsafe { libc::geteuid() }
                && opened.mode() & 0o022 == 0,
            "job template {adapter}/{template} changed while it was being checked"
        );
        let mut bytes = Vec::new();
        file.by_ref().take(1024 * 1024).read_to_end(&mut bytes)?;
        let parsed: JobTemplate = serde_json::from_slice(&bytes)
            .with_context(|| format!("job template {adapter}/{template} is invalid"))?;
        ensure!(
            parsed.name == template,
            "job template {adapter}/{template} names itself {}",
            parsed.name
        );
        validate_spec(&parsed.spec)
            .with_context(|| format!("job template {adapter}/{template} is not admissible"))?;
        Ok(parsed)
    }

    /// The job record, if this session may see or wait on it: it holds the
    /// job, or (when `coalesced` is allowed) submit handed it this id.
    fn authorized(
        &self,
        store: &LaneStore,
        caller: &Caller,
        id: Uuid,
        coalesced: bool,
    ) -> Result<LaneRecord> {
        let record = store
            .snapshot()?
            .into_iter()
            .find(|record| record.ticket.id == id);
        let submitted = coalesced
            && self
                .submitted
                .lock()
                .is_ok_and(|ids| ids.contains(&(caller.session_id, id)));
        match record {
            Some(record) if caller.owns(&record.request.holder) || submitted => Ok(record),
            _ => bail!("job {id} is not one of your session's jobs"),
        }
    }

    pub(crate) async fn job(
        &self,
        caller: &Caller,
        arguments: Value,
    ) -> Result<(Value, Option<WatchRequest>)> {
        refuse_identity_fields(&arguments)?;
        let args: JobArgs = serde_json::from_value(arguments)?;
        let store = self.store()?;
        match args.op {
            JobOp::Submit => {
                let adapter = safe_name("adapter", args.adapter.as_deref())?;
                let name = safe_name("template", args.template.as_deref())?;
                let template = self.template(&adapter, &name)?;
                let mut spec = template.spec;
                spec.lease.holder = caller.holder(&format!("{adapter}/{name}"));
                self.admit_exclusive(&spec.lease.resources)?;
                let job = tokio::task::spawn_blocking(move || store.enqueue_job(spec)).await??;
                if let Ok(mut submitted) = self.submitted.lock() {
                    submitted.insert((caller.session_id, job.id));
                }
                let watch = args.watch.then(|| self.watch_request(&job)).transpose()?;
                Ok((job_value(&job), watch))
            }
            JobOp::Wait => {
                let id = args.job_id.context("job_id is required")?;
                self.authorized(&store, caller, id, true)?;
                let timeout = args
                    .timeout_seconds
                    .unwrap_or(DEFAULT_WAIT_SECONDS)
                    .clamp(1, MAX_WAIT_SECONDS);
                Ok((self.wait(&store, id, timeout).await?, None))
            }
            JobOp::Status => {
                let id = args.job_id.context("job_id is required")?;
                self.authorized(&store, caller, id, true)?;
                Ok((job_value(&store.job_status(id)?), None))
            }
            JobOp::List => {
                let own: Vec<Value> = store
                    .snapshot()?
                    .into_iter()
                    .filter(|record| caller.owns(&record.request.holder))
                    .map(|record| {
                        json!({
                            "job_id": record.ticket.id,
                            "ticket": record.ticket.sequence,
                            "state": record.job.as_ref().map(|job| json!(job.state))
                                .unwrap_or_else(|| json!(ticket_state_name(&record.state))),
                            "wait_reason": record.wait_reason,
                        })
                    })
                    .collect();
                Ok((json!({"jobs": own}), None))
            }
            JobOp::Cancel => {
                let id = args.job_id.context("job_id is required")?;
                self.authorized(&store, caller, id, false)
                    .with_context(|| format!("only the session holding job {id} can cancel it"))?;
                store.cancel_ticket(id, "cancelled by its submitting session")?;
                Ok((json!({"job_id": id, "state": "cancelled"}), None))
            }
        }
    }

    /// An exclusive request pre-yields every service bound to its keys, and
    /// a snapshot of their leases cannot be made atomic with lease grants
    /// from here. Until lanes offers an owner-aware preemption check under
    /// its own gate, a model may not take a key any service is bound to.
    fn admit_exclusive(&self, resources: &[borg_lanes::lanes::ResourceRequest]) -> Result<()> {
        let exclusive: Vec<&ResourceKey> = resources
            .iter()
            .filter(|request| matches!(request.access, Access::Exclusive))
            .map(|request| &request.key)
            .collect();
        if exclusive.is_empty() {
            return Ok(());
        }
        let Ok(entries) = std::fs::read_dir(self.root.join("services")) else {
            return Ok(());
        };
        for entry in entries.flatten() {
            let Ok(spec) = std::fs::read(entry.path().join("spec.json"))
                .map_err(anyhow::Error::from)
                .and_then(|bytes| Ok(serde_json::from_slice::<ServiceSpec>(&bytes)?))
            else {
                continue;
            };
            if spec
                .resources
                .iter()
                .any(|request| exclusive.contains(&&request.key))
            {
                bail!(
                    "this template takes a resource that service {} is bound to exclusively, which would stop it; exclusive jobs over services run from the CLI (`borg lane job submit`) for now",
                    spec.id
                );
            }
        }
        Ok(())
    }

    fn wait_command(&self, id: Uuid) -> Result<(PathBuf, Vec<String>)> {
        let executable = self
            .executable
            .clone()
            .context("the borg executable is unavailable to wait on lane jobs")?;
        Ok((
            executable,
            vec![
                "lane".into(),
                "--state-dir".into(),
                self.root.display().to_string(),
                "--json".into(),
                "job".into(),
                "wait".into(),
                id.to_string(),
            ],
        ))
    }

    fn watch_request(&self, job: &JobHandle) -> Result<WatchRequest> {
        let (executable, args) = self.wait_command(job.id)?;
        let command = std::iter::once(executable.display().to_string())
            .chain(args)
            .map(|part| shell_quote(&part))
            .collect::<Vec<_>>()
            .join(" ");
        Ok(WatchRequest {
            command,
            label: format!("lane job {}", &job.id.to_string()[..8]),
        })
    }

    /// Block on the job's completion through `borg lane job wait`, a child
    /// process that a timeout or interrupt kills cleanly.
    async fn wait(&self, store: &LaneStore, id: Uuid, timeout: u64) -> Result<Value> {
        let current = store.job_status(id)?;
        if terminal(&current) {
            return Ok(job_value(&current));
        }
        let (executable, args) = self.wait_command(id)?;
        let child = tokio::process::Command::new(executable)
            .args(args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .context("failed to wait on the lane job")?;
        let finished =
            tokio::time::timeout(Duration::from_secs(timeout), child.wait_with_output()).await;
        let job = store.job_status(id)?;
        let mut value = job_value(&job);
        if finished.is_err() && !terminal(&job) {
            value["timed_out"] = json!(true);
            value["hint"] = json!(
                "still running; submit with watch=true (or watch `borg lane job wait`) instead of waiting again"
            );
        }
        Ok(value)
    }

    pub(crate) async fn service(&self, caller: &Caller, arguments: Value) -> Result<Value> {
        refuse_identity_fields(&arguments)?;
        let args: ServiceArgs = serde_json::from_value(arguments)?;
        let id = safe_name("id", Some(&args.id))?;
        let manager = self.services();
        let timeout = Duration::from_secs(30);
        let status = manager.read_status(&id)?;
        let own_lease = status
            .clients
            .iter()
            .find(|lease| caller.owns(&lease.owner))
            .cloned();
        match args.op {
            ServiceOp::Status => Ok(service_value(&status, caller)),
            ServiceOp::Lease => {
                let ttl = args.ttl_seconds.unwrap_or(600);
                ensure!(
                    (1..=MAX_LEASE_SECONDS).contains(&ttl),
                    "ttl_seconds must be between 1 and {MAX_LEASE_SECONDS}"
                );
                let purpose = args.purpose.unwrap_or_else(|| "work".into());
                ensure!(purpose.len() <= 200, "purpose is limited to 200 bytes");
                let status = manager
                    .send(
                        &id,
                        ServiceRequest::Lease {
                            owner: caller.holder(&purpose),
                            purpose,
                            ttl_ms: ttl * 1000,
                        },
                        timeout,
                    )
                    .await?;
                Ok(service_value(&status, caller))
            }
            ServiceOp::Release => {
                let lease = own_lease.context("you hold no lease on this service")?;
                let status = manager
                    .send(
                        &id,
                        ServiceRequest::Release {
                            lease_id: lease.id,
                            owner: caller.holder(&lease.purpose),
                        },
                        timeout,
                    )
                    .await?;
                Ok(service_value(&status, caller))
            }
            ServiceOp::Restart => {
                ensure!(
                    own_lease.is_some(),
                    "restart needs a lease on {id} held by your session; lease it first"
                );
                let reason = args
                    .reason
                    .unwrap_or_else(|| "requested by its lease holder".into());
                ensure!(reason.len() <= 200, "reason is limited to 200 bytes");
                let status = manager
                    .send(
                        &id,
                        ServiceRequest::Restart {
                            reason,
                            force: false,
                        },
                        timeout,
                    )
                    .await?;
                Ok(service_value(&status, caller))
            }
            ServiceOp::Read => {
                ensure!(
                    own_lease.is_some(),
                    "read needs a lease on {id} held by your session; lease it first"
                );
                let path = args.path.context("path is required")?;
                ensure!(
                    path.starts_with('/')
                        && !path.contains(['%', '?', '#', '\\'])
                        && !path.contains("..")
                        && !path.chars().any(|c| c.is_whitespace() || c.is_control()),
                    "path must be a plain absolute path with no query, fragment, encoding or '..'"
                );
                read_service(&manager.root, &id, &status, &path).await
            }
        }
    }
}

fn ticket_state_name(state: &TicketState) -> &'static str {
    match state {
        TicketState::Queued => "queued",
        TicketState::Preparing => "preparing",
        TicketState::Granted(_) => "granted",
        TicketState::Finished => "finished",
        TicketState::Cancelled { .. } => "cancelled",
    }
}

/// Status with other sessions' identities reduced to "another session".
fn service_value(status: &ServiceStatus, caller: &Caller) -> Value {
    let clients: Vec<Value> = status
        .clients
        .iter()
        .map(|lease| {
            json!({
                "yours": caller.owns(&lease.owner),
                "purpose": lease.purpose,
                "expires_at_unix_ms": lease.expires_at_unix_ms,
            })
        })
        .collect();
    json!({
        "id": status.id,
        "state": status.state,
        "reason": status.reason,
        "endpoint": status.endpoint,
        "backend_pid": status.backend_pid,
        "restarts": status.restarts,
        "yielded": !status.yields.is_empty(),
        "clients": clients,
    })
}

/// Capture-class read: a GET of an exact audited read-only path through the
/// service's own front endpoint (the proxy forwards nothing else unfenced).
async fn read_service(
    services_root: &Path,
    id: &str,
    status: &ServiceStatus,
    path: &str,
) -> Result<Value> {
    let spec: ServiceSpec = serde_json::from_slice(
        &std::fs::read(services_root.join(id).join("spec.json"))
            .with_context(|| format!("service {id} has no registered spec"))?,
    )?;
    ensure!(
        spec.read_only_paths.iter().any(|allowed| allowed == path),
        "{path} is not one of service {id}'s audited read-only paths"
    );
    let endpoint = status
        .endpoint
        .as_ref()
        .or(spec.endpoint.as_ref())
        .context("service has no endpoint")?;
    let listen: std::net::SocketAddr = endpoint
        .listen
        .parse()
        .context("service endpoint is not a socket address")?;
    ensure!(
        listen.ip().is_loopback(),
        "service endpoint is not loopback"
    );
    let response = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .build()?
        .get(format!("http://{listen}{path}"))
        .send()
        .await
        .context("service read failed")?;
    let code = response.status().as_u16();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    let body = response.bytes().await?;
    ensure!(
        body.len() <= MAX_READ_BYTES,
        "service response exceeds {MAX_READ_BYTES} bytes"
    );
    let mut value = json!({"status": code, "content_type": content_type});
    if matches!(content_type.as_str(), "image/png" | "image/jpeg") {
        use base64::Engine;
        value["borg_attachments"] = json!([{
            "media_type": content_type,
            "data_base64": base64::engine::general_purpose::STANDARD.encode(&body),
        }]);
    } else {
        value["body"] = json!(String::from_utf8_lossy(&body));
    }
    Ok(value)
}

/// What a registered template may run: absolute canonical programs and
/// working directory, and canonical resource keys (no aliases, no `..`).
fn validate_spec(spec: &JobSpec) -> Result<()> {
    let canonical = |path: &Path, what: &str| -> Result<()> {
        ensure!(
            path.is_absolute(),
            "{what} must be absolute: {}",
            path.display()
        );
        let resolved = std::fs::canonicalize(path)
            .with_context(|| format!("{what} must exist: {}", path.display()))?;
        ensure!(
            resolved == path,
            "{what} is not canonical: {} (expected {})",
            path.display(),
            resolved.display()
        );
        Ok(())
    };
    canonical(&spec.cwd, "cwd")?;
    let program = spec.argv.first().context("argv is empty")?;
    canonical(Path::new(program), "program")?;
    for hook in [&spec.pre_hook, &spec.post_hook].into_iter().flatten() {
        let program = hook.argv.first().context("hook argv is empty")?;
        canonical(Path::new(program), "hook program")?;
    }
    for request in &spec.lease.resources {
        request.key.validate_canonical()?;
    }
    Ok(())
}

/// Only files and directories this user owns and nobody else can write are
/// trusted as registered templates.
fn trusted_by_user(path: &Path) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::symlink_metadata(path)?;
    ensure!(
        !metadata.file_type().is_symlink(),
        "{} is a symlink",
        path.display()
    );
    ensure!(
        metadata.uid() == unsafe { libc::geteuid() },
        "{} is not owned by this user",
        path.display()
    );
    ensure!(
        metadata.mode() & 0o022 == 0,
        "{} is writable by other users",
        path.display()
    );
    Ok(())
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

pub(crate) fn lane_job_spec() -> (&'static str, &'static str, Value) {
    (
        "lane_job",
        "Host-local build/run/test jobs through Borg lanes (FIFO admission, RAM/disk budgets, exclusive project resources). submit {adapter, template, watch?} runs a job from a template a human registered for this host (~/.config/borg/lane-templates); you never supply commands, paths or resources. It returns job_id at once; pass watch=true to be notified on exit (then await_watchers only when nothing else is actionable) or call wait {job_id, timeout_seconds?} for a bounded blocking wait. status {job_id}, list (your jobs) and cancel {job_id} (jobs you submitted). Jobs run as your own session. Exclusive jobs over a resource a service is bound to (for example an editor) run only from the CLI for now. Requires Full Access or approval, which is the real boundary: templates are a convenience a same-user shell could edit.",
        json!({
            "type": "object",
            "properties": {
                "op": {"type": "string", "enum": ["submit", "wait", "status", "list", "cancel"]},
                "adapter": {"type": "string", "maxLength": 64},
                "template": {"type": "string", "maxLength": 64},
                "job_id": {"type": "string", "format": "uuid"},
                "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": MAX_WAIT_SECONDS},
                "watch": {"type": "boolean"}
            },
            "required": ["op"],
            "additionalProperties": false
        }),
    )
}

pub(crate) fn lane_service_spec() -> (&'static str, &'static str, Value) {
    (
        "lane_service",
        "Supervised host services started by a human or adapter (for example an editor MCP backend). status {id}; lease {id, purpose?, ttl_seconds?} takes a client lease as your own session (one owner at a time); release {id} gives yours back; restart {id, reason?} and read {id, path} (a capture-class GET of one of the service's audited read-only paths) need your lease. Start, stop, yield, resume and forced restarts are for humans through `borg lane service`. Requires Full Access or approval.",
        json!({
            "type": "object",
            "properties": {
                "op": {"type": "string", "enum": ["status", "lease", "release", "restart", "read"]},
                "id": {"type": "string", "maxLength": 64},
                "purpose": {"type": "string", "maxLength": 200},
                "ttl_seconds": {"type": "integer", "minimum": 1, "maximum": MAX_LEASE_SECONDS},
                "reason": {"type": "string", "maxLength": 200},
                "path": {"type": "string", "maxLength": 512}
            },
            "required": ["op", "id"],
            "additionalProperties": false
        }),
    )
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use borg_lanes::adapter::JobKind;
    use borg_lanes::lanes::{
        AdmissionBudget, JobFingerprint, LeaseRequest, ResourceRequest, ResourceScope,
    };
    use borg_lanes::services::{ClientLease, ServiceState};

    pub(crate) fn caller(id: u128) -> Caller {
        Caller {
            participant_id: Uuid::from_u128(id),
            session_id: Uuid::from_u128(id),
        }
    }

    struct Fixture {
        _directory: tempfile::TempDir,
        tools: LaneTools,
        project: PathBuf,
    }

    pub(crate) fn fixture_tools() -> (tempfile::TempDir, LaneTools, PathBuf) {
        let fixture = fixture();
        (fixture._directory, fixture.tools, fixture.project)
    }

    fn fixture() -> Fixture {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("lanes");
        let templates = directory.path().join("templates");
        let project = std::fs::canonicalize(directory.path()).unwrap();
        std::fs::create_dir_all(templates.join("native")).unwrap();
        Fixture {
            tools: LaneTools::new(root, templates, None),
            project,
            _directory: directory,
        }
    }

    pub(crate) fn spec(project: &Path, access: Access) -> JobSpec {
        JobSpec {
            fingerprint: JobFingerprint("fp".into()),
            lease: LeaseRequest {
                resources: vec![ResourceRequest {
                    key: ResourceKey {
                        scope: ResourceScope::Project(project.to_path_buf()),
                        name: "editor".into(),
                    },
                    access,
                }],
                holder: caller(99).holder("forged in the template"),
                queue_timeout_ms: None,
            },
            argv: vec![
                std::fs::canonicalize("/bin/sh")
                    .unwrap()
                    .display()
                    .to_string(),
            ],
            cwd: project.to_path_buf(),
            env: vec![],
            memory_max_bytes: None,
            admission: AdmissionBudget {
                min_available_ram_bytes: 0,
                reserve_ram_bytes: 0,
                min_free_disk_bytes: 0,
                reserve_disk_bytes: 0,
                disk_path: project.to_path_buf(),
            },
            pre_hook: None,
            post_hook: None,
            timeout_ms: 60_000,
            stall_timeout_ms: None,
            coalesce: false,
        }
    }

    fn register(fixture: &Fixture, name: &str, spec: JobSpec) -> PathBuf {
        register_template(&fixture.tools.templates, name, spec)
    }

    pub(crate) fn register_template(templates: &Path, name: &str, spec: JobSpec) -> PathBuf {
        let path = templates.join("native").join(format!("{name}.json"));
        let template = JobTemplate {
            name: name.into(),
            kind: JobKind::Build,
            resources: spec.lease.resources.clone(),
            spec,
        };
        std::fs::write(&path, serde_json::to_vec(&template).unwrap()).unwrap();
        path
    }

    fn bind_service(fixture: &Fixture, owner: Option<&Caller>) {
        write_test_service(&fixture.tools.root, &fixture.project, owner);
    }

    /// A registered "editor" service bound to `project`, leased by `owner`.
    pub(crate) fn write_test_service(root: &Path, project: &Path, owner: Option<&Caller>) {
        let directory = root.join("services").join("editor");
        std::fs::create_dir_all(&directory).unwrap();
        let job = spec(project, Access::Shared { slots: 1 });
        let service = serde_json::json!({
            "id": "editor",
            "argv": job.argv,
            "cwd": job.cwd,
            "env": [],
            "resources": job.lease.resources,
            "memory_max_bytes": null,
            "admission": job.admission,
            "health": {"argv": ["/bin/true"], "interval_ms": 1000, "timeout_ms": 1000},
            "restart": {"max_restarts": 1, "backoff_ms": 1000, "debounce_ms": 1000},
            "endpoint": {"listen": "127.0.0.1:9", "backend_ports": [10, 11]},
            "restore": null,
            "read_only_paths": ["/capture"],
        });
        std::fs::write(directory.join("spec.json"), service.to_string()).unwrap();
        let status = ServiceStatus {
            id: "editor".into(),
            state: ServiceState::Healthy { backend: None },
            endpoint: None,
            clients: owner
                .map(|owner| ClientLease {
                    id: Uuid::from_u128(7),
                    service_id: "editor".into(),
                    owner: owner.holder("editing"),
                    expires_at_unix_ms: u64::MAX,
                    purpose: "editing".into(),
                })
                .into_iter()
                .collect(),
            reason: String::new(),
            backend_pid: None,
            supervisor_pid: None,
            restarts: 0,
            yields: Default::default(),
        };
        std::fs::write(
            directory.join("state.json"),
            serde_json::to_vec(&status).unwrap(),
        )
        .unwrap();
    }

    /// Failure mode: a model naming another session (or a fence, approval or
    /// lease id) as its own and acting with that authority.
    #[tokio::test]
    async fn supplied_identity_and_authority_fields_are_refused() {
        let fixture = fixture();
        let someone_else = Uuid::from_u128(2).to_string();
        for arguments in [
            json!({"op": "lease", "id": "editor", "owner": someone_else}),
            json!({"op": "release", "id": "editor", "lease_id": someone_else}),
            json!({"op": "restart", "id": "editor", "force": true}),
            json!({"op": "status", "id": "editor", "session_id": someone_else}),
        ] {
            let error = fixture
                .tools
                .service(&caller(1), arguments.clone())
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("cannot be supplied"),
                "{arguments}: {error}"
            );
        }
        for arguments in [
            json!({"op": "submit", "adapter": "native", "template": "build", "confirmed": true}),
            json!({"op": "submit", "adapter": "native", "template": "build", "fence": 3}),
            json!({"op": "submit", "adapter": "native", "template": "build", "argv": ["/bin/sh"]}),
            json!({"op": "cancel", "job_id": someone_else, "holder": someone_else}),
        ] {
            let error = fixture
                .tools
                .job(&caller(1), arguments.clone())
                .await
                .err()
                .unwrap();
            assert!(
                error.to_string().contains("cannot be supplied"),
                "{arguments}: {error}"
            );
        }
    }

    /// Failure mode: a model getting arbitrary commands admitted through the
    /// template path (unregistered names, traversal, links, writable files or
    /// non-canonical programs).
    #[tokio::test]
    async fn only_trusted_registered_templates_are_admitted() {
        use std::os::unix::fs::PermissionsExt;
        let fixture = fixture();
        let submit =
            |template: &str| json!({"op": "submit", "adapter": "native", "template": template});
        let refused = |result: Result<(Value, Option<WatchRequest>)>| {
            result.err().expect("must be refused").to_string()
        };
        assert!(
            refused(fixture.tools.job(&caller(1), submit("missing")).await)
                .contains("not registered")
        );
        for name in ["../escape", "a.b", ""] {
            assert!(
                refused(fixture.tools.job(&caller(1), submit(name)).await)
                    .contains("letters, digits")
            );
        }
        let path = register(
            &fixture,
            "writable",
            spec(&fixture.project, Access::Shared { slots: 1 }),
        );
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o666)).unwrap();
        assert!(
            refused(fixture.tools.job(&caller(1), submit("writable")).await)
                .contains("not registered")
        );
        let target = register(
            &fixture,
            "target",
            spec(&fixture.project, Access::Shared { slots: 1 }),
        );
        std::os::unix::fs::symlink(&target, fixture.tools.templates.join("native/linked.json"))
            .unwrap();
        assert!(
            refused(fixture.tools.job(&caller(1), submit("linked")).await)
                .contains("not registered")
        );
        let mut relative = spec(&fixture.project, Access::Shared { slots: 1 });
        relative.argv = vec!["sh".into()];
        register(&fixture, "relative", relative);
        let error = refused(fixture.tools.job(&caller(1), submit("relative")).await);
        assert!(error.contains("not admissible"), "{error}");
    }

    /// Failure mode: a model preempting a service (an editor another session
    /// may lease at any moment) with an exclusive job.
    #[tokio::test]
    async fn exclusive_templates_over_a_bound_service_are_cli_only() {
        let fixture = fixture();
        register(&fixture, "run", spec(&fixture.project, Access::Exclusive));
        bind_service(&fixture, None);
        let error = fixture
            .tools
            .job(
                &caller(1),
                json!({"op": "submit", "adapter": "native", "template": "run"}),
            )
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("run from the CLI"), "{error}");
    }

    /// Failure mode: one session acting with another's service lease
    /// (restart, capture reads or release), including a sub-agent using its
    /// parent's.
    #[tokio::test]
    async fn service_mutations_need_the_callers_own_lease() {
        let fixture = fixture();
        let holder = caller(1);
        bind_service(&fixture, Some(&holder));
        let other = caller(2);
        for (arguments, refusal) in [
            (
                json!({"op": "restart", "id": "editor"}),
                "restart needs a lease",
            ),
            (
                json!({"op": "read", "id": "editor", "path": "/capture"}),
                "read needs a lease",
            ),
            (
                json!({"op": "release", "id": "editor"}),
                "you hold no lease",
            ),
        ] {
            let error = fixture
                .tools
                .service(&other, arguments.clone())
                .await
                .unwrap_err();
            assert!(error.to_string().contains(refusal), "{arguments}: {error}");
        }
        let status = fixture
            .tools
            .service(&other, json!({"op": "status", "id": "editor"}))
            .await
            .unwrap();
        assert_eq!(status["clients"][0]["yours"], false);
        // The holder passes the lease check (and would then reach the supervisor).
        let error = fixture
            .tools
            .service(
                &holder,
                json!({"op": "read", "id": "editor", "path": "/elsewhere"}),
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("audited read-only paths"),
            "{error}"
        );
        for path in ["/capture?x=1", "/capture/../x", "/cap%74ure", "capture"] {
            let error = fixture
                .tools
                .service(&holder, json!({"op": "read", "id": "editor", "path": path}))
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("plain absolute path"),
                "{path}: {error}"
            );
        }
    }

    /// Failure mode: reading another session's job log path, waiting on it or
    /// cancelling it by guessing its id.
    #[tokio::test]
    async fn job_ids_are_not_authority() {
        let fixture = fixture();
        let store = fixture.tools.store().unwrap();
        let mut request = spec(&fixture.project, Access::Shared { slots: 1 }).lease;
        request.holder = caller(1).holder("owner");
        let ticket = store.enqueue_lease(request).unwrap();
        for op in ["status", "wait", "cancel"] {
            let error = fixture
                .tools
                .job(&caller(2), json!({"op": op, "job_id": ticket.id}))
                .await
                .err()
                .unwrap();
            assert!(
                format!("{error:#}").contains("not one of your session's jobs"),
                "{op}: {error:#}"
            );
        }
        let listed = fixture
            .tools
            .job(&caller(2), json!({"op": "list"}))
            .await
            .unwrap()
            .0;
        assert_eq!(listed["jobs"], json!([]));
        let listed = fixture
            .tools
            .job(&caller(1), json!({"op": "list"}))
            .await
            .unwrap()
            .0;
        assert_eq!(listed["jobs"][0]["job_id"], json!(ticket.id));
    }
}
