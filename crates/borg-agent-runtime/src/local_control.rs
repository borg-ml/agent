use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicUsize, Ordering},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, watch};
use uuid::Uuid;

use crate::{
    EventActor, HostCommand, MessageStatus, SessionEvent, SessionEventKind, SessionStatus,
    SessionStore, SessionWriterLease,
};

const MAX_CONTROL_COMMAND_BYTES: u64 = 1024 * 1024;
const ATTACHED_SESSION_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(25);
const ATTACHED_STORE_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(100);
const LOCAL_LIVE_EVENT_BUFFER: usize = 128;
const LOCAL_LIVE_FRAME_MAX_BYTES: usize = 1024 * 1024;
const LOCAL_LIVE_DELTA_BATCH_DELAY: std::time::Duration = std::time::Duration::from_millis(8);
const LOCAL_LIVE_DELTA_BATCH_MAX_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum LocalLiveFrame {
    Hello {
        latest_sequence: u64,
        snapshots: Vec<SessionEvent>,
    },
    Event {
        event: SessionEvent,
        text_start: Option<usize>,
        durable_watermark: u64,
    },
    Lagged,
}

struct LocalLivePublisher {
    events: broadcast::Sender<LocalLiveWire>,
    latest_sequence: u64,
    reasoning_bytes: HashMap<Uuid, usize>,
    message_bytes: HashMap<(Uuid, Uuid), usize>,
    reasoning_snapshots: HashMap<Uuid, SessionEvent>,
    message_snapshots: HashMap<(Uuid, Uuid), SessionEvent>,
    pending_delta: Option<PendingLiveDelta>,
    pending_generation: u64,
    last_delta_sent_at: Option<tokio::time::Instant>,
    last_delta_key: Option<LiveDeltaKey>,
}

#[derive(PartialEq, Eq)]
enum LiveDeltaKey {
    Reasoning(Uuid),
    Message(Uuid, Uuid),
}

struct PendingLiveDelta {
    key: LiveDeltaKey,
    event: SessionEvent,
    text_start: usize,
    durable_watermark: u64,
    generation: u64,
}

#[derive(Clone)]
struct LocalLiveWire {
    bytes: Arc<[u8]>,
    lagged: bool,
}

impl LocalLivePublisher {
    fn publish(&mut self, event: &SessionEvent) -> Option<(u64, tokio::time::Instant)> {
        self.latest_sequence = self.latest_sequence.max(event.sequence);
        let (session_id, kind) = preview_event_kind(event);
        let text_start = match kind {
            SessionEventKind::ReasoningTextDelta { delta } => {
                let start = self.reasoning_bytes.entry(session_id).or_default();
                let previous = *start;
                *start += delta.len();
                Some(previous)
            }
            SessionEventKind::MessageDelta { message_id, delta } => {
                let start = self
                    .message_bytes
                    .entry((session_id, *message_id))
                    .or_default();
                let previous = *start;
                *start += delta.len();
                Some(previous)
            }
            SessionEventKind::ReasoningDelta { text } => {
                self.reasoning_bytes.insert(session_id, text.len());
                self.reasoning_snapshots.insert(session_id, event.clone());
                None
            }
            SessionEventKind::Message {
                message_id,
                actor: EventActor::Assistant,
                text,
                status,
                ..
            } => {
                if *status == MessageStatus::InProgress {
                    self.message_bytes
                        .insert((session_id, *message_id), text.len());
                    self.message_snapshots
                        .insert((session_id, *message_id), event.clone());
                } else {
                    self.message_bytes.remove(&(session_id, *message_id));
                    self.message_snapshots.remove(&(session_id, *message_id));
                }
                self.reasoning_bytes.remove(&session_id);
                self.reasoning_snapshots.remove(&session_id);
                None
            }
            SessionEventKind::ReasoningCompleted
            | SessionEventKind::ToolStarted { .. }
            | SessionEventKind::ToolUpdated { .. }
            | SessionEventKind::ToolCompleted { .. } => {
                self.reasoning_bytes.remove(&session_id);
                self.reasoning_snapshots.remove(&session_id);
                None
            }
            SessionEventKind::TurnStarted { .. }
            | SessionEventKind::TurnCompleted { .. }
            | SessionEventKind::ContextCleared => {
                self.reasoning_bytes.remove(&session_id);
                self.message_bytes
                    .retain(|(owner, _), _| *owner != session_id);
                self.reasoning_snapshots.remove(&session_id);
                self.message_snapshots
                    .retain(|(owner, _), _| *owner != session_id);
                None
            }
            _ => None,
        };
        if self.events.receiver_count() == 0 {
            self.pending_delta = None;
            self.last_delta_sent_at = None;
            self.last_delta_key = None;
            return None;
        }
        let key = live_delta_key(event);
        if let (Some(key), Some(_), Some(pending)) =
            (key.as_ref(), text_start, self.pending_delta.as_mut())
            && pending.key == *key
            && live_delta_len(&pending.event).saturating_add(live_delta_len(event))
                <= LOCAL_LIVE_DELTA_BATCH_MAX_BYTES
            && append_live_delta(&mut pending.event, event)
        {
            return None;
        }
        if self.pending_delta.is_some() {
            self.flush_pending_delta();
            self.last_delta_sent_at = None;
            self.last_delta_key = None;
        }
        if let (Some(key), Some(start)) = (key, text_start) {
            let now = tokio::time::Instant::now();
            if self
                .last_delta_sent_at
                .is_some_and(|last| now.duration_since(last) < LOCAL_LIVE_DELTA_BATCH_DELAY)
                && self.last_delta_key.as_ref() == Some(&key)
                && live_delta_len(event) <= LOCAL_LIVE_DELTA_BATCH_MAX_BYTES
            {
                self.pending_generation = self.pending_generation.wrapping_add(1);
                let generation = self.pending_generation;
                let deadline = now + LOCAL_LIVE_DELTA_BATCH_DELAY;
                self.pending_delta = Some(PendingLiveDelta {
                    key,
                    event: event.clone(),
                    text_start: start,
                    durable_watermark: self.latest_sequence,
                    generation,
                });
                return Some((generation, deadline));
            }
            self.send_event(event.clone(), Some(start), self.latest_sequence);
            self.last_delta_sent_at = Some(now);
            self.last_delta_key = Some(key);
        } else {
            self.send_event(event.clone(), text_start, self.latest_sequence);
            self.last_delta_sent_at = None;
            self.last_delta_key = None;
        }
        None
    }

    fn flush_pending_delta(&mut self) {
        if let Some(pending) = self.pending_delta.take() {
            self.last_delta_key = Some(pending.key);
            self.send_event(
                pending.event,
                Some(pending.text_start),
                pending.durable_watermark,
            );
            self.last_delta_sent_at = Some(tokio::time::Instant::now());
        }
    }

    fn flush_pending_generation(&mut self, generation: u64) {
        if self
            .pending_delta
            .as_ref()
            .is_some_and(|pending| pending.generation == generation)
        {
            self.flush_pending_delta();
        }
    }

    fn send_event(&self, event: SessionEvent, text_start: Option<usize>, durable_watermark: u64) {
        let frame = LocalLiveFrame::Event {
            event,
            text_start,
            durable_watermark,
        };
        let wire = encode_live_frame(&frame)
            .ok()
            .filter(|bytes| bytes.len() <= LOCAL_LIVE_FRAME_MAX_BYTES)
            .map(|bytes| LocalLiveWire {
                bytes: Arc::from(bytes),
                lagged: false,
            })
            .unwrap_or_else(|| LocalLiveWire {
                bytes: Arc::from(
                    encode_live_frame(&LocalLiveFrame::Lagged)
                        .expect("static lag marker is serializable"),
                ),
                lagged: true,
            });
        let _ = self.events.send(wire);
    }

    fn snapshots(&self) -> Vec<SessionEvent> {
        let mut snapshots = self
            .reasoning_snapshots
            .values()
            .chain(self.message_snapshots.values())
            .cloned()
            .collect::<Vec<_>>();
        snapshots.sort_by_key(|event| (event.created_at, event.id));
        snapshots
    }
}

fn live_delta_key(event: &SessionEvent) -> Option<LiveDeltaKey> {
    if event.sequence != 0 {
        return None;
    }
    match &event.kind {
        SessionEventKind::ReasoningTextDelta { .. } => {
            Some(LiveDeltaKey::Reasoning(event.session_id))
        }
        SessionEventKind::MessageDelta { message_id, .. } => {
            Some(LiveDeltaKey::Message(event.session_id, *message_id))
        }
        _ => None,
    }
}

fn live_delta_len(event: &SessionEvent) -> usize {
    match &event.kind {
        SessionEventKind::ReasoningTextDelta { delta }
        | SessionEventKind::MessageDelta { delta, .. } => delta.len(),
        _ => 0,
    }
}

fn append_live_delta(pending: &mut SessionEvent, event: &SessionEvent) -> bool {
    match (&mut pending.kind, &event.kind) {
        (
            SessionEventKind::ReasoningTextDelta { delta: text },
            SessionEventKind::ReasoningTextDelta { delta },
        )
        | (
            SessionEventKind::MessageDelta { delta: text, .. },
            SessionEventKind::MessageDelta { delta, .. },
        ) => {
            text.push_str(delta);
            true
        }
        _ => false,
    }
}

fn preview_event_kind(event: &SessionEvent) -> (Uuid, &SessionEventKind) {
    match &event.kind {
        SessionEventKind::SubagentActivity {
            event: Some(child), ..
        } => (child.session_id, &child.kind),
        kind => (event.session_id, kind),
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct LocalSessionOwnerMetadata {
    schema_version: u8,
    pid: u32,
    executable_identity: String,
    #[serde(default)]
    process_start_time: Option<u64>,
}

/// Path used by additional local terminals to attach to a session owner.
pub fn session_control_socket_path(sessions_dir: &Path, session_id: Uuid) -> PathBuf {
    sessions_dir.join(format!("{session_id}.control.sock"))
}

/// Private, short-lived presence channel used by attached terminals. Keeping
/// this separate from the command socket means a viewer can be counted even
/// while it is idle and has not sent a command recently.
pub fn session_control_presence_socket_path(sessions_dir: &Path, session_id: Uuid) -> PathBuf {
    sessions_dir.join(format!("{session_id}.control.presence.sock"))
}

fn session_control_owner_path(sessions_dir: &Path, session_id: Uuid) -> PathBuf {
    sessions_dir.join(format!("{session_id}.control.owner.json"))
}

/// Whether the process holding this local session's writer lease is running
/// the exact same Borg executable as the caller.
///
/// Older Borg owners did not publish metadata. Treating absent, stale, or
/// malformed metadata as a mismatch prevents a newly installed CLI from
/// silently attaching its terminal to an obsolete long-lived process.
pub fn local_session_owner_uses_current_binary(
    sessions_dir: &Path,
    session_id: Uuid,
) -> Result<bool> {
    let Some(metadata) = read_local_session_owner_metadata(sessions_dir, session_id)? else {
        return Ok(false);
    };
    if !owner_process_matches_metadata(&metadata)? {
        return Ok(false);
    }
    Ok(metadata.executable_identity == current_executable_identity()?)
}

/// Whether the recorded local session owner process is still alive, regardless
/// of which Borg frontend binary is asking.
pub fn local_session_owner_is_active(sessions_dir: &Path, session_id: Uuid) -> Result<bool> {
    let Some(metadata) = read_local_session_owner_metadata(sessions_dir, session_id)? else {
        return Ok(false);
    };
    owner_process_matches_metadata(&metadata)
}

fn read_local_session_owner_metadata(
    sessions_dir: &Path,
    session_id: Uuid,
) -> Result<Option<LocalSessionOwnerMetadata>> {
    let path = session_control_owner_path(sessions_dir, session_id);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {}", path.display()));
        }
    };
    let metadata: LocalSessionOwnerMetadata = match serde_json::from_slice(&bytes) {
        Ok(metadata) => metadata,
        Err(error) => {
            tracing::warn!(%error, path = %path.display(), "invalid local session owner metadata");
            return Ok(None);
        }
    };
    if metadata.schema_version != 1 {
        return Ok(None);
    }
    Ok(Some(metadata))
}

fn current_executable_identity() -> Result<String> {
    static IDENTITY: OnceLock<String> = OnceLock::new();
    if let Some(identity) = IDENTITY.get() {
        return Ok(identity.clone());
    }
    let executable = std::env::current_exe().context("failed to locate the Borg executable")?;
    let metadata = fs::metadata(&executable)
        .with_context(|| format!("failed to identify {}", executable.display()))?;
    let identity = executable_identity(&metadata);
    IDENTITY.set(identity.clone()).ok();
    Ok(identity)
}

fn executable_identity(metadata: &fs::Metadata) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        format!(
            "{}:{}:{}:{}:{}",
            metadata.dev(),
            metadata.ino(),
            metadata.len(),
            metadata.mtime(),
            metadata.mtime_nsec()
        )
    }
    #[cfg(not(unix))]
    {
        format!("{}:{:?}", metadata.len(), metadata.modified().ok())
    }
}

#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    let result = unsafe { libc::kill(pid, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(not(unix))]
fn process_is_alive(_pid: u32) -> bool {
    true
}

#[cfg(target_os = "linux")]
fn process_executable_identity(pid: u32) -> Result<Option<String>> {
    let path = PathBuf::from("/proc").join(pid.to_string()).join("exe");
    let file = match fs::File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to inspect {}", path.display()));
        }
    };
    Ok(Some(executable_identity(&file.metadata()?)))
}

#[cfg(target_os = "linux")]
fn process_start_time(pid: u32) -> Result<Option<u64>> {
    let path = PathBuf::from("/proc").join(pid.to_string()).join("stat");
    let contents = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to inspect {}", path.display()));
        }
    };
    let Some((_, fields)) = contents.rsplit_once(") ") else {
        return Ok(None);
    };
    Ok(fields
        .split_whitespace()
        .nth(19)
        .and_then(|value| value.parse().ok()))
}

#[cfg(not(target_os = "linux"))]
fn process_start_time(_pid: u32) -> Result<Option<u64>> {
    Ok(None)
}

fn owner_process_matches_metadata(metadata: &LocalSessionOwnerMetadata) -> Result<bool> {
    if !process_is_alive(metadata.pid) {
        return Ok(false);
    }
    #[cfg(target_os = "linux")]
    {
        let Some(identity) = process_executable_identity(metadata.pid)? else {
            return Ok(false);
        };
        if identity != metadata.executable_identity {
            return Ok(false);
        }
        if let Some(expected) = metadata.process_start_time
            && process_start_time(metadata.pid)? != Some(expected)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Return the obsolete owner only when its metadata, executable identity, and
/// open file descriptors prove that it owns this exact session lock.
#[cfg(target_os = "linux")]
pub fn obsolete_local_session_owner_pid(
    sessions_dir: &Path,
    session_id: Uuid,
    lock_path: &Path,
) -> Result<Option<u32>> {
    let Some(metadata) = read_local_session_owner_metadata(sessions_dir, session_id)? else {
        return Ok(None);
    };
    if metadata.executable_identity == current_executable_identity()?
        || !owner_process_matches_metadata(&metadata)?
    {
        return Ok(None);
    }
    #[cfg(target_os = "linux")]
    if !process_tree_holds_lock(metadata.pid, lock_path)? {
        return Ok(None);
    }
    Ok(Some(metadata.pid))
}

#[cfg(not(target_os = "linux"))]
pub fn obsolete_local_session_owner_pid(
    _sessions_dir: &Path,
    _session_id: Uuid,
    _lock_path: &Path,
) -> Result<Option<u32>> {
    Ok(None)
}

#[cfg(target_os = "linux")]
fn process_holds_lock(pid: u32, lock_path: &Path) -> Result<bool> {
    use std::os::unix::fs::MetadataExt;

    let lock_metadata = match fs::metadata(lock_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to inspect {}", lock_path.display()));
        }
    };
    let fd_directory = PathBuf::from("/proc").join(pid.to_string()).join("fd");
    let entries = match fs::read_dir(&fd_directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return Ok(false),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to inspect {}", fd_directory.display()));
        }
    };
    for entry in entries.flatten() {
        let Ok(metadata) = fs::metadata(entry.path()) else {
            continue;
        };
        if metadata.dev() == lock_metadata.dev() && metadata.ino() == lock_metadata.ino() {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(target_os = "linux")]
fn process_tree_pids(root_pid: u32) -> Vec<u32> {
    let mut pending = vec![root_pid];
    let mut pids = Vec::new();
    while let Some(pid) = pending.pop() {
        if pids.contains(&pid) {
            continue;
        }
        pids.push(pid);
        let children_path = PathBuf::from("/proc")
            .join(pid.to_string())
            .join("task")
            .join(pid.to_string())
            .join("children");
        let Ok(children) = fs::read_to_string(children_path) else {
            continue;
        };
        pending.extend(
            children
                .split_whitespace()
                .filter_map(|child| child.parse::<u32>().ok()),
        );
    }
    pids
}

#[cfg(target_os = "linux")]
fn process_tree_holds_lock(pid: u32, lock_path: &Path) -> Result<bool> {
    for candidate in process_tree_pids(pid) {
        if process_holds_lock(candidate, lock_path)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Force-stop a verified obsolete local session owner.
#[cfg(unix)]
pub fn force_terminate_local_session_owner(pid: u32) -> Result<()> {
    #[cfg(target_os = "linux")]
    for child_pid in process_tree_pids(pid).into_iter().rev() {
        if child_pid != pid {
            terminate_local_session_process(child_pid)?;
        }
    }
    terminate_local_session_process(pid)
}

#[cfg(unix)]
fn terminate_local_session_process(pid: u32) -> Result<()> {
    let pid = i32::try_from(pid).context("obsolete local session owner PID is invalid")?;
    anyhow::ensure!(pid > 1, "refusing to terminate a system process");
    anyhow::ensure!(
        pid as u32 != std::process::id(),
        "refusing to terminate Borg itself"
    );
    let result = unsafe { libc::kill(pid, libc::SIGKILL) };
    if result == -1 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(error).with_context(|| format!("failed to terminate process {pid}"));
        }
    }
    Ok(())
}

#[cfg(not(unix))]
pub fn force_terminate_local_session_owner(_pid: u32) -> Result<()> {
    bail!("local session owner termination is only supported on Unix")
}

#[cfg(unix)]
fn write_local_session_owner_metadata(sessions_dir: &Path, session_id: Uuid) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let path = session_control_owner_path(sessions_dir, session_id);
    let temporary = sessions_dir.join(format!(
        ".{session_id}.control.owner.{}.tmp",
        std::process::id()
    ));
    let metadata = LocalSessionOwnerMetadata {
        schema_version: 1,
        pid: std::process::id(),
        executable_identity: current_executable_identity()?,
        process_start_time: process_start_time(std::process::id())?,
    };
    fs::write(&temporary, serde_json::to_vec(&metadata)?)
        .with_context(|| format!("failed to write {}", temporary.display()))?;
    fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("failed to secure {}", temporary.display()))?;
    fs::rename(&temporary, &path)
        .with_context(|| format!("failed to publish {}", path.display()))?;
    Ok(())
}

/// Whether a session's control socket currently has a listener.
///
/// A `<session>.control.sock` file outlives the process that created it, so
/// its mere existence reports long-dead sessions as reachable. Connecting is
/// the only honest test: a stale path refuses immediately, and this is the
/// same thing `send_local_session_command` learns when it dispatches, so
/// discovery and delivery agree instead of contradicting each other.
///
/// The owner process being alive is NOT sufficient either — a running owner
/// that has stopped serving its socket still refuses connections.
#[cfg(unix)]
pub async fn session_control_socket_is_reachable(socket_path: &Path) -> bool {
    use tokio::net::UnixStream;

    // Bounded so one unresponsive peer cannot stall a whole listing.
    matches!(
        tokio::time::timeout(
            std::time::Duration::from_millis(250),
            UnixStream::connect(socket_path),
        )
        .await,
        Ok(Ok(_))
    )
}

#[cfg(not(unix))]
pub async fn session_control_socket_is_reachable(_socket_path: &Path) -> bool {
    false
}

/// Send one typed command to the process holding a session's writer lease.
#[cfg(unix)]
pub async fn send_local_session_command(
    socket_path: &Path,
    session_id: Uuid,
    command: HostCommand,
) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixStream;

    anyhow::ensure!(
        command.session_id() == Some(session_id),
        "command targets a different session"
    );
    let mut stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("failed to connect to {}", socket_path.display()))?;
    stream.write_all(&serde_json::to_vec(&command)?).await?;
    stream.shutdown().await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let response: serde_json::Value =
        serde_json::from_slice(&response).context("session owner returned invalid control JSON")?;
    if let Some(error) = response.get("error").and_then(serde_json::Value::as_str) {
        bail!("session owner rejected command: {error}");
    }
    anyhow::ensure!(
        response.get("ok").and_then(serde_json::Value::as_bool) == Some(true),
        "session owner did not acknowledge command"
    );
    Ok(())
}

#[cfg(not(unix))]
pub async fn send_local_session_command(
    _socket_path: &Path,
    _session_id: Uuid,
    _command: HostCommand,
) -> Result<()> {
    bail!("local session control is only supported on Unix")
}

/// Single-owner local command endpoint for a durable session.
///
/// The session lock remains exclusive. Additional terminals tail journal
/// events and send typed commands through this endpoint.
#[cfg(unix)]
pub struct LocalSessionControlServer {
    task: tokio::task::JoinHandle<()>,
    attached_viewers: Arc<AtomicUsize>,
    live: Arc<Mutex<LocalLivePublisher>>,
    shutdown: watch::Sender<bool>,
}

#[cfg(not(unix))]
pub struct LocalSessionControlServer;

#[cfg(not(unix))]
impl LocalSessionControlServer {
    pub fn start(
        _socket_path: PathBuf,
        _session_id: Uuid,
        _writer: &SessionWriterLease,
        _commands: mpsc::Sender<HostCommand>,
    ) -> Result<Self> {
        Ok(Self)
    }

    pub fn start_with_prompt_admissions(
        socket_path: PathBuf,
        session_id: Uuid,
        _writer: &SessionWriterLease,
        commands: mpsc::Sender<HostCommand>,
        _prompt_admissions: Option<Arc<Mutex<HashSet<Uuid>>>>,
    ) -> Result<Self> {
        Self::start(socket_path, session_id, _writer, commands)
    }

    pub fn start_with_durable_prompt_admissions(
        socket_path: PathBuf,
        session_id: Uuid,
        _writer: &SessionWriterLease,
        commands: mpsc::Sender<HostCommand>,
        _prompt_admissions: Option<Arc<Mutex<HashSet<Uuid>>>>,
        _store: Arc<dyn SessionStore>,
    ) -> Result<Self> {
        Self::start(socket_path, session_id, _writer, commands)
    }

    pub fn has_attached_viewers(&self) -> bool {
        false
    }

    pub fn publish_live_event(&self, _event: &SessionEvent) {}

    pub fn seed_durable_watermark(&self, _sequence: u64) {}
}

#[cfg(unix)]
impl LocalSessionControlServer {
    pub fn start(
        socket_path: PathBuf,
        session_id: Uuid,
        writer: &SessionWriterLease,
        commands: mpsc::Sender<HostCommand>,
    ) -> Result<Self> {
        Self::start_with_prompt_admissions(socket_path, session_id, writer, commands, None)
    }

    pub fn start_with_prompt_admissions(
        socket_path: PathBuf,
        session_id: Uuid,
        _writer: &SessionWriterLease,
        commands: mpsc::Sender<HostCommand>,
        prompt_admissions: Option<Arc<Mutex<HashSet<Uuid>>>>,
    ) -> Result<Self> {
        Self::start_control_server(socket_path, session_id, commands, prompt_admissions, None)
    }

    pub fn start_with_durable_prompt_admissions(
        socket_path: PathBuf,
        session_id: Uuid,
        _writer: &SessionWriterLease,
        commands: mpsc::Sender<HostCommand>,
        prompt_admissions: Option<Arc<Mutex<HashSet<Uuid>>>>,
        store: Arc<dyn SessionStore>,
    ) -> Result<Self> {
        Self::start_control_server(
            socket_path,
            session_id,
            commands,
            prompt_admissions,
            Some(store),
        )
    }

    fn start_control_server(
        socket_path: PathBuf,
        session_id: Uuid,
        commands: mpsc::Sender<HostCommand>,
        prompt_admissions: Option<Arc<Mutex<HashSet<Uuid>>>>,
        store: Option<Arc<dyn SessionStore>>,
    ) -> Result<Self> {
        use std::os::unix::fs::PermissionsExt;
        use tokio::net::UnixListener;

        if socket_path.exists() {
            fs::remove_file(&socket_path)
                .with_context(|| format!("failed to remove stale {}", socket_path.display()))?;
        }
        let listener = UnixListener::bind(&socket_path)
            .with_context(|| format!("failed to bind {}", socket_path.display()))?;
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to secure {}", socket_path.display()))?;
        let presence_socket_path = session_control_presence_socket_path(
            socket_path.parent().unwrap_or_else(|| Path::new(".")),
            session_id,
        );
        if presence_socket_path.exists() {
            fs::remove_file(&presence_socket_path).with_context(|| {
                format!("failed to remove stale {}", presence_socket_path.display())
            })?;
        }
        let presence_listener = UnixListener::bind(&presence_socket_path)
            .with_context(|| format!("failed to bind {}", presence_socket_path.display()))?;
        fs::set_permissions(&presence_socket_path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to secure {}", presence_socket_path.display()))?;
        write_local_session_owner_metadata(
            socket_path.parent().unwrap_or_else(|| Path::new(".")),
            session_id,
        )?;
        let task_socket_path = socket_path.clone();
        let attached_viewers = Arc::new(AtomicUsize::new(0));
        let task_attached_viewers = Arc::clone(&attached_viewers);
        let (live_events, _) = broadcast::channel(LOCAL_LIVE_EVENT_BUFFER);
        let live = Arc::new(Mutex::new(LocalLivePublisher {
            events: live_events,
            latest_sequence: 0,
            reasoning_bytes: HashMap::new(),
            message_bytes: HashMap::new(),
            reasoning_snapshots: HashMap::new(),
            message_snapshots: HashMap::new(),
            pending_delta: None,
            pending_generation: 0,
            last_delta_sent_at: None,
            last_delta_key: None,
        }));
        let task_live = Arc::clone(&live);
        let (shutdown, _) = watch::channel(false);
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, _)) => {
                                let commands = commands.clone();
                                let prompt_admissions = prompt_admissions.clone();
                                let store = store.clone();
                                tokio::spawn(handle_control_connection(
                                    stream,
                                    session_id,
                                    commands,
                                    prompt_admissions,
                                    store,
                                ));
                            }
                            Err(error) => {
                                tracing::warn!(
                                    %error,
                                    socket_path = %task_socket_path.display(),
                                    "local session control listener stopped"
                                );
                                break;
                            }
                        }
                    }
                    result = presence_listener.accept() => {
                        match result {
                            Ok((stream, _)) => {
                                let attached_viewers = Arc::clone(&task_attached_viewers);
                                let live = Arc::clone(&task_live);
                                let mut shutdown = task_shutdown.subscribe();
                                tokio::spawn(async move {
                                    use tokio::io::{AsyncReadExt, AsyncWriteExt};

                                    attached_viewers.fetch_add(1, Ordering::AcqRel);
                                    // The acknowledgement makes the attachment
                                    // visible before its session loop proceeds.
                                    let (mut reader, mut writer) = stream.into_split();
                                    if writer.write_all(&[2]).await.is_err() {
                                        attached_viewers.fetch_sub(1, Ordering::AcqRel);
                                        return;
                                    }
                                    let mut byte = [0_u8; 1];
                                    let opted_in = tokio::select! {
                                        read = reader.read_exact(&mut byte) => read.is_ok() && byte == [2],
                                        _ = shutdown.changed() => false,
                                    };
                                    if opted_in {
                                        let (latest_sequence, snapshots, mut live_events) = {
                                            let mut live = live.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                                            live.flush_pending_delta();
                                            (live.latest_sequence, live.snapshots(), live.events.subscribe())
                                        };
                                        let hello = LocalLiveFrame::Hello { latest_sequence, snapshots };
                                        let hello_sent = tokio::select! {
                                            sent = write_live_frame(&mut writer, &hello) => sent.is_ok(),
                                            _ = shutdown.changed() => false,
                                        };
                                        if hello_sent {
                                            loop {
                                                tokio::select! {
                                                    _ = shutdown.changed() => break,
                                                    read = reader.read(&mut byte) => {
                                                        if !matches!(read, Ok(1..)) {
                                                            break;
                                                        }
                                                    }
                                                    received = live_events.recv() => {
                                                        let frame = match received {
                                                            Ok(frame) => frame,
                                                            Err(broadcast::error::RecvError::Lagged(_)) => LocalLiveWire {
                                                                bytes: Arc::from(encode_live_frame(&LocalLiveFrame::Lagged).expect("static lag marker is serializable")),
                                                                lagged: true,
                                                            },
                                                            Err(broadcast::error::RecvError::Closed) => break,
                                                        };
                                                        let sent = tokio::select! {
                                                            sent = writer.write_all(&frame.bytes) => sent.is_ok(),
                                                            _ = shutdown.changed() => false,
                                                        };
                                                        if !sent || frame.lagged {
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    } else if !*shutdown.borrow() {
                                        // Older attached terminals keep this socket open only
                                        // for presence. They never subscribe to event traffic.
                                        loop {
                                            tokio::select! {
                                                _ = shutdown.changed() => break,
                                                read = reader.read(&mut byte) => {
                                                    if !matches!(read, Ok(1..)) { break; }
                                                }
                                            }
                                        }
                                    }
                                    attached_viewers.fetch_sub(1, Ordering::AcqRel);
                                });
                            }
                            Err(error) => {
                                tracing::warn!(
                                    %error,
                                    socket_path = %presence_socket_path.display(),
                                    "local session presence listener stopped"
                                );
                                break;
                            }
                        }
                    }
                }
            }
        });
        Ok(Self {
            task,
            attached_viewers,
            live,
            shutdown,
        })
    }

    pub fn has_attached_viewers(&self) -> bool {
        self.attached_viewers.load(Ordering::Acquire) > 0
    }

    pub fn publish_live_event(&self, event: &SessionEvent) {
        let pending = self
            .live
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .publish(event);
        if let Some((generation, deadline)) = pending {
            let live = Arc::clone(&self.live);
            tokio::spawn(async move {
                tokio::time::sleep_until(deadline).await;
                live.lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .flush_pending_generation(generation);
            });
        }
    }

    pub fn seed_durable_watermark(&self, sequence: u64) {
        let mut live = self
            .live
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        live.latest_sequence = live.latest_sequence.max(sequence);
    }
}

#[cfg(unix)]
async fn write_live_frame<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &LocalLiveFrame,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    let bytes = encode_live_frame(frame)?;
    anyhow::ensure!(
        bytes.len() <= LOCAL_LIVE_FRAME_MAX_BYTES,
        "local live event is too large"
    );
    writer.write_all(&bytes).await?;
    Ok(())
}

fn encode_live_frame(frame: &LocalLiveFrame) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(frame)?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(unix)]
async fn handle_control_connection(
    mut stream: tokio::net::UnixStream,
    session_id: Uuid,
    commands: mpsc::Sender<HostCommand>,
    prompt_admissions: Option<Arc<Mutex<HashSet<Uuid>>>>,
    store: Option<Arc<dyn SessionStore>>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let result = async {
        let mut payload = Vec::new();
        (&mut stream)
            .take(MAX_CONTROL_COMMAND_BYTES + 1)
            .read_to_end(&mut payload)
            .await?;
        anyhow::ensure!(
            payload.len() as u64 <= MAX_CONTROL_COMMAND_BYTES,
            "control command exceeds the 1 MiB limit"
        );
        let command: HostCommand = serde_json::from_slice(&payload)?;
        if command.session_id() != Some(session_id) {
            bail!("command targets a different session");
        }
        if let HostCommand::Prompt {
            message_id,
            text,
            attachments,
            delivery,
            ..
        } = &command
        {
            let store = store
                .as_ref()
                .context("session owner cannot accept prompts without a durable journal")?;
            let workspace_durable = match store.workspace_store().await? {
                Some(workspace) => workspace.contains_message(*message_id).await?,
                None => false,
            };
            if !workspace_durable {
                store
                    .admit_prompt(SessionEvent::new(
                        session_id,
                        0,
                        SessionEventKind::Message {
                            message_id: *message_id,
                            actor: EventActor::User,
                            text: text.clone(),
                            attachments: attachments.clone(),
                            status: MessageStatus::Queued,
                            delivery: Some(*delivery),
                        },
                    ))
                    .await?;
            }
            if let Some(admissions) = prompt_admissions.as_ref() {
                admissions
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(*message_id);
            }
        }
        if matches!(command, HostCommand::FlushPendingInput { .. }) {
            let store = store
                .as_ref()
                .context("session owner cannot recover pending input without a durable journal")?;
            // Admission can succeed before the original control handoff fails.
            // Re-send those identities before flushing; the actor deduplicates
            // prompts already present in its queue or active turn.
            for action in store.pending_actions(session_id, usize::MAX).await? {
                if !matches!(
                    action.kind,
                    crate::SessionActionKind::Prompt
                        | crate::SessionActionKind::Steering
                        | crate::SessionActionKind::FollowUp
                ) || !matches!(
                    action.state,
                    crate::SessionActionState::Queued | crate::SessionActionState::Admitted
                ) {
                    continue;
                }
                let text = action
                    .payload
                    .get("text")
                    .and_then(serde_json::Value::as_str)
                    .context("pending prompt is missing its text")?
                    .to_owned();
                let attachments = serde_json::from_value(
                    action
                        .payload
                        .get("attachments")
                        .cloned()
                        .context("pending prompt is missing its attachments")?,
                )?;
                commands
                    .send(HostCommand::Prompt {
                        session_id,
                        message_id: action.action_id,
                        text,
                        attachments,
                        output_schema: action.payload.get("output_schema").cloned(),
                        delivery: crate::PromptDelivery::Steer,
                    })
                    .await
                    .map_err(|_| anyhow::anyhow!("session owner stopped"))?;
            }
        }
        commands
            .send(command)
            .await
            .map_err(|_| anyhow::anyhow!("session owner stopped"))?;
        Result::<()>::Ok(())
    }
    .await;
    let response = match result {
        Ok(()) => serde_json::json!({ "ok": true }),
        Err(error) => serde_json::json!({ "error": error.to_string() }),
    };
    let _ = stream.write_all(response.to_string().as_bytes()).await;
    let _ = stream.shutdown().await;
}

#[cfg(unix)]
impl Drop for LocalSessionControlServer {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
        self.task.abort();
        // Leave the path in place. The next journal owner safely reclaims a
        // refused socket; unlinking here could remove a successor's endpoint
        // after this owner releases its journal lease.
    }
}

/// Run the read-only side of a local terminal attachment.
///
/// `Stop` detaches only this terminal. All other commands are acknowledged by
/// the owning process before this adapter continues.
#[cfg(unix)]
pub async fn run_attached_session(
    store: Arc<dyn SessionStore>,
    session_id: Uuid,
    lock_path: PathBuf,
    socket_path: PathBuf,
    last_sequence: u64,
    mut commands: mpsc::Receiver<HostCommand>,
    events: mpsc::Sender<SessionEvent>,
) -> Result<()> {
    let presence_socket_path = session_control_presence_socket_path(
        socket_path.parent().unwrap_or_else(|| Path::new(".")),
        session_id,
    );
    let command_events = events.clone();
    let mut event_forwarder = tokio::spawn(forward_attached_events(
        Arc::clone(&store),
        session_id,
        lock_path.clone(),
        presence_socket_path,
        last_sequence,
        events,
    ));
    loop {
        tokio::select! {
            result = &mut event_forwarder => {
                return result.context("attached session event forwarder failed")?;
            }
            command = commands.recv() => {
                let Some(command) = command else {
                    event_forwarder.abort();
                    return Ok(())
                };
                if matches!(command, HostCommand::Stop { .. }) {
                    event_forwarder.abort();
                    return Ok(());
                }
                match forward_attached_command(&lock_path, &socket_path, command).await {
                    Ok(true) => {
                        event_forwarder.abort();
                        tracing::info!(
                            lock_path = %lock_path.display(),
                            "local session owner released its writer lease; detaching terminal"
                        );
                        return Ok(());
                    }
                    Ok(false) => {}
                    Err(error) => {
                        if writer_is_active(&lock_path)? {
                            tracing::warn!(
                                %error,
                                %session_id,
                                "local session owner command channel is unavailable; keeping the attached transcript open"
                            );
                            let _ = command_events
                                .send(SessionEvent::new(
                                    session_id,
                                    0,
                                    SessionEventKind::Error {
                                        message: format!(
                                            "The active session owner is not accepting commands yet: {error:#}"
                                        ),
                                    },
                                ))
                                .await;
                        } else {
                            event_forwarder.abort();
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

#[cfg(unix)]
async fn forward_attached_command(
    lock_path: &Path,
    socket_path: &Path,
    command: HostCommand,
) -> Result<bool> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixStream;

    if !writer_is_active(lock_path)? {
        return Ok(true);
    }
    let mut stream = match UnixStream::connect(socket_path).await {
        Ok(stream) => stream,
        Err(_) if !writer_is_active(lock_path)? => return Ok(true),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "session writer is active but its local control channel is unavailable ({})",
                    socket_path.display()
                )
            });
        }
    };
    stream.write_all(&serde_json::to_vec(&command)?).await?;
    stream.shutdown().await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let response: serde_json::Value = serde_json::from_slice(&response)
        .context("session owner returned an invalid control response")?;
    if let Some(error) = response.get("error").and_then(serde_json::Value::as_str) {
        bail!("session owner rejected command: {error}");
    }
    anyhow::ensure!(
        response.get("ok").and_then(serde_json::Value::as_bool) == Some(true),
        "session owner did not acknowledge command"
    );
    Ok(false)
}

#[cfg(unix)]
struct LocalLiveConnection {
    reader: tokio::io::BufReader<tokio::net::UnixStream>,
    latest_sequence: u64,
    snapshots: Vec<SessionEvent>,
}

#[cfg(unix)]
enum LocalLiveConnect {
    Connected(LocalLiveConnection),
    Unsupported(tokio::net::UnixStream),
    Unavailable,
}

#[cfg(unix)]
async fn connect_local_live_stream(path: &Path) -> LocalLiveConnect {
    use tokio::io::AsyncReadExt;

    let stream = match tokio::time::timeout(
        std::time::Duration::from_millis(250),
        tokio::net::UnixStream::connect(path),
    )
    .await
    {
        Ok(Ok(stream)) => stream,
        _ => return LocalLiveConnect::Unavailable,
    };
    let mut reader = tokio::io::BufReader::new(stream);
    let mut acknowledgement = [0_u8; 1];
    if !matches!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            reader.read_exact(&mut acknowledgement),
        )
        .await,
        Ok(Ok(_))
    ) {
        return LocalLiveConnect::Unavailable;
    }
    if acknowledgement != [2] {
        return LocalLiveConnect::Unsupported(reader.into_inner());
    }
    use tokio::io::AsyncWriteExt;
    if reader.get_mut().write_all(&[2]).await.is_err() {
        return LocalLiveConnect::Unavailable;
    }
    match tokio::time::timeout(
        std::time::Duration::from_millis(250),
        read_live_frame(&mut reader),
    )
    .await
    {
        Ok(Ok(Some(LocalLiveFrame::Hello {
            latest_sequence,
            snapshots,
        }))) => LocalLiveConnect::Connected(LocalLiveConnection {
            reader,
            latest_sequence,
            snapshots,
        }),
        _ => LocalLiveConnect::Unavailable,
    }
}

#[cfg(unix)]
async fn read_live_frame(
    reader: &mut tokio::io::BufReader<tokio::net::UnixStream>,
) -> Result<Option<LocalLiveFrame>> {
    use tokio::io::AsyncBufReadExt;

    let mut bytes = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            anyhow::ensure!(bytes.is_empty(), "truncated local live event");
            return Ok(None);
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |index| index + 1);
        anyhow::ensure!(
            bytes.len().saturating_add(consumed) <= LOCAL_LIVE_FRAME_MAX_BYTES,
            "local live event exceeds frame limit"
        );
        bytes.extend_from_slice(&available[..consumed]);
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(Some(serde_json::from_slice(&bytes)?));
        }
    }
}

#[derive(Default)]
struct ReasoningPreviewCursor {
    text: String,
    known: bool,
    raw_sent: bool,
}

#[derive(Default)]
struct AttachedPreviewCursor {
    reasoning: HashMap<Uuid, ReasoningPreviewCursor>,
    messages: HashMap<(Uuid, Uuid), String>,
    fresh_turns: HashSet<Uuid>,
}

impl AttachedPreviewCursor {
    fn require_snapshot_for_existing_streams(&mut self) {
        for reasoning in self.reasoning.values_mut() {
            reasoning.known = false;
        }
        self.messages.clear();
        self.fresh_turns.clear();
    }

    fn accept_store_snapshot(&mut self, event: SessionEvent) -> Option<SessionEvent> {
        let (session_id, kind) = preview_event_kind(&event);
        let result = if let SessionEventKind::ReasoningDelta { .. } = kind {
            let cursor = self.reasoning.entry(session_id).or_default();
            if cursor.raw_sent {
                Some(event.clone())
            } else {
                reasoning_delta_from_snapshot(event.clone(), &mut cursor.text)
            }
        } else {
            Some(event.clone())
        };
        self.accept(event, None)?;
        result
    }

    fn accept(
        &mut self,
        mut event: SessionEvent,
        text_start: Option<usize>,
    ) -> Option<SessionEvent> {
        let (session_id, kind) = preview_event_kind_mut(&mut event);
        match kind {
            SessionEventKind::ReasoningTextDelta { delta } => {
                let cursor = self.reasoning.entry(session_id).or_default();
                if !cursor.known {
                    return None;
                }
                let start = text_start?;
                let skip = cursor.text.len().checked_sub(start)?;
                let suffix = delta.get(skip..)?.to_string();
                if suffix.is_empty() {
                    return None;
                }
                cursor.text.push_str(&suffix);
                cursor.raw_sent = true;
                *delta = suffix;
            }
            SessionEventKind::MessageDelta { message_id, delta } => {
                let key = (session_id, *message_id);
                if !self.messages.contains_key(&key) && !self.fresh_turns.contains(&session_id) {
                    return None;
                }
                let text = self.messages.entry(key).or_default();
                let start = text_start?;
                let skip = text.len().checked_sub(start)?;
                let suffix = delta.get(skip..)?.to_string();
                if suffix.is_empty() {
                    return None;
                }
                text.push_str(&suffix);
                *delta = suffix;
            }
            SessionEventKind::ReasoningDelta { text } => {
                let cursor = self.reasoning.entry(session_id).or_default();
                if !cursor.text.starts_with(text.as_str()) {
                    cursor.text.clone_from(text);
                }
                cursor.known = true;
            }
            SessionEventKind::Message {
                actor: EventActor::Assistant,
                message_id,
                text,
                status,
                ..
            } => {
                let key = (session_id, *message_id);
                if *status == MessageStatus::InProgress {
                    let current = self.messages.entry(key).or_default();
                    if !current.starts_with(text.as_str()) {
                        current.clone_from(text);
                    }
                } else {
                    self.messages.remove(&key);
                }
                self.reasoning.insert(
                    session_id,
                    ReasoningPreviewCursor {
                        known: true,
                        ..Default::default()
                    },
                );
            }
            SessionEventKind::ReasoningCompleted
            | SessionEventKind::ToolStarted { .. }
            | SessionEventKind::ToolUpdated { .. }
            | SessionEventKind::ToolCompleted { .. } => {
                self.reasoning.insert(
                    session_id,
                    ReasoningPreviewCursor {
                        known: true,
                        ..Default::default()
                    },
                );
            }
            SessionEventKind::TurnStarted { .. } => {
                self.clear_session(session_id);
                self.fresh_turns.insert(session_id);
                self.reasoning.insert(
                    session_id,
                    ReasoningPreviewCursor {
                        known: true,
                        ..Default::default()
                    },
                );
            }
            SessionEventKind::TurnCompleted { .. } | SessionEventKind::ContextCleared => {
                self.clear_session(session_id);
            }
            _ => {}
        }
        Some(event)
    }

    fn clear_session(&mut self, session_id: Uuid) {
        self.reasoning.remove(&session_id);
        self.messages.retain(|(owner, _), _| *owner != session_id);
        self.fresh_turns.remove(&session_id);
    }
}

fn preview_event_kind_mut(event: &mut SessionEvent) -> (Uuid, &mut SessionEventKind) {
    match &mut event.kind {
        SessionEventKind::SubagentActivity {
            event: Some(child), ..
        } => (child.session_id, &mut child.kind),
        kind => (event.session_id, kind),
    }
}

/// Forward the canonical event stream independently from the command path.
///
/// The event channel is deliberately backpressured: dropping a durable event
/// would make the attached projection unverifiable. That backpressure must not
/// also stop an attached terminal from delivering Escape, prompts, or goal
/// commands to the owner, so this loop owns only event forwarding.
#[cfg(unix)]
async fn forward_attached_events(
    store: Arc<dyn SessionStore>,
    session_id: Uuid,
    lock_path: PathBuf,
    presence_socket_path: PathBuf,
    mut last_sequence: u64,
    events: mpsc::Sender<SessionEvent>,
) -> Result<()> {
    let mut refresh = tokio::time::interval(ATTACHED_SESSION_REFRESH_INTERVAL);
    let mut live_revision = 0_u64;
    let mut preview = AttachedPreviewCursor::default();
    let mut reconnect_at = tokio::time::Instant::now();
    let mut live_protocol_supported = true;
    let mut _legacy_presence = None;
    loop {
        let connection = if live_protocol_supported && tokio::time::Instant::now() >= reconnect_at {
            match connect_local_live_stream(&presence_socket_path).await {
                LocalLiveConnect::Connected(connection)
                    if connection.latest_sequence < last_sequence =>
                {
                    reconnect_at = tokio::time::Instant::now() + ATTACHED_STORE_RETRY_DELAY;
                    None
                }
                LocalLiveConnect::Connected(connection) => Some(connection),
                LocalLiveConnect::Unsupported(presence) => {
                    _legacy_presence = Some(presence);
                    live_protocol_supported = false;
                    None
                }
                LocalLiveConnect::Unavailable => {
                    reconnect_at = tokio::time::Instant::now() + ATTACHED_STORE_RETRY_DELAY;
                    None
                }
            }
        } else {
            None
        };
        if let Some(mut connection) = connection {
            // Subscription and watermark are captured together by the owner.
            // Store replay fills only the durable prefix preceding that point;
            // the socket then supplies all later events in the owner's order.
            while last_sequence < connection.latest_sequence {
                let historical = match store.events_after(session_id, last_sequence, 1_000).await {
                    Ok(events) => events,
                    Err(error) if attached_store_error_is_retryable(&error) => {
                        tracing::debug!(%error, %session_id, "attached durable catch-up is busy; retrying");
                        tokio::time::sleep(ATTACHED_STORE_RETRY_DELAY).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                anyhow::ensure!(
                    !historical.is_empty(),
                    "attached session cannot fill durable gap through {}",
                    connection.latest_sequence
                );
                let before = last_sequence;
                for event in historical {
                    if event.sequence > connection.latest_sequence {
                        break;
                    }
                    let stopped = matches!(
                        event.kind,
                        SessionEventKind::StatusChanged {
                            status: SessionStatus::Stopped,
                            ..
                        }
                    );
                    last_sequence = event.sequence;
                    if let Some(event) = preview.accept(event, None)
                        && events.send(event).await.is_err()
                    {
                        return Ok(());
                    }
                    if stopped {
                        return Ok(());
                    }
                }
                anyhow::ensure!(
                    last_sequence > before,
                    "attached session cannot advance durable gap through {}",
                    connection.latest_sequence
                );
            }
            preview.require_snapshot_for_existing_streams();
            if connection.latest_sequence >= last_sequence {
                for snapshot in connection.snapshots.drain(..) {
                    if let Some(snapshot) = preview.accept(snapshot, None)
                        && events.send(snapshot).await.is_err()
                    {
                        return Ok(());
                    }
                }
            }
            loop {
                let frame = match read_live_frame(&mut connection.reader).await {
                    Ok(Some(frame)) => frame,
                    Ok(None) => break,
                    Err(error) => {
                        tracing::debug!(%error, %session_id, "local live stream ended; using store fallback");
                        break;
                    }
                };
                let LocalLiveFrame::Event {
                    event,
                    text_start,
                    durable_watermark,
                } = frame
                else {
                    break;
                };
                if event.sequence == 0 && durable_watermark < last_sequence {
                    continue;
                }
                if event.sequence > 0 {
                    if event.sequence <= last_sequence {
                        continue;
                    }
                    while event.sequence > last_sequence.saturating_add(1) {
                        let historical = match store
                            .events_after(session_id, last_sequence, 1_000)
                            .await
                        {
                            Ok(events) => events,
                            Err(error) if attached_store_error_is_retryable(&error) => {
                                tracing::debug!(%error, %session_id, "attached durable gap repair is busy; retrying");
                                tokio::time::sleep(ATTACHED_STORE_RETRY_DELAY).await;
                                continue;
                            }
                            Err(error) => return Err(error),
                        };
                        anyhow::ensure!(
                            !historical.is_empty(),
                            "attached session cannot repair durable gap before {}",
                            event.sequence
                        );
                        let before = last_sequence;
                        for missed in historical {
                            if missed.sequence >= event.sequence {
                                break;
                            }
                            last_sequence = missed.sequence;
                            if let Some(missed) = preview.accept(missed, None)
                                && events.send(missed).await.is_err()
                            {
                                return Ok(());
                            }
                        }
                        anyhow::ensure!(
                            last_sequence > before,
                            "attached session cannot advance durable gap before {}",
                            event.sequence
                        );
                    }
                    last_sequence = event.sequence;
                }
                let stopped = matches!(
                    event.kind,
                    SessionEventKind::StatusChanged {
                        status: SessionStatus::Stopped,
                        ..
                    }
                );
                if let Some(event) = preview.accept(event, text_start)
                    && events.send(event).await.is_err()
                {
                    return Ok(());
                }
                if stopped {
                    return Ok(());
                }
            }
            reconnect_at = tokio::time::Instant::now() + ATTACHED_STORE_RETRY_DELAY;
        }

        refresh.tick().await;
        let historical = match store.events_after(session_id, last_sequence, 1_000).await {
            Ok(events) => events,
            Err(error) if attached_store_error_is_retryable(&error) => {
                tracing::debug!(%error, %session_id, "attached session store read is busy; retrying");
                tokio::time::sleep(ATTACHED_STORE_RETRY_DELAY).await;
                continue;
            }
            Err(error) => return Err(error),
        };
        for event in historical {
            let stopped = matches!(
                event.kind,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Stopped,
                    ..
                }
            );
            let sequence = event.sequence;
            if let Some(event) = preview.accept(event, None)
                && events.send(event).await.is_err()
            {
                return Ok(());
            }
            last_sequence = sequence;
            if stopped {
                return Ok(());
            }
        }
        let live_events = match store.live_events_after(session_id, live_revision).await {
            Ok(events) => events,
            Err(error) if attached_store_error_is_retryable(&error) => {
                tracing::debug!(%error, %session_id, "attached live-state read is busy; retrying");
                tokio::time::sleep(ATTACHED_STORE_RETRY_DELAY).await;
                continue;
            }
            Err(error) => return Err(error),
        };
        for live in live_events {
            live_revision = live_revision.max(live.revision);
            if let Some(event) = preview.accept_store_snapshot(live.event)
                && events.send(event).await.is_err()
            {
                return Ok(());
            }
        }
        if !writer_is_active(&lock_path)? {
            return Ok(());
        }
    }
}

#[cfg(unix)]
fn attached_store_error_is_retryable(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    message.contains("database is locked")
        || message.contains("database is busy")
        || message.contains("pool timed out")
}

fn reasoning_delta_from_snapshot(
    mut event: SessionEvent,
    previous_snapshot: &mut String,
) -> Option<SessionEvent> {
    let SessionEventKind::ReasoningDelta { text } = &mut event.kind else {
        return Some(event);
    };
    let delta = text
        .strip_prefix(previous_snapshot.as_str())
        .unwrap_or(text)
        .to_string();
    *previous_snapshot = text.clone();
    if delta.is_empty() {
        return None;
    }
    *text = delta;
    Some(event)
}

fn writer_is_active(lock_path: &Path) -> Result<bool> {
    Ok(SessionWriterLease::try_acquire(lock_path)?.is_none())
}

#[cfg(not(unix))]
pub async fn run_attached_session(
    _store: Arc<dyn SessionStore>,
    _session_id: Uuid,
    _lock_path: PathBuf,
    _socket_path: PathBuf,
    _last_sequence: u64,
    _commands: mpsc::Receiver<HostCommand>,
    _events: mpsc::Sender<SessionEvent>,
) -> Result<()> {
    bail!("concurrent local session attachment is only supported on Unix")
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use super::*;
    use crate::PromptDelivery;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const MAX_UNIX_SOCKET_TEMP_ROOT_LENGTH: usize = 32;

    #[tokio::test]
    async fn reachability_distinguishes_a_stale_socket_file_from_a_served_one() {
        // A <session>.control.sock file outlives the process that made it.
        // Discovery used to report mere existence as liveness, so a dead
        // session stayed "live" forever while every send to it fell back to
        // queued_offline.
        let directory = short_socket_tempdir();
        let path = directory.path().join("probe.sock");

        assert!(
            !session_control_socket_is_reachable(&path).await,
            "a missing socket is not reachable"
        );

        // A plain file at the socket path is the stale-entry case.
        std::fs::write(&path, b"stale").unwrap();
        assert!(
            !session_control_socket_is_reachable(&path).await,
            "a leftover file with no listener must not read as reachable"
        );
        std::fs::remove_file(&path).unwrap();

        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        assert!(
            session_control_socket_is_reachable(&path).await,
            "a bound socket with a listener is reachable"
        );

        // Dropping the listener leaves the path behind: this is exactly the
        // state that used to be misreported as live.
        drop(listener);
        assert!(path.exists(), "the socket path outlives its listener");
        assert!(
            !session_control_socket_is_reachable(&path).await,
            "an abandoned socket path must read as unreachable"
        );
    }

    fn short_socket_tempdir() -> tempfile::TempDir {
        let temp_root = std::env::var_os("TMPDIR")
            .map(PathBuf::from)
            .filter(|path| path.to_string_lossy().len() <= MAX_UNIX_SOCKET_TEMP_ROOT_LENGTH)
            .unwrap_or_else(|| PathBuf::from("/tmp"));
        tempfile::Builder::new()
            .prefix("borg-session-")
            .tempdir_in(temp_root)
            .expect("short Unix socket test directory")
    }

    #[tokio::test]
    async fn owner_metadata_prevents_silent_attachment_to_an_obsolete_binary() {
        let root = short_socket_tempdir();
        let session_id = Uuid::new_v4();
        let journal_path = root.path().join(format!("{session_id}.lock"));
        let socket_path = session_control_socket_path(root.path(), session_id);
        let writer = SessionWriterLease::try_acquire(&journal_path)
            .unwrap()
            .unwrap();
        assert!(!local_session_owner_uses_current_binary(root.path(), session_id).unwrap());

        let (commands, _rx) = mpsc::channel(1);
        let _server =
            LocalSessionControlServer::start(socket_path, session_id, &writer, commands).unwrap();
        assert!(local_session_owner_uses_current_binary(root.path(), session_id).unwrap());

        let stale = LocalSessionOwnerMetadata {
            schema_version: 1,
            pid: u32::MAX,
            executable_identity: current_executable_identity().unwrap(),
            process_start_time: None,
        };
        fs::write(
            session_control_owner_path(root.path(), session_id),
            serde_json::to_vec(&stale).unwrap(),
        )
        .unwrap();
        assert!(!local_session_owner_uses_current_binary(root.path(), session_id).unwrap());
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn verified_obsolete_owner_can_be_terminated_and_releases_its_lock() {
        use std::process::Command;
        use std::thread;
        use std::time::Duration;

        let root = short_socket_tempdir();
        let session_id = Uuid::new_v4();
        let lock_path = root.path().join(format!("{session_id}.lock"));
        let mut owner = Command::new("flock")
            .args([
                "--exclusive",
                "--no-fork",
                lock_path.to_str().unwrap(),
                "sleep",
                "30",
            ])
            .spawn()
            .expect("flock is required for the Linux owner recovery test");

        for _ in 0..100 {
            if SessionWriterLease::try_acquire(&lock_path)
                .unwrap()
                .is_none()
            {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            SessionWriterLease::try_acquire(&lock_path)
                .unwrap()
                .is_none()
        );

        let metadata = LocalSessionOwnerMetadata {
            schema_version: 1,
            pid: owner.id(),
            executable_identity: process_executable_identity(owner.id())
                .unwrap()
                .expect("flock child executable identity"),
            process_start_time: process_start_time(owner.id()).unwrap(),
        };
        fs::write(
            session_control_owner_path(root.path(), session_id),
            serde_json::to_vec(&metadata).unwrap(),
        )
        .unwrap();

        assert_eq!(
            obsolete_local_session_owner_pid(root.path(), session_id, &lock_path).unwrap(),
            Some(owner.id())
        );
        force_terminate_local_session_owner(owner.id()).unwrap();
        owner.wait().unwrap();
        assert!(
            SessionWriterLease::try_acquire(&lock_path)
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn attached_commands_are_acknowledged_and_session_scoped() {
        let root = short_socket_tempdir();
        let session_id = Uuid::new_v4();
        let socket_path = session_control_socket_path(root.path(), session_id);
        let journal_path = root.path().join(format!("{session_id}.lock"));
        let writer = SessionWriterLease::try_acquire(&journal_path)
            .unwrap()
            .unwrap();
        let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
        let store: Arc<dyn SessionStore> = Arc::new(store);
        store.create_session(session_id).await.unwrap();
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
            .unwrap();
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::SessionConfigured {
                    cwd: root.path().to_path_buf(),
                    provider: crate::CodingProvider::Codex,
                    model: Some("gpt-test".to_string()),
                    effort: None,
                    fast: false,
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: crate::PermissionMode::Manual,
                },
            ))
            .await
            .unwrap();
        let (owner_tx, mut owner_rx) = mpsc::channel(1);
        let admissions = Arc::new(Mutex::new(HashSet::new()));
        let _server = LocalSessionControlServer::start_with_durable_prompt_admissions(
            socket_path.clone(),
            session_id,
            &writer,
            owner_tx,
            Some(Arc::clone(&admissions)),
            Arc::clone(&store),
        )
        .unwrap();

        let message_id = Uuid::new_v4();
        let command = HostCommand::Prompt {
            session_id,
            message_id,
            text: "hello".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        };
        let mut stream = tokio::net::UnixStream::connect(&socket_path).await.unwrap();
        stream
            .write_all(&serde_json::to_vec(&command).unwrap())
            .await
            .unwrap();
        stream.shutdown().await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();

        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&response).unwrap()["ok"],
            true
        );
        assert!(
            store
                .contains_message(session_id, message_id)
                .await
                .unwrap()
        );
        assert_eq!(
            owner_rx.recv().await.unwrap().session_id(),
            Some(session_id)
        );
        assert!(
            admissions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .contains(&message_id)
        );

        let wrong_session_command = HostCommand::Interrupt {
            session_id: Uuid::new_v4(),
        };
        let mut stream = tokio::net::UnixStream::connect(&socket_path).await.unwrap();
        stream
            .write_all(&serde_json::to_vec(&wrong_session_command).unwrap())
            .await
            .unwrap();
        stream.shutdown().await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();

        assert!(
            serde_json::from_slice::<serde_json::Value>(&response).unwrap()["error"]
                .as_str()
                .unwrap()
                .contains("different session")
        );
        assert!(owner_rx.try_recv().is_err());
        scratch.discard().await;
    }

    #[tokio::test]
    async fn prompt_acknowledgement_requires_a_durable_journal() {
        let root = short_socket_tempdir();
        let session_id = Uuid::new_v4();
        let socket_path = session_control_socket_path(root.path(), session_id);
        let journal_path = root.path().join(format!("{session_id}.lock"));
        let writer = SessionWriterLease::try_acquire(&journal_path)
            .unwrap()
            .unwrap();
        let (owner_tx, mut owner_rx) = mpsc::channel(1);
        let _server =
            LocalSessionControlServer::start(socket_path.clone(), session_id, &writer, owner_tx)
                .unwrap();
        let command = HostCommand::Prompt {
            session_id,
            message_id: Uuid::new_v4(),
            text: "must survive acknowledgement".to_string(),
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        };

        let mut stream = tokio::net::UnixStream::connect(&socket_path).await.unwrap();
        stream
            .write_all(&serde_json::to_vec(&command).unwrap())
            .await
            .unwrap();
        stream.shutdown().await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();

        assert!(
            serde_json::from_slice::<serde_json::Value>(&response).unwrap()["error"]
                .as_str()
                .unwrap()
                .contains("durable journal")
        );
        assert!(owner_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn flush_recovers_admitted_prompt_with_attachments_before_flushing_owner() {
        let root = short_socket_tempdir();
        let session_id = Uuid::new_v4();
        let message_id = Uuid::new_v4();
        let lock_path = root.path().join("session.lock");
        let socket_path = session_control_socket_path(root.path(), session_id);
        let writer = SessionWriterLease::try_acquire(&lock_path)
            .unwrap()
            .unwrap();
        let (scratch, store) = crate::session_store::postgres::testing::session_store().await;
        let store: Arc<dyn SessionStore> = Arc::new(store);
        store.create_session(session_id).await.unwrap();
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
            .unwrap();
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::SessionConfigured {
                    cwd: root.path().to_path_buf(),
                    provider: crate::CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: false,
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: crate::PermissionMode::Manual,
                },
            ))
            .await
            .unwrap();
        let attachments = vec![root.path().join("original.png")];
        store
            .admit_prompt(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::Message {
                    message_id,
                    actor: EventActor::User,
                    text: "use this image".into(),
                    attachments: attachments.clone(),
                    status: MessageStatus::Queued,
                    delivery: Some(PromptDelivery::Queue),
                },
            ))
            .await
            .unwrap();
        let (owner_tx, mut owner_rx) = mpsc::channel(4);
        let _server = LocalSessionControlServer::start_with_durable_prompt_admissions(
            socket_path.clone(),
            session_id,
            &writer,
            owner_tx,
            None,
            store,
        )
        .unwrap();
        assert!(
            !forward_attached_command(
                &lock_path,
                &socket_path,
                HostCommand::FlushPendingInput { session_id }
            )
            .await
            .unwrap()
        );
        match owner_rx.recv().await.unwrap() {
            HostCommand::Prompt {
                message_id: actual,
                text,
                attachments: actual_files,
                delivery,
                ..
            } => {
                assert_eq!(actual, message_id);
                assert_eq!(text, "use this image");
                assert_eq!(actual_files, attachments);
                assert_eq!(delivery, PromptDelivery::Steer);
            }
            command => panic!("expected recovered prompt, got {command:?}"),
        }
        assert!(matches!(
            owner_rx.recv().await.unwrap(),
            HostCommand::FlushPendingInput { .. }
        ));
        scratch.discard().await;
    }

    #[tokio::test]
    async fn presence_channel_tracks_idle_attached_viewers() {
        let root = short_socket_tempdir();
        let session_id = Uuid::new_v4();
        let journal_path = root.path().join(format!("{session_id}.lock"));
        let socket_path = session_control_socket_path(root.path(), session_id);
        let presence_path = session_control_presence_socket_path(root.path(), session_id);
        let writer = SessionWriterLease::try_acquire(&journal_path)
            .unwrap()
            .unwrap();
        let (owner_tx, _owner_rx) = mpsc::channel(1);
        let server =
            LocalSessionControlServer::start(socket_path, session_id, &writer, owner_tx).unwrap();

        assert!(!server.has_attached_viewers());
        let mut presence = tokio::net::UnixStream::connect(presence_path)
            .await
            .unwrap();
        let mut acknowledgement = [0_u8; 1];
        presence.read_exact(&mut acknowledgement).await.unwrap();
        assert!(server.has_attached_viewers());

        drop(presence);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while server.has_attached_viewers() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("viewer presence should be released when the attachment closes");
    }

    #[tokio::test(start_paused = true)]
    async fn live_delta_batches_keep_text_offsets_and_event_order() {
        let root = short_socket_tempdir();
        let session_id = Uuid::new_v4();
        let message_id = Uuid::new_v4();
        let lock_path = root.path().join("session.lock");
        let writer = SessionWriterLease::try_acquire(&lock_path)
            .unwrap()
            .unwrap();
        let (commands, _rx) = mpsc::channel(1);
        let server = LocalSessionControlServer::start(
            session_control_socket_path(root.path(), session_id),
            session_id,
            &writer,
            commands,
        )
        .unwrap();
        let mut frames = server.live.lock().unwrap().events.subscribe();
        let mut preview = AttachedPreviewCursor::default();
        let turn = SessionEvent::new(
            session_id,
            1,
            SessionEventKind::TurnStarted {
                message_id,
                provider: crate::CodingProvider::Codex,
                model: None,
                effort: None,
                fast: false,
            },
        );
        server.publish_live_event(&turn);
        let first = frames.recv().await.unwrap();
        let LocalLiveFrame::Event { event, .. } =
            serde_json::from_slice::<LocalLiveFrame>(&first.bytes).unwrap()
        else {
            panic!("turn should arrive first");
        };
        preview.accept(event, None).unwrap();

        for delta in ["a", "b", "c"] {
            server.publish_live_event(&SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ReasoningTextDelta {
                    delta: delta.into(),
                },
            ));
        }
        let first = frames.recv().await.unwrap();
        let LocalLiveFrame::Event {
            event,
            text_start: Some(0),
            ..
        } = serde_json::from_slice::<LocalLiveFrame>(&first.bytes).unwrap()
        else {
            panic!("first reasoning fragment should arrive immediately");
        };
        assert!(
            matches!(event.kind, SessionEventKind::ReasoningTextDelta { ref delta } if delta == "a")
        );
        preview.accept(event, Some(0)).unwrap();
        assert!(frames.try_recv().is_err(), "the next fragments are batched");
        tokio::time::advance(LOCAL_LIVE_DELTA_BATCH_DELAY).await;
        let merged = frames.recv().await.unwrap();
        let LocalLiveFrame::Event {
            event,
            text_start: Some(1),
            ..
        } = serde_json::from_slice::<LocalLiveFrame>(&merged.bytes).unwrap()
        else {
            panic!("merged reasoning fragments should retain their first offset");
        };
        assert!(
            matches!(event.kind, SessionEventKind::ReasoningTextDelta { ref delta } if delta == "bc")
        );
        preview.accept(event, Some(1)).unwrap();

        for delta in ["x", "y", "z"] {
            server.publish_live_event(&SessionEvent::new(
                session_id,
                0,
                SessionEventKind::MessageDelta {
                    message_id,
                    delta: delta.into(),
                },
            ));
        }
        server.publish_live_event(&SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningDelta { text: "abc".into() },
        ));
        let mut seen = Vec::new();
        for _ in 0..3 {
            let frame = frames.recv().await.unwrap();
            let LocalLiveFrame::Event {
                event, text_start, ..
            } = serde_json::from_slice::<LocalLiveFrame>(&frame.bytes).unwrap()
            else {
                panic!("expected a live event");
            };
            seen.push(event.kind.clone());
            preview.accept(event, text_start).unwrap();
        }
        assert!(
            matches!(seen[0], SessionEventKind::MessageDelta { ref delta, .. } if delta == "x")
        );
        assert!(
            matches!(seen[1], SessionEventKind::MessageDelta { ref delta, .. } if delta == "yz")
        );
        assert!(matches!(seen[2], SessionEventKind::ReasoningDelta { ref text } if text == "abc"));
        assert_eq!(preview.reasoning[&session_id].text, "abc");
        assert_eq!(preview.messages[&(session_id, message_id)], "xyz");
    }

    #[tokio::test]
    async fn attached_viewer_receives_raw_text_before_the_next_store_snapshot() {
        let root = short_socket_tempdir();
        let session_id = Uuid::new_v4();
        let lock_path = root.path().join(format!("{session_id}.lock"));
        let socket_path = session_control_socket_path(root.path(), session_id);
        let writer = SessionWriterLease::try_acquire(&lock_path)
            .unwrap()
            .unwrap();
        let (scratch, postgres) = crate::session_store::postgres::testing::session_store().await;
        let postgres = Arc::new(postgres);
        postgres.create_session(session_id).await.unwrap();
        let started = postgres
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
            .unwrap();
        let store: Arc<dyn SessionStore> = postgres.clone();
        let (owner_tx, _owner_rx) = mpsc::channel(1);
        let server =
            LocalSessionControlServer::start(socket_path.clone(), session_id, &writer, owner_tx)
                .unwrap();
        server.seed_durable_watermark(started.sequence);
        let (command_tx, command_rx) = mpsc::channel(1);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let attachment = tokio::spawn(run_attached_session(
            store,
            session_id,
            lock_path,
            socket_path,
            started.sequence,
            command_rx,
            event_tx,
        ));
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if server.live.lock().unwrap().events.receiver_count() > 0 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("attached viewer should subscribe to the owner event stream");

        let message_id = Uuid::new_v4();
        let turn = postgres
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::TurnStarted {
                    message_id,
                    provider: crate::CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: false,
                },
            ))
            .await
            .unwrap();
        server.publish_live_event(&turn);
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(1), event_rx.recv())
                .await
                .unwrap()
                .unwrap()
                .kind,
            SessionEventKind::TurnStarted { .. }
        ));

        server.publish_live_event(&SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningTextDelta {
                delta: "think".into(),
            },
        ));
        let first_reasoning =
            tokio::time::timeout(std::time::Duration::from_secs(1), event_rx.recv())
                .await
                .expect("first fragment should arrive without waiting for the batch")
                .unwrap();
        server.publish_live_event(&SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningTextDelta { delta: "in".into() },
        ));
        server.publish_live_event(&SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningTextDelta { delta: "g".into() },
        ));
        server.publish_live_event(&SessionEvent::new(
            session_id,
            0,
            SessionEventKind::MessageDelta {
                message_id,
                delta: "answer".into(),
            },
        ));
        let mut reasoning = String::new();
        let SessionEventKind::ReasoningTextDelta { delta } = first_reasoning.kind else {
            panic!("first fragment should be reasoning");
        };
        reasoning.push_str(&delta);
        let message = loop {
            let event = tokio::time::timeout(std::time::Duration::from_secs(1), event_rx.recv())
                .await
                .unwrap()
                .unwrap();
            match &event.kind {
                SessionEventKind::ReasoningTextDelta { delta } => reasoning.push_str(delta),
                SessionEventKind::MessageDelta { .. } => break event,
                _ => panic!("reasoning should arrive before the message"),
            }
        };
        assert_eq!(reasoning, "thinking");
        assert!(
            matches!(message.kind, SessionEventKind::MessageDelta { ref delta, .. } if delta == "answer")
        );
        command_tx
            .send(HostCommand::Stop { session_id })
            .await
            .unwrap();
        attachment.await.unwrap().unwrap();
        scratch.discard().await;
    }

    #[test]
    fn reconnect_store_snapshot_keeps_reasoning_cumulative_after_raw_preview() {
        let session_id = Uuid::new_v4();
        let mut preview = AttachedPreviewCursor::default();
        let turn = SessionEvent::new(
            session_id,
            1,
            SessionEventKind::TurnStarted {
                message_id: Uuid::new_v4(),
                provider: crate::CodingProvider::Codex,
                model: None,
                effort: None,
                fast: false,
            },
        );
        preview.accept(turn, None).unwrap();
        let raw = SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningTextDelta {
                delta: "thinking".into(),
            },
        );
        assert!(
            matches!(preview.accept(raw, Some(0)).unwrap().kind, SessionEventKind::ReasoningTextDelta { ref delta } if delta == "thinking")
        );
        preview.require_snapshot_for_existing_streams();
        let snapshot = SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningDelta {
                text: "thinking again".into(),
            },
        );
        assert!(
            matches!(preview.accept_store_snapshot(snapshot).unwrap().kind, SessionEventKind::ReasoningDelta { ref text } if text == "thinking again")
        );
        let resumed = SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningTextDelta {
                delta: " more".into(),
            },
        );
        assert!(
            matches!(preview.accept(resumed, Some("thinking again".len())).unwrap().kind, SessionEventKind::ReasoningTextDelta { ref delta } if delta == " more")
        );
        assert_eq!(preview.reasoning[&session_id].text, "thinking again more");
    }

    #[tokio::test]
    async fn attached_viewer_rejects_snapshots_from_an_owner_behind_the_store() {
        let root = short_socket_tempdir();
        let session_id = Uuid::new_v4();
        let lock_path = root.path().join(format!("{session_id}.lock"));
        let socket_path = session_control_socket_path(root.path(), session_id);
        let writer = SessionWriterLease::try_acquire(&lock_path)
            .unwrap()
            .unwrap();
        let (scratch, postgres) = crate::session_store::postgres::testing::session_store().await;
        let postgres = Arc::new(postgres);
        postgres.create_session(session_id).await.unwrap();
        let started = postgres
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
            .unwrap();
        let message_id = Uuid::new_v4();
        let in_progress = postgres
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::Message {
                    message_id,
                    actor: EventActor::Assistant,
                    text: "stale draft".into(),
                    attachments: Vec::new(),
                    status: MessageStatus::InProgress,
                    delivery: None,
                },
            ))
            .await
            .unwrap();
        let complete = postgres
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::Message {
                    message_id,
                    actor: EventActor::Assistant,
                    text: "finished".into(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ))
            .await
            .unwrap();
        let store: Arc<dyn SessionStore> = postgres;
        let (owner_tx, _owner_rx) = mpsc::channel(1);
        let server =
            LocalSessionControlServer::start(socket_path.clone(), session_id, &writer, owner_tx)
                .unwrap();
        server.seed_durable_watermark(started.sequence);
        server.publish_live_event(&in_progress);
        let (command_tx, command_rx) = mpsc::channel(1);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let attachment = tokio::spawn(run_attached_session(
            store,
            session_id,
            lock_path,
            socket_path,
            complete.sequence,
            command_rx,
            event_tx,
        ));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(150), event_rx.recv())
                .await
                .is_err(),
            "a stale owner snapshot must not resurrect a completed message"
        );
        server.publish_live_event(&complete);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(150), event_rx.recv())
                .await
                .is_err(),
            "catching up the owner must not replay its stale snapshot"
        );
        command_tx
            .send(HostCommand::Stop { session_id })
            .await
            .unwrap();
        attachment.await.unwrap().unwrap();
        scratch.discard().await;
    }

    #[tokio::test]
    async fn attached_viewer_keeps_presence_with_an_older_owner() {
        let root = short_socket_tempdir();
        let session_id = Uuid::new_v4();
        let lock_path = root.path().join(format!("{session_id}.lock"));
        let socket_path = session_control_socket_path(root.path(), session_id);
        let presence_path = session_control_presence_socket_path(root.path(), session_id);
        let listener = tokio::net::UnixListener::bind(presence_path).unwrap();
        let _writer = SessionWriterLease::try_acquire(&lock_path)
            .unwrap()
            .unwrap();
        let (scratch, postgres) = crate::session_store::postgres::testing::session_store().await;
        let postgres = Arc::new(postgres);
        postgres.create_session(session_id).await.unwrap();
        let store: Arc<dyn SessionStore> = postgres;
        let (command_tx, command_rx) = mpsc::channel(1);
        let (event_tx, _event_rx) = mpsc::channel(1);
        let attachment = tokio::spawn(run_attached_session(
            store,
            session_id,
            lock_path,
            socket_path,
            0,
            command_rx,
            event_tx,
        ));
        let (mut old_owner, _) =
            tokio::time::timeout(std::time::Duration::from_secs(1), listener.accept())
                .await
                .unwrap()
                .unwrap();
        old_owner.write_all(&[1]).await.unwrap();
        let mut byte = [0_u8; 1];
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(150),
                old_owner.read(&mut byte)
            )
            .await
            .is_err(),
            "an older owner must keep counting the attached viewer"
        );
        command_tx
            .send(HostCommand::Stop { session_id })
            .await
            .unwrap();
        attachment.await.unwrap().unwrap();
        assert_eq!(old_owner.read(&mut byte).await.unwrap(), 0);
        scratch.discard().await;
    }

    #[tokio::test]
    async fn attachment_ends_cleanly_when_the_writer_disappears() {
        let root = short_socket_tempdir();
        let session_id = Uuid::new_v4();
        let journal_path = root.path().join(format!("{session_id}.lock"));
        let socket_path = session_control_socket_path(root.path(), session_id);
        let writer = SessionWriterLease::try_acquire(&journal_path)
            .unwrap()
            .unwrap();
        let (_command_tx, command_rx) = mpsc::channel(1);
        let (event_tx, _event_rx) = mpsc::channel(1);
        let (scratch, postgres) = crate::session_store::postgres::testing::session_store().await;
        let postgres = Arc::new(postgres);
        postgres.create_session(session_id).await.unwrap();
        let store: Arc<dyn SessionStore> = postgres;

        let attachment = tokio::spawn(run_attached_session(
            store,
            session_id,
            journal_path,
            socket_path,
            0,
            command_rx,
            event_tx,
        ));
        tokio::task::yield_now().await;
        drop(writer);

        tokio::time::timeout(std::time::Duration::from_secs(1), attachment)
            .await
            .expect("attachment should notice released ownership")
            .expect("attachment task should not panic")
            .expect("owner loss is a clean detach");
        scratch.discard().await;
    }

    #[tokio::test]
    async fn connected_attachment_ends_when_owner_drops_without_a_stop_event() {
        let root = short_socket_tempdir();
        let session_id = Uuid::new_v4();
        let lock_path = root.path().join(format!("{session_id}.lock"));
        let socket_path = session_control_socket_path(root.path(), session_id);
        let writer = SessionWriterLease::try_acquire(&lock_path)
            .unwrap()
            .unwrap();
        let (scratch, postgres) = crate::session_store::postgres::testing::session_store().await;
        let postgres = Arc::new(postgres);
        postgres.create_session(session_id).await.unwrap();
        let store: Arc<dyn SessionStore> = postgres;
        let (owner_tx, _owner_rx) = mpsc::channel(1);
        let server =
            LocalSessionControlServer::start(socket_path.clone(), session_id, &writer, owner_tx)
                .unwrap();
        let (_command_tx, command_rx) = mpsc::channel(1);
        let (event_tx, _event_rx) = mpsc::channel(1);
        let attachment = tokio::spawn(run_attached_session(
            store,
            session_id,
            lock_path,
            socket_path,
            0,
            command_rx,
            event_tx,
        ));
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if server.live.lock().unwrap().events.receiver_count() > 0 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("attached viewer should establish the live socket");
        drop(writer);
        drop(server);
        tokio::time::timeout(std::time::Duration::from_secs(1), attachment)
            .await
            .expect("attached viewer should leave a silent closed owner")
            .expect("attachment task should not panic")
            .expect("owner loss is a clean detach");
        scratch.discard().await;
    }

    #[tokio::test]
    async fn reachability_separates_a_stale_socket_file_from_a_served_one() {
        // list_instances used to report liveness from the socket PATH alone.
        // A control socket file outlives the process that bound it, so every
        // dead session stayed "live" forever while send_message - which
        // actually connects - quietly downgraded those peers to
        // queued_offline. Discovery and delivery must agree.
        let root = short_socket_tempdir();
        let session_id = Uuid::new_v4();
        let socket_path = session_control_socket_path(root.path(), session_id);

        assert!(
            !session_control_socket_is_reachable(&socket_path).await,
            "a session that never existed is not reachable"
        );

        // Bind then drop: the file remains, the listener does not. This is
        // exactly what a crashed or exited session leaves behind.
        let stale = tokio::net::UnixListener::bind(&socket_path).unwrap();
        drop(stale);
        assert!(socket_path.exists(), "the stale socket file survives");
        // Polled rather than asserted instantly. A listener that has just been
        // dropped can still accept from its backlog for a moment, and under
        // load that window widens -- which is a kernel teardown artifact, not
        // the bug this guards. The bug was that a dead session stayed live
        // FOREVER, so the property is that unreachability arrives, promptly
        // and on its own, without anything cleaning the file up.
        let mut went_stale = false;
        for _ in 0..40 {
            if !session_control_socket_is_reachable(&socket_path).await {
                went_stale = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(
            went_stale,
            "an existing socket file with no listener must not read as live"
        );

        let lock_path = root.path().join(format!("{session_id}.lock"));
        let writer = SessionWriterLease::try_acquire(&lock_path)
            .unwrap()
            .unwrap();
        let (owner_tx, _owner_rx) = mpsc::channel(1);
        let server =
            LocalSessionControlServer::start(socket_path.clone(), session_id, &writer, owner_tx)
                .unwrap();
        // Retried, because the probe is bounded at 250ms so that one
        // unresponsive peer cannot stall a whole listing. That bound is right
        // for production and wrong as a test assertion: on a loaded machine a
        // connect to a genuinely live socket can miss it, which says nothing
        // about reachability and everything about the scheduler. Retrying
        // keeps the property under test -- a served socket IS reachable --
        // without asserting anything about how promptly this host schedules.
        let mut reachable = false;
        for _ in 0..20 {
            if session_control_socket_is_reachable(&socket_path).await {
                reachable = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(reachable, "a served socket is reachable");
        drop(server);
    }

    #[tokio::test]
    async fn a_new_owner_reclaims_a_refused_socket() {
        let root = short_socket_tempdir();
        let session_id = Uuid::new_v4();
        let socket_path = session_control_socket_path(root.path(), session_id);
        let lock_path = root.path().join(format!("{session_id}.lock"));
        let writer = SessionWriterLease::try_acquire(&lock_path)
            .unwrap()
            .unwrap();

        let stale = tokio::net::UnixListener::bind(&socket_path).unwrap();
        drop(stale);
        assert!(socket_path.exists());

        let (owner_tx, _owner_rx) = mpsc::channel(1);
        let server =
            LocalSessionControlServer::start(socket_path.clone(), session_id, &writer, owner_tx)
                .unwrap();
        assert!(tokio::net::UnixStream::connect(&socket_path).await.is_ok());
        drop(server);
    }

    #[tokio::test]
    async fn attachment_delivers_durable_status_before_live_projection_state() {
        let root = short_socket_tempdir();
        let session_id = Uuid::new_v4();
        let journal_path = root.path().join(format!("{session_id}.lock"));
        let socket_path = session_control_socket_path(root.path(), session_id);
        let _writer = SessionWriterLease::try_acquire(&journal_path)
            .unwrap()
            .unwrap();
        let (scratch, postgres) = crate::session_store::postgres::testing::session_store().await;
        let postgres = Arc::new(postgres);
        postgres.create_session(session_id).await.unwrap();
        postgres
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
            .unwrap();
        let ready = postgres
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Ready,
                    detail: None,
                },
            ))
            .await
            .unwrap();
        let running = postgres
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Running,
                    detail: None,
                },
            ))
            .await
            .unwrap();
        postgres
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ContextWindowUpdated {
                    context_tokens: 80,
                    context_window_tokens: 100,
                },
            ))
            .await
            .unwrap();

        let (command_tx, command_rx) = mpsc::channel(1);
        let (event_tx, mut event_rx) = mpsc::channel(4);
        let store: Arc<dyn SessionStore> = postgres;
        let attachment = tokio::spawn(run_attached_session(
            store,
            session_id,
            journal_path,
            socket_path,
            ready.sequence,
            command_rx,
            event_tx,
        ));

        let durable = tokio::time::timeout(std::time::Duration::from_secs(1), event_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let live = tokio::time::timeout(std::time::Duration::from_secs(1), event_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(durable.sequence, running.sequence);
        assert!(matches!(
            durable.kind,
            SessionEventKind::StatusChanged {
                status: SessionStatus::Running,
                ..
            }
        ));
        assert_eq!(live.sequence, 0);
        assert!(matches!(
            live.kind,
            SessionEventKind::ContextWindowUpdated {
                context_tokens: 80,
                context_window_tokens: 100,
            }
        ));

        command_tx
            .send(HostCommand::Stop { session_id })
            .await
            .unwrap();
        attachment.await.unwrap().unwrap();
        scratch.discard().await;
    }

    #[tokio::test]
    async fn blocked_attached_event_delivery_does_not_block_owner_commands() {
        let root = short_socket_tempdir();
        let session_id = Uuid::new_v4();
        let journal_path = root.path().join(format!("{session_id}.lock"));
        let socket_path = session_control_socket_path(root.path(), session_id);
        let _writer = SessionWriterLease::try_acquire(&journal_path)
            .unwrap()
            .unwrap();
        let (scratch, postgres) = crate::session_store::postgres::testing::session_store().await;
        let postgres = Arc::new(postgres);
        postgres.create_session(session_id).await.unwrap();
        postgres
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
            .unwrap();
        postgres
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Ready,
                    detail: None,
                },
            ))
            .await
            .unwrap();

        let (owner_tx, mut owner_rx) = mpsc::channel(4);
        let _server =
            LocalSessionControlServer::start(socket_path.clone(), session_id, &_writer, owner_tx)
                .unwrap();
        let (command_tx, command_rx) = mpsc::channel(4);
        let (event_tx, _event_rx) = mpsc::channel(1);
        let store: Arc<dyn SessionStore> = postgres;
        let attachment = tokio::spawn(run_attached_session(
            store,
            session_id,
            journal_path,
            socket_path,
            0,
            command_rx,
            event_tx,
        ));

        // The first durable event fills the bounded projection channel; the
        // second event then blocks the event forwarder. Commands must still
        // reach the owner while that backpressure is present.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        command_tx
            .send(HostCommand::Interrupt { session_id })
            .await
            .unwrap();
        let forwarded = tokio::time::timeout(std::time::Duration::from_secs(1), owner_rx.recv())
            .await
            .expect("owner command should not wait behind event backpressure")
            .expect("owner command channel should remain open");
        assert!(matches!(forwarded, HostCommand::Interrupt { session_id: id } if id == session_id));

        command_tx
            .send(HostCommand::Stop { session_id })
            .await
            .unwrap();
        attachment.await.unwrap().unwrap();
        scratch.discard().await;
    }

    #[test]
    fn attached_reasoning_snapshots_become_incremental_deltas() {
        let session_id = Uuid::new_v4();
        let mut previous = String::new();
        let snapshot = |text: &str| {
            SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ReasoningDelta {
                    text: text.to_string(),
                },
            )
        };

        let first = reasoning_delta_from_snapshot(snapshot("thinking "), &mut previous).unwrap();
        let second =
            reasoning_delta_from_snapshot(snapshot("thinking carefully"), &mut previous).unwrap();
        let duplicate =
            reasoning_delta_from_snapshot(snapshot("thinking carefully"), &mut previous);

        assert!(matches!(
            first.kind,
            SessionEventKind::ReasoningDelta { ref text } if text == "thinking "
        ));
        assert!(matches!(
            second.kind,
            SessionEventKind::ReasoningDelta { ref text } if text == "carefully"
        ));
        assert!(duplicate.is_none());
    }

    #[test]
    fn attached_reasoning_accepts_a_new_snapshot_after_live_state_reset() {
        let session_id = Uuid::new_v4();
        let mut previous = "old reasoning".to_string();
        let event = SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningDelta {
                text: "new reasoning".to_string(),
            },
        );

        let delta = reasoning_delta_from_snapshot(event, &mut previous).unwrap();

        assert!(matches!(
            delta.kind,
            SessionEventKind::ReasoningDelta { ref text } if text == "new reasoning"
        ));
        assert_eq!(previous, "new reasoning");
    }
}
