use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicUsize, Ordering},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    EventActor, HostCommand, MessageStatus, SessionEvent, SessionEventKind, SessionStatus,
    SessionStore, SessionWriterLease,
};

const MAX_CONTROL_COMMAND_BYTES: u64 = 1024 * 1024;
const ATTACHED_SESSION_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(25);
const ATTACHED_STORE_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(100);

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
/// The session lock remains exclusive. Additional terminals tail SQLite
/// events and send typed commands through this endpoint.
#[cfg(unix)]
pub struct LocalSessionControlServer {
    task: tokio::task::JoinHandle<()>,
    attached_viewers: Arc<AtomicUsize>,
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
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
                            Ok((mut stream, _)) => {
                                let attached_viewers = Arc::clone(&task_attached_viewers);
                                tokio::spawn(async move {
                                    attached_viewers.fetch_add(1, Ordering::AcqRel);
                                    // The acknowledgement makes the attachment
                                    // visible before its session loop proceeds.
                                    let _ = stream.write_all(&[1]).await;
                                    let mut byte = [0_u8; 1];
                                    loop {
                                        match stream.read(&mut byte).await {
                                            Ok(0) | Err(_) => break,
                                            Ok(_) => {}
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
        })
    }

    pub fn has_attached_viewers(&self) -> bool {
        self.attached_viewers.load(Ordering::Acquire) > 0
    }
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
    use tokio::io::AsyncReadExt;
    use tokio::net::UnixStream;

    let presence_socket_path = session_control_presence_socket_path(
        socket_path.parent().unwrap_or_else(|| Path::new(".")),
        session_id,
    );
    let mut _presence = match UnixStream::connect(&presence_socket_path).await {
        Ok(mut stream) => {
            let mut acknowledgement = [0_u8; 1];
            match tokio::time::timeout(
                std::time::Duration::from_secs(1),
                stream.read_exact(&mut acknowledgement),
            )
            .await
            {
                Ok(Ok(_)) => Some(stream),
                Ok(Err(error)) => {
                    tracing::debug!(
                        %error,
                        socket_path = %presence_socket_path.display(),
                        "local session presence handshake failed"
                    );
                    None
                }
                Err(_) => {
                    tracing::debug!(
                        socket_path = %presence_socket_path.display(),
                        "local session presence handshake timed out"
                    );
                    None
                }
            }
        }
        Err(error) => {
            tracing::debug!(
                %error,
                socket_path = %presence_socket_path.display(),
                "local session owner does not expose a viewer presence channel"
            );
            None
        }
    };

    let command_events = events.clone();
    let mut event_forwarder = tokio::spawn(forward_attached_events(
        Arc::clone(&store),
        session_id,
        lock_path.clone(),
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
    mut last_sequence: u64,
    events: mpsc::Sender<SessionEvent>,
) -> Result<()> {
    let mut refresh = tokio::time::interval(ATTACHED_SESSION_REFRESH_INTERVAL);
    let mut live_revision = 0_u64;
    let mut reasoning_snapshot = String::new();
    loop {
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
            if event.kind.clears_live_turn_state()
                || event
                    .kind
                    .cleared_live_state_keys()
                    .iter()
                    .any(|key| key == "reasoning")
            {
                reasoning_snapshot.clear();
            }
            let stopped = matches!(
                event.kind,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Stopped,
                    ..
                }
            );
            let sequence = event.sequence;
            if events.send(event).await.is_err() {
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
            if let Some(event) = reasoning_delta_from_snapshot(live.event, &mut reasoning_snapshot)
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
        let store: Arc<dyn SessionStore> = Arc::new(
            crate::SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
                .await
                .unwrap(),
        );
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
        let store: Arc<dyn SessionStore> = Arc::new(
            crate::SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
                .await
                .unwrap(),
        );
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
        let sqlite = Arc::new(
            crate::SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
                .await
                .unwrap(),
        );
        sqlite.create_session(session_id).await.unwrap();
        let store: Arc<dyn SessionStore> = sqlite;

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
        let sqlite = Arc::new(
            crate::SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
                .await
                .unwrap(),
        );
        sqlite.create_session(session_id).await.unwrap();
        sqlite
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
            .unwrap();
        let ready = sqlite
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
        let running = sqlite
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
        sqlite
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
        let store: Arc<dyn SessionStore> = sqlite;
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
        let sqlite = Arc::new(
            crate::SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
                .await
                .unwrap(),
        );
        sqlite.create_session(session_id).await.unwrap();
        sqlite
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
            .unwrap();
        sqlite
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
        let store: Arc<dyn SessionStore> = sqlite;
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
