use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, bail};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    HostCommand, SessionEvent, SessionEventKind, SessionStatus, SessionStore, SessionWriterLease,
};

const MAX_CONTROL_COMMAND_BYTES: u64 = 1024 * 1024;
const ATTACHED_SESSION_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(25);

/// Path used by additional local terminals to attach to a session owner.
pub fn session_control_socket_path(sessions_dir: &Path, session_id: Uuid) -> PathBuf {
    sessions_dir.join(format!("{session_id}.control.sock"))
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
/// The journal writer remains exclusive. Additional terminals tail the
/// journal and send typed commands through this endpoint.
#[cfg(unix)]
pub struct LocalSessionControlServer {
    task: tokio::task::JoinHandle<()>,
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
        writer: &SessionWriterLease,
        commands: mpsc::Sender<HostCommand>,
        _prompt_admissions: Option<Arc<Mutex<HashSet<Uuid>>>>,
    ) -> Result<Self> {
        Self::start(socket_path, session_id, writer, commands)
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
        writer: &SessionWriterLease,
        commands: mpsc::Sender<HostCommand>,
        prompt_admissions: Option<Arc<Mutex<HashSet<Uuid>>>>,
    ) -> Result<Self> {
        use std::os::unix::fs::PermissionsExt;
        use tokio::net::UnixListener;

        let journal_path = socket_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(format!("{session_id}.jsonl"));
        writer.ensure_journal(&journal_path)?;
        if socket_path.exists() {
            fs::remove_file(&socket_path)
                .with_context(|| format!("failed to remove stale {}", socket_path.display()))?;
        }
        let listener = UnixListener::bind(&socket_path)
            .with_context(|| format!("failed to bind {}", socket_path.display()))?;
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to secure {}", socket_path.display()))?;
        let task_socket_path = socket_path.clone();
        let task = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let commands = commands.clone();
                        let prompt_admissions = prompt_admissions.clone();
                        tokio::spawn(handle_control_connection(
                            stream,
                            session_id,
                            commands,
                            prompt_admissions,
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
        });
        Ok(Self { task })
    }
}

#[cfg(unix)]
async fn handle_control_connection(
    mut stream: tokio::net::UnixStream,
    session_id: Uuid,
    commands: mpsc::Sender<HostCommand>,
    prompt_admissions: Option<Arc<Mutex<HashSet<Uuid>>>>,
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
        if let HostCommand::Prompt { message_id, .. } = &command {
            if let Some(admissions) = prompt_admissions.as_ref() {
                admissions
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(*message_id);
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
    journal_path: PathBuf,
    socket_path: PathBuf,
    mut last_sequence: u64,
    mut commands: mpsc::Receiver<HostCommand>,
    events: mpsc::Sender<SessionEvent>,
) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixStream;

    let mut refresh = tokio::time::interval(ATTACHED_SESSION_REFRESH_INTERVAL);
    let mut live_revision = 0_u64;
    let mut reasoning_snapshot = String::new();
    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { return Ok(()) };
                if matches!(command, HostCommand::Stop { .. }) {
                    return Ok(());
                }
                if !writer_is_active(&journal_path)? {
                    tracing::info!(
                        journal_path = %journal_path.display(),
                        "local session owner released its writer lease; detaching terminal"
                    );
                    return Ok(());
                }
                let mut stream = match UnixStream::connect(&socket_path).await {
                    Ok(stream) => stream,
                    Err(_) if !writer_is_active(&journal_path)? => {
                        tracing::info!(
                            journal_path = %journal_path.display(),
                            "local session owner exited before accepting a command; detaching terminal"
                        );
                        return Ok(());
                    }
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
            }
            _ = refresh.tick() => {
                for event in store.events_after(session_id, last_sequence, 1_000).await? {
                    last_sequence = event.sequence;
                    if event
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
                    if events.send(event).await.is_err() || stopped {
                        return Ok(());
                    }
                }
                for live in store.live_events_after(session_id, live_revision).await? {
                    live_revision = live_revision.max(live.revision);
                    if let Some(event) =
                        reasoning_delta_from_snapshot(live.event, &mut reasoning_snapshot)
                        && events.send(event).await.is_err()
                    {
                        return Ok(());
                    }
                }
                if !writer_is_active(&journal_path)? {
                    tracing::info!(
                        journal_path = %journal_path.display(),
                        "local session owner released its writer lease; detaching terminal"
                    );
                    return Ok(());
                }
            }
        }
    }
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

fn writer_is_active(journal_path: &Path) -> Result<bool> {
    Ok(SessionWriterLease::try_acquire(journal_path)?.is_none())
}

#[cfg(not(unix))]
pub async fn run_attached_session(
    _store: Arc<dyn SessionStore>,
    _session_id: Uuid,
    _journal_path: PathBuf,
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
    use crate::{PromptDelivery, SessionJournal};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn attached_commands_are_acknowledged_and_session_scoped() {
        let root = tempfile::tempdir().unwrap();
        let session_id = Uuid::new_v4();
        let socket_path = session_control_socket_path(root.path(), session_id);
        let journal =
            SessionJournal::open(root.path().join(format!("{session_id}.jsonl"))).unwrap();
        let writer = journal.try_acquire_writer().unwrap().unwrap();
        let (owner_tx, mut owner_rx) = mpsc::channel(1);
        let admissions = Arc::new(Mutex::new(HashSet::new()));
        let _server = LocalSessionControlServer::start_with_prompt_admissions(
            socket_path.clone(),
            session_id,
            &writer,
            owner_tx,
            Some(Arc::clone(&admissions)),
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
    async fn attachment_ends_cleanly_when_the_writer_disappears() {
        let root = tempfile::tempdir().unwrap();
        let session_id = Uuid::new_v4();
        let journal_path = root.path().join(format!("{session_id}.jsonl"));
        let socket_path = session_control_socket_path(root.path(), session_id);
        let journal = SessionJournal::open(&journal_path).unwrap();
        let writer = journal.try_acquire_writer().unwrap().unwrap();
        let (_command_tx, command_rx) = mpsc::channel(1);
        let (event_tx, _event_rx) = mpsc::channel(1);
        let store: Arc<dyn SessionStore> =
            Arc::new(crate::JsonlSessionStore::open(&journal_path).unwrap());

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
    async fn a_new_owner_reclaims_a_refused_socket() {
        let root = tempfile::tempdir().unwrap();
        let session_id = Uuid::new_v4();
        let socket_path = session_control_socket_path(root.path(), session_id);
        let journal =
            SessionJournal::open(root.path().join(format!("{session_id}.jsonl"))).unwrap();
        let writer = journal.try_acquire_writer().unwrap().unwrap();

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
        let root = tempfile::tempdir().unwrap();
        let session_id = Uuid::new_v4();
        let journal_path = root.path().join(format!("{session_id}.jsonl"));
        let socket_path = session_control_socket_path(root.path(), session_id);
        let journal = SessionJournal::open(&journal_path).unwrap();
        let _writer = journal.try_acquire_writer().unwrap().unwrap();
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
