use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{ChildStdin, Command};
use tokio::sync::{Notify, broadcast};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    RuntimeProcessStatus, RuntimeProcessStream, SessionEvent, SessionEventKind, SessionStore,
    SqliteSessionStore,
};

const MAX_ACTIVE_PROCESSES: usize = 8;
const CAPTURE_BYTES: usize = 512 * 1024;
const DEFAULT_YIELD_MS: u64 = 10_000;
const MAX_YIELD_MS: u64 = 30_000;
const DEFAULT_OUTPUT_TOKENS: usize = 10_000;
const MAX_OUTPUT_TOKENS: usize = 64_000;
const JOURNAL_OUTPUT_TOKENS: usize = 16_384;

#[derive(Debug, Clone)]
pub(crate) struct ProcessManager {
    inner: Arc<ProcessManagerInner>,
}

#[derive(Debug)]
struct ProcessManagerInner {
    processes: Mutex<HashMap<Uuid, Arc<ProcessEntry>>>,
    recovered_sessions: Mutex<HashSet<Uuid>>,
    updates: broadcast::Sender<(Uuid, Option<Vec<u8>>)>,
}

struct ProcessStartupGuard {
    manager: Arc<ProcessManagerInner>,
    process_id: Uuid,
    pid: Option<u32>,
}

impl Drop for ProcessStartupGuard {
    fn drop(&mut self) {
        if let Some(pid) = self.pid {
            force_kill_process_tree_now(pid);
            self.manager
                .processes
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .remove(&self.process_id);
        }
    }
}

#[derive(Debug)]
struct ProcessEntry {
    process_id: Uuid,
    session_id: Uuid,
    command: String,
    cwd: PathBuf,
    pid: u32,
    stdin: tokio::sync::Mutex<Option<ChildStdin>>,
    output: Mutex<ProcessOutput>,
    status: Mutex<ProcessStatus>,
    changed: Notify,
    finished: Notify,
    updates: broadcast::Sender<(Uuid, Option<Vec<u8>>)>,
}

#[derive(Debug, Clone, Default)]
struct ProcessOutput {
    stdout: HeadTailBuffer,
    stderr: HeadTailBuffer,
    journaled_stdout_bytes: usize,
    journaled_stderr_bytes: usize,
}

#[derive(Debug, Clone, Default)]
struct ProcessStatus {
    running: bool,
    exit_code: Option<i32>,
    timed_out: bool,
    terminated: bool,
    error: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct HeadTailBuffer {
    head: Vec<u8>,
    tail: VecDeque<u8>,
    total_bytes: usize,
}

#[derive(Debug, Serialize)]
pub struct ProcessSnapshot {
    pub session_id: Uuid,
    pub running: bool,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub command: String,
    pub cwd: PathBuf,
    pub stdout: String,
    pub stderr: String,
    pub stdout_omitted_bytes: usize,
    pub stderr_omitted_bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Default for ProcessManager {
    fn default() -> Self {
        Self {
            inner: Arc::new(ProcessManagerInner {
                processes: Mutex::new(HashMap::new()),
                recovered_sessions: Mutex::new(HashSet::new()),
                updates: broadcast::channel(256).0,
            }),
        }
    }
}

impl ProcessManager {
    pub(crate) fn subscribe_output(&self) -> broadcast::Receiver<(Uuid, Option<Vec<u8>>)> {
        self.inner.updates.subscribe()
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn exec(
        &self,
        owner_session_id: Uuid,
        root: &Path,
        command: String,
        workdir: Option<&str>,
        yield_time_ms: Option<u64>,
        max_output_tokens: Option<usize>,
        timeout_ms: u64,
        journal: Option<SqliteSessionStore>,
    ) -> Result<ProcessSnapshot> {
        self.exec_with_environment(
            owner_session_id,
            root,
            command,
            workdir,
            yield_time_ms,
            max_output_tokens,
            timeout_ms,
            journal,
            &BTreeMap::new(),
        )
        .await
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn exec_with_environment(
        &self,
        owner_session_id: Uuid,
        root: &Path,
        command: String,
        workdir: Option<&str>,
        yield_time_ms: Option<u64>,
        max_output_tokens: Option<usize>,
        timeout_ms: u64,
        journal: Option<SqliteSessionStore>,
        environment: &BTreeMap<String, String>,
    ) -> Result<ProcessSnapshot> {
        self.exec_with_cancel_and_environment(
            owner_session_id,
            root,
            command,
            workdir,
            yield_time_ms,
            max_output_tokens,
            timeout_ms,
            journal,
            CancellationToken::new(),
            environment,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn exec_with_cancel(
        &self,
        owner_session_id: Uuid,
        root: &Path,
        command: String,
        workdir: Option<&str>,
        yield_time_ms: Option<u64>,
        max_output_tokens: Option<usize>,
        timeout_ms: u64,
        journal: Option<SqliteSessionStore>,
        cancel: CancellationToken,
    ) -> Result<ProcessSnapshot> {
        self.exec_with_cancel_and_environment(
            owner_session_id,
            root,
            command,
            workdir,
            yield_time_ms,
            max_output_tokens,
            timeout_ms,
            journal,
            cancel,
            &BTreeMap::new(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn exec_with_cancel_and_environment(
        &self,
        owner_session_id: Uuid,
        root: &Path,
        command: String,
        workdir: Option<&str>,
        yield_time_ms: Option<u64>,
        max_output_tokens: Option<usize>,
        timeout_ms: u64,
        journal: Option<SqliteSessionStore>,
        cancel: CancellationToken,
        environment: &BTreeMap<String, String>,
    ) -> Result<ProcessSnapshot> {
        ensure!(!cancel.is_cancelled(), "process execution was cancelled");
        let cwd = resolve_workdir(root, workdir)?;
        let active = self
            .inner
            .processes
            .lock()
            .expect("native process registry lock poisoned")
            .values()
            .filter(|entry| {
                entry.session_id == owner_session_id
                    && entry
                        .status
                        .lock()
                        .expect("native process status lock poisoned")
                        .running
            })
            .count();
        if active >= MAX_ACTIVE_PROCESSES {
            bail!(
                "this agent session already has {MAX_ACTIVE_PROCESSES} active processes; finish or terminate one before starting another"
            );
        }

        let process_id = Uuid::new_v4();
        let mut process = shell_command(&command);
        crate::process_environment::configure_host_child_environment(&mut process);
        process.envs(environment);
        process
            .current_dir(&cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        process.process_group(0);
        #[cfg(windows)]
        process.creation_flags(0x0000_0200);

        let mut child = process
            .spawn()
            .with_context(|| format!("failed to start shell command `{command}`"))?;
        let pid = child
            .id()
            .context("spawned command did not expose a process id")?;
        let mut startup = ProcessStartupGuard {
            manager: self.inner.clone(),
            process_id,
            pid: Some(pid),
        };
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().context("command stdout pipe missing")?;
        let stderr = child.stderr.take().context("command stderr pipe missing")?;
        let entry = Arc::new(ProcessEntry {
            process_id,
            session_id: owner_session_id,
            command,
            cwd,
            pid,
            stdin: tokio::sync::Mutex::new(stdin),
            output: Mutex::new(ProcessOutput::default()),
            status: Mutex::new(ProcessStatus {
                running: true,
                ..ProcessStatus::default()
            }),
            changed: Notify::new(),
            finished: Notify::new(),
            updates: self.inner.updates.clone(),
        });
        self.inner
            .processes
            .lock()
            .expect("native process registry lock poisoned")
            .insert(process_id, Arc::clone(&entry));

        if let Some(store) = journal.as_ref()
            && let Err(error) = append_runtime_event(
                store,
                owner_session_id,
                SessionEventKind::RuntimeProcessStarted {
                    process_id,
                    pid,
                    command: entry.command.clone(),
                    cwd: entry.cwd.clone(),
                },
            )
            .await
        {
            self.inner
                .processes
                .lock()
                .expect("native process registry lock poisoned")
                .remove(&process_id);
            entry.stdin.lock().await.take();
            terminate_process_tree(pid).await;
            let _ = child.wait().await;
            startup.pid = None;
            return Err(error.context("failed to journal native process start"));
        }

        let stdout_task = tokio::spawn(read_pipe(
            stdout,
            Arc::clone(&entry),
            OutputStream::Stdout,
            journal.clone(),
        ));
        let stderr_task = tokio::spawn(read_pipe(
            stderr,
            Arc::clone(&entry),
            OutputStream::Stderr,
            journal.clone(),
        ));
        tokio::spawn(supervise_process(
            child,
            Arc::clone(&entry),
            Duration::from_millis(timeout_ms),
            stdout_task,
            stderr_task,
            journal,
            cancel.clone(),
        ));
        startup.pid = None;

        let yield_for =
            Duration::from_millis(yield_time_ms.unwrap_or(DEFAULT_YIELD_MS).min(MAX_YIELD_MS));
        if entry
            .status
            .lock()
            .expect("native process status lock poisoned")
            .running
        {
            tokio::select! {
                _ = tokio::time::timeout(yield_for, entry.finished.notified()) => {}
                _ = cancel.cancelled() => {
                    let _ = tokio::time::timeout(
                        Duration::from_secs(2),
                        wait_for_process_finish(&entry),
                    )
                    .await;
                    if !entry
                        .status
                        .lock()
                        .expect("native process status lock poisoned")
                        .running
                    {
                        self.inner
                            .processes
                            .lock()
                            .expect("native process registry lock poisoned")
                            .remove(&process_id);
                    }
                    bail!("process execution was cancelled");
                }
            }
        }
        let result = snapshot(
            process_id,
            &entry,
            max_output_tokens.unwrap_or(DEFAULT_OUTPUT_TOKENS),
        );
        if !result.running {
            self.inner
                .processes
                .lock()
                .expect("native process registry lock poisoned")
                .remove(&process_id);
        }
        Ok(result)
    }

    pub(crate) async fn write_stdin(
        &self,
        owner_session_id: Uuid,
        process_id: Uuid,
        chars: Option<&str>,
        terminate: bool,
        yield_time_ms: Option<u64>,
        max_output_tokens: Option<usize>,
    ) -> Result<ProcessSnapshot> {
        let entry = self.entry(owner_session_id, process_id)?;
        if terminate {
            {
                let mut status = entry
                    .status
                    .lock()
                    .expect("native process status lock poisoned");
                if status.running {
                    status.terminated = true;
                }
            }
            terminate_process_tree(entry.pid).await;
            entry.stdin.lock().await.take();
        } else if let Some(chars) = chars {
            let mut stdin = entry.stdin.lock().await;
            let pipe = stdin
                .as_mut()
                .context("process stdin is closed or the process has exited")?;
            pipe.write_all(chars.as_bytes()).await?;
            pipe.flush().await?;
        }
        let yield_for = Duration::from_millis(yield_time_ms.unwrap_or(250).min(MAX_YIELD_MS));
        if yield_for != Duration::ZERO
            && entry
                .status
                .lock()
                .expect("native process status lock poisoned")
                .running
        {
            let _ = tokio::time::timeout(yield_for, entry.changed.notified()).await;
        }
        let result = snapshot(
            process_id,
            &entry,
            max_output_tokens.unwrap_or(DEFAULT_OUTPUT_TOKENS),
        );
        if !result.running {
            self.inner
                .processes
                .lock()
                .expect("native process registry lock poisoned")
                .remove(&process_id);
        }
        Ok(result)
    }

    fn entry(&self, owner_session_id: Uuid, process_id: Uuid) -> Result<Arc<ProcessEntry>> {
        let entry = self
            .inner
            .processes
            .lock()
            .expect("native process registry lock poisoned")
            .get(&process_id)
            .cloned()
            .with_context(|| format!("unknown or expired process session `{process_id}`"))?;
        if entry.session_id != owner_session_id {
            bail!("process session `{process_id}` belongs to another agent session");
        }
        Ok(entry)
    }

    pub(crate) async fn terminate_session(&self, owner_session_id: Uuid) {
        let entries = {
            let mut processes = self
                .inner
                .processes
                .lock()
                .expect("native process registry lock poisoned");
            let ids = processes
                .iter()
                .filter_map(|(id, entry)| (entry.session_id == owner_session_id).then_some(*id))
                .collect::<Vec<_>>();
            ids.into_iter()
                .filter_map(|id| processes.remove(&id))
                .collect::<Vec<_>>()
        };
        for entry in &entries {
            let mut status = entry
                .status
                .lock()
                .expect("native process status lock poisoned");
            if status.running {
                status.terminated = true;
            }
        }
        futures::future::join_all(
            entries
                .iter()
                .map(|entry| terminate_process_tree(entry.pid)),
        )
        .await;
        for entry in entries {
            entry.stdin.lock().await.take();
        }
    }

    pub(crate) async fn recover_session(
        &self,
        session_id: Uuid,
        store: SqliteSessionStore,
    ) -> Result<()> {
        {
            let mut recovered = self
                .inner
                .recovered_sessions
                .lock()
                .expect("native process recovery lock poisoned");
            if !recovered.insert(session_id) {
                return Ok(());
            }
        }

        let result = self.recover_session_inner(session_id, store).await;
        if result.is_err() {
            self.inner
                .recovered_sessions
                .lock()
                .expect("native process recovery lock poisoned")
                .remove(&session_id);
        }
        result
    }

    async fn recover_session_inner(
        &self,
        session_id: Uuid,
        store: SqliteSessionStore,
    ) -> Result<()> {
        let processes = replay_process_events(&store.read(session_id).await?);
        for process in processes.into_values().filter(|process| !process.completed) {
            if self.has_process(session_id, process.process_id) {
                continue;
            }
            if process_is_alive(process.pid) {
                terminate_process_tree(process.pid).await;
            }
            let (stdout, stdout_omitted_bytes) =
                process.output.stdout.render(JOURNAL_OUTPUT_TOKENS);
            let (stderr, stderr_omitted_bytes) =
                process.output.stderr.render(JOURNAL_OUTPUT_TOKENS);
            append_runtime_completed(
                &store,
                session_id,
                process.process_id,
                process.pid,
                RuntimeProcessStatus::Orphaned,
                None,
                false,
                stdout,
                stderr,
                stdout_omitted_bytes,
                stderr_omitted_bytes,
                Some(
                    "native process owner was lost; the process was recovered and terminated"
                        .to_string(),
                ),
            )
            .await?;
        }
        Ok(())
    }

    fn has_process(&self, session_id: Uuid, process_id: Uuid) -> bool {
        self.inner
            .processes
            .lock()
            .expect("native process registry lock poisoned")
            .get(&process_id)
            .is_some_and(|entry| entry.session_id == session_id)
    }
}

impl Drop for ProcessManagerInner {
    fn drop(&mut self) {
        let entries = self
            .processes
            .get_mut()
            .expect("native process registry lock poisoned");
        for entry in entries.values() {
            terminate_process_tree_now(entry.pid);
        }
    }
}

impl HeadTailBuffer {
    fn from_text(text: &str) -> Self {
        let mut buffer = Self::default();
        buffer.push(text.as_bytes());
        buffer
    }

    fn push(&mut self, bytes: &[u8]) {
        self.total_bytes = self.total_bytes.saturating_add(bytes.len());
        let half = CAPTURE_BYTES / 2;
        let head_remaining = half.saturating_sub(self.head.len());
        let head_bytes = head_remaining.min(bytes.len());
        self.head.extend_from_slice(&bytes[..head_bytes]);
        for byte in &bytes[head_bytes..] {
            if self.tail.len() == half {
                self.tail.pop_front();
            }
            self.tail.push_back(*byte);
        }
    }

    fn render(&self, max_tokens: usize) -> (String, usize) {
        let max_bytes = max_tokens.clamp(1, MAX_OUTPUT_TOKENS).saturating_mul(4);
        let available = self.head.len().saturating_add(self.tail.len());
        let (head_keep, tail_keep) = if available <= max_bytes {
            (self.head.len(), self.tail.len())
        } else {
            let head_keep = max_bytes.div_ceil(2).min(self.head.len());
            (
                head_keep,
                max_bytes.saturating_sub(head_keep).min(self.tail.len()),
            )
        };
        let omitted = self.total_bytes.saturating_sub(head_keep + tail_keep);
        let mut bytes = Vec::with_capacity(head_keep + tail_keep + 80);
        bytes.extend_from_slice(&self.head[..head_keep]);
        if omitted > 0 {
            bytes.extend_from_slice(format!("\n… {omitted} bytes omitted …\n").as_bytes());
        }
        if tail_keep > 0 {
            bytes.extend(self.tail.iter().skip(self.tail.len() - tail_keep));
        }
        (String::from_utf8_lossy(&bytes).into_owned(), omitted)
    }
}

fn snapshot(process_id: Uuid, entry: &ProcessEntry, max_output_tokens: usize) -> ProcessSnapshot {
    let output = entry
        .output
        .lock()
        .expect("native process output lock poisoned");
    let status = entry
        .status
        .lock()
        .expect("native process status lock poisoned")
        .clone();
    let (stdout, stdout_omitted_bytes) = output.stdout.render(max_output_tokens);
    let (stderr, stderr_omitted_bytes) = output.stderr.render(max_output_tokens);
    ProcessSnapshot {
        session_id: process_id,
        running: status.running,
        exit_code: status.exit_code,
        timed_out: status.timed_out,
        command: entry.command.clone(),
        cwd: entry.cwd.clone(),
        stdout,
        stderr,
        stdout_omitted_bytes,
        stderr_omitted_bytes,
        error: status.error,
    }
}

fn snapshot_output(
    entry: &ProcessEntry,
    max_output_tokens: usize,
    stdout: bool,
) -> (String, usize) {
    let output = entry
        .output
        .lock()
        .expect("native process output lock poisoned");
    if stdout {
        output.stdout.render(max_output_tokens)
    } else {
        output.stderr.render(max_output_tokens)
    }
}

async fn wait_for_process_finish(entry: &Arc<ProcessEntry>) {
    loop {
        if !entry
            .status
            .lock()
            .expect("native process status lock poisoned")
            .running
        {
            return;
        }
        let notified = entry.finished.notified();
        if !entry
            .status
            .lock()
            .expect("native process status lock poisoned")
            .running
        {
            return;
        }
        notified.await;
    }
}

fn runtime_process_status(status: &ProcessStatus) -> RuntimeProcessStatus {
    if status.timed_out {
        RuntimeProcessStatus::TimedOut
    } else if status.terminated {
        RuntimeProcessStatus::Terminated
    } else if status.error.is_some() {
        RuntimeProcessStatus::Failed
    } else {
        RuntimeProcessStatus::Exited
    }
}

async fn append_runtime_event(
    store: &SqliteSessionStore,
    session_id: Uuid,
    kind: SessionEventKind,
) -> Result<SessionEvent> {
    store.append(SessionEvent::new(session_id, 0, kind)).await
}

#[allow(clippy::too_many_arguments)]
async fn append_runtime_completed(
    store: &SqliteSessionStore,
    session_id: Uuid,
    process_id: Uuid,
    pid: u32,
    status: RuntimeProcessStatus,
    exit_code: Option<i32>,
    timed_out: bool,
    stdout: String,
    stderr: String,
    stdout_omitted_bytes: usize,
    stderr_omitted_bytes: usize,
    error: Option<String>,
) -> Result<SessionEvent> {
    append_runtime_event(
        store,
        session_id,
        SessionEventKind::RuntimeProcessCompleted {
            process_id,
            pid,
            status,
            exit_code,
            timed_out,
            stdout,
            stderr,
            stdout_omitted_bytes,
            stderr_omitted_bytes,
            error,
        },
    )
    .await
}

#[derive(Debug, Clone)]
struct PersistedProcess {
    process_id: Uuid,
    pid: u32,
    output: ProcessOutput,
    completed: bool,
}

fn replay_process_events(events: &[SessionEvent]) -> HashMap<Uuid, PersistedProcess> {
    let mut processes = HashMap::new();
    for event in events {
        match &event.kind {
            SessionEventKind::RuntimeProcessStarted {
                process_id, pid, ..
            } => {
                processes.insert(
                    *process_id,
                    PersistedProcess {
                        process_id: *process_id,
                        pid: *pid,
                        output: ProcessOutput::default(),
                        completed: false,
                    },
                );
            }
            SessionEventKind::RuntimeProcessOutput {
                process_id, chunk, ..
            } => {
                if let Some(process) = processes.get_mut(process_id)
                    && !process.completed
                {
                    match &event.kind {
                        SessionEventKind::RuntimeProcessOutput {
                            stream: RuntimeProcessStream::Stdout,
                            ..
                        } => process.output.stdout.push(chunk.as_bytes()),
                        SessionEventKind::RuntimeProcessOutput {
                            stream: RuntimeProcessStream::Stderr,
                            ..
                        } => process.output.stderr.push(chunk.as_bytes()),
                        _ => unreachable!("matched runtime output event"),
                    }
                }
            }
            SessionEventKind::RuntimeProcessCompleted {
                process_id,
                pid,
                stdout,
                stderr,
                ..
            } => {
                let process = processes
                    .entry(*process_id)
                    .or_insert_with(|| PersistedProcess {
                        process_id: *process_id,
                        pid: *pid,
                        output: ProcessOutput::default(),
                        completed: false,
                    });
                process.pid = *pid;
                process.output.stdout = HeadTailBuffer::from_text(stdout);
                process.output.stderr = HeadTailBuffer::from_text(stderr);
                process.completed = true;
            }
            _ => {}
        }
    }
    processes
}

#[derive(Clone, Copy)]
enum OutputStream {
    Stdout,
    Stderr,
}

impl From<OutputStream> for RuntimeProcessStream {
    fn from(stream: OutputStream) -> Self {
        match stream {
            OutputStream::Stdout => Self::Stdout,
            OutputStream::Stderr => Self::Stderr,
        }
    }
}

async fn read_pipe<R>(
    mut reader: R,
    entry: Arc<ProcessEntry>,
    stream: OutputStream,
    journal: Option<SqliteSessionStore>,
) where
    R: AsyncRead + Unpin,
{
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => break,
            Ok(read) => {
                if matches!(stream, OutputStream::Stdout) {
                    let _ = entry
                        .updates
                        .send((entry.process_id, Some(buffer[..read].to_vec())));
                }
                let journal_chunk = {
                    let mut output = entry
                        .output
                        .lock()
                        .expect("native process output lock poisoned");
                    let journal_bytes = match stream {
                        OutputStream::Stdout => {
                            let keep = CAPTURE_BYTES
                                .saturating_sub(output.journaled_stdout_bytes)
                                .min(read);
                            output.journaled_stdout_bytes =
                                output.journaled_stdout_bytes.saturating_add(keep);
                            keep
                        }
                        OutputStream::Stderr => {
                            let keep = CAPTURE_BYTES
                                .saturating_sub(output.journaled_stderr_bytes)
                                .min(read);
                            output.journaled_stderr_bytes =
                                output.journaled_stderr_bytes.saturating_add(keep);
                            keep
                        }
                    };
                    match stream {
                        OutputStream::Stdout => output.stdout.push(&buffer[..read]),
                        OutputStream::Stderr => output.stderr.push(&buffer[..read]),
                    }
                    (journal_bytes > 0 && journal.is_some()).then(|| {
                        (
                            RuntimeProcessStream::from(stream),
                            String::from_utf8_lossy(&buffer[..journal_bytes]).into_owned(),
                        )
                    })
                };
                if let (Some(store), Some((stream, chunk))) = (journal.as_ref(), journal_chunk)
                    && let Err(error) = append_runtime_event(
                        store,
                        entry.session_id,
                        SessionEventKind::RuntimeProcessOutput {
                            process_id: entry.process_id,
                            stream,
                            chunk,
                        },
                    )
                    .await
                {
                    {
                        let mut status = entry
                            .status
                            .lock()
                            .expect("native process status lock poisoned");
                        if status.error.is_none() {
                            status.error =
                                Some(format!("failed to journal process output: {error}"));
                        }
                    }
                    terminate_process_tree(entry.pid).await;
                    break;
                }
                entry.changed.notify_waiters();
            }
            Err(error) => {
                entry
                    .status
                    .lock()
                    .expect("native process status lock poisoned")
                    .error = Some(format!("failed to read process output: {error}"));
                entry.changed.notify_waiters();
                break;
            }
        }
    }
}

async fn supervise_process(
    mut child: tokio::process::Child,
    entry: Arc<ProcessEntry>,
    timeout: Duration,
    stdout_task: tokio::task::JoinHandle<()>,
    stderr_task: tokio::task::JoinHandle<()>,
    journal: Option<SqliteSessionStore>,
    cancel: CancellationToken,
) {
    let result = tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            entry.status.lock().expect("native process status lock poisoned").terminated = true;
            terminate_process_tree(entry.pid).await;
            child.wait().await
        }
        result = tokio::time::timeout(timeout, child.wait()) => match result {
            Ok(result) => result,
            Err(_) => {
                entry
                    .status
                    .lock()
                    .expect("native process status lock poisoned")
                    .timed_out = true;
                terminate_process_tree(entry.pid).await;
                child.wait().await
            }
        },
    };
    entry.stdin.lock().await.take();
    let _ = tokio::join!(stdout_task, stderr_task);
    let (runtime_status, exit_code, timed_out, error) = {
        let mut status = entry
            .status
            .lock()
            .expect("native process status lock poisoned");
        status.running = false;
        match result {
            Ok(exit) => status.exit_code = exit.code(),
            Err(error) => status.error = Some(format!("failed to wait for process: {error}")),
        }
        (
            runtime_process_status(&status),
            status.exit_code,
            status.timed_out,
            status.error.clone(),
        )
    };
    if let Some(store) = journal.as_ref() {
        let (stdout, stdout_omitted_bytes) = snapshot_output(&entry, JOURNAL_OUTPUT_TOKENS, true);
        let (stderr, stderr_omitted_bytes) = snapshot_output(&entry, JOURNAL_OUTPUT_TOKENS, false);
        let _ = append_runtime_completed(
            store,
            entry.session_id,
            entry.process_id,
            entry.pid,
            runtime_status,
            exit_code,
            timed_out,
            stdout,
            stderr,
            stdout_omitted_bytes,
            stderr_omitted_bytes,
            error,
        )
        .await;
    }
    entry.changed.notify_waiters();
    let _ = entry.updates.send((entry.process_id, None));
    entry.finished.notify_waiters();
}

fn resolve_workdir(root: &Path, workdir: Option<&str>) -> Result<PathBuf> {
    let root = root.canonicalize().context("canonicalize workspace root")?;
    let candidate = match workdir {
        None | Some("") | Some(".") => root.clone(),
        Some(workdir) => {
            let workdir = Path::new(workdir);
            if workdir.is_absolute() {
                workdir.to_path_buf()
            } else {
                root.join(workdir)
            }
        }
    };
    let cwd = candidate
        .canonicalize()
        .with_context(|| format!("resolve workdir {}", candidate.display()))?;
    if !cwd.starts_with(&root) || !cwd.is_dir() {
        bail!("workdir must be an existing directory inside the workspace");
    }
    Ok(cwd)
}

#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    // SAFETY: signal zero performs no mutation and only probes process
    // existence/permission for the recorded process id.
    let result = unsafe { libc::kill(pid as i32, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(windows)]
fn process_is_alive(pid: u32) -> bool {
    pid != 0
}

#[cfg(unix)]
fn shell_command(command: &str) -> Command {
    let shell = std::env::var_os("SHELL")
        .filter(|value| Path::new(value).is_absolute())
        .unwrap_or_else(|| "/bin/sh".into());
    let mut process = Command::new(shell);
    process.args(["-lc", command]);
    process
}

#[cfg(windows)]
fn shell_command(command: &str) -> Command {
    let shell = std::env::var_os("ComSpec").unwrap_or_else(|| "cmd.exe".into());
    let mut process = Command::new(shell);
    process.args(["/D", "/S", "/C", command]);
    process
}

#[cfg(unix)]
pub(crate) async fn terminate_process_tree(pid: u32) {
    if pid == 0 || pid > i32::MAX as u32 {
        return;
    }
    terminate_process_tree_now(pid);
    let deadline = Instant::now() + Duration::from_millis(750);
    while process_group_is_alive(pid) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    if !process_group_is_alive(pid) {
        return;
    }
    force_kill_process_tree_now(pid);
}

#[cfg(unix)]
fn process_group_is_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    // SAFETY: signal zero checks whether the isolated child process group still
    // exists without changing it. EPERM also proves that the group is alive.
    let result = unsafe { libc::kill(-(pid as i32), 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(windows)]
pub(crate) async fn terminate_process_tree(pid: u32) {
    let _ = Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
}

#[cfg(unix)]
fn terminate_process_tree_now(pid: u32) {
    if pid == 0 || pid > i32::MAX as u32 {
        return;
    }
    // SAFETY: the process was spawned into its own process group with this id.
    unsafe {
        libc::kill(-(pid as i32), libc::SIGTERM);
    }
}

#[cfg(unix)]
pub(crate) fn force_kill_process_tree_now(pid: u32) {
    if pid == 0 || pid > i32::MAX as u32 {
        return;
    }
    // SAFETY: the caller retains the id of a child spawned as its own group leader.
    unsafe {
        libc::kill(-(pid as i32), libc::SIGKILL);
    }
}

#[cfg(windows)]
pub(crate) fn force_kill_process_tree_now(pid: u32) {
    terminate_process_tree_now(pid);
}

#[cfg(windows)]
fn terminate_process_tree_now(pid: u32) {
    let _ = std::process::Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn head_tail_output_preserves_the_failure_tail() {
        let mut output = HeadTailBuffer::default();
        output.push(&vec![b'x'; CAPTURE_BYTES]);
        output.push(b"fatal: final failure");
        let (text, omitted) = output.render(100);
        assert!(omitted > 0);
        assert!(text.starts_with('x'));
        assert!(text.ends_with("fatal: final failure"));
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn process_can_background_poll_and_receive_stdin() {
        let root = tempfile::tempdir().expect("workspace");
        let manager = ProcessManager::default();
        let owner = Uuid::new_v4();
        let first = manager
            .exec(
                owner,
                root.path(),
                "read line; printf 'got:%s\\n' \"$line\"".to_string(),
                None,
                Some(5),
                Some(1_000),
                10_000,
                None,
            )
            .await
            .expect("spawn");
        assert!(first.running);
        let mut completed = manager
            .write_stdin(
                owner,
                first.session_id,
                Some("hello\n"),
                false,
                Some(2_000),
                Some(1_000),
            )
            .await
            .expect("write stdin");
        for _ in 0..10 {
            if !completed.running {
                break;
            }
            completed = manager
                .write_stdin(owner, first.session_id, None, false, Some(250), Some(1_000))
                .await
                .expect("poll");
        }
        assert!(!completed.running);
        assert_eq!(completed.exit_code, Some(0));
        assert!(
            completed.stdout.contains("got:hello"),
            "unexpected process result: {completed:?}"
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn process_inherits_explicit_session_capabilities() {
        let root = tempfile::tempdir().expect("workspace");
        let manager = ProcessManager::default();
        let environment = BTreeMap::from([(
            "BORG_AGENT_TOOL_SOCKET".to_string(),
            "/tmp/borg-session.sock".to_string(),
        )]);
        let result = manager
            .exec_with_environment(
                Uuid::new_v4(),
                root.path(),
                "printf %s \"$BORG_AGENT_TOOL_SOCKET\"".to_string(),
                None,
                Some(2_000),
                Some(100),
                10_000,
                None,
                &environment,
            )
            .await
            .expect("command");

        assert_eq!(result.stdout, "/tmp/borg-session.sock");
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn stopping_a_session_reaps_its_background_processes() {
        let root = tempfile::tempdir().expect("workspace");
        let manager = ProcessManager::default();
        let owner = Uuid::new_v4();
        let process = manager
            .exec(
                owner,
                root.path(),
                "sleep 30".to_string(),
                None,
                Some(1),
                Some(100),
                60_000,
                None,
            )
            .await
            .expect("spawn");
        assert!(process.running);
        manager.terminate_session(owner).await;
        assert!(manager.entry(owner, process.session_id).is_err());
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn background_cancellation_is_scoped_and_journaled() {
        let root = tempfile::tempdir().unwrap();
        let store = SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
            .await
            .unwrap();
        let owner = Uuid::new_v4();
        store.create_session(owner).await.unwrap();
        let manager = ProcessManager::default();
        let cancel = CancellationToken::new();
        let scoped = manager
            .exec_with_cancel(
                owner,
                root.path(),
                "sleep 30".into(),
                None,
                Some(1),
                Some(100),
                60_000,
                Some(store.clone()),
                cancel.clone(),
            )
            .await
            .unwrap();
        let unrelated = manager
            .exec(
                owner,
                root.path(),
                "sleep 30".into(),
                None,
                Some(1),
                Some(100),
                60_000,
                None,
            )
            .await
            .unwrap();
        assert!(scoped.running && unrelated.running);
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let events = store.events_after(owner, 0, 100).await.unwrap();
                if events.iter().any(|event| matches!(event.kind,
                    SessionEventKind::RuntimeProcessCompleted { process_id, status: RuntimeProcessStatus::Terminated, .. }
                    if process_id == scoped.session_id)) { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("background cancellation must reach its durable terminal event");
        assert!(
            manager
                .write_stdin(owner, unrelated.session_id, None, false, Some(1), Some(100))
                .await
                .unwrap()
                .running
        );
        manager.terminate_session(owner).await;
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn dropping_command_startup_kills_the_unjournaled_process() {
        let root = tempfile::tempdir().unwrap();
        let store = SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
            .await
            .unwrap();
        let owner = Uuid::new_v4();
        store.create_session(owner).await.unwrap();
        let transaction = store.pool().begin_with("BEGIN IMMEDIATE").await.unwrap();
        let manager = ProcessManager::default();
        let task_manager = manager.clone();
        let cwd = root.path().to_path_buf();
        let task = tokio::spawn(async move {
            task_manager
                .exec(
                    owner,
                    &cwd,
                    "/bin/sh -c 'echo $$ > startup.pid; exec sleep 30'".into(),
                    None,
                    Some(30_000),
                    Some(100),
                    60_000,
                    Some(store),
                )
                .await
        });
        let pid: i32 = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Ok(pid) = tokio::fs::read_to_string(root.path().join("startup.pid")).await
                    && let Ok(pid) = pid.trim().parse()
                {
                    break pid;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        transaction.rollback().await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while unsafe { libc::kill(pid, 0) } == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("aborted startup must reap its child");
        assert!(manager.inner.processes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn cancellable_process_execution_reaps_the_workflow_child() {
        let root = tempfile::tempdir().expect("workspace");
        let manager = ProcessManager::default();
        let owner = Uuid::new_v4();
        let cancel = CancellationToken::new();
        let task_manager = manager.clone();
        let task_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            task_manager
                .exec_with_cancel(
                    owner,
                    root.path(),
                    "sleep 30".to_string(),
                    None,
                    Some(30_000),
                    Some(100),
                    60_000,
                    None,
                    task_cancel,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancel.cancel();
        let error = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("cancellation completes")
            .expect("process task")
            .expect_err("cancelled process must not return a snapshot");
        assert!(error.to_string().contains("cancelled"));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            manager
                .inner
                .processes
                .lock()
                .expect("process registry lock")
                .values()
                .all(|entry| entry.session_id != owner)
        );
    }

    #[tokio::test]
    #[ignore = "explicit native process cancellation performance gate"]
    #[cfg(unix)]
    async fn responsive_process_cancellation_profile() {
        let root = tempfile::tempdir().expect("workspace");
        let manager = ProcessManager::default();
        let owner = Uuid::new_v4();
        let cancel = CancellationToken::new();
        let task_manager = manager.clone();
        let task_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            task_manager
                .exec_with_cancel(
                    owner,
                    root.path(),
                    "sleep 30".to_string(),
                    None,
                    Some(30_000),
                    Some(100),
                    60_000,
                    None,
                    task_cancel,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let started = std::time::Instant::now();
        cancel.cancel();
        let error = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("cancellation completes")
            .expect("process task")
            .expect_err("cancelled process must not return a snapshot");
        let elapsed = started.elapsed();
        eprintln!("native process cancellation: {elapsed:?}");

        assert!(error.to_string().contains("cancelled"));
        assert!(
            elapsed < Duration::from_millis(200),
            "native process cancellation exceeded 200 ms: {elapsed:?}"
        );
        assert!(
            manager
                .inner
                .processes
                .lock()
                .expect("process registry lock")
                .values()
                .all(|entry| entry.session_id != owner)
        );
    }

    #[test]
    fn replay_process_events_keeps_running_processes_recoverable() {
        let session_id = Uuid::new_v4();
        let process_id = Uuid::new_v4();
        let events = vec![
            SessionEvent::new(
                session_id,
                1,
                SessionEventKind::RuntimeProcessStarted {
                    process_id,
                    pid: 42,
                    command: "sleep 30".to_string(),
                    cwd: "/workspace".into(),
                },
            ),
            SessionEvent::new(
                session_id,
                2,
                SessionEventKind::RuntimeProcessOutput {
                    process_id,
                    stream: RuntimeProcessStream::Stdout,
                    chunk: "still running\n".to_string(),
                },
            ),
        ];
        let processes = replay_process_events(&events);
        let process = processes.get(&process_id).expect("running process");
        assert!(!process.completed);
        let (output, _) = process.output.stdout.render(100);
        assert_eq!(output, "still running\n");
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn completed_processes_are_durably_journaled_before_poll_returns() {
        let directory = tempfile::tempdir().expect("database directory");
        let store = SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .expect("session store");
        let session_id = Uuid::new_v4();
        store.create_session(session_id).await.expect("session");
        let manager = ProcessManager::default();
        let result = manager
            .exec(
                session_id,
                directory.path(),
                "printf 'hello'; printf 'warning' >&2".to_string(),
                None,
                Some(2_000),
                Some(1_000),
                10_000,
                Some(store.clone()),
            )
            .await
            .expect("command");
        assert!(!result.running);
        let events = store.read(session_id).await.expect("journal");
        assert!(matches!(
            events.first().map(|event| &event.kind),
            Some(SessionEventKind::RuntimeProcessStarted { .. })
        ));
        assert!(events.iter().any(|event| matches!(
            event.kind,
            SessionEventKind::RuntimeProcessOutput {
                stream: RuntimeProcessStream::Stdout,
                ..
            }
        )));
        assert!(events.iter().any(|event| matches!(
            event.kind,
            SessionEventKind::RuntimeProcessOutput {
                stream: RuntimeProcessStream::Stderr,
                ..
            }
        )));
        assert!(events.iter().any(|event| matches!(
            event.kind,
            SessionEventKind::RuntimeProcessCompleted {
                status: RuntimeProcessStatus::Exited,
                ..
            }
        )));
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn concurrent_processes_keep_session_ownership_and_output_separate() {
        let root = tempfile::tempdir().expect("workspace");
        let manager = ProcessManager::default();
        let owner = Uuid::new_v4();
        let (left, right) = tokio::join!(
            manager.exec(
                owner,
                root.path(),
                "printf left".to_string(),
                None,
                Some(2_000),
                Some(100),
                10_000,
                None,
            ),
            manager.exec(
                owner,
                root.path(),
                "printf right".to_string(),
                None,
                Some(2_000),
                Some(100),
                10_000,
                None,
            ),
        );
        let left = left.expect("left command");
        let right = right.expect("right command");
        assert!(!left.running);
        assert!(!right.running);
        assert_eq!(left.stdout, "left");
        assert_eq!(right.stdout, "right");
        assert_ne!(left.session_id, right.session_id);
    }

    #[tokio::test]
    async fn sqlite_recovery_is_idempotent_for_an_orphaned_process() {
        let directory = tempfile::tempdir().expect("database directory");
        let store = SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .expect("session store");
        let session_id = Uuid::new_v4();
        let process_id = Uuid::new_v4();
        store.create_session(session_id).await.expect("session");
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::RuntimeProcessStarted {
                    process_id,
                    pid: 0,
                    command: "sleep 30".to_string(),
                    cwd: directory.path().to_path_buf(),
                },
            ))
            .await
            .expect("start journal");

        let manager = ProcessManager::default();
        manager
            .recover_session(session_id, store.clone())
            .await
            .expect("recover");
        manager
            .recover_session(session_id, store.clone())
            .await
            .expect("repeat recovery");

        let completions = store
            .read(session_id)
            .await
            .expect("read journal")
            .into_iter()
            .filter(|event| {
                matches!(
                    event.kind,
                    SessionEventKind::RuntimeProcessCompleted {
                        process_id: id,
                        status: RuntimeProcessStatus::Orphaned,
                        ..
                    } if id == process_id
                )
            })
            .count();
        assert_eq!(completions, 1);
    }
}
