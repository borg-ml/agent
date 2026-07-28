use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{ChildStdin, Command};
use tokio::sync::Notify;
use uuid::Uuid;

const MAX_ACTIVE_PROCESSES: usize = 8;
const CAPTURE_BYTES: usize = 512 * 1024;
const DEFAULT_YIELD_MS: u64 = 10_000;
const MAX_YIELD_MS: u64 = 30_000;
const DEFAULT_OUTPUT_TOKENS: usize = 10_000;
const MAX_OUTPUT_TOKENS: usize = 64_000;

#[derive(Debug, Clone)]
pub(crate) struct ProcessManager {
    inner: Arc<ProcessManagerInner>,
}

#[derive(Debug)]
struct ProcessManagerInner {
    processes: Mutex<HashMap<Uuid, Arc<ProcessEntry>>>,
}

#[derive(Debug)]
struct ProcessEntry {
    session_id: Uuid,
    command: String,
    cwd: PathBuf,
    pid: u32,
    stdin: tokio::sync::Mutex<Option<ChildStdin>>,
    output: Mutex<ProcessOutput>,
    status: Mutex<ProcessStatus>,
    changed: Notify,
    finished: Notify,
}

#[derive(Debug, Default)]
struct ProcessOutput {
    stdout: HeadTailBuffer,
    stderr: HeadTailBuffer,
}

#[derive(Debug, Clone, Default)]
struct ProcessStatus {
    running: bool,
    exit_code: Option<i32>,
    timed_out: bool,
    error: Option<String>,
}

#[derive(Debug, Default)]
struct HeadTailBuffer {
    head: Vec<u8>,
    tail: VecDeque<u8>,
    total_bytes: usize,
}

#[derive(Debug, Serialize)]
pub(crate) struct ProcessSnapshot {
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
            }),
        }
    }
}

impl ProcessManager {
    pub(crate) async fn exec(
        &self,
        owner_session_id: Uuid,
        root: &Path,
        command: String,
        workdir: Option<&str>,
        yield_time_ms: Option<u64>,
        max_output_tokens: Option<usize>,
        timeout_ms: u64,
    ) -> Result<ProcessSnapshot> {
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
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().context("command stdout pipe missing")?;
        let stderr = child.stderr.take().context("command stderr pipe missing")?;
        let entry = Arc::new(ProcessEntry {
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
        });
        self.inner
            .processes
            .lock()
            .expect("native process registry lock poisoned")
            .insert(process_id, Arc::clone(&entry));

        let stdout_task = tokio::spawn(read_pipe(stdout, Arc::clone(&entry), OutputStream::Stdout));
        let stderr_task = tokio::spawn(read_pipe(stderr, Arc::clone(&entry), OutputStream::Stderr));
        tokio::spawn(supervise_process(
            child,
            Arc::clone(&entry),
            Duration::from_millis(timeout_ms),
            stdout_task,
            stderr_task,
        ));

        let yield_for =
            Duration::from_millis(yield_time_ms.unwrap_or(DEFAULT_YIELD_MS).min(MAX_YIELD_MS));
        if entry
            .status
            .lock()
            .expect("native process status lock poisoned")
            .running
        {
            let _ = tokio::time::timeout(yield_for, entry.finished.notified()).await;
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

#[derive(Clone, Copy)]
enum OutputStream {
    Stdout,
    Stderr,
}

async fn read_pipe<R>(mut reader: R, entry: Arc<ProcessEntry>, stream: OutputStream)
where
    R: AsyncRead + Unpin,
{
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => break,
            Ok(read) => {
                let mut output = entry
                    .output
                    .lock()
                    .expect("native process output lock poisoned");
                match stream {
                    OutputStream::Stdout => output.stdout.push(&buffer[..read]),
                    OutputStream::Stderr => output.stderr.push(&buffer[..read]),
                }
                drop(output);
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
) {
    let result = match tokio::time::timeout(timeout, child.wait()).await {
        Ok(result) => result,
        Err(_) => {
            {
                entry
                    .status
                    .lock()
                    .expect("native process status lock poisoned")
                    .timed_out = true;
            }
            terminate_process_tree(entry.pid).await;
            child.wait().await
        }
    };
    entry.stdin.lock().await.take();
    let _ = tokio::join!(stdout_task, stderr_task);
    let mut status = entry
        .status
        .lock()
        .expect("native process status lock poisoned");
    status.running = false;
    match result {
        Ok(exit) => status.exit_code = exit.code(),
        Err(error) => status.error = Some(format!("failed to wait for process: {error}")),
    }
    drop(status);
    entry.changed.notify_waiters();
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
async fn terminate_process_tree(pid: u32) {
    terminate_process_tree_now(pid);
    tokio::time::sleep(Duration::from_millis(750)).await;
    // SAFETY: a negative, checked child process id addresses only the process group
    // created for this command. ESRCH is harmless when the group already exited.
    unsafe {
        libc::kill(-(pid as i32), libc::SIGKILL);
    }
}

#[cfg(windows)]
async fn terminate_process_tree(pid: u32) {
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
    // SAFETY: the process was spawned into its own process group with this id.
    unsafe {
        libc::kill(-(pid as i32), libc::SIGTERM);
    }
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
}
