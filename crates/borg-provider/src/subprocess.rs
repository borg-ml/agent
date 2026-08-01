//! Timeout + process-group wrappers for external provider commands.
//!
//! Why this exists:
//!   - Without a timeout, a provider process can hang indefinitely.
//!   - Without a process group, `run_stop` / systemd's SIGTERM
//!     targets only the Rust orchestrator. Children (cargo, git,
//!     borg subprocesses) keep running. This module always spawns
//!     with `process_group(0)` on Unix so the parent and all its
//!     descendants die together when the group is signalled.

use std::io;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::bounded_io::read_reader_lossy_text_with_limit;

const MAX_COMMAND_PIPE_BYTES: usize = 128 * 1024 * 1024;

/// Outcome of running a command with a timeout. Distinguishes clean
/// exit status (success/failure) from the timeout case so callers can
/// log it differently.
#[derive(Debug)]
pub struct CommandOutcome {
    pub success: bool,
    pub status_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub elapsed: Duration,
    pub timed_out: bool,
}

impl CommandOutcome {
    pub fn require_success(&self, label: &str) -> Result<()> {
        if self.timed_out {
            bail!(
                "{label} timed out after {:.1}s\nstderr:\n{}",
                self.elapsed.as_secs_f64(),
                self.stderr.trim()
            );
        }
        if !self.success {
            bail!(
                "{label} failed (code {:?}) after {:.1}s\nstderr:\n{}",
                self.status_code,
                self.elapsed.as_secs_f64(),
                self.stderr.trim()
            );
        }
        Ok(())
    }
}

/// Run a command with a wall-clock timeout and capture stdout/stderr.
/// The child is placed in its own process group on Unix so that
/// timing out (or the parent being signalled) cleanly kills every
/// descendant with `killpg`.
pub fn run_with_timeout<I, S>(
    program: &str,
    args: I,
    cwd: Option<&Path>,
    stdin: Option<&[u8]>,
    timeout: Duration,
) -> Result<CommandOutcome>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    run_with_optional_timeout(program, args, cwd, stdin, Some(timeout))
}

/// Run a command and capture stdout/stderr with bounded pipe readers.
/// When `timeout` is `None`, no wall-clock deadline is enforced, but stdout
/// and stderr still use the same byte caps as timed runs.
pub fn run_with_optional_timeout<I, S>(
    program: &str,
    args: I,
    cwd: Option<&Path>,
    stdin: Option<&[u8]>,
    timeout: Option<Duration>,
) -> Result<CommandOutcome>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let mut command = Command::new(program);
    command.args(args);
    if let Some(dir) = cwd {
        command.current_dir(dir);
    }
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    command.stdin(if stdin.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    });
    isolate_std_process_from_terminal(&mut command);

    let started_at = Instant::now();
    let mut child = command
        .spawn()
        .with_context(|| format!("spawn {program}"))?;
    let child_pid = child.id();

    if let (Some(bytes), Some(mut handle)) = (stdin, child.stdin.take()) {
        use std::io::Write;
        handle
            .write_all(bytes)
            .with_context(|| format!("pipe stdin to {program}"))?;
        drop(handle);
    }

    // Reader threads; keeps the child's pipes draining so large
    // outputs don't deadlock on a full pipe buffer.
    let stdout_reader = child
        .stdout
        .take()
        .map(|out| std::thread::spawn(move || read_pipe_lossy(out)));
    let stderr_reader = child
        .stderr
        .take()
        .map(|err| std::thread::spawn(move || read_pipe_lossy(err)));

    let deadline = timeout.map(|timeout| Instant::now() + timeout);
    let mut timed_out = false;
    let mut kill_wait_error: Option<io::Error> = None;
    let poll = Duration::from_millis(100);
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    timed_out = true;
                    kill_process_group(child_pid);
                    if let Err(error) = child.wait() {
                        kill_wait_error = Some(error);
                    }
                    break std::process::ExitStatus::default();
                }
                std::thread::sleep(poll);
            }
            Err(err) => {
                return Err(err).with_context(|| format!("wait on {program}"))?;
            }
        }
    };

    let stdout = collect_pipe_reader(stdout_reader, program, "stdout")?;
    let stderr = collect_pipe_reader(stderr_reader, program, "stderr")?;
    if let Some(error) = kill_wait_error {
        return Err(error).with_context(|| format!("wait on killed {program} after timeout"));
    }
    let elapsed = started_at.elapsed();
    Ok(CommandOutcome {
        success: status.success() && !timed_out,
        status_code: status.code(),
        stdout,
        stderr,
        elapsed,
        timed_out,
    })
}

fn read_pipe_lossy(pipe: impl io::Read) -> io::Result<String> {
    read_pipe_lossy_with_limit(pipe, MAX_COMMAND_PIPE_BYTES)
}

fn read_pipe_lossy_with_limit(pipe: impl io::Read, max_bytes: usize) -> io::Result<String> {
    read_reader_lossy_text_with_limit(pipe, "command pipe", max_bytes)
}

fn collect_pipe_reader(
    reader: Option<JoinHandle<io::Result<String>>>,
    program: &str,
    stream: &str,
) -> Result<String> {
    let Some(reader) = reader else {
        return Ok(String::new());
    };
    match reader.join() {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(error)) => Err(error).with_context(|| format!("read {stream} from {program}")),
        Err(_) => bail!("{stream} reader thread panicked for {program}"),
    }
}

/// Convenience: run + require success + return the captured stdout.
pub fn run_checked<I, S>(
    program: &str,
    args: I,
    cwd: Option<&Path>,
    timeout: Duration,
    label: &str,
) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let outcome = run_with_timeout(program, args, cwd, None, timeout)?;
    outcome.require_success(label)?;
    Ok(outcome.stdout)
}

/// Place a subprocess in its own process group on Unix. No-op on
/// platforms without process groups.
#[cfg(unix)]
pub(crate) fn isolate_std_process_from_terminal(command: &mut Command) {
    use std::os::unix::process::CommandExt;
    // Setting pgid to 0 tells the child to become the leader of a new
    // process group. Its own children then inherit the group, so
    // signalling the group SIGTERMs the entire subtree.
    command.process_group(0);
}

#[cfg(not(unix))]
pub(crate) fn isolate_std_process_from_terminal(_command: &mut Command) {}

/// Keep a long-lived async provider subprocess out of Borg's foreground
/// terminal process group. Closing one attached terminal may SIGHUP Borg's
/// foreground group, but it must not kill the app-server that owns the turn.
#[cfg(unix)]
pub(crate) fn isolate_async_process_from_terminal(command: &mut tokio::process::Command) {
    command.process_group(0);
}

#[cfg(not(unix))]
pub(crate) fn isolate_async_process_from_terminal(_command: &mut tokio::process::Command) {}

#[cfg(unix)]
fn kill_process_group(pid: u32) {
    // SAFETY: `killpg` takes a PGID (here the child's PID, since it
    // was set as group leader via `process_group(0)` at spawn) and a
    // signal number. We ignore the return; the caller will wait() for
    // the child regardless.
    let pgid = pid as i32;
    unsafe {
        libc::killpg(pgid, libc::SIGTERM);
    }
    // Give a short grace period then escalate to SIGKILL if the child
    // is still there. 500ms is enough for a `git` subprocess to tear
    // down; cargo will take longer but SIGKILL is fine for us.
    std::thread::sleep(Duration::from_millis(500));
    unsafe {
        libc::killpg(pgid, libc::SIGKILL);
    }
}

#[cfg(not(unix))]
fn kill_process_group(_pid: u32) {}

/// Reasonable defaults for common command timeouts.
pub mod timeouts {
    use std::time::Duration;

    /// `git status`, `git rev-parse`, `git add`, `git commit`, etc.
    /// These should always finish in seconds; allow generous headroom
    /// for a locked index on a slow disk.
    pub const GIT_QUICK: Duration = Duration::from_secs(60);

    /// `git revert` / `git apply`. A bit more headroom than GIT_QUICK
    /// in case we're operating on a multi-MB diff.
    pub const GIT_PATCH: Duration = Duration::from_secs(120);

    /// `cargo build --release`.
    pub const CARGO_BUILD: Duration = Duration::from_secs(900);

    /// Long-running Borg subcommands.
    pub const BORG_SUBCOMMAND: Duration = Duration::from_secs(600);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quick_command_succeeds() {
        let out = run_with_timeout("true", &[] as &[&str], None, None, Duration::from_secs(5))
            .expect("spawn");
        assert!(out.success);
        assert!(!out.timed_out);
    }

    #[test]
    fn non_zero_status_reports_failure() {
        let out = run_with_timeout("false", &[] as &[&str], None, None, Duration::from_secs(5))
            .expect("spawn");
        assert!(!out.success);
        assert!(!out.timed_out);
    }

    #[test]
    fn hanging_command_is_killed() {
        let out = run_with_timeout("sleep", ["30"], None, None, Duration::from_millis(300))
            .expect("spawn");
        assert!(out.timed_out);
        assert!(!out.success);
        assert!(
            out.elapsed < Duration::from_secs(3),
            "timeout should fire promptly, got {:?}",
            out.elapsed
        );
    }

    #[test]
    fn stdin_is_piped_through() {
        let out = run_with_timeout(
            "cat",
            &[] as &[&str],
            None,
            Some(b"hello from test"),
            Duration::from_secs(5),
        )
        .expect("spawn");
        assert!(out.success);
        assert_eq!(out.stdout.trim(), "hello from test");
    }

    #[test]
    fn non_utf8_output_is_preserved_lossily() {
        let out = run_with_timeout("printf", ["\\377ok"], None, None, Duration::from_secs(5))
            .expect("spawn");
        assert!(out.success);
        assert_eq!(out.stdout, "\u{FFFD}ok");
    }

    #[test]
    fn pipe_reader_rejects_oversized_output() {
        let error = read_pipe_lossy_with_limit(io::Cursor::new(b"abcde"), 4).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("exceeded"));
    }

    #[test]
    fn require_success_wraps_failures_with_label() {
        let out = run_with_timeout("false", &[] as &[&str], None, None, Duration::from_secs(5))
            .expect("spawn");
        let err = out.require_success("testing").expect_err("should fail");
        assert!(format!("{err}").contains("testing failed"));
    }
}
