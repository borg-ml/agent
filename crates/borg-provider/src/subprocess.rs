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
#[cfg(any(unix, test))]
use std::process::Child;
#[cfg(test)]
use std::process::ExitStatus;
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
    let poll = Duration::from_millis(10);
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    timed_out = true;
                    if let Err(error) = kill_process_group(&mut child) {
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

#[cfg(unix)]
fn kill_process_group(child: &mut Child) -> io::Result<()> {
    let pid = child.id();
    // SAFETY: `killpg` takes a PGID (here the child's PID, since it
    // was set as group leader via `process_group(0)` at spawn) and a
    // signal number. The probes and wait below establish the outcome.
    let pgid = pid as i32;
    unsafe {
        libc::killpg(pgid, libc::SIGTERM);
    }
    let deadline = Instant::now() + Duration::from_millis(500);
    let mut leader_reaped = child.try_wait()?.is_some();
    loop {
        if !process_group_exists(pid) {
            break;
        }
        if Instant::now() >= deadline {
            // SAFETY: the stored PGID identifies only the isolated child
            // group. ESRCH is harmless if it exited after the last probe.
            unsafe {
                libc::killpg(pgid, libc::SIGKILL);
            }
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
        if !leader_reaped {
            leader_reaped = child.try_wait()?.is_some();
        }
    }
    if !leader_reaped {
        child.wait()?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn kill_process_group(child: &mut std::process::Child) -> io::Result<()> {
    child.kill()?;
    child.wait()?;
    Ok(())
}

#[cfg(unix)]
fn process_group_exists(pid: u32) -> bool {
    // Signal 0 performs existence/permission checking without changing the
    // process group. EPERM still proves that the group exists.
    let result = unsafe { libc::killpg(pid as i32, 0) };
    result == 0 || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// Terminate and reap a long-lived std subprocess together with every process
/// that inherited its process group.
///
/// Provider adapters are deliberately isolated with `process_group(0)`. A
/// plain `Child::kill` only signals the group leader and can leave an active
/// shell/tool running after the adapter has disappeared. Cancellation paths
/// must use this helper before they report the owning turn as settled.
#[cfg(test)]
pub(crate) fn terminate_std_process_tree(child: &mut Child) -> io::Result<ExitStatus> {
    let pid = child.id();
    let status = child.try_wait()?;
    #[cfg(unix)]
    if process_group_exists(pid) {
        // The group can outlive its leader. Always signal the stored PGID when
        // it still exists, even if `try_wait` already reaped the leader.
        kill_process_group(child)?;
    }
    #[cfg(not(unix))]
    if status.is_none() {
        child.kill()?;
    }
    match status {
        Some(status) => Ok(status),
        None => child.wait(),
    }
}

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
    #[ignore = "explicit short provider subprocess performance gate"]
    fn short_provider_subprocess_completion_profile() {
        let mut samples = Vec::with_capacity(12);
        for _ in 0..12 {
            let out = run_with_timeout("true", &[] as &[&str], None, None, Duration::from_secs(5))
                .expect("spawn");
            assert!(out.success);
            samples.push(out.elapsed);
        }
        samples.sort_unstable();
        let p95 = samples[(samples.len() * 95).div_ceil(100).saturating_sub(1)];
        eprintln!("short provider subprocess completion p95: {p95:?}");
        assert!(
            p95 < Duration::from_millis(50),
            "short provider subprocess completion p95 exceeded 50 ms: {p95:?}"
        );
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

    #[cfg(unix)]
    #[test]
    #[ignore = "explicit provider timeout cleanup performance gate"]
    fn responsive_provider_timeout_cleanup_profile() {
        let out = run_with_timeout("sleep", ["30"], None, None, Duration::from_millis(50))
            .expect("spawn");
        eprintln!("responsive provider timeout cleanup: {:?}", out.elapsed);

        assert!(out.timed_out);
        assert!(
            out.elapsed < Duration::from_millis(250),
            "responsive provider timeout cleanup exceeded 250 ms: {:?}",
            out.elapsed
        );
    }

    #[cfg(unix)]
    #[test]
    fn process_tree_cleanup_kills_the_group_after_its_leader_exits() {
        let root = tempfile::tempdir().expect("temp root");
        let descendant_path = root.path().join("descendant.pid");
        let mut command = Command::new("sh");
        command
            .args([
                "-c",
                &format!(
                    "sleep 30 & printf '%s' \"$!\" > '{}'",
                    descendant_path.display()
                ),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        isolate_std_process_from_terminal(&mut command);
        let mut child = command.spawn().expect("isolated leader");
        assert!(child.wait().expect("leader exit").success());
        let descendant_pid = std::fs::read_to_string(&descendant_path)
            .expect("descendant pid")
            .parse::<i32>()
            .expect("numeric descendant pid");
        assert_eq!(unsafe { libc::kill(descendant_pid, 0) }, 0);

        terminate_std_process_tree(&mut child).expect("tree cleanup after leader exit");

        let deadline = Instant::now() + Duration::from_secs(1);
        while unsafe { libc::kill(descendant_pid, 0) } == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_ne!(
            unsafe { libc::kill(descendant_pid, 0) },
            0,
            "provider descendant survived after its process-group leader exited"
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
