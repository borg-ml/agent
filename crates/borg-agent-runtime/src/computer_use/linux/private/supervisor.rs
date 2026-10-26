//! Keep a non-detached app's process group alive and kill it on display/helper loss.
use std::ffi::OsString;
use std::io::{BufRead, BufReader, Read};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};

pub(super) fn command(argv: &[String], control: &Path) -> Result<(Command, UnixStream)> {
    let (startup, report) = UnixStream::pair()?;
    let mut command = Command::new(crate::subagents::borg_executable()?);
    command
        .args(["__computer-use-helper", "--private-app"])
        .arg(control)
        .arg(report.as_raw_fd().to_string())
        .args(argv);
    configure(&mut command);
    unsafe {
        command.pre_exec(move || {
            if libc::fcntl(report.as_raw_fd(), libc::F_SETFD, 0) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok((command, startup))
}

pub(super) fn started(startup: UnixStream) -> Result<()> {
    startup.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut line = Vec::new();
    BufReader::new(startup)
        .take(16 * 1024)
        .read_until(b'\n', &mut line)?;
    let response: serde_json::Value =
        serde_json::from_slice(&line).context("app supervisor startup report unavailable")?;
    ensure!(
        response["ok"] == true,
        "{}",
        response["error"]
            .as_str()
            .unwrap_or("app supervisor startup failed")
    );
    Ok(())
}

fn configure(command: &mut Command) {
    let owner = unsafe { libc::getpid() };
    // Block parent-death SIGTERM across exec until the supervisor can consume
    // it. Otherwise worker death can kill the supervisor before it cleans up.
    unsafe {
        command.pre_exec(move || {
            let mut mask = std::mem::zeroed();
            libc::sigemptyset(&mut mask);
            libc::sigaddset(&mut mask, libc::SIGTERM);
            if libc::setsid() < 0
                || libc::sigprocmask(libc::SIG_BLOCK, &mask, std::ptr::null_mut()) < 0
                || libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) < 0
            {
                return Err(std::io::Error::last_os_error());
            }
            if libc::getppid() != owner {
                return Err(std::io::Error::other(
                    "desktop worker exited before app startup",
                ));
            }
            Ok(())
        });
    }
}

pub(super) fn run(control: &Path, argv: &[OsString], ready: impl FnOnce()) -> Result<i32> {
    ensure!(!argv.is_empty(), "private app argv is required");
    ensure!(
        unsafe { libc::getpgrp() == libc::getpid() },
        "app supervisor must own its process group"
    );
    let display = UnixStream::connect(control).context("monitoring the private display")?;
    // Adopt descendants when a launcher shell exits; do not release/reuse the
    // process-group ID while its game is still running.
    ensure!(
        unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1) } == 0,
        "cannot supervise app descendants"
    );
    let mut mask = unsafe { std::mem::zeroed() };
    unsafe {
        libc::sigemptyset(&mut mask);
        for signal in [libc::SIGTERM, libc::SIGINT, libc::SIGCHLD] {
            libc::sigaddset(&mut mask, signal);
        }
    }
    let mut child_mask = unsafe { std::mem::zeroed() };
    ensure!(
        unsafe { libc::sigprocmask(libc::SIG_BLOCK, &mask, &mut child_mask) } == 0,
        "cannot block supervisor signals"
    );
    unsafe {
        for signal in [libc::SIGTERM, libc::SIGINT, libc::SIGCHLD] {
            libc::sigdelset(&mut child_mask, signal);
        }
    }
    let fd = unsafe { libc::signalfd(-1, &mask, libc::SFD_CLOEXEC | libc::SFD_NONBLOCK) };
    ensure!(fd >= 0, "cannot monitor supervisor signals");
    let signals = unsafe { OwnedFd::from_raw_fd(fd) };
    let mut command = Command::new(&argv[0]);
    command.args(&argv[1..]);
    unsafe {
        command.pre_exec(move || {
            if libc::sigprocmask(libc::SIG_SETMASK, &child_mask, std::ptr::null_mut()) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command.spawn().context("starting private app")?;
    let leader = child.id() as i32;
    ready();
    let mut exit_code = 0;
    let mut stopping = None;
    loop {
        // Reap both the launcher and any adopted descendants.
        loop {
            let mut status = 0;
            let pid = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
            if pid == leader {
                exit_code = if libc::WIFEXITED(status) {
                    libc::WEXITSTATUS(status)
                } else {
                    -libc::WTERMSIG(status)
                };
            }
            if pid > 0 {
                continue;
            }
            if pid < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD) {
                return Ok(exit_code);
            }
            break;
        }
        let timeout = stopping.map_or(-1, |deadline: Instant| {
            deadline
                .saturating_duration_since(Instant::now())
                .as_millis()
                .min(i32::MAX as u128) as i32
        });
        if stopping.is_some() && timeout == 0 {
            // Includes this supervisor: no group member can survive escalation.
            unsafe {
                libc::killpg(0, libc::SIGKILL);
            }
            unreachable!("SIGKILL must terminate the app group");
        }
        let mut polls = [
            libc::pollfd {
                fd: signals.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: if stopping.is_some() {
                    -1
                } else {
                    display.as_raw_fd()
                },
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        if unsafe { libc::poll(polls.as_mut_ptr(), polls.len() as _, timeout) } < 0 {
            if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            unsafe {
                libc::killpg(0, libc::SIGKILL);
            }
            unreachable!("SIGKILL must terminate the app group");
        }
        let mut stop = polls[1].revents != 0;
        let mut signal: libc::signalfd_siginfo = unsafe { std::mem::zeroed() };
        while unsafe {
            libc::read(
                signals.as_raw_fd(),
                (&mut signal as *mut libc::signalfd_siginfo).cast(),
                std::mem::size_of_val(&signal),
            )
        } > 0
        {
            stop |= matches!(signal.ssi_signo as i32, libc::SIGTERM | libc::SIGINT);
        }
        if stop && stopping.is_none() {
            stopping = Some(Instant::now() + Duration::from_secs(3));
            unsafe {
                libc::killpg(0, libc::SIGTERM);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::ManagedChild;
    use super::*;
    use std::os::unix::net::UnixListener;

    #[test]
    fn failed_startup_is_not_a_successful_launch() {
        use std::io::Write;
        let (startup, mut report) = UnixStream::pair().unwrap();
        report
            .write_all(b"{\"ok\":false,\"error\":\"starting private app failed\"}\n")
            .unwrap();
        assert!(started(startup).is_err());
        let (startup, mut report) = UnixStream::pair().unwrap();
        report.write_all(b"{\"ok\":true}\n").unwrap();
        started(startup).unwrap();
    }

    #[test]
    #[ignore = "subprocess entry for process-group lifetime tests"]
    fn supervisor_entry() {
        let control = std::env::var_os("BORG_TEST_APP_CONTROL").unwrap();
        let script = std::env::var_os("BORG_TEST_APP_SCRIPT").unwrap();
        let code = run(
            Path::new(&control),
            &["sh".into(), "-c".into(), script],
            || {},
        )
        .unwrap();
        std::process::exit(code);
    }

    // A launcher exiting must not release its group while children remain.
    // Exercise real signals/socket loss: compiler checks cannot catch orphans.
    #[test]
    fn descendants_die_on_display_loss_or_helper_death_signal() {
        for reason in ["display_loss", "helper_death", "supervisor_crash"] {
            let directory = tempfile::tempdir().unwrap();
            let control = directory.path().join("control");
            let listener = UnixListener::bind(&control).unwrap();
            let pid_file = directory.path().join("pid");
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--ignored",
                    "--exact",
                    "computer_use::linux::private::supervisor::tests::supervisor_entry",
                    "--nocapture",
                ])
                .env("BORG_TEST_APP_CONTROL", &control)
                .env(
                    "BORG_TEST_APP_SCRIPT",
                    format!("sleep 60 & echo $! > {}; exit 0", pid_file.display()),
                );
            configure(&mut command);
            let mut supervisor = super::super::PrivateApp::new(command.spawn().unwrap(), false);
            let (connection, _) = listener.accept().unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            let pid = loop {
                if let Ok(pid) = std::fs::read_to_string(&pid_file)
                    .and_then(|text| text.trim().parse::<u32>().map_err(std::io::Error::other))
                {
                    break pid;
                }
                assert!(Instant::now() < deadline);
                std::thread::sleep(Duration::from_millis(10));
            };
            assert!(
                supervisor.try_wait().unwrap().is_none(),
                "launcher descendants still need supervision"
            );
            if reason == "display_loss" {
                drop(connection);
            } else {
                unsafe {
                    libc::kill(
                        supervisor.id() as i32,
                        if reason == "supervisor_crash" {
                            libc::SIGKILL
                        } else {
                            libc::SIGTERM
                        },
                    );
                }
            }
            let deadline = Instant::now() + Duration::from_secs(5);
            while supervisor.try_wait().unwrap().is_none() {
                assert!(Instant::now() < deadline);
                std::thread::sleep(Duration::from_millis(10));
            }
            // SIGKILL delivery to a different process is asynchronous.
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"));
                if stat.is_err() || stat.unwrap().split_once(") ").unwrap().1.starts_with('Z') {
                    break;
                }
                assert!(Instant::now() < deadline, "orphaned live app descendant");
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}
