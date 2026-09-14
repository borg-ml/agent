use tracing::warn;

#[cfg(any(target_os = "linux", target_os = "windows"))]
const INHIBITION_REASON: &str = "Borg is running active work";

trait SleepGuard {
    fn is_alive(&mut self) -> bool;
}

/// Keeps the machine awake while Borg has work in flight and the user has left
/// the setting enabled. The root turn and the subagents are tracked separately
/// because the root routinely returns to `Ready` while spawned children keep
/// running, and releasing then lets the host idle-sleep out from under them.
/// On systemd Linux this also asks logind not to handle a lid switch while
/// that work runs; other platforms retain their own lid policy because they do
/// not expose a safe per-process lid override.
pub(crate) struct SleepInhibitor {
    enabled: bool,
    turn_active: bool,
    children_active: bool,
    guard: Option<Box<dyn SleepGuard>>,
    unavailable_logged: bool,
}

impl SleepInhibitor {
    pub(crate) fn new(enabled: bool) -> Self {
        Self {
            enabled,
            turn_active: false,
            children_active: false,
            guard: None,
            unavailable_logged: false,
        }
    }

    pub(crate) fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
        self.reconcile();
    }

    pub(crate) fn set_turn_active(&mut self, turn_active: bool) {
        self.turn_active = turn_active;
        self.reconcile();
    }

    /// Tracked independently of the root turn; see the type-level note.
    pub(crate) fn set_children_active(&mut self, children_active: bool) {
        self.children_active = children_active;
        self.reconcile();
    }

    /// Re-check a live backend and restart it if the helper exited while the
    /// turn was still active. The caller invokes this from its periodic UI
    /// tick, so an unexpected helper exit does not silently lose protection.
    pub(crate) fn refresh(&mut self) {
        self.reconcile();
    }

    fn reconcile(&mut self) {
        if !self.enabled || !(self.turn_active || self.children_active) {
            self.release();
            return;
        }

        if self.guard.as_mut().is_some_and(|guard| guard.is_alive()) {
            return;
        }
        self.release();

        if let Some(guard) = acquire() {
            self.guard = Some(guard);
            self.unavailable_logged = false;
        } else if !self.unavailable_logged {
            warn!(
                "Borg could not find a supported system sleep-prevention backend; continuing without sleep inhibition"
            );
            self.unavailable_logged = true;
        }
    }

    fn release(&mut self) {
        self.guard.take();
    }
}

fn acquire() -> Option<Box<dyn SleepGuard>> {
    #[cfg(target_os = "linux")]
    {
        linux::acquire()
    }
    #[cfg(target_os = "macos")]
    {
        macos::acquire()
    }
    #[cfg(target_os = "windows")]
    {
        windows::acquire()
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        None
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
struct ChildGuard(std::process::Child);

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl SleepGuard for ChildGuard {
    fn is_alive(&mut self) -> bool {
        match self.0.try_wait() {
            Ok(None) => true,
            Ok(Some(status)) => {
                warn!(?status, "sleep-prevention helper exited unexpectedly");
                false
            }
            Err(error) => {
                warn!(%error, "could not check the sleep-prevention helper");
                false
            }
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Err(error) = self.0.kill()
            && error.kind() != std::io::ErrorKind::InvalidInput
        {
            warn!(%error, "could not stop the sleep-prevention helper");
        }
        if let Err(error) = self.0.wait()
            && error.kind() != std::io::ErrorKind::InvalidInput
        {
            warn!(%error, "could not reap the sleep-prevention helper");
        }
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{ChildGuard, INHIBITION_REASON, SleepGuard};
    use std::io;
    use std::os::unix::process::CommandExt;
    use std::process::{Child, Command, Stdio};
    use tracing::warn;

    const BLOCKER_SLEEP_SECONDS: &str = "2147483647";
    const SYSTEMD_INHIBITION_SCOPE: &str = "idle:handle-lid-switch";

    pub(super) fn acquire() -> Option<Box<dyn SleepGuard>> {
        let backends = [
            ("systemd-inhibit", systemd_command()),
            ("gnome-session-inhibit", gnome_command()),
        ];
        for (name, command) in backends {
            match spawn_backend(command) {
                Ok(child) => return Some(Box::new(ChildGuard(child))),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => warn!(backend = name, %error, "sleep-prevention backend failed"),
            }
        }
        None
    }

    fn systemd_command() -> Command {
        let mut command = Command::new("systemd-inhibit");
        command
            .arg(format!("--what={SYSTEMD_INHIBITION_SCOPE}"))
            .args([
                "--mode=block",
                "--who=Borg",
                "--why",
                INHIBITION_REASON,
                "--",
                "sleep",
                BLOCKER_SLEEP_SECONDS,
            ]);
        quiet(&mut command);
        command
    }

    fn gnome_command() -> Command {
        let mut command = Command::new("gnome-session-inhibit");
        // GNOME's helper has no portable lid-switch lock, so this fallback
        // still covers idle sleep without changing the desktop's lid policy.
        command.args([
            "--inhibit",
            "idle",
            "--reason",
            INHIBITION_REASON,
            "sleep",
            BLOCKER_SLEEP_SECONDS,
        ]);
        quiet(&mut command);
        command
    }

    fn quiet(command: &mut Command) {
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
    }

    fn spawn_backend(mut command: Command) -> io::Result<Child> {
        // If Borg is killed without running Drop, do not leave a decades-long
        // inhibitor process behind. This mirrors the parent-death guard used
        // by Codex CLI's sleep inhibitor.
        let parent_pid = unsafe { libc::getpid() };
        // SAFETY: the hook only installs a child-process death signal and
        // checks the parent PID between fork and exec.
        unsafe {
            command.pre_exec(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) == -1 {
                    return Err(io::Error::last_os_error());
                }
                if libc::getppid() != parent_pid {
                    libc::raise(libc::SIGTERM);
                }
                Ok(())
            });
        }

        let mut child = command.spawn()?;
        match child.try_wait()? {
            None => Ok(child),
            Some(status) => Err(io::Error::other(format!(
                "backend exited immediately with {status}"
            ))),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn systemd_inhibits_idle_and_lid_switch_handling() {
            let command = systemd_command();
            let args = command
                .get_args()
                .map(|arg| arg.to_string_lossy().into_owned())
                .collect::<Vec<_>>();

            assert!(args.contains(&format!("--what={SYSTEMD_INHIBITION_SCOPE}")));
            assert!(args.contains(&"--mode=block".to_string()));
            assert!(args.contains(&"--".to_string()));
        }
    }
}

#[cfg(target_os = "macos")]
mod macos {
    use super::{ChildGuard, SleepGuard};
    use std::process::{Command, Stdio};

    pub(super) fn acquire() -> Option<Box<dyn SleepGuard>> {
        let pid = std::process::id().to_string();
        Command::new("caffeinate")
            .args(["-i", "-w", pid.as_str()])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .ok()
            .map(|child| Box::new(ChildGuard(child)) as Box<dyn SleepGuard>)
    }
}

#[cfg(target_os = "windows")]
mod windows {
    use super::{INHIBITION_REASON, SleepGuard};
    use std::ffi::OsStr;
    use std::iter::once;
    use std::os::windows::ffi::OsStrExt;
    use tracing::warn;
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Power::{
        POWER_REQUEST_TYPE, PowerClearRequest, PowerCreateRequest, PowerRequestSystemRequired,
        PowerSetRequest,
    };
    use windows_sys::Win32::System::SystemServices::POWER_REQUEST_CONTEXT_VERSION;
    use windows_sys::Win32::System::Threading::{
        POWER_REQUEST_CONTEXT_SIMPLE_STRING, REASON_CONTEXT, REASON_CONTEXT_0,
    };

    pub(super) fn acquire() -> Option<Box<dyn SleepGuard>> {
        match PowerGuard::new() {
            Ok(guard) => Some(Box::new(guard)),
            Err(error) => {
                warn!(%error, "could not acquire Windows sleep prevention");
                None
            }
        }
    }

    struct PowerGuard {
        handle: HANDLE,
        request_type: POWER_REQUEST_TYPE,
    }

    impl PowerGuard {
        fn new() -> Result<Self, String> {
            let mut reason: Vec<u16> = OsStr::new(INHIBITION_REASON)
                .encode_wide()
                .chain(once(0))
                .collect();
            let context = REASON_CONTEXT {
                Version: POWER_REQUEST_CONTEXT_VERSION,
                Flags: POWER_REQUEST_CONTEXT_SIMPLE_STRING,
                Reason: REASON_CONTEXT_0 {
                    SimpleReasonString: reason.as_mut_ptr(),
                },
            };
            // SAFETY: `context` contains a valid, NUL-terminated UTF-16 reason
            // for the duration of the call.
            let handle = unsafe { PowerCreateRequest(&context) };
            if handle.is_null() || handle == INVALID_HANDLE_VALUE {
                return Err(format!(
                    "PowerCreateRequest failed: {}",
                    std::io::Error::last_os_error()
                ));
            }

            let request_type = PowerRequestSystemRequired;
            // SAFETY: `handle` was returned by `PowerCreateRequest` and the
            // request type is the documented system-required request.
            if unsafe { PowerSetRequest(handle, request_type) } == 0 {
                let error = std::io::Error::last_os_error();
                // SAFETY: the handle is owned on this error path.
                unsafe { CloseHandle(handle) };
                return Err(format!("PowerSetRequest failed: {error}"));
            }

            Ok(Self {
                handle,
                request_type,
            })
        }
    }

    impl SleepGuard for PowerGuard {
        fn is_alive(&mut self) -> bool {
            true
        }
    }

    impl Drop for PowerGuard {
        fn drop(&mut self) {
            // SAFETY: both calls operate on the live handle owned by this guard
            // and are made exactly once before closing it.
            if unsafe { PowerClearRequest(self.handle, self.request_type) } == 0 {
                warn!(
                    error = %std::io::Error::last_os_error(),
                    "could not clear Windows sleep prevention"
                );
            }
            // SAFETY: the handle is owned by this guard.
            if unsafe { CloseHandle(self.handle) } == 0 {
                warn!(
                    error = %std::io::Error::last_os_error(),
                    "could not close Windows sleep-prevention handle"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SleepGuard, SleepInhibitor};

    /// Stands in for a live OS backend so guard lifetime can be asserted
    /// without spawning caffeinate or systemd-inhibit. Reporting itself
    /// alive also keeps `reconcile` on its early-return path, so no test
    /// here can reach the real `acquire`.
    struct FakeGuard;

    impl SleepGuard for FakeGuard {
        fn is_alive(&mut self) -> bool {
            true
        }
    }

    #[test]
    fn disabled_inhibitor_never_acquires_a_backend() {
        let mut inhibitor = SleepInhibitor::new(false);
        inhibitor.set_turn_active(true);
        inhibitor.refresh();
        assert!(inhibitor.guard.is_none());
        inhibitor.set_turn_active(false);
    }

    /// The regression behind stuck subagents after host sleep: the root turn
    /// reported `Ready` while two children were still Running, the inhibitor
    /// released on that transition alone, and the host idle-slept under them.
    #[test]
    fn children_keep_the_guard_after_the_root_turn_ends() {
        let mut inhibitor = SleepInhibitor::new(true);
        inhibitor.turn_active = true;
        inhibitor.guard = Some(Box::new(FakeGuard));

        inhibitor.set_children_active(true);
        assert!(inhibitor.guard.is_some());

        inhibitor.set_turn_active(false);
        assert!(
            inhibitor.guard.is_some(),
            "an idle root released the guard while a subagent was still working"
        );

        // Only the last child leaving a working state releases the host.
        inhibitor.set_children_active(false);
        assert!(inhibitor.guard.is_none());
    }

    #[test]
    fn disabling_the_setting_releases_an_active_child_guard() {
        let mut inhibitor = SleepInhibitor::new(true);
        inhibitor.children_active = true;
        inhibitor.guard = Some(Box::new(FakeGuard));

        inhibitor.set_enabled(false);
        assert!(
            inhibitor.guard.is_none(),
            "the disabled setting must release child-driven inhibition"
        );
    }

    #[test]
    fn setting_and_turn_lifecycle_are_idempotent() {
        let mut inhibitor = SleepInhibitor::new(false);
        inhibitor.set_enabled(false);
        inhibitor.set_turn_active(true);
        inhibitor.set_turn_active(true);
        inhibitor.set_enabled(true);
        inhibitor.set_enabled(false);
        assert!(inhibitor.guard.is_none());
    }
}
