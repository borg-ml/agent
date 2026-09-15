use tracing::warn;

#[cfg(any(target_os = "linux", target_os = "windows"))]
const INHIBITION_REASON: &str = "Borg is running active work";

trait SleepGuard {
    fn is_alive(&mut self) -> bool;
}

/// What the lid-close protection can do right now. Surfaced so the UI can
/// explain why the lid is or is not covered instead of failing silently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LidSleepStatus {
    /// This platform has no separate lid override (Linux systemd handles the
    /// lid inside the idle inhibition; Windows exposes none).
    #[cfg_attr(target_os = "macos", allow(dead_code))]
    Unsupported,
    /// A desktop Mac: no lid, so there is nothing to protect against and the
    /// one-time authorization is never requested.
    #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
    NoLid,
    /// macOS needs a one-time admin authorization that has not been granted.
    NeedsAuthorization,
    /// Authorized, but the machine is on battery so the override stays off.
    #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
    OnBattery,
    /// Authorized and eligible: the lid override engages while work runs.
    #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
    Ready,
}

/// Keeps the machine awake while Borg has work in flight and the user has left
/// the setting enabled. The root turn and the subagents are tracked separately
/// because the root routinely returns to `Ready` while spawned children keep
/// running, and releasing then lets the host idle-sleep out from under them.
/// On systemd Linux this also asks logind not to handle a lid switch while
/// that work runs. On macOS, lid-close sleep can only be vetoed by root, so
/// `lid` engages a `pmset disablesleep` override once the user has granted a
/// one-time admin authorization (see [`authorize_lid_sleep`]); until then the
/// idle-only `caffeinate` guard is used.
pub(crate) struct SleepInhibitor {
    enabled: bool,
    lid: bool,
    turn_active: bool,
    children_active: bool,
    guard: Option<Box<dyn SleepGuard>>,
    guard_covers_lid: bool,
    unavailable_logged: bool,
    #[cfg(target_os = "macos")]
    lid_policy: macos::LidPolicy,
}

impl SleepInhibitor {
    pub(crate) fn new(enabled: bool, lid: bool) -> Self {
        #[cfg(target_os = "macos")]
        let lid_policy = macos::LidPolicy::new();
        #[cfg(target_os = "macos")]
        if enabled && lid {
            lid_policy.clear_stale_override();
        }
        Self {
            enabled,
            lid,
            turn_active: false,
            children_active: false,
            guard: None,
            guard_covers_lid: false,
            unavailable_logged: false,
            #[cfg(target_os = "macos")]
            lid_policy,
        }
    }

    pub(crate) fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
        self.reconcile();
    }

    pub(crate) fn set_lid_enabled(&mut self, lid: bool) {
        self.lid = lid;
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
    /// On macOS this also lets the lid override follow AC/battery changes and
    /// a freshly granted authorization.
    pub(crate) fn refresh(&mut self) {
        self.reconcile();
    }

    /// Forget cached authorization state, e.g. right after the user completed
    /// the admin prompt, so the next reconcile re-evaluates it.
    pub(crate) fn invalidate_lid_authorization(&mut self) {
        #[cfg(target_os = "macos")]
        self.lid_policy.invalidate();
        self.reconcile();
    }

    pub(crate) fn lid_status(&mut self) -> LidSleepStatus {
        #[cfg(target_os = "macos")]
        {
            self.lid_policy.status()
        }
        #[cfg(not(target_os = "macos"))]
        {
            LidSleepStatus::Unsupported
        }
    }

    /// Whether the guard should include the lid override on this reconcile.
    fn lid_wanted(&mut self) -> bool {
        if !self.lid {
            return false;
        }
        #[cfg(target_os = "macos")]
        {
            self.lid_policy.status() == LidSleepStatus::Ready
        }
        #[cfg(not(target_os = "macos"))]
        {
            false
        }
    }

    fn reconcile(&mut self) {
        if !self.enabled || !(self.turn_active || self.children_active) {
            self.release();
            return;
        }

        let want_lid = self.lid_wanted();
        if self.guard_covers_lid == want_lid
            && self.guard.as_mut().is_some_and(|guard| guard.is_alive())
        {
            return;
        }
        self.release();

        if let Some(guard) = acquire(want_lid) {
            self.guard = Some(guard);
            self.guard_covers_lid = want_lid;
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
        self.guard_covers_lid = false;
    }
}

/// Run the one-time macOS admin authorization (Touch ID or password via the
/// system dialog) that installs a narrow sudoers rule allowing only
/// `pmset -a disablesleep 0|1` without a password. Blocking; call it off the
/// UI thread. Returns a user-facing error string on failure or cancellation.
pub(crate) fn authorize_lid_sleep() -> Result<(), String> {
    #[cfg(target_os = "macos")]
    {
        macos::authorize()
    }
    #[cfg(not(target_os = "macos"))]
    {
        Err("lid-close sleep prevention only needs authorization on macOS".to_string())
    }
}

fn acquire(lid: bool) -> Option<Box<dyn SleepGuard>> {
    #[cfg(target_os = "linux")]
    {
        let _ = lid;
        linux::acquire()
    }
    #[cfg(target_os = "macos")]
    {
        macos::acquire(lid)
    }
    #[cfg(target_os = "windows")]
    {
        let _ = lid;
        windows::acquire()
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        let _ = lid;
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
    use super::{ChildGuard, LidSleepStatus, SleepGuard};
    use std::os::unix::process::CommandExt;
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};
    use tracing::warn;

    const PMSET: &str = "/usr/bin/pmset";
    const SUDOERS_FILE: &str = "/etc/sudoers.d/borg-lid-sleep";
    /// Marker embedded as `$0` of the watcher shell so a later Borg launch
    /// can tell whether some other live instance still owns the override.
    const WATCHER_MARKER: &str = "borg-lid-sleep-watcher";
    /// How often the cached authorization / power-source probes are re-run.
    const PROBE_INTERVAL: Duration = Duration::from_secs(10);
    const AUTH_PROMPT: &str = "Borg wants permission to keep this Mac awake while it is working with the lid closed. \
         This installs a rule that lets Borg run only `pmset disablesleep` without a password.";

    pub(super) fn acquire(lid: bool) -> Option<Box<dyn SleepGuard>> {
        let caffeinate = spawn_caffeinate()?;
        if !lid {
            return Some(Box::new(caffeinate));
        }
        match LidGuard::new(caffeinate) {
            Ok(guard) => Some(Box::new(guard)),
            Err(error) => {
                warn!(%error, "could not engage the lid-close sleep override; using idle-only prevention");
                spawn_caffeinate().map(|guard| Box::new(guard) as Box<dyn SleepGuard>)
            }
        }
    }

    fn spawn_caffeinate() -> Option<ChildGuard> {
        let pid = std::process::id().to_string();
        Command::new("caffeinate")
            .args(["-i", "-w", pid.as_str()])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .ok()
            .map(ChildGuard)
    }

    /// `pmset -a disablesleep N` through the passwordless sudoers rule.
    fn set_disable_sleep(on: bool) -> Result<(), String> {
        let status = Command::new("/usr/bin/sudo")
            .args([
                "-n",
                PMSET,
                "-a",
                "disablesleep",
                if on { "1" } else { "0" },
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|error| format!("could not run sudo: {error}"))?;
        if status.success() {
            Ok(())
        } else {
            Err(format!(
                "`sudo -n pmset -a disablesleep` exited with {status}"
            ))
        }
    }

    /// True when `pmset -g` reports the override currently active. The
    /// `SleepDisabled` line is only printed while it is set, so a missing
    /// line means off; `None` only when `pmset` itself could not be run.
    fn override_active() -> Option<bool> {
        let output = Command::new(PMSET).arg("-g").output().ok()?;
        let text = String::from_utf8_lossy(&output.stdout);
        Some(text.lines().any(|line| {
            let mut parts = line.split_whitespace();
            parts.next() == Some("SleepDisabled") && parts.next() == Some("1")
        }))
    }

    /// Laptops list an `InternalBattery` entry; desktops do not.
    fn has_internal_battery() -> bool {
        Command::new(PMSET)
            .args(["-g", "batt"])
            .output()
            .map(|output| String::from_utf8_lossy(&output.stdout).contains("InternalBattery"))
            .unwrap_or(false)
    }

    fn on_ac_power() -> bool {
        Command::new(PMSET)
            .args(["-g", "batt"])
            .output()
            .map(|output| String::from_utf8_lossy(&output.stdout).contains("AC Power"))
            // If the probe fails, assume a desktop-class machine rather than
            // silently dropping the feature.
            .unwrap_or(true)
    }

    /// `sudo -n -l <command>` exits 0 only if the rule allows it without a
    /// password, which is exactly the state the authorization installs.
    fn authorized() -> bool {
        Command::new("/usr/bin/sudo")
            .args(["-n", "-l", PMSET, "-a", "disablesleep", "1"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }

    fn another_watcher_running() -> bool {
        Command::new("/usr/bin/pgrep")
            .args(["-f", WATCHER_MARKER])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }

    /// Cached probes so the once-a-second refresh does not fork `sudo` and
    /// `pmset` every tick.
    pub(super) struct LidPolicy {
        probed_at: Option<Instant>,
        /// Fixed for the lifetime of the process; probed once.
        laptop: bool,
        authorized: bool,
        on_ac: bool,
    }

    impl LidPolicy {
        pub(super) fn new() -> Self {
            Self {
                probed_at: None,
                laptop: has_internal_battery(),
                authorized: false,
                on_ac: true,
            }
        }

        pub(super) fn invalidate(&mut self) {
            self.probed_at = None;
        }

        pub(super) fn status(&mut self) -> LidSleepStatus {
            if !self.laptop {
                return LidSleepStatus::NoLid;
            }
            if self
                .probed_at
                .is_none_or(|probed_at| probed_at.elapsed() >= PROBE_INTERVAL)
            {
                self.authorized = authorized();
                self.on_ac = self.authorized && on_ac_power();
                self.probed_at = Some(Instant::now());
            }
            if !self.authorized {
                LidSleepStatus::NeedsAuthorization
            } else if !self.on_ac {
                LidSleepStatus::OnBattery
            } else {
                LidSleepStatus::Ready
            }
        }

        /// `disablesleep` persists across reboots, so a kernel panic or power
        /// loss mid-turn would otherwise leave the Mac unable to sleep for
        /// good. Reset it at launch unless a live watcher from another Borg
        /// instance still legitimately owns it.
        pub(super) fn clear_stale_override(&self) {
            if !authorized() || override_active() != Some(true) || another_watcher_running() {
                return;
            }
            warn!("clearing a stale lid-close sleep override left by a previous run");
            if let Err(error) = set_disable_sleep(false) {
                warn!(%error, "could not clear the stale lid-close sleep override");
            }
        }
    }

    /// Idle guard plus the root-level lid override. The detached watcher is
    /// the crash safety net: it polls for this process to disappear and then
    /// clears the override even if Borg was killed without running `Drop`.
    struct LidGuard {
        caffeinate: ChildGuard,
        watcher: Child,
        last_check: Instant,
    }

    impl LidGuard {
        fn new(caffeinate: ChildGuard) -> Result<Self, String> {
            let pid = std::process::id().to_string();
            let script = format!(
                "while kill -0 \"$1\" 2>/dev/null; do sleep 3; done; \
                 exec /usr/bin/sudo -n {PMSET} -a disablesleep 0"
            );
            let watcher = Command::new("/bin/sh")
                .args(["-c", script.as_str(), WATCHER_MARKER, pid.as_str()])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                // Its own process group so a terminal hang-up that takes Borg
                // down does not take the safety net down with it.
                .process_group(0)
                .spawn()
                .map_err(|error| format!("could not start the lid-sleep watcher: {error}"))?;
            set_disable_sleep(true)?;
            Ok(Self {
                caffeinate,
                watcher,
                last_check: Instant::now(),
            })
        }
    }

    impl SleepGuard for LidGuard {
        fn is_alive(&mut self) -> bool {
            if !self.caffeinate.is_alive() {
                return false;
            }
            match self.watcher.try_wait() {
                Ok(None) => {}
                Ok(Some(status)) => {
                    warn!(?status, "lid-sleep watcher exited unexpectedly");
                    return false;
                }
                Err(error) => {
                    warn!(%error, "could not check the lid-sleep watcher");
                    return false;
                }
            }
            // Another Borg instance finishing its own turn clears the shared
            // override; re-assert it while this instance still has work.
            if self.last_check.elapsed() >= PROBE_INTERVAL {
                self.last_check = Instant::now();
                if override_active() == Some(false)
                    && let Err(error) = set_disable_sleep(true)
                {
                    warn!(%error, "could not re-assert the lid-close sleep override");
                    return false;
                }
            }
            true
        }
    }

    impl Drop for LidGuard {
        fn drop(&mut self) {
            // Release the override first: the watcher is killed with SIGKILL
            // below, so it cannot be relied on for the orderly path.
            if !another_watcher_running_excluding(self.watcher.id())
                && let Err(error) = set_disable_sleep(false)
            {
                warn!(%error, "could not release the lid-close sleep override");
            }
            if let Err(error) = self.watcher.kill()
                && error.kind() != std::io::ErrorKind::InvalidInput
            {
                warn!(%error, "could not stop the lid-sleep watcher");
            }
            let _ = self.watcher.wait();
        }
    }

    /// Like [`another_watcher_running`] but ignoring our own watcher, so the
    /// override stays up when a second Borg instance is still working.
    fn another_watcher_running_excluding(own: u32) -> bool {
        let Ok(output) = Command::new("/usr/bin/pgrep")
            .args(["-f", WATCHER_MARKER])
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
        else {
            return false;
        };
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|line| line.trim().parse::<u32>().ok())
            .any(|pid| pid != own)
    }

    /// Install the sudoers rule through the system authorization dialog.
    pub(super) fn authorize() -> Result<(), String> {
        if authorized() {
            return Ok(());
        }
        let user = std::env::var("USER")
            .ok()
            .filter(|user| !user.is_empty())
            .or_else(|| {
                Command::new("/usr/bin/id")
                    .arg("-un")
                    .output()
                    .ok()
                    .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
            })
            .ok_or_else(|| "could not determine the current user name".to_string())?;
        if user.is_empty()
            || !user
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
        {
            return Err(format!(
                "unsupported user name for a sudoers rule: {user:?}"
            ));
        }
        let rule = format!(
            "{user} ALL=(root) NOPASSWD: {PMSET} -a disablesleep 1, {PMSET} -a disablesleep 0"
        );
        // Validate with visudo before installing so a malformed file can never
        // break sudo for the whole machine.
        let install = format!(
            "set -e; tmp=$(/usr/bin/mktemp); printf '%s\\n' '{rule}' > \"$tmp\"; \
             /usr/sbin/visudo -cf \"$tmp\" >/dev/null; \
             /usr/bin/install -m 0440 -o root -g wheel \"$tmp\" '{SUDOERS_FILE}'; rm -f \"$tmp\""
        );
        let output = Command::new("/usr/bin/osascript")
            .args([
                "-e",
                "on run argv",
                "-e",
                "do shell script (item 1 of argv) with administrator privileges with prompt (item 2 of argv)",
                "-e",
                "end run",
                install.as_str(),
                AUTH_PROMPT,
            ])
            .stdin(Stdio::null())
            .output()
            .map_err(|error| format!("could not launch the authorization dialog: {error}"))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(if stderr.contains("-128") {
                "authorization cancelled".to_string()
            } else {
                format!("authorization failed: {}", stderr.trim())
            });
        }
        if authorized() {
            Ok(())
        } else {
            Err(
                "the rule was installed but sudo still requires a password; check that \
                 /etc/sudoers includes /etc/sudoers.d"
                    .to_string(),
            )
        }
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

    /// Manual end-to-end check on a Mac: pops the system admin dialog, then
    /// engages and releases the real lid override. Run with
    /// `cargo test -p borg sleep_inhibitor -- --ignored --nocapture`.
    #[test]
    #[ignore = "interactive: requires the macOS authorization dialog"]
    #[cfg(target_os = "macos")]
    fn manual_lid_override_roundtrip() {
        super::authorize_lid_sleep().expect("authorization");
        let mut inhibitor = SleepInhibitor::new(true, true);
        let status = inhibitor.lid_status();
        eprintln!("lid status after authorization: {status:?}");
        assert_ne!(status, super::LidSleepStatus::NeedsAuthorization);
        inhibitor.set_turn_active(true);
        assert!(inhibitor.guard.is_some());
        eprintln!(
            "guard covers lid: {} (expected {})",
            inhibitor.guard_covers_lid,
            status == super::LidSleepStatus::Ready
        );
        assert_eq!(
            inhibitor.guard_covers_lid,
            status == super::LidSleepStatus::Ready
        );
        let pm = || {
            std::process::Command::new("/usr/bin/pmset")
                .arg("-g")
                .output()
                .map(|o| {
                    String::from_utf8_lossy(&o.stdout).contains("SleepDisabled\t\t1")
                        || String::from_utf8_lossy(&o.stdout).lines().any(|l| {
                            l.trim_start().starts_with("SleepDisabled")
                                && l.trim_end().ends_with('1')
                        })
                })
                .unwrap_or(false)
        };
        eprintln!("SleepDisabled while working: {}", pm());
        assert_eq!(pm(), inhibitor.guard_covers_lid);
        inhibitor.set_turn_active(false);
        eprintln!("SleepDisabled after release: {}", pm());
        assert!(!pm());
    }

    /// Manual: engage the real lid override regardless of power source and
    /// confirm the orderly release path clears it.
    #[test]
    #[ignore = "mutates the host's pmset state; needs the sudoers rule"]
    #[cfg(target_os = "macos")]
    fn manual_forced_lid_guard_roundtrip() {
        let sleep_disabled = || {
            std::process::Command::new("/usr/bin/pmset")
                .arg("-g")
                .output()
                .map(|o| {
                    String::from_utf8_lossy(&o.stdout).lines().any(|l| {
                        let mut p = l.split_whitespace();
                        p.next() == Some("SleepDisabled") && p.next() == Some("1")
                    })
                })
                .unwrap()
        };
        assert!(!sleep_disabled(), "precondition: override off");
        let guard = super::acquire(true).expect("lid guard");
        eprintln!("SleepDisabled with guard: {}", sleep_disabled());
        assert!(sleep_disabled());
        drop(guard);
        eprintln!("SleepDisabled after drop: {}", sleep_disabled());
        assert!(!sleep_disabled());
    }

    #[test]
    fn disabled_inhibitor_never_acquires_a_backend() {
        let mut inhibitor = SleepInhibitor::new(false, false);
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
        let mut inhibitor = SleepInhibitor::new(true, false);
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
        let mut inhibitor = SleepInhibitor::new(true, false);
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
        let mut inhibitor = SleepInhibitor::new(false, false);
        inhibitor.set_enabled(false);
        inhibitor.set_turn_active(true);
        inhibitor.set_turn_active(true);
        inhibitor.set_enabled(true);
        inhibitor.set_enabled(false);
        assert!(inhibitor.guard.is_none());
    }
}
