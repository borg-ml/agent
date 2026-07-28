use std::process::{Child, Command, Stdio};

/// Keeps the desktop awake only while Borg has an active turn.
pub(crate) struct SleepInhibitor {
    enabled: bool,
    turn_active: bool,
    child: Option<Child>,
}

impl SleepInhibitor {
    pub(crate) fn new(enabled: bool) -> Self {
        Self {
            enabled,
            turn_active: false,
            child: None,
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

    fn reconcile(&mut self) {
        if self.enabled && self.turn_active {
            if self.child.is_none() {
                self.child = acquire();
            }
        } else {
            self.release();
        }
    }

    fn release(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for SleepInhibitor {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(target_os = "linux")]
fn acquire() -> Option<Child> {
    let quiet = || Stdio::null();
    Command::new("systemd-inhibit")
        .args([
            "--what=idle",
            "--mode=block",
            "--who=Borg",
            "--why=Borg is running an active turn",
            "sleep",
            "4294967295",
        ])
        .stdin(quiet())
        .stdout(quiet())
        .stderr(quiet())
        .spawn()
        .or_else(|_| {
            Command::new("gnome-session-inhibit")
                .args([
                    "--inhibit",
                    "idle",
                    "--reason",
                    "Borg is running an active turn",
                    "sleep",
                    "4294967295",
                ])
                .stdin(quiet())
                .stdout(quiet())
                .stderr(quiet())
                .spawn()
        })
        .ok()
}

#[cfg(target_os = "macos")]
fn acquire() -> Option<Child> {
    Command::new("caffeinate")
        .arg("-i")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn acquire() -> Option<Child> {
    None
}
