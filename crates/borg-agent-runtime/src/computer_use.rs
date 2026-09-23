//! Session-owned native desktop helper. Handles never survive a helper restart.
use std::collections::HashMap;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;

const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Default)]
pub(crate) struct ComputerUse {
    process: Mutex<Option<DesktopProcess>>,
    /// Role and name of every element from the latest observation per window,
    /// so consequential targets can be gated before the helper acts.
    observed: Mutex<HashMap<String, HashMap<String, ObservedElement>>>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ObservedElement {
    role: String,
    name: String,
}

/// Words that mark an on-screen control as consequential: acting on it can
/// send, spend, destroy, publish, or change security state. Matching is on
/// whole words of the observed name, case-insensitively.
const CONSEQUENTIAL_WORDS: &[&str] = &[
    "send",
    "submit",
    "post",
    "publish",
    "share",
    "reply",
    "tweet",
    "buy",
    "purchase",
    "pay",
    "checkout",
    "order",
    "subscribe",
    "donate",
    "transfer",
    "withdraw",
    "delete",
    "remove",
    "erase",
    "discard",
    "uninstall",
    "format",
    "reset",
    "wipe",
    "empty",
    "confirm",
    "agree",
    "accept",
    "apply",
    "install",
    "sign",
    "authorize",
    "approve",
    "grant",
    "allow",
    "revoke",
    "permission",
    "permissions",
    "privacy",
    "security",
    "password",
    "passcode",
    "pin",
    "unlock",
    "login",
    "logout",
    "card",
    "cvv",
    "iban",
];

/// Sub-agents drive only a private display: refuse anything that would read
/// or act on the user's desktop (desktop windows, desktop screenshots, or
/// input that reaches the user's seat). Private window ids start with `pd:`.
pub(crate) fn ensure_private_display_only(arguments: &Value) -> Result<()> {
    let op = arguments
        .get("op")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let private_display = arguments.get("display").and_then(Value::as_str) == Some("private");
    let private_window = arguments
        .get("window_id")
        .and_then(Value::as_str)
        .is_some_and(|id| id.starts_with("pd:"));
    ensure!(
        arguments
            .get("display")
            .is_none_or(|display| display == "private"),
        "sub-agents may only use their private display (display=\"private\"); the user's desktop is reserved for the top-level session"
    );
    let allowed = match op {
        "capabilities" | "start_display" | "stop_display" | "attach_display" | "launch" => true,
        "list_windows" => private_display,
        "screenshot" => private_display || private_window,
        _ => private_window,
    };
    ensure!(
        allowed,
        "sub-agents may only use their private display: {op} needs display=\"private\" or a pd: window_id from list_windows with display=private; launch the app there first"
    );
    Ok(())
}

/// Whether an effect on this element needs explicit human confirmation.
pub(crate) fn action_is_consequential(op: &str, element: &ObservedElement) -> bool {
    if !matches!(op, "click" | "set_value" | "pointer_click" | "type_text") {
        return false;
    }
    let lowered = element.name.to_ascii_lowercase();
    lowered
        .split(|c: char| !c.is_alphanumeric())
        .any(|word| CONSEQUENTIAL_WORDS.contains(&word))
}

fn element_from_node(node: &Value) -> Option<(String, ObservedElement)> {
    let id = node.get("id")?.as_str()?.to_string();
    let element = ObservedElement {
        role: node
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        name: node
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
    };
    Some((id, element))
}

struct DesktopProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl ComputerUse {
    /// Close the helper's stdin so it tears down any private display and the
    /// apps launched into it, then kill it if it does not exit promptly.
    pub(crate) async fn stop(&self) {
        let Some(DesktopProcess {
            mut child, stdin, ..
        }) = self.process.lock().await.take()
        else {
            return;
        };
        drop(stdin);
        if tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .is_err()
        {
            let _ = child.kill().await;
        }
    }

    pub(crate) async fn call(&self, arguments: Value) -> Result<Value> {
        let op = arguments
            .get("op")
            .and_then(Value::as_str)
            .context("op is required")?;
        if !matches!(std::env::consts::OS, "linux" | "macos" | "windows") {
            if op == "capabilities" {
                return Ok(json!({"platform": std::env::consts::OS, "available": false,
                    "reason": "A native Borg computer-use driver is not implemented for this platform yet."}));
            }
            bail!("computer use is not available on this platform; query capabilities");
        }
        ensure!(
            matches!(
                op,
                "capabilities"
                    | "list_windows"
                    | "observe"
                    | "screenshot"
                    | "click"
                    | "set_value"
                    | "type_text"
                    | "key"
                    | "pointer_click"
                    | "pointer_move"
                    | "scroll"
                    | "drag"
                    | "start_display"
                    | "stop_display"
                    | "attach_display"
                    | "launch"
            ),
            "unsupported computer-use operation `{op}`"
        );
        self.gate_consequential_action(op, &arguments).await?;
        let mut request = serde_json::to_vec(&arguments)?;
        ensure!(
            request.len() <= 128 * 1024,
            "computer-use request exceeds 128 KiB"
        );
        request.push(b"\n"[0]);
        let mut slot = self.process.lock().await;
        // Keep the child out of the registry during I/O: cancellation, timeout,
        // malformed output and broken pipes all drop/kill it, invalidating handles.
        let mut process = match slot.take() {
            Some(process) => process,
            None => {
                let mut child = helper_command()
                    .await?
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::null())
                    .kill_on_drop(true)
                    .spawn()
                    .context(HELPER_REQUIREMENTS)?;
                DesktopProcess {
                    stdin: child
                        .stdin
                        .take()
                        .context("desktop helper stdin unavailable")?,
                    stdout: BufReader::new(
                        child
                            .stdout
                            .take()
                            .context("desktop helper stdout unavailable")?,
                    ),
                    child,
                }
            }
        };
        // Starting a private display, launching into it and tearing it down
        // wait on child processes; everything else is a bounded desktop call.
        let limit = match op {
            "start_display" | "launch" | "stop_display" | "attach_display" => {
                Duration::from_secs(45)
            }
            _ => Duration::from_secs(15),
        };
        let response: Value = tokio::time::timeout(limit, async {
            process.stdin.write_all(&request).await?;
            process.stdin.flush().await?;
            let mut line = Vec::new();
            (&mut process.stdout)
                .take(MAX_RESPONSE_BYTES as u64 + 1)
                .read_until(b"\n"[0], &mut line)
                .await?;
            ensure!(
                !line.is_empty(),
                "desktop helper exited; {HELPER_REQUIREMENTS}"
            );
            ensure!(
                line.len() <= MAX_RESPONSE_BYTES,
                "desktop helper response exceeds 8 MiB"
            );
            Ok::<Value, anyhow::Error>(serde_json::from_slice(&line)?)
        })
        .await
        .context(
            "desktop helper timed out; action outcome is unknown, observe before retrying",
        )??;
        ensure!(
            response.get("ok").is_some_and(Value::is_boolean),
            "invalid desktop helper response"
        );
        *slot = Some(process);
        ensure!(
            response["ok"] == true,
            "{}",
            response["error"]
                .as_str()
                .unwrap_or("desktop operation failed")
        );
        self.remember_observation(&response["result"]).await;
        Ok(response["result"].clone())
    }

    /// Refuse to act on a consequential element unless the caller states that
    /// the human already confirmed this specific action (`confirmed: true`).
    async fn gate_consequential_action(&self, op: &str, arguments: &Value) -> Result<()> {
        if !matches!(op, "click" | "set_value" | "pointer_click") {
            return Ok(());
        }
        let window_id = arguments
            .get("window_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let element_id = arguments
            .get("element_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let observed = self.observed.lock().await;
        let Some(element) = observed
            .get(window_id)
            .and_then(|nodes| nodes.get(element_id))
        else {
            return Ok(());
        };
        if action_is_consequential(op, element)
            && arguments.get("confirmed") != Some(&Value::Bool(true))
        {
            bail!(
                "{op} on {} \"{}\" is consequential (it can send, spend, destroy, publish, or change security state); ask the human to confirm this exact action, then repeat the call with confirmed: true",
                element.role,
                element.name
            );
        }
        Ok(())
    }

    async fn remember_observation(&self, result: &Value) {
        let Some(window_id) = result.get("window_id").and_then(Value::as_str) else {
            return;
        };
        let mut observed = self.observed.lock().await;
        if let Some(nodes) = result.get("nodes").and_then(Value::as_array) {
            observed.insert(
                window_id.to_string(),
                nodes.iter().filter_map(element_from_node).collect(),
            );
            return;
        }
        let entry = observed.entry(window_id.to_string()).or_default();
        if let Some(changed) = result.get("changed").and_then(Value::as_array) {
            entry.extend(changed.iter().filter_map(element_from_node));
        }
        if let Some(removed) = result.get("removed").and_then(Value::as_array) {
            for id in removed.iter().filter_map(Value::as_str) {
                entry.remove(id);
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn seed_observation(&self, window_id: &str, nodes: Value) {
        self.remember_observation(&json!({"window_id": window_id, "nodes": nodes}))
            .await;
    }
}

const HELPER_REQUIREMENTS: &str = if cfg!(target_os = "macos") {
    "macOS computer use requires the Xcode Command Line Tools (xcode-select --install) plus Accessibility and Screen Recording permission for the terminal running Borg"
} else if cfg!(target_os = "windows") {
    "Windows computer use requires Windows PowerShell 5.1+ (or pwsh) in the interactive user session"
} else {
    "Linux computer use requires python3, PyGObject and AT-SPI2 on the desktop session bus; desktop input injection also needs python-evdev, a writable /dev/uinput and wtype (Wayland) or xdotool (X11); the private display needs the borg-display binary"
};

/// Platform helper process. Linux runs the AT-SPI worker under the system
/// Python; macOS compiles the Swift accessibility worker once per source
/// revision and caches the binary under `~/.borg/state/computer-use`.
async fn helper_command() -> Result<Command> {
    if cfg!(target_os = "macos") {
        let binary = macos_helper_binary().await?;
        return Ok(Command::new(binary));
    }
    if cfg!(target_os = "windows") {
        let script = cached_helper_source("windows", "ps1", WINDOWS_HELPER_SOURCE).await?;
        let shell = if which_in_path("pwsh") {
            "pwsh"
        } else {
            "powershell"
        };
        let mut command = Command::new(shell);
        command
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
            ])
            .arg(script);
        return Ok(command);
    }
    let mut command = Command::new("python3");
    command.args(["-I", "-u", "-c", LINUX_HELPER_SOURCE]);
    // Release archives ship the private-display compositor next to borg.
    if let Some(display) = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|dir| dir.join("borg-display")))
        .filter(|path| path.is_file())
    {
        command.env("BORG_DISPLAY_BIN", display);
    }
    Ok(command)
}

const MACOS_HELPER_SOURCE: &str = include_str!("computer_use/macos.swift");
/// Compositor window parsing/mapping is a separate AT-SPI-free module so it can
/// be unit tested; it runs as the prologue of the Linux worker.
const LINUX_HELPER_SOURCE: &str = concat!(
    include_str!("computer_use/linux_windows.py"),
    "\n",
    include_str!("computer_use/linux.py")
);
const WINDOWS_HELPER_SOURCE: &str = include_str!("computer_use/windows.ps1");

fn which_in_path(program: &str) -> bool {
    let Some(paths) = std::env::var_os("PATH") else {
        return false;
    };
    let names: Vec<String> = if cfg!(windows) {
        vec![format!("{program}.exe"), program.to_string()]
    } else {
        vec![program.to_string()]
    };
    std::env::split_paths(&paths).any(|dir| names.iter().any(|name| dir.join(name).is_file()))
}

fn helper_cache_dir() -> Result<std::path::PathBuf> {
    Ok(dirs::home_dir()
        .context("home directory unavailable for the computer-use helper cache")?
        .join(".borg")
        .join("state")
        .join("computer-use"))
}

/// Write an embedded helper source to the cache under a content hash so the
/// file on disk always matches the running binary's revision.
async fn cached_helper_source(
    platform: &str,
    extension: &str,
    source: &str,
) -> Result<std::path::PathBuf> {
    use sha2::{Digest, Sha256};
    let digest = hex::encode(Sha256::digest(source.as_bytes()));
    let cache = helper_cache_dir()?;
    let path = cache.join(format!("{platform}-{}.{extension}", &digest[..16]));
    if tokio::fs::metadata(&path).await.is_err() {
        tokio::fs::create_dir_all(&cache).await?;
        let staging = cache.join(format!(
            "{platform}-{}.{}.tmp",
            &digest[..16],
            std::process::id()
        ));
        tokio::fs::write(&staging, source).await?;
        tokio::fs::rename(&staging, &path).await?;
    }
    Ok(path)
}

async fn macos_helper_binary() -> Result<std::path::PathBuf> {
    use sha2::{Digest, Sha256};
    let digest = hex::encode(Sha256::digest(MACOS_HELPER_SOURCE.as_bytes()));
    let cache = helper_cache_dir()?;
    let binary = cache.join(format!("macos-{}", &digest[..16]));
    if tokio::fs::metadata(&binary).await.is_ok() {
        return Ok(binary);
    }
    let source = cached_helper_source("macos", "swift", MACOS_HELPER_SOURCE).await?;
    let staging = cache.join(format!(
        "macos-{}.{}.tmp",
        &digest[..16],
        std::process::id()
    ));
    let output = tokio::time::timeout(
        Duration::from_secs(300),
        Command::new("swiftc")
            .args(["-O", "-o"])
            .arg(&staging)
            .arg(&source)
            .stdin(Stdio::null())
            .output(),
    )
    .await
    .context("compiling the macOS computer-use helper timed out")?
    .context(
        "swiftc is unavailable; install the Xcode Command Line Tools (xcode-select --install)",
    )?;
    ensure!(
        output.status.success(),
        "compiling the macOS computer-use helper failed: {}",
        String::from_utf8_lossy(&output.stderr)
            .chars()
            .take(4096)
            .collect::<String>()
    );
    // Rename last so a concurrent session never executes a partial binary.
    tokio::fs::rename(&staging, &binary).await?;
    Ok(binary)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn element(role: &str, name: &str) -> ObservedElement {
        ObservedElement {
            role: role.into(),
            name: name.into(),
        }
    }

    #[test]
    fn consequential_names_match_whole_words_only() {
        assert!(action_is_consequential(
            "click",
            &element("push button", "Send")
        ));
        assert!(action_is_consequential(
            "click",
            &element("AXButton", "Delete all messages")
        ));
        assert!(action_is_consequential(
            "set_value",
            &element("text", "Card number")
        ));
        assert!(action_is_consequential(
            "click",
            &element("Button", "sign-in")
        ));
        assert!(!action_is_consequential(
            "click",
            &element("push button", "Sender options")
        ));
        assert!(!action_is_consequential(
            "click",
            &element("push button", "Verify click")
        ));
        assert!(!action_is_consequential(
            "observe",
            &element("push button", "Send")
        ));
        assert!(action_is_consequential(
            "pointer_click",
            &element("AXButton", "Pay now")
        ));
    }

    #[test]
    fn sub_agents_are_confined_to_the_private_display() {
        for allowed in [
            json!({"op": "capabilities"}),
            json!({"op": "launch", "argv": ["vkcube"]}),
            json!({"op": "attach_display", "display_id": "0123456789abcdef"}),
            json!({"op": "list_windows", "display": "private"}),
            json!({"op": "screenshot", "display": "private", "scope": "desktop"}),
            json!({"op": "screenshot", "scope": "window", "window_id": "pd:2"}),
            json!({"op": "observe", "window_id": "pd:2"}),
            json!({"op": "pointer_click", "window_id": "pd:2", "x": 1, "y": 1}),
            json!({"op": "key", "window_id": "pd:2", "keys": "ctrl+s"}),
        ] {
            ensure_private_display_only(&allowed)
                .unwrap_or_else(|error| panic!("{allowed}: {error}"));
        }
        for refused in [
            json!({"op": "list_windows"}),
            json!({"op": "list_windows", "display": "desktop"}),
            json!({"op": "screenshot", "scope": "desktop"}),
            json!({"op": "screenshot", "scope": "window", "window_id": "niri:17"}),
            json!({"op": "observe", "window_id": "a1b2:3"}),
            json!({"op": "type_text", "window_id": "niri:17", "text": "x"}),
            json!({"op": "pointer_click", "x": 10, "y": 10}),
            json!({"op": "click", "window_id": "pd:2", "display": "desktop"}),
        ] {
            assert!(
                ensure_private_display_only(&refused).is_err(),
                "{refused} must be refused for a sub-agent"
            );
        }
    }

    /// Compositor window parsing and window-to-desktop mapping decide where
    /// injected clicks land, so a regression would click the wrong pixels.
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_window_backends_parse_and_map_coordinates() {
        let source = concat!(
            include_str!("computer_use/linux_windows.py"),
            "\n",
            include_str!("computer_use/linux_windows_test.py")
        );
        let Ok(output) = std::process::Command::new("python3")
            .args(["-I", "-c", source])
            .output()
        else {
            eprintln!("python3 unavailable; skipping the Linux helper unit tests");
            return;
        };
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[tokio::test]
    async fn consequential_click_is_refused_before_any_helper_runs() {
        let desktop = ComputerUse::default();
        desktop
            .seed_observation(
                "w1",
                json!([
                    {"id": "e1", "role": "push button", "name": "Send"},
                    {"id": "e2", "role": "push button", "name": "Verify click"}
                ]),
            )
            .await;
        let error = desktop
            .call(json!({"op": "click", "window_id": "w1", "element_id": "e1", "observation_id": "o"}))
            .await
            .expect_err("consequential click must be refused");
        assert!(error.to_string().contains("confirmed: true"), "{error}");
        assert!(
            desktop.process.lock().await.is_none(),
            "no helper may be spawned"
        );
        // Element-targeted pointer clicks are gated by the same observed name;
        // a raw coordinate click has no element to gate on.
        let error = desktop
            .call(json!({"op": "pointer_click", "window_id": "w1", "element_id": "e1", "observation_id": "o"}))
            .await
            .expect_err("consequential pointer click must be refused");
        assert!(
            error
                .to_string()
                .contains("pointer_click on push button \"Send\""),
            "{error}"
        );
        assert!(desktop.process.lock().await.is_none());
        // Diffs keep the gate current: a removed element no longer gates.
        desktop
            .remember_observation(&json!({"window_id": "w1", "changed": [], "removed": ["e1"]}))
            .await;
        assert!(!desktop.observed.lock().await["w1"].contains_key("e1"));
    }
}
