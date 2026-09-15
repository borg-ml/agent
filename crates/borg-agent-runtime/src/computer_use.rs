//! Session-owned native desktop helper. Handles never survive a helper restart.
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
}

struct DesktopProcess {
    _child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl ComputerUse {
    pub(crate) async fn stop(&self) {
        self.process.lock().await.take();
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
                "capabilities" | "list_windows" | "observe" | "screenshot" | "click" | "set_value"
            ),
            "unsupported computer-use operation `{op}`"
        );
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
                    _child: child,
                }
            }
        };
        let response: Value = tokio::time::timeout(Duration::from_secs(15), async {
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
        Ok(response["result"].clone())
    }
}

const HELPER_REQUIREMENTS: &str = if cfg!(target_os = "macos") {
    "macOS computer use requires the Xcode Command Line Tools (xcode-select --install) plus Accessibility and Screen Recording permission for the terminal running Borg"
} else if cfg!(target_os = "windows") {
    "Windows computer use requires Windows PowerShell 5.1+ (or pwsh) in the interactive user session"
} else {
    "Linux computer use requires python3, PyGObject and AT-SPI2 on the desktop session bus"
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
    command.args(["-I", "-u", "-c", include_str!("computer_use/linux.py")]);
    Ok(command)
}

const MACOS_HELPER_SOURCE: &str = include_str!("computer_use/macos.swift");
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
