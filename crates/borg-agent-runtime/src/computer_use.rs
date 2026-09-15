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
        if !cfg!(target_os = "linux") {
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
                let mut child = Command::new("python3")
                    .args(["-I", "-u", "-c", include_str!("computer_use/linux.py")])
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::null())
                    .kill_on_drop(true)
                    .spawn()
                    .context("Linux computer use requires python3, PyGObject and AT-SPI2 on the desktop session bus")?;
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
            (&mut process.stdout).take(MAX_RESPONSE_BYTES as u64 + 1)
                .read_until(b"\n"[0], &mut line).await?;
            ensure!(!line.is_empty(), "desktop helper exited; check python3, PyGObject, AT-SPI2 and the desktop session bus");
            ensure!(line.len() <= MAX_RESPONSE_BYTES, "desktop helper response exceeds 8 MiB");
            Ok::<Value, anyhow::Error>(serde_json::from_slice(&line)?)
        }).await.context("desktop helper timed out; action outcome is unknown, observe before retrying")??;
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
