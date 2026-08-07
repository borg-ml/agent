//! Session-scoped language runtimes for model-facing programmatic work.
//!
//! The ordinary workflow runner deliberately starts a fresh external process
//! for every invocation.  That is the right default for replayable workflows,
//! but it is not a persistent control environment.  This module provides the
//! smaller, explicit primitive needed by a Prime/RLM-style session: one
//! trusted Python worker can retain its namespace across multiple executions
//! while Borg remains the authority for host effects.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;
use uuid::Uuid;

const MAX_CODE_BYTES: usize = 512 * 1024;
const MAX_RUNTIME_RESULT_BYTES: usize = 1024 * 1024;
const DEFAULT_EXECUTION_TIMEOUT_MS: u64 = 30 * 60 * 1000;
const MAX_EXECUTION_TIMEOUT_MS: u64 = 30 * 60 * 1000;

/// The common host-call boundary exposed to persistent language workers.
///
/// The worker never receives Borg's database, filesystem, provider, or
/// process handles.  It asks the host to perform a named operation and gets a
/// bounded JSON value back instead.
#[async_trait]
pub(crate) trait RuntimeHost: Send + Sync {
    async fn call(&self, operation: &str, arguments: Value) -> Result<Value>;
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PersistentRuntimeResult {
    pub runtime: &'static str,
    pub persistent: bool,
    pub value: Value,
    pub stdout: String,
    pub stderr: String,
}

/// Registry of runtimes owned by one Borg agent executor.
///
/// The registry is deliberately keyed by durable session id.  A native turn
/// creates a short-lived tool facade, but the kernel behind that facade stays
/// alive until the session stops.
#[derive(Clone, Default)]
pub(crate) struct PersistentRuntimeRegistry {
    python: Arc<Mutex<HashMap<Uuid, Arc<PersistentPythonRuntime>>>>,
}

impl PersistentRuntimeRegistry {
    pub(crate) async fn python_for_session(
        &self,
        session_id: Uuid,
        root: &Path,
    ) -> Arc<PersistentPythonRuntime> {
        let mut runtimes = self.python.lock().await;
        runtimes
            .entry(session_id)
            .or_insert_with(|| Arc::new(PersistentPythonRuntime::new(root.to_path_buf())))
            .clone()
    }
}

pub(crate) struct PersistentPythonRuntime {
    root: PathBuf,
    process: Mutex<Option<PythonProcess>>,
}

struct PythonProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl PersistentPythonRuntime {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            process: Mutex::new(None),
        }
    }

    pub(crate) async fn execute(
        &self,
        code: &str,
        timeout_ms: Option<u64>,
        host: Arc<dyn RuntimeHost>,
    ) -> Result<PersistentRuntimeResult> {
        ensure!(!code.trim().is_empty(), "runtime code is empty");
        ensure!(
            code.len() <= MAX_CODE_BYTES,
            "runtime code exceeds {MAX_CODE_BYTES} bytes"
        );
        ensure!(
            self.root.is_dir(),
            "runtime working directory does not exist"
        );
        let timeout = Duration::from_millis(
            timeout_ms
                .unwrap_or(DEFAULT_EXECUTION_TIMEOUT_MS)
                .clamp(1, MAX_EXECUTION_TIMEOUT_MS),
        );

        let mut process = self.process.lock().await;
        if process.is_none() {
            *process = Some(spawn_python_worker(&self.root)?);
        }

        let request_id = Uuid::new_v4().to_string();
        let result = execute_request(
            process.as_mut().expect("persistent Python process exists"),
            &request_id,
            code,
            timeout,
            host,
        )
        .await;
        if result.is_err() {
            if let Some(process) = process.as_mut() {
                let _ = process.child.kill().await;
            }
            *process = None;
        }
        result
    }

    pub(crate) async fn stop(&self) {
        let mut process = self.process.lock().await;
        if let Some(mut process) = process.take() {
            let _ = process.child.kill().await;
            let _ = process.child.wait().await;
        }
    }
}

fn spawn_python_worker(root: &Path) -> Result<PythonProcess> {
    let command = std::env::var("BORG_PYTHON_RUNTIME").unwrap_or_else(|_| {
        if cfg!(windows) {
            "python".to_string()
        } else {
            "python3".to_string()
        }
    });
    let mut child = Command::new(&command)
        .arg("-u")
        .arg("-c")
        .arg(PYTHON_WORKER_SOURCE)
        .current_dir(root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("failed to start persistent Python runtime `{command}`"))?;
    let stdin = child
        .stdin
        .take()
        .context("persistent Python runtime stdin was not piped")?;
    let stdout = child
        .stdout
        .take()
        .context("persistent Python runtime stdout was not piped")?;
    Ok(PythonProcess {
        child,
        stdin,
        stdout: BufReader::new(stdout),
    })
}

async fn execute_request(
    process: &mut PythonProcess,
    request_id: &str,
    code: &str,
    timeout: Duration,
    host: Arc<dyn RuntimeHost>,
) -> Result<PersistentRuntimeResult> {
    write_json_line(
        &mut process.stdin,
        &json!({
            "type": "execute",
            "id": request_id,
            "code": code,
        }),
    )
    .await?;

    let deadline = Instant::now() + timeout;
    loop {
        let line = read_line_until(&mut process.stdout, deadline).await?;
        ensure!(
            line.len() <= MAX_RUNTIME_RESULT_BYTES,
            "persistent runtime message exceeds {MAX_RUNTIME_RESULT_BYTES} bytes"
        );
        let message: Value = serde_json::from_str(&line)
            .with_context(|| "persistent Python runtime returned invalid protocol JSON")?;
        match message.get("type").and_then(Value::as_str) {
            Some("host_call") => {
                let call_id = message
                    .get("id")
                    .and_then(Value::as_str)
                    .context("persistent runtime host call has no id")?;
                let operation = message
                    .get("operation")
                    .and_then(Value::as_str)
                    .context("persistent runtime host call has no operation")?;
                let arguments = message.get("arguments").cloned().unwrap_or(Value::Null);
                let host_message = match host.call(operation, arguments).await {
                    Ok(result) => json!({
                        "type": "host_result",
                        "id": call_id,
                        "ok": true,
                        "result": result,
                    }),
                    Err(error) => json!({
                        "type": "host_result",
                        "id": call_id,
                        "ok": false,
                        "error": format!("{error:#}"),
                    }),
                };
                write_json_line(&mut process.stdin, &host_message).await?;
            }
            Some("result") => {
                ensure!(
                    message.get("id").and_then(Value::as_str) == Some(request_id),
                    "persistent runtime response id did not match request"
                );
                ensure!(
                    message.get("ok").and_then(Value::as_bool).unwrap_or(false),
                    "{}",
                    message
                        .get("error")
                        .and_then(Value::as_str)
                        .unwrap_or("persistent Python execution failed")
                );
                let result = PersistentRuntimeResult {
                    runtime: "python",
                    persistent: true,
                    value: message.get("value").cloned().unwrap_or(Value::Null),
                    stdout: message
                        .get("stdout")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    stderr: message
                        .get("stderr")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                };
                ensure!(
                    serde_json::to_vec(&result)?.len() <= MAX_RUNTIME_RESULT_BYTES,
                    "persistent runtime result exceeds {MAX_RUNTIME_RESULT_BYTES} bytes"
                );
                return Ok(result);
            }
            Some(other) => {
                bail!("persistent Python runtime returned unknown message type `{other}`")
            }
            None => bail!("persistent Python runtime message has no type"),
        }
    }
}

async fn write_json_line(writer: &mut ChildStdin, value: &Value) -> Result<()> {
    let mut line = serde_json::to_vec(value)?;
    line.push(b'\n');
    ensure!(
        line.len() <= MAX_RUNTIME_RESULT_BYTES,
        "persistent runtime request exceeds {MAX_RUNTIME_RESULT_BYTES} bytes"
    );
    writer.write_all(&line).await?;
    writer.flush().await?;
    Ok(())
}

async fn read_line_until(reader: &mut BufReader<ChildStdout>, deadline: Instant) -> Result<String> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    ensure!(
        !remaining.is_zero(),
        "persistent Python execution timed out"
    );
    let mut line = String::new();
    let bytes = tokio::time::timeout(remaining, reader.read_line(&mut line))
        .await
        .context("persistent Python execution timed out")??;
    ensure!(bytes > 0, "persistent Python runtime exited unexpectedly");
    Ok(line.trim_end_matches(['\r', '\n']).to_string())
}

// This worker intentionally has no third-party dependency.  It preserves a
// normal Python namespace across requests and supports top-level await when
// the selected Python environment provides an async callable.  A future
// `ipy` adapter can replace this worker with ipykernel without changing the
// host contract above.
const PYTHON_WORKER_SOURCE: &str = r#"
import ast
import asyncio
import contextlib
import io
import inspect
import json
import sys
import traceback
import uuid

PROTOCOL_OUT = sys.__stdout__
NAMESPACE = {"__name__": "__borg_runtime__"}


def _json_safe(value):
    try:
        json.dumps(value)
        return value
    except Exception:
        return repr(value)


class Borg:
    def call(self, operation, arguments=None):
        call_id = str(uuid.uuid4())
        PROTOCOL_OUT.write(json.dumps({
            "type": "host_call",
            "id": call_id,
            "operation": operation,
            "arguments": {} if arguments is None else arguments,
        }) + "\n")
        PROTOCOL_OUT.flush()
        while True:
            line = sys.stdin.readline()
            if not line:
                raise RuntimeError("Borg host closed the runtime")
            message = json.loads(line)
            if message.get("type") != "host_result" or message.get("id") != call_id:
                continue
            if message.get("ok"):
                return message.get("result")
            raise RuntimeError(message.get("error", "Borg host call failed"))

    def exec(self, command, **kwargs):
        arguments = {"cmd": command}
        arguments.update(kwargs)
        return self.call("exec_command", arguments)

    def read(self, path, **kwargs):
        arguments = {"path": path}
        arguments.update(kwargs)
        return self.call("read_file", arguments)

    def search(self, pattern, **kwargs):
        arguments = {"pattern": pattern}
        arguments.update(kwargs)
        return self.call("search_files", arguments)

    def write(self, path, content, **kwargs):
        arguments = {"path": path, "content": content}
        arguments.update(kwargs)
        return self.call("write_file", arguments)

    def tool(self, name, arguments=None):
        return self.call("borg_tool", {"name": name, "arguments": {} if arguments is None else arguments})


NAMESPACE["borg"] = Borg()


def _run_code(source):
    tree = ast.parse(source, filename="<borg-runtime>", mode="exec")
    stdout = io.StringIO()
    stderr = io.StringIO()
    value = None
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        if tree.body and isinstance(tree.body[-1], ast.Expr):
            prefix = ast.Module(body=tree.body[:-1], type_ignores=[])
            if prefix.body:
                exec(compile(prefix, "<borg-runtime>", "exec"), NAMESPACE, NAMESPACE)
            expression = ast.Expression(tree.body[-1].value)
            compiled = compile(expression, "<borg-runtime>", "eval", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)
            value = eval(compiled, NAMESPACE, NAMESPACE)
        else:
            compiled = compile(tree, "<borg-runtime>", "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)
            value = eval(compiled, NAMESPACE, NAMESPACE)
        if inspect.iscoroutine(value):
            value = asyncio.run(value)
    return _json_safe(value), stdout.getvalue(), stderr.getvalue()


for line in sys.stdin:
    try:
        message = json.loads(line)
        if message.get("type") != "execute":
            continue
        try:
            value, stdout, stderr = _run_code(message.get("code", ""))
            response = {
                "type": "result",
                "id": message.get("id"),
                "ok": True,
                "value": value,
                "stdout": stdout,
                "stderr": stderr,
            }
        except BaseException as error:
            response = {
                "type": "result",
                "id": message.get("id"),
                "ok": False,
                "error": "".join(traceback.format_exception(error)),
            }
        PROTOCOL_OUT.write(json.dumps(response, default=_json_safe) + "\n")
        PROTOCOL_OUT.flush()
    except BaseException as error:
        PROTOCOL_OUT.write(json.dumps({
            "type": "result",
            "id": None,
            "ok": False,
            "error": "".join(traceback.format_exception(error)),
        }) + "\n")
        PROTOCOL_OUT.flush()
"#;

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    struct TestHost;

    #[async_trait]
    impl RuntimeHost for TestHost {
        async fn call(&self, operation: &str, arguments: Value) -> Result<Value> {
            match operation {
                "echo" => Ok(arguments),
                other => bail!("unknown test operation `{other}`"),
            }
        }
    }

    async fn python_available() -> bool {
        tokio::process::Command::new(std::env::var("BORG_PYTHON_RUNTIME").unwrap_or_else(|_| {
            if cfg!(windows) {
                "python".to_string()
            } else {
                "python3".to_string()
            }
        }))
        .arg("--version")
        .output()
        .await
        .is_ok_and(|output| output.status.success())
    }

    #[tokio::test]
    async fn python_namespace_survives_multiple_requests() {
        if !python_available().await {
            return;
        }
        let root = tempdir().expect("temporary runtime root");
        let runtime = PersistentPythonRuntime::new(root.path().to_path_buf());
        let host: Arc<dyn RuntimeHost> = Arc::new(TestHost);
        let first = runtime
            .execute(
                "answer = 40\nanswer = answer + 2\nanswer",
                None,
                Arc::clone(&host),
            )
            .await
            .expect("first Python execution");
        assert_eq!(first.value, json!(42));
        let second = runtime
            .execute("answer += 1\nanswer", None, host)
            .await
            .expect("second Python execution");
        assert_eq!(second.value, json!(43));
        let asynchronous = runtime
            .execute(
                "import asyncio\nasync def bump():\n    await asyncio.sleep(0)\n    return answer + 1\nprint('namespace survives')\nawait bump()",
                None,
                Arc::new(TestHost),
            )
            .await
            .expect("top-level await execution");
        assert_eq!(asynchronous.value, json!(44));
        assert_eq!(asynchronous.stdout, "namespace survives\n");
        runtime.stop().await;
    }

    #[tokio::test]
    async fn python_worker_round_trips_host_calls() {
        if !python_available().await {
            return;
        }
        let root = tempdir().expect("temporary runtime root");
        let runtime = PersistentPythonRuntime::new(root.path().to_path_buf());
        let host: Arc<dyn RuntimeHost> = Arc::new(TestHost);
        let result = runtime
            .execute("borg.call('echo', {'value': 7})", None, host)
            .await
            .expect("host call execution");
        assert_eq!(result.value, json!({"value": 7}));
        runtime.stop().await;
    }

    #[tokio::test]
    async fn registry_reuses_one_runtime_per_session() {
        if !python_available().await {
            return;
        }
        let root = tempdir().expect("temporary runtime root");
        let registry = PersistentRuntimeRegistry::default();
        let session_id = Uuid::new_v4();
        let first = registry.python_for_session(session_id, root.path()).await;
        let second = registry.python_for_session(session_id, root.path()).await;
        assert!(Arc::ptr_eq(&first, &second));
    }
}
