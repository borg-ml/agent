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
use sha2::{Digest, Sha256};
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
    pub recovered_from_manifest: bool,
    pub execution_count: u64,
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
    python: Arc<Mutex<HashMap<Uuid, Arc<PersistentRuntimeWorker>>>>,
    bun: Arc<Mutex<HashMap<Uuid, Arc<PersistentRuntimeWorker>>>>,
}

impl PersistentRuntimeRegistry {
    pub(crate) async fn python_for_session(
        &self,
        session_id: Uuid,
        root: &Path,
        store: Option<crate::SqliteSessionStore>,
    ) -> Arc<PersistentRuntimeWorker> {
        let mut runtimes = self.python.lock().await;
        runtimes
            .entry(session_id)
            .or_insert_with(|| {
                Arc::new(PersistentRuntimeWorker::for_python(
                    session_id,
                    root.to_path_buf(),
                    store,
                ))
            })
            .clone()
    }

    pub(crate) async fn bun_for_session(
        &self,
        session_id: Uuid,
        root: &Path,
        store: Option<crate::SqliteSessionStore>,
    ) -> Arc<PersistentRuntimeWorker> {
        let mut runtimes = self.bun.lock().await;
        runtimes
            .entry(session_id)
            .or_insert_with(|| {
                Arc::new(PersistentRuntimeWorker::for_bun(
                    session_id,
                    root.to_path_buf(),
                    store,
                ))
            })
            .clone()
    }
}

pub(crate) struct PersistentRuntimeWorker {
    session_id: Uuid,
    root: PathBuf,
    store: Option<crate::SqliteSessionStore>,
    worker_id: Uuid,
    runtime: &'static str,
    command: String,
    worker_source: &'static str,
    metadata: Mutex<RuntimeMetadata>,
    process: Mutex<Option<PythonProcess>>,
}

#[derive(Default)]
struct RuntimeMetadata {
    manifest_activated: bool,
    recovered_from_manifest: bool,
    execution_count: u64,
    namespace_recovery_pending: bool,
}

struct PythonProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl PersistentRuntimeWorker {
    #[cfg(test)]
    fn new(root: PathBuf) -> Self {
        Self::for_python(Uuid::nil(), root, None)
    }

    fn for_python(
        session_id: Uuid,
        root: PathBuf,
        store: Option<crate::SqliteSessionStore>,
    ) -> Self {
        Self::with_session(
            session_id,
            root,
            store,
            "python",
            python_command(),
            PYTHON_WORKER_SOURCE,
        )
    }

    fn for_bun(session_id: Uuid, root: PathBuf, store: Option<crate::SqliteSessionStore>) -> Self {
        Self::with_session(
            session_id,
            root,
            store,
            "javascript",
            bun_command(),
            BUN_WORKER_SOURCE,
        )
    }

    fn with_session(
        session_id: Uuid,
        root: PathBuf,
        store: Option<crate::SqliteSessionStore>,
        runtime: &'static str,
        command: String,
        worker_source: &'static str,
    ) -> Self {
        Self {
            session_id,
            root,
            store,
            worker_id: Uuid::new_v4(),
            runtime,
            command,
            worker_source,
            metadata: Mutex::new(RuntimeMetadata::default()),
            process: Mutex::new(None),
        }
    }

    pub(crate) fn worker_id(&self) -> Uuid {
        self.worker_id
    }

    async fn activate_manifest(&self) -> Result<bool> {
        let mut metadata = self.metadata.lock().await;
        if metadata.manifest_activated {
            return Ok(metadata.recovered_from_manifest);
        }
        let recovered = if let Some(store) = &self.store {
            let activation = store
                .activate_runtime_manifest(
                    self.session_id,
                    self.runtime,
                    &self.root.to_string_lossy(),
                    &self.command,
                    self.worker_id,
                )
                .await?;
            metadata.execution_count = activation.manifest.execution_count;
            metadata.namespace_recovery_pending = activation.recovered_from_previous_worker;
            activation.recovered_from_previous_worker
        } else {
            false
        };
        metadata.manifest_activated = true;
        metadata.recovered_from_manifest = recovered;
        Ok(recovered)
    }

    pub(crate) async fn execute(
        &self,
        code: &str,
        timeout_ms: Option<u64>,
        host: Arc<dyn RuntimeHost>,
    ) -> Result<PersistentRuntimeResult> {
        self.execute_as(self.runtime, code, timeout_ms, host).await
    }

    pub(crate) async fn execute_as(
        &self,
        requested_runtime: &'static str,
        code: &str,
        timeout_ms: Option<u64>,
        host: Arc<dyn RuntimeHost>,
    ) -> Result<PersistentRuntimeResult> {
        ensure!(
            (self.runtime == "python" && requested_runtime == "python")
                || (self.runtime == "javascript"
                    && matches!(requested_runtime, "javascript" | "typescript")),
            "runtime `{requested_runtime}` is incompatible with the persistent `{}` worker",
            self.runtime
        );
        ensure!(!code.trim().is_empty(), "runtime code is empty");
        ensure!(
            code.len() <= MAX_CODE_BYTES,
            "runtime code exceeds {MAX_CODE_BYTES} bytes"
        );
        ensure!(
            self.root.is_dir(),
            "runtime working directory does not exist"
        );
        let recovered_from_manifest = self.activate_manifest().await?;
        let timeout = Duration::from_millis(
            timeout_ms
                .unwrap_or(DEFAULT_EXECUTION_TIMEOUT_MS)
                .clamp(1, MAX_EXECUTION_TIMEOUT_MS),
        );

        let result = {
            let mut process = self.process.lock().await;
            let process_exited = if let Some(process) = process.as_mut() {
                process.child.try_wait()?.is_some()
            } else {
                false
            };
            if process_exited {
                *process = None;
            }
            let should_recover_namespace =
                process.is_none() && self.metadata.lock().await.namespace_recovery_pending;
            let checkpoint_state = if should_recover_namespace {
                if let Some(store) = &self.store {
                    store
                        .runtime_checkpoint(self.session_id, None)
                        .await?
                        .map(|checkpoint| checkpoint.state)
                } else {
                    None
                }
            } else {
                None
            };
            if process.is_none() {
                *process = Some(spawn_worker(
                    &self.root,
                    &self.command,
                    self.worker_source,
                    self.runtime,
                )?);
            }

            let request_id = Uuid::new_v4().to_string();
            let result = execute_request(
                process.as_mut().expect("persistent Python process exists"),
                &request_id,
                requested_runtime,
                code,
                timeout,
                checkpoint_state.as_ref(),
                host,
            )
            .await;
            if result.is_err() {
                if let Some(process) = process.as_mut() {
                    let _ = process.child.kill().await;
                }
                *process = None;
            }
            {
                let mut metadata = self.metadata.lock().await;
                if result.is_err() {
                    metadata.namespace_recovery_pending = true;
                } else if should_recover_namespace {
                    metadata.namespace_recovery_pending = false;
                }
            }
            result
        };

        let code_hash = format!("sha256:{:x}", Sha256::digest(code.as_bytes()));
        if let Some(store) = &self.store {
            let execution_error = result.as_ref().err().map(ToString::to_string);
            store
                .record_runtime_execution(
                    self.session_id,
                    self.worker_id,
                    &code_hash,
                    result.is_err(),
                    execution_error.as_deref(),
                )
                .await?;
        }
        let execution_count = {
            let mut metadata = self.metadata.lock().await;
            metadata.execution_count = metadata.execution_count.saturating_add(1);
            metadata.execution_count
        };
        let mut result = result?;
        result.recovered_from_manifest = recovered_from_manifest;
        result.execution_count = execution_count;
        Ok(result)
    }

    pub(crate) async fn stop(&self) {
        let mut process = self.process.lock().await;
        if let Some(mut process) = process.take() {
            let _ = process.child.kill().await;
            let _ = process.child.wait().await;
        }
        drop(process);
        if let Some(store) = &self.store {
            let _ = store
                .stop_runtime_manifest(self.session_id, self.worker_id)
                .await;
        }
    }
}

fn python_command() -> String {
    std::env::var("BORG_PYTHON_RUNTIME").unwrap_or_else(|_| {
        if cfg!(windows) {
            "python".to_string()
        } else {
            "python3".to_string()
        }
    })
}

fn bun_command() -> String {
    std::env::var("BORG_BUN_RUNTIME").unwrap_or_else(|_| "bun".to_string())
}

fn spawn_worker(root: &Path, command: &str, source: &str, runtime: &str) -> Result<PythonProcess> {
    let mut process = Command::new(command);
    // Model-authored code may use the worker namespace and the explicit Borg
    // host-call protocol, but it must not inherit deployment, provider, or
    // control-plane credentials from the supervising process.
    crate::process_environment::configure_runtime_environment(&mut process);
    if matches!(runtime, "javascript" | "typescript") {
        process.arg("--eval").arg(source);
    } else {
        process.arg("-u").arg("-c").arg(source);
    }
    let mut child = process
        .current_dir(root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("failed to start persistent runtime `{command}`"))?;
    let stdin = child
        .stdin
        .take()
        .context("persistent runtime stdin was not piped")?;
    let stdout = child
        .stdout
        .take()
        .context("persistent runtime stdout was not piped")?;
    Ok(PythonProcess {
        child,
        stdin,
        stdout: BufReader::new(stdout),
    })
}

async fn execute_request(
    process: &mut PythonProcess,
    request_id: &str,
    runtime: &'static str,
    code: &str,
    timeout: Duration,
    bootstrap: Option<&Value>,
    host: Arc<dyn RuntimeHost>,
) -> Result<PersistentRuntimeResult> {
    write_json_line(
        &mut process.stdin,
        &json!({
            "type": "execute",
            "id": request_id,
            "runtime": runtime,
            "code": code,
            "bootstrap": bootstrap,
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
            .with_context(|| "persistent runtime returned invalid protocol JSON")?;
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
                        .unwrap_or("persistent runtime execution failed")
                );
                let result = PersistentRuntimeResult {
                    runtime,
                    persistent: true,
                    recovered_from_manifest: false,
                    execution_count: 0,
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
                bail!("persistent runtime returned unknown message type `{other}`")
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


def _restore_namespace(state):
    if not isinstance(state, dict):
        raise RuntimeError("runtime checkpoint namespace state must be an object")
    for name, value in state.items():
        # Checkpoints are data, not executable code. Only ordinary public
        # Python identifiers become namespace bindings; internal/runtime names
        # cannot be shadowed by persisted state.
        if isinstance(name, str) and name.isidentifier() and not name.startswith("_"):
            NAMESPACE[name] = value


class Borg:
    def __init__(self):
        self.rlm = Rlm(self)
        self.harness = Harness(self)

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

    def history(self, text=None, **kwargs):
        arguments = {}
        if text is not None:
            arguments["text"] = text
        arguments.update(kwargs)
        return self.call("history", arguments)

    def history_index(self, after_sequence=0, limit=1000):
        return self.call("history_index", {
            "after_sequence": after_sequence,
            "limit": limit,
        })

    def semantic_search(self, query, **kwargs):
        """Run read-only BorgSearch retrieval through the scoped Web MCP bridge.

        Hits are candidates and must be resolved through the returned source
        contract before they are treated as evidence.
        """
        arguments = {"query": query}
        arguments.update(kwargs)
        return self.call("mcp_call", {
            "name": "mcp__borg__search_documents",
            "arguments": arguments,
        })

    def retrieval_adapter(self, adapter_id, query=None):
        spec = self.call("retrieval_adapter", {"id": adapter_id})
        language = spec.get("manifest", {}).get("language")
        if language != "python":
            raise RuntimeError(
                f"retrieval adapter {adapter_id!r} uses {language!r}; execute it from a matching runtime"
            )
        namespace = {"borg": self, "__name__": f"__borg_retriever_{adapter_id}__"}
        exec(compile(spec["source"], f"<retrieval-adapter:{adapter_id}>", "exec"), namespace, namespace)
        retrieve = namespace.get("retrieve")
        if not callable(retrieve):
            raise RuntimeError(f"retrieval adapter {adapter_id!r} has no retrieve(query) entrypoint")
        value = retrieve(query)
        if inspect.iscoroutine(value):
            value = asyncio.run(value)
        return _json_safe(value)

    def test_retrieval_adapter(self, adapter_id):
        spec = self.call("retrieval_adapter", {"id": adapter_id})
        tests = spec.get("tests")
        if not tests:
            return {"id": adapter_id, "tested": False, "reason": "no tests.source"}
        namespace = {"borg": self, "__name__": f"__borg_retriever_test_{adapter_id}__"}
        exec(compile(spec["source"], f"<retrieval-adapter:{adapter_id}>", "exec"), namespace, namespace)
        exec(compile(tests, f"<retrieval-adapter-tests:{adapter_id}>", "exec"), namespace, namespace)
        retrieve = namespace.get("retrieve")
        test = namespace.get("test")
        if not callable(retrieve) or not callable(test):
            raise RuntimeError(f"retrieval adapter {adapter_id!r} must define retrieve and test")
        value = test(retrieve, self)
        if inspect.iscoroutine(value):
            value = asyncio.run(value)
        return {"id": adapter_id, "tested": True, "passed": True, "result": _json_safe(value)}

    def runtime_status(self):
        return self.call("runtime_status", {})

    def checkpoint(self, key, state):
        return self.call("runtime_checkpoint", {"key": key, "state": state})

    def restore(self, key=None):
        arguments = {} if key is None else {"key": key}
        return self.call("runtime_restore", arguments)

    def write(self, path, content, **kwargs):
        arguments = {"path": path, "content": content}
        arguments.update(kwargs)
        return self.call("write_file", arguments)

    def tool(self, name, arguments=None):
        return self.call("borg_tool", {"name": name, "arguments": {} if arguments is None else arguments})

    def mcp_tools(self):
        return self.call("mcp_tools", {})

    def mcp(self, name, arguments=None):
        return self.call("mcp_call", {"name": name, "arguments": {} if arguments is None else arguments})

    def environment(self, extension_id, server=None):
        return ExtensionEnvironment(self, extension_id, server)

    def storage(self, extension_id):
        return PluginStorage(self, extension_id)


class Harness:
    def __init__(self, borg):
        self.borg = borg

    def _call(self, op, **arguments):
        return self.borg.call("harness", {"op": op, **arguments})

    def list(self, kind=None, scope=None, limit=128):
        arguments = {"limit": limit}
        if kind is not None:
            arguments["kind"] = kind
        if scope is not None:
            arguments["scope"] = scope
        return self._call("list", **arguments).get("entries", [])

    def overview(self, limit=8):
        return self._call("overview", limit=limit)

    def get(self, kind, entry_id, scope="local"):
        return self._call("get", kind=kind, id=entry_id, scope=scope).get("entry")

    def create(self, kind, title, content, scope="local", **options):
        return self._call("create", kind=kind, title=title, content=content, scope=scope, **options)["entry"]

    def update(self, kind, entry_id, title=None, content=None, scope="local", **options):
        arguments = {"kind": kind, "id": entry_id, "scope": scope, **options}
        if title is not None:
            arguments["title"] = title
        if content is not None:
            arguments["content"] = content
        return self._call("update", **arguments)["entry"]

    def delete(self, entry_id, kind=None, scope="local"):
        arguments = {"id": entry_id, "scope": scope}
        if kind is not None:
            arguments["kind"] = kind
        return self._call("delete", **arguments)

    def refine(self, trigger, changes=None, evidence="", outcome="", scope="local"):
        if isinstance(changes, str):
            changes = [changes]
        return self._call(
            "refine",
            trigger=trigger,
            changes=[] if changes is None else list(changes),
            evidence=evidence,
            outcome=outcome,
            scope=scope,
        )["refinement"]

    def plan_refinement(self, observation):
        return self._call("plan_refinement", content=observation).get("steps", [])

    def rollback(self, steps=1):
        return self._call("rollback", steps=steps)["state"]


class ExtensionEnvironment:
    def __init__(self, borg, extension_id, server=None):
        self.borg = borg
        self.extension_id = str(extension_id)
        self.server = None if server is None else str(server)

    def _prefix(self):
        extension = "".join(character if character.isalnum() or character == "_" else "_" for character in self.extension_id)
        prefix = f"mcp__{extension}__"
        if self.server:
            server = "".join(character if character.isalnum() or character == "_" else "_" for character in self.server)
            prefix += f"{server}__"
        return prefix

    @staticmethod
    def _normalize_tool_name(value):
        return "".join(
            character if ("A" <= character <= "Z" or "a" <= character <= "z" or "0" <= character <= "9" or character in "_-") else "_"
            for character in str(value)
        )

    def tools(self):
        return [tool for tool in self.borg.mcp_tools() if tool.get("name", "").startswith(self._prefix())]

    def call(self, tool, arguments=None):
        name = str(tool)
        if not name.startswith("mcp__"):
            normalized = self._normalize_tool_name(name)
            matches = [
                candidate["name"]
                for candidate in self.tools()
                if candidate["name"].endswith(f"__{name}")
                or candidate["name"].endswith(f"__{normalized}")
            ]
            if len(matches) != 1:
                raise RuntimeError(f"environment tool {name!r} resolved to {len(matches)} tools")
            name = matches[0]
        else:
            name = self._normalize_tool_name(name)
            if not name.startswith(self._prefix()):
                raise RuntimeError(f"tool {tool!r} is outside this environment")
        return self.borg.mcp(name, arguments)

    def __getattr__(self, name):
        return lambda arguments=None: self.call(name, arguments)


class PluginStorage:
    def __init__(self, borg, extension_id):
        self.borg = borg
        self.extension_id = str(extension_id)

    def _call(self, operation, scope="session", **arguments):
        return self.borg.call("plugin_store", {
            "extension_id": self.extension_id,
            "scope": scope,
            "op": operation,
            **arguments,
        })

    def get(self, key, scope="session"):
        return self._call("get", scope, key=key).get("entry")

    def list(self, prefix=None, scope="session", limit=200):
        arguments = {"limit": limit}
        if prefix is not None:
            arguments["prefix"] = prefix
        return self._call("list", scope, **arguments).get("entries", [])

    def commit(self, idempotency_key, writes=None, artifacts=None, provenance=None, scope="session"):
        return self._call(
            "commit",
            scope,
            idempotency_key=idempotency_key,
            writes=[] if writes is None else list(writes),
            artifacts=[] if artifacts is None else list(artifacts),
            provenance={} if provenance is None else provenance,
        )

    def verify_artifact(self, artifact_id, scope="session"):
        return self._call("verify_artifact", scope, artifact_id=artifact_id)


class RlmHandle:
    def __init__(self, borg, snapshot):
        self.borg = borg
        self.snapshot = snapshot
        self.session_id = snapshot.get("session_id")
        self.task_name = snapshot.get("task_name")

    def __await__(self):
        async def ready():
            return self
        return ready().__await__()

    def refresh(self):
        agents = self.borg.rlm.list()
        for agent in agents:
            if agent.get("session_id") == self.session_id:
                self.snapshot = agent
                return self
        return self

    def followup(self, message):
        return self.borg.tool("followup_task", {"target": self.task_name or self.session_id, "message": message})

    def interrupt(self):
        return self.borg.tool("interrupt_agent", {"target": self.task_name or self.session_id})

    def send(self, message):
        return self.borg.tool("send_message", {"target": self.task_name or self.session_id, "message": message})

    def wait(self, timeout_ms=30000):
        return self.borg.tool("wait_agent", {"timeout_ms": timeout_ms})


class Rlm:
    def __init__(self, borg):
        self.borg = borg

    def __call__(self, message, task_name=None, **options):
        task_name = task_name or f"runtime-{uuid.uuid4().hex[:12]}"
        arguments = {"task_name": task_name, "message": str(message)}
        for key in ("provider", "model", "reasoning_effort"):
            if key in options and options[key] is not None:
                arguments[key] = options[key]
        return RlmHandle(self.borg, self.borg.tool("spawn_agent", arguments))

    def list(self, path_prefix=None):
        arguments = {} if path_prefix is None else {"path_prefix": path_prefix}
        return self.borg.tool("list_agents", arguments).get("agents", [])

    run = __call__
    list_subagents = list


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
                prefix_code = compile(
                    prefix,
                    "<borg-runtime>",
                    "exec",
                    flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT,
                )
                prefix_value = eval(prefix_code, NAMESPACE, NAMESPACE)
                if inspect.iscoroutine(prefix_value):
                    asyncio.run(prefix_value)
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
            if message.get("bootstrap") is not None:
                _restore_namespace(message["bootstrap"])
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

// Bun is optional. The worker uses one persistent VM context and the same
// JSON-lines host protocol as Python. Synchronous top-level declarations and
// assignments survive requests; code that needs an async block can use an
// explicit `return` from the worker's async wrapper. TypeScript is transpiled
// by Bun when its loader is available, while JavaScript is passed through.
const BUN_WORKER_SOURCE: &str = r#"
const readline = require("node:readline");
const vm = require("node:vm");

const protocolOut = process.stdout;
const pending = new Map();
const context = vm.createContext({});
let stdout = "";
let stderr = "";

function send(message) {
  protocolOut.write(JSON.stringify(message) + "\n");
}

function jsonSafe(value) {
  if (value === undefined) return null;
  try {
    return JSON.parse(JSON.stringify(value));
  } catch (_) {
    return String(value);
  }
}

function restoreNamespace(state) {
  if (state === null || typeof state !== "object" || Array.isArray(state)) {
    throw new Error("runtime checkpoint namespace state must be an object");
  }
  for (const [name, value] of Object.entries(state)) {
    // Checkpoints are data, not executable code. Do not allow persisted data
    // to shadow runtime internals or create arbitrary global property names.
    if (/^[A-Za-z][A-Za-z0-9]*$/.test(name)) context[name] = value;
  }
}

function format(value) {
  if (typeof value === "string") return value;
  return JSON.stringify(jsonSafe(value));
}

const runtimeConsole = {
  log: (...values) => { stdout += values.map(format).join(" ") + "\n"; },
  info: (...values) => { stdout += values.map(format).join(" ") + "\n"; },
  warn: (...values) => { stderr += values.map(format).join(" ") + "\n"; },
  error: (...values) => { stderr += values.map(format).join(" ") + "\n"; },
};

function hostCall(operation, arguments_) {
  const id = crypto.randomUUID();
  send({type: "host_call", id, operation, arguments: arguments_ ?? {}});
  return new Promise((resolve, reject) => pending.set(id, {resolve, reject}));
}

const borg = {
  call: hostCall,
  exec: (command, options = {}) => hostCall("exec_command", {cmd: command, ...options}),
  read: (path, options = {}) => hostCall("read_file", {path, ...options}),
  search: (pattern, options = {}) => hostCall("search_files", {pattern, ...options}),
  history: (text, options = {}) => hostCall("history", {
    ...(text === undefined || text === null ? {} : {text}), ...options,
  }),
  history_index: (afterSequence = 0, limit = 1000) => hostCall("history_index", {
    after_sequence: afterSequence, limit,
  }),
  semantic_search: (query, options = {}) => hostCall("mcp_call", {
    name: "mcp__borg__search_documents",
    arguments: {query, ...options},
  }),
  retrieval_adapter: async (adapterId, query = null) => {
    const spec = await hostCall("retrieval_adapter", {id: adapterId});
    const language = spec?.manifest?.language;
    if (language !== "javascript") {
      throw new Error(`retrieval adapter ${adapterId} uses ${language}; execute it from a matching runtime`);
    }
    const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
    const runner = new AsyncFunction("borg", "query", `${spec.source}\nif (typeof retrieve !== "function") throw new Error("retrieve(query) entrypoint is required");\nreturn await retrieve(query);`);
    return jsonSafe(await runner(borg, query));
  },
  test_retrieval_adapter: async (adapterId) => {
    const spec = await hostCall("retrieval_adapter", {id: adapterId});
    if (!spec.tests) return {id: adapterId, tested: false, reason: "no tests.source"};
    const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
    const runner = new AsyncFunction("borg", `${spec.source}\n${spec.tests}\nif (typeof retrieve !== "function" || typeof test !== "function") throw new Error("retrieve and test entrypoints are required");\nreturn await test(retrieve, borg);`);
    return {id: adapterId, tested: true, passed: true, result: jsonSafe(await runner(borg))};
  },
  runtime_status: () => hostCall("runtime_status", {}),
  checkpoint: (key, state) => hostCall("runtime_checkpoint", {key, state}),
  restore: (key) => hostCall("runtime_restore", key === undefined ? {} : {key}),
  write: (path, content, options = {}) => hostCall("write_file", {path, content, ...options}),
  tool: (name, arguments_ = {}) => hostCall("borg_tool", {name, arguments: arguments_}),
  mcp_tools: () => hostCall("mcp_tools", {}),
  mcp: (name, arguments_ = {}) => hostCall("mcp_call", {name, arguments: arguments_}),
};

function normalizeEnvironmentPart(value) {
  return String(value).replace(/[^A-Za-z0-9_]/g, "_");
}

function normalizeEnvironmentToolName(value) {
  return String(value).replace(/[^A-Za-z0-9_-]/g, "_");
}

borg.harness = {
  list: async (kind = undefined, scope = undefined, limit = 128) => {
    const result = await hostCall("harness", {op: "list", ...(kind === undefined ? {} : {kind}), ...(scope === undefined ? {} : {scope}), limit});
    return result.entries || [];
  },
  overview: (limit = 8) => hostCall("harness", {op: "overview", limit}),
  get: async (kind, id, scope = "local") => (await hostCall("harness", {op: "get", kind, id, scope})).entry || null,
  create: async (kind, title, content, options = {}) => (await hostCall("harness", {op: "create", kind, title, content, scope: options.scope || "local", ...options})).entry,
  update: async (kind, id, fields = {}) => (await hostCall("harness", {op: "update", kind, id, scope: fields.scope || "local", ...fields})).entry,
  delete: (id, kind = undefined, scope = "local") => hostCall("harness", {op: "delete", id, scope, ...(kind === undefined ? {} : {kind})}),
  refine: async (trigger, changes = [], options = {}) => (await hostCall("harness", {op: "refine", trigger, changes: typeof changes === "string" ? [changes] : changes, evidence: options.evidence || "", outcome: options.outcome || "", scope: options.scope || "local"})).refinement,
  plan_refinement: async observation => (await hostCall("harness", {op: "plan_refinement", content: observation})).steps || [],
  rollback: async (steps = 1) => (await hostCall("harness", {op: "rollback", steps})).state,
};

borg.environment = (extensionId, server = undefined) => {
  const prefix = `mcp__${normalizeEnvironmentPart(extensionId)}__${server === undefined ? "" : `${normalizeEnvironmentPart(server)}__`}`;
  return {
    tools: async () => (await borg.mcp_tools()).filter(tool => String(tool.name || "").startsWith(prefix)),
    call: async (tool, arguments_ = {}) => {
      let name = String(tool);
      if (!name.startsWith("mcp__")) {
        const normalized = normalizeEnvironmentToolName(name);
        const matches = (await borg.mcp_tools())
          .filter(candidate => String(candidate.name || "").startsWith(prefix) && (String(candidate.name).endsWith(`__${name}`) || String(candidate.name).endsWith(`__${normalized}`)));
        if (matches.length !== 1) throw new Error(`environment tool ${name} resolved to ${matches.length} tools`);
        name = matches[0].name;
      } else {
        name = normalizeEnvironmentToolName(name);
        if (!name.startsWith(prefix)) {
          throw new Error(`tool ${tool} is outside this environment`);
        }
      }
      return borg.mcp(name, arguments_);
    },
  };
};

borg.storage = (extensionId) => {
  const id = String(extensionId);
  const call = (operation, scope = "session", arguments_ = {}) => hostCall("plugin_store", {
    extension_id: id,
    scope,
    op: operation,
    ...arguments_,
  });
  return {
    get: async (key, scope = "session") => (await call("get", scope, {key})).entry || null,
    list: async (prefix = undefined, scope = "session", limit = 200) => {
      const arguments_ = {limit};
      if (prefix !== undefined && prefix !== null) arguments_.prefix = prefix;
      return (await call("list", scope, arguments_)).entries || [];
    },
    commit: (idempotencyKey, writes = [], artifacts = [], provenance = {}, scope = "session") => call("commit", scope, {
      idempotency_key: idempotencyKey,
      writes,
      artifacts,
      provenance,
    }),
    verifyArtifact: (artifactId, scope = "session") => call("verify_artifact", scope, {artifact_id: artifactId}),
  };
};

const rlm = async (message, options = {}) => {
  const taskName = options.task_name || `runtime-${crypto.randomUUID().slice(0, 12)}`;
  const snapshot = await borg.tool("spawn_agent", {
    task_name: taskName,
    message: String(message),
    ...(options.provider === undefined ? {} : {provider: options.provider}),
    ...(options.model === undefined ? {} : {model: options.model}),
    ...(options.reasoning_effort === undefined ? {} : {reasoning_effort: options.reasoning_effort}),
  });
  const target = snapshot.task_name || snapshot.session_id;
  return {
    ...snapshot,
    snapshot,
    refresh: async () => {
      const agents = await rlm.list();
      return agents.find(agent => agent.session_id === snapshot.session_id) || snapshot;
    },
    followup: message_ => borg.tool("followup_task", {target, message: message_}),
    send: message_ => borg.tool("send_message", {target, message: message_}),
    interrupt: () => borg.tool("interrupt_agent", {target}),
    wait: (timeout_ms = 30000) => borg.tool("wait_agent", {timeout_ms}),
  };
};
rlm.list = async (pathPrefix = undefined) => (await borg.tool("list_agents", pathPrefix === undefined ? {} : {path_prefix: pathPrefix})).agents || [];
rlm.run = rlm;
rlm.list_subagents = rlm.list;
borg.rlm = rlm;
context.borg = borg;
context.console = runtimeConsole;

function finalExpressionParts(source) {
  const lines = source.replace(/\r\n/g, "\n").split("\n");
  while (lines.length && lines[lines.length - 1].trim() === "") lines.pop();
  if (!lines.length) return null;
  const last = lines[lines.length - 1].trim().replace(/;$/, "");
  if (!last || /^(const|let|var|function|class|if|for|while|try|catch|switch|throw|return)\b/.test(last)) {
    return null;
  }
  return {body: lines.slice(0, -1).join("\n"), expression: last};
}

async function execute(source, runtime) {
  stdout = "";
  stderr = "";
  context.resultSlot = null;
  let compiled = source;
  if (runtime === "typescript" && typeof Bun !== "undefined" && Bun.Transpiler) {
    compiled = new Bun.Transpiler({loader: "tsx"}).transformSync(source);
  }
  const parts = finalExpressionParts(compiled);
  try {
    if (parts) {
      try {
        const evaluated = vm.runInContext(`${parts.body}\n;(${parts.expression})`, context);
        context.resultSlot = await Promise.resolve(evaluated);
      } catch (error) {
        if (!/await|return/.test(String(error))) throw error;
        context.resultSlot = await vm.runInContext(`(async () => {${parts.body}\nreturn (${parts.expression});})()`, context);
      }
    } else {
      try {
        context.resultSlot = await Promise.resolve(vm.runInContext(compiled, context));
      } catch (error) {
        if (!/await|return/.test(String(error))) throw error;
        context.resultSlot = await vm.runInContext(`(async () => {${compiled}\n})()`, context);
      }
    }
  } catch (error) {
    throw error;
  }
  return {value: jsonSafe(context.resultSlot), stdout, stderr};
}

const input = readline.createInterface({input: process.stdin, crlfDelay: Infinity});
input.on("line", async (line) => {
  let message;
  try { message = JSON.parse(line); } catch (_) { return; }
  if (message.type === "host_result") {
    const waiter = pending.get(message.id);
    if (!waiter) return;
    pending.delete(message.id);
    if (message.ok) waiter.resolve(message.result);
    else waiter.reject(new Error(message.error || "Borg host call failed"));
    return;
  }
  if (message.type !== "execute") return;
  try {
    if (message.bootstrap !== undefined && message.bootstrap !== null) restoreNamespace(message.bootstrap);
    const output = await execute(message.code || "", message.runtime || "javascript");
    send({type: "result", id: message.id, ok: true, ...output});
  } catch (error) {
    send({type: "result", id: message.id, ok: false, error: String(error && error.stack || error)});
  }
});
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
                "history" => Ok(json!({ "query": arguments, "backend": "test" })),
                "history_index" => Ok(json!({
                    "documents": [],
                    "after_sequence": arguments
                        .get("after_sequence")
                        .cloned()
                        .unwrap_or(json!(0)),
                    "next_after_sequence": arguments
                        .get("after_sequence")
                        .cloned()
                        .unwrap_or(json!(0)),
                    "has_more": false,
                })),
                "mcp_call" => {
                    if arguments.get("name").and_then(Value::as_str)
                        == Some("mcp__borg__search_documents")
                    {
                        Ok(json!({
                            "query": arguments["arguments"]["query"],
                            "hits": [{
                                "document_id": "source-1",
                                "locator": "source-1#chunk-0",
                            "match_mode": "semantic"
                            }]
                        }))
                    } else if arguments.get("name").and_then(Value::as_str)
                        == Some("mcp__surf_lab__lab__step")
                    {
                        Ok(json!({
                            "content": [{"type": "text", "text": "advanced"}]
                        }))
                    } else {
                        bail!("unknown test MCP tool")
                    }
                }
                "mcp_tools" => Ok(json!([
                    {"name": "mcp__surf_lab__lab__step", "inputSchema": {"type": "object"}}
                ])),
                "borg_tool" => match arguments.get("name").and_then(Value::as_str) {
                    Some("spawn_agent") => Ok(json!({
                        "session_id": Uuid::nil(),
                        "task_name": "runtime-child",
                        "status": "starting"
                    })),
                    Some("list_agents") => Ok(json!({"agents": []})),
                    other => bail!("unknown test Borg tool {other:?}"),
                },
                "harness" => Ok(json!({"counts": {"memory": 1}})),
                "retrieval_adapter" => {
                    if arguments.get("id").and_then(Value::as_str) == Some("js-ranker") {
                        Ok(json!({
                            "manifest": {"language": "javascript"},
                            "source": "function retrieve(query) { return {query, source: 'adapter'}; }",
                            "tests": "function test(retrieve, borg) { if (retrieve('test').source !== 'adapter') throw new Error('bad adapter'); return {ok: true}; }"
                        }))
                    } else {
                        Ok(json!({
                            "manifest": {"language": "python"},
                            "source": "def retrieve(query):\n    return {'query': query, 'source': 'adapter'}\n",
                            "tests": "def test(retrieve, borg):\n    assert retrieve('test')['source'] == 'adapter'\n    return {'ok': True}\n"
                        }))
                    }
                }
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

    #[test]
    fn persistent_runtime_environment_contains_no_inherited_configuration() {
        let environment = crate::process_environment::sanitized_environment();
        assert_eq!(environment[0].0, "PATH");
        assert!(
            environment
                .iter()
                .all(|(name, _)| !name.starts_with("BORG_"))
        );
        assert!(!environment.iter().any(|(name, _)| *name == "HOME"));
    }

    #[tokio::test]
    async fn python_namespace_survives_multiple_requests() {
        if !python_available().await {
            return;
        }
        let root = tempdir().expect("temporary runtime root");
        let runtime = PersistentRuntimeWorker::new(root.path().to_path_buf());
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
        let retrieved = runtime
            .execute(
                "borg.retrieval_adapter('ranker', 'needle')",
                None,
                Arc::new(TestHost),
            )
            .await
            .expect("Python retrieval adapter execution");
        assert_eq!(
            retrieved.value,
            json!({"query": "needle", "source": "adapter"})
        );
        let tested = runtime
            .execute(
                "borg.test_retrieval_adapter('ranker')",
                None,
                Arc::new(TestHost),
            )
            .await
            .expect("Python retrieval adapter test execution");
        assert_eq!(tested.value["passed"], true);
        runtime.stop().await;
    }

    #[tokio::test]
    async fn python_worker_restarts_after_managed_process_termination() {
        if !python_available().await {
            return;
        }
        let root = tempdir().expect("temporary runtime root");
        let runtime = PersistentRuntimeWorker::new(root.path().to_path_buf());
        let first = runtime
            .execute("answer = 41\nanswer", None, Arc::new(TestHost))
            .await
            .expect("first Python execution");
        assert_eq!(first.value, json!(41));

        {
            let mut process = runtime.process.lock().await;
            let process = process.as_mut().expect("Python process was started");
            process
                .child
                .start_kill()
                .expect("terminate managed Python process");
            process
                .child
                .wait()
                .await
                .expect("wait for managed Python process");
        }

        let restarted = runtime
            .execute("answer = 42\nanswer", None, Arc::new(TestHost))
            .await
            .expect("runtime should respawn a terminated Python process");
        assert_eq!(restarted.value, json!(42));
        runtime.stop().await;
    }

    #[tokio::test]
    async fn optional_bun_namespace_survives_multiple_requests_and_host_calls() {
        if !tokio::process::Command::new(bun_command())
            .arg("--version")
            .output()
            .await
            .is_ok_and(|output| output.status.success())
        {
            return;
        }
        let root = tempdir().expect("temporary runtime root");
        let runtime =
            PersistentRuntimeWorker::for_bun(Uuid::new_v4(), root.path().to_path_buf(), None);
        let first = runtime
            .execute(
                "answer = 40\nanswer = answer + 2\nanswer",
                None,
                Arc::new(TestHost),
            )
            .await
            .expect("first Bun execution");
        assert_eq!(first.value, json!(42));
        let second = runtime
            .execute("answer += 1\nanswer", None, Arc::new(TestHost))
            .await
            .expect("second Bun execution");
        assert_eq!(second.value, json!(43));
        let host_call = runtime
            .execute("borg.call('echo', {value: 7})", None, Arc::new(TestHost))
            .await
            .expect("Bun host call execution");
        assert_eq!(host_call.value, json!({"value": 7}));
        let history_index = runtime
            .execute(
                "borg.history_index(12, 4).then(page => page.next_after_sequence)",
                None,
                Arc::new(TestHost),
            )
            .await
            .expect("Bun history index host call execution");
        assert_eq!(history_index.value, json!(12));
        let semantic = runtime
            .execute(
                "borg.semantic_search('contract risk', {limit: 3}).then(result => result.hits[0].match_mode)",
                None,
                Arc::new(TestHost),
            )
            .await
            .expect("Bun semantic search host call execution");
        assert_eq!(semantic.value, json!("semantic"));
        let typescript = runtime
            .execute_as(
                "typescript",
                "const typedAnswer: number = 41; typedAnswer + 1",
                None,
                Arc::new(TestHost),
            )
            .await
            .expect("Bun TypeScript execution");
        assert_eq!(typescript.runtime, "typescript");
        assert_eq!(typescript.value, json!(42));
        let retrieved = runtime
            .execute(
                "borg.retrieval_adapter('js-ranker', 'needle')",
                None,
                Arc::new(TestHost),
            )
            .await
            .expect("Bun retrieval adapter execution");
        assert_eq!(
            retrieved.value,
            json!({"query": "needle", "source": "adapter"})
        );
        let tested = runtime
            .execute(
                "borg.test_retrieval_adapter('js-ranker')",
                None,
                Arc::new(TestHost),
            )
            .await
            .expect("Bun retrieval adapter test execution");
        assert_eq!(tested.value["passed"], true);
        runtime.stop().await;
    }

    #[tokio::test]
    async fn python_worker_round_trips_host_calls() {
        if !python_available().await {
            return;
        }
        let root = tempdir().expect("temporary runtime root");
        let runtime = PersistentRuntimeWorker::new(root.path().to_path_buf());
        let host: Arc<dyn RuntimeHost> = Arc::new(TestHost);
        let result = runtime
            .execute("borg.call('echo', {'value': 7})", None, host)
            .await
            .expect("host call execution");
        assert_eq!(result.value, json!({"value": 7}));
        let history = runtime
            .execute(
                "borg.history('needle', mode='regex', limit=3)",
                None,
                Arc::new(TestHost),
            )
            .await
            .expect("history host call execution");
        assert_eq!(history.value["backend"], "test");
        assert_eq!(history.value["query"]["text"], "needle");
        assert_eq!(history.value["query"]["mode"], "regex");
        let history_index = runtime
            .execute(
                "borg.history_index(12, 4)['next_after_sequence']",
                None,
                Arc::new(TestHost),
            )
            .await
            .expect("history index host call execution");
        assert_eq!(history_index.value, json!(12));
        let semantic = runtime
            .execute(
                "borg.semantic_search('contract risk', limit=3)",
                None,
                Arc::new(TestHost),
            )
            .await
            .expect("Python semantic search host call execution");
        assert_eq!(semantic.value["hits"][0]["match_mode"], "semantic");
        runtime.stop().await;
    }

    #[tokio::test]
    async fn python_runtime_exposes_environment_rlm_and_harness_bridges() {
        if !python_available().await {
            return;
        }
        let root = tempdir().expect("temporary runtime root");
        let runtime = PersistentRuntimeWorker::new(root.path().to_path_buf());
        let result = runtime
            .execute(
                "env = borg.environment('surf-lab', 'lab')\ntools = env.tools()\nresponse = env.step({'ticks': 1})\nchild = await borg.rlm('inspect trajectory')\nsummary = borg.harness.overview()\n{'tool': tools[0]['name'], 'text': response['content'][0]['text'], 'child': child.task_name, 'memory_count': summary['counts']['memory']}",
                None,
                Arc::new(TestHost),
            )
            .await
            .expect("environment, RLM, and harness bridge execution");
        assert_eq!(
            result.value,
            json!({
                "tool": "mcp__surf_lab__lab__step",
                "text": "advanced",
                "child": "runtime-child",
                "memory_count": 1
            })
        );
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
        let first = registry
            .python_for_session(session_id, root.path(), None)
            .await;
        let second = registry
            .python_for_session(session_id, root.path(), None)
            .await;
        assert!(Arc::ptr_eq(&first, &second));
    }
}
