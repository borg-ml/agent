//! Bounded Blu guest execution over Borg's durable host boundary.
//!
//! Blu owns control flow. Borg owns every effect: guest code receives no
//! SQLite, filesystem, provider, or process handles. Each host call is
//! journaled before and after execution and is replayed by workflow/call id.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use blu_lang::bytecode::blu::BluLimits;
use blu_lang::frontend::OwnedCompileLimits;
use blu_lang::{
    Dialect, Engine, InterruptHandle, RuntimeError, SemanticProfile, Value as BluValue, Vm,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::runtime::Handle;
use tokio::task;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::native_process::ProcessManager;
use crate::{
    AgentToolDispatcher, EnqueueAutonomyJob, PermissionMode, SessionEvent, SessionEventKind,
    SessionStore, SqliteAutonomyStore, SqliteSessionStore,
};

const MAX_SOURCE_BYTES: usize = 256 * 1024;
const MAX_NAME_BYTES: usize = 128;
const MAX_CALL_JSON_BYTES: usize = 512 * 1024;
const MAX_RESULT_JSON_BYTES: usize = 512 * 1024;
const MAX_INSTRUCTIONS: u64 = 2_000_000;
const MAX_PROCESS_TIMEOUT_MS: u64 = 30 * 60 * 1000;
const MAX_PROCESS_OUTPUT_TOKENS: usize = 64_000;
const WORKFLOW_LEASE_DURATION: Duration = Duration::from_secs(60);
const WORKFLOW_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BluWorkflowRequest {
    pub workflow_id: Uuid,
    pub name: String,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BluWorkflowResult {
    pub workflow_id: Uuid,
    pub source_hash: String,
    pub success: bool,
    pub values: Vec<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone)]
pub(crate) struct BluWorkflowRunner {
    session_id: Uuid,
    store: SqliteSessionStore,
    autonomy: SqliteAutonomyStore,
    dispatcher: Option<AgentToolDispatcher>,
    processes: ProcessManager,
    root: PathBuf,
    permission: PermissionMode,
}

impl BluWorkflowRunner {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        session_id: Uuid,
        store: SqliteSessionStore,
        autonomy: SqliteAutonomyStore,
        dispatcher: Option<AgentToolDispatcher>,
        processes: ProcessManager,
        root: PathBuf,
        permission: PermissionMode,
    ) -> Self {
        Self {
            session_id,
            store,
            autonomy,
            dispatcher,
            processes,
            root,
            permission,
        }
    }

    pub(crate) async fn run(&self, request: BluWorkflowRequest) -> Result<BluWorkflowResult> {
        self.run_with_cancel(request, CancellationToken::new())
            .await
    }

    pub(crate) async fn run_with_cancel(
        &self,
        request: BluWorkflowRequest,
        cancel: CancellationToken,
    ) -> Result<BluWorkflowResult> {
        ensure!(
            !request.name.trim().is_empty(),
            "Blu workflow name is empty"
        );
        ensure!(
            request.name.len() <= MAX_NAME_BYTES,
            "Blu workflow name is too long"
        );
        ensure!(
            !request.source.trim().is_empty(),
            "Blu workflow source is empty"
        );
        ensure!(
            request.source.len() <= MAX_SOURCE_BYTES,
            "Blu workflow source exceeds {MAX_SOURCE_BYTES} bytes"
        );
        ensure!(!cancel.is_cancelled(), "Blu workflow was cancelled");
        let source_hash = source_hash(&request.source);
        let events = self.store.read(self.session_id).await?;

        if let Some(kind) = find_completed(&events, request.workflow_id) {
            ensure!(
                completed_hash(kind) == source_hash,
                "Blu workflow source changed"
            );
            return Ok(completed_result(kind));
        }
        if let Some(kind) = find_started(&events, request.workflow_id) {
            ensure!(
                started_hash(kind) == source_hash,
                "Blu workflow source changed"
            );
        } else {
            self.store
                .append(SessionEvent::new(
                    self.session_id,
                    0,
                    SessionEventKind::BluWorkflowStarted {
                        workflow_id: request.workflow_id,
                        source_hash: source_hash.clone(),
                        name: request.name.clone(),
                    },
                ))
                .await
                .context("journal Blu workflow admission")?;
        }

        let action_payload = json!({
            "workflow_id": request.workflow_id,
            "source_hash": source_hash.clone(),
            "name": request.name.clone(),
        });
        self.store
            .ensure_workflow_action(self.session_id, request.workflow_id, &action_payload)
            .await
            .context("ensure Blu workflow action")?;
        let lease_owner = format!(
            "blu-workflow/{}/{}/{}",
            self.session_id,
            request.workflow_id,
            Uuid::new_v4()
        );
        let action = self
            .store
            .claim_action(
                self.session_id,
                request.workflow_id,
                &lease_owner,
                WORKFLOW_LEASE_DURATION,
            )
            .await?
            .with_context(|| {
                format!(
                    "Blu workflow {} already has a live execution lease",
                    request.workflow_id
                )
            })?;
        let lease_token = action
            .lease_token
            .context("Blu workflow lease did not return a token")?;

        let journal = WorkflowCallJournal::from_events(
            self.session_id,
            request.workflow_id,
            self.store.clone(),
            Handle::current(),
            &events,
        )?;
        let bridge = HostBridge {
            session_id: self.session_id,
            root: self.root.clone(),
            permission: self.permission,
            cancel: cancel.clone(),
            journal,
            autonomy: self.autonomy.clone(),
            dispatcher: self.dispatcher.clone(),
            processes: self.processes.clone(),
        };
        let source = request.source.clone();
        let name = request.name.clone();
        let interrupt = Arc::new(Mutex::new(None::<InterruptHandle>));
        let interrupt_for_worker = Arc::clone(&interrupt);
        let interrupt_for_watcher = Arc::clone(&interrupt);
        let cancel_for_worker = cancel.clone();
        let heartbeat_stop = CancellationToken::new();
        let heartbeat_cancel = heartbeat_stop.clone();
        let heartbeat_store = self.store.clone();
        let heartbeat_owner = lease_owner.clone();
        let heartbeat_workflow_cancel = cancel.clone();
        let heartbeat_session_id = self.session_id;
        let heartbeat_workflow_id = request.workflow_id;
        let heartbeat = tokio::spawn(async move {
            let mut tick = tokio::time::interval(WORKFLOW_HEARTBEAT_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = heartbeat_cancel.cancelled() => break,
                    _ = tick.tick() => {
                        if heartbeat_store
                            .heartbeat_action(
                                heartbeat_session_id,
                                heartbeat_workflow_id,
                                &heartbeat_owner,
                                lease_token,
                                WORKFLOW_LEASE_DURATION,
                            )
                            .await
                            .is_err()
                        {
                            heartbeat_workflow_cancel.cancel();
                            break;
                        }
                    }
                }
            }
        });
        let watcher = tokio::spawn(async move {
            cancel.cancelled().await;
            if let Some(handle) = interrupt_for_watcher
                .lock()
                .expect("Blu interrupt lock poisoned")
                .as_ref()
            {
                handle.interrupt();
            }
        });
        let execution = task::spawn_blocking(move || {
            execute_blu_source(
                &source,
                &name,
                bridge,
                interrupt_for_worker,
                cancel_for_worker,
            )
        })
        .await;
        watcher.abort();
        heartbeat_stop.cancel();
        heartbeat.abort();
        let (success, values, error) = match execution {
            Ok(Ok(values)) => (true, values, None),
            Ok(Err(error)) => (false, Vec::new(), Some(format!("{error:#}"))),
            Err(error) => (
                false,
                Vec::new(),
                Some(format!("Blu workflow worker failed: {error}")),
            ),
        };
        let result = success.then(|| Value::Array(values.clone()));
        let completed = self
            .store
            .append_with_action_lease(
                SessionEvent::new(
                    self.session_id,
                    0,
                    SessionEventKind::BluWorkflowCompleted {
                        workflow_id: request.workflow_id,
                        source_hash,
                        success,
                        result,
                        error: error.clone(),
                    },
                ),
                request.workflow_id,
                &lease_owner,
                lease_token,
            )
            .await
            .context("journal Blu workflow completion")?;
        let SessionEventKind::BluWorkflowCompleted {
            workflow_id,
            source_hash,
            success,
            result,
            error,
        } = completed.kind
        else {
            unreachable!("wrong event kind returned by workflow completion")
        };
        Ok(BluWorkflowResult {
            workflow_id,
            source_hash,
            success,
            values: result
                .and_then(|value| value.as_array().cloned())
                .unwrap_or_default(),
            error,
        })
    }
}

fn source_hash(source: &str) -> String {
    Sha256::digest(source.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn find_started(events: &[SessionEvent], id: Uuid) -> Option<&SessionEventKind> {
    events.iter().find_map(|event| match &event.kind {
        SessionEventKind::BluWorkflowStarted { workflow_id, .. } if *workflow_id == id => {
            Some(&event.kind)
        }
        _ => None,
    })
}

fn find_completed(events: &[SessionEvent], id: Uuid) -> Option<&SessionEventKind> {
    events.iter().find_map(|event| match &event.kind {
        SessionEventKind::BluWorkflowCompleted { workflow_id, .. } if *workflow_id == id => {
            Some(&event.kind)
        }
        _ => None,
    })
}

fn started_hash(kind: &SessionEventKind) -> &str {
    let SessionEventKind::BluWorkflowStarted { source_hash, .. } = kind else {
        unreachable!("wrong started event kind")
    };
    source_hash
}

fn completed_hash(kind: &SessionEventKind) -> &str {
    let SessionEventKind::BluWorkflowCompleted { source_hash, .. } = kind else {
        unreachable!("wrong completed event kind")
    };
    source_hash
}

fn completed_result(kind: &SessionEventKind) -> BluWorkflowResult {
    let SessionEventKind::BluWorkflowCompleted {
        workflow_id,
        source_hash,
        success,
        result,
        error,
    } = kind
    else {
        unreachable!("wrong completed event kind")
    };
    BluWorkflowResult {
        workflow_id: *workflow_id,
        source_hash: source_hash.clone(),
        success: *success,
        values: result
            .as_ref()
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
        error: error.clone(),
    }
}

#[derive(Debug, Clone)]
struct ReplayCall {
    operation: String,
    request: Value,
    response: Option<Value>,
    error: Option<String>,
}

#[derive(Clone)]
struct WorkflowCallJournal {
    session_id: Uuid,
    workflow_id: Uuid,
    store: SqliteSessionStore,
    handle: Handle,
    replay: Arc<Mutex<HashMap<u64, ReplayCall>>>,
}

impl WorkflowCallJournal {
    fn from_events(
        session_id: Uuid,
        workflow_id: Uuid,
        store: SqliteSessionStore,
        handle: Handle,
        events: &[SessionEvent],
    ) -> Result<Self> {
        let mut replay: HashMap<u64, ReplayCall> = HashMap::new();
        for event in events {
            match &event.kind {
                SessionEventKind::BluWorkflowCallRequested {
                    workflow_id: id,
                    call_id,
                    operation,
                    request,
                } if *id == workflow_id => {
                    if let Some(previous) = replay.get(call_id) {
                        ensure!(
                            previous.operation == *operation && previous.request == *request,
                            "Blu workflow call {call_id} was requested with conflicting data"
                        );
                    } else {
                        replay.insert(
                            *call_id,
                            ReplayCall {
                                operation: operation.clone(),
                                request: request.clone(),
                                response: None,
                                error: None,
                            },
                        );
                    }
                }
                SessionEventKind::BluWorkflowCallCompleted {
                    workflow_id: id,
                    call_id,
                    operation,
                    response,
                    error,
                } if *id == workflow_id => {
                    let previous = replay
                        .get_mut(call_id)
                        .with_context(|| format!("Blu call {call_id} completed without request"))?;
                    ensure!(
                        previous.operation == *operation,
                        "Blu call operation changed"
                    );
                    ensure!(
                        previous.response.is_none() && previous.error.is_none(),
                        "Blu call {call_id} completed twice"
                    );
                    previous.response = response.clone();
                    previous.error = error.clone();
                }
                _ => {}
            }
        }
        Ok(Self {
            session_id,
            workflow_id,
            store,
            handle,
            replay: Arc::new(Mutex::new(replay)),
        })
    }

    fn call(
        &self,
        call_id: u64,
        operation: &str,
        request: Value,
        effect: impl FnOnce() -> Result<Value>,
    ) -> Result<Value> {
        ensure!(call_id > 0, "Blu call_id must be greater than zero");
        ensure_json_size(&request, MAX_CALL_JSON_BYTES, "Blu call request")?;
        if let Some(previous) = self
            .replay
            .lock()
            .expect("Blu replay lock poisoned")
            .get(&call_id)
            .cloned()
        {
            ensure!(
                previous.operation == operation && previous.request == request,
                "Blu call {call_id} was reused with different data"
            );
            if let Some(response) = previous.response {
                return Ok(response);
            }
            if let Some(error) = previous.error {
                bail!("replayed Blu call {call_id} failed: {error}");
            }
            bail!("Blu call {call_id} has no terminal record; recovery is required");
        }
        self.append(SessionEventKind::BluWorkflowCallRequested {
            workflow_id: self.workflow_id,
            call_id,
            operation: operation.to_string(),
            request: request.clone(),
        })?;
        let outcome = match effect() {
            Ok(value) => match ensure_json_size(&value, MAX_CALL_JSON_BYTES, "Blu call response") {
                Ok(()) => Ok(value),
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        };
        let (response, error) = match &outcome {
            Ok(value) => (Some(value.clone()), None),
            Err(error) => (None, Some(format!("{error:#}"))),
        };
        self.append(SessionEventKind::BluWorkflowCallCompleted {
            workflow_id: self.workflow_id,
            call_id,
            operation: operation.to_string(),
            response: response.clone(),
            error: error.clone(),
        })?;
        self.replay
            .lock()
            .expect("Blu replay lock poisoned")
            .insert(
                call_id,
                ReplayCall {
                    operation: operation.to_string(),
                    request,
                    response,
                    error,
                },
            );
        outcome
    }

    fn append(&self, kind: SessionEventKind) -> Result<SessionEvent> {
        self.handle.block_on(
            self.store
                .append(SessionEvent::new(self.session_id, 0, kind)),
        )
    }
}

#[derive(Clone)]
struct HostBridge {
    session_id: Uuid,
    root: PathBuf,
    permission: PermissionMode,
    cancel: CancellationToken,
    journal: WorkflowCallJournal,
    autonomy: SqliteAutonomyStore,
    dispatcher: Option<AgentToolDispatcher>,
    processes: ProcessManager,
}

impl HostBridge {
    fn invoke(&self, operation: &'static str, args: &[BluValue]) -> Result<Vec<BluValue>> {
        ensure!(!self.cancel.is_cancelled(), "Blu workflow was cancelled");
        let call_id = guest_u64(args, 0, "call_id")?;
        let response = match operation {
            "emit" => {
                self.require_full_access(operation)?;
                let kind = guest_string(args, 1, "kind")?;
                let payload = guest_json(args, 2, "payload_json")?;
                let request = json!({ "kind": kind, "payload": payload });
                self.journal.call(call_id, operation, request, || {
                    Ok(json!({ "kind": kind, "payload": payload }))
                })?
            }
            "tool" => {
                self.require_full_access(operation)?;
                let name = guest_string(args, 1, "name")?;
                let arguments = guest_json(args, 2, "arguments_json")?;
                let request = json!({ "name": name, "arguments": arguments });
                let dispatcher = self
                    .dispatcher
                    .clone()
                    .context("Borg agent tools are unavailable")?;
                self.journal.call(call_id, operation, request, || {
                    self.block_on(dispatcher.call(&name, arguments))
                })?
            }
            "enqueue" => {
                self.require_full_access(operation)?;
                let key = guest_string(args, 1, "idempotency_key")?;
                let kind = guest_string(args, 2, "kind")?;
                let payload = guest_json(args, 3, "payload_json")?;
                let delay_ms = guest_optional_u64(args, 4, "delay_ms")?.unwrap_or(0);
                let max_attempts = guest_optional_u64(args, 5, "max_attempts")?
                    .unwrap_or(3)
                    .clamp(1, 32) as u32;
                let due_at = Utc::now()
                    .checked_add_signed(chrono::Duration::from_std(Duration::from_millis(
                        delay_ms,
                    ))?)
                    .context("Blu due time overflow")?;
                let durable_key = format!(
                    "blu/{}/{}/{}",
                    self.session_id, self.journal.workflow_id, key
                );
                let request = json!({
                    "idempotency_key": durable_key,
                    "kind": kind,
                    "payload": payload,
                    "due_at": due_at,
                    "max_attempts": max_attempts
                });
                let autonomy = self.autonomy.clone();
                self.journal.call(call_id, operation, request, || {
                    let job = self.block_on(autonomy.enqueue(EnqueueAutonomyJob {
                        job_id: None,
                        idempotency_key: durable_key,
                        kind,
                        payload,
                        due_at,
                        max_attempts,
                        session_id: Some(self.session_id),
                        goal_id: None,
                    }))?;
                    Ok(serde_json::to_value(job)?)
                })?
            }
            "job" => {
                let job_id = guest_uuid(args, 1, "job_id")?;
                let request = json!({ "job_id": job_id });
                let autonomy = self.autonomy.clone();
                self.journal.call(call_id, operation, request, || {
                    let job = self
                        .block_on(autonomy.get(job_id))?
                        .with_context(|| format!("unknown runtime job {job_id}"))?;
                    ensure!(
                        job.session_id == Some(self.session_id),
                        "job belongs to another session"
                    );
                    Ok(serde_json::to_value(job)?)
                })?
            }
            "checkpoint" => {
                self.require_full_access(operation)?;
                let job_id = guest_uuid(args, 1, "job_id")?;
                let checkpoint_key = guest_string(args, 2, "checkpoint_key")?;
                let kind = guest_string(args, 3, "kind")?;
                let state = guest_json(args, 4, "state_json")?;
                let evidence = guest_json(args, 5, "evidence_json")?;
                let request = json!({
                    "job_id": job_id,
                    "checkpoint_key": checkpoint_key,
                    "kind": kind,
                    "state": state,
                    "evidence": evidence
                });
                let autonomy = self.autonomy.clone();
                self.journal.call(call_id, operation, request, || {
                    let job = self
                        .block_on(autonomy.get(job_id))?
                        .with_context(|| format!("unknown runtime job {job_id}"))?;
                    ensure!(
                        job.session_id == Some(self.session_id),
                        "job belongs to another session"
                    );
                    let checkpoint =
                        self.block_on(autonomy.save_checkpoint(crate::SaveAutonomyCheckpoint {
                            checkpoint_id: None,
                            job_id,
                            checkpoint_key,
                            session_id: Some(self.session_id),
                            goal_id: job.goal_id,
                            kind,
                            state,
                            evidence,
                            created_at: Utc::now(),
                        }))?;
                    Ok(serde_json::to_value(checkpoint)?)
                })?
            }
            "exec" => {
                self.require_full_access(operation)?;
                let command = guest_string(args, 1, "command")?;
                let workdir = guest_optional_string(args, 2, "workdir")?;
                let yield_time_ms = guest_optional_u64(args, 3, "yield_time_ms")?;
                let timeout_ms = guest_optional_u64(args, 4, "timeout_ms")?
                    .unwrap_or(10_000)
                    .clamp(1, MAX_PROCESS_TIMEOUT_MS);
                let output_tokens = guest_optional_u64(args, 5, "max_output_tokens")?
                    .unwrap_or(10_000)
                    .clamp(1, MAX_PROCESS_OUTPUT_TOKENS as u64)
                    as usize;
                let request = json!({
                    "command": command,
                    "workdir": workdir,
                    "yield_time_ms": yield_time_ms,
                    "timeout_ms": timeout_ms,
                    "max_output_tokens": output_tokens
                });
                let processes = self.processes.clone();
                let root = self.root.clone();
                self.journal.call(call_id, operation, request, || {
                    let snapshot = self.block_on(processes.exec_with_cancel(
                        self.session_id,
                        &root,
                        command,
                        workdir.as_deref(),
                        yield_time_ms,
                        Some(output_tokens),
                        timeout_ms,
                        Some(self.journal.store.clone()),
                        self.cancel.clone(),
                    ))?;
                    ensure!(!self.cancel.is_cancelled(), "Blu workflow was cancelled");
                    Ok(serde_json::to_value(snapshot)?)
                })?
            }
            _ => bail!("unknown Blu host operation {operation}"),
        };
        ensure_json_size(&response, MAX_RESULT_JSON_BYTES, "Blu host result")?;
        Ok(vec![BluValue::String(Arc::from(
            response.to_string().into_bytes(),
        ))])
    }

    fn require_full_access(&self, operation: &str) -> Result<()> {
        ensure!(
            self.permission == PermissionMode::FullAccess,
            "Blu workflow operation {operation} requires full access"
        );
        Ok(())
    }

    fn block_on<T>(&self, future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
        self.journal.handle.block_on(future)
    }
}

fn execute_blu_source(
    source: &str,
    name: &str,
    bridge: HostBridge,
    interrupt: Arc<Mutex<Option<InterruptHandle>>>,
    cancel: CancellationToken,
) -> Result<Vec<Value>> {
    let vm = Vm::new(Dialect::Blu)
        .with_instruction_limit(MAX_INSTRUCTIONS)
        .with_task_limit(64)
        .with_global_limit(64)
        .with_host_value_limit(512)
        .with_native_result_limit(64)
        .with_heap_object_limit(100_000)
        .with_call_limit(4_096);
    let mut engine = Engine::new(blu_lang::Compiler::default(), vm);
    engine.set_deadline(Some(Instant::now() + Duration::from_secs(30)));
    let interrupt_handle = engine.interrupt_handle();
    *interrupt.lock().expect("Blu interrupt lock poisoned") = Some(interrupt_handle.clone());
    if cancel.is_cancelled() {
        interrupt_handle.interrupt();
    }
    for (name, operation, bridge) in [
        ("borg_emit", "emit", bridge.clone()),
        ("borg_tool", "tool", bridge.clone()),
        ("borg_enqueue", "enqueue", bridge.clone()),
        ("borg_job", "job", bridge.clone()),
        ("borg_checkpoint", "checkpoint", bridge.clone()),
        ("borg_exec", "exec", bridge),
    ] {
        let id = engine
            .vm_mut()
            .try_register_function(move |_vm, args| {
                bridge.invoke(operation, args).map_err(|error| {
                    RuntimeError::LuaMessage(Arc::from(error.to_string().into_bytes()))
                })
            })
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        engine
            .vm_mut()
            .try_set_global(
                Arc::<[u8]>::from(name.as_bytes()),
                BluValue::NativeFunction(id),
            )
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    }
    let mut limits = OwnedCompileLimits::default();
    limits.max_instructions = MAX_INSTRUCTIONS as usize;
    limits.max_bindings = 16_384;
    limits.max_constants = 65_536;
    limits.max_return_values = 64;
    let values = engine
        .execute_owned_source_named_with_limits(
            source,
            name,
            SemanticProfile::Blu,
            limits,
            BluLimits::default(),
        )
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    interrupt
        .lock()
        .expect("Blu interrupt lock poisoned")
        .take();
    let mut result = Vec::with_capacity(values.len());
    for value in values {
        let value = match value {
            BluValue::Nil => Value::Null,
            BluValue::Boolean(value) => Value::Bool(value),
            BluValue::Integer(value) => Value::Number(value.into()),
            BluValue::Number(value) if value.is_finite() => {
                Value::Number(serde_json::Number::from_f64(value).context("non-finite Blu number")?)
            }
            BluValue::String(value) => {
                Value::String(String::from_utf8(value.to_vec()).context("Blu result is not UTF-8")?)
            }
            value => bail!(
                "Blu result contains unsupported {} value",
                value.type_name()
            ),
        };
        result.push(value);
    }
    ensure_json_size(
        &Value::Array(result.clone()),
        MAX_RESULT_JSON_BYTES,
        "Blu workflow result",
    )?;
    Ok(result)
}

fn guest_value<'a>(args: &'a [BluValue], index: usize, name: &str) -> Result<&'a BluValue> {
    args.get(index)
        .with_context(|| format!("Blu argument {index} ({name}) is missing"))
}

fn guest_string(args: &[BluValue], index: usize, name: &str) -> Result<String> {
    match guest_value(args, index, name)? {
        BluValue::String(value) => String::from_utf8(value.to_vec())
            .with_context(|| format!("Blu argument {name} is not UTF-8")),
        value => bail!(
            "Blu argument {name} must be a string, got {}",
            value.type_name()
        ),
    }
}

fn guest_optional_string(args: &[BluValue], index: usize, name: &str) -> Result<Option<String>> {
    match args.get(index) {
        None | Some(BluValue::Nil) => Ok(None),
        Some(_) => guest_string(args, index, name).map(Some),
    }
}

fn guest_u64(args: &[BluValue], index: usize, name: &str) -> Result<u64> {
    match guest_value(args, index, name)? {
        BluValue::Integer(value) if *value >= 0 => {
            u64::try_from(*value).context("integer overflow")
        }
        BluValue::Number(value) if value.is_finite() && *value >= 0.0 && value.fract() == 0.0 => {
            Ok(*value as u64)
        }
        value => bail!(
            "Blu argument {name} must be a non-negative integer, got {}",
            value.type_name()
        ),
    }
}

fn guest_optional_u64(args: &[BluValue], index: usize, name: &str) -> Result<Option<u64>> {
    match args.get(index) {
        None | Some(BluValue::Nil) => Ok(None),
        Some(_) => guest_u64(args, index, name).map(Some),
    }
}

fn guest_uuid(args: &[BluValue], index: usize, name: &str) -> Result<Uuid> {
    Uuid::parse_str(&guest_string(args, index, name)?)
        .with_context(|| format!("Blu argument {name} must be a UUID"))
}

fn guest_json(args: &[BluValue], index: usize, name: &str) -> Result<Value> {
    let value: Value = serde_json::from_str(&guest_string(args, index, name)?)
        .with_context(|| format!("Blu argument {name} is not JSON"))?;
    ensure_json_size(&value, MAX_CALL_JSON_BYTES, name)?;
    Ok(value)
}

fn ensure_json_size(value: &Value, limit: usize, label: &str) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    ensure!(bytes.len() <= limit, "{label} exceeds {limit} bytes");
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    async fn runner(permission: PermissionMode) -> BluWorkflowRunner {
        // The runner owns only the SQLite pool; keep the test database alive
        // across the extra connections used by workflow leases/heartbeats.
        let directory = tempdir().expect("tempdir").keep();
        let store = SqliteSessionStore::open(directory.join("sessions.sqlite3"))
            .await
            .expect("store");
        let session_id = Uuid::new_v4();
        store.create_session(session_id).await.expect("session");
        let autonomy = SqliteAutonomyStore::open(store.pool().clone())
            .await
            .expect("autonomy");
        BluWorkflowRunner::new(
            session_id,
            store,
            autonomy,
            None,
            ProcessManager::default(),
            PathBuf::from("."),
            permission,
        )
    }

    #[tokio::test]
    async fn pure_workflow_is_durable_and_idempotent() {
        let runner = runner(PermissionMode::FullAccess).await;
        let request = BluWorkflowRequest {
            workflow_id: Uuid::new_v4(),
            name: "math".to_string(),
            source: "return 2 + 2".to_string(),
        };
        let first = runner.run(request.clone()).await.expect("run");
        assert_eq!(first.values, vec![json!(4)]);
        assert!(first.success);
        assert_eq!(first, runner.run(request).await.expect("replay"));
    }

    #[tokio::test]
    async fn host_call_is_journaled_and_replayed() {
        let runner = runner(PermissionMode::FullAccess).await;
        let request = BluWorkflowRequest {
            workflow_id: Uuid::new_v4(),
            name: "emit".to_string(),
            source: r##"return borg_emit(1, "note", "{\"ok\":true}")"##.to_string(),
        };
        let first = runner.run(request.clone()).await.expect("run");
        assert!(first.success, "{first:?}");
        assert_eq!(first, runner.run(request).await.expect("replay"));
        let events = runner.store.read(runner.session_id).await.expect("events");
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event.kind,
                    SessionEventKind::BluWorkflowCallRequested { .. }
                ))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event.kind,
                    SessionEventKind::BluWorkflowCallCompleted { .. }
                ))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn expired_workflow_lease_is_reclaimable_and_stale_completion_is_fenced() {
        let runner = runner(PermissionMode::FullAccess).await;
        let request = BluWorkflowRequest {
            workflow_id: Uuid::new_v4(),
            name: "recoverable".to_string(),
            source: "return 9".to_string(),
        };
        let request_hash = source_hash(&request.source);
        runner
            .store
            .append(SessionEvent::new(
                runner.session_id,
                0,
                SessionEventKind::BluWorkflowStarted {
                    workflow_id: request.workflow_id,
                    source_hash: request_hash.clone(),
                    name: request.name.clone(),
                },
            ))
            .await
            .expect("started event");
        runner
            .store
            .ensure_workflow_action(
                runner.session_id,
                request.workflow_id,
                &json!({
                    "workflow_id": request.workflow_id,
                    "source_hash": request_hash,
                    "name": request.name,
                }),
            )
            .await
            .expect("workflow action");
        let stale = runner
            .store
            .claim_action(
                runner.session_id,
                request.workflow_id,
                "crashed-worker",
                Duration::from_millis(20),
            )
            .await
            .expect("claim")
            .expect("stale claim");
        tokio::time::sleep(Duration::from_millis(40)).await;
        let stale_completion = runner
            .store
            .append_with_action_lease(
                SessionEvent::new(
                    runner.session_id,
                    0,
                    SessionEventKind::BluWorkflowCompleted {
                        workflow_id: request.workflow_id,
                        source_hash: source_hash(&request.source),
                        success: true,
                        result: Some(json!([9])),
                        error: None,
                    },
                ),
                request.workflow_id,
                "crashed-worker",
                stale.lease_token.expect("stale lease token"),
            )
            .await;
        assert!(stale_completion.is_err(), "expired worker must be fenced");

        let recovered = runner.run(request).await.expect("recovered workflow");
        assert!(recovered.success, "{recovered:?}");
        assert_eq!(recovered.values, vec![json!(9)]);
        assert_eq!(
            runner
                .store
                .read(runner.session_id)
                .await
                .expect("events")
                .iter()
                .filter(|event| matches!(event.kind, SessionEventKind::BluWorkflowCompleted { .. }))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn mutating_calls_require_full_access() {
        let runner = runner(PermissionMode::Manual).await;
        let result = runner
            .run(BluWorkflowRequest {
                workflow_id: Uuid::new_v4(),
                name: "denied".to_string(),
                source: r##"return borg_emit(1, "note", "{}")"##.to_string(),
            })
            .await
            .expect("terminal failure");
        assert!(!result.success);
        assert!(result.error.unwrap_or_default().contains("full access"));
    }

    #[tokio::test]
    async fn cancellation_interrupts_a_running_blu_program_and_commits_failure() {
        let runner = runner(PermissionMode::FullAccess).await;
        let cancel = CancellationToken::new();
        let workflow_id = Uuid::new_v4();
        let task_runner = runner.clone();
        let task_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            task_runner
                .run_with_cancel(
                    BluWorkflowRequest {
                        workflow_id,
                        name: "cancelled".to_string(),
                        source: "while true do end".to_string(),
                    },
                    task_cancel,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel.cancel();
        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("cancellation completes")
            .expect("workflow task")
            .expect("workflow terminal record");
        assert!(!result.success);
        assert!(result.error.unwrap_or_default().contains("interrupt"));
        assert!(
            runner
                .store
                .read(runner.session_id)
                .await
                .unwrap()
                .iter()
                .any(|event| matches!(event.kind, SessionEventKind::BluWorkflowCompleted { .. }))
        );
    }
}
