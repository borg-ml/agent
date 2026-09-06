use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
#[cfg(unix)]
use std::{fs::Permissions, os::unix::fs::PermissionsExt};

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
#[cfg(not(unix))]
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::sync::{Mutex, OnceCell, broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use ts_rs::TS;
use uuid::Uuid;

use crate::persistent_runtime::{PersistentRuntimeRegistry, RuntimeHost};
use crate::{
    ApprovalDecision, AtomicWorkClaim, Audience, CodingProvider, DeliveryMode, EventActor,
    HostCommand, HostResourceLimits, LaunchSession, MessageStatus, ModelGoalStatus,
    NewWorkspaceMessage, PromptDelivery, Provenance, SessionConsultationTools, SessionEvent,
    SessionEventKind, SessionGoalToolRequest, SessionGoalTools, SessionStatus, SessionStore,
    SessionTodoToolRequest, SessionTodoTools, SharedWork, SqliteWorkspaceStore, StructuredMention,
    TodoItemUpdate, WorkDependency, WorkReview, WorkspaceArtifact, WorkspaceDecision,
    WorkspaceEvent, WorkspaceEventKind, WorkspaceFilesystemOperation, WorkspaceFilesystemOutcome,
    WorkspaceFilesystemRequest, WorkspaceMessageReceipt, WorkspaceReference,
    WorkspaceReviewRequest, WorkspaceStore,
};

pub const DEFAULT_MAX_SUBAGENTS: usize = 16;
const ROOT_MESSAGE_RETRY_INTERVAL: Duration = Duration::from_millis(500);
const RUNTIME_DEFAULT_FILE_BYTES: u64 = 256 * 1024;
const RUNTIME_MAX_FILE_BYTES: u64 = 8 * 1024 * 1024;
const RUNTIME_DEFAULT_COMMAND_TIMEOUT_MS: u64 = 120_000;
const RUNTIME_MAX_COMMAND_TIMEOUT_MS: u64 = 30 * 60 * 1000;
const MAX_RUNTIME_HOST_CALLS: usize = 128;
const DEFAULT_PERSISTENT_PEER_CONSULTATION_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const MIN_PERSISTENT_PEER_CONSULTATION_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_PERSISTENT_PEER_CONSULTATION_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);
const SIDECAR_STOP_TIMEOUT: Duration = Duration::from_secs(30);

fn persistent_peer_consultation_timeout() -> Duration {
    let seconds = std::env::var("BORG_PEER_CONSULTATION_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(|seconds| {
            seconds.clamp(
                MIN_PERSISTENT_PEER_CONSULTATION_TIMEOUT.as_secs(),
                MAX_PERSISTENT_PEER_CONSULTATION_TIMEOUT.as_secs(),
            )
        });
    Duration::from_secs(seconds.unwrap_or(DEFAULT_PERSISTENT_PEER_CONSULTATION_TIMEOUT.as_secs()))
}

fn format_timeout(timeout: Duration) -> String {
    let seconds = timeout.as_secs();
    if seconds.is_multiple_of(60) {
        format!("{} minutes", seconds / 60)
    } else {
        format!("{seconds} seconds")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum SubagentStatus {
    Starting,
    Running,
    Ready,
    WaitingForApproval,
    Stopped,
    Failed,
}

impl SubagentStatus {
    fn is_terminal(self) -> bool {
        matches!(self, Self::Stopped | Self::Failed)
    }

    /// Whether this child currently owns one provider-execution slot.
    ///
    /// `Ready` is deliberately non-terminal because the same conversational
    /// worker can accept a follow-up assignment, but an idle ready worker is
    /// not concurrently executing and must not block an unrelated spawn.
    fn consumes_concurrency_slot(self) -> bool {
        matches!(
            self,
            Self::Starting | Self::Running | Self::WaitingForApproval
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(default)]
#[ts(export)]
pub struct SubagentUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    pub context_tokens: Option<u64>,
    pub cost_microusd: Option<u64>,
    pub cost_basis: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SubagentSnapshot {
    pub session_id: Uuid,
    pub parent_session_id: Uuid,
    pub task_name: String,
    pub status: SubagentStatus,
    pub provider: CodingProvider,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub cwd: PathBuf,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub detail: Option<String>,
    pub final_text: Option<String>,
    #[serde(default)]
    pub usage: SubagentUsage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum SubagentActivityKind {
    Started,
    Updated,
    Completed,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SubagentActivity {
    Started {
        agent: SubagentSnapshot,
    },
    SessionEvent {
        parent_session_id: Uuid,
        task_name: String,
        event: SessionEvent,
    },
    Stopped {
        agent: SubagentSnapshot,
    },
    Failed {
        agent: SubagentSnapshot,
    },
    Completed {
        agent: SubagentSnapshot,
    },
}

#[derive(Debug, Clone)]
pub struct SpawnSubagent {
    pub task_name: String,
    pub message: String,
    pub provider: Option<CodingProvider>,
    pub model: Option<String>,
    pub effort: Option<String>,
}

#[derive(Clone)]
struct BluWorkflowToolContext {
    session_id: Uuid,
    root: PathBuf,
    permission: crate::PermissionMode,
    snapshot: Arc<dyn Fn() -> Vec<crate::BluWorkflowDefinition> + Send + Sync>,
    processes: crate::native_process::ProcessManager,
    store: crate::SqliteSessionStore,
    autonomy: crate::SqliteAutonomyStore,
}

/// One provider-neutral model-tool dispatcher for durable goals and child
/// sessions. Provider adapters should transport this catalog, not implement
/// their own goal or collaboration semantics.
#[derive(Clone)]
pub struct AgentToolDispatcher {
    monitors: Option<crate::monitor::Monitors>,
    goals: SessionGoalTools,
    todos: SessionTodoTools,
    consultation: Option<SessionConsultationTools>,
    subagents: Option<SubagentCoordinator>,
    subagents_enabled: bool,
    shared_work: Option<SharedWorkToolContext>,
    lsp: crate::LspService,
    provider: CodingProvider,
    actor_session_id: Uuid,
    consultation_enabled: bool,
    team_policy: Option<crate::TeamPolicy>,
    self_service: crate::self_service::SelfServiceContext,
    autonomy: Option<crate::SqliteAutonomyStore>,
    provider_capabilities: Vec<crate::ProviderCapability>,
    blu_workflows: Option<BluWorkflowToolContext>,
    extension_workflows: Arc<RwLock<Vec<crate::BluWorkflowDefinition>>>,
    extension_api: Arc<RwLock<crate::ExtensionApiSnapshot>>,
    runtime_root: PathBuf,
    runtime_permission: crate::PermissionMode,
    tool_approvals: Arc<RwLock<Option<crate::session::SessionToolApprovals>>>,
    resource_limits: Option<HostResourceLimits>,
    execution_provider: Arc<RwLock<Arc<dyn crate::ExecutionProvider>>>,
    persistent_runtimes: PersistentRuntimeRegistry,
    runtime_mcp: Arc<Mutex<RuntimeMcpState>>,
    harness_lock: Arc<Mutex<()>>,
    web_search: Option<Arc<dyn borg_search::WebSearchProvider>>,
}

#[derive(Default)]
struct RuntimeMcpState {
    base_servers: Option<Vec<borg_provider::mcp::ExternalMcpServer>>,
    extension_servers: Vec<borg_provider::mcp::ExternalMcpServer>,
    configured_servers: Vec<borg_provider::mcp::ExternalMcpServer>,
    runtime: Option<crate::native_mcp::NativeMcpRuntime>,
}

fn same_mcp_server(
    left: &borg_provider::mcp::ExternalMcpServer,
    right: &borg_provider::mcp::ExternalMcpServer,
) -> bool {
    left.name == right.name
        && left.command == right.command
        && left.args == right.args
        && left.env == right.env
        && left.allowed_tools == right.allowed_tools
}

fn same_mcp_servers(
    left: &[borg_provider::mcp::ExternalMcpServer],
    right: &[borg_provider::mcp::ExternalMcpServer],
) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| same_mcp_server(left, right))
}

fn effective_mcp_servers(
    state: &RuntimeMcpState,
) -> Result<Vec<borg_provider::mcp::ExternalMcpServer>> {
    let mut names = BTreeSet::new();
    let mut servers: Vec<borg_provider::mcp::ExternalMcpServer> = Vec::new();
    let base = state.base_servers.as_deref().unwrap_or(&[]);
    for server in base.iter().chain(&state.extension_servers) {
        ensure!(
            !server.name.trim().is_empty(),
            "runtime MCP server name is empty"
        );
        if !names.insert(server.name.clone()) {
            let existing = servers
                .iter()
                .find(|candidate| candidate.name == server.name)
                .expect("duplicate MCP name has an existing server");
            ensure!(
                same_mcp_server(existing, server),
                "runtime MCP server `{}` was configured with conflicting definitions",
                server.name
            );
            continue;
        }
        servers.push(server.clone());
    }
    Ok(servers)
}

#[derive(Debug)]
pub struct AgentToolServer {
    #[cfg(unix)]
    socket_path: PathBuf,
    #[cfg(not(unix))]
    tcp_addr: std::net::SocketAddr,
    #[cfg(not(unix))]
    token: String,
    provider: CodingProvider,
    subagents_enabled: bool,
    consultation_enabled: bool,
    shared_work_enabled: bool,
    web_search_enabled: bool,
    extension_tool_names: Vec<String>,
    team_policy: Option<crate::TeamPolicy>,
    cancel: CancellationToken,
}

#[derive(Clone)]
pub(crate) struct SharedWorkToolContext {
    store: SqliteWorkspaceStore,
    workspace_id: Uuid,
    participant_id: Uuid,
}

impl SharedWorkToolContext {
    pub(crate) fn new(
        store: SqliteWorkspaceStore,
        workspace_id: Uuid,
        participant_id: Uuid,
    ) -> Self {
        Self {
            store,
            workspace_id,
            participant_id,
        }
    }
}

impl AgentToolServer {
    pub async fn start(
        runtime_dir: impl Into<PathBuf>,
        session_id: Uuid,
        dispatcher: AgentToolDispatcher,
    ) -> Result<Self> {
        #[cfg(unix)]
        {
            Self::start_unix(runtime_dir.into(), session_id, dispatcher).await
        }
        #[cfg(not(unix))]
        {
            Self::start_loopback(runtime_dir.into(), session_id, dispatcher).await
        }
    }

    #[cfg(unix)]
    async fn start_unix(
        runtime_dir: PathBuf,
        session_id: Uuid,
        dispatcher: AgentToolDispatcher,
    ) -> Result<Self> {
        let runtime_dir = runtime_dir.join("agent-tools");
        std::fs::create_dir_all(&runtime_dir)?;
        std::fs::set_permissions(&runtime_dir, Permissions::from_mode(0o700))
            .with_context(|| format!("failed to secure {}", runtime_dir.display()))?;
        let socket_path = agent_tool_socket_path(&runtime_dir, session_id)?;
        if socket_path.exists() {
            std::fs::remove_file(&socket_path)?;
        }
        let listener = UnixListener::bind(&socket_path)
            .with_context(|| format!("failed to bind {}", socket_path.display()))?;
        std::fs::set_permissions(&socket_path, Permissions::from_mode(0o600))
            .with_context(|| format!("failed to secure {}", socket_path.display()))?;
        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let cleanup_path = socket_path.clone();
        let provider = dispatcher.provider;
        let subagents_enabled = dispatcher.subagents_enabled;
        let consultation_enabled = dispatcher.consultation_enabled();
        let shared_work_enabled = dispatcher.shared_work.is_some();
        let web_search_enabled = dispatcher.web_search.is_some();
        let extension_tool_names = dispatcher.extension_tool_names();
        let team_policy = dispatcher.team_policy.clone();
        tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    accepted = listener.accept() => accepted,
                    _ = server_cancel.cancelled() => break,
                };
                let Ok((stream, _)) = accepted else { break };
                let dispatcher = dispatcher.clone();
                tokio::spawn(serve_agent_tool_connection(
                    stream,
                    dispatcher,
                    None,
                    server_cancel.clone(),
                ));
            }
            let _ = std::fs::remove_file(cleanup_path);
            dispatcher
                .persistent_runtimes
                .stop_session(dispatcher.actor_session_id)
                .await;
        });
        Ok(Self {
            socket_path,
            provider,
            subagents_enabled,
            consultation_enabled,
            shared_work_enabled,
            web_search_enabled,
            extension_tool_names,
            team_policy,
            cancel,
        })
    }

    #[cfg(not(unix))]
    async fn start_loopback(
        runtime_dir: PathBuf,
        _session_id: Uuid,
        dispatcher: AgentToolDispatcher,
    ) -> Result<Self> {
        std::fs::create_dir_all(runtime_dir.join("agent-tools"))?;
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .context("failed to bind local agent tool server")?;
        let tcp_addr = listener.local_addr()?;
        let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let server_token = token.clone();
        let provider = dispatcher.provider;
        let subagents_enabled = dispatcher.subagents_enabled;
        let consultation_enabled = dispatcher.consultation_enabled();
        let shared_work_enabled = dispatcher.shared_work.is_some();
        let web_search_enabled = dispatcher.web_search.is_some();
        let extension_tool_names = dispatcher.extension_tool_names();
        let team_policy = dispatcher.team_policy.clone();
        tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    accepted = listener.accept() => accepted,
                    _ = server_cancel.cancelled() => break,
                };
                let Ok((stream, peer)) = accepted else { break };
                if !peer.ip().is_loopback() {
                    continue;
                }
                tokio::spawn(serve_agent_tool_connection(
                    stream,
                    dispatcher.clone(),
                    Some(server_token.clone()),
                    server_cancel.clone(),
                ));
            }
            dispatcher
                .persistent_runtimes
                .stop_session(dispatcher.actor_session_id)
                .await;
        });
        Ok(Self {
            tcp_addr,
            token,
            provider,
            subagents_enabled,
            consultation_enabled,
            shared_work_enabled,
            web_search_enabled,
            extension_tool_names,
            team_policy,
            cancel,
        })
    }

    pub fn external_mcp_server(&self) -> Result<borg_provider::mcp::ExternalMcpServer> {
        let mut env = BTreeMap::new();
        env.insert(
            "BORG_AGENT_TOOL_PROVIDER".to_string(),
            self.provider.catalog_backend().to_string(),
        );
        if let Some(policy) = &self.team_policy
            && let Ok(policy) = serde_json::to_string(policy)
        {
            env.insert("BORG_AGENT_TEAM_POLICY".to_string(), policy);
        }
        env.insert(
            "BORG_AGENT_SHARED_WORK_ENABLED".to_string(),
            self.shared_work_enabled.to_string(),
        );
        env.insert(
            "BORG_AGENT_CONSULTATION_ENABLED".to_string(),
            self.consultation_enabled.to_string(),
        );
        #[cfg(unix)]
        env.insert(
            "BORG_AGENT_TOOL_SOCKET".to_string(),
            self.socket_path.display().to_string(),
        );
        #[cfg(not(unix))]
        {
            env.insert("BORG_AGENT_TOOL_TCP".to_string(), self.tcp_addr.to_string());
            env.insert("BORG_AGENT_TOOL_TOKEN".to_string(), self.token.clone());
        }
        Ok(borg_provider::mcp::ExternalMcpServer {
            name: "borg_agent".to_string(),
            command: agent_mcp_executable()?.to_string_lossy().into_owned(),
            args: vec!["__agent-mcp".to_string()],
            env,
            allowed_tools: agent_tool_specs_with_capabilities_and_consultation_and_search(
                self.provider,
                self.subagents_enabled,
                self.shared_work_enabled,
                self.team_policy.as_ref(),
                self.consultation_enabled,
                self.web_search_enabled,
            )
            .into_iter()
            .filter_map(|tool| {
                tool["name"]
                    .as_str()
                    .map(|name| format!("mcp__borg_agent__{name}"))
            })
            .chain(self.extension_tool_names.iter().cloned())
            .collect(),
        })
    }
}

fn agent_mcp_executable() -> Result<PathBuf> {
    let current = std::env::current_exe().context("failed to locate the Borg executable")?;
    resolve_agent_mcp_executable(&current)
}

fn resolve_agent_mcp_executable(current: &Path) -> Result<PathBuf> {
    if current.is_file() {
        return Ok(current.to_path_buf());
    }

    // Linux decorates /proc/self/exe with a literal ` (deleted)` suffix after
    // an atomic in-place upgrade. std::env::current_exe() preserves that
    // decoration, so passing it directly to Codex or Claude makes their MCP
    // launcher look for a filename that cannot exist. Prefer the replacement
    // now installed at the original path. This also keeps provider-persisted
    // MCP configuration valid after the old Borg host exits.
    #[cfg(target_os = "linux")]
    if let Some(replacement) = current
        .to_string_lossy()
        .strip_suffix(" (deleted)")
        .map(PathBuf::from)
        .filter(|path| path.is_file())
    {
        tracing::warn!(
            running_executable = %current.display(),
            replacement = %replacement.display(),
            "Borg was upgraded while this host remained active; using the installed replacement for agent MCP"
        );
        return Ok(replacement);
    }

    // A process may also have been launched from a temporary executable that
    // was removed without replacement. The live inode remains executable via
    // procfs for this host's lifetime, which is safer than emitting a broken
    // MCP command and failing the user's turn at provider startup.
    #[cfg(target_os = "linux")]
    {
        let live_executable = PathBuf::from(format!("/proc/{}/exe", std::process::id()));
        if live_executable.is_file() {
            tracing::warn!(
                running_executable = %current.display(),
                live_executable = %live_executable.display(),
                "Borg's launch path is unavailable; using the live executable for agent MCP"
            );
            return Ok(live_executable);
        }
    }

    bail!(
        "Borg cannot expose session tools because its executable is unavailable at {}",
        current.display()
    )
}

impl Drop for AgentToolServer {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[derive(Deserialize)]
struct AgentToolWireRequest {
    name: String,
    #[serde(default)]
    arguments: Value,
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    workflow_approved: bool,
}

async fn serve_agent_tool_connection<S>(
    stream: S,
    dispatcher: AgentToolDispatcher,
    expected_token: Option<String>,
    shutdown: CancellationToken,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (read, mut write) = tokio::io::split(stream);
    let mut lines = BufReader::new(read).lines();
    while let Ok(Some(line)) = tokio::select! {
        biased;
        _ = shutdown.cancelled() => Ok(None),
        line = lines.next_line() => line,
    } {
        let response = match serde_json::from_str::<AgentToolWireRequest>(&line) {
            Ok(request)
                if expected_token
                    .as_ref()
                    .is_some_and(|token| request.token.as_ref() != Some(token)) =>
            {
                json!({ "error": "agent tool authentication failed" })
            }
            Ok(request) if request.name == "__borg_tools" => {
                json!({ "result": dispatcher.specs() })
            }
            Ok(request) => {
                let cancel = shutdown.child_token();
                let call = dispatcher.call_with_workflow_control(
                    &request.name,
                    request.arguments,
                    request.workflow_approved,
                    Some(cancel.clone()),
                );
                tokio::pin!(call);
                tokio::select! {
                    result = &mut call => match result {
                        Ok(result) => json!({ "result": result }),
                        Err(error) => json!({ "error": format!("{error:#}") }),
                    },
                    next = async {
                        tokio::select! {
                            biased;
                            _ = shutdown.cancelled() => Ok(None),
                            next = lines.next_line() => next,
                        }
                    } => {
                        cancel.cancel();
                        // Poll cancellation cleanup before dropping the tool future.
                        if tokio::time::timeout(Duration::from_secs(2), &mut call).await.is_err() {
                            tracing::warn!(tool = %request.name, "agent tool cancellation cleanup timed out");
                        }
                        match next {
                            Ok(Some(_)) => json!({
                                "error": "agent tool connection received a second request before the first completed"
                            }),
                            Ok(None) | Err(_) => return,
                        }
                    }
                }
            }
            Err(error) => json!({ "error": error.to_string() }),
        };
        let response = format!("{response}\n");
        let written = tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            written = write.write_all(response.as_bytes()) => written,
        };
        if written.is_err() {
            break;
        }
    }
}

impl AgentToolDispatcher {
    // The dispatcher deliberately receives each independently disableable
    // service explicitly at its construction boundary.
    #[allow(clippy::too_many_arguments)]
    #[allow(dead_code)]
    pub(crate) fn new(
        goals: SessionGoalTools,
        todos: SessionTodoTools,
        subagents: Option<SubagentCoordinator>,
        lsp: crate::LspService,
        provider: CodingProvider,
        actor_session_id: Uuid,
        subagents_enabled: bool,
        shared_work: Option<SharedWorkToolContext>,
        team_policy: Option<crate::TeamPolicy>,
        cwd: PathBuf,
        consultation: Option<SessionConsultationTools>,
        autonomy: Option<crate::SqliteAutonomyStore>,
        provider_capabilities: Vec<crate::ProviderCapability>,
        workflow_snapshot: Option<Arc<dyn Fn() -> Vec<crate::BluWorkflowDefinition> + Send + Sync>>,
        workflow_processes: crate::native_process::ProcessManager,
        permission: crate::PermissionMode,
    ) -> Self {
        Self::new_with_search(
            goals,
            todos,
            subagents,
            lsp,
            provider,
            actor_session_id,
            subagents_enabled,
            shared_work,
            team_policy,
            cwd,
            consultation,
            autonomy,
            provider_capabilities,
            workflow_snapshot,
            workflow_processes,
            permission,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_search(
        goals: SessionGoalTools,
        todos: SessionTodoTools,
        subagents: Option<SubagentCoordinator>,
        lsp: crate::LspService,
        provider: CodingProvider,
        actor_session_id: Uuid,
        subagents_enabled: bool,
        shared_work: Option<SharedWorkToolContext>,
        team_policy: Option<crate::TeamPolicy>,
        cwd: PathBuf,
        consultation: Option<SessionConsultationTools>,
        autonomy: Option<crate::SqliteAutonomyStore>,
        provider_capabilities: Vec<crate::ProviderCapability>,
        workflow_snapshot: Option<Arc<dyn Fn() -> Vec<crate::BluWorkflowDefinition> + Send + Sync>>,
        workflow_processes: crate::native_process::ProcessManager,
        permission: crate::PermissionMode,
        web_search: Option<Arc<dyn borg_search::WebSearchProvider>>,
    ) -> Self {
        let consultation_enabled = subagents
            .as_ref()
            .is_none_or(|team| team.is_root_session(actor_session_id));
        let runtime_root = cwd.clone();
        let execution_provider: Arc<dyn crate::ExecutionProvider> = Arc::new(
            crate::LocalExecutionProvider::with_process_manager(workflow_processes.clone()),
        );
        let extension_workflows = Arc::new(RwLock::new(
            workflow_snapshot
                .as_ref()
                .map(|snapshot| snapshot())
                .unwrap_or_default(),
        ));
        let workflow_state = Arc::clone(&extension_workflows);
        let blu_workflows = workflow_snapshot
            .zip(autonomy.clone())
            .map(|(_snapshot, autonomy)| BluWorkflowToolContext {
                session_id: actor_session_id,
                root: cwd.clone(),
                permission,
                snapshot: Arc::new(move || {
                    workflow_state
                        .read()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .clone()
                }),
                processes: workflow_processes,
                store: autonomy.session_store(),
                autonomy,
            });
        Self {
            monitors: None,
            goals,
            todos,
            consultation,
            subagents,
            subagents_enabled,
            shared_work,
            lsp,
            provider,
            actor_session_id,
            consultation_enabled,
            team_policy,
            self_service: crate::self_service::SelfServiceContext::new(cwd),
            autonomy,
            provider_capabilities,
            blu_workflows,
            extension_workflows,
            extension_api: Arc::new(RwLock::new(crate::ExtensionApiSnapshot::default())),
            runtime_root,
            runtime_permission: permission,
            tool_approvals: Arc::new(RwLock::new(None)),
            resource_limits: None,
            execution_provider: Arc::new(RwLock::new(execution_provider)),
            persistent_runtimes: PersistentRuntimeRegistry::default(),
            runtime_mcp: Arc::new(Mutex::new(RuntimeMcpState::default())),
            harness_lock: Arc::new(Mutex::new(())),
            web_search,
        }
    }

    pub(crate) fn with_monitors(mut self, monitors: crate::monitor::Monitors) -> Self {
        self.monitors = Some(monitors);
        self
    }

    pub(crate) fn configure_tool_approvals(&self, approvals: crate::session::SessionToolApprovals) {
        *self
            .tool_approvals
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(approvals);
    }

    pub(crate) fn with_resource_limits(mut self, limits: Option<HostResourceLimits>) -> Self {
        self.resource_limits = limits;
        self
    }

    pub(crate) fn configure_execution_provider(&self, provider: Arc<dyn crate::ExecutionProvider>) {
        *self
            .execution_provider
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = provider;
    }

    pub(crate) fn execution_provider(&self) -> Arc<dyn crate::ExecutionProvider> {
        self.execution_provider
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Configure the external MCP grant for the session without starting any
    /// child process yet. The persistent runtime starts its MCP clients only
    /// when code explicitly calls `borg.mcp(...)`, so ordinary sessions do
    /// not pay for unused integrations.
    pub(crate) async fn configure_runtime_mcp(
        &self,
        servers: Vec<borg_provider::mcp::ExternalMcpServer>,
    ) -> Result<()> {
        let mut state = self.runtime_mcp.lock().await;
        if let Some(existing) = &state.base_servers {
            ensure!(
                same_mcp_servers(existing, &servers),
                "runtime MCP base grant cannot change after session startup"
            );
        } else {
            state.base_servers = Some(servers);
        }
        let effective = effective_mcp_servers(&state)?;
        if !same_mcp_servers(&state.configured_servers, &effective) {
            state.runtime = None;
            state.configured_servers = effective;
        }
        Ok(())
    }

    pub(crate) async fn configure_runtime_mcp_extensions(
        &self,
        servers: Vec<borg_provider::mcp::ExternalMcpServer>,
    ) -> Result<()> {
        let mut state = self.runtime_mcp.lock().await;
        state.extension_servers = servers;
        let effective = effective_mcp_servers(&state)?;
        if !same_mcp_servers(&state.configured_servers, &effective) {
            state.runtime = None;
            state.configured_servers = effective;
        }
        Ok(())
    }

    pub(crate) async fn runtime_mcp_tools(&self) -> Result<Value> {
        let mut state = self.runtime_mcp.lock().await;
        ensure!(
            !state.configured_servers.is_empty(),
            "external MCP is unavailable for this session"
        );
        if state.runtime.is_none() {
            state.runtime = Some(
                crate::native_mcp::NativeMcpRuntime::start(state.configured_servers.clone())
                    .await?,
            );
        }
        Ok(serde_json::to_value(
            state
                .runtime
                .as_ref()
                .expect("runtime was initialized")
                .definitions(),
        )?)
    }

    pub(crate) async fn runtime_mcp_call(&self, name: &str, arguments: Value) -> Result<Value> {
        let mut state = self.runtime_mcp.lock().await;
        ensure!(
            !state.configured_servers.is_empty(),
            "external MCP is unavailable for this session"
        );
        if state.runtime.is_none() {
            state.runtime = Some(
                crate::native_mcp::NativeMcpRuntime::start(state.configured_servers.clone())
                    .await?,
            );
        }
        state
            .runtime
            .as_ref()
            .expect("runtime was initialized")
            .call(name, arguments, None)
            .await
    }

    pub fn specs(&self) -> Vec<Value> {
        let mut specs = agent_tool_specs_with_capabilities_and_consultation(
            self.provider,
            self.subagents_enabled,
            self.shared_work.is_some(),
            self.team_policy.as_ref(),
            self.consultation_enabled,
        );
        if self.web_search.is_some() {
            specs.push(web_search_tool_spec());
        }
        if self.autonomy.is_some() {
            specs.extend(autonomy_tool_specs());
        }
        let extension_api = self.extension_api_snapshot();
        specs.extend(extension_api.tool_specs());
        specs.extend(extension_api.command_specs());
        add_action_metadata(&mut specs);
        specs
    }

    pub(crate) fn configure_extension_workflows(
        &self,
        workflows: Vec<crate::BluWorkflowDefinition>,
    ) {
        *self
            .extension_workflows
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = workflows;
    }

    pub(crate) fn configure_extension_api(
        &self,
        snapshot: crate::ExtensionApiSnapshot,
    ) -> Result<()> {
        snapshot.validate()?;
        *self
            .extension_api
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = snapshot;
        Ok(())
    }

    fn extension_api_snapshot(&self) -> crate::ExtensionApiSnapshot {
        self.extension_api
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn extension_tool_names(&self) -> Vec<String> {
        let snapshot = self.extension_api_snapshot();
        snapshot
            .tool_wires()
            .into_iter()
            .chain(snapshot.command_wires())
            .map(|wire| format!("mcp__borg_agent__{wire}"))
            .collect()
    }

    pub(crate) fn session_store(&self) -> Option<crate::SqliteSessionStore> {
        self.autonomy
            .as_ref()
            .map(crate::SqliteAutonomyStore::session_store)
    }

    pub(crate) async fn harness_prompt_appendix(&self) -> Result<String> {
        let store = self.session_store();
        let mut appendix = crate::harness::prompt_appendix(
            self.actor_session_id,
            &self.runtime_root,
            store.as_ref(),
            &self.harness_lock,
        )
        .await?;
        appendix.push_str(&crate::imported_memory::prompt_appendix(&self.runtime_root).await?);
        Ok(appendix)
    }

    fn consultation_enabled(&self) -> bool {
        self.consultation_enabled
    }

    pub async fn call(&self, name: &str, arguments: Value) -> Result<Value> {
        self.call_with_workflow_control(name, arguments, false, None)
            .await
    }

    async fn read_workspace_tool(
        &self,
        execution_provider: &dyn crate::ExecutionProvider,
        name: &str,
        mut arguments: Value,
    ) -> Result<Value> {
        if let Some(arguments) = arguments.as_object_mut() {
            arguments.remove("action");
        }
        match name {
            "read_file" => {
                let args: RuntimeReadFileArgs = serde_json::from_value(arguments)?;
                execution_provider
                    .read_file(crate::ExecutionReadRequest {
                        root: self.runtime_root.clone(),
                        path: PathBuf::from(args.path),
                        offset_line: args.offset_line.unwrap_or(1),
                        limit_lines: args.limit_lines.unwrap_or(2_000),
                        max_bytes: args
                            .max_bytes
                            .unwrap_or(RUNTIME_DEFAULT_FILE_BYTES)
                            .min(
                                self.resource_limits
                                    .as_ref()
                                    .map(|limits| limits.max_file_transfer_bytes)
                                    .unwrap_or(RUNTIME_MAX_FILE_BYTES),
                            )
                            .clamp(1, RUNTIME_MAX_FILE_BYTES)
                            as usize,
                    })
                    .await
            }
            "search_files" => {
                let args: RuntimeSearchFilesArgs = serde_json::from_value(arguments)?;
                execution_provider
                    .search_files(crate::ExecutionSearchRequest {
                        root: self.runtime_root.clone(),
                        path: PathBuf::from(args.path.unwrap_or_else(|| ".".to_string())),
                        pattern: args.pattern,
                        literal: args.literal.unwrap_or(false),
                        case_sensitive: args.case_sensitive.unwrap_or(true),
                        offset: args.offset.unwrap_or(0),
                        limit: args.limit.unwrap_or(200),
                    })
                    .await
            }
            "list_files" => {
                let args: RuntimeListFilesArgs = serde_json::from_value(arguments)?;
                self.workspace_filesystem(
                    execution_provider,
                    WorkspaceFilesystemOperation::List {
                        path: PathBuf::from(args.path.unwrap_or_else(|| ".".to_string())),
                        limit: args.limit.unwrap_or(200).clamp(1, 2_000),
                    },
                )
                .await
            }
            _ => bail!("unknown workspace read tool {name}"),
        }
    }

    async fn workspace_filesystem(
        &self,
        execution_provider: &dyn crate::ExecutionProvider,
        operation: WorkspaceFilesystemOperation,
    ) -> Result<Value> {
        let response = execution_provider
            .filesystem(
                std::slice::from_ref(&self.runtime_root),
                WorkspaceFilesystemRequest {
                    request_id: Uuid::new_v4(),
                    workspace_id: self.actor_session_id,
                    root_path: self.runtime_root.clone(),
                    timeout_ms: 30_000,
                    operation,
                },
                &self.resource_limits.clone().unwrap_or_default(),
            )
            .await;
        match response.outcome {
            WorkspaceFilesystemOutcome::Success { output } => Ok(serde_json::to_value(output)?),
            WorkspaceFilesystemOutcome::Failure {
                code,
                message,
                retryable,
            } => {
                bail!("{code:?}: {message} (retryable={retryable})")
            }
        }
    }

    // Only authorized native/runtime callers may enter here; this is not an MCP dispatch route.
    pub(crate) async fn mutate_workspace_tool(
        &self,
        execution_provider: &dyn crate::ExecutionProvider,
        name: &str,
        mut arguments: Value,
    ) -> Result<Value> {
        if let Some(arguments) = arguments.as_object_mut() {
            arguments.remove("action");
        }
        let operation = match name {
            "write_file" => {
                let args: RuntimeWriteFileArgs = serde_json::from_value(arguments)?;
                WorkspaceFilesystemOperation::WriteText {
                    path: PathBuf::from(args.path),
                    text: args.content,
                    overwrite: args.overwrite.unwrap_or(false),
                    create_parent_dirs: args.create_parent_dirs.unwrap_or(true),
                }
            }
            "edit_file" => {
                let args: RuntimeEditFileArgs = serde_json::from_value(arguments)?;
                ensure!(
                    !args.old_text.is_empty(),
                    "edit_file old_text must not be empty"
                );
                let read = self
                    .workspace_filesystem(
                        execution_provider,
                        WorkspaceFilesystemOperation::ReadText {
                            path: PathBuf::from(&args.path),
                            max_bytes: RUNTIME_MAX_FILE_BYTES,
                        },
                    )
                    .await?;
                let current = read
                    .get("text")
                    .and_then(Value::as_str)
                    .context("workspace read did not return text")?;
                let matches = current.matches(&args.old_text).count();
                ensure!(
                    matches > 0,
                    "edit_file old_text was not found in {}",
                    args.path
                );
                ensure!(
                    matches == 1 || args.replace_all.unwrap_or(false),
                    "edit_file old_text matched {matches} locations in {}; set replace_all=true or provide more context",
                    args.path
                );
                let text = if args.replace_all.unwrap_or(false) {
                    current.replace(&args.old_text, &args.new_text)
                } else {
                    current.replacen(&args.old_text, &args.new_text, 1)
                };
                WorkspaceFilesystemOperation::WriteText {
                    path: PathBuf::from(args.path),
                    text,
                    overwrite: true,
                    create_parent_dirs: false,
                }
            }
            _ => bail!("unknown workspace mutation tool {name}"),
        };
        self.workspace_filesystem(execution_provider, operation)
            .await
    }

    pub(crate) async fn call_without_extension_hooks(
        &self,
        name: &str,
        arguments: Value,
        workflow_approved: bool,
        workflow_cancel: Option<CancellationToken>,
    ) -> Result<Value> {
        self.call_with_workflow_control_unhooked(
            name,
            arguments,
            workflow_approved,
            workflow_cancel,
            None,
        )
        .await
    }

    pub(crate) async fn call_extension_command(
        &self,
        name: &str,
        arguments: Value,
        invocation_id: Uuid,
        workflow_approved: bool,
        workflow_cancel: Option<CancellationToken>,
    ) -> Result<Value> {
        anyhow::ensure!(
            self.extension_api_snapshot().command(name).is_some(),
            "unknown extension command {name}"
        );
        self.call_with_workflow_control_and_invocation(
            name,
            arguments,
            workflow_approved,
            workflow_cancel,
            Some(invocation_id),
        )
        .await
    }

    pub(crate) async fn call_with_workflow_control(
        &self,
        name: &str,
        arguments: Value,
        workflow_approved: bool,
        workflow_cancel: Option<CancellationToken>,
    ) -> Result<Value> {
        let mut workflow_approved = workflow_approved;
        if name == "runtime_exec"
            && !workflow_approved
            && self.runtime_permission != crate::PermissionMode::FullAccess
        {
            let approvals = self
                .tool_approvals
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            if let Some(approvals) = approvals {
                let detail = serde_json::to_string(&arguments)?;
                ensure!(
                    detail.len() <= crate::MAX_HOOK_ARGUMENT_BYTES,
                    "tool request is too large to display for approval; split it into smaller calls"
                );
                let request = approvals.request("Use Borg persistent runtime".to_string(), detail);
                let decision = if let Some(cancel) = &workflow_cancel {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => bail!("tool approval was cancelled"),
                        decision = request => decision?,
                    }
                } else {
                    request.await?
                };
                ensure!(
                    decision != crate::ApprovalDecision::Deny,
                    "tool execution was denied by the approval policy"
                );
                workflow_approved = true;
            }
        }
        self.call_with_workflow_control_and_invocation(
            name,
            arguments,
            workflow_approved,
            workflow_cancel,
            None,
        )
        .await
    }

    async fn call_with_workflow_control_and_invocation(
        &self,
        name: &str,
        arguments: Value,
        workflow_approved: bool,
        workflow_cancel: Option<CancellationToken>,
        explicit_invocation_id: Option<Uuid>,
    ) -> Result<Value> {
        let is_command = self.extension_api_snapshot().command(name).is_some();
        let event_prefix = if is_command { "command" } else { "tool" };
        let serialized = serde_json::to_vec(&arguments)?;
        let invocation_id = explicit_invocation_id.unwrap_or_else(|| {
            Uuid::new_v5(
                &self.actor_session_id,
                &[
                    event_prefix.as_bytes(),
                    b"\0",
                    name.as_bytes(),
                    b"\0",
                    &serialized,
                ]
                .concat(),
            )
        });
        let hook_arguments = crate::bounded_hook_arguments(&arguments);
        let before_event = format!("{event_prefix}_execute_before");
        self.run_extension_hooks(
            &before_event,
            invocation_id,
            json!({
                "event": before_event.as_str(),
                "session_id": self.actor_session_id,
                "call_id": invocation_id,
                "tool": (!is_command).then_some(name),
                "command": is_command.then_some(name),
                "arguments": hook_arguments.clone(),
            }),
        )
        .await?;
        let result = self
            .call_with_workflow_control_unhooked(
                name,
                arguments,
                workflow_approved,
                workflow_cancel,
                explicit_invocation_id,
            )
            .await;
        let after_event = format!("{event_prefix}_execute_after");
        let after_arguments = match &result {
            Ok(output) => json!({
                "event": after_event.as_str(),
                "session_id": self.actor_session_id,
                "call_id": invocation_id,
                "tool": (!is_command).then_some(name),
                "command": is_command.then_some(name),
                "arguments": hook_arguments,
                "result": crate::bounded_hook_arguments(output),
            }),
            Err(error) => json!({
                "event": after_event.as_str(),
                "session_id": self.actor_session_id,
                "call_id": invocation_id,
                "tool": (!is_command).then_some(name),
                "command": is_command.then_some(name),
                "arguments": hook_arguments,
                "error": error.to_string(),
            }),
        };
        if let Err(error) = self
            .run_extension_hooks(&after_event, invocation_id, after_arguments)
            .await
        {
            tracing::warn!(%error, event = %after_event, name, "extension lifecycle hook failed");
        }
        result
    }

    pub(crate) async fn run_extension_hooks(
        &self,
        event: &str,
        invocation_id: Uuid,
        arguments: Value,
    ) -> Result<()> {
        let snapshot = self.extension_api_snapshot();
        let Some(context) = self.blu_workflows.as_ref() else {
            anyhow::ensure!(
                !snapshot.hooks.iter().any(|hook| hook.event == event),
                "extension hooks are unavailable for this session"
            );
            return Ok(());
        };
        let arguments = crate::bounded_hook_arguments(&arguments);
        for hook in snapshot.hooks.iter().filter(|hook| hook.event == event) {
            let workflow = (context.snapshot)()
                .into_iter()
                .find(|workflow| {
                    workflow.extension_id == hook.extension_id && workflow.name == hook.workflow
                })
                .with_context(|| {
                    format!(
                        "extension hook {}:{} references missing workflow {}:{}",
                        hook.extension_id, hook.name, hook.extension_id, hook.workflow
                    )
                })?;
            let workflow_id = Uuid::new_v5(
                &invocation_id,
                format!("extension-hook:{event}:{}:{}", hook.extension_id, hook.name).as_bytes(),
            );
            self.run_workflow_definition_with_arguments(
                context,
                workflow,
                workflow_id,
                arguments.clone(),
                false,
                None,
            )
            .await?;
        }
        Ok(())
    }

    async fn call_with_workflow_control_unhooked(
        &self,
        name: &str,
        arguments: Value,
        workflow_approved: bool,
        workflow_cancel: Option<CancellationToken>,
        explicit_invocation_id: Option<Uuid>,
    ) -> Result<Value> {
        if let Some(tool) = self.extension_api_snapshot().tool(name).cloned() {
            let context = self
                .blu_workflows
                .as_ref()
                .context("extension workflow tools are unavailable for this session")?;
            let workflow = (context.snapshot)()
                .into_iter()
                .find(|workflow| {
                    workflow.extension_id == tool.extension_id && workflow.name == tool.workflow
                })
                .with_context(|| {
                    format!(
                        "extension tool {} references missing workflow {}:{}",
                        tool.wire_name, tool.extension_id, tool.workflow
                    )
                })?;
            let serialized = serde_json::to_vec(&arguments)?;
            let workflow_id = Uuid::new_v5(
                &self.actor_session_id,
                &[tool.wire_name.as_bytes(), b"\0", &serialized].concat(),
            );
            return self
                .run_workflow_definition_with_arguments(
                    context,
                    workflow,
                    workflow_id,
                    arguments,
                    workflow_approved,
                    workflow_cancel,
                )
                .await;
        }
        if let Some(command) = self.extension_api_snapshot().command(name).cloned() {
            let context = self
                .blu_workflows
                .as_ref()
                .context("extension command workflows are unavailable for this session")?;
            let workflow = (context.snapshot)()
                .into_iter()
                .find(|workflow| {
                    workflow.extension_id == command.extension_id
                        && workflow.name == command.workflow
                })
                .with_context(|| {
                    format!(
                        "extension command {} references missing workflow {}:{}",
                        command.name, command.extension_id, command.workflow
                    )
                })?;
            let serialized = serde_json::to_vec(&arguments)?;
            let workflow_id = explicit_invocation_id.unwrap_or_else(|| {
                Uuid::new_v5(
                    &self.actor_session_id,
                    &[name.as_bytes(), b"\0", &serialized].concat(),
                )
            });
            return self
                .run_workflow_definition_with_arguments(
                    context,
                    workflow,
                    workflow_id,
                    arguments,
                    workflow_approved,
                    workflow_cancel,
                )
                .await;
        }
        match name {
            "read_file" | "search_files" | "list_files" => {
                self.read_workspace_tool(self.execution_provider().as_ref(), name, arguments)
                    .await
            }
            "monitor" => {
                ensure!(
                    self.runtime_permission == crate::PermissionMode::FullAccess
                        || workflow_approved,
                    "monitor requires Full Access or an explicit approval"
                );
                let args = serde_json::from_value(arguments)?;
                let monitors = self
                    .monitors
                    .as_ref()
                    .context("monitors are unavailable for this session")?;
                let timeout = self
                    .resource_limits
                    .as_ref()
                    .map(|limits| limits.max_workspace_command_timeout_ms)
                    .unwrap_or(24 * 60 * 60 * 1000);
                Ok(serde_json::to_value(
                    monitors
                        .start(
                            self.actor_session_id,
                            &self.runtime_root,
                            args,
                            self.session_store(),
                            timeout,
                        )
                        .await?,
                )?)
            }
            "list_monitors" => {
                let _: NoArgs = serde_json::from_value(arguments)?;
                Ok(serde_json::to_value(
                    self.monitors
                        .as_ref()
                        .context("monitors are unavailable for this session")?
                        .list()
                        .await,
                )?)
            }
            "stop_monitor" => {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Args {
                    monitor_id: Uuid,
                }
                let args: Args = serde_json::from_value(arguments)?;
                Ok(serde_json::to_value(
                    self.monitors
                        .as_ref()
                        .context("monitors are unavailable for this session")?
                        .stop(args.monitor_id)
                        .await?,
                )?)
            }
            "list_workflows" | "list_blu_workflows" => {
                let _: NoArgs = serde_json::from_value(arguments)?;
                let context = self
                    .blu_workflows
                    .as_ref()
                    .context("workflow tools are unavailable for this session")?;
                let blu_only = name == "list_blu_workflows";
                Ok(json!({
                    "workflows": (context.snapshot)()
                        .into_iter()
                        .filter(|workflow| !blu_only || workflow.runtime == crate::WorkflowRuntime::Blu)
                        .map(|workflow| json!({
                            "extension_id": workflow.extension_id,
                            "name": workflow.name,
                            "description": workflow.description,
                            "runtime": workflow.runtime,
                        }))
                        .collect::<Vec<_>>()
                }))
            }
            "run_workflow" => {
                let args: RunWorkflowArgs = serde_json::from_value(arguments)?;
                let context = self
                    .blu_workflows
                    .as_ref()
                    .context("workflow tools are unavailable for this session")?;
                let workflow = (context.snapshot)()
                    .into_iter()
                    .find(|workflow| {
                        workflow.extension_id == args.extension_id && workflow.name == args.name
                    })
                    .with_context(|| {
                        format!(
                            "workflow {}:{} is not present in the current extension snapshot",
                            args.extension_id, args.name
                        )
                    })?;
                self.run_workflow_definition(
                    context,
                    workflow,
                    args.workflow_id,
                    workflow_approved,
                    workflow_cancel,
                )
                .await
            }
            "run_blu_extension" => {
                let args: RunBluExtensionArgs = serde_json::from_value(arguments)?;
                let context = self
                    .blu_workflows
                    .as_ref()
                    .context("Blu workflow tools are unavailable for this session")?;
                let workflow = (context.snapshot)()
                    .into_iter()
                    .find(|workflow| {
                        workflow.extension_id == args.extension_id && workflow.name == args.name
                    })
                    .with_context(|| {
                        format!(
                            "Blu workflow {}:{} is not present in the current extension snapshot",
                            args.extension_id, args.name
                        )
                    })?;
                anyhow::ensure!(
                    workflow.runtime == crate::WorkflowRuntime::Blu,
                    "run_blu_extension can only invoke Blu workflows; use run_workflow for {}",
                    workflow.runtime.label()
                );
                self.run_workflow_definition(
                    context,
                    workflow,
                    args.workflow_id,
                    workflow_approved,
                    workflow_cancel,
                )
                .await
            }
            "runtime_exec" => {
                let args: PersistentRuntimeArgs = serde_json::from_value(arguments)?;
                self.run_persistent_runtime(args, workflow_approved, workflow_cancel)
                    .await
            }
            "query_history" => {
                let query: crate::SessionHistoryQuery = serde_json::from_value(arguments)?;
                let store = self
                    .session_store()
                    .context("lossless session history is unavailable for this session")?;
                Ok(serde_json::to_value(
                    store.query_history(self.actor_session_id, query).await?,
                )?)
            }
            "history_index" => {
                let args: HistoryIndexArgs = serde_json::from_value(arguments)?;
                let store = self
                    .session_store()
                    .context("lossless session history is unavailable for this session")?;
                history_index_response(&store, self.actor_session_id, args).await
            }
            "get_goal" => {
                let _: NoArgs = serde_json::from_value(arguments)?;
                goal_response(self.goals.call(SessionGoalToolRequest::Get).await)
            }
            "create_goal" => {
                let args: CreateGoalArgs = serde_json::from_value(arguments)?;
                goal_response(
                    self.goals
                        .call(SessionGoalToolRequest::Create {
                            objective: args.objective,
                            token_budget: args.token_budget,
                        })
                        .await,
                )
            }
            "update_goal" => {
                let args: UpdateGoalArgs = serde_json::from_value(arguments)?;
                goal_response(
                    self.goals
                        .call(SessionGoalToolRequest::Update {
                            status: args.status,
                        })
                        .await,
                )
            }
            "consult_model" => {
                anyhow::ensure!(
                    self.consultation_enabled,
                    "model consultation is disabled for peer sessions"
                );
                let args: ConsultModelArgs = serde_json::from_value(arguments)?;
                let consultation = self
                    .consultation
                    .as_ref()
                    .context("model consultation is disabled for this session")?
                    .call(args.profile, args.prompt)
                    .await
                    .map_err(|error| anyhow::anyhow!(error))?;
                Ok(json!({
                    "provider": consultation.provider.catalog_backend(),
                    "model": consultation.model,
                    "response": consultation.final_text,
                }))
            }
            "consult_peer" => {
                anyhow::ensure!(
                    self.consultation_enabled,
                    "persistent peer consultation is disabled for peer sessions"
                );
                anyhow::ensure!(
                    self.subagents_enabled,
                    "persistent peer consultation requires subagents"
                );
                let args: ConsultPeerArgs = serde_json::from_value(arguments)?;
                self.subagents
                    .as_ref()
                    .context("persistent peer consultation is disabled for this session")?
                    .consult_peer(self.provider, args.profile.as_deref(), &args.prompt)
                    .await
            }
            "rotate_peer" => {
                anyhow::ensure!(
                    self.consultation_enabled,
                    "persistent peer rotation is disabled for peer sessions"
                );
                anyhow::ensure!(
                    self.subagents_enabled,
                    "persistent peer rotation requires subagents"
                );
                let args: RotatePeerArgs = serde_json::from_value(arguments)?;
                self.subagents
                    .as_ref()
                    .context("persistent peer rotation is disabled for this session")?
                    .rotate_peer(
                        self.provider,
                        args.profile.as_deref(),
                        args.handoff.as_deref(),
                    )
                    .await
            }
            "get_plan" => {
                let _: NoArgs = serde_json::from_value(arguments)?;
                todo_response(self.todos.call(SessionTodoToolRequest::Get).await)
            }
            "update_plan" => {
                let args: UpdatePlanArgs = serde_json::from_value(arguments)?;
                todo_response(
                    self.todos
                        .call(SessionTodoToolRequest::Update { items: args.plan })
                        .await,
                )
            }
            "lsp_status" => {
                let _: NoArgs = serde_json::from_value(arguments)?;
                Ok(self.lsp.status().await)
            }
            "get_provider_capabilities" => {
                let _: NoArgs = serde_json::from_value(arguments)?;
                let providers =
                    crate::host::refresh_provider_capability_usage(&self.provider_capabilities)
                        .await;
                Ok(json!({
                    "providers": providers,
                    "instruction": "Check usage availability before cross-provider spawn or consultation. Only providers with can_spawn=true are eligible."
                }))
            }
            "web_search" => {
                let args: WebSearchArgs = serde_json::from_value(arguments)?;
                let provider = self
                    .web_search
                    .as_ref()
                    .context("web search is unavailable for this session")?;
                let request = borg_search::SearchRequest {
                    query: args.query,
                    max_results: args.max_results.unwrap_or(borg_search::DEFAULT_RESULTS),
                    include_domains: args.include_domains.unwrap_or_default(),
                    exclude_domains: args.exclude_domains.unwrap_or_default(),
                };
                Ok(serde_json::to_value(provider.search(request).await?)?)
            }
            "lsp_diagnostics" => {
                let args: LspPathArgs = serde_json::from_value(arguments)?;
                self.lsp.diagnostics(&args.path).await
            }
            "lsp_workspace_diagnostics" => {
                let args: LspWorkspaceDiagnosticsArgs = serde_json::from_value(arguments)?;
                self.lsp.workspace_diagnostics(args.path.as_deref()).await
            }
            "lsp_hover" => {
                let args: LspPositionArgs = serde_json::from_value(arguments)?;
                self.lsp.hover(&args.path, args.line, args.character).await
            }
            "lsp_definition" => {
                let args: LspPositionArgs = serde_json::from_value(arguments)?;
                self.lsp
                    .definition(&args.path, args.line, args.character)
                    .await
            }
            "lsp_references" => {
                let args: LspPositionArgs = serde_json::from_value(arguments)?;
                self.lsp
                    .references(&args.path, args.line, args.character)
                    .await
            }
            "lsp_document_symbols" => {
                let args: LspPathArgs = serde_json::from_value(arguments)?;
                self.lsp.document_symbols(&args.path).await
            }
            "lsp_workspace_symbols" => {
                let args: LspWorkspaceSymbolArgs = serde_json::from_value(arguments)?;
                self.lsp.workspace_symbols(&args.query).await
            }
            name if is_shared_work_tool(name) => {
                self.shared_work
                    .as_ref()
                    .context("shared-work tools are disabled by session capabilities")?
                    .call(name, arguments)
                    .await
            }
            name if crate::self_service::is_tool(name) => self.self_service.call(name, arguments),
            name if is_autonomy_tool(name) => {
                let store = self
                    .autonomy
                    .as_ref()
                    .context("durable autonomous runtime is unavailable")?;
                call_autonomy_tool(store, self.actor_session_id, name, arguments).await
            }
            _ => {
                if !self.subagents_enabled {
                    bail!("subagent tools are disabled by session capabilities");
                }
                self.subagents
                    .as_ref()
                    .context("subagent coordinator is disabled")?
                    .call_tool_as(self.actor_session_id, name, arguments)
                    .await
            }
        }
    }

    async fn run_workflow_definition(
        &self,
        context: &BluWorkflowToolContext,
        workflow: crate::BluWorkflowDefinition,
        workflow_id: Uuid,
        workflow_approved: bool,
        workflow_cancel: Option<CancellationToken>,
    ) -> Result<Value> {
        self.run_workflow_definition_with_arguments(
            context,
            workflow,
            workflow_id,
            Value::Object(Default::default()),
            workflow_approved,
            workflow_cancel,
        )
        .await
    }

    async fn run_workflow_definition_with_arguments(
        &self,
        context: &BluWorkflowToolContext,
        workflow: crate::BluWorkflowDefinition,
        workflow_id: Uuid,
        invocation_arguments: Value,
        workflow_approved: bool,
        workflow_cancel: Option<CancellationToken>,
    ) -> Result<Value> {
        let permission = if workflow_approved {
            crate::PermissionMode::FullAccess
        } else {
            context.permission
        };
        let runner = crate::blu_workflow::BluWorkflowRunner::new(
            context.session_id,
            context.store.clone(),
            context.autonomy.clone(),
            Some(self.clone()),
            context.processes.clone(),
            context.root.clone(),
            permission,
        )
        .with_extension_id(workflow.extension_id.clone())
        .with_invocation_arguments(invocation_arguments);
        let cancel = workflow_cancel.unwrap_or_default();
        if workflow.runtime == crate::WorkflowRuntime::Blu {
            let profile = crate::blu_workflow::embedded_source_profile(&workflow.entrypoint);
            return Ok(serde_json::to_value(
                runner
                    .run_with_profile(
                        crate::BluWorkflowRequest {
                            workflow_id,
                            name: format!("{}:{}", workflow.extension_id, workflow.name),
                            source: workflow.source,
                        },
                        profile,
                        cancel,
                    )
                    .await?,
            )?);
        }
        anyhow::ensure!(
            permission == crate::PermissionMode::FullAccess,
            "external workflow runtime {} requires full access or an explicit approval",
            workflow.runtime.label()
        );
        let command = workflow
            .command
            .clone()
            .unwrap_or_else(|| workflow.runtime.default_command().to_string());
        let artifact_hash = crate::blu_workflow::runtime_artifact_hash(&workflow);
        Ok(serde_json::to_value(
            runner
                .run_runtime_with_cancel(
                    crate::blu_workflow::RuntimeWorkflowRequest {
                        workflow_id,
                        name: format!("{}:{}", workflow.extension_id, workflow.name),
                        runtime: workflow.runtime,
                        artifact_hash,
                        command,
                        args: workflow.args,
                        entrypoint: workflow.entrypoint,
                        working_directory: workflow.working_directory,
                    },
                    cancel,
                )
                .await?,
        )?)
    }

    async fn run_persistent_runtime(
        &self,
        args: PersistentRuntimeArgs,
        workflow_approved: bool,
        cancellation: Option<CancellationToken>,
    ) -> Result<Value> {
        let runtime = args.runtime.as_deref().unwrap_or("python");
        let runtime: &'static str = match runtime {
            "python" => "python",
            "javascript" => "javascript",
            "typescript" => "typescript",
            other => bail!(
                "persistent runtime `{other}` is not available; use `python`, `javascript`, or `typescript`"
            ),
        };
        ensure!(
            self.runtime_permission == crate::PermissionMode::FullAccess || workflow_approved,
            "persistent runtime requires Full Access or an explicit approval"
        );
        if let Some(cancel) = cancellation.as_ref() {
            ensure!(!cancel.is_cancelled(), "persistent runtime was cancelled");
        }
        let runtime_worker = match runtime {
            "python" => {
                self.persistent_runtimes
                    .python_for_session(
                        self.actor_session_id,
                        &self.runtime_root,
                        self.session_store(),
                    )
                    .await
            }
            "javascript" | "typescript" => {
                self.persistent_runtimes
                    .bun_for_session(
                        self.actor_session_id,
                        &self.runtime_root,
                        self.session_store(),
                    )
                    .await
            }
            _ => unreachable!("persistent runtime was validated above"),
        };
        let cancellation = cancellation.unwrap_or_default().child_token();
        let host: Arc<dyn RuntimeHost> = Arc::new(DispatcherRuntimeHost {
            session_id: self.actor_session_id,
            root: self.runtime_root.clone(),
            allow_effects: self.runtime_permission == crate::PermissionMode::FullAccess
                || workflow_approved,
            dispatcher: self.clone(),
            execution_provider: self.execution_provider(),
            host_calls: Arc::new(AtomicUsize::new(0)),
            session_store: self.session_store(),
            runtime_worker_id: runtime_worker.worker_id(),
            process_cancellation: cancellation.clone(),
        });
        let timeout_ms = match (args.timeout_ms, self.resource_limits.as_ref()) {
            (Some(requested), Some(limits)) => Some(requested.min(limits.max_runtime_execution_ms)),
            (None, Some(limits)) => Some(limits.max_runtime_execution_ms),
            (requested, None) => requested,
        };
        Ok(serde_json::to_value(
            runtime_worker
                .execute_as(runtime, &args.code, timeout_ms, host, Some(cancellation))
                .await?,
        )?)
    }
}

#[derive(Clone)]
struct DispatcherRuntimeHost {
    session_id: Uuid,
    root: PathBuf,
    allow_effects: bool,
    dispatcher: AgentToolDispatcher,
    execution_provider: Arc<dyn crate::ExecutionProvider>,
    host_calls: Arc<AtomicUsize>,
    session_store: Option<crate::SqliteSessionStore>,
    runtime_worker_id: Uuid,
    process_cancellation: CancellationToken,
}

impl DispatcherRuntimeHost {
    fn ensure_effects(&self) -> Result<()> {
        ensure!(
            self.allow_effects,
            "persistent runtime host mutation requires Full Access or an approved runtime call"
        );
        Ok(())
    }
}

#[async_trait::async_trait]
impl RuntimeHost for DispatcherRuntimeHost {
    async fn call(&self, operation: &str, arguments: Value) -> Result<Value> {
        let call_number = self.host_calls.fetch_add(1, Ordering::Relaxed) + 1;
        ensure!(
            call_number <= MAX_RUNTIME_HOST_CALLS,
            "persistent runtime exceeded the per-execution limit of {MAX_RUNTIME_HOST_CALLS} host calls"
        );
        match operation {
            "read_file" | "search_files" | "list_files" => {
                self.dispatcher
                    .read_workspace_tool(self.execution_provider.as_ref(), operation, arguments)
                    .await
            }
            "write_file" | "edit_file" => {
                self.ensure_effects()?;
                self.dispatcher
                    .mutate_workspace_tool(self.execution_provider.as_ref(), operation, arguments)
                    .await
            }
            "exec_command" => {
                self.ensure_effects()?;
                let args: RuntimeExecCommandArgs = serde_json::from_value(arguments)?;
                let output_token_limit = self.dispatcher.resource_limits.as_ref().map(|limits| {
                    usize::try_from((limits.max_workspace_command_output_bytes / 4).max(1))
                        .unwrap_or(usize::MAX)
                });
                let timeout_limit = self
                    .dispatcher
                    .resource_limits
                    .as_ref()
                    .map(|limits| limits.max_workspace_command_timeout_ms)
                    .unwrap_or(RUNTIME_MAX_COMMAND_TIMEOUT_MS);
                Ok(serde_json::to_value(
                    self.execution_provider
                        .command(crate::ExecutionCommandRequest {
                            owner_session_id: self.session_id,
                            root: self.root.clone(),
                            command: args.cmd,
                            workdir: args.workdir,
                            yield_time_ms: args.yield_time_ms,
                            max_output_tokens: args
                                .max_output_tokens
                                .map(|tokens| {
                                    output_token_limit
                                        .map(|limit| tokens.min(limit))
                                        .unwrap_or(tokens)
                                })
                                .or(output_token_limit),
                            timeout_ms: args
                                .timeout_ms
                                .unwrap_or(RUNTIME_DEFAULT_COMMAND_TIMEOUT_MS)
                                .min(timeout_limit)
                                .clamp(1, RUNTIME_MAX_COMMAND_TIMEOUT_MS),
                            journal: self.session_store.clone(),
                            environment: BTreeMap::new(),
                            cancellation: Some(self.process_cancellation.clone()),
                        })
                        .await?,
                )?)
            }
            "write_stdin" => {
                let args: RuntimeWriteStdinArgs = serde_json::from_value(arguments)?;
                Ok(serde_json::to_value(
                    self.execution_provider
                        .write_stdin(crate::ExecutionStdinRequest {
                            owner_session_id: self.session_id,
                            process_id: args.session_id,
                            chars: args.chars,
                            terminate: args.terminate.unwrap_or(false),
                            yield_time_ms: args.yield_time_ms,
                            max_output_tokens: args.max_output_tokens,
                        })
                        .await?,
                )?)
            }
            "borg_tool" => {
                let args: RuntimeBorgToolArgs = serde_json::from_value(arguments)?;
                ensure!(
                    args.name != "runtime_exec",
                    "nested runtime code calls are not supported"
                );
                if matches!(
                    args.name.as_str(),
                    "list_files"
                        | "read_file"
                        | "search_files"
                        | "write_file"
                        | "edit_file"
                        | "exec_command"
                        | "write_stdin"
                ) {
                    return self.call(&args.name, args.arguments).await;
                }
                if matches!(
                    args.name.as_str(),
                    "run_workflow" | "run_blu_workflow" | "run_blu_extension"
                ) {
                    self.ensure_effects()?;
                }
                self.dispatcher
                    .call_with_workflow_control(
                        &args.name,
                        args.arguments,
                        self.allow_effects,
                        None,
                    )
                    .await
            }
            "mcp_tools" => self.dispatcher.runtime_mcp_tools().await,
            "mcp_call" => {
                let args: RuntimeMcpCallArgs = serde_json::from_value(arguments)?;
                if args.name != "mcp__borg__search_documents" {
                    self.ensure_effects()?;
                }
                self.dispatcher
                    .runtime_mcp_call(&args.name, args.arguments)
                    .await
            }
            "history" => {
                let query: crate::SessionHistoryQuery = serde_json::from_value(arguments)?;
                let store = self
                    .session_store
                    .as_ref()
                    .context("lossless session history is unavailable for this runtime")?;
                Ok(serde_json::to_value(
                    store.query_history(self.session_id, query).await?,
                )?)
            }
            "history_index" => {
                let args: HistoryIndexArgs = serde_json::from_value(arguments)?;
                let store = self
                    .session_store
                    .as_ref()
                    .context("lossless session history is unavailable for this runtime")?;
                history_index_response(store, self.session_id, args).await
            }
            "plugin_store" => {
                let is_mutation = arguments.get("op").and_then(Value::as_str) == Some("commit");
                if is_mutation {
                    self.ensure_effects()?;
                }
                let store = self
                    .session_store
                    .as_ref()
                    .context("extension storage is unavailable for this session")?
                    .plugin_store();
                store
                    .call(self.session_id, &self.root, None, arguments)
                    .await
            }
            "retrieval_adapter" => {
                let id = arguments
                    .get("id")
                    .and_then(Value::as_str)
                    .context("retrieval adapter id is required")?;
                self.dispatcher
                    .call("read_retrieval_adapter", json!({ "id": id }))
                    .await
            }
            "harness" => {
                crate::harness::call(
                    arguments,
                    self.session_id,
                    &self.root,
                    self.session_store.as_ref(),
                    &self.dispatcher.harness_lock,
                    self.allow_effects,
                )
                .await
            }
            "runtime_status" => {
                let store = self
                    .session_store
                    .as_ref()
                    .context("durable runtime manifests are unavailable for this session")?;
                Ok(json!({
                    "manifest": store.runtime_manifest(self.session_id).await?,
                    "checkpoints": store.list_runtime_checkpoints(self.session_id, 100).await?,
                }))
            }
            "runtime_checkpoint" => {
                self.ensure_effects()?;
                let key = arguments
                    .get("key")
                    .and_then(Value::as_str)
                    .context("runtime checkpoint key is required")?;
                let state = arguments
                    .get("state")
                    .cloned()
                    .context("runtime checkpoint state is required")?;
                let store = self
                    .session_store
                    .as_ref()
                    .context("durable runtime checkpoints are unavailable for this session")?;
                Ok(serde_json::to_value(
                    store
                        .save_runtime_checkpoint(
                            self.session_id,
                            self.runtime_worker_id,
                            key,
                            &state,
                        )
                        .await?,
                )?)
            }
            "runtime_restore" => {
                let key = arguments.get("key").and_then(Value::as_str);
                let store = self
                    .session_store
                    .as_ref()
                    .context("durable runtime checkpoints are unavailable for this session")?;
                let checkpoint = store
                    .runtime_checkpoint(self.session_id, key)
                    .await?
                    .with_context(|| {
                        key.map_or_else(
                            || "runtime has no durable checkpoint".to_string(),
                            |key| format!("runtime checkpoint `{key}` does not exist"),
                        )
                    })?;
                Ok(serde_json::to_value(checkpoint)?)
            }
            other => bail!("unknown persistent runtime host operation `{other}`"),
        }
    }
}

pub(crate) async fn history_index_response(
    store: &crate::SqliteSessionStore,
    session_id: Uuid,
    args: HistoryIndexArgs,
) -> Result<Value> {
    const MAX_PAGE_BYTES: usize = 768 * 1024;
    let after_sequence = args.after_sequence.unwrap_or(0);
    let limit = args.limit.unwrap_or(1_000).clamp(1, 1_000);
    let fetched_documents = store
        .history_index_documents_after(session_id, after_sequence, limit)
        .await?;
    let fetched_count = fetched_documents.len();
    let mut documents = fetched_documents;
    let mut oversized_document = None;
    while serde_json::to_vec(&documents)?.len() > MAX_PAGE_BYTES {
        if documents.len() == 1 {
            oversized_document = documents.pop().map(|document| {
                json!({
                    "document_id": document.document_id,
                    "event_id": document.event_id,
                    "sequence": document.sequence,
                    "content_bytes": document.content.len(),
                })
            });
            break;
        }
        documents.pop();
    }
    let next_after_sequence = documents
        .last()
        .map_or(after_sequence, |document| document.sequence);
    let page_truncated = documents.len() < fetched_count;
    let has_more = page_truncated || documents.len() == limit;
    let page_bytes = serde_json::to_vec(&documents)?.len();
    Ok(json!({
        "documents": documents,
        "after_sequence": after_sequence,
        "next_after_sequence": next_after_sequence,
        "has_more": has_more,
        "page_bytes": page_bytes,
        "page_truncated": page_truncated,
        "oversized_document": oversized_document,
    }))
}

impl SharedWorkToolContext {
    async fn call(&self, name: &str, arguments: Value) -> Result<Value> {
        match name {
            "list_shared_work" => {
                let args: ListSharedWorkArgs = serde_json::from_value(arguments)?;
                let limit = args.limit.unwrap_or(200).clamp(1, 1_000);
                let events = self
                    .store
                    .replay(
                        self.workspace_id,
                        self.participant_id,
                        args.after_sequence.unwrap_or(0),
                        limit,
                    )
                    .await?
                    .into_iter()
                    .filter(|event| is_shared_work_event(&event.kind))
                    .collect::<Vec<_>>();
                Ok(json!({ "events": events }))
            }
            "create_shared_work" => {
                let args: CreateSharedWorkArgs = serde_json::from_value(arguments)?;
                let key = required_idempotency_key(&args.idempotency_key)?;
                let work = SharedWork {
                    id: self.stable_object_id("work", &key),
                    title: required_tool_text("title", &args.title)?,
                    detail: optional_tool_text(args.detail),
                };
                self.append(
                    key,
                    WorkspaceEventKind::WorkCreated {
                        work,
                        mode: DeliveryMode::Notify,
                    },
                )
                .await
            }
            "claim_shared_work" => {
                let args: ClaimSharedWorkArgs = serde_json::from_value(arguments)?;
                self.append(
                    required_idempotency_key(&args.idempotency_key)?,
                    WorkspaceEventKind::WorkClaimed {
                        claim: AtomicWorkClaim {
                            work_id: args.work_id,
                            claimant_id: self.participant_id,
                            expected_claim_id: args.expected_claim_id,
                        },
                        mode: DeliveryMode::Notify,
                    },
                )
                .await
            }
            "declare_work_dependency" => {
                let args: DeclareWorkDependencyArgs = serde_json::from_value(arguments)?;
                self.append(
                    required_idempotency_key(&args.idempotency_key)?,
                    WorkspaceEventKind::DependencyDeclared {
                        dependency: WorkDependency {
                            work_id: args.work_id,
                            depends_on_work_id: args.depends_on_work_id,
                        },
                        mode: DeliveryMode::Notify,
                    },
                )
                .await
            }
            "publish_workspace_artifact" => {
                let args: PublishWorkspaceArtifactArgs = serde_json::from_value(arguments)?;
                let key = required_idempotency_key(&args.idempotency_key)?;
                let artifact = WorkspaceArtifact {
                    id: self.stable_object_id("artifact", &key),
                    work_id: args.work_id,
                    name: required_tool_text("name", &args.name)?,
                    media_type: optional_tool_text(args.media_type),
                    uri: required_tool_text("uri", &args.uri)?,
                    content_hash: optional_tool_text(args.content_hash),
                };
                self.append(
                    key,
                    WorkspaceEventKind::ArtifactPublished {
                        artifact,
                        mode: DeliveryMode::Notify,
                    },
                )
                .await
            }
            "record_workspace_decision" => {
                let args: RecordWorkspaceDecisionArgs = serde_json::from_value(arguments)?;
                let key = required_idempotency_key(&args.idempotency_key)?;
                let decision = WorkspaceDecision {
                    id: self.stable_object_id("decision", &key),
                    subject: required_tool_text("subject", &args.subject)?,
                    outcome: required_tool_text("outcome", &args.outcome)?,
                    rationale: optional_tool_text(args.rationale),
                };
                self.append(
                    key,
                    WorkspaceEventKind::DecisionRecorded {
                        decision,
                        mode: DeliveryMode::Notify,
                    },
                )
                .await
            }
            "request_work_review" => {
                let args: RequestWorkReviewArgs = serde_json::from_value(arguments)?;
                let key = required_idempotency_key(&args.idempotency_key)?;
                let request = WorkspaceReviewRequest {
                    id: self.stable_object_id("review-request", &key),
                    work_id: args.work_id,
                    requested_reviewer_id: args.requested_reviewer_id,
                    instructions: optional_tool_text(args.instructions),
                };
                self.append(
                    key,
                    WorkspaceEventKind::ReviewRequested {
                        request,
                        mode: DeliveryMode::Notify,
                    },
                )
                .await
            }
            "record_work_review" => {
                let args: RecordWorkReviewArgs = serde_json::from_value(arguments)?;
                let review = WorkReview {
                    work_id: args.work_id,
                    reviewer_id: self.participant_id,
                    verdict: required_tool_text("verdict", &args.verdict)?,
                    detail: optional_tool_text(args.detail),
                };
                self.append(
                    required_idempotency_key(&args.idempotency_key)?,
                    WorkspaceEventKind::ReviewRecorded {
                        review,
                        mode: DeliveryMode::Notify,
                    },
                )
                .await
            }
            "add_workspace_reference" => {
                let args: AddWorkspaceReferenceArgs = serde_json::from_value(arguments)?;
                let key = required_idempotency_key(&args.idempotency_key)?;
                let reference = WorkspaceReference {
                    id: self.stable_object_id("reference", &key),
                    label: required_tool_text("label", &args.label)?,
                    target: required_tool_text("target", &args.target)?,
                };
                self.append(
                    key,
                    WorkspaceEventKind::ReferenceAdded {
                        reference,
                        mode: DeliveryMode::Notify,
                    },
                )
                .await
            }
            "record_workspace_provenance" => {
                let args: RecordWorkspaceProvenanceArgs = serde_json::from_value(arguments)?;
                let provenance = Provenance {
                    subject_id: args.subject_id,
                    source_kind: required_tool_text("source_kind", &args.source_kind)?,
                    source_id: required_tool_text("source_id", &args.source_id)?,
                    detail: optional_tool_text(args.detail),
                };
                self.append(
                    required_idempotency_key(&args.idempotency_key)?,
                    WorkspaceEventKind::ProvenanceRecorded {
                        provenance,
                        mode: DeliveryMode::Notify,
                    },
                )
                .await
            }
            other => bail!("unknown shared-work tool: {other}"),
        }
    }

    fn stable_object_id(&self, kind: &str, idempotency_key: &str) -> Uuid {
        Uuid::new_v5(
            &self.workspace_id,
            format!("{kind}:{}:{idempotency_key}", self.participant_id).as_bytes(),
        )
    }

    async fn append(&self, idempotency_key: String, kind: WorkspaceEventKind) -> Result<Value> {
        let appended = self
            .store
            .append(WorkspaceEvent {
                id: Uuid::new_v4(),
                workspace_id: self.workspace_id,
                sequence: 0,
                author_id: self.participant_id,
                idempotency_key,
                created_at: Utc::now(),
                kind,
            })
            .await?;
        Ok(serde_json::to_value(appended)?)
    }
}

fn is_shared_work_tool(name: &str) -> bool {
    matches!(
        name,
        "list_shared_work"
            | "create_shared_work"
            | "claim_shared_work"
            | "declare_work_dependency"
            | "publish_workspace_artifact"
            | "record_workspace_decision"
            | "request_work_review"
            | "record_work_review"
            | "add_workspace_reference"
            | "record_workspace_provenance"
    )
}

fn is_shared_work_event(kind: &WorkspaceEventKind) -> bool {
    matches!(
        kind,
        WorkspaceEventKind::WorkCreated { .. }
            | WorkspaceEventKind::ArtifactPublished { .. }
            | WorkspaceEventKind::DecisionRecorded { .. }
            | WorkspaceEventKind::WorkClaimed { .. }
            | WorkspaceEventKind::DependencyDeclared { .. }
            | WorkspaceEventKind::ReviewRequested { .. }
            | WorkspaceEventKind::ReviewRecorded { .. }
            | WorkspaceEventKind::ReferenceAdded { .. }
            | WorkspaceEventKind::ProvenanceRecorded { .. }
    )
}

struct SubagentEntry {
    snapshot: SubagentSnapshot,
    commands: Option<mpsc::Sender<HostCommand>>,
    inbox: Vec<TeamInboxMessage>,
    /// A delegation has selected this ready worker but its actor has not yet
    /// projected the next status boundary. This closes the double-assignment
    /// window without misreporting the durable lifecycle state.
    assignment_claimed: bool,
    /// Restored children are metadata-only until an explicit child-directed
    /// action wakes them. This prevents resuming an idle root from silently
    /// starting providers in the background.
    dormant: bool,
}

struct SubagentTable {
    root_session_id: Uuid,
    max_children: usize,
    entries: HashMap<Uuid, SubagentEntry>,
    task_names: HashMap<String, Uuid>,
}

impl SubagentTable {
    fn reserve(&mut self, task_name: &str, launch: &LaunchSession) -> Result<SubagentSnapshot> {
        let task_name = canonical_task_name(task_name)?;
        if self.task_names.contains_key(&task_name) {
            bail!("subagent task name already exists: {task_name}");
        }
        let active = self
            .entries
            .values()
            .filter(|entry| entry.snapshot.status.consumes_concurrency_slot() && !entry.dormant)
            .count();
        if active >= self.max_children {
            bail!("subagent concurrency limit reached ({})", self.max_children);
        }
        let now = Utc::now();
        let snapshot = SubagentSnapshot {
            session_id: Uuid::new_v4(),
            parent_session_id: self.root_session_id,
            task_name: task_name.clone(),
            status: SubagentStatus::Starting,
            provider: launch.provider,
            model: launch.model.clone(),
            effort: launch.effort.clone(),
            cwd: launch.cwd.clone(),
            created_at: now,
            updated_at: now,
            detail: None,
            final_text: None,
            usage: SubagentUsage::default(),
        };
        self.task_names.insert(task_name, snapshot.session_id);
        self.entries.insert(
            snapshot.session_id,
            SubagentEntry {
                snapshot: snapshot.clone(),
                commands: None,
                inbox: Vec::new(),
                assignment_claimed: false,
                dormant: false,
            },
        );
        Ok(snapshot)
    }

    fn resolve(&self, target: &str) -> Result<Uuid> {
        let target = target.trim();
        if target == "/root" || target == "root" {
            return Ok(self.root_session_id);
        }
        let session_id = target.strip_prefix("session:").unwrap_or(target);
        if let Ok(id) = Uuid::parse_str(session_id)
            && (id == self.root_session_id || self.entries.contains_key(&id))
        {
            return Ok(id);
        }
        let canonical = if target.starts_with('/') {
            target.to_string()
        } else {
            format!("/root/{target}")
        };
        self.task_names
            .get(&canonical)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("unknown subagent target: {target}"))
    }

    fn task_name(&self, session_id: Uuid) -> Result<String> {
        if session_id == self.root_session_id {
            return Ok("/root".to_string());
        }
        self.entries
            .get(&session_id)
            .map(|entry| entry.snapshot.task_name.clone())
            .ok_or_else(|| anyhow::anyhow!("session {session_id} is not part of this agent team"))
    }

    fn snapshots(&self) -> Vec<SubagentSnapshot> {
        let mut agents = self
            .entries
            .values()
            .map(|entry| entry.snapshot.clone())
            .collect::<Vec<_>>();
        agents.sort_by(|left, right| left.task_name.cmp(&right.task_name));
        agents
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TeamInboxMessage {
    pub message_id: Uuid,
    pub text: String,
    pub report_text: String,
    pub sender_session_id: Uuid,
    pub delivery: PromptDelivery,
}

#[derive(Debug, Clone, Default)]
pub struct TeamMessageOptions {
    pub mentions: Vec<StructuredMention>,
    pub reply_to_message_id: Option<Uuid>,
}

struct RoutedTeamMessage {
    receipt: Option<WorkspaceMessageReceipt>,
    dispatched_locally: bool,
    relay_pending: bool,
}

#[derive(Clone)]
enum PeerConsultationOutcome {
    Completed(String),
    Failed(String),
}

#[derive(Debug, Clone)]
pub struct PeerRotation {
    pub archived: Option<SubagentSnapshot>,
    pub replacement: SubagentSnapshot,
}

/// Borg-native child sessions for one root CLI session.
///
/// Each child reuses the canonical session actor with its own provider context
/// and store identity. This layer only owns topology, bounded admission, messaging,
/// and the event projection consumed by terminal and Remote adapters.
#[derive(Clone)]
pub struct SubagentCoordinator {
    journal_root: PathBuf,
    root_session_id: Uuid,
    root_launch: LaunchSession,
    executor: Arc<dyn crate::AgentTurnExecutor>,
    store: Arc<dyn SessionStore>,
    workspace_store: Arc<OnceCell<SqliteWorkspaceStore>>,
    table: Arc<Mutex<SubagentTable>>,
    activity_tx: broadcast::Sender<SubagentActivity>,
    root_inbox: Arc<Mutex<Vec<TeamInboxMessage>>>,
    root_message_tx: broadcast::Sender<TeamInboxMessage>,
    root_message_dispatches: Arc<Mutex<HashMap<Uuid, Instant>>>,
    projected_root_messages: Arc<Mutex<HashSet<Uuid>>>,
    consultation_lock: Arc<Mutex<()>>,
}

impl SubagentCoordinator {
    pub fn new_with_store_and_executor(
        journal_root: impl Into<PathBuf>,
        root_session_id: Uuid,
        root_launch: LaunchSession,
        max_children: usize,
        executor: Arc<dyn crate::AgentTurnExecutor>,
        store: Arc<dyn SessionStore>,
    ) -> Result<Self> {
        if max_children == 0 {
            bail!("subagent concurrency limit must be positive");
        }
        let (activity_tx, _) = broadcast::channel(512);
        let (root_message_tx, _) = broadcast::channel(128);
        Ok(Self {
            journal_root: journal_root.into(),
            root_session_id,
            root_launch,
            executor,
            store,
            workspace_store: Arc::new(OnceCell::new()),
            table: Arc::new(Mutex::new(SubagentTable {
                root_session_id,
                max_children,
                entries: HashMap::new(),
                task_names: HashMap::new(),
            })),
            activity_tx,
            root_inbox: Arc::new(Mutex::new(Vec::new())),
            root_message_tx,
            root_message_dispatches: Arc::new(Mutex::new(HashMap::new())),
            projected_root_messages: Arc::new(Mutex::new(HashSet::new())),
            consultation_lock: Arc::new(Mutex::new(())),
        })
    }

    pub(crate) fn is_root_session(&self, session_id: Uuid) -> bool {
        self.root_session_id == session_id
    }

    pub fn subscribe(&self) -> broadcast::Receiver<SubagentActivity> {
        self.activity_tx.subscribe()
    }

    pub(crate) fn subscribe_root_messages(&self) -> broadcast::Receiver<TeamInboxMessage> {
        self.root_message_tx.subscribe()
    }

    pub(crate) async fn take_root_inbox(&self) -> Vec<TeamInboxMessage> {
        std::mem::take(&mut *self.root_inbox.lock().await)
    }

    async fn broadcast_root_message(&self, message: TeamInboxMessage) {
        match self.root_message_tx.send(message.clone()) {
            Ok(_) => {
                self.root_message_dispatches
                    .lock()
                    .await
                    .insert(message.message_id, Instant::now());
                self.root_inbox
                    .lock()
                    .await
                    .retain(|queued| queued.message_id != message.message_id);
            }
            Err(error) => {
                self.root_inbox.lock().await.push(error.0);
            }
        }
    }

    /// Re-emit durable wake/boundary messages that could not reach the root's
    /// in-memory receiver. Queue/next-turn messages remain dormant until the
    /// root explicitly reaches its next normal boundary.
    pub(crate) async fn wake_pending_root_messages(&self) {
        let messages = {
            let dispatched = self.root_message_dispatches.lock().await;
            let mut inbox = self.root_inbox.lock().await;
            let mut wake = Vec::new();
            let mut retained = Vec::with_capacity(inbox.len());
            for message in inbox.drain(..) {
                if message.delivery == PromptDelivery::Steer
                    && !dispatched.contains_key(&message.message_id)
                {
                    wake.push(message);
                } else {
                    retained.push(message);
                }
            }
            *inbox = retained;
            wake
        };
        for message in messages {
            self.broadcast_root_message(message).await;
        }
    }

    async fn workspace_store(&self) -> Result<&SqliteWorkspaceStore> {
        self.workspace_store
            .get_or_try_init(|| async {
                self.store.workspace_store().await?.with_context(
                    || "subagent multiplayer requires the canonical SQLite workspace projection",
                )
            })
            .await
    }

    /// Reconcile durable workspace deliveries into the director's local
    /// inbox and return any child reports that still need a root transcript
    /// projection. This is deliberately pollable: agent MCP calls may run
    /// outside the director process, where an in-memory broadcast cannot wake
    /// or project into the root session.
    pub(crate) async fn refresh_root_inbox_reports(&self) -> Result<Vec<(Uuid, SubagentActivity)>> {
        let root_session_id = self.table.lock().await.root_session_id;
        let pending = self.pending_messages_for_session(root_session_id).await?;
        let projected = self.projected_root_messages.lock().await.clone();
        let mut reports = Vec::new();
        for message in pending {
            if self
                .store
                .contains_message(root_session_id, message.message_id)
                .await?
            {
                self.root_message_dispatches
                    .lock()
                    .await
                    .remove(&message.message_id);
                self.root_inbox
                    .lock()
                    .await
                    .retain(|queued| queued.message_id != message.message_id);
                self.acknowledge_message_for_session(root_session_id, message.message_id)
                    .await?;
                continue;
            }
            let dispatch_is_recent = self
                .root_message_dispatches
                .lock()
                .await
                .get(&message.message_id)
                .is_some_and(|sent_at| sent_at.elapsed() < ROOT_MESSAGE_RETRY_INTERVAL);
            if dispatch_is_recent {
                continue;
            }
            self.root_message_dispatches
                .lock()
                .await
                .remove(&message.message_id);
            {
                let mut inbox = self.root_inbox.lock().await;
                if !inbox
                    .iter()
                    .any(|queued| queued.message_id == message.message_id)
                {
                    inbox.push(message.clone());
                }
            }
            if projected.contains(&message.message_id) {
                continue;
            }
            let Some(agent) = self.get(message.sender_session_id).await else {
                continue;
            };
            if agent.session_id == root_session_id {
                continue;
            }
            reports.push((
                message.message_id,
                SubagentActivity::SessionEvent {
                    parent_session_id: root_session_id,
                    task_name: agent.task_name.clone(),
                    event: SessionEvent::new(
                        agent.session_id,
                        0,
                        SessionEventKind::Message {
                            message_id: message.message_id,
                            actor: crate::EventActor::Assistant,
                            text: message.report_text.clone(),
                            attachments: Vec::new(),
                            status: MessageStatus::Complete,
                            delivery: None,
                        },
                    ),
                },
            ));
        }
        Ok(reports)
    }

    pub(crate) async fn mark_root_message_projected(&self, message_id: Uuid) {
        self.projected_root_messages.lock().await.insert(message_id);
    }

    pub(crate) async fn root_message_is_projected(&self, message_id: Uuid) -> bool {
        self.projected_root_messages
            .lock()
            .await
            .contains(&message_id)
    }

    // Message persistence keeps sender, recipient, admission, and audience
    // metadata explicit so none can be inferred incorrectly during replay.
    #[allow(clippy::too_many_arguments)]
    async fn persist_team_message(
        &self,
        actor_session_id: Uuid,
        recipient_session_id: Uuid,
        actor: &str,
        message: &str,
        prompt_delivery: PromptDelivery,
        delivery_mode: DeliveryMode,
        options: TeamMessageOptions,
    ) -> Result<(TeamInboxMessage, Option<WorkspaceMessageReceipt>)> {
        if !self.root_launch.capabilities.multiplayer {
            return Ok((
                TeamInboxMessage {
                    message_id: Uuid::new_v4(),
                    text: attributed_team_message(actor, message),
                    report_text: message.to_string(),
                    sender_session_id: actor_session_id,
                    delivery: prompt_delivery,
                },
                None,
            ));
        }
        let actor_binding = self
            .store
            .workspace_binding(actor_session_id)
            .await?
            .with_context(|| format!("team sender session {actor_session_id} has no workspace"))?;
        let recipient_binding = self
            .store
            .workspace_binding(recipient_session_id)
            .await?
            .with_context(|| {
                format!("team recipient session {recipient_session_id} has no workspace")
            })?;
        let workspace_store = self.workspace_store().await?;
        let workspace_id = if actor_binding.workspace_id == recipient_binding.workspace_id {
            actor_binding.workspace_id
        } else {
            workspace_store
                .ensure_direct_workspace(
                    actor_binding.participant_id,
                    recipient_binding.participant_id,
                )
                .await?
        };
        let text = attributed_team_message(actor, message);
        let idempotency_id = Uuid::new_v4();
        let receipt = workspace_store
            .append_message(NewWorkspaceMessage {
                workspace_id,
                author_id: actor_binding.participant_id,
                text: message.to_string(),
                mentions: options.mentions,
                audience: Audience::Direct {
                    participant: recipient_binding.participant_id,
                },
                mode: delivery_mode,
                thread_id: None,
                reply_to_message_id: options.reply_to_message_id,
                idempotency_key: format!("team-message:{idempotency_id}"),
            })
            .await?;
        Ok((
            TeamInboxMessage {
                message_id: receipt.message_id,
                text,
                report_text: message.to_string(),
                sender_session_id: actor_session_id,
                delivery: prompt_delivery,
            },
            Some(receipt),
        ))
    }

    async fn pending_messages_for_session(
        &self,
        session_id: Uuid,
    ) -> Result<Vec<TeamInboxMessage>> {
        if !self.root_launch.capabilities.multiplayer {
            return Ok(Vec::new());
        }
        let binding = self
            .store
            .workspace_binding(session_id)
            .await?
            .with_context(|| format!("team session {session_id} has no workspace"))?;
        let workspace_store = self.workspace_store().await?;
        let workspaces = workspace_store
            .list_workspaces_for_participant(binding.participant_id)
            .await?;
        let mut messages = Vec::new();
        for workspace in workspaces {
            let pending = workspace_store
                .pending_message_events(workspace.id, binding.participant_id, 10_000)
                .await?;
            for (event, delivery) in pending {
                let ordering = (event.created_at, event.workspace_id, event.sequence);
                let WorkspaceEventKind::Message { message, .. } = event.kind else {
                    continue;
                };
                let actor = match self.task_name_for_session(message.author_id).await {
                    Ok(task_name) => task_name,
                    Err(_) => workspace_store
                        .participant(message.author_id)
                        .await?
                        .map(|participant| {
                            format!("{} ({})", participant.display_name, message.author_id)
                        })
                        .unwrap_or_else(|| message.author_id.to_string()),
                };
                messages.push((
                    ordering,
                    TeamInboxMessage {
                        message_id: message.id,
                        text: attributed_team_message(&actor, &message.body.text),
                        report_text: message.body.text,
                        sender_session_id: message.author_id,
                        delivery: match delivery.mode {
                            DeliveryMode::Boundary | DeliveryMode::Wake => PromptDelivery::Steer,
                            DeliveryMode::NextTurn | DeliveryMode::Notify => PromptDelivery::Queue,
                        },
                    },
                ));
            }
        }
        messages.sort_by_key(|(ordering, _)| *ordering);
        Ok(messages.into_iter().map(|(_, message)| message).collect())
    }

    pub(crate) async fn unread_messages_for_session(
        &self,
        session_id: Uuid,
    ) -> Result<Vec<TeamInboxMessage>> {
        self.pending_messages_for_session(session_id).await
    }

    pub async fn acknowledge_message_for_session(
        &self,
        session_id: Uuid,
        message_id: Uuid,
    ) -> Result<()> {
        anyhow::ensure!(
            self.root_launch.capabilities.multiplayer,
            "team acknowledgements require multiplayer capability"
        );
        let binding = self
            .store
            .workspace_binding(session_id)
            .await?
            .context("team session has no workspace")?;
        let store = self.workspace_store().await?;
        let mut addressed_workspace_id = None;
        for workspace in store
            .list_workspaces_for_participant(binding.participant_id)
            .await?
        {
            if store
                .pending_message_events(workspace.id, binding.participant_id, 10_000)
                .await?
                .into_iter()
                .any(|(event, _)| matches!(&event.kind, WorkspaceEventKind::Message { message, .. } if message.id == message_id))
            {
                addressed_workspace_id = Some(workspace.id);
                break;
            }
        }
        let workspace_id = addressed_workspace_id.context("unread team message not found")?;
        store
            .transition_message_delivery(
                workspace_id,
                message_id,
                binding.participant_id,
                crate::DeliveryState::Admitted,
                None,
            )
            .await?;
        store
            .transition_message_delivery(
                workspace_id,
                message_id,
                binding.participant_id,
                crate::DeliveryState::Acknowledged,
                None,
            )
            .await?;
        Ok(())
    }

    /// Rebuild the coordinator projection from the durable parent event
    /// stream without starting child actors.
    ///
    /// Parent `SubagentActivity` events remain the topology authority; child
    /// projections only supply each child actor's conversational state.
    pub async fn restore_from_events(
        &self,
        events: &[SessionEvent],
    ) -> Result<Vec<SubagentActivity>> {
        let mut latest = HashMap::<Uuid, SubagentSnapshot>::new();
        let mut projected_root_messages = HashSet::new();
        for event in events {
            if let SessionEventKind::SubagentActivity {
                agent,
                event: child_event,
                ..
            } = &event.kind
            {
                latest.insert(agent.session_id, agent.clone());
                if let Some(child_event) = child_event
                    && let SessionEventKind::Message {
                        message_id,
                        status: MessageStatus::Complete,
                        ..
                    } = &child_event.kind
                {
                    projected_root_messages.insert(*message_id);
                }
            }
        }
        *self.projected_root_messages.lock().await = projected_root_messages;
        let mut recovery_updates = Vec::new();
        let root_session_id = self.table.lock().await.root_session_id;
        for mut snapshot in latest.into_values() {
            if snapshot.parent_session_id != root_session_id {
                continue;
            }
            let mirrored_status = snapshot.status;
            let actor_path = child_lock_path(&self.journal_root, snapshot.session_id);
            let mut recovery_failed = false;
            if !snapshot.status.is_terminal() {
                let recovered = async {
                    let _writer = crate::SessionWriterLease::try_acquire(&actor_path)?
                        .with_context(|| {
                            format!("subagent session {} is already active", snapshot.session_id)
                        })?;
                    self.store
                        .register_child_session(root_session_id, snapshot.session_id)
                        .await?;
                    self.store.state(snapshot.session_id).await
                }
                .await;
                match recovered {
                    Ok(state) if state.latest_sequence > 0 => {
                        project_child_state(&mut snapshot, &state);
                        if !snapshot.status.is_terminal() {
                            snapshot.status = SubagentStatus::Ready;
                            snapshot.detail = Some(
                                "Paused with the parent session; follow up to wake".to_string(),
                            );
                        }
                    }
                    Ok(_) => {
                        snapshot.status = SubagentStatus::Failed;
                        snapshot.updated_at = Utc::now();
                        snapshot.detail = Some("child session is unavailable after restart".into());
                        recovery_failed = true;
                    }
                    Err(error) => {
                        snapshot.status = SubagentStatus::Failed;
                        snapshot.updated_at = Utc::now();
                        snapshot.detail =
                            Some(format!("child session cannot be recovered: {error:#}"));
                        recovery_failed = true;
                    }
                }
            }
            {
                let mut table = self.table.lock().await;
                table
                    .task_names
                    .insert(snapshot.task_name.clone(), snapshot.session_id);
                table.entries.insert(
                    snapshot.session_id,
                    SubagentEntry {
                        snapshot: snapshot.clone(),
                        commands: None,
                        inbox: Vec::new(),
                        assignment_claimed: false,
                        dormant: !snapshot.status.is_terminal() && !recovery_failed,
                    },
                );
            }
            if snapshot.status != mirrored_status {
                let update = match snapshot.status {
                    SubagentStatus::Ready => Some(SubagentActivity::Completed {
                        agent: snapshot.clone(),
                    }),
                    SubagentStatus::Stopped => Some(SubagentActivity::Stopped {
                        agent: snapshot.clone(),
                    }),
                    SubagentStatus::Failed => Some(SubagentActivity::Failed {
                        agent: snapshot.clone(),
                    }),
                    SubagentStatus::Starting
                    | SubagentStatus::Running
                    | SubagentStatus::WaitingForApproval => None,
                };
                recovery_updates.extend(update);
            }
        }
        for message in self.pending_messages_for_session(root_session_id).await? {
            if self
                .store
                .contains_message(root_session_id, message.message_id)
                .await?
            {
                self.acknowledge_message_for_session(root_session_id, message.message_id)
                    .await?;
                continue;
            }
            // A restart is not a delivery action. Preserve every pending root
            // message for the next explicit root turn instead of letting an
            // old wake/boundary delivery start work behind an idle TUI.
            self.root_inbox.lock().await.push(message);
        }
        let child_ids = self
            .table
            .lock()
            .await
            .entries
            .values()
            .filter(|entry| !entry.snapshot.status.is_terminal())
            .map(|entry| entry.snapshot.session_id)
            .collect::<Vec<_>>();
        for child_id in child_ids {
            let messages = self.pending_messages_for_session(child_id).await?;
            if messages.is_empty() {
                continue;
            }
            let mut fresh_messages = Vec::with_capacity(messages.len());
            for message in messages {
                if self
                    .store
                    .contains_message(child_id, message.message_id)
                    .await?
                {
                    self.acknowledge_message_for_session(child_id, message.message_id)
                        .await?;
                } else {
                    fresh_messages.push(message);
                }
            }
            if fresh_messages.is_empty() {
                continue;
            }
            let mut table = self.table.lock().await;
            let Some(entry) = table.entries.get_mut(&child_id) else {
                continue;
            };
            entry.inbox.extend(fresh_messages);
        }
        // The child ledger is authoritative. The root actor durably records
        // these corrections before it publishes its initial Ready boundary;
        // returning them avoids a best-effort broadcast race during restore.
        Ok(recovery_updates)
    }

    pub async fn spawn(&self, request: SpawnSubagent) -> Result<SubagentSnapshot> {
        let launch = self.subagent_launch(&request)?;
        self.spawn_with_launch(&request.task_name, launch).await
    }

    fn subagent_launch(&self, request: &SpawnSubagent) -> Result<LaunchSession> {
        let message = required_message(&request.message)?;
        let mut launch = self.root_launch.clone();
        launch.request_id = Uuid::new_v4();
        launch.initial_prompt = Some(message);
        let parent_provider = launch.provider;
        launch.provider = request.provider.unwrap_or(parent_provider);
        ensure_provider_can_spawn(&launch, launch.provider)?;
        validate_subagent_overrides(
            launch.provider,
            request.model.as_deref(),
            request.effort.as_deref(),
        )?;
        if launch.provider != parent_provider {
            launch.model = request
                .model
                .clone()
                .or_else(|| default_model_for_cross_provider_peer(launch.provider));
            launch.effort = request
                .effort
                .clone()
                .or_else(|| default_effort_for_cross_provider_peer(launch.provider));
        } else {
            if request.model.is_some() {
                launch.model = request.model.clone();
            }
            launch.effort = effective_worker_effort(&launch, request.effort.clone());
        }
        anyhow::ensure!(
            !launch.provider.uses_native_harness() || launch.model.is_some(),
            "{:?} peer requires an explicit model",
            launch.provider
        );
        launch.name = Some(canonical_task_name(&request.task_name)?);
        Ok(launch)
    }

    async fn spawn_with_launch(
        &self,
        task_name: &str,
        launch: LaunchSession,
    ) -> Result<SubagentSnapshot> {
        let snapshot = self.table.lock().await.reserve(task_name, &launch)?;
        self.start_reserved(snapshot.clone(), launch, true).await?;
        Ok(snapshot)
    }

    async fn assign_task_as(
        &self,
        actor_session_id: Uuid,
        request: SpawnSubagent,
    ) -> Result<Value> {
        let launch = self.subagent_launch(&request)?;
        let assignment_name = launch
            .name
            .as_deref()
            .expect("subagent launch has a canonical task name")
            .to_string();
        let claimed = {
            let mut table = self.table.lock().await;
            anyhow::ensure!(
                !table.task_names.contains_key(&assignment_name),
                "subagent task name already exists: {assignment_name}"
            );
            table
                .entries
                .values_mut()
                .filter(|entry| {
                    entry.snapshot.status == SubagentStatus::Ready
                        && !entry.assignment_claimed
                        && !is_persistent_peer_lane(&entry.snapshot.task_name)
                        && entry.snapshot.provider == launch.provider
                        && entry.snapshot.model == launch.model
                        && entry.snapshot.effort == launch.effort
                })
                .min_by_key(|entry| entry.snapshot.updated_at)
                .map(|entry| {
                    entry.assignment_claimed = true;
                    entry.snapshot.updated_at = Utc::now();
                    entry.snapshot.detail = Some(format!("Assigned new task {assignment_name}"));
                    entry.snapshot.clone()
                })
        };

        if let Some(claimed) = claimed {
            let target = format!("session:{}", claimed.session_id);
            if let Err(error) = self
                .route_followup_task_with_options_as(
                    actor_session_id,
                    &target,
                    launch
                        .initial_prompt
                        .as_deref()
                        .expect("subagent launch has an initial prompt"),
                    TeamMessageOptions::default(),
                )
                .await
            {
                let mut table = self.table.lock().await;
                if let Some(entry) = table.entries.get_mut(&claimed.session_id)
                    && entry.assignment_claimed
                {
                    entry.assignment_claimed = false;
                    entry.snapshot.updated_at = Utc::now();
                    entry.snapshot.detail =
                        Some(format!("Automatic task assignment failed: {error:#}"));
                }
                return Err(error);
            }
            let mut value =
                serde_json::to_value(self.get(claimed.session_id).await.unwrap_or(claimed))?;
            value["reused"] = Value::Bool(true);
            value["assignment_task_name"] = Value::String(assignment_name);
            return Ok(value);
        }

        let agent = self.spawn_with_launch(&request.task_name, launch).await?;
        let mut value = serde_json::to_value(agent)?;
        value["reused"] = Value::Bool(false);
        value["assignment_task_name"] = Value::String(assignment_name);
        Ok(value)
    }

    /// Return the durable child for a provider-specific sidecar, creating it
    /// without an initial model turn when this is the first request. The task
    /// name is deliberately deterministic so every future `/claude`, `/gpt`,
    /// or `/peer` command resolves to the same child session after hydration.
    pub async fn ensure_sidecar(
        &self,
        task_name: &str,
        provider: CodingProvider,
        model: Option<String>,
        effort: Option<String>,
    ) -> Result<SubagentSnapshot> {
        ensure_provider_can_spawn(&self.root_launch, provider)?;
        let task_name = canonical_task_name(task_name)?;
        let existing = {
            let table = self.table.lock().await;
            table
                .task_names
                .get(&task_name)
                .and_then(|session_id| table.entries.get(session_id))
                .map(|entry| entry.snapshot.clone())
        };
        if let Some(snapshot) = existing {
            anyhow::ensure!(
                snapshot.provider == provider,
                "sidecar {} is pinned to {}, not {}",
                snapshot.task_name,
                snapshot.provider.label(),
                provider.label()
            );
            if let Some(model) = model.as_deref() {
                anyhow::ensure!(
                    snapshot.model.as_deref() == Some(model),
                    "sidecar {} is pinned to model {}, not {}",
                    snapshot.task_name,
                    snapshot.model.as_deref().unwrap_or("<none>"),
                    model
                );
            }
            if let Some(effort) = effort.as_deref() {
                anyhow::ensure!(
                    snapshot.effort.as_deref() == Some(effort),
                    "sidecar {} is pinned to effort {}, not {}",
                    snapshot.task_name,
                    snapshot.effort.as_deref().unwrap_or("<none>"),
                    effort
                );
            }
            if snapshot.status.is_terminal() {
                let mut revived = snapshot.clone();
                revived.status = SubagentStatus::Starting;
                revived.detail = Some("Waking persistent peer after resume".to_string());
                revived.updated_at = Utc::now();
                let mut launch = self.root_launch.clone();
                launch.request_id = Uuid::new_v4();
                launch.initial_prompt = None;
                launch.provider = snapshot.provider;
                launch.model = snapshot.model.clone();
                launch.effort = snapshot.effort.clone();
                launch.cwd = snapshot.cwd.clone();
                launch.name = Some(snapshot.task_name.clone());
                {
                    let mut table = self.table.lock().await;
                    let entry = table
                        .entries
                        .get_mut(&snapshot.session_id)
                        .expect("existing sidecar remains in the coordinator");
                    entry.snapshot = revived.clone();
                    entry.commands = None;
                    entry.dormant = false;
                }
                if let Err(error) = self.start_reserved(revived, launch, true).await {
                    let mut table = self.table.lock().await;
                    if let Some(entry) = table.entries.get_mut(&snapshot.session_id) {
                        entry.snapshot.status = SubagentStatus::Stopped;
                        entry.snapshot.detail = Some(format!("Could not wake: {error:#}"));
                        entry.snapshot.updated_at = Utc::now();
                        entry.dormant = false;
                    }
                    return Err(error);
                }
                return Ok(self
                    .get(snapshot.session_id)
                    .await
                    .expect("revived sidecar remains in the coordinator"));
            }
            self.ensure_child_actor(snapshot.session_id).await?;
            return Ok(self
                .get(snapshot.session_id)
                .await
                .expect("ensured sidecar remains in the coordinator"));
        }

        let mut launch = self.root_launch.clone();
        launch.request_id = Uuid::new_v4();
        launch.initial_prompt = None;
        launch.provider = provider;
        launch.model = model.or_else(|| default_model_for_cross_provider_peer(provider));
        launch.effort = effort.or_else(|| default_effort_for_cross_provider_peer(provider));
        validate_subagent_overrides(
            launch.provider,
            launch.model.as_deref(),
            launch.effort.as_deref(),
        )?;
        anyhow::ensure!(
            !launch.provider.uses_native_harness() || launch.model.is_some(),
            "{} sidecar requires an explicit model",
            provider.label()
        );
        launch.name = Some(task_name.clone());
        let snapshot = self
            .table
            .lock()
            .await
            .reserve(task_name.trim_start_matches("/root/"), &launch)?;
        self.start_reserved(snapshot.clone(), launch, true).await?;
        Ok(snapshot)
    }

    /// Replace a deterministic sidecar lane with a fresh child. The previous
    /// child keeps its UUID, journal, usage, and transcript, but is moved to a
    /// unique archived task name so hydration cannot confuse it with the new
    /// `/root/{task_name}` mapping.
    pub async fn rotate_sidecar(
        &self,
        task_name: &str,
        provider: CodingProvider,
        model: Option<String>,
        effort: Option<String>,
    ) -> Result<SubagentSnapshot> {
        let _consultation_guard = self.consultation_lock.lock().await;
        Ok(self
            .rotate_sidecar_locked(task_name, provider, model, effort)
            .await?
            .replacement)
    }

    async fn rotate_sidecar_locked(
        &self,
        task_name: &str,
        provider: CodingProvider,
        model: Option<String>,
        effort: Option<String>,
    ) -> Result<PeerRotation> {
        ensure_provider_can_spawn(&self.root_launch, provider)?;
        let task_name = canonical_sidecar_task_name(task_name)?;
        let launch = self.sidecar_launch(&task_name, provider, model, effort)?;
        let existing = {
            let table = self.table.lock().await;
            table
                .task_names
                .get(&task_name)
                .and_then(|session_id| table.entries.get(session_id))
                .map(|entry| entry.snapshot.clone())
        };
        if let Some(snapshot) = existing.as_ref() {
            anyhow::ensure!(
                snapshot.provider == provider,
                "sidecar {} is pinned to {}, not {}",
                snapshot.task_name,
                snapshot.provider.label(),
                provider.label()
            );
            self.stop_and_wait(snapshot.session_id).await?;
        }

        let archived = if let Some(snapshot) = existing.as_ref() {
            let archived_name = format!("/root/peer_archive_{}", snapshot.session_id.simple());
            let archived = {
                let mut table = self.table.lock().await;
                let entry = table
                    .entries
                    .get_mut(&snapshot.session_id)
                    .expect("sidecar still exists while rotating");
                let mut archived = entry.snapshot.clone();
                archived.task_name = archived_name.clone();
                if archived.status != SubagentStatus::Failed {
                    archived.status = SubagentStatus::Stopped;
                }
                archived.detail = Some(format!(
                    "Archived persistent peer; replaced at {}",
                    task_name
                ));
                archived.updated_at = Utc::now();
                entry.snapshot = archived.clone();
                entry.commands = None;
                entry.dormant = false;
                table.task_names.remove(&task_name);
                table
                    .task_names
                    .insert(archived_name.clone(), snapshot.session_id);
                archived
            };
            Some(archived)
        } else {
            None
        };

        let replacement = {
            let mut table = self.table.lock().await;
            let reserved = table.reserve(
                task_name
                    .strip_prefix("/root/")
                    .expect("canonical sidecar task name"),
                &launch,
            );
            match reserved {
                Ok(replacement) => replacement,
                Err(error) => {
                    table.task_names.remove(&task_name);
                    if let Some(previous) = existing.as_ref() {
                        let previous_id = previous.session_id;
                        let archived_name = format!("/root/peer_archive_{}", previous_id.simple());
                        let mut restored = previous.clone();
                        restored.task_name = task_name.clone();
                        restored.status = SubagentStatus::Stopped;
                        restored.detail = Some(format!("Peer rotation failed: {error:#}"));
                        restored.updated_at = Utc::now();
                        let entry = table
                            .entries
                            .get_mut(&previous_id)
                            .expect("archived sidecar still exists after reservation failure");
                        entry.snapshot = restored;
                        entry.commands = None;
                        entry.dormant = false;
                        table.task_names.remove(&archived_name);
                        table.task_names.insert(task_name.clone(), previous_id);
                    }
                    return Err(error);
                }
            }
        };
        if let Err(error) = self
            .start_reserved(replacement.clone(), launch, false)
            .await
        {
            let mut table = self.table.lock().await;
            table.task_names.remove(&task_name);
            table.entries.remove(&replacement.session_id);
            if let Some(previous) = existing {
                let previous_id = previous.session_id;
                let archived_name = format!("/root/peer_archive_{}", previous_id.simple());
                let mut restored = previous;
                restored.task_name = task_name.clone();
                restored.status = SubagentStatus::Stopped;
                restored.detail = Some(format!("Peer rotation failed: {error:#}"));
                restored.updated_at = Utc::now();
                let entry = table
                    .entries
                    .get_mut(&previous_id)
                    .expect("archived sidecar still exists after replacement failure");
                entry.snapshot = restored;
                entry.commands = None;
                entry.dormant = false;
                table.task_names.remove(&archived_name);
                table.task_names.insert(task_name, previous_id);
            }
            return Err(error);
        }

        if let Some(archived) = archived.clone() {
            let _ = self
                .activity_tx
                .send(SubagentActivity::Stopped { agent: archived });
        }
        let _ = self.activity_tx.send(SubagentActivity::Started {
            agent: replacement.clone(),
        });
        Ok(PeerRotation {
            archived,
            replacement,
        })
    }

    fn sidecar_launch(
        &self,
        task_name: &str,
        provider: CodingProvider,
        model: Option<String>,
        effort: Option<String>,
    ) -> Result<LaunchSession> {
        let mut launch = self.root_launch.clone();
        launch.request_id = Uuid::new_v4();
        launch.initial_prompt = None;
        launch.provider = provider;
        launch.model = model.or_else(|| default_model_for_cross_provider_peer(provider));
        launch.effort = effort.or_else(|| default_effort_for_cross_provider_peer(provider));
        validate_subagent_overrides(
            launch.provider,
            launch.model.as_deref(),
            launch.effort.as_deref(),
        )?;
        anyhow::ensure!(
            !launch.provider.uses_native_harness() || launch.model.is_some(),
            "{} sidecar requires an explicit model",
            provider.label()
        );
        launch.name = Some(task_name.to_string());
        Ok(launch)
    }

    async fn stop_and_wait(&self, session_id: Uuid) -> Result<()> {
        let deadline = tokio::time::Instant::now() + SIDECAR_STOP_TIMEOUT;
        let mut stop_sent = false;
        loop {
            let state = {
                let table = self.table.lock().await;
                let entry = table
                    .entries
                    .get(&session_id)
                    .with_context(|| format!("unknown sidecar session {session_id}"))?;
                (
                    entry.commands.clone(),
                    entry.snapshot.status.is_terminal(),
                    entry.dormant,
                    entry.snapshot.task_name.clone(),
                )
            };
            if state.0.is_none() && (state.1 || state.2) {
                return Ok(());
            }
            if let Some(commands) = state.0
                && !stop_sent
            {
                commands
                    .send(HostCommand::Stop { session_id })
                    .await
                    .map_err(|_| anyhow::anyhow!("{} peer command channel closed", state.3))?;
                stop_sent = true;
            }
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "{} peer did not stop within {} seconds",
                state.3,
                SIDECAR_STOP_TIMEOUT.as_secs()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Rotate a persistent peer from an agent-facing control. An optional
    /// handoff is queued as the first prompt in the replacement thread after
    /// the archive/new-child boundary is durable.
    pub async fn rotate_peer(
        &self,
        parent_provider: CodingProvider,
        profile: Option<&str>,
        handoff: Option<&str>,
    ) -> Result<Value> {
        let handoff = handoff
            .map(required_message)
            .transpose()?
            .map(|handoff| {
                anyhow::ensure!(
                    handoff.chars().count() <= 200_000,
                    "persistent peer handoff is too long"
                );
                Ok::<_, anyhow::Error>(handoff)
            })
            .transpose()?;
        let _consultation_guard = self.consultation_lock.lock().await;
        let (provider, model, effort) = resolve_persistent_peer_profile(parent_provider, profile)?;
        let (task_name, label) = persistent_peer_lane(provider);
        let rotation = self
            .rotate_sidecar_locked(task_name, provider, model, effort)
            .await?;
        let handoff_queued = if let Some(handoff) = handoff {
            let text = format!(
                "You are taking over the persistent private {label} peer thread. The previous peer was archived and this is a fresh context. Do not invoke another model or ask the human for clarification. Use this handoff as your starting context:\n\n{handoff}"
            );
            self.prompt_child(
                &rotation.replacement.task_name,
                Uuid::new_v4(),
                text,
                Vec::new(),
                PromptDelivery::Queue,
            )
            .await?;
            true
        } else {
            false
        };
        Ok(json!({
            "persistent": true,
            "rotated": true,
            "provider": provider.catalog_backend(),
            "model": rotation.replacement.model,
            "effort": rotation.replacement.effort,
            "thread": rotation.replacement.task_name,
            "archived": rotation.archived,
            "replacement": rotation.replacement,
            "handoff_queued": handoff_queued,
        }))
    }

    /// Ask the persistent provider sidecar for a private second opinion.
    ///
    /// This deliberately reuses the same `/root/claude` or `/root/gpt` child
    /// that the interactive sidecar commands address. The primary model gets
    /// the completed answer synchronously as a tool result; the peer never
    /// receives a tool that can invoke another peer, so consultation cannot
    /// turn into an unbounded model-to-model loop.
    pub async fn consult_peer(
        &self,
        parent_provider: CodingProvider,
        profile: Option<&str>,
        prompt: &str,
    ) -> Result<Value> {
        let prompt = required_message(prompt)?;
        anyhow::ensure!(
            prompt.chars().count() <= 200_000,
            "persistent peer briefing is too long"
        );
        let _consultation_guard = self.consultation_lock.lock().await;
        let (provider, model, effort) = resolve_persistent_peer_profile(parent_provider, profile)?;
        let explicit_profile = profile
            .map(str::trim)
            .is_some_and(|profile| !profile.is_empty());
        let (task_name, label) = persistent_peer_lane(provider);
        let activity = self.subscribe();
        let current = {
            let table = self.table.lock().await;
            let canonical = canonical_sidecar_task_name(task_name)?;
            table
                .task_names
                .get(&canonical)
                .and_then(|session_id| table.entries.get(session_id))
                .map(|entry| entry.snapshot.clone())
        };
        let sidecar = match current {
            Some(current)
                if current.provider != provider
                    || (explicit_profile
                        && (current.model != model || current.effort != effort)) =>
            {
                self.rotate_sidecar_locked(task_name, provider, model.clone(), effort.clone())
                    .await?
                    .replacement
            }
            Some(current) if !explicit_profile => {
                self.ensure_sidecar(
                    task_name,
                    provider,
                    current.model.clone(),
                    current.effort.clone(),
                )
                .await?
            }
            _ => {
                self.ensure_sidecar(task_name, provider, model.clone(), effort.clone())
                    .await?
            }
        };
        let message_id = Uuid::new_v4();
        let peer_prompt = format!(
            "You are the persistent private {label} peer for the primary Borg agent. This is an \
             internal consultation, not a user-facing turn. Do not invoke another model, send \
             team messages, edit files, or ask the human for clarification. You may inspect \
             workspace state when it is necessary to validate the briefing. Return concise, \
             self-contained analysis that the primary agent can use immediately.\n\n{prompt}"
        );
        self.prompt_child(
            &sidecar.task_name,
            message_id,
            peer_prompt,
            Vec::new(),
            // A canceled primary turn must not steer or replace peer work that
            // is still completing. Queueing also gives every consultation a
            // distinct TurnCompleted boundary that can be correlated below.
            PromptDelivery::Queue,
        )
        .await?;

        // The waiter is intentionally detached from the originating MCP call.
        // Provider steering can cancel that call while the peer is still
        // reasoning. In that case the completed result is queued privately at
        // the director's next boundary instead of being orphaned.
        let (result_tx, result_rx) = oneshot::channel();
        let (ack_tx, ack_rx) = oneshot::channel();
        let coordinator = self.clone();
        let background_sidecar = sidecar.clone();
        let background_label = label.to_string();
        tokio::spawn(async move {
            let outcome = coordinator
                .await_peer_consultation(
                    activity,
                    &background_sidecar,
                    message_id,
                    &background_label,
                )
                .await;
            if (result_tx.send(outcome.clone()).is_err() || ack_rx.await.is_err())
                && let Err(error) = coordinator
                    .queue_abandoned_peer_outcome(
                        &background_sidecar,
                        message_id,
                        &background_label,
                        outcome,
                    )
                    .await
            {
                tracing::warn!(
                    %error,
                    consultation_id = %message_id,
                    peer = %background_sidecar.task_name,
                    "could not queue an abandoned peer consultation result"
                );
            }
        });

        let outcome = result_rx
            .await
            .context("persistent peer consultation waiter stopped")?;
        let _ = ack_tx.send(());
        match outcome {
            PeerConsultationOutcome::Completed(response) => Ok(json!({
                "persistent": true,
                "completed": true,
                "consultation_id": message_id,
                "provider": provider.catalog_backend(),
                "model": sidecar.model,
                "effort": sidecar.effort,
                "thread": sidecar.task_name,
                "response": response,
            })),
            PeerConsultationOutcome::Failed(error) => bail!(error),
        }
    }

    async fn await_peer_consultation(
        &self,
        mut activity: broadcast::Receiver<SubagentActivity>,
        sidecar: &SubagentSnapshot,
        message_id: Uuid,
        label: &str,
    ) -> PeerConsultationOutcome {
        let timeout = persistent_peer_consultation_timeout();
        let timeout_label = format_timeout(timeout);
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return PeerConsultationOutcome::Failed(format!(
                    "persistent {label} peer consultation {message_id} timed out after {timeout_label}"
                ));
            }
            match tokio::time::timeout(remaining, activity.recv()).await {
                Ok(Ok(SubagentActivity::SessionEvent { event, .. }))
                    if event.session_id == sidecar.session_id =>
                {
                    match event.kind {
                        SessionEventKind::TurnCompleted {
                            message_id: completed_message_id,
                            final_text,
                            error,
                            ..
                        } if completed_message_id == message_id => {
                            if let Some(error) = error {
                                return PeerConsultationOutcome::Failed(format!(
                                    "persistent {label} peer failed consultation {message_id}: {error}"
                                ));
                            }
                            let response = final_text.trim().to_string();
                            if response.is_empty() {
                                return PeerConsultationOutcome::Failed(format!(
                                    "persistent {label} peer returned an empty response for consultation {message_id}"
                                ));
                            }
                            return PeerConsultationOutcome::Completed(response);
                        }
                        SessionEventKind::StatusChanged {
                            status: SessionStatus::Failed | SessionStatus::Stopped,
                            detail,
                        } => {
                            return PeerConsultationOutcome::Failed(format!(
                                "persistent {label} peer stopped before replying{}",
                                detail
                                    .map(|detail| format!(": {detail}"))
                                    .unwrap_or_default()
                            ));
                        }
                        _ => {}
                    }
                }
                Ok(Ok(SubagentActivity::Failed { agent }))
                    if agent.session_id == sidecar.session_id =>
                {
                    return PeerConsultationOutcome::Failed(format!(
                        "persistent {label} peer failed{}",
                        agent
                            .detail
                            .map(|detail| format!(": {detail}"))
                            .unwrap_or_default()
                    ));
                }
                Ok(Ok(_)) | Ok(Err(broadcast::error::RecvError::Lagged(_))) => {}
                Ok(Err(broadcast::error::RecvError::Closed)) => {
                    return PeerConsultationOutcome::Failed(
                        "persistent peer activity stream closed".to_string(),
                    );
                }
                Err(_) => {
                    return PeerConsultationOutcome::Failed(format!(
                        "persistent {label} peer consultation {message_id} timed out after {timeout_label}"
                    ));
                }
            }
        }
    }

    async fn queue_abandoned_peer_outcome(
        &self,
        sidecar: &SubagentSnapshot,
        consultation_id: Uuid,
        label: &str,
        outcome: PeerConsultationOutcome,
    ) -> Result<()> {
        let message = match outcome {
            PeerConsultationOutcome::Completed(response) => format!(
                "Persistent {label} peer consultation {consultation_id} completed after its \
                 original tool call ended. Reconcile this private result before answering the \
                 user.\n\n{response}"
            ),
            PeerConsultationOutcome::Failed(error) => format!(
                "Persistent {label} peer consultation {consultation_id} finished after its \
                 original tool call ended, but did not return a usable result: {error}"
            ),
        };
        let (inbox, _) = self
            .persist_team_message(
                sidecar.session_id,
                self.root_session_id,
                &sidecar.task_name,
                &message,
                PromptDelivery::Queue,
                DeliveryMode::NextTurn,
                TeamMessageOptions::default(),
            )
            .await?;
        self.root_inbox.lock().await.push(inbox);
        Ok(())
    }

    /// Lazily start one metadata-only child after an explicit action targets
    /// it. Concurrent callers either own the wake transition or wait for the
    /// command channel installed by that owner.
    async fn ensure_child_actor(&self, session_id: Uuid) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let wake = {
                let mut table = self.table.lock().await;
                let active = table
                    .entries
                    .values()
                    .filter(|entry| {
                        entry.snapshot.status.consumes_concurrency_slot() && !entry.dormant
                    })
                    .count();
                let max_children = table.max_children;
                let entry = table
                    .entries
                    .get_mut(&session_id)
                    .with_context(|| format!("unknown subagent session {session_id}"))?;
                anyhow::ensure!(
                    !entry.snapshot.status.is_terminal(),
                    "subagent {} is not running",
                    entry.snapshot.task_name
                );
                if entry.commands.is_some() {
                    return Ok(());
                }
                if entry.dormant {
                    anyhow::ensure!(
                        active < max_children,
                        "subagent concurrency limit reached ({max_children})"
                    );
                    entry.dormant = false;
                    entry.snapshot.status = SubagentStatus::Starting;
                    entry.snapshot.updated_at = Utc::now();
                    entry.snapshot.detail = Some("Waking after parent resume".to_string());
                    Some(entry.snapshot.clone())
                } else {
                    None
                }
            };
            if let Some(snapshot) = wake {
                let mut launch = self.root_launch.clone();
                launch.request_id = Uuid::new_v4();
                launch.initial_prompt = None;
                launch.provider = snapshot.provider;
                launch.model = snapshot.model.clone();
                launch.effort = snapshot.effort.clone();
                launch.cwd = snapshot.cwd.clone();
                launch.name = Some(snapshot.task_name.clone());
                if let Err(error) = self.start_reserved(snapshot.clone(), launch, false).await {
                    let mut table = self.table.lock().await;
                    if let Some(entry) = table.entries.get_mut(&session_id) {
                        entry.dormant = true;
                        entry.snapshot.status = SubagentStatus::Ready;
                        entry.snapshot.detail = Some(format!("Could not wake: {error:#}"));
                    }
                    return Err(error);
                }
                return Ok(());
            }
            anyhow::ensure!(
                tokio::time::Instant::now() < deadline,
                "subagent actor did not finish starting"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn start_reserved(
        &self,
        snapshot: SubagentSnapshot,
        launch: LaunchSession,
        announce: bool,
    ) -> Result<()> {
        let (command_tx, command_rx) = mpsc::channel(64);
        let (event_tx, mut event_rx) = mpsc::channel(256);
        let actor_path = child_lock_path(&self.journal_root, snapshot.session_id);
        let actor_session_id = snapshot.session_id;
        let writer = crate::SessionWriterLease::try_acquire(&actor_path)?
            .with_context(|| format!("subagent session {actor_session_id} is already active"))?;
        self.store
            .register_child_session(snapshot.parent_session_id, actor_session_id)
            .await?;
        if self.root_launch.capabilities.multiplayer {
            let binding = self
                .store
                .workspace_binding(actor_session_id)
                .await?
                .with_context(|| {
                    format!("subagent session {actor_session_id} has no workspace binding")
                })?;
            let workspace_store = self.workspace_store().await?;
            let human_display_name =
                std::env::var("USER").unwrap_or_else(|_| "Local user".to_string());
            let human_participant_id = crate::local_human_participant_id(&human_display_name);
            workspace_store
                .ensure_execution_workspace(
                    binding.workspace_id,
                    self.root_launch
                        .cwd
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or("Borg workspace"),
                    human_participant_id,
                    &human_display_name,
                    binding.participant_id,
                    &snapshot.task_name,
                )
                .await?;
        }
        let queued_inbox = {
            let mut table = self.table.lock().await;
            let entry = table
                .entries
                .get_mut(&snapshot.session_id)
                .expect("reserved subagent exists");
            entry.commands = Some(command_tx.clone());
            entry.dormant = false;
            std::mem::take(&mut entry.inbox)
        };
        if announce {
            let _ = self.activity_tx.send(SubagentActivity::Started {
                agent: snapshot.clone(),
            });
        }
        let actor = tokio::spawn(boxed_agent_store_session(
            self.journal_root.clone(),
            actor_session_id,
            launch,
            command_rx,
            event_tx,
            Arc::clone(&self.executor),
            Arc::clone(&self.store),
            writer,
            self.clone(),
        ));
        let table = self.table.clone();
        let activity_tx = self.activity_tx.clone();
        let task_name = snapshot.task_name.clone();
        let parent_session_id = snapshot.parent_session_id;
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                update_from_session_event(&table, actor_session_id, &event).await;
                let _ = activity_tx.send(SubagentActivity::SessionEvent {
                    parent_session_id,
                    task_name: task_name.clone(),
                    event,
                });
            }
            let outcome = actor
                .await
                .context("subagent actor task failed")
                .and_then(|x| x);
            if let Some(activity) = finish_agent(&table, actor_session_id, outcome.err()).await {
                let table = table.lock().await;
                let activity = match (
                    activity,
                    table
                        .entries
                        .get(&actor_session_id)
                        .map(|entry| entry.snapshot.clone()),
                ) {
                    (SubagentActivity::Stopped { .. }, Some(agent)) => {
                        SubagentActivity::Stopped { agent }
                    }
                    (SubagentActivity::Failed { .. }, Some(agent)) => {
                        SubagentActivity::Failed { agent }
                    }
                    (activity, _) => activity,
                };
                let _ = activity_tx.send(activity);
            }
        });
        for message in queued_inbox {
            command_tx
                .send(HostCommand::Prompt {
                    session_id: actor_session_id,
                    message_id: message.message_id,
                    text: message.text,
                    attachments: Vec::new(),
                    output_schema: None,
                    delivery: message.delivery,
                })
                .await
                .map_err(|_| anyhow::anyhow!("subagent command channel closed while waking"))?;
        }
        Ok(())
    }

    pub async fn list(&self, path_prefix: Option<&str>) -> Vec<SubagentSnapshot> {
        let prefix = path_prefix
            .map(str::trim)
            .filter(|prefix| !prefix.is_empty());
        self.table
            .lock()
            .await
            .snapshots()
            .into_iter()
            .filter(|agent| prefix.is_none_or(|prefix| agent.task_name.starts_with(prefix)))
            .collect()
    }

    pub async fn get(&self, session_id: Uuid) -> Option<SubagentSnapshot> {
        self.table
            .lock()
            .await
            .entries
            .get(&session_id)
            .map(|entry| entry.snapshot.clone())
    }

    async fn task_name_for_session(&self, session_id: Uuid) -> Result<String> {
        self.table.lock().await.task_name(session_id)
    }

    pub async fn resolve_snapshot(&self, target: &str) -> Result<SubagentSnapshot> {
        let table = self.table.lock().await;
        let id = table.resolve(target)?;
        Ok(table
            .entries
            .get(&id)
            .expect("resolved subagent exists")
            .snapshot
            .clone())
    }

    /// Send human-authored input directly to a child actor.
    ///
    /// Unlike team messages, this records an ordinary user prompt in the
    /// child's own thread, matching input from a focused TUI composer.
    pub async fn prompt_child(
        &self,
        target: &str,
        message_id: Uuid,
        text: String,
        attachments: Vec<PathBuf>,
        delivery: PromptDelivery,
    ) -> Result<()> {
        anyhow::ensure!(
            !text.trim().is_empty() || !attachments.is_empty(),
            "subagent prompt must not be empty"
        );
        let id = {
            let table = self.table.lock().await;
            let id = table.resolve(target)?;
            anyhow::ensure!(
                id != table.root_session_id,
                "director is not a child session"
            );
            id
        };
        self.ensure_child_actor(id).await?;
        self.store
            .admit_prompt(SessionEvent::new(
                id,
                0,
                SessionEventKind::Message {
                    message_id,
                    actor: EventActor::User,
                    text: text.clone(),
                    attachments: attachments.clone(),
                    status: MessageStatus::Queued,
                    delivery: Some(delivery),
                },
            ))
            .await?;
        let (commands, task_name) = {
            let table = self.table.lock().await;
            let entry = table
                .entries
                .get(&id)
                .ok_or_else(|| anyhow::anyhow!("unknown subagent target: {target}"))?;
            anyhow::ensure!(
                !entry.snapshot.status.is_terminal(),
                "subagent {} is not running",
                entry.snapshot.task_name
            );
            (
                entry.commands.clone().ok_or_else(|| {
                    anyhow::anyhow!("subagent {} is still starting", entry.snapshot.task_name)
                })?,
                entry.snapshot.task_name.clone(),
            )
        };
        commands
            .send(HostCommand::Prompt {
                session_id: id,
                message_id,
                text,
                attachments,
                output_schema: None,
                delivery,
            })
            .await
            .map_err(|_| anyhow::anyhow!("subagent {task_name} command channel closed"))
    }

    pub async fn recall_child_prompt(&self, target: &str, message_id: Option<Uuid>) -> Result<()> {
        let id = {
            let table = self.table.lock().await;
            let id = table.resolve(target)?;
            anyhow::ensure!(
                id != table.root_session_id,
                "director is not a child session"
            );
            id
        };
        self.ensure_child_actor(id).await?;
        let (commands, task_name) = {
            let table = self.table.lock().await;
            let entry = table
                .entries
                .get(&id)
                .ok_or_else(|| anyhow::anyhow!("unknown subagent target: {target}"))?;
            (
                entry.commands.clone().ok_or_else(|| {
                    anyhow::anyhow!("subagent {} is still starting", entry.snapshot.task_name)
                })?,
                entry.snapshot.task_name.clone(),
            )
        };
        commands
            .send(HostCommand::RecallQueuedPrompt {
                session_id: id,
                message_id,
            })
            .await
            .map_err(|_| anyhow::anyhow!("subagent {task_name} command channel closed"))
    }

    /// Queue a message without waking an idle child.
    pub async fn send_message(&self, target: &str, message: &str) -> Result<()> {
        let root_session_id = self.table.lock().await.root_session_id;
        self.send_message_as(root_session_id, target, message).await
    }

    /// Append one workspace message and immediately fan its single ID out to
    /// locally visible team members. Independent sessions receive the same
    /// durable message from the workspace delivery log.
    pub async fn broadcast_message_as(
        &self,
        actor_session_id: Uuid,
        message: &str,
    ) -> Result<WorkspaceMessageReceipt> {
        anyhow::ensure!(
            self.root_launch.capabilities.multiplayer,
            "team broadcast requires multiplayer capability"
        );
        let message = required_message(message)?;
        let (actor, recipients) =
            {
                let table = self.table.lock().await;
                let actor = table.task_name(actor_session_id)?;
                let mut recipients = vec![table.root_session_id];
                recipients.extend(table.entries.iter().filter_map(|(id, entry)| {
                    (!entry.snapshot.status.is_terminal()).then_some(*id)
                }));
                recipients.sort_unstable();
                recipients.dedup();
                (actor, recipients)
            };
        let sender = self
            .store
            .workspace_binding(actor_session_id)
            .await?
            .context("team sender has no workspace")?;
        let idempotency_id = Uuid::new_v4();
        let receipt = self
            .workspace_store()
            .await?
            .append_message(NewWorkspaceMessage {
                workspace_id: sender.workspace_id,
                author_id: sender.participant_id,
                text: message.clone(),
                mentions: Vec::new(),
                audience: Audience::Workspace,
                mode: DeliveryMode::NextTurn,
                thread_id: None,
                reply_to_message_id: None,
                idempotency_key: format!("team-broadcast:{idempotency_id}"),
            })
            .await?;
        let inbox = TeamInboxMessage {
            message_id: receipt.message_id,
            text: attributed_team_message(&actor, &message),
            report_text: message,
            sender_session_id: actor_session_id,
            delivery: PromptDelivery::Queue,
        };
        let root_session_id = self.table.lock().await.root_session_id;
        for recipient in recipients {
            if recipient == actor_session_id {
                continue;
            }
            if recipient == root_session_id {
                self.root_inbox.lock().await.push(inbox.clone());
            } else {
                let mut table = self.table.lock().await;
                if let Some(entry) = table.entries.get_mut(&recipient) {
                    if matches!(
                        entry.snapshot.status,
                        SubagentStatus::Running
                            | SubagentStatus::WaitingForApproval
                            | SubagentStatus::Starting
                    ) {
                        send_prompt(entry, recipient, inbox.clone()).await?;
                    } else {
                        entry.inbox.push(inbox.clone());
                    }
                }
            }
        }
        Ok(receipt)
    }

    /// Send a team-attributed message. Child reports addressed to `/root` use
    /// the wake path; sibling and child messages remain next-turn queue work.
    pub async fn send_message_as(
        &self,
        actor_session_id: Uuid,
        target: &str,
        message: &str,
    ) -> Result<()> {
        self.send_message_with_options_as(
            actor_session_id,
            target,
            message,
            TeamMessageOptions::default(),
        )
        .await
    }

    pub async fn send_message_with_options_as(
        &self,
        actor_session_id: Uuid,
        target: &str,
        message: &str,
        options: TeamMessageOptions,
    ) -> Result<()> {
        self.route_message_with_options_as(actor_session_id, target, message, options)
            .await
            .map(|_| ())
    }

    async fn route_message_with_options_as(
        &self,
        actor_session_id: Uuid,
        target: &str,
        message: &str,
        options: TeamMessageOptions,
    ) -> Result<RoutedTeamMessage> {
        let message = required_message(message)?;
        let (actor, local_id, root_session_id, status) = {
            let table = self.table.lock().await;
            let actor = table.task_name(actor_session_id)?;
            let id = table.resolve(target).ok();
            let status =
                id.and_then(|id| table.entries.get(&id).map(|entry| entry.snapshot.status));
            (actor, id, table.root_session_id, status)
        };
        if local_id.is_none()
            && let Some(participant_id) = parse_workspace_participant_target(target)?
        {
            return self
                .route_workspace_participant_message_as(
                    actor_session_id,
                    participant_id,
                    &message,
                    options,
                    DeliveryMode::NextTurn,
                )
                .await;
        }
        let id = match local_id {
            Some(id) => id,
            None => parse_session_message_target(target)?,
        };
        ensure!(
            id != actor_session_id,
            "message recipient must differ from its author"
        );
        if local_id.is_none() {
            self.store
                .workspace_binding(id)
                .await?
                .with_context(|| format!("unknown session message target: {id}"))?;
        }
        if status.is_some_and(SubagentStatus::is_terminal) {
            bail!("subagent {target} is not running");
        }
        let wakes_root = id == root_session_id && actor_session_id != root_session_id;
        let (inbox_message, receipt) = self
            .persist_team_message(
                actor_session_id,
                id,
                &actor,
                &message,
                if wakes_root {
                    PromptDelivery::Steer
                } else {
                    PromptDelivery::Queue
                },
                if wakes_root {
                    DeliveryMode::Wake
                } else {
                    DeliveryMode::NextTurn
                },
                options,
            )
            .await?;
        let relay_pending = self
            .session_message_needs_relay(actor_session_id, id)
            .await?;
        if local_id.is_none() {
            let socket_path = crate::session_control_socket_path(&self.journal_root, id);
            let dispatched_locally =
                if !relay_pending && tokio::fs::try_exists(&socket_path).await.unwrap_or(false) {
                    match crate::send_local_session_command(
                        &socket_path,
                        id,
                        HostCommand::TeamPrompt {
                            session_id: id,
                            message_id: inbox_message.message_id,
                            text: inbox_message.text.clone(),
                            attachments: Vec::new(),
                            output_schema: None,
                            delivery: inbox_message.delivery,
                        },
                    )
                    .await
                    {
                        Ok(()) => true,
                        Err(error) => {
                            tracing::debug!(
                                %error,
                                target_session_id = %id,
                                "durable session message awaits the recipient boundary"
                            );
                            false
                        }
                    }
                } else {
                    false
                };
            return Ok(RoutedTeamMessage {
                receipt,
                dispatched_locally,
                relay_pending,
            });
        }
        if id == root_session_id {
            if wakes_root {
                self.broadcast_root_message(inbox_message.clone()).await;
            } else {
                self.root_inbox.lock().await.push(inbox_message.clone());
            }
            if actor_session_id != root_session_id {
                // Project a child-authored report through the activity stream;
                // the root actor also receives the durable Wake delivery above
                // and can reconcile it without requiring a human relay.
                let _ = self.activity_tx.send(SubagentActivity::SessionEvent {
                    parent_session_id: root_session_id,
                    task_name: actor,
                    event: SessionEvent::new(
                        actor_session_id,
                        0,
                        SessionEventKind::Message {
                            message_id: inbox_message.message_id,
                            actor: crate::EventActor::Assistant,
                            text: message,
                            attachments: Vec::new(),
                            status: MessageStatus::Complete,
                            delivery: None,
                        },
                    ),
                });
            }
            return Ok(RoutedTeamMessage {
                receipt,
                dispatched_locally: true,
                relay_pending,
            });
        }
        let mut table = self.table.lock().await;
        let entry = table
            .entries
            .get_mut(&id)
            .expect("resolved subagent exists");
        if entry.snapshot.status.is_terminal() {
            bail!("subagent {} is not running", entry.snapshot.task_name);
        }
        if matches!(
            entry.snapshot.status,
            SubagentStatus::Running | SubagentStatus::WaitingForApproval | SubagentStatus::Starting
        ) {
            send_prompt(entry, id, inbox_message).await?;
        } else {
            entry.inbox.push(inbox_message);
        }
        Ok(RoutedTeamMessage {
            receipt,
            dispatched_locally: true,
            relay_pending,
        })
    }

    /// Wake an idle child, or steer a running provider when supported.
    pub async fn followup_task(&self, target: &str, message: &str) -> Result<()> {
        let root_session_id = self.table.lock().await.root_session_id;
        self.followup_task_as(root_session_id, target, message)
            .await
    }

    /// Wake or steer a team recipient while preserving the sender identity.
    pub async fn followup_task_as(
        &self,
        actor_session_id: Uuid,
        target: &str,
        message: &str,
    ) -> Result<()> {
        self.followup_task_with_options_as(
            actor_session_id,
            target,
            message,
            TeamMessageOptions::default(),
        )
        .await
    }

    pub async fn followup_task_with_options_as(
        &self,
        actor_session_id: Uuid,
        target: &str,
        message: &str,
        options: TeamMessageOptions,
    ) -> Result<()> {
        self.route_followup_task_with_options_as(actor_session_id, target, message, options)
            .await
            .map(|_| ())
    }

    async fn route_followup_task_with_options_as(
        &self,
        actor_session_id: Uuid,
        target: &str,
        message: &str,
        options: TeamMessageOptions,
    ) -> Result<RoutedTeamMessage> {
        let message = required_message(message)?;
        let (actor, local_id, root_session_id, status) = {
            let table = self.table.lock().await;
            let actor = table.task_name(actor_session_id)?;
            let id = table.resolve(target).ok();
            let status =
                id.and_then(|id| table.entries.get(&id).map(|entry| entry.snapshot.status));
            (actor, id, table.root_session_id, status)
        };
        if local_id.is_none()
            && let Some(participant_id) = parse_workspace_participant_target(target)?
        {
            return self
                .route_workspace_participant_message_as(
                    actor_session_id,
                    participant_id,
                    &message,
                    options,
                    DeliveryMode::Wake,
                )
                .await;
        }
        let id = match local_id {
            Some(id) => id,
            None => parse_session_message_target(target)?,
        };
        ensure!(
            id != actor_session_id,
            "message recipient must differ from its author"
        );
        if local_id.is_none() {
            self.store
                .workspace_binding(id)
                .await?
                .with_context(|| format!("unknown session message target: {id}"))?;
        }
        if status.is_some_and(SubagentStatus::is_terminal) {
            bail!("subagent {target} is not running");
        }
        let (inbox_message, receipt) = self
            .persist_team_message(
                actor_session_id,
                id,
                &actor,
                &message,
                PromptDelivery::Steer,
                if local_id.is_none() || status == Some(SubagentStatus::Ready) {
                    DeliveryMode::Wake
                } else {
                    DeliveryMode::Boundary
                },
                options,
            )
            .await?;
        let relay_pending = self
            .session_message_needs_relay(actor_session_id, id)
            .await?;
        if local_id.is_none() {
            let socket_path = crate::session_control_socket_path(&self.journal_root, id);
            let dispatched_locally =
                if !relay_pending && tokio::fs::try_exists(&socket_path).await.unwrap_or(false) {
                    match crate::send_local_session_command(
                        &socket_path,
                        id,
                        HostCommand::TeamPrompt {
                            session_id: id,
                            message_id: inbox_message.message_id,
                            text: inbox_message.text,
                            attachments: Vec::new(),
                            output_schema: None,
                            delivery: PromptDelivery::Steer,
                        },
                    )
                    .await
                    {
                        Ok(()) => true,
                        Err(error) => {
                            tracing::debug!(
                                %error,
                                target_session_id = %id,
                                "durable follow-up awaits the recipient boundary"
                            );
                            false
                        }
                    }
                } else {
                    false
                };
            return Ok(RoutedTeamMessage {
                receipt,
                dispatched_locally,
                relay_pending,
            });
        }
        if id == root_session_id {
            let mut messages = self.take_root_inbox().await;
            messages.push(inbox_message);
            for message in messages {
                self.broadcast_root_message(message).await;
            }
            return Ok(RoutedTeamMessage {
                receipt,
                dispatched_locally: true,
                relay_pending,
            });
        }
        self.ensure_child_actor(id).await?;
        let mut table = self.table.lock().await;
        let entry = table
            .entries
            .get_mut(&id)
            .expect("resolved subagent exists");
        if entry.snapshot.status.is_terminal() {
            bail!("subagent {} is not running", entry.snapshot.task_name);
        }
        let mut messages = std::mem::take(&mut entry.inbox);
        messages.push(inbox_message);
        for message in messages {
            send_prompt(entry, id, message).await?;
        }
        Ok(RoutedTeamMessage {
            receipt,
            dispatched_locally: true,
            relay_pending,
        })
    }

    async fn session_message_needs_relay(&self, sender: Uuid, recipient: Uuid) -> Result<bool> {
        let sender_host = self
            .store
            .workspace_binding(sender)
            .await?
            .and_then(|binding| binding.host_id);
        let recipient_host = self
            .store
            .workspace_binding(recipient)
            .await?
            .and_then(|binding| binding.host_id);
        Ok(sender_host.is_some() && recipient_host.is_some() && sender_host != recipient_host)
    }

    async fn route_workspace_participant_message_as(
        &self,
        actor_session_id: Uuid,
        recipient_participant_id: Uuid,
        message: &str,
        options: TeamMessageOptions,
        mode: DeliveryMode,
    ) -> Result<RoutedTeamMessage> {
        let actor = self
            .store
            .workspace_binding(actor_session_id)
            .await?
            .context("message sender has no workspace")?;
        ensure!(
            actor.participant_id != recipient_participant_id,
            "message recipient must differ from its author"
        );
        let store = self.workspace_store().await?;
        if self
            .store
            .workspace_binding(recipient_participant_id)
            .await?
            .is_some()
        {
            return match mode {
                DeliveryMode::Wake => {
                    Box::pin(self.route_followup_task_with_options_as(
                        actor_session_id,
                        &format!("session:{recipient_participant_id}"),
                        message,
                        options,
                    ))
                    .await
                }
                _ => {
                    Box::pin(self.route_message_with_options_as(
                        actor_session_id,
                        &format!("session:{recipient_participant_id}"),
                        message,
                        options,
                    ))
                    .await
                }
            };
        }
        let roster = store
            .workspace_roster(actor.workspace_id, actor.participant_id)
            .await?;
        let workspace_id = if roster
            .iter()
            .any(|entry| entry.participant.id == recipient_participant_id)
        {
            actor.workspace_id
        } else {
            store
                .ensure_direct_workspace(actor.participant_id, recipient_participant_id)
                .await?
        };
        let idempotency_id = Uuid::new_v4();
        let receipt = store
            .append_message(NewWorkspaceMessage {
                workspace_id,
                author_id: actor.participant_id,
                text: message.to_string(),
                mentions: options.mentions,
                audience: Audience::Direct {
                    participant: recipient_participant_id,
                },
                mode,
                thread_id: None,
                reply_to_message_id: options.reply_to_message_id,
                idempotency_key: format!("participant-message:{idempotency_id}"),
            })
            .await?;
        Ok(RoutedTeamMessage {
            receipt: Some(receipt),
            dispatched_locally: false,
            relay_pending: actor.host_id.is_some(),
        })
    }

    pub async fn interrupt(&self, target: &str) -> Result<()> {
        self.send_command(target, |session_id| HostCommand::Interrupt { session_id })
            .await
    }

    pub async fn flush_pending_input(&self, target: &str) -> Result<()> {
        self.send_command(target, |session_id| HostCommand::FlushPendingInput {
            session_id,
        })
        .await
    }

    pub async fn stop(&self, target: &str) -> Result<()> {
        self.send_command(target, |session_id| HostCommand::Stop { session_id })
            .await
    }

    /// Stop every currently live child when the owning root session stops.
    /// Dormant metadata-only children have no process or command channel and
    /// therefore require no work here.
    pub(crate) async fn stop_all(&self) -> Vec<SubagentActivity> {
        let children = self
            .table
            .lock()
            .await
            .entries
            .iter()
            .filter_map(|(session_id, entry)| {
                entry
                    .commands
                    .clone()
                    .map(|commands| (*session_id, commands))
            })
            .collect::<Vec<_>>();
        let child_ids = children
            .iter()
            .map(|(session_id, _)| *session_id)
            .collect::<Vec<_>>();
        for (session_id, commands) in children {
            let _ = commands.send(HostCommand::Stop { session_id }).await;
        }
        let mut warn_at = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let live = self
                .table
                .lock()
                .await
                .entries
                .values()
                .filter(|entry| entry.commands.is_some())
                .count();
            if live == 0 {
                break;
            }
            if tokio::time::Instant::now() >= warn_at {
                tracing::warn!(live, "still waiting for child actors to stop with root");
                warn_at = tokio::time::Instant::now() + Duration::from_secs(5);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let table = self.table.lock().await;
        child_ids
            .into_iter()
            .filter_map(|session_id| {
                let entry = table.entries.get(&session_id)?;
                if entry.commands.is_some() {
                    return None;
                }
                match entry.snapshot.status {
                    SubagentStatus::Stopped => Some(SubagentActivity::Stopped {
                        agent: entry.snapshot.clone(),
                    }),
                    SubagentStatus::Failed => Some(SubagentActivity::Failed {
                        agent: entry.snapshot.clone(),
                    }),
                    _ => None,
                }
            })
            .collect()
    }

    pub async fn approve(
        &self,
        target: &str,
        approval_id: String,
        decision: ApprovalDecision,
    ) -> Result<()> {
        self.send_command(target, |session_id| HostCommand::Approve {
            session_id,
            approval_id,
            decision,
        })
        .await
    }

    async fn send_command(
        &self,
        target: &str,
        command: impl FnOnce(Uuid) -> HostCommand,
    ) -> Result<()> {
        let id = self.table.lock().await.resolve(target)?;
        self.ensure_child_actor(id).await?;
        let table = self.table.lock().await;
        let entry = table.entries.get(&id).expect("resolved subagent exists");
        let sender = entry.commands.clone().ok_or_else(|| {
            anyhow::anyhow!("subagent {} is still starting", entry.snapshot.task_name)
        })?;
        drop(table);
        sender
            .send(command(id))
            .await
            .map_err(|_| anyhow::anyhow!("subagent command channel closed"))
    }

    pub async fn clear_context(&self, target: &str) -> Result<()> {
        self.send_command(target, |session_id| HostCommand::ClearContext {
            session_id,
        })
        .await
    }

    pub async fn wait(&self, timeout: Duration) -> Result<Option<SubagentActivity>> {
        let timeout = timeout.clamp(Duration::from_millis(100), Duration::from_secs(60));
        if let Some(agent) = self
            .table
            .lock()
            .await
            .snapshots()
            .into_iter()
            .find(|agent| agent.status == SubagentStatus::Ready && agent.final_text.is_some())
        {
            return Ok(Some(SubagentActivity::Completed { agent }));
        }
        let mut receiver = self.subscribe();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining, receiver.recv()).await {
                Ok(Ok(activity)) => {
                    if let Some(session_id) = ready_session_id(&activity)
                        && let Some(agent) = self
                            .table
                            .lock()
                            .await
                            .entries
                            .get(&session_id)
                            .map(|entry| entry.snapshot.clone())
                    {
                        return Ok(Some(SubagentActivity::Completed { agent }));
                    }
                    if significant_activity(&activity) {
                        return Ok(Some(activity));
                    }
                }
                Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue,
                Ok(Err(broadcast::error::RecvError::Closed)) => {
                    bail!("subagent activity stream closed")
                }
                Err(_) => return Ok(None),
            }
        }
    }

    /// Execute one model collaboration tool against this typed lifecycle.
    pub async fn call_tool(&self, name: &str, arguments: Value) -> Result<Value> {
        let root_session_id = self.table.lock().await.root_session_id;
        self.call_tool_as(root_session_id, name, arguments).await
    }

    /// Execute one collaboration tool as a specific member of the shared team.
    pub async fn call_tool_as(
        &self,
        actor_session_id: Uuid,
        name: &str,
        arguments: Value,
    ) -> Result<Value> {
        match name {
            "spawn_agent" => {
                let args: SpawnAgentArgs = serde_json::from_value(arguments)?;
                self.assign_task_as(
                    actor_session_id,
                    SpawnSubagent {
                        task_name: args.task_name,
                        message: args.message,
                        provider: args.provider,
                        model: args.model,
                        effort: args.reasoning_effort,
                    },
                )
                .await
            }
            "list_agents" => {
                let args: ListAgentsArgs = serde_json::from_value(arguments)?;
                Ok(json!({ "agents": self.list(args.path_prefix.as_deref()).await }))
            }
            "list_workspace_participants" => {
                let _: NoArgs = serde_json::from_value(arguments)?;
                let binding = self
                    .store
                    .workspace_binding(actor_session_id)
                    .await?
                    .context("session has no workspace")?;
                Ok(json!({
                    "workspace_id": binding.workspace_id,
                    "participants": self.workspace_store().await?
                        .workspace_roster(binding.workspace_id, binding.participant_id)
                        .await?,
                }))
            }
            "list_instances" => {
                let _: NoArgs = serde_json::from_value(arguments)?;
                let mut instances = Vec::new();
                for mut instance in self.workspace_store().await?.list_instances().await? {
                    let binding = self
                        .store
                        .workspace_binding(instance.participant.id)
                        .await?;
                    let local = binding.is_some();
                    if let Some(binding) = binding {
                        instance.host_id = binding.host_id.or(instance.host_id);
                        instance.workspace_id = Some(binding.workspace_id);
                    }
                    let mut entry = serde_json::to_value(instance)?;
                    entry["local"] = json!(local);
                    instances.push(entry);
                }
                Ok(json!({ "session_id": actor_session_id, "instances": instances }))
            }
            "send_message" => {
                let args: MessageArgs = serde_json::from_value(arguments)?;
                let routed = self
                    .route_message_with_options_as(
                        actor_session_id,
                        &args.target,
                        &args.message,
                        args.options(),
                    )
                    .await?;
                Ok(routed_message_json(routed, "queued"))
            }
            "followup_task" => {
                let args: MessageArgs = serde_json::from_value(arguments)?;
                let routed = self
                    .route_followup_task_with_options_as(
                        actor_session_id,
                        &args.target,
                        &args.message,
                        args.options(),
                    )
                    .await?;
                Ok(routed_message_json(routed, "accepted"))
            }
            "broadcast_team" => {
                let args: BroadcastArgs = serde_json::from_value(arguments)?;
                let receipt = self
                    .broadcast_message_as(actor_session_id, &args.message)
                    .await?;
                let relay_pending = self
                    .store
                    .workspace_binding(actor_session_id)
                    .await?
                    .is_some_and(|binding| binding.host_id.is_some());
                Ok(json!({
                    "message_id": receipt.message_id,
                    "workspace_id": receipt.workspace_id,
                    "sequence": receipt.sequence,
                    "recipient_count": receipt.recipient_ids.len(),
                    "recipient_ids": receipt.recipient_ids,
                    "delivery_mode": receipt.mode,
                    "queued": true,
                    "relay_pending": relay_pending,
                }))
            }
            "list_unread_team_messages" => Ok(serde_json::to_value(
                self.unread_messages_for_session(actor_session_id).await?,
            )?),
            "acknowledge_team_message" => {
                let args: AcknowledgeMessageArgs = serde_json::from_value(arguments)?;
                self.acknowledge_message_for_session(actor_session_id, args.message_id)
                    .await?;
                Ok(json!({ "acknowledged": true }))
            }
            "interrupt_agent" => {
                let args: TargetArgs = serde_json::from_value(arguments)?;
                self.interrupt(&args.target).await?;
                Ok(json!({ "accepted": true }))
            }
            "wait_agent" => {
                let args: WaitAgentArgs = serde_json::from_value(arguments)?;
                Ok(json!({
                    "activity": self.wait(Duration::from_millis(args.timeout_ms.unwrap_or(30_000))).await?
                }))
            }
            other => bail!("unknown subagent tool: {other}"),
        }
    }
}

fn default_model_for_cross_provider_peer(provider: CodingProvider) -> Option<String> {
    match provider {
        CodingProvider::Codex => Some(borg_provider::codex_product_model().to_string()),
        CodingProvider::Claude => None,
        CodingProvider::OpenCode => None,
        CodingProvider::Kimi => Some(borg_provider::kimi_product_model().to_string()),
        CodingProvider::Glm => Some(borg_provider::glm_product_model().to_string()),
        CodingProvider::OpenRouter => Some(borg_provider::openrouter_product_model().to_string()),
        CodingProvider::OpenAiCompatible => None,
    }
}

fn default_effort_for_cross_provider_peer(provider: CodingProvider) -> Option<String> {
    match provider {
        CodingProvider::Codex => Some(borg_provider::codex_default_effort().to_string()),
        CodingProvider::Kimi => Some(borg_provider::kimi_default_effort().to_string()),
        CodingProvider::Glm => Some(borg_provider::kimi_default_effort().to_string()),
        CodingProvider::Claude
        | CodingProvider::OpenCode
        | CodingProvider::OpenRouter
        | CodingProvider::OpenAiCompatible => None,
    }
}

fn persistent_peer_lane(provider: CodingProvider) -> (&'static str, &'static str) {
    match provider {
        CodingProvider::Claude => ("claude", "Claude"),
        CodingProvider::Codex => ("gpt", "GPT"),
        _ => unreachable!("persistent peer profile is restricted to GPT and Claude"),
    }
}

fn is_persistent_peer_lane(task_name: &str) -> bool {
    matches!(task_name, "/root/claude" | "/root/gpt")
}

fn resolve_persistent_peer_profile(
    parent_provider: CodingProvider,
    profile: Option<&str>,
) -> Result<(CodingProvider, Option<String>, Option<String>)> {
    let normalized = profile
        .map(str::trim)
        .filter(|profile| !profile.is_empty())
        .map(str::to_ascii_lowercase);
    let (provider, explicit_model, requested_effort) = if let Some(profile) = normalized {
        let (profile, requested_effort) = profile
            .rsplit_once('@')
            .map_or((profile.as_str(), None), |(profile, effort)| {
                (profile, (!effort.is_empty()).then_some(effort))
            });
        let (provider, explicit_model) = if let Some((provider_hint, model)) =
            profile.split_once('/')
        {
            let provider = match provider_hint {
                "claude" | "anthropic" => CodingProvider::Claude,
                "gpt" | "codex" | "openai" => CodingProvider::Codex,
                _ => CodingProvider::for_model(provider_hint).with_context(|| {
                    format!("unknown persistent peer provider `{provider_hint}`")
                })?,
            };
            (provider, (!model.is_empty()).then_some(model))
        } else {
            match profile {
                "claude" | "anthropic" => (CodingProvider::Claude, None),
                "gpt" | "codex" | "openai" => (CodingProvider::Codex, None),
                model => (
                    CodingProvider::for_model(model)
                        .with_context(|| format!("unknown persistent peer profile `{profile}`"))?,
                    Some(model),
                ),
            }
        };
        (
            provider,
            explicit_model.map(str::to_string),
            requested_effort.map(str::to_string),
        )
    } else {
        let provider = match parent_provider {
            CodingProvider::Claude => CodingProvider::Codex,
            _ => CodingProvider::Claude,
        };
        (provider, None, None)
    };
    anyhow::ensure!(
        matches!(provider, CodingProvider::Claude | CodingProvider::Codex),
        "persistent peers currently support only GPT and Claude"
    );
    let default_model = match provider {
        CodingProvider::Claude => Some("claude-opus-5".to_string()),
        CodingProvider::Codex => Some(borg_provider::codex_product_model().to_string()),
        _ => unreachable!(),
    };
    let default_effort = match provider {
        CodingProvider::Claude => Some("high".to_string()),
        CodingProvider::Codex => Some(borg_provider::codex_default_effort().to_string()),
        _ => unreachable!(),
    };
    let model = explicit_model.or(default_model);
    let effort = requested_effort.or(default_effort);
    validate_subagent_overrides(provider, model.as_deref(), effort.as_deref())?;
    Ok((provider, model, effort))
}

#[allow(clippy::too_many_arguments)]
fn boxed_agent_store_session(
    session_root: PathBuf,
    session_id: Uuid,
    launch: LaunchSession,
    commands: mpsc::Receiver<HostCommand>,
    events: mpsc::Sender<SessionEvent>,
    executor: Arc<dyn crate::AgentTurnExecutor>,
    store: Arc<dyn SessionStore>,
    writer: crate::SessionWriterLease,
    team: SubagentCoordinator,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
    Box::pin(async move {
        crate::session::run_agent_session_with_store_and_writer_and_team(
            &session_root,
            session_id,
            launch,
            commands,
            events,
            executor,
            store,
            writer,
            team,
        )
        .await
    })
}

/// Provider-neutral schemas exposed to every supported execution lane.
pub fn subagent_tool_specs(provider: CodingProvider) -> Vec<Value> {
    let description = subagent_tool_description(provider);
    let model_description = subagent_model_override_description();
    let model_examples = borg_provider::runtime::MODEL_CATALOGS
        .iter()
        .flat_map(|catalog| catalog.selectable_models.iter().map(|(model, _)| *model))
        .collect::<Vec<_>>();
    vec![
        tool(
            "spawn_agent",
            &description,
            json!({
                "type": "object",
                "properties": {
                    "task_name": {
                        "type": "string", "minLength": 1, "maxLength": 64,
                        "pattern": "^[a-z0-9_]+$",
                        "description": "Short task name using lowercase letters, digits, and underscores, such as nuclear_art."
                    },
                    "message": { "type": "string" },
                    "provider": {
                        "type": "string",
                        "enum": [
                            "codex",
                            "claude",
                            "open_router",
                            "open_ai_compatible"
                        ]
                    },
                    "model": {
                        "type": "string",
                        "description": model_description,
                        "examples": model_examples
                    },
                    "reasoning_effort": { "type": "string" }
                },
                "required": ["task_name", "message"],
                "additionalProperties": false
            }),
        ),
        tool(
            "list_agents",
            "List child agents and their current lifecycle state.",
            json!({
                "type": "object",
                "properties": { "path_prefix": { "type": "string" } },
                "additionalProperties": false
            }),
        ),
        tool(
            "list_workspace_participants",
            "List every durable participant visible in the current workspace, including independent and remote-synced agents. Use participant:<UUID> to message one.",
            json!({"type":"object","properties":{},"additionalProperties":false}),
        ),
        tool(
            "list_instances",
            "Discover Borg agent instances across all local workspaces and the authenticated remote instance directory. Use participant:<id> with send_message or followup_task. Remote entries require an enrolled host relay and may be offline; discovery does not grant project access.",
            json!({"type":"object","properties":{},"additionalProperties":false}),
        ),
        message_tool(
            "send_message",
            "Queue a durable message for any discovered Borg instance, across projects and enrolled machines. Use participant:<id> from list_instances, session:<UUID> for a local session, or a team path. Cross-workspace messages use a private channel. Reports sent by a child to /root wake the director for reconciliation.",
        ),
        message_tool(
            "followup_task",
            "Send a durable follow-up and wake or steer a discovered local or remote Borg instance when possible. Use participant:<id> from list_instances or session:<UUID> for a local session.",
        ),
        tool(
            "broadcast_team",
            "Broadcast one durable message to every participant in the current workspace, including independent sessions and remote-synced participants.",
            json!({"type":"object","properties":{"message":{"type":"string"}},"required":["message"],"additionalProperties":false}),
        ),
        tool(
            "list_unread_team_messages",
            "List unread team messages for this participant.",
            json!({"type":"object","properties":{},"additionalProperties":false}),
        ),
        tool(
            "acknowledge_team_message",
            "Acknowledge one unread team message.",
            json!({"type":"object","properties":{"message_id":{"type":"string"}},"required":["message_id"],"additionalProperties":false}),
        ),
        tool(
            "interrupt_agent",
            "Interrupt a child agent's current turn.",
            target_schema(),
        ),
        tool(
            "wait_agent",
            "Wait for a child lifecycle or session update.",
            json!({
                "type": "object",
                "properties": {
                    "timeout_ms": { "type": "integer", "minimum": 100, "maximum": 60000 }
                },
                "additionalProperties": false
            }),
        ),
    ]
}

fn subagent_tool_description(provider: CodingProvider) -> String {
    let inheritance = provider
        .model_catalog()
        .map(|catalog| {
            let models = catalog
                .selectable_models
                .iter()
                .map(|(model, _)| *model)
                .collect::<Vec<_>>()
                .join(", ");
            let efforts = if catalog.effort_levels.is_empty() {
                "provider default".to_string()
            } else {
                catalog.effort_levels.join(", ")
            };
            format!(
                "Available {} model overrides: {models}. Reasoning efforts: {efforts}.",
                catalog.backend
            )
        })
        .unwrap_or_else(|| {
            format!(
                "{} accepts provider-defined model identifiers.",
                provider.catalog_backend()
            )
        });
    format!(
        "Delegate a concrete, bounded task. Borg atomically reuses a compatible idle worker \
         when one is available and otherwise spawns an isolated child session; do not list \
         agents or issue follow-up calls just to manage worker capacity. Omit provider, model, \
         and reasoning_effort to inherit the parent. {inheritance} All catalog-backed subagent \
         choices are also available explicitly: {}",
        subagent_model_override_description()
    )
}

fn subagent_model_override_description() -> String {
    CodingProvider::CATALOG_PROVIDERS
        .into_iter()
        .filter_map(|provider| {
            provider.model_catalog().map(|catalog| {
                let models = catalog
                    .selectable_models
                    .iter()
                    .map(|(model, label)| format!("{model} ({label})"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}: {models}", provider.catalog_backend())
            })
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn validate_subagent_overrides(
    provider: CodingProvider,
    model: Option<&str>,
    effort: Option<&str>,
) -> Result<()> {
    let Some(catalog) = provider.model_catalog() else {
        return Ok(());
    };
    if let Some(model) = model
        && !catalog.supports_model(model)
    {
        let allowed = catalog
            .selectable_models
            .iter()
            .map(|(model, _)| *model)
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "unsupported {} subagent model `{model}`; available models: {allowed}",
            catalog.backend
        );
    }
    if let Some(effort) = effort
        && !catalog.effort_levels.is_empty()
        && !catalog.supports_effort(effort)
    {
        bail!(
            "unsupported {} subagent reasoning effort `{effort}`; available efforts: {}",
            catalog.backend,
            catalog.effort_levels.join(", ")
        );
    }
    Ok(())
}

pub(crate) fn ensure_provider_can_spawn(
    launch: &LaunchSession,
    provider: CodingProvider,
) -> Result<()> {
    let capability = launch
        .capabilities
        .provider_capabilities
        .iter()
        .find(|capability| capability.provider == provider)
        .with_context(|| {
            format!(
                "{} provider capability is unknown on this host; call get_provider_capabilities before spawning",
                provider.label()
            )
        })?;
    anyhow::ensure!(
        capability.can_spawn,
        "{} cannot spawn on this host: {}",
        provider.label(),
        capability
            .usage
            .as_ref()
            .filter(|usage| { usage.availability == crate::ProviderUsageAvailability::Exhausted })
            .and_then(|usage| usage.detail.as_deref())
            .or(capability.auth_detail.as_deref())
            .unwrap_or("no authenticated subscription, API key, or configured endpoint")
    );
    Ok(())
}

fn effective_worker_effort(
    launch: &LaunchSession,
    requested_effort: Option<String>,
) -> Option<String> {
    requested_effort
        .or_else(|| {
            // The only opt-in preset assigns workers low effort. Without a policy,
            // retain the existing inheritance from the root launch.
            launch.team_policy.as_ref().map(|_| "low".to_string())
        })
        .or_else(|| launch.effort.clone())
}

pub fn agent_tool_specs(provider: CodingProvider) -> Vec<Value> {
    agent_tool_specs_with_capabilities(provider, true, true, None)
}

pub fn agent_tool_specs_with_subagents(
    provider: CodingProvider,
    subagents_enabled: bool,
) -> Vec<Value> {
    agent_tool_specs_with_capabilities(provider, subagents_enabled, true, None)
}

pub fn agent_tool_specs_with_team_policy(
    provider: CodingProvider,
    subagents_enabled: bool,
    team_policy: Option<&crate::TeamPolicy>,
) -> Vec<Value> {
    agent_tool_specs_with_capabilities(provider, subagents_enabled, true, team_policy)
}

pub fn agent_tool_specs_with_capabilities(
    provider: CodingProvider,
    subagents_enabled: bool,
    shared_work_enabled: bool,
    team_policy: Option<&crate::TeamPolicy>,
) -> Vec<Value> {
    agent_tool_specs_with_capabilities_and_consultation(
        provider,
        subagents_enabled,
        shared_work_enabled,
        team_policy,
        true,
    )
}

pub fn agent_tool_specs_with_capabilities_and_consultation(
    provider: CodingProvider,
    subagents_enabled: bool,
    shared_work_enabled: bool,
    team_policy: Option<&crate::TeamPolicy>,
    consultation_enabled: bool,
) -> Vec<Value> {
    agent_tool_specs_with_capabilities_and_consultation_and_search(
        provider,
        subagents_enabled,
        shared_work_enabled,
        team_policy,
        consultation_enabled,
        false,
    )
}

fn agent_tool_specs_with_capabilities_and_consultation_and_search(
    provider: CodingProvider,
    subagents_enabled: bool,
    shared_work_enabled: bool,
    team_policy: Option<&crate::TeamPolicy>,
    consultation_enabled: bool,
    web_search_enabled: bool,
) -> Vec<Value> {
    let update_plan_item_schema = json!({
        "type": "object",
        "properties": {
            "id": {
                "type": "string",
                "format": "uuid",
                "description": "Existing item UUID copied exactly from get_plan. Omit for new items; never invent labels."
            },
            "content": {
                "type": "string",
                "minLength": 1,
                "maxLength": crate::session::MAX_PLAN_ITEM_CONTENT_CHARS,
                "description": "Concise plan step, at most 500 characters."
            },
            "status": {
                "type": "string",
                "enum": ["pending", "in_progress", "completed"]
            }
        },
        "required": ["content", "status"],
        "additionalProperties": false
    });
    let update_plan_items_schema = json!({
        "type": "array",
        "maxItems": crate::session::MAX_PLAN_ITEMS,
        "items": update_plan_item_schema
    });
    let update_plan_schema = json!({
        "type": "object",
        "properties": {
            "explanation": { "type": "string" },
            "plan": update_plan_items_schema
        },
        "required": ["plan"],
        "additionalProperties": false
    });
    let mut specs = vec![
        tool(
            "list_files",
            "List one workspace directory without following symlinks.",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "default": "." },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 2000 }
                },
                "additionalProperties": false
            }),
        ),
        tool(
            "read_file",
            "Read a bounded line range from a UTF-8 workspace file. Continue with next_line when truncated.",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "minLength": 1 },
                    "offset_line": { "type": "integer", "minimum": 1, "default": 1 },
                    "limit_lines": { "type": "integer", "minimum": 1, "maximum": 20000, "default": 2000 },
                    "max_bytes": { "type": "integer", "minimum": 1, "maximum": RUNTIME_MAX_FILE_BYTES }
                },
                "required": ["path"],
                "additionalProperties": false
            }),
        ),
        tool(
            "search_files",
            "Search workspace text without requiring external executables. Results are gitignore-aware, bounded, and resumable with next_offset.",
            json!({
                "type": "object",
                "properties": {
                    "pattern": { "type": "string", "minLength": 1 },
                    "path": { "type": "string", "default": "." },
                    "literal": { "type": "boolean", "default": false },
                    "case_sensitive": { "type": "boolean", "default": true },
                    "offset": { "type": "integer", "minimum": 0, "default": 0 },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 2000, "default": 200 }
                },
                "required": ["pattern"],
                "additionalProperties": false
            }),
        ),
        tool(
            "monitor",
            "Start a session-scoped background command that watches logs, files, or external status. Each stdout line is delivered to you automatically in bounded batches, including when idle. Use a command that emits only meaningful changes. Do not poll or wait for it. Requires shell approval; runs until stopped, session exit, or 24 hours. Use list_monitors and stop_monitor to manage watches.",
            json!({
                "type": "object", "properties": {
                    "command": {"type": "string", "minLength": 1},
                    "label": {"type": "string", "minLength": 1, "maxLength": 100},
                    "workdir": {"type": "string"}
                }, "required": ["command", "label"], "additionalProperties": false
            }),
        ),
        tool(
            "list_monitors",
            "List this session's background monitors and whether they are running.",
            json!({
                "type": "object", "properties": {}, "additionalProperties": false
            }),
        ),
        tool(
            "stop_monitor",
            "Stop a background monitor and its process tree.",
            json!({
                "type": "object", "properties": {"monitor_id": {"type": "string", "format": "uuid"}},
                "required": ["monitor_id"], "additionalProperties": false
            }),
        ),
        tool(
            "list_workflows",
            "List active trusted extension workflows across embedded Blu/Lua/Luau, Python, IPython, JavaScript, and TypeScript runtimes. Sources are never exposed; use extension_id and name with run_workflow.",
            json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
        ),
        tool(
            "run_workflow",
            "Run one active trusted extension workflow through Borg's durable workflow boundary. The selected runtime is declared by the extension and may be embedded Blu/Lua/Luau, Python, IPython, JavaScript, or TypeScript. External runtimes are supervised user processes, not sandboxes; approval and the session permission mode still apply. Use a fresh workflow_id UUID for a new execution; repeated IDs are idempotent.",
            json!({
                "type": "object",
                "properties": {
                    "workflow_id": { "type": "string", "format": "uuid" },
                    "extension_id": { "type": "string", "minLength": 1, "maxLength": 64 },
                    "name": { "type": "string", "minLength": 1, "maxLength": 128 }
                },
                "required": ["workflow_id", "extension_id", "name"],
                "additionalProperties": false
            }),
        ),
        tool(
            "list_blu_workflows",
            "Compatibility alias: list active trusted Blu extension workflows. Use list_workflows for all selectable runtimes.",
            json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
        ),
        tool(
            "run_blu_extension",
            "Run one active trusted Blu extension workflow through Borg's durable, capability-gated host APIs. Use a fresh workflow_id UUID for a new execution; repeated IDs are idempotent. The session's permission mode still applies, and workflows cannot grant themselves access.",
            json!({
                "type": "object",
                "properties": {
                    "workflow_id": {
                        "type": "string",
                        "format": "uuid",
                        "description": "Stable idempotency key for this workflow execution."
                    },
                    "extension_id": { "type": "string", "minLength": 1, "maxLength": 64 },
                    "name": { "type": "string", "minLength": 1, "maxLength": 128 }
                },
                "required": ["workflow_id", "extension_id", "name"],
                "additionalProperties": false
            }),
        ),
        tool(
            "query_history",
            "Search this session's canonical, lossless event journal. Empty text performs fast exact/typed/sequence retrieval; lexical uses the local FTS5 projection; regex is bounded. Results always resolve to canonical event ids and can expand deferred tool payloads. Use this for programmatic recall instead of relying on the compacted model transcript.",
            json!({
                "type": "object",
                "properties": {
                    "text": { "type": "string", "maxLength": crate::session_store::MAX_HISTORY_QUERY_BYTES },
                    "mode": {
                        "type": "string",
                        "enum": ["lexical", "regex"],
                        "default": "lexical"
                    },
                    "prefilter": {
                        "type": "string",
                        "maxLength": crate::session_store::MAX_HISTORY_QUERY_BYTES,
                        "description": "Optional required literal terms used by FTS to narrow regex candidates before matching."
                    },
                    "event_id": { "type": "string", "format": "uuid" },
                    "start_sequence": { "type": "integer", "minimum": 1 },
                    "end_sequence": { "type": "integer", "minimum": 1 },
                    "event_kinds": {
                        "type": "array",
                        "items": { "type": "string", "minLength": 1 },
                        "maxItems": 64
                    },
                    "actors": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": ["user", "assistant", "tool", "system"]
                        },
                        "maxItems": 4
                    },
                    "newest_first": { "type": "boolean", "default": false },
                    "case_sensitive": { "type": "boolean", "default": false },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": crate::session_store::MAX_HISTORY_LIMIT,
                        "default": crate::session_store::DEFAULT_HISTORY_LIMIT
                    },
                    "scan_limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": crate::session_store::MAX_HISTORY_SCAN_LIMIT,
                        "default": crate::session_store::DEFAULT_HISTORY_SCAN_LIMIT
                    },
                    "expand_payloads": { "type": "boolean", "default": false },
                    "max_payload_bytes": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": crate::session_store::MAX_HISTORY_PAYLOAD_BYTES,
                        "default": crate::session_store::DEFAULT_HISTORY_PAYLOAD_BYTES
                    }
                },
                "additionalProperties": false
            }),
        ),
        tool(
            "history_index",
            "Read a bounded, sequence-cursored feed of normalized canonical history documents. Use it to code a task-specific lexical, vector, graph, or BorgSearch retrieval adapter in the persistent runtime; document ids are locators and must be resolved through query_history before treating a hit as authoritative.",
            json!({
                "type": "object",
                "properties": {
                    "after_sequence": { "type": "integer", "minimum": 0, "default": 0 },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 1000, "default": 1000 }
                },
                "additionalProperties": false
            }),
        ),
        tool(
            "consult_model",
            "Ask another configured model for a second opinion. The caller must choose the complete freeform briefing: include whatever context, excerpts, constraints, and questions the other model needs. The response is returned to the calling model for reconciliation; this does not switch the main session provider.",
            json!({
                "type": "object",
                "properties": {
                    "profile": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 128,
                        "description": "Provider alias or model id, optionally with @EFFORT; examples: claude, gpt, claude-opus-5@high, or gpt-5.6-sol@xhigh."
                    },
                    "prompt": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 200000,
                        "description": "The complete freeform briefing to send to the consultant."
                    }
                },
                "required": ["profile", "prompt"],
                "additionalProperties": false
            }),
        ),
        tool(
            "get_provider_capabilities",
            "Read a fresh host-local, secret-free authentication, usage, and admission snapshot before cross-provider collaboration or consultation. It distinguishes authenticated subscriptions, configured API-key routes, and exhausted usage windows. Only providers with can_spawn=true can be used; never ask for or expose credentials.",
            json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
        ),
        tool(
            "get_goal",
            "Get the current durable goal, status, usage, and remaining token budget. Call with {}.",
            json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
        ),
        tool(
            "create_goal",
            "Create a durable goal only when get_goal reports none. Exact call: {\"objective\":\"concise objective\"}. Add token_budget only when the user explicitly requests a token budget.",
            json!({
                "type": "object",
                "properties": {
                    "objective": { "type": "string", "minLength": 1, "maxLength": 4096 },
                    "token_budget": { "type": "integer", "minimum": 1 }
                },
                "required": ["objective"],
                "additionalProperties": false
            }),
        ),
        tool(
            "update_goal",
            "Mark the current goal complete with {\"status\":\"complete\"}, or blocked with {\"status\":\"blocked\"} only after the same blocker prevents progress for three consecutive goal turns.",
            json!({
                "type": "object",
                "properties": {
                    "status": { "type": "string", "enum": ["complete", "blocked"] }
                },
                "required": ["status"],
                "additionalProperties": false
            }),
        ),
        tool(
            "get_plan",
            "Get the current durable task plan. Call with {} before changing an existing plan.",
            json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
        ),
        tool(
            "update_plan",
            "Replace the durable task plan. Exact call: {\"explanation\":\"optional\",\"plan\":[{\"id\":\"UUID from get_plan\",\"content\":\"concise step\",\"status\":\"pending\"}]}. Call get_plan first, reuse exact UUIDs for existing items, and omit id for new items. Use at most 100 items, 500 characters per content, and one in_progress item.",
            update_plan_schema,
        ),
        tool(
            "lsp_status",
            "List Borg's supported and currently active session language servers.",
            json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
        ),
        tool(
            "lsp_diagnostics",
            "Read current language-server diagnostics for a source file. Trusted sessions may use an absolute path in another local project; starts the matching server lazily.",
            lsp_path_schema(),
        ),
        tool(
            "lsp_workspace_diagnostics",
            "Read diagnostics for every document in each active language-server workspace in one call. Optionally provide any supported source path to initialize its language server; trusted sessions may use an absolute path in another local project.",
            json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Optional representative source file. Omit it to query already-active language-server workspaces."
                    }
                },
                "additionalProperties": false
            }),
        ),
        tool(
            "lsp_hover",
            "Read language-server hover/type information at a one-based source position. Trusted sessions may use an absolute path in another local project.",
            lsp_position_schema(),
        ),
        tool(
            "lsp_definition",
            "Find the definition at a one-based source position. Trusted sessions may use an absolute path in another local project.",
            lsp_position_schema(),
        ),
        tool(
            "lsp_references",
            "Find references, including the declaration, at a one-based source position. Trusted sessions may use an absolute path in another local project.",
            lsp_position_schema(),
        ),
        tool(
            "lsp_document_symbols",
            "List language-server symbols in a source file. Trusted sessions may use an absolute path in another local project.",
            lsp_path_schema(),
        ),
        tool(
            "lsp_workspace_symbols",
            "Search symbols across active language-server workspaces.",
            json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string", "maxLength": 512 }
                },
                "required": ["query"],
                "additionalProperties": false
            }),
        ),
    ];
    if web_search_enabled {
        specs.push(web_search_tool_spec());
    }
    if subagents_enabled && consultation_enabled {
        specs.insert(
            1,
            tool(
                "consult_peer",
                "Ask the opposite GPT/Claude model for a private second opinion through its persistent peer thread. The peer keeps its context across calls and the completed response returns only to you for reconciliation. Use this when another viewpoint would materially improve the result; do not ask the human to relay messages, and do not call it reflexively on every turn.",
                json!({
                    "type": "object",
                    "properties": {
                        "prompt": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": 200000,
                            "description": "A concise briefing containing the relevant objective, evidence, constraints, and the exact question for the peer. The peer already remembers prior consultations in its persistent thread."
                        },
                        "profile": {
                            "type": "string",
                            "maxLength": 128,
                            "description": "Optional persistent peer profile such as claude, gpt, claude-opus-5@high, or gpt-5.6-sol@xhigh. Omit to choose the opposite provider automatically."
                        }
                    },
                    "required": ["prompt"],
                    "additionalProperties": false
                }),
            ),
        );
        specs.insert(
            2,
            tool(
                "rotate_peer",
                "Archive the current persistent GPT/Claude peer thread and replace it with a fresh thread. Use this when a different model or reasoning effort is needed; the archived child remains recoverable by its UUID. Optionally provide a concise handoff for the replacement.",
                json!({
                    "type": "object",
                    "properties": {
                        "profile": {
                            "type": "string",
                            "maxLength": 128,
                            "description": "Optional target peer profile such as claude-opus-5@max or gpt-5.6-luna@max. Omit to use the opposite provider default."
                        },
                        "handoff": {
                            "type": "string",
                            "maxLength": 200000,
                            "description": "Optional context to queue as the replacement peer's first prompt."
                        }
                    },
                    "additionalProperties": false
                }),
            ),
        );
    }
    if !consultation_enabled {
        specs.retain(|spec| {
            !matches!(
                spec.get("name").and_then(Value::as_str),
                Some("consult_model" | "consult_peer" | "rotate_peer")
            )
        });
    }
    specs.extend(crate::self_service::tool_specs());
    if shared_work_enabled {
        specs.extend(shared_work_tool_specs());
    }
    if subagents_enabled {
        let mut subagent_specs = subagent_tool_specs(provider);
        if let Some(policy) = team_policy {
            let metadata = serde_json::to_string(policy)
                .unwrap_or_else(|_| "autonomous team policy enabled".to_string());
            if let Some(description) = subagent_specs
                .first_mut()
                .and_then(|spec| spec.get_mut("description"))
                .and_then(|value| value.as_str())
                .map(str::to_owned)
            {
                subagent_specs[0]["description"] = Value::String(format!(
                    "{description} Effective autonomous-team policy: {metadata}"
                ));
            }
        }
        specs.extend(subagent_specs);
    }
    add_action_metadata(&mut specs);
    specs
}

fn web_search_tool_spec() -> Value {
    tool(
        "web_search",
        "Search the public web through Borg's host-configured providers. Auto mode may query Exa, Firecrawl, Parallel, and Brave concurrently and return a deduplicated federated result; credentials never belong in tool input. Results include source URLs, snippets, and publication metadata; use URLs as provenance and do not treat snippets as authoritative page contents.",
        json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": borg_search::MAX_QUERY_CHARS
                },
                "max_results": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": borg_search::MAX_RESULTS,
                    "default": borg_search::DEFAULT_RESULTS
                },
                "include_domains": {
                    "type": "array",
                    "maxItems": borg_search::MAX_DOMAIN_FILTERS,
                    "items": { "type": "string", "minLength": 1, "maxLength": borg_search::MAX_DOMAIN_CHARS }
                },
                "exclude_domains": {
                    "type": "array",
                    "maxItems": borg_search::MAX_DOMAIN_FILTERS,
                    "items": { "type": "string", "minLength": 1, "maxLength": borg_search::MAX_DOMAIN_CHARS }
                }
            },
            "required": ["query"],
            "additionalProperties": false
        }),
    )
}

fn shared_work_tool_specs() -> Vec<Value> {
    let idempotency_key = || {
        json!({
            "type": "string",
            "minLength": 1,
            "maxLength": 256,
            "description": "Caller-stable key. Exact retries return the original event; conflicting reuse is rejected."
        })
    };
    vec![
        tool(
            "list_shared_work",
            "Replay durable shared-work, artifact, decision, review, reference, and provenance events visible to this workspace participant.",
            json!({
                "type": "object",
                "properties": {
                    "after_sequence": { "type": "integer", "minimum": 0 },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 1000 }
                },
                "additionalProperties": false
            }),
        ),
        tool(
            "create_shared_work",
            "Create one durable shared work item in the current workspace.",
            json!({
                "type": "object",
                "properties": {
                    "title": { "type": "string", "minLength": 1 },
                    "detail": { "type": "string" },
                    "idempotency_key": idempotency_key()
                },
                "required": ["title", "idempotency_key"],
                "additionalProperties": false
            }),
        ),
        tool(
            "claim_shared_work",
            "Atomically claim a work item as this agent using the claim event ID currently observed, or null when unclaimed.",
            json!({
                "type": "object",
                "properties": {
                    "work_id": { "type": "string", "format": "uuid" },
                    "expected_claim_id": { "type": "string", "format": "uuid" },
                    "idempotency_key": idempotency_key()
                },
                "required": ["work_id", "idempotency_key"],
                "additionalProperties": false
            }),
        ),
        tool(
            "declare_work_dependency",
            "Declare that one existing work item depends on another existing work item.",
            json!({
                "type": "object",
                "properties": {
                    "work_id": { "type": "string", "format": "uuid" },
                    "depends_on_work_id": { "type": "string", "format": "uuid" },
                    "idempotency_key": idempotency_key()
                },
                "required": ["work_id", "depends_on_work_id", "idempotency_key"],
                "additionalProperties": false
            }),
        ),
        tool(
            "publish_workspace_artifact",
            "Publish a durable artifact reference, optionally attached to a shared work item.",
            json!({
                "type": "object",
                "properties": {
                    "work_id": { "type": "string", "format": "uuid" },
                    "name": { "type": "string", "minLength": 1 },
                    "media_type": { "type": "string" },
                    "uri": { "type": "string", "minLength": 1 },
                    "content_hash": { "type": "string" },
                    "idempotency_key": idempotency_key()
                },
                "required": ["name", "uri", "idempotency_key"],
                "additionalProperties": false
            }),
        ),
        tool(
            "record_workspace_decision",
            "Record a durable workspace decision and optional rationale.",
            json!({
                "type": "object",
                "properties": {
                    "subject": { "type": "string", "minLength": 1 },
                    "outcome": { "type": "string", "minLength": 1 },
                    "rationale": { "type": "string" },
                    "idempotency_key": idempotency_key()
                },
                "required": ["subject", "outcome", "idempotency_key"],
                "additionalProperties": false
            }),
        ),
        tool(
            "request_work_review",
            "Request review of a shared work item, optionally from one workspace participant.",
            json!({
                "type": "object",
                "properties": {
                    "work_id": { "type": "string", "format": "uuid" },
                    "requested_reviewer_id": { "type": "string", "format": "uuid" },
                    "instructions": { "type": "string" },
                    "idempotency_key": idempotency_key()
                },
                "required": ["work_id", "idempotency_key"],
                "additionalProperties": false
            }),
        ),
        tool(
            "record_work_review",
            "Record this participant's durable verdict for a shared work item.",
            json!({
                "type": "object",
                "properties": {
                    "work_id": { "type": "string", "format": "uuid" },
                    "verdict": { "type": "string", "minLength": 1 },
                    "detail": { "type": "string" },
                    "idempotency_key": idempotency_key()
                },
                "required": ["work_id", "verdict", "idempotency_key"],
                "additionalProperties": false
            }),
        ),
        tool(
            "add_workspace_reference",
            "Add a durable named reference to the workspace.",
            json!({
                "type": "object",
                "properties": {
                    "label": { "type": "string", "minLength": 1 },
                    "target": { "type": "string", "minLength": 1 },
                    "idempotency_key": idempotency_key()
                },
                "required": ["label", "target", "idempotency_key"],
                "additionalProperties": false
            }),
        ),
        tool(
            "record_workspace_provenance",
            "Attach durable source provenance to a workspace subject.",
            json!({
                "type": "object",
                "properties": {
                    "subject_id": { "type": "string", "format": "uuid" },
                    "source_kind": { "type": "string", "minLength": 1 },
                    "source_id": { "type": "string", "minLength": 1 },
                    "detail": { "type": "string" },
                    "idempotency_key": idempotency_key()
                },
                "required": ["subject_id", "source_kind", "source_id", "idempotency_key"],
                "additionalProperties": false
            }),
        ),
    ]
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistentRuntimeArgs {
    runtime: Option<String>,
    code: String,
    timeout_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeReadFileArgs {
    path: String,
    offset_line: Option<usize>,
    limit_lines: Option<usize>,
    max_bytes: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeSearchFilesArgs {
    pattern: String,
    path: Option<String>,
    literal: Option<bool>,
    case_sensitive: Option<bool>,
    offset: Option<usize>,
    limit: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeListFilesArgs {
    path: Option<String>,
    limit: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeWriteFileArgs {
    path: String,
    content: String,
    overwrite: Option<bool>,
    create_parent_dirs: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeExecCommandArgs {
    cmd: String,
    workdir: Option<String>,
    yield_time_ms: Option<u64>,
    max_output_tokens: Option<usize>,
    timeout_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeWriteStdinArgs {
    session_id: Uuid,
    chars: Option<String>,
    yield_time_ms: Option<u64>,
    max_output_tokens: Option<usize>,
    terminate: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeEditFileArgs {
    path: String,
    old_text: String,
    new_text: String,
    replace_all: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeBorgToolArgs {
    name: String,
    #[serde(default)]
    arguments: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeMcpCallArgs {
    name: String,
    #[serde(default)]
    arguments: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HistoryIndexArgs {
    after_sequence: Option<u64>,
    limit: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NoArgs {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WebSearchArgs {
    query: String,
    max_results: Option<usize>,
    include_domains: Option<Vec<String>>,
    exclude_domains: Option<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunBluExtensionArgs {
    workflow_id: Uuid,
    extension_id: String,
    name: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunWorkflowArgs {
    workflow_id: Uuid,
    extension_id: String,
    name: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsultModelArgs {
    profile: String,
    prompt: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsultPeerArgs {
    prompt: String,
    #[serde(default)]
    profile: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RotatePeerArgs {
    #[serde(default)]
    profile: Option<String>,
    #[serde(default)]
    handoff: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateGoalArgs {
    objective: String,
    token_budget: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateGoalArgs {
    status: ModelGoalStatus,
}

#[derive(Deserialize)]
struct UpdatePlanArgs {
    #[serde(default)]
    #[allow(dead_code)]
    explanation: Option<String>,
    #[serde(alias = "steps", alias = "items", alias = "todos", alias = "todo_list")]
    plan: Vec<TodoItemUpdate>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LspPathArgs {
    path: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LspWorkspaceDiagnosticsArgs {
    #[serde(default)]
    path: Option<PathBuf>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LspPositionArgs {
    path: PathBuf,
    line: u32,
    character: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LspWorkspaceSymbolArgs {
    query: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListSharedWorkArgs {
    after_sequence: Option<u64>,
    limit: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateSharedWorkArgs {
    title: String,
    detail: Option<String>,
    idempotency_key: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaimSharedWorkArgs {
    work_id: Uuid,
    expected_claim_id: Option<Uuid>,
    idempotency_key: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeclareWorkDependencyArgs {
    work_id: Uuid,
    depends_on_work_id: Uuid,
    idempotency_key: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishWorkspaceArtifactArgs {
    work_id: Option<Uuid>,
    name: String,
    media_type: Option<String>,
    uri: String,
    content_hash: Option<String>,
    idempotency_key: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordWorkspaceDecisionArgs {
    subject: String,
    outcome: String,
    rationale: Option<String>,
    idempotency_key: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestWorkReviewArgs {
    work_id: Uuid,
    requested_reviewer_id: Option<Uuid>,
    instructions: Option<String>,
    idempotency_key: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordWorkReviewArgs {
    work_id: Uuid,
    verdict: String,
    detail: Option<String>,
    idempotency_key: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AddWorkspaceReferenceArgs {
    label: String,
    target: String,
    idempotency_key: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordWorkspaceProvenanceArgs {
    subject_id: Uuid,
    source_kind: String,
    source_id: String,
    detail: Option<String>,
    idempotency_key: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SpawnAgentArgs {
    task_name: String,
    message: String,
    provider: Option<CodingProvider>,
    model: Option<String>,
    reasoning_effort: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListAgentsArgs {
    path_prefix: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MessageArgs {
    target: String,
    message: String,
    #[serde(default)]
    mentions: Vec<StructuredMention>,
    #[serde(default)]
    reply_to_message_id: Option<Uuid>,
}

impl MessageArgs {
    fn options(&self) -> TeamMessageOptions {
        TeamMessageOptions {
            mentions: self.mentions.clone(),
            reply_to_message_id: self.reply_to_message_id,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BroadcastArgs {
    message: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AcknowledgeMessageArgs {
    message_id: Uuid,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TargetArgs {
    target: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitAgentArgs {
    timeout_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EnqueueRuntimeJobArgs {
    idempotency_key: String,
    kind: String,
    payload: Value,
    due_at: Option<String>,
    max_attempts: Option<u32>,
    goal_id: Option<Uuid>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeJobArgs {
    job_id: Uuid,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SaveRuntimeCheckpointArgs {
    job_id: Uuid,
    checkpoint_key: String,
    kind: String,
    state: Value,
    evidence: Value,
}

fn is_autonomy_tool(name: &str) -> bool {
    matches!(
        name,
        "enqueue_runtime_job"
            | "get_runtime_job"
            | "save_runtime_checkpoint"
            | "list_runtime_checkpoints"
    )
}

fn autonomy_tool_specs() -> Vec<Value> {
    vec![
        tool(
            "enqueue_runtime_job",
            "Durably schedule an idempotent provider-neutral runtime job for later execution or verification.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["idempotency_key", "kind", "payload"],
                "properties": {
                    "idempotency_key": {"type": "string", "minLength": 1, "maxLength": 256},
                    "kind": {"type": "string", "minLength": 1, "maxLength": 128},
                    "payload": {"type": "object"},
                    "due_at": {"type": "string", "format": "date-time"},
                    "max_attempts": {"type": "integer", "minimum": 1, "maximum": 32},
                    "goal_id": {"type": "string", "format": "uuid"}
                }
            }),
        ),
        tool(
            "get_runtime_job",
            "Read one durable runtime job owned by this session, including its lease and retry state.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["job_id"],
                "properties": {"job_id": {"type": "string", "format": "uuid"}}
            }),
        ),
        tool(
            "save_runtime_checkpoint",
            "Persist reproducible runtime state and verification evidence for an owned job; repeated keys are idempotent.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["job_id", "checkpoint_key", "kind", "state", "evidence"],
                "properties": {
                    "job_id": {"type": "string", "format": "uuid"},
                    "checkpoint_key": {"type": "string", "minLength": 1, "maxLength": 256},
                    "kind": {"type": "string", "minLength": 1, "maxLength": 128},
                    "state": {"type": "object"},
                    "evidence": {"type": "object"}
                }
            }),
        ),
        tool(
            "list_runtime_checkpoints",
            "List reproducible checkpoints and evidence for an owned runtime job.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["job_id"],
                "properties": {"job_id": {"type": "string", "format": "uuid"}}
            }),
        ),
    ]
}

async fn owned_runtime_job(
    store: &crate::SqliteAutonomyStore,
    session_id: Uuid,
    job_id: Uuid,
) -> Result<crate::AutonomyJob> {
    let job = store
        .get(job_id)
        .await?
        .with_context(|| format!("unknown runtime job {job_id}"))?;
    anyhow::ensure!(
        job.session_id == Some(session_id),
        "runtime job {job_id} belongs to another session"
    );
    Ok(job)
}

async fn call_autonomy_tool(
    store: &crate::SqliteAutonomyStore,
    session_id: Uuid,
    name: &str,
    arguments: Value,
) -> Result<Value> {
    match name {
        "enqueue_runtime_job" => {
            let args: EnqueueRuntimeJobArgs = serde_json::from_value(arguments)?;
            let due_at = args
                .due_at
                .as_deref()
                .map(DateTime::parse_from_rfc3339)
                .transpose()
                .context("due_at must be an RFC3339 timestamp")?
                .map(|value| value.with_timezone(&Utc))
                .unwrap_or_else(Utc::now);
            let job = store
                .enqueue(crate::EnqueueAutonomyJob {
                    job_id: None,
                    idempotency_key: args.idempotency_key,
                    kind: args.kind,
                    payload: args.payload,
                    due_at,
                    max_attempts: args.max_attempts.unwrap_or(3).clamp(1, 32),
                    session_id: Some(session_id),
                    goal_id: args.goal_id,
                })
                .await?;
            Ok(serde_json::to_value(job)?)
        }
        "get_runtime_job" => {
            let args: RuntimeJobArgs = serde_json::from_value(arguments)?;
            Ok(serde_json::to_value(
                owned_runtime_job(store, session_id, args.job_id).await?,
            )?)
        }
        "save_runtime_checkpoint" => {
            let args: SaveRuntimeCheckpointArgs = serde_json::from_value(arguments)?;
            let job = owned_runtime_job(store, session_id, args.job_id).await?;
            let checkpoint = store
                .save_checkpoint(crate::SaveAutonomyCheckpoint {
                    checkpoint_id: None,
                    job_id: args.job_id,
                    checkpoint_key: args.checkpoint_key,
                    session_id: Some(session_id),
                    goal_id: job.goal_id,
                    kind: args.kind,
                    state: args.state,
                    evidence: args.evidence,
                    created_at: Utc::now(),
                })
                .await?;
            Ok(serde_json::to_value(checkpoint)?)
        }
        "list_runtime_checkpoints" => {
            let args: RuntimeJobArgs = serde_json::from_value(arguments)?;
            owned_runtime_job(store, session_id, args.job_id).await?;
            Ok(serde_json::to_value(
                store.list_checkpoints(args.job_id).await?,
            )?)
        }
        _ => bail!("unknown autonomous runtime tool `{name}`"),
    }
}

fn goal_response(
    response: std::result::Result<crate::SessionGoalToolResponse, String>,
) -> Result<Value> {
    response
        .map_err(anyhow::Error::msg)
        .and_then(|response| serde_json::to_value(response).map_err(Into::into))
}

fn todo_response(
    response: std::result::Result<crate::SessionTodoToolResponse, String>,
) -> Result<Value> {
    response
        .map_err(anyhow::Error::msg)
        .and_then(|response| serde_json::to_value(response).map_err(Into::into))
}

fn add_action_metadata(specs: &mut [Value]) {
    for spec in specs {
        let Some(schema) = spec.get_mut("inputSchema").and_then(Value::as_object_mut) else {
            continue;
        };
        let Some(properties) = schema
            .entry("properties")
            .or_insert_with(|| json!({}))
            .as_object_mut()
        else {
            continue;
        };
        let mut existing = std::mem::take(properties);
        existing.remove("action");
        properties.insert(
            "action".to_string(),
            json!({
                "type": "string",
                "minLength": 1,
                "maxLength": 64,
                "description": "One- or two-word summary for the live UI. Emit this as the first argument field. Presentation metadata only; it does not affect tool execution."
            }),
        );
        properties.extend(existing);
        if let Some(required) = schema
            .entry("required")
            .or_insert_with(|| json!([]))
            .as_array_mut()
        {
            required.retain(|field| field != "action");
        }
    }
}

fn tool(name: &str, description: &str, input_schema: Value) -> Value {
    json!({ "name": name, "description": description, "inputSchema": input_schema })
}

fn lsp_path_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "minLength": 1,
                "description": "Session-relative or absolute source path; trusted sessions may target another local project."
            }
        },
        "required": ["path"],
        "additionalProperties": false
    })
}

fn lsp_position_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "minLength": 1,
                "description": "Session-relative or absolute source path; trusted sessions may target another local project."
            },
            "line": { "type": "integer", "minimum": 1 },
            "character": { "type": "integer", "minimum": 1 }
        },
        "required": ["path", "line", "character"],
        "additionalProperties": false
    })
}

fn message_tool(name: &str, description: &str) -> Value {
    tool(
        name,
        description,
        json!({
            "type": "object",
            "properties": {
                "target": { "type": "string" },
                "message": { "type": "string" },
                "mentions": { "type": "array" },
                "reply_to_message_id": { "type": "string" }
            },
            "required": ["target", "message"],
            "additionalProperties": false
        }),
    )
}

fn target_schema() -> Value {
    json!({
        "type": "object",
        "properties": { "target": { "type": "string" } },
        "required": ["target"],
        "additionalProperties": false
    })
}

fn canonical_task_name(task_name: &str) -> Result<String> {
    let task_name = task_name.trim();
    if task_name.is_empty()
        || task_name.len() > 64
        || !task_name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        bail!("task_name must contain 1-64 lowercase letters, digits, or underscores");
    }
    Ok(format!("/root/{task_name}"))
}

fn canonical_sidecar_task_name(task_name: &str) -> Result<String> {
    let task_name = task_name.trim();
    let task_name = task_name.strip_prefix("/root/").unwrap_or(task_name);
    canonical_task_name(task_name)
}

fn required_message(message: &str) -> Result<String> {
    let message = message.trim();
    if message.is_empty() {
        bail!("subagent message must not be empty");
    }
    Ok(message.to_string())
}

fn parse_session_message_target(target: &str) -> Result<Uuid> {
    let target = target.trim();
    let target = target.strip_prefix("session:").unwrap_or(target);
    Uuid::parse_str(target).with_context(|| {
        "unknown team target; use a visible task name or session:<UUID> for an independent session"
    })
}

fn parse_workspace_participant_target(target: &str) -> Result<Option<Uuid>> {
    let target = target.trim();
    let Some(target) = target.strip_prefix("participant:") else {
        return Ok(None);
    };
    Ok(Some(Uuid::parse_str(target).with_context(
        || "workspace participant target must be participant:<UUID>",
    )?))
}

fn routed_message_json(routed: RoutedTeamMessage, accepted_field: &str) -> Value {
    let delivery_state = if routed.dispatched_locally {
        "dispatched"
    } else if routed.relay_pending {
        "relay_pending"
    } else {
        "queued_offline"
    };
    let mut value = match routed.receipt {
        Some(receipt) => json!({
            "message_id": receipt.message_id,
            "workspace_id": receipt.workspace_id,
            "sequence": receipt.sequence,
            "recipient_count": receipt.recipient_ids.len(),
            "recipient_ids": receipt.recipient_ids,
            "delivery_mode": receipt.mode,
            "dispatched_locally": routed.dispatched_locally,
            "relay_pending": routed.relay_pending,
            "delivery_state": delivery_state,
        }),
        None => json!({
            "recipient_count": 1,
            "dispatched_locally": routed.dispatched_locally,
            "relay_pending": routed.relay_pending,
            "delivery_state": delivery_state,
        }),
    };
    value[accepted_field] = Value::Bool(true);
    value
}

fn required_idempotency_key(value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        bail!("idempotency_key must not be empty");
    }
    if value.len() > 256 {
        bail!("idempotency_key must be at most 256 bytes");
    }
    Ok(value.to_string())
}

fn required_tool_text(field: &str, value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        bail!("{field} must not be empty");
    }
    Ok(value.to_string())
}

fn optional_tool_text(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim();
        (!value.is_empty()).then(|| value.to_string())
    })
}

fn attributed_team_message(actor: &str, message: &str) -> String {
    format!("Team message from {actor}:\n\n{message}")
}

fn child_lock_path(root: &Path, session_id: Uuid) -> PathBuf {
    root.join("subagents").join(format!("{session_id}.lock"))
}

async fn send_prompt(
    entry: &SubagentEntry,
    session_id: Uuid,
    message: TeamInboxMessage,
) -> Result<()> {
    entry
        .commands
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("subagent {} is still starting", entry.snapshot.task_name))?
        .send(HostCommand::TeamPrompt {
            session_id,
            message_id: message.message_id,
            text: message.text,
            attachments: Vec::new(),
            output_schema: None,
            delivery: message.delivery,
        })
        .await
        .map_err(|_| anyhow::anyhow!("subagent command channel closed"))
}

async fn update_from_session_event(
    table: &Arc<Mutex<SubagentTable>>,
    session_id: Uuid,
    event: &SessionEvent,
) {
    let mut table = table.lock().await;
    let Some(entry) = table.entries.get_mut(&session_id) else {
        return;
    };
    match &event.kind {
        SessionEventKind::StatusChanged { status, detail } => {
            entry.assignment_claimed = false;
            entry.snapshot.status = match status {
                SessionStatus::Starting => SubagentStatus::Starting,
                SessionStatus::Running => SubagentStatus::Running,
                SessionStatus::WaitingForApproval => SubagentStatus::WaitingForApproval,
                SessionStatus::Ready | SessionStatus::Completed => SubagentStatus::Ready,
                SessionStatus::Failed => SubagentStatus::Failed,
                SessionStatus::Stopped => SubagentStatus::Stopped,
            };
            entry.snapshot.detail = detail.clone();
        }
        SessionEventKind::Message {
            actor: EventActor::Assistant,
            text,
            status: MessageStatus::Complete,
            ..
        } => entry.snapshot.final_text = Some(text.clone()),
        SessionEventKind::UsageUpdated {
            input_tokens,
            output_tokens,
            total_tokens,
            context_tokens,
            cost_microusd,
            cost_basis,
            ..
        } => {
            entry.snapshot.usage.input_tokens = entry
                .snapshot
                .usage
                .input_tokens
                .saturating_add(*input_tokens);
            entry.snapshot.usage.output_tokens = entry
                .snapshot
                .usage
                .output_tokens
                .saturating_add(*output_tokens);
            entry.snapshot.usage.total_tokens = entry
                .snapshot
                .usage
                .total_tokens
                .saturating_add(*total_tokens);
            if context_tokens.is_some() {
                entry.snapshot.usage.context_tokens = *context_tokens;
            }
            entry.snapshot.usage.cost_microusd =
                match (entry.snapshot.usage.cost_microusd, cost_microusd) {
                    (Some(current), Some(additional)) => Some(current.saturating_add(*additional)),
                    (None, Some(value)) => Some(*value),
                    (current, None) => current,
                };
            entry.snapshot.usage.cost_basis = cost_basis.clone();
        }
        SessionEventKind::ContextWindowUpdated { context_tokens, .. } => {
            entry.snapshot.usage.context_tokens = Some(*context_tokens);
        }
        _ => {}
    }
    entry.snapshot.updated_at = event.created_at;
}

fn project_child_state(snapshot: &mut SubagentSnapshot, state: &crate::SessionState) {
    if let Some(status) = state.status {
        snapshot.status = match status {
            SessionStatus::Starting => SubagentStatus::Starting,
            SessionStatus::Running => SubagentStatus::Running,
            SessionStatus::WaitingForApproval => SubagentStatus::WaitingForApproval,
            SessionStatus::Ready | SessionStatus::Completed => SubagentStatus::Ready,
            SessionStatus::Failed => SubagentStatus::Failed,
            SessionStatus::Stopped => SubagentStatus::Stopped,
        };
        snapshot.detail = state.status_detail.clone();
    }
    if let Some(updated_at) = state.activity_at {
        snapshot.updated_at = updated_at;
    }
    snapshot.final_text = state.latest_response.clone();
    snapshot.usage = SubagentUsage {
        input_tokens: state.usage.input_tokens,
        output_tokens: state.usage.output_tokens,
        total_tokens: state.usage.total_tokens,
        context_tokens: state.usage.context_tokens,
        cost_microusd: state.usage.cost_microusd,
        cost_basis: state.usage.cost_basis.clone(),
    };
}

fn significant_activity(activity: &SubagentActivity) -> bool {
    match activity {
        SubagentActivity::Started { .. }
        | SubagentActivity::Stopped { .. }
        | SubagentActivity::Failed { .. }
        | SubagentActivity::Completed { .. } => true,
        SubagentActivity::SessionEvent { event, .. } => matches!(
            event.kind,
            SessionEventKind::ApprovalRequested { .. }
                | SessionEventKind::StatusChanged {
                    status: SessionStatus::Failed | SessionStatus::Stopped,
                    ..
                }
        ),
    }
}

fn ready_session_id(activity: &SubagentActivity) -> Option<Uuid> {
    match activity {
        SubagentActivity::SessionEvent { event, .. }
            if matches!(
                event.kind,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Ready | SessionStatus::Completed,
                    ..
                }
            ) =>
        {
            Some(event.session_id)
        }
        _ => None,
    }
}

async fn finish_agent(
    table: &Arc<Mutex<SubagentTable>>,
    session_id: Uuid,
    error: Option<anyhow::Error>,
) -> Option<SubagentActivity> {
    let mut table = table.lock().await;
    let entry = table.entries.get_mut(&session_id)?;
    entry.snapshot.status = if error.is_some() {
        SubagentStatus::Failed
    } else {
        SubagentStatus::Stopped
    };
    entry.snapshot.detail = error.map(|error| format!("{error:#}"));
    entry.snapshot.updated_at = Utc::now();
    entry.commands = None;
    Some(if entry.snapshot.status == SubagentStatus::Failed {
        SubagentActivity::Failed {
            agent: entry.snapshot.clone(),
        }
    } else {
        SubagentActivity::Stopped {
            agent: entry.snapshot.clone(),
        }
    })
}

#[cfg(test)]
mod tests;

/// Unix domain sockets have a hard limit on their path: `sun_path` is 104 bytes
/// on macOS and the BSDs, 108 on Linux. The session runtime directory can
/// easily exceed that on macOS, where the per-user temporary directory alone is
/// ~48 bytes before any session identifier is appended — so binding inside it
/// fails with `path must be shorter than SUN_LEN` and takes the whole session
/// down with it.
///
/// Prefer the runtime directory, because keeping the socket beside the rest of
/// the session state is what makes cleanup and inspection obvious. Fall back to
/// a short private directory only when the preferred path will not fit.
#[cfg(unix)]
const MAX_UNIX_SOCKET_PATH_BYTES: usize = 100;

#[cfg(unix)]
fn unix_socket_path_fits(path: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes().len() < MAX_UNIX_SOCKET_PATH_BYTES
}

#[cfg(unix)]
fn agent_tool_socket_path(runtime_dir: &Path, session_id: Uuid) -> Result<PathBuf> {
    // `simple` drops the hyphens, buying four bytes for free.
    let name = format!("{}.sock", session_id.simple());
    let preferred = runtime_dir.join(&name);
    if unix_socket_path_fits(&preferred) {
        return Ok(preferred);
    }
    let short = short_socket_dir()?;
    let fallback = short.join(&name);
    anyhow::ensure!(
        unix_socket_path_fits(&fallback),
        "agent tool socket path is too long even under {}",
        short.display()
    );
    Ok(fallback)
}

/// Create a fresh, private directory with a short path.
///
/// `create_dir` fails if anything already exists at the name — including a
/// symlink — so a successful call proves we created it and therefore own it.
/// That is what makes this safe to place under a world-writable `/tmp`.
#[cfg(unix)]
fn short_socket_dir() -> Result<PathBuf> {
    let base = Path::new("/tmp");
    for _ in 0..8 {
        let candidate = base.join(format!(
            ".borg-{}",
            &Uuid::new_v4().simple().to_string()[..12]
        ));
        match std::fs::create_dir(&candidate) {
            Ok(()) => {
                std::fs::set_permissions(&candidate, Permissions::from_mode(0o700))
                    .with_context(|| format!("failed to secure {}", candidate.display()))?;
                return Ok(candidate);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to create {}", candidate.display()));
            }
        }
    }
    anyhow::bail!("could not create a short directory for the agent tool socket")
}

#[cfg(all(test, unix))]
mod socket_path_tests {
    use super::*;

    #[test]
    fn a_short_runtime_directory_is_used_directly() {
        let session = Uuid::new_v4();
        let path = agent_tool_socket_path(Path::new("/tmp/borg-test"), session).expect("path");
        assert!(path.starts_with("/tmp/borg-test"));
        assert!(unix_socket_path_fits(&path));
    }

    #[test]
    fn an_overlong_runtime_directory_falls_back_to_a_short_path() {
        let deep = PathBuf::from(format!("/tmp/{}", "d".repeat(120)));
        let session = Uuid::new_v4();
        let path = agent_tool_socket_path(&deep, session).expect("path");
        assert!(
            !path.starts_with(&deep),
            "should not use the overlong directory"
        );
        assert!(
            unix_socket_path_fits(&path),
            "fallback must fit in sun_path: {}",
            path.display()
        );
        // The fallback directory is created privately; clean it up.
        if let Some(parent) = path.parent() {
            let mode = std::fs::metadata(parent).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o700, "fallback directory must be private");
            std::fs::remove_dir_all(parent).ok();
        }
    }

    #[test]
    fn the_macos_socket_limit_is_respected() {
        // The exact case that failed: macOS per-user temp + session subdirectory.
        let runtime = PathBuf::from(
            "/var/folders/h0/twccxwr971j2bs699hk_k9w00000gn/T/.tmp9Neylt/agent-tools",
        );
        let path = agent_tool_socket_path(&runtime, Uuid::new_v4()).expect("path");
        assert!(
            unix_socket_path_fits(&path),
            "{} is too long",
            path.display()
        );
        if let Some(parent) = path.parent()
            && parent.starts_with("/tmp/.borg-")
        {
            std::fs::remove_dir_all(parent).ok();
        }
    }
}
