use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
#[cfg(unix)]
use std::{fs::Permissions, os::unix::fs::PermissionsExt};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
#[cfg(not(unix))]
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use ts_rs::TS;
use uuid::Uuid;

use crate::{
    ApprovalDecision, Audience, CodingProvider, DeliveryMode, EventActor, HostCommand,
    LaunchSession, MessageStatus, ModelGoalStatus, PromptDelivery, SessionEvent, SessionEventKind,
    SessionGoalToolRequest, SessionGoalTools, SessionStatus, SessionStore, SessionTodoToolRequest,
    SessionTodoTools, SqliteWorkspaceStore, StructuredMention, TodoItemUpdate, WorkspaceEvent,
    WorkspaceEventKind, WorkspaceMessage, WorkspaceMessageBody, WorkspaceStore,
};

pub const DEFAULT_MAX_SUBAGENTS: usize = 3;

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
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(default)]
#[ts(export)]
pub struct SubagentUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    pub cost_microusd: Option<u64>,
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

/// One provider-neutral model-tool dispatcher for durable goals and child
/// sessions. Provider adapters should transport this catalog, not implement
/// their own goal or collaboration semantics.
#[derive(Clone)]
pub struct AgentToolDispatcher {
    goals: SessionGoalTools,
    todos: SessionTodoTools,
    subagents: Option<SubagentCoordinator>,
    subagents_enabled: bool,
    lsp: crate::LspService,
    provider: CodingProvider,
    actor_session_id: Uuid,
    team_policy: Option<crate::TeamPolicy>,
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
    team_policy: Option<crate::TeamPolicy>,
    cancel: CancellationToken,
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
        let socket_path = runtime_dir.join(format!("{session_id}.sock"));
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
        let team_policy = dispatcher.team_policy.clone();
        tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    accepted = listener.accept() => accepted,
                    _ = server_cancel.cancelled() => break,
                };
                let Ok((stream, _)) = accepted else { break };
                let dispatcher = dispatcher.clone();
                tokio::spawn(serve_agent_tool_connection(stream, dispatcher, None));
            }
            let _ = std::fs::remove_file(cleanup_path);
        });
        Ok(Self {
            socket_path,
            provider,
            subagents_enabled,
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
                ));
            }
        });
        Ok(Self {
            tcp_addr,
            token,
            provider,
            subagents_enabled,
            team_policy,
            cancel,
        })
    }

    pub fn external_mcp_server(&self) -> borg_provider::mcp::ExternalMcpServer {
        let mut env = BTreeMap::new();
        env.insert(
            "BORG_AGENT_TOOL_PROVIDER".to_string(),
            self.provider.catalog_backend().to_string(),
        );
        if let Some(policy) = &self.team_policy {
            if let Ok(policy) = serde_json::to_string(policy) {
                env.insert("BORG_AGENT_TEAM_POLICY".to_string(), policy);
            }
        }
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
        borg_provider::mcp::ExternalMcpServer {
            name: "borg_agent".to_string(),
            command: std::env::current_exe()
                .expect("Borg cannot expose session tools without its executable path")
                .to_string_lossy()
                .into_owned(),
            args: vec!["__agent-mcp".to_string()],
            env,
            allowed_tools: agent_tool_specs_with_subagents(self.provider, self.subagents_enabled)
                .into_iter()
                .filter_map(|tool| {
                    tool["name"]
                        .as_str()
                        .map(|name| format!("mcp__borg_agent__{name}"))
                })
                .collect(),
        }
    }
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
}

async fn serve_agent_tool_connection<S>(
    stream: S,
    dispatcher: AgentToolDispatcher,
    expected_token: Option<String>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (read, mut write) = tokio::io::split(stream);
    let mut lines = BufReader::new(read).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let response = match serde_json::from_str::<AgentToolWireRequest>(&line) {
            Ok(request)
                if expected_token
                    .as_ref()
                    .is_some_and(|token| request.token.as_ref() != Some(token)) =>
            {
                json!({ "error": "agent tool authentication failed" })
            }
            Ok(request) => match dispatcher.call(&request.name, request.arguments).await {
                Ok(result) => json!({ "result": result }),
                Err(error) => json!({ "error": format!("{error:#}") }),
            },
            Err(error) => json!({ "error": error.to_string() }),
        };
        if write
            .write_all(format!("{response}\n").as_bytes())
            .await
            .is_err()
        {
            break;
        }
    }
}

impl AgentToolDispatcher {
    pub fn new(
        goals: SessionGoalTools,
        todos: SessionTodoTools,
        subagents: Option<SubagentCoordinator>,
        lsp: crate::LspService,
        provider: CodingProvider,
        actor_session_id: Uuid,
        subagents_enabled: bool,
        team_policy: Option<crate::TeamPolicy>,
    ) -> Self {
        Self {
            goals,
            todos,
            subagents,
            subagents_enabled,
            lsp,
            provider,
            actor_session_id,
            team_policy,
        }
    }

    pub fn specs(&self) -> Vec<Value> {
        agent_tool_specs_with_team_policy(
            self.provider,
            self.subagents_enabled,
            self.team_policy.as_ref(),
        )
    }

    pub async fn call(&self, name: &str, arguments: Value) -> Result<Value> {
        match name {
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
            "lsp_diagnostics" => {
                let args: LspPathArgs = serde_json::from_value(arguments)?;
                self.lsp.diagnostics(&args.path).await
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
}

struct SubagentEntry {
    snapshot: SubagentSnapshot,
    commands: Option<mpsc::Sender<HostCommand>>,
    inbox: Vec<TeamInboxMessage>,
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
            .filter(|entry| !entry.snapshot.status.is_terminal())
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
            },
        );
        Ok(snapshot)
    }

    fn resolve(&self, target: &str) -> Result<Uuid> {
        let target = target.trim();
        if target == "/root" || target == "root" {
            return Ok(self.root_session_id);
        }
        if let Ok(id) = Uuid::parse_str(target)
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
    pub delivery: PromptDelivery,
}

#[derive(Debug, Clone, Default)]
pub struct TeamMessageOptions {
    pub mentions: Vec<StructuredMention>,
    pub reply_to_message_id: Option<Uuid>,
}

/// Borg-native child sessions for one root CLI session.
///
/// Each child reuses the canonical session actor with its own provider context
/// and store identity. This layer only owns topology, bounded admission, messaging,
/// and the event projection consumed by terminal and Remote adapters.
#[derive(Clone)]
pub struct SubagentCoordinator {
    journal_root: PathBuf,
    root_launch: LaunchSession,
    executor: Arc<dyn crate::AgentTurnExecutor>,
    store: Arc<dyn SessionStore>,
    table: Arc<Mutex<SubagentTable>>,
    activity_tx: broadcast::Sender<SubagentActivity>,
    root_inbox: Arc<Mutex<Vec<TeamInboxMessage>>>,
    root_message_tx: broadcast::Sender<TeamInboxMessage>,
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
            root_launch,
            executor,
            store,
            table: Arc::new(Mutex::new(SubagentTable {
                root_session_id,
                max_children,
                entries: HashMap::new(),
                task_names: HashMap::new(),
            })),
            activity_tx,
            root_inbox: Arc::new(Mutex::new(Vec::new())),
            root_message_tx,
        })
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

    async fn persist_team_message(
        &self,
        actor_session_id: Uuid,
        recipient_session_id: Uuid,
        actor: &str,
        message: &str,
        prompt_delivery: PromptDelivery,
        delivery_mode: DeliveryMode,
        options: TeamMessageOptions,
    ) -> Result<TeamInboxMessage> {
        if !self.root_launch.capabilities.multiplayer {
            return Ok(TeamInboxMessage {
                message_id: Uuid::new_v4(),
                text: attributed_team_message(actor, message),
                delivery: prompt_delivery,
            });
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
        anyhow::ensure!(
            actor_binding.workspace_id == recipient_binding.workspace_id,
            "team participants are attached to different workspaces"
        );
        let text = attributed_team_message(actor, message);
        let message_id = Uuid::new_v4();
        let created_at = Utc::now();
        let workspace_store =
            SqliteWorkspaceStore::open(self.journal_root.join("workspaces.sqlite3")).await?;
        workspace_store
            .append(WorkspaceEvent {
                id: message_id,
                workspace_id: actor_binding.workspace_id,
                sequence: 0,
                author_id: actor_binding.participant_id,
                idempotency_key: format!("team-message:{message_id}"),
                created_at,
                kind: WorkspaceEventKind::Message {
                    message: WorkspaceMessage {
                        id: message_id,
                        workspace_id: actor_binding.workspace_id,
                        thread_id: None,
                        reply_to_message_id: options.reply_to_message_id,
                        author_id: actor_binding.participant_id,
                        body: WorkspaceMessageBody {
                            text: text.clone(),
                            mentions: options.mentions,
                        },
                        audience: Audience::Direct {
                            participant: recipient_binding.participant_id,
                        },
                        created_at,
                    },
                    mode: delivery_mode,
                },
            })
            .await?;
        Ok(TeamInboxMessage {
            message_id,
            text,
            delivery: prompt_delivery,
        })
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
        let workspace_store =
            SqliteWorkspaceStore::open(self.journal_root.join("workspaces.sqlite3")).await?;
        let messages = workspace_store
            .pending_message_events(binding.workspace_id, binding.participant_id, 10_000)
            .await?
            .into_iter()
            .filter_map(|(event, delivery)| {
                let WorkspaceEventKind::Message { message, .. } = event.kind else {
                    return None;
                };
                Some(TeamInboxMessage {
                    message_id: message.id,
                    text: message.body.text,
                    delivery: match delivery.mode {
                        DeliveryMode::Boundary | DeliveryMode::Wake => PromptDelivery::Steer,
                        DeliveryMode::NextTurn | DeliveryMode::Notify => PromptDelivery::Queue,
                    },
                })
            })
            .collect::<Vec<_>>();
        Ok(messages)
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
        let store =
            SqliteWorkspaceStore::open(self.journal_root.join("workspaces.sqlite3")).await?;
        let _ = store.pending_message_events(binding.workspace_id, binding.participant_id, 10_000).await?
            .into_iter().find(|(event, _)| matches!(&event.kind, WorkspaceEventKind::Message { message, .. } if message.id == message_id))
            .context("unread team message not found")?;
        store
            .transition_message_delivery(
                binding.workspace_id,
                message_id,
                binding.participant_id,
                crate::DeliveryState::Admitted,
                None,
            )
            .await?;
        store
            .transition_message_delivery(
                binding.workspace_id,
                message_id,
                binding.participant_id,
                crate::DeliveryState::Acknowledged,
                None,
            )
            .await?;
        Ok(())
    }

    /// Rebuild the coordinator projection from the durable parent event
    /// stream, then resume non-terminal children from the shared typed store.
    ///
    /// Parent `SubagentActivity` events remain the topology authority; child
    /// projections only supply each child actor's conversational state.
    pub async fn restore_from_events(&self, events: &[SessionEvent]) -> Result<()> {
        let mut latest = HashMap::<Uuid, SubagentSnapshot>::new();
        for event in events {
            if let SessionEventKind::SubagentActivity { agent, .. } = &event.kind {
                latest.insert(agent.session_id, agent.clone());
            }
        }
        let mut resumable = Vec::new();
        let mut recovery_failures = Vec::new();
        let root_session_id = self.table.lock().await.root_session_id;
        for mut snapshot in latest.into_values() {
            if snapshot.parent_session_id != root_session_id {
                continue;
            }
            let actor_path = child_journal_path(&self.journal_root, snapshot.session_id);
            let mut recovery_failed = false;
            if !snapshot.status.is_terminal() {
                let recovered = async {
                    let writer = crate::SessionWriterLease::try_acquire(&actor_path)?
                        .with_context(|| {
                            format!("subagent session {} is already active", snapshot.session_id)
                        })?;
                    self.store
                        .register_child_session(
                            root_session_id,
                            snapshot.session_id,
                            &actor_path,
                            &writer,
                        )
                        .await?;
                    self.store.state(snapshot.session_id).await
                }
                .await;
                match recovered {
                    Ok(state) if state.latest_sequence > 0 => {
                        project_child_state(&mut snapshot, &state);
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
                    },
                );
            }
            if recovery_failed {
                recovery_failures.push(snapshot);
            } else if !snapshot.status.is_terminal() {
                resumable.push(snapshot);
            }
        }
        for snapshot in recovery_failures {
            let _ = self
                .activity_tx
                .send(SubagentActivity::Failed { agent: snapshot });
        }
        for snapshot in resumable {
            let mut launch = self.root_launch.clone();
            launch.request_id = Uuid::new_v4();
            launch.initial_prompt = None;
            launch.provider = snapshot.provider;
            launch.model = snapshot.model.clone();
            launch.effort = snapshot.effort.clone();
            launch.cwd = snapshot.cwd.clone();
            launch.name = Some(snapshot.task_name.clone());
            self.start_reserved(snapshot, launch, false).await?;
        }
        for message in self.pending_messages_for_session(root_session_id).await? {
            if let Err(error) = self.root_message_tx.send(message) {
                self.root_inbox.lock().await.push(error.0);
            }
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
            let mut table = self.table.lock().await;
            let Some(entry) = table.entries.get_mut(&child_id) else {
                continue;
            };
            for message in messages {
                send_prompt(entry, child_id, message).await?;
            }
        }
        Ok(())
    }

    pub async fn spawn(&self, request: SpawnSubagent) -> Result<SubagentSnapshot> {
        let message = required_message(&request.message)?;
        let mut launch = self.root_launch.clone();
        launch.request_id = Uuid::new_v4();
        launch.initial_prompt = Some(message);
        launch.provider = request.provider.unwrap_or(launch.provider);
        validate_subagent_overrides(
            launch.provider,
            request.model.as_deref(),
            request.effort.as_deref(),
        )?;
        if request.model.is_some() {
            launch.model = request.model;
        }
        launch.effort = effective_worker_effort(&launch, request.effort);
        launch.name = Some(canonical_task_name(&request.task_name)?);

        let snapshot = self
            .table
            .lock()
            .await
            .reserve(&request.task_name, &launch)?;
        self.start_reserved(snapshot.clone(), launch, true).await?;
        Ok(snapshot)
    }

    async fn start_reserved(
        &self,
        snapshot: SubagentSnapshot,
        launch: LaunchSession,
        announce: bool,
    ) -> Result<()> {
        let (command_tx, command_rx) = mpsc::channel(64);
        let (event_tx, mut event_rx) = mpsc::channel(256);
        let actor_path = child_journal_path(&self.journal_root, snapshot.session_id);
        let actor_session_id = snapshot.session_id;
        let writer = crate::SessionWriterLease::try_acquire(&actor_path)?
            .with_context(|| format!("subagent session {actor_session_id} is already active"))?;
        self.store
            .register_child_session(
                snapshot.parent_session_id,
                actor_session_id,
                &actor_path,
                &writer,
            )
            .await?;
        if self.root_launch.capabilities.multiplayer {
            let binding = self
                .store
                .workspace_binding(actor_session_id)
                .await?
                .with_context(|| {
                    format!("subagent session {actor_session_id} has no workspace binding")
                })?;
            let workspace_store =
                SqliteWorkspaceStore::open(self.journal_root.join("workspaces.sqlite3")).await?;
            let human_participant_id =
                Uuid::new_v5(&binding.workspace_id, b"borg-local-human-participant");
            let human_display_name =
                std::env::var("USER").unwrap_or_else(|_| "Local user".to_string());
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
        self.table
            .lock()
            .await
            .entries
            .get_mut(&snapshot.session_id)
            .expect("reserved subagent exists")
            .commands = Some(command_tx);
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
                let _ = activity_tx.send(activity);
            }
        });
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

    /// Queue a message without waking an idle child.
    pub async fn send_message(&self, target: &str, message: &str) -> Result<()> {
        let root_session_id = self.table.lock().await.root_session_id;
        self.send_message_as(root_session_id, target, message).await
    }

    /// Append one workspace message and fan its single ID out to every visible team member.
    pub async fn broadcast_message_as(
        &self,
        actor_session_id: Uuid,
        message: &str,
    ) -> Result<Uuid> {
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
        let mut participant_ids = Vec::with_capacity(recipients.len());
        for recipient in &recipients {
            let binding = self
                .store
                .workspace_binding(*recipient)
                .await?
                .context("team recipient has no workspace")?;
            anyhow::ensure!(
                binding.workspace_id == sender.workspace_id,
                "team participants are attached to different workspaces"
            );
            participant_ids.push(binding.participant_id);
        }
        let message_id = Uuid::new_v4();
        let created_at = Utc::now();
        SqliteWorkspaceStore::open(self.journal_root.join("workspaces.sqlite3"))
            .await?
            .append(WorkspaceEvent {
                id: message_id,
                workspace_id: sender.workspace_id,
                sequence: 0,
                author_id: sender.participant_id,
                idempotency_key: format!("team-broadcast:{message_id}"),
                created_at,
                kind: WorkspaceEventKind::Message {
                    message: WorkspaceMessage {
                        id: message_id,
                        workspace_id: sender.workspace_id,
                        thread_id: None,
                        reply_to_message_id: None,
                        author_id: sender.participant_id,
                        body: WorkspaceMessageBody {
                            text: attributed_team_message(&actor, &message),
                            mentions: Vec::new(),
                        },
                        audience: Audience::Participants {
                            participants: participant_ids,
                        },
                        created_at,
                    },
                    mode: DeliveryMode::NextTurn,
                },
            })
            .await?;
        let inbox = TeamInboxMessage {
            message_id,
            text: attributed_team_message(&actor, &message),
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
        Ok(message_id)
    }

    /// Queue a team-attributed message without waking an idle recipient.
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
        let message = required_message(message)?;
        let (actor, id, root_session_id, status) = {
            let table = self.table.lock().await;
            let actor = table.task_name(actor_session_id)?;
            let id = table.resolve(target)?;
            let status = table.entries.get(&id).map(|entry| entry.snapshot.status);
            (actor, id, table.root_session_id, status)
        };
        if status.is_some_and(SubagentStatus::is_terminal) {
            bail!("subagent {target} is not running");
        }
        let inbox_message = self
            .persist_team_message(
                actor_session_id,
                id,
                &actor,
                &message,
                PromptDelivery::Queue,
                DeliveryMode::NextTurn,
                options,
            )
            .await?;
        if id == root_session_id {
            self.root_inbox.lock().await.push(inbox_message);
            return Ok(());
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
            send_prompt(entry, id, inbox_message).await
        } else {
            entry.inbox.push(inbox_message);
            Ok(())
        }
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
        let message = required_message(message)?;
        let (actor, id, root_session_id, status) = {
            let table = self.table.lock().await;
            let actor = table.task_name(actor_session_id)?;
            let id = table.resolve(target)?;
            let status = table.entries.get(&id).map(|entry| entry.snapshot.status);
            (actor, id, table.root_session_id, status)
        };
        if status.is_some_and(SubagentStatus::is_terminal) {
            bail!("subagent {target} is not running");
        }
        let inbox_message = self
            .persist_team_message(
                actor_session_id,
                id,
                &actor,
                &message,
                PromptDelivery::Steer,
                if status == Some(SubagentStatus::Ready) {
                    DeliveryMode::Wake
                } else {
                    DeliveryMode::Boundary
                },
                options,
            )
            .await?;
        if id == root_session_id {
            let mut messages = self.take_root_inbox().await;
            messages.push(inbox_message);
            for message in messages {
                if let Err(error) = self.root_message_tx.send(message) {
                    self.root_inbox.lock().await.push(error.0);
                }
            }
            return Ok(());
        }
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
        Ok(())
    }

    pub async fn interrupt(&self, target: &str) -> Result<()> {
        self.send_command(target, |session_id| HostCommand::Interrupt { session_id })
            .await
    }

    pub async fn stop(&self, target: &str) -> Result<()> {
        self.send_command(target, |session_id| HostCommand::Stop { session_id })
            .await
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
        let table = self.table.lock().await;
        let id = table.resolve(target)?;
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
                let agent = self
                    .spawn(SpawnSubagent {
                        task_name: args.task_name,
                        message: args.message,
                        provider: args.provider,
                        model: args.model,
                        effort: args.reasoning_effort,
                    })
                    .await?;
                Ok(serde_json::to_value(agent)?)
            }
            "list_agents" => {
                let args: ListAgentsArgs = serde_json::from_value(arguments)?;
                Ok(json!({ "agents": self.list(args.path_prefix.as_deref()).await }))
            }
            "send_message" => {
                let args: MessageArgs = serde_json::from_value(arguments)?;
                self.send_message_with_options_as(
                    actor_session_id,
                    &args.target,
                    &args.message,
                    args.options(),
                )
                .await?;
                Ok(json!({ "queued": true }))
            }
            "followup_task" => {
                let args: MessageArgs = serde_json::from_value(arguments)?;
                self.followup_task_with_options_as(
                    actor_session_id,
                    &args.target,
                    &args.message,
                    args.options(),
                )
                .await?;
                Ok(json!({ "accepted": true }))
            }
            "broadcast_team" => {
                let args: BroadcastArgs = serde_json::from_value(arguments)?;
                let message_id = self
                    .broadcast_message_as(actor_session_id, &args.message)
                    .await?;
                Ok(json!({ "message_id": message_id, "queued": true }))
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

/// Provider-neutral schemas: Codex consumes these as app-server dynamic tools;
/// Claude and OpenCode consume the same catalog through the local MCP bridge.
pub fn subagent_tool_specs(provider: CodingProvider) -> Vec<Value> {
    let description = subagent_tool_description(provider);
    vec![
        tool(
            "spawn_agent",
            &description,
            json!({
                "type": "object",
                "properties": {
                    "task_name": { "type": "string" },
                    "message": { "type": "string" },
                    "provider": {
                        "type": "string",
                        "enum": [
                            "codex",
                            "claude",
                            "open_code",
                            "kimi",
                            "open_router",
                            "open_ai_compatible"
                        ]
                    },
                    "model": { "type": "string" },
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
        message_tool(
            "send_message",
            "Queue a message for a child without waking an idle child.",
        ),
        message_tool("followup_task", "Send a follow-up and wake an idle child."),
        tool(
            "broadcast_team",
            "Broadcast one durable team message to all visible team participants.",
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
        "Spawn an isolated child Borg session for a concrete, bounded task. \
         Omit provider, model, and reasoning_effort to inherit the parent. {inheritance}"
    )
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

fn effective_worker_effort(launch: &LaunchSession, requested_effort: Option<String>) -> Option<String> {
    requested_effort.or_else(|| {
        // The only opt-in preset assigns workers low effort. Without a policy,
        // retain the existing inheritance from the root launch.
        launch.team_policy.as_ref().map(|_| "low".to_string())
    }).or_else(|| launch.effort.clone())
}

pub fn agent_tool_specs(provider: CodingProvider) -> Vec<Value> {
    agent_tool_specs_with_subagents(provider, true)
}

pub fn agent_tool_specs_with_subagents(
    provider: CodingProvider,
    subagents_enabled: bool,
) -> Vec<Value> {
    agent_tool_specs_with_team_policy(provider, subagents_enabled, None)
}

pub fn agent_tool_specs_with_team_policy(
    provider: CodingProvider,
    subagents_enabled: bool,
    team_policy: Option<&crate::TeamPolicy>,
) -> Vec<Value> {
    let mut specs = vec![
        tool(
            "get_goal",
            "Get the current durable goal, status, usage, and remaining token budget.",
            json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
        ),
        tool(
            "create_goal",
            "Create a durable goal for an explicit substantial multi-step user request when get_goal reports none.",
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
            "Mark the current goal complete, or blocked only after the same blocker prevents progress for three consecutive goal turns.",
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
            "Get the current durable task plan.",
            json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
        ),
        tool(
            "update_plan",
            "Replace the durable task plan. Call get_plan first when updating an existing plan, copy its exact UUIDs, and omit id for new items. Invalid non-UUID IDs are treated as omitted. Keep at most one item in progress.",
            json!({
                "type": "object",
                "properties": {
                    "explanation": { "type": "string" },
                    "plan": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {
                                    "type": "string",
                                    "format": "uuid",
                                    "description": "Existing item UUID copied exactly from get_plan. Omit for new items; never invent labels."
                                },
                                "content": { "type": "string" },
                                "status": {
                                    "type": "string",
                                    "enum": ["pending", "in_progress", "completed"]
                                }
                            },
                            "required": ["content", "status"],
                            "additionalProperties": false
                        }
                    }
                },
                "required": ["plan"],
                "additionalProperties": false
            }),
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
            "Read current language-server diagnostics for a workspace source file. Starts the matching server lazily.",
            lsp_path_schema(),
        ),
        tool(
            "lsp_hover",
            "Read language-server hover/type information at a one-based source position.",
            lsp_position_schema(),
        ),
        tool(
            "lsp_definition",
            "Find the definition at a one-based source position.",
            lsp_position_schema(),
        ),
        tool(
            "lsp_references",
            "Find references, including the declaration, at a one-based source position.",
            lsp_position_schema(),
        ),
        tool(
            "lsp_document_symbols",
            "List language-server symbols in a workspace source file.",
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
    specs
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NoArgs {}

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
#[serde(deny_unknown_fields)]
struct UpdatePlanArgs {
    #[serde(default)]
    #[allow(dead_code)]
    explanation: Option<String>,
    plan: Vec<TodoItemUpdate>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LspPathArgs {
    path: PathBuf,
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

fn tool(name: &str, description: &str, input_schema: Value) -> Value {
    json!({ "name": name, "description": description, "inputSchema": input_schema })
}

fn lsp_path_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "path": { "type": "string", "minLength": 1 }
        },
        "required": ["path"],
        "additionalProperties": false
    })
}

fn lsp_position_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "path": { "type": "string", "minLength": 1 },
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

fn required_message(message: &str) -> Result<String> {
    let message = message.trim();
    if message.is_empty() {
        bail!("subagent message must not be empty");
    }
    Ok(message.to_string())
}

fn attributed_team_message(actor: &str, message: &str) -> String {
    format!("Team message from {actor}:\n\n{message}")
}

fn child_journal_path(root: &Path, session_id: Uuid) -> PathBuf {
    root.join("subagents").join(format!("{session_id}.jsonl"))
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
        .send(HostCommand::Prompt {
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
            cost_microusd,
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
            entry.snapshot.usage.cost_microusd =
                match (entry.snapshot.usage.cost_microusd, cost_microusd) {
                    (Some(current), Some(additional)) => Some(current.saturating_add(*additional)),
                    (None, Some(value)) => Some(*value),
                    (current, None) => current,
                };
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
        cost_microusd: state.usage.cost_microusd,
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
mod tests {
    use super::*;
    use crate::{PermissionMode, SessionEventKind};
    use tempfile::tempdir;

    fn launch() -> LaunchSession {
        LaunchSession {
            request_id: Uuid::new_v4(),
            cwd: PathBuf::from("/workspace"),
            provider: CodingProvider::Codex,
            model: Some("gpt-test".into()),
            effort: Some("high".into()),
            fast: Some(false),
            response_language: crate::ResponseLanguage::Auto,
            permission_mode: PermissionMode::Manual,
            name: None,
            initial_prompt: None,
            capabilities: Default::default(),
            team_policy: None,
        }
    }

    async fn bind_test_team(
        directory: &Path,
        store: &crate::SqliteSessionStore,
        root: Uuid,
        children: &[Uuid],
    ) {
        let workspace = crate::SqliteWorkspaceStore::open(directory.join("workspaces.sqlite3"))
            .await
            .unwrap();
        let human = Uuid::new_v5(&root, b"borg-local-human-participant");
        workspace
            .ensure_execution_workspace(root, "test team", human, "Human", root, "Director")
            .await
            .unwrap();
        for child in children {
            let journal = child_journal_path(directory, *child);
            let writer = crate::SessionWriterLease::acquire(&journal).unwrap();
            store
                .register_child_session(root, *child, &journal, &writer)
                .await
                .unwrap();
            workspace
                .ensure_execution_workspace(root, "test team", human, "Human", *child, "Worker")
                .await
                .unwrap();
        }
    }

    #[test]
    fn child_identity_is_stable_and_inherits_execution_context() {
        let root = Uuid::new_v4();
        let mut table = SubagentTable {
            root_session_id: root,
            max_children: 2,
            entries: HashMap::new(),
            task_names: HashMap::new(),
        };
        let child = table.reserve("review_api", &launch()).unwrap();
        assert_eq!(child.parent_session_id, root);
        assert_eq!(child.task_name, "/root/review_api");
        assert_eq!(child.provider, CodingProvider::Codex);
        assert_eq!(child.model.as_deref(), Some("gpt-test"));
        assert_eq!(table.resolve("review_api").unwrap(), child.session_id);
        assert_eq!(table.resolve("/root/review_api").unwrap(), child.session_id);
    }

    #[test]
    fn live_child_limit_and_task_names_are_enforced() {
        let mut table = SubagentTable {
            root_session_id: Uuid::new_v4(),
            max_children: 1,
            entries: HashMap::new(),
            task_names: HashMap::new(),
        };
        let child = table.reserve("first", &launch()).unwrap();
        assert!(table.reserve("second", &launch()).is_err());
        table
            .entries
            .get_mut(&child.session_id)
            .unwrap()
            .snapshot
            .status = SubagentStatus::Stopped;
        assert!(table.reserve("second", &launch()).is_ok());
        assert!(table.reserve("SECOND", &launch()).is_err());
    }

    #[tokio::test]
    async fn child_messages_are_team_scoped_and_can_report_to_root() {
        let directory = tempdir().unwrap();
        let root = Uuid::new_v4();
        let store = Arc::new(
            crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
                .await
                .unwrap(),
        );
        store.create_session(root).await.unwrap();
        let coordinator = SubagentCoordinator::new_with_store_and_executor(
            directory.path(),
            root,
            launch(),
            3,
            Arc::new(crate::LocalAgentTurnExecutor::default()),
            store.clone(),
        )
        .unwrap();
        let worker = coordinator
            .table
            .lock()
            .await
            .reserve("worker", &launch())
            .unwrap();
        bind_test_team(directory.path(), store.as_ref(), root, &[worker.session_id]).await;
        let mut wake = coordinator.subscribe_root_messages();

        coordinator
            .send_message_as(worker.session_id, "/root", "blocked on an API decision")
            .await
            .unwrap();
        assert!(matches!(
            wake.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));

        coordinator
            .followup_task_as(worker.session_id, "/root", "please review")
            .await
            .unwrap();
        let queued = wake.recv().await.unwrap();
        let followup = wake.recv().await.unwrap();
        assert!(queued.text.contains("Team message from /root/worker"));
        assert!(queued.text.contains("blocked on an API decision"));
        assert!(followup.text.contains("please review"));
        assert!(coordinator.take_root_inbox().await.is_empty());
    }

    #[tokio::test]
    async fn sibling_messages_use_the_shared_team_directory() {
        let directory = tempdir().unwrap();
        let root = Uuid::new_v4();
        let store = Arc::new(
            crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
                .await
                .unwrap(),
        );
        store.create_session(root).await.unwrap();
        let coordinator = SubagentCoordinator::new_with_store_and_executor(
            directory.path(),
            root,
            launch(),
            3,
            Arc::new(crate::LocalAgentTurnExecutor::default()),
            store.clone(),
        )
        .unwrap();
        let mut table = coordinator.table.lock().await;
        let sender = table.reserve("sender", &launch()).unwrap();
        let recipient = table.reserve("recipient", &launch()).unwrap();
        let (commands, mut received) = mpsc::channel(1);
        let entry = table.entries.get_mut(&recipient.session_id).unwrap();
        entry.snapshot.status = SubagentStatus::Running;
        entry.commands = Some(commands);
        drop(table);
        bind_test_team(
            directory.path(),
            store.as_ref(),
            root,
            &[sender.session_id, recipient.session_id],
        )
        .await;

        coordinator
            .send_message_as(sender.session_id, "recipient", "share the benchmark")
            .await
            .unwrap();
        let HostCommand::Prompt {
            session_id,
            text,
            delivery,
            ..
        } = received.recv().await.unwrap()
        else {
            panic!("expected prompt");
        };
        assert_eq!(session_id, recipient.session_id);
        assert_eq!(delivery, PromptDelivery::Queue);
        assert!(text.contains("Team message from /root/sender"));
        assert!(text.contains("share the benchmark"));

        let broadcast_id = coordinator
            .broadcast_message_as(sender.session_id, "team checkpoint")
            .await
            .unwrap();
        let HostCommand::Prompt { message_id, .. } = received.recv().await.unwrap() else {
            panic!("expected broadcast prompt");
        };
        assert_eq!(message_id, broadcast_id);
        assert_eq!(
            coordinator.take_root_inbox().await[0].message_id,
            broadcast_id
        );
        let workspace =
            crate::SqliteWorkspaceStore::open(directory.path().join("workspaces.sqlite3"))
                .await
                .unwrap();
        let binding = store
            .workspace_binding(recipient.session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            workspace
                .deliveries_after(binding.workspace_id, binding.participant_id, 0, 10)
                .await
                .unwrap()
                .iter()
                .filter(|delivery| delivery.sequence > 0)
                .count(),
            2
        );
        coordinator
            .acknowledge_message_for_session(recipient.session_id, broadcast_id)
            .await
            .unwrap();
        assert!(
            coordinator
                .unread_messages_for_session(recipient.session_id)
                .await
                .unwrap()
                .iter()
                .all(|message| message.message_id != broadcast_id)
        );
    }

    #[tokio::test]
    async fn broadcast_is_rejected_when_multiplayer_is_disabled() {
        let directory = tempdir().unwrap();
        let root = Uuid::new_v4();
        let store = Arc::new(
            crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
                .await
                .unwrap(),
        );
        let mut disabled = launch();
        disabled.capabilities.multiplayer = false;
        let coordinator = SubagentCoordinator::new_with_store_and_executor(
            directory.path(),
            root,
            disabled,
            1,
            Arc::new(crate::LocalAgentTurnExecutor::default()),
            store,
        )
        .unwrap();
        assert!(
            coordinator
                .broadcast_message_as(root, "blocked")
                .await
                .is_err()
        );
    }

    #[test]
    fn tool_catalog_exposes_one_complete_lifecycle() {
        let names = subagent_tool_specs(CodingProvider::Codex)
            .into_iter()
            .filter_map(|tool| tool["name"].as_str().map(str::to_string))
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "spawn_agent",
                "list_agents",
                "send_message",
                "followup_task",
                "broadcast_team",
                "list_unread_team_messages",
                "acknowledge_team_message",
                "interrupt_agent",
                "wait_agent"
            ]
        );
    }

    #[test]
    fn autonomous_team_defaults_workers_to_low_without_overriding_tool_input() {
        let mut team_launch = launch();
        team_launch.team_policy = Some(TeamPreset::XhighDirectorLowWorkers.policy(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            std::iter::empty(),
            crate::ProviderId("codex".into()),
        ));
        assert_eq!(effective_worker_effort(&team_launch, None).as_deref(), Some("low"));
        assert_eq!(
            effective_worker_effort(&team_launch, Some("high".into())).as_deref(),
            Some("high")
        );
        team_launch.team_policy = None;
        assert_eq!(
            effective_worker_effort(&team_launch, None).as_deref(),
            Some("high")
        );
    }

    #[test]
    fn autonomous_team_policy_is_visible_in_spawn_tool_metadata() {
        let policy = TeamPreset::XhighDirectorLowWorkers.policy(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            std::iter::empty(),
            crate::ProviderId("codex".into()),
        );
        let spawn = agent_tool_specs_with_team_policy(CodingProvider::Codex, true, Some(&policy))
            .into_iter()
            .find(|tool| tool["name"] == "spawn_agent")
            .unwrap();
        assert!(spawn["description"]
            .as_str()
            .unwrap()
            .contains("Effective autonomous-team policy"));
    }

    #[test]
    fn disabled_catalog_omits_subagent_tools() {
        let names = agent_tool_specs_with_subagents(CodingProvider::Codex, false)
            .into_iter()
            .filter_map(|tool| tool["name"].as_str().map(str::to_string))
            .collect::<Vec<_>>();
        assert!(!names.iter().any(|name| name == "spawn_agent"));
        assert!(!names.iter().any(|name| name == "send_message"));
    }

    #[test]
    fn subagent_tool_and_validation_use_the_provider_model_catalog() {
        let catalog = CodingProvider::Codex
            .model_catalog()
            .expect("Codex catalog");
        let spawn = subagent_tool_specs(CodingProvider::Codex)
            .into_iter()
            .find(|tool| tool["name"] == "spawn_agent")
            .expect("spawn_agent tool");
        let description = spawn["description"].as_str().expect("description");
        for (model, _) in catalog.selectable_models {
            assert!(
                description.contains(model),
                "agent-facing description omitted {model}"
            );
            validate_subagent_overrides(CodingProvider::Codex, Some(model), None)
                .expect("catalog model should be accepted");
        }
        assert!(description.contains("gpt-5.6-luna"));
        assert!(
            validate_subagent_overrides(CodingProvider::Codex, Some("not-a-codex-model"), None)
                .is_err()
        );
    }

    #[tokio::test]
    async fn durable_parent_activity_restores_child_topology() {
        let directory = tempdir().unwrap();
        let root = Uuid::new_v4();
        let child_id = Uuid::new_v4();
        let now = Utc::now();
        let mut snapshot = SubagentSnapshot {
            session_id: child_id,
            parent_session_id: root,
            task_name: "/root/review_api".into(),
            status: SubagentStatus::Starting,
            provider: CodingProvider::Codex,
            model: Some("gpt-test".into()),
            effort: Some("high".into()),
            cwd: PathBuf::from("/workspace"),
            created_at: now,
            updated_at: now,
            detail: None,
            final_text: None,
            usage: SubagentUsage::default(),
        };
        let started = SessionEvent::new(
            root,
            1,
            SessionEventKind::SubagentActivity {
                activity: SubagentActivityKind::Started,
                agent: snapshot.clone(),
                event: None,
            },
        );
        snapshot.status = SubagentStatus::Stopped;
        snapshot.detail = Some("done".into());
        let stopped = SessionEvent::new(
            root,
            2,
            SessionEventKind::SubagentActivity {
                activity: SubagentActivityKind::Stopped,
                agent: snapshot.clone(),
                event: None,
            },
        );
        let store = Arc::new(
            crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
                .await
                .unwrap(),
        );
        store.create_session(root).await.unwrap();
        let coordinator = SubagentCoordinator::new_with_store_and_executor(
            directory.path(),
            root,
            launch(),
            3,
            Arc::new(crate::LocalAgentTurnExecutor::default()),
            store,
        )
        .unwrap();
        coordinator
            .restore_from_events(&[started, stopped])
            .await
            .unwrap();

        assert_eq!(
            coordinator
                .resolve_snapshot(child_id.to_string().as_str())
                .await
                .unwrap()
                .status,
            SubagentStatus::Stopped
        );
        assert_eq!(coordinator.list(None).await.len(), 1);
    }

    #[tokio::test]
    async fn restored_live_child_migrates_to_sqlite_and_accepts_typed_control() {
        let directory = tempdir().unwrap();
        let root = Uuid::new_v4();
        let child_id = Uuid::new_v4();
        let now = Utc::now();
        let snapshot = SubagentSnapshot {
            session_id: child_id,
            parent_session_id: root,
            task_name: "/root/review_api".into(),
            status: SubagentStatus::Ready,
            provider: CodingProvider::Codex,
            model: Some("gpt-test".into()),
            effort: Some("high".into()),
            cwd: PathBuf::from("/workspace"),
            created_at: now,
            updated_at: now,
            detail: None,
            final_text: Some("ready".into()),
            usage: SubagentUsage::default(),
        };
        let parent_event = SessionEvent::new(
            root,
            1,
            SessionEventKind::SubagentActivity {
                activity: SubagentActivityKind::Completed,
                agent: snapshot,
                event: None,
            },
        );
        let child_path = child_journal_path(directory.path(), child_id);
        let mut child_journal = crate::SessionJournal::open(&child_path).unwrap();
        child_journal
            .append(SessionEvent::new(
                child_id,
                0,
                SessionEventKind::SessionStarted,
            ))
            .unwrap();
        child_journal
            .append(SessionEvent::new(
                child_id,
                0,
                SessionEventKind::SessionConfigured {
                    cwd: PathBuf::from("/workspace"),
                    provider: CodingProvider::Codex,
                    model: Some("gpt-test".into()),
                    effort: Some("high".into()),
                    fast: false,
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: PermissionMode::Manual,
                },
            ))
            .unwrap();
        child_journal
            .append(SessionEvent::new(
                child_id,
                0,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Ready,
                    detail: None,
                },
            ))
            .unwrap();

        let store = Arc::new(
            crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
                .await
                .unwrap(),
        );
        store.create_session(root).await.unwrap();
        let session_store: Arc<dyn SessionStore> = store.clone();
        let coordinator = SubagentCoordinator::new_with_store_and_executor(
            directory.path(),
            root,
            launch(),
            3,
            Arc::new(crate::LocalAgentTurnExecutor::default()),
            session_store,
        )
        .unwrap();
        let mut activity_rx = coordinator.subscribe();
        coordinator
            .restore_from_events(&[parent_event])
            .await
            .unwrap();
        assert!(store.contains_session(child_id).await.unwrap());
        assert!(child_path.with_extension("jsonl.bak").is_file());
        assert_eq!(
            store
                .list_sessions(10)
                .await
                .unwrap()
                .into_iter()
                .map(|session| session.session_id)
                .collect::<Vec<_>>(),
            vec![root]
        );
        coordinator.stop(&child_id.to_string()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if matches!(
                    activity_rx.recv().await.unwrap(),
                    SubagentActivity::Stopped { .. }
                ) {
                    break;
                }
            }
        })
        .await
        .expect("restored child emits stop activity");
    }
}
