use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
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
use tokio::sync::{Mutex, OnceCell, broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use ts_rs::TS;
use uuid::Uuid;

use crate::{
    ApprovalDecision, AtomicWorkClaim, Audience, CodingProvider, DeliveryMode, EventActor,
    HostCommand, LaunchSession, MessageStatus, ModelGoalStatus, PromptDelivery, Provenance,
    SessionConsultationTools, SessionEvent, SessionEventKind, SessionGoalToolRequest,
    SessionGoalTools, SessionStatus, SessionStore, SessionTodoToolRequest, SessionTodoTools,
    SharedWork, SqliteWorkspaceStore, StructuredMention, TodoItemUpdate, WorkDependency,
    WorkReview, WorkspaceArtifact, WorkspaceDecision, WorkspaceEvent, WorkspaceEventKind,
    WorkspaceMessage, WorkspaceMessageBody, WorkspaceReference, WorkspaceReviewRequest,
    WorkspaceStore,
};

pub const DEFAULT_MAX_SUBAGENTS: usize = 16;
const ROOT_MESSAGE_RETRY_INTERVAL: Duration = Duration::from_millis(500);

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
        let consultation_enabled = dispatcher.consultation_enabled();
        let shared_work_enabled = dispatcher.shared_work.is_some();
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
            consultation_enabled,
            shared_work_enabled,
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
            consultation_enabled,
            shared_work_enabled,
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
        borg_provider::mcp::ExternalMcpServer {
            name: "borg_agent".to_string(),
            command: std::env::current_exe()
                .expect("Borg cannot expose session tools without its executable path")
                .to_string_lossy()
                .into_owned(),
            args: vec!["__agent-mcp".to_string()],
            env,
            allowed_tools: agent_tool_specs_with_capabilities_and_consultation(
                self.provider,
                self.subagents_enabled,
                self.shared_work_enabled,
                self.team_policy.as_ref(),
                self.consultation_enabled,
            )
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
    // The dispatcher deliberately receives each independently disableable
    // service explicitly at its construction boundary.
    #[allow(clippy::too_many_arguments)]
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
    ) -> Self {
        let consultation_enabled = subagents
            .as_ref()
            .is_none_or(|team| team.is_root_session(actor_session_id));
        Self {
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
        }
    }

    pub fn specs(&self) -> Vec<Value> {
        agent_tool_specs_with_capabilities_and_consultation(
            self.provider,
            self.subagents_enabled,
            self.shared_work.is_some(),
            self.team_policy.as_ref(),
            self.consultation_enabled,
        )
    }

    fn consultation_enabled(&self) -> bool {
        self.consultation_enabled
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
            name if is_shared_work_tool(name) => {
                self.shared_work
                    .as_ref()
                    .context("shared-work tools are disabled by session capabilities")?
                    .call(name, arguments)
                    .await
            }
            name if crate::self_service::is_tool(name) => self.self_service.call(name, arguments),
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
            .filter(|entry| !entry.snapshot.status.is_terminal() && !entry.dormant)
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
    pub report_text: String,
    pub sender_session_id: Uuid,
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
        let path = self.journal_root.join("workspaces.sqlite3");
        self.workspace_store
            .get_or_try_init(|| async move { SqliteWorkspaceStore::open(path).await })
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
    ) -> Result<TeamInboxMessage> {
        if !self.root_launch.capabilities.multiplayer {
            return Ok(TeamInboxMessage {
                message_id: Uuid::new_v4(),
                text: attributed_team_message(actor, message),
                report_text: message.to_string(),
                sender_session_id: actor_session_id,
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
        let workspace_store = self.workspace_store().await?;
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
                            text: message.to_string(),
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
            report_text: message.to_string(),
            sender_session_id: actor_session_id,
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
        let workspace_store = self.workspace_store().await?;
        let pending = workspace_store
            .pending_message_events(binding.workspace_id, binding.participant_id, 10_000)
            .await?;
        let mut messages = Vec::with_capacity(pending.len());
        for (event, delivery) in pending {
            let WorkspaceEventKind::Message { message, .. } = event.kind else {
                continue;
            };
            let Ok(actor) = self.task_name_for_session(message.author_id).await else {
                continue;
            };
            messages.push(TeamInboxMessage {
                message_id: message.id,
                text: attributed_team_message(&actor, &message.body.text),
                report_text: message.body.text,
                sender_session_id: message.author_id,
                delivery: match delivery.mode {
                    DeliveryMode::Boundary | DeliveryMode::Wake => PromptDelivery::Steer,
                    DeliveryMode::NextTurn | DeliveryMode::Notify => PromptDelivery::Queue,
                },
            });
        }
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
        let store = self.workspace_store().await?;
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
        let message = required_message(&request.message)?;
        let mut launch = self.root_launch.clone();
        launch.request_id = Uuid::new_v4();
        launch.initial_prompt = Some(message);
        let parent_provider = launch.provider;
        launch.provider = request.provider.unwrap_or(parent_provider);
        validate_subagent_overrides(
            launch.provider,
            request.model.as_deref(),
            request.effort.as_deref(),
        )?;
        if launch.provider != parent_provider {
            launch.model = request
                .model
                .or_else(|| default_model_for_cross_provider_peer(launch.provider));
            launch.effort = request
                .effort
                .or_else(|| default_effort_for_cross_provider_peer(launch.provider));
        } else {
            if request.model.is_some() {
                launch.model = request.model;
            }
            launch.effort = effective_worker_effort(&launch, request.effort);
        }
        anyhow::ensure!(
            !launch.provider.uses_native_harness() || launch.model.is_some(),
            "{:?} peer requires an explicit model",
            launch.provider
        );
        launch.name = Some(canonical_task_name(&request.task_name)?);

        let snapshot = self
            .table
            .lock()
            .await
            .reserve(&request.task_name, &launch)?;
        self.start_reserved(snapshot.clone(), launch, true).await?;
        Ok(snapshot)
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
        let (task_name, label) = match provider {
            CodingProvider::Claude => ("claude", "Claude"),
            CodingProvider::Codex => ("gpt", "GPT"),
            _ => unreachable!("persistent peer profile is restricted to GPT and Claude"),
        };
        let mut activity = self.subscribe();
        let sidecar = self
            .ensure_sidecar(task_name, provider, model.clone(), effort.clone())
            .await?;
        let message_id = Uuid::new_v4();
        let peer_prompt = format!(
            "You are the persistent private {label} peer for the primary Borg agent. This is an +             internal consultation, not a user-facing turn. Do not invoke another model, send +             team messages, edit files, or ask the human for clarification. You may inspect +             workspace state when it is necessary to validate the briefing. Return concise, +             self-contained analysis that the primary agent can use immediately.\n\n{prompt}"
        );
        self.prompt_child(
            &sidecar.task_name,
            message_id,
            peer_prompt,
            Vec::new(),
            PromptDelivery::Steer,
        )
        .await?;

        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            anyhow::ensure!(
                !remaining.is_zero(),
                "persistent peer consultation timed out"
            );
            match tokio::time::timeout(remaining, activity.recv()).await {
                Ok(Ok(SubagentActivity::SessionEvent { event, .. }))
                    if event.session_id == sidecar.session_id =>
                {
                    match event.kind {
                        SessionEventKind::Message {
                            actor: EventActor::Assistant,
                            text,
                            status: MessageStatus::Complete,
                            ..
                        } => {
                            let text = text.trim().to_string();
                            anyhow::ensure!(
                                !text.is_empty(),
                                "persistent peer returned an empty response"
                            );
                            return Ok(json!({
                                "persistent": true,
                                "provider": provider.catalog_backend(),
                                "model": model,
                                "thread": sidecar.task_name,
                                "response": text,
                            }));
                        }
                        SessionEventKind::StatusChanged {
                            status: SessionStatus::Failed | SessionStatus::Stopped,
                            detail,
                        } => {
                            bail!(
                                "persistent {} peer stopped before replying{}",
                                label,
                                detail
                                    .map(|detail| format!(": {detail}"))
                                    .unwrap_or_default()
                            );
                        }
                        _ => {}
                    }
                }
                Ok(Ok(SubagentActivity::Failed { agent }))
                    if agent.session_id == sidecar.session_id =>
                {
                    bail!(
                        "persistent {} peer failed{}",
                        label,
                        agent
                            .detail
                            .map(|detail| format!(": {detail}"))
                            .unwrap_or_default()
                    );
                }
                Ok(Ok(SubagentActivity::Completed { agent }))
                    if agent.session_id == sidecar.session_id =>
                {
                    if let Some(text) = agent.final_text.filter(|text| !text.trim().is_empty()) {
                        return Ok(json!({
                            "persistent": true,
                            "provider": provider.catalog_backend(),
                            "model": model,
                            "thread": sidecar.task_name,
                            "response": text,
                        }));
                    }
                }
                Ok(Ok(_)) => {}
                Ok(Err(broadcast::error::RecvError::Lagged(_))) => {}
                Ok(Err(broadcast::error::RecvError::Closed)) => {
                    bail!("persistent peer activity stream closed")
                }
                Err(_) => bail!("persistent peer consultation timed out"),
            }
        }
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
                    .filter(|entry| !entry.snapshot.status.is_terminal() && !entry.dormant)
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
        self.workspace_store()
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
                            text: message.clone(),
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
        Ok(message_id)
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
        let wakes_root = id == root_session_id && actor_session_id != root_session_id;
        let inbox_message = self
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
                self.broadcast_root_message(message).await;
            }
            return Ok(());
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

fn default_model_for_cross_provider_peer(provider: CodingProvider) -> Option<String> {
    match provider {
        CodingProvider::Codex => Some(borg_provider::codex_product_model().to_string()),
        CodingProvider::Claude => None,
        CodingProvider::OpenRouter => Some(borg_provider::openrouter_product_model().to_string()),
        CodingProvider::OpenAiCompatible => None,
    }
}

fn default_effort_for_cross_provider_peer(provider: CodingProvider) -> Option<String> {
    match provider {
        CodingProvider::Codex => Some(borg_provider::codex_default_effort().to_string()),
        CodingProvider::Claude | CodingProvider::OpenRouter | CodingProvider::OpenAiCompatible => {
            None
        }
    }
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
        let (provider_hint, explicit_model) = profile
            .split_once('/')
            .map_or((profile, None), |(provider, model)| {
                (provider, (!model.is_empty()).then_some(model))
            });
        let provider = match provider_hint {
            "claude" | "anthropic" => CodingProvider::Claude,
            "gpt" | "codex" | "openai" => CodingProvider::Codex,
            _ => CodingProvider::for_model(provider_hint)
                .with_context(|| format!("unknown persistent peer profile `{profile}`"))?,
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
                            "open_router",
                            "open_ai_compatible"
                        ]
                    },
                    "model": {
                        "type": "string",
                        "description": model_description,
                        "examples": [
                            "gpt-5.6-sol",
                            "gpt-5.6-terra",
                            "gpt-5.6-luna",
                            "claude-opus-5",
                            "claude-sonnet-5"
                        ]
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
        message_tool(
            "send_message",
            "Queue a message for a child without waking an idle child. Reports sent by a child to /root wake the director for reconciliation.",
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
         Omit provider, model, and reasoning_effort to inherit the parent. {inheritance} \
         All catalog-backed subagent choices are also available explicitly: {}",
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
    let mut specs = vec![
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
    }
    if !consultation_enabled {
        specs.retain(|spec| {
            !matches!(
                spec.get("name").and_then(Value::as_str),
                Some("consult_model" | "consult_peer")
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
    specs
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
struct NoArgs {}

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
    use std::sync::Mutex as StdMutex;
    use tempfile::tempdir;

    #[derive(Clone, Default)]
    struct RecordingPeerExecutor {
        prompts: Arc<StdMutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl crate::AgentTurnExecutor for RecordingPeerExecutor {
        async fn execute(
            &self,
            turn: crate::AgentTurn,
            events: mpsc::Sender<SessionEventKind>,
            _controls: Option<mpsc::Receiver<crate::AgentTurnControl>>,
        ) -> Result<crate::AgentTurnResult> {
            let response = {
                let mut prompts = self.prompts.lock().expect("peer prompt lock");
                let response = format!("persistent peer reply {}", prompts.len() + 1);
                prompts.push(turn.prompt);
                response
            };
            events
                .send(SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: EventActor::Assistant,
                    text: response.clone(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                })
                .await
                .map_err(|_| anyhow::anyhow!("peer event receiver closed"))?;
            Ok(crate::AgentTurnResult {
                provider_session_id: Some("persistent-peer-session".to_string()),
                final_text: response,
            })
        }
    }

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
            subagent_concurrency_limit: None,
            extension_skill_roots: Vec::new(),
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
        let human = crate::local_human_participant_id("Human");
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

    #[tokio::test]
    async fn ensuring_a_sidecar_reuses_one_idle_provider_session() {
        let directory = tempdir().unwrap();
        let root = Uuid::new_v4();
        let store = Arc::new(
            crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
                .await
                .unwrap(),
        );
        store.create_session(root).await.unwrap();
        let mut root_launch = launch();
        root_launch.capabilities.multiplayer = false;
        let coordinator = SubagentCoordinator::new_with_store_and_executor(
            directory.path(),
            root,
            root_launch,
            3,
            Arc::new(crate::LocalAgentTurnExecutor::default()),
            store,
        )
        .unwrap();

        let first = coordinator
            .ensure_sidecar(
                "claude",
                CodingProvider::Claude,
                Some("claude-opus-5".to_string()),
                Some("high".to_string()),
            )
            .await
            .unwrap();
        let second = coordinator
            .ensure_sidecar(
                "claude",
                CodingProvider::Claude,
                Some("claude-opus-5".to_string()),
                Some("high".to_string()),
            )
            .await
            .unwrap();

        assert_eq!(first.session_id, second.session_id);
        assert_eq!(second.task_name, "/root/claude");
        assert_eq!(second.provider, CodingProvider::Claude);
        assert_eq!(second.model.as_deref(), Some("claude-opus-5"));
        assert_eq!(second.effort.as_deref(), Some("high"));

        coordinator.stop("/root/claude").await.unwrap();
        let resumed = coordinator
            .ensure_sidecar(
                "claude",
                CodingProvider::Claude,
                Some("claude-opus-5".to_string()),
                Some("high".to_string()),
            )
            .await
            .unwrap();
        assert_eq!(resumed.session_id, first.session_id);
        coordinator.stop("/root/claude").await.unwrap();
    }

    #[tokio::test]
    async fn persistent_peer_consultation_reuses_the_sidecar_and_returns_to_the_primary() {
        let directory = tempdir().unwrap();
        let root = Uuid::new_v4();
        let store = Arc::new(
            crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
                .await
                .unwrap(),
        );
        store.create_session(root).await.unwrap();
        let prompts = Arc::new(StdMutex::new(Vec::new()));
        let executor = RecordingPeerExecutor {
            prompts: Arc::clone(&prompts),
        };
        let mut root_launch = launch();
        root_launch.capabilities.multiplayer = false;
        root_launch.cwd = directory.path().to_path_buf();
        let coordinator = SubagentCoordinator::new_with_store_and_executor(
            directory.path(),
            root,
            root_launch,
            3,
            Arc::new(executor),
            store,
        )
        .unwrap();

        let first = coordinator
            .consult_peer(CodingProvider::Codex, None, "Compare the API boundaries.")
            .await
            .unwrap();
        let first_thread = first["thread"].as_str().unwrap().to_string();
        let second = coordinator
            .consult_peer(
                CodingProvider::Codex,
                None,
                "Now revisit the cancellation edge case.",
            )
            .await
            .unwrap();
        let reverse = coordinator
            .consult_peer(
                CodingProvider::Claude,
                None,
                "As the Claude primary, ask GPT to challenge the conclusion.",
            )
            .await
            .unwrap();

        assert_eq!(first["persistent"], true);
        assert_eq!(first["provider"], "claude");
        assert_eq!(first["response"], "persistent peer reply 1");
        assert_eq!(second["response"], "persistent peer reply 2");
        assert_eq!(second["thread"], first_thread);
        assert_eq!(reverse["provider"], "codex");
        assert_eq!(reverse["thread"], "/root/gpt");
        assert_eq!(reverse["response"], "persistent peer reply 3");
        let prompts = prompts.lock().unwrap();
        assert_eq!(prompts.len(), 3);
        assert!(prompts[0].contains("Compare the API boundaries."));
        assert!(prompts[1].contains("cancellation edge case"));
        assert!(prompts[0].contains("persistent private Claude peer"));
        assert!(prompts[2].contains("As the Claude primary"));
        coordinator.stop("/root/claude").await.unwrap();
        coordinator.stop("/root/gpt").await.unwrap();
    }

    #[test]
    fn cross_provider_peer_does_not_inherit_an_incompatible_model_or_effort() {
        assert_eq!(
            default_model_for_cross_provider_peer(CodingProvider::Claude),
            None
        );
        assert_eq!(
            default_effort_for_cross_provider_peer(CodingProvider::Claude),
            None
        );
        assert_eq!(
            default_model_for_cross_provider_peer(CodingProvider::Codex).as_deref(),
            Some(borg_provider::codex_product_model())
        );
        assert_eq!(
            default_model_for_cross_provider_peer(CodingProvider::OpenRouter).as_deref(),
            Some(borg_provider::openrouter_product_model())
        );
        assert_eq!(
            default_effort_for_cross_provider_peer(CodingProvider::OpenRouter),
            None
        );
    }

    #[test]
    fn persistent_peer_defaults_to_the_opposite_provider_and_stable_sidecar_profile() {
        let (provider, model, effort) =
            resolve_persistent_peer_profile(CodingProvider::Codex, None).unwrap();
        assert_eq!(provider, CodingProvider::Claude);
        assert_eq!(model.as_deref(), Some("claude-opus-5"));
        assert_eq!(effort.as_deref(), Some("high"));

        let (provider, model, effort) =
            resolve_persistent_peer_profile(CodingProvider::Claude, None).unwrap();
        assert_eq!(provider, CodingProvider::Codex);
        assert_eq!(model.as_deref(), Some(borg_provider::codex_product_model()));
        assert_eq!(
            effort.as_deref(),
            Some(borg_provider::codex_default_effort())
        );

        let (provider, model, effort) =
            resolve_persistent_peer_profile(CodingProvider::Codex, Some("claude-opus-5@high"))
                .unwrap();
        assert_eq!(provider, CodingProvider::Claude);
        assert_eq!(model.as_deref(), Some("claude-opus-5"));
        assert_eq!(effort.as_deref(), Some("high"));
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
        let mut activity = coordinator.subscribe();

        coordinator
            .send_message_as(worker.session_id, "/root", "blocked on an API decision")
            .await
            .unwrap();
        let projected = activity.recv().await.unwrap();
        assert!(matches!(
            projected,
            SubagentActivity::SessionEvent {
                event: SessionEvent {
                    kind: SessionEventKind::Message {
                        actor: crate::EventActor::Assistant,
                        status: MessageStatus::Complete,
                        ref text,
                        ..
                    },
                    ..
                },
                ..
            } if text == "blocked on an API decision"
        ));
        assert!(matches!(
            wake.try_recv(),
            Ok(message)
                if message.delivery == PromptDelivery::Steer
                    && message.text.contains("blocked on an API decision")
        ));

        coordinator
            .followup_task_as(worker.session_id, "/root", "please review")
            .await
            .unwrap();
        let followup = wake.recv().await.unwrap();
        assert!(followup.text.contains("please review"));
        assert!(coordinator.take_root_inbox().await.is_empty());
    }

    #[tokio::test]
    async fn durable_root_inbox_poll_replays_a_report_when_wake_delivery_was_missed() {
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

        // No root receiver exists. The durable inbox must retain the wake and
        // replay it when the root session reconnects.
        coordinator
            .send_message_as(worker.session_id, "/root", "durable result")
            .await
            .unwrap();

        let mut wake = coordinator.subscribe_root_messages();
        let reports = coordinator.refresh_root_inbox_reports().await.unwrap();
        assert_eq!(reports.len(), 1);
        let (message_id, SubagentActivity::SessionEvent { event, .. }) = &reports[0] else {
            panic!("expected a durable child report projection");
        };
        assert!(matches!(
            &event.kind,
            SessionEventKind::Message {
                actor: crate::EventActor::Assistant,
                text,
                status: MessageStatus::Complete,
                ..
            } if text == "durable result"
        ));
        coordinator.mark_root_message_projected(*message_id).await;
        coordinator.wake_pending_root_messages().await;
        let wake = wake.recv().await.unwrap();
        assert_eq!(wake.delivery, PromptDelivery::Steer);
        assert!(wake.text.contains("durable result"));
        assert!(coordinator.take_root_inbox().await.is_empty());
    }

    #[tokio::test]
    async fn durable_root_wake_retries_after_the_receiver_is_lost() {
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

        // A broadcast can report success even when the receiver disappears
        // before reading it. The durable poll must retry after the grace
        // interval instead of treating that send as an acknowledgement.
        let first_wake = coordinator.subscribe_root_messages();
        coordinator
            .send_message_as(worker.session_id, "/root", "retry this report")
            .await
            .unwrap();
        drop(first_wake);

        tokio::time::sleep(ROOT_MESSAGE_RETRY_INTERVAL + Duration::from_millis(25)).await;
        let mut wake = coordinator.subscribe_root_messages();
        let reports = coordinator.refresh_root_inbox_reports().await.unwrap();
        assert_eq!(reports.len(), 1);
        let (message_id, _) = &reports[0];
        coordinator.mark_root_message_projected(*message_id).await;
        coordinator.wake_pending_root_messages().await;

        let message = wake.recv().await.unwrap();
        assert_eq!(message.delivery, PromptDelivery::Steer);
        assert!(message.text.contains("retry this report"));
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
    async fn focused_human_prompt_and_recall_target_the_exact_child_actor() {
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
            store,
        )
        .unwrap();
        let mut table = coordinator.table.lock().await;
        let child = table.reserve("worker", &launch()).unwrap();
        let (commands, mut received) = mpsc::channel(2);
        let entry = table.entries.get_mut(&child.session_id).unwrap();
        entry.snapshot.status = SubagentStatus::Running;
        entry.commands = Some(commands);
        drop(table);

        let message_id = Uuid::new_v4();
        coordinator
            .prompt_child(
                "worker",
                message_id,
                "inspect the scheduler".to_string(),
                vec![PathBuf::from("/workspace/trace.txt")],
                PromptDelivery::Steer,
            )
            .await
            .unwrap();
        let HostCommand::Prompt {
            session_id,
            message_id: received_id,
            text,
            attachments,
            delivery,
            ..
        } = received.recv().await.unwrap()
        else {
            panic!("expected direct child prompt");
        };
        assert_eq!(session_id, child.session_id);
        assert_eq!(received_id, message_id);
        assert_eq!(text, "inspect the scheduler");
        assert_eq!(attachments, [PathBuf::from("/workspace/trace.txt")]);
        assert_eq!(delivery, PromptDelivery::Steer);

        coordinator
            .recall_child_prompt("worker", Some(message_id))
            .await
            .unwrap();
        let HostCommand::RecallQueuedPrompt {
            session_id,
            message_id: recalled_id,
        } = received.recv().await.unwrap()
        else {
            panic!("expected exact child prompt recall");
        };
        assert_eq!(session_id, child.session_id);
        assert_eq!(recalled_id, Some(message_id));
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
    fn shared_work_tools_are_absent_when_the_capability_is_disabled() {
        let names = agent_tool_specs_with_capabilities(CodingProvider::Codex, false, false, None)
            .into_iter()
            .filter_map(|tool| tool["name"].as_str().map(str::to_string))
            .collect::<Vec<_>>();
        assert!(!names.iter().any(|name| is_shared_work_tool(name)));
        assert!(!names.iter().any(|name| name == "spawn_agent"));

        let enabled = agent_tool_specs_with_capabilities(CodingProvider::Codex, false, true, None)
            .into_iter()
            .filter_map(|tool| tool["name"].as_str().map(str::to_string))
            .collect::<Vec<_>>();
        assert!(enabled.iter().any(|name| name == "create_shared_work"));
        assert!(enabled.iter().any(|name| name == "request_work_review"));
    }

    #[test]
    fn every_execution_lane_exposes_the_same_borg_control_plane() {
        let common = [
            "consult_model",
            "get_goal",
            "get_plan",
            "update_plan",
            "lsp_diagnostics",
            "list_plugins",
            "read_plugin",
            "get_agent_settings",
            "update_agent_settings",
            "create_plugin",
        ];
        for provider in [
            CodingProvider::Codex,
            CodingProvider::Claude,
            CodingProvider::OpenRouter,
            CodingProvider::OpenAiCompatible,
        ] {
            let names = agent_tool_specs_with_capabilities(provider, false, false, None)
                .into_iter()
                .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
                .collect::<Vec<_>>();
            for required in common {
                assert!(
                    names.iter().any(|name| name == required),
                    "{provider:?} lane is missing Borg tool {required}"
                );
            }
        }
    }

    #[test]
    fn persistent_peer_tool_is_root_only_and_not_recursive() {
        let root_names = agent_tool_specs_with_capabilities_and_consultation(
            CodingProvider::Codex,
            true,
            false,
            None,
            true,
        )
        .into_iter()
        .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
        .collect::<Vec<_>>();
        assert!(root_names.iter().any(|name| name == "consult_peer"));

        let child_names = agent_tool_specs_with_capabilities_and_consultation(
            CodingProvider::Claude,
            true,
            false,
            None,
            false,
        )
        .into_iter()
        .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
        .collect::<Vec<_>>();
        assert!(!child_names.iter().any(|name| name == "consult_peer"));
        assert!(!child_names.iter().any(|name| name == "consult_model"));
    }

    #[tokio::test]
    async fn shared_work_tools_are_idempotent_atomic_and_replayable() {
        let directory = tempdir().unwrap();
        let workspace_id = Uuid::new_v4();
        let human_id = Uuid::new_v4();
        let agent_id = Uuid::new_v4();
        let store = SqliteWorkspaceStore::open(directory.path().join("workspaces.sqlite3"))
            .await
            .unwrap();
        store
            .ensure_execution_workspace(
                workspace_id,
                "shared tools",
                human_id,
                "Human",
                agent_id,
                "Agent",
            )
            .await
            .unwrap();
        let tools = SharedWorkToolContext::new(store, workspace_id, agent_id);

        let create_args = json!({
            "title": "Verify boundary delivery",
            "detail": "Exercise the real provider boundary.",
            "idempotency_key": "work:boundary-delivery"
        });
        let created = tools
            .call("create_shared_work", create_args.clone())
            .await
            .unwrap();
        let retried = tools.call("create_shared_work", create_args).await.unwrap();
        assert_eq!(created, retried);
        assert!(
            tools
                .call(
                    "create_shared_work",
                    json!({
                        "title": "Conflicting payload",
                        "idempotency_key": "work:boundary-delivery"
                    }),
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("idempotency conflict")
        );

        let work_id: Uuid = serde_json::from_value(created["kind"]["work"]["id"].clone()).unwrap();
        let claim_args = json!({
            "work_id": work_id,
            "idempotency_key": "claim:boundary-delivery"
        });
        let claim = tools
            .call("claim_shared_work", claim_args.clone())
            .await
            .unwrap();
        assert_eq!(
            claim,
            tools.call("claim_shared_work", claim_args).await.unwrap()
        );
        tools
            .call(
                "request_work_review",
                json!({
                    "work_id": work_id,
                    "requested_reviewer_id": human_id,
                    "instructions": "Review the boundary trace.",
                    "idempotency_key": "review-request:boundary-delivery"
                }),
            )
            .await
            .unwrap();

        let replay = tools
            .call("list_shared_work", json!({ "limit": 20 }))
            .await
            .unwrap();
        let events = replay["events"].as_array().unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0]["kind"]["type"], "work_created");
        assert_eq!(events[1]["kind"]["type"], "work_claimed");
        assert_eq!(events[2]["kind"]["type"], "review_requested");
    }

    #[test]
    fn autonomous_team_defaults_workers_to_low_without_overriding_tool_input() {
        let mut team_launch = launch();
        team_launch.team_policy = Some(crate::TeamPreset::XhighDirectorLowWorkers.policy(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            std::iter::empty(),
            crate::ProviderId("codex".into()),
        ));
        assert_eq!(
            effective_worker_effort(&team_launch, None).as_deref(),
            Some("low")
        );
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
        let policy = crate::TeamPreset::XhighDirectorLowWorkers.policy(
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
        assert!(
            spawn["description"]
                .as_str()
                .unwrap()
                .contains("Effective autonomous-team policy")
        );
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

    #[test]
    fn every_parent_model_can_see_codex_luna_as_a_subagent_option() {
        for parent in [
            CodingProvider::Codex,
            CodingProvider::Claude,
            CodingProvider::OpenRouter,
        ] {
            let spawn = subagent_tool_specs(parent)
                .into_iter()
                .find(|tool| tool["name"] == "spawn_agent")
                .expect("spawn_agent tool");
            assert!(
                spawn["description"]
                    .as_str()
                    .is_some_and(|description| description.contains("gpt-5.6-luna (Luna)")),
                "parent {parent:?} omitted Luna from its orchestration instructions"
            );
            assert!(
                spawn["inputSchema"]["properties"]["model"]["description"]
                    .as_str()
                    .is_some_and(|description| description.contains("gpt-5.6-luna")),
                "parent {parent:?} omitted Luna from the model argument metadata"
            );
        }
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

        let message_id = Uuid::new_v4();
        let partial = SessionEvent::new(
            root,
            3,
            SessionEventKind::SubagentActivity {
                activity: SubagentActivityKind::Updated,
                agent: snapshot.clone(),
                event: Some(Box::new(SessionEvent::new(
                    child_id,
                    0,
                    SessionEventKind::Message {
                        message_id,
                        actor: crate::EventActor::Assistant,
                        text: "I".into(),
                        attachments: Vec::new(),
                        status: MessageStatus::InProgress,
                        delivery: None,
                    },
                ))),
            },
        );
        coordinator
            .restore_from_events(std::slice::from_ref(&partial))
            .await
            .unwrap();
        assert!(!coordinator.root_message_is_projected(message_id).await);

        let completed = SessionEvent::new(
            root,
            4,
            SessionEventKind::SubagentActivity {
                activity: SubagentActivityKind::Completed,
                agent: snapshot,
                event: Some(Box::new(SessionEvent::new(
                    child_id,
                    8,
                    SessionEventKind::Message {
                        message_id,
                        actor: crate::EventActor::Assistant,
                        text: "I am complete".into(),
                        attachments: Vec::new(),
                        status: MessageStatus::Complete,
                        delivery: None,
                    },
                ))),
            },
        );
        coordinator
            .restore_from_events(&[partial, completed])
            .await
            .unwrap();
        assert!(coordinator.root_message_is_projected(message_id).await);
    }

    #[tokio::test]
    async fn restore_mirrors_a_child_stop_journaled_before_the_parent_crashed() {
        let directory = tempdir().unwrap();
        let workspace = directory.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let root = Uuid::new_v4();
        let child_id = Uuid::new_v4();
        let now = Utc::now();
        let parent_event = SessionEvent::new(
            root,
            1,
            SessionEventKind::SubagentActivity {
                activity: SubagentActivityKind::Updated,
                agent: SubagentSnapshot {
                    session_id: child_id,
                    parent_session_id: root,
                    task_name: "/root/review_api".into(),
                    status: SubagentStatus::Running,
                    provider: CodingProvider::Codex,
                    model: Some("gpt-test".into()),
                    effort: Some("high".into()),
                    cwd: workspace.clone(),
                    created_at: now,
                    updated_at: now,
                    detail: Some("turn phase: provider active".into()),
                    final_text: None,
                    usage: SubagentUsage::default(),
                },
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
                    cwd: workspace,
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
                    status: SessionStatus::Stopped,
                    detail: Some("crash cleanup completed".into()),
                },
            ))
            .unwrap();

        let store = Arc::new(
            crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
                .await
                .unwrap(),
        );
        store.create_session(root).await.unwrap();
        let session_store: Arc<dyn SessionStore> = store;
        let coordinator = SubagentCoordinator::new_with_store_and_executor(
            directory.path(),
            root,
            launch(),
            3,
            Arc::new(crate::LocalAgentTurnExecutor::default()),
            session_store,
        )
        .unwrap();
        let updates = coordinator
            .restore_from_events(&[parent_event])
            .await
            .unwrap();

        assert_eq!(
            coordinator
                .resolve_snapshot(&child_id.to_string())
                .await
                .unwrap()
                .status,
            SubagentStatus::Stopped
        );
        assert!(matches!(
            updates.as_slice(),
            [SubagentActivity::Stopped { agent }] if agent.session_id == child_id
        ));
        let idle_writer = crate::SessionWriterLease::try_acquire(&child_path)
            .unwrap()
            .expect("a reconciled stopped child remains dormant");
        drop(idle_writer);
    }

    #[tokio::test]
    async fn restored_live_child_stays_dormant_and_stops_with_its_root() {
        let directory = tempdir().unwrap();
        let workspace = directory.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
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
            cwd: workspace.clone(),
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
                    cwd: workspace,
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
        let idle_writer = crate::SessionWriterLease::try_acquire(&child_path)
            .unwrap()
            .expect("restoring the root must not start or lock the child actor");
        drop(idle_writer);
        let restored = coordinator
            .resolve_snapshot(&child_id.to_string())
            .await
            .unwrap();
        assert_eq!(restored.status, SubagentStatus::Ready);
        assert!(
            restored
                .detail
                .as_deref()
                .unwrap()
                .contains("follow up to wake")
        );
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
        coordinator.ensure_child_actor(child_id).await.unwrap();
        assert!(
            crate::SessionWriterLease::try_acquire(&child_path)
                .unwrap()
                .is_none(),
            "explicit wake should own the child writer"
        );
        let terminal_updates = coordinator.stop_all().await;
        assert!(matches!(
            terminal_updates.as_slice(),
            [SubagentActivity::Stopped { agent }] if agent.session_id == child_id
        ));
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
        let released_writer = crate::SessionWriterLease::try_acquire(&child_path)
            .unwrap()
            .expect("root stop must release the child writer");
        drop(released_writer);
    }
}
