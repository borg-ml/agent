use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use agent_client_protocol::schema::v1::{
    AgentCapabilities, CancelNotification, CloseSessionRequest, CloseSessionResponse, ContentBlock,
    ContentChunk, EmbeddedResourceResource, InitializeRequest, InitializeResponse,
    LoadSessionRequest, LoadSessionResponse, NewSessionRequest, NewSessionResponse,
    PermissionOption, PermissionOptionKind, Plan as AcpPlan, PlanEntry, PlanEntryPriority,
    PlanEntryStatus, PromptCapabilities, PromptRequest, PromptResponse, RequestPermissionOutcome,
    RequestPermissionRequest, SessionCapabilities as AcpSessionCapabilities,
    SessionCloseCapabilities, SessionId, SessionNotification, SessionUpdate, StopReason,
    TextContent, ToolCall, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields, UsageUpdate,
};
use agent_client_protocol::{Agent, ConnectionTo, Error, Responder, Stdio};
use anyhow::{Context, Result};
use borg_remote::{
    ApprovalDecision, EventActor, HostCommand, LaunchSession, MessageStatus, PlanItemStatus,
    PromptDelivery, ResponseLanguage, SessionCapabilities, SessionConfiguration, SessionEvent,
    SessionEventKind, SessionStore, SessionWriterLease, SqliteSessionStore,
    default_host_config_path, probe_provider_capabilities, run_agent_session_with_writer,
};
use tokio::sync::{Mutex, broadcast, mpsc};
use uuid::Uuid;

use crate::agent_config::AgentConfig;
use crate::cli::AcpArgs;

#[derive(Clone)]
struct AcpRuntime {
    args: AcpArgs,
    config: AgentConfig,
    sessions_dir: PathBuf,
    store: Arc<SqliteSessionStore>,
    sessions: Arc<Mutex<HashMap<SessionId, AcpSession>>>,
}

#[derive(Clone)]
struct AcpSession {
    id: Uuid,
    commands: mpsc::Sender<HostCommand>,
    events: broadcast::Sender<SessionEvent>,
}

pub(crate) async fn run(args: AcpArgs) -> Result<()> {
    let config = AgentConfig::load(args.config.as_deref())?;
    let sessions_dir = default_host_config_path()
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("sessions");
    let store = Arc::new(SqliteSessionStore::open(sessions_dir.join("sessions.sqlite3")).await?);
    let runtime = AcpRuntime {
        args,
        config,
        sessions_dir,
        store,
        sessions: Arc::new(Mutex::new(HashMap::new())),
    };

    let initialize_runtime = runtime.clone();
    let new_runtime = runtime.clone();
    let load_runtime = runtime.clone();
    let prompt_runtime = runtime.clone();
    let close_runtime = runtime.clone();
    let cancel_runtime = runtime;
    Agent
        .builder()
        .name("borg")
        .on_receive_request(
            async move |request: InitializeRequest, responder, _connection| {
                let _ = &initialize_runtime;
                responder.respond(
                    InitializeResponse::new(request.protocol_version).agent_capabilities(
                        AgentCapabilities::new()
                            .load_session(true)
                            .prompt_capabilities(PromptCapabilities::new().embedded_context(true))
                            .session_capabilities(
                                AcpSessionCapabilities::new()
                                    .close(SessionCloseCapabilities::new()),
                            ),
                    ),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            move |request: LoadSessionRequest, responder, connection| {
                let runtime = load_runtime.clone();
                async move { respond_load_session(runtime, request, responder, connection).await }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            move |request: NewSessionRequest, responder, _connection| {
                let runtime = new_runtime.clone();
                async move { respond_new_session(runtime, request, responder).await }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            move |request: PromptRequest, responder, connection| {
                let runtime = prompt_runtime.clone();
                async move { respond_prompt(runtime, request, responder, connection).await }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            move |request: CloseSessionRequest, responder, _connection| {
                let runtime = close_runtime.clone();
                async move { respond_close(runtime, request, responder).await }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_notification(
            move |notification: CancelNotification, _connection| {
                let runtime = cancel_runtime.clone();
                async move { cancel(runtime, notification).await }
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .connect_to(Stdio::new())
        .await
        .map_err(|error| anyhow::anyhow!("ACP server stopped: {error}"))
}

async fn respond_new_session(
    runtime: AcpRuntime,
    request: NewSessionRequest,
    responder: Responder<NewSessionResponse>,
) -> agent_client_protocol::Result<()> {
    let session = runtime
        .start_session(request.cwd)
        .await
        .map_err(internal_error)?;
    responder.respond(NewSessionResponse::new(session.id.to_string()))
}

async fn respond_load_session(
    runtime: AcpRuntime,
    request: LoadSessionRequest,
    responder: Responder<LoadSessionResponse>,
    connection: ConnectionTo<agent_client_protocol::Client>,
) -> agent_client_protocol::Result<()> {
    let id =
        Uuid::parse_str(&request.session_id.to_string()).map_err(|_| Error::invalid_params())?;
    let cwd = request
        .cwd
        .canonicalize()
        .map_err(|_| Error::invalid_params())?;
    let state = runtime.store.state(id).await.map_err(internal_error)?;
    let configuration = state.configuration.ok_or_else(Error::invalid_params)?;
    if configuration.cwd != cwd {
        return Err(Error::invalid_params());
    }
    for event in runtime.store.read(id).await.map_err(internal_error)? {
        if let Some(update) = replay_update(event.kind) {
            connection
                .send_notification(SessionNotification::new(request.session_id.clone(), update))?;
        }
    }
    runtime
        .resume_session(id, configuration)
        .await
        .map_err(internal_error)?;
    responder.respond(LoadSessionResponse::new())
}

fn replay_update(kind: SessionEventKind) -> Option<SessionUpdate> {
    match kind {
        SessionEventKind::Message {
            actor,
            text,
            status: MessageStatus::Complete,
            ..
        } => match actor {
            EventActor::User => Some(SessionUpdate::UserMessageChunk(ContentChunk::new(
                ContentBlock::Text(TextContent::new(text)),
            ))),
            EventActor::Assistant => Some(SessionUpdate::AgentMessageChunk(ContentChunk::new(
                ContentBlock::Text(TextContent::new(text)),
            ))),
            _ => None,
        },
        SessionEventKind::ToolStarted {
            tool_call_id,
            name,
            input,
            ..
        } => Some(SessionUpdate::ToolCall(
            ToolCall::new(tool_call_id, name)
                .status(ToolCallStatus::InProgress)
                .raw_input(input),
        )),
        SessionEventKind::ToolCompleted {
            tool_call_id,
            output,
            is_error,
            ..
        } => Some(SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
            tool_call_id,
            ToolCallUpdateFields::new()
                .status(if is_error {
                    ToolCallStatus::Failed
                } else {
                    ToolCallStatus::Completed
                })
                .raw_output(serde_json::Value::String(output)),
        ))),
        SessionEventKind::PlanUpdated { items } => Some(SessionUpdate::Plan(AcpPlan::new(
            items
                .into_iter()
                .map(|item| {
                    PlanEntry::new(
                        item.content,
                        PlanEntryPriority::Medium,
                        match item.status {
                            PlanItemStatus::Pending => PlanEntryStatus::Pending,
                            PlanItemStatus::InProgress => PlanEntryStatus::InProgress,
                            PlanItemStatus::Completed => PlanEntryStatus::Completed,
                        },
                    )
                })
                .collect(),
        ))),
        SessionEventKind::UsageUpdated {
            context_tokens: Some(used),
            context_window_tokens: Some(size),
            ..
        }
        | SessionEventKind::ContextWindowUpdated {
            context_tokens: used,
            context_window_tokens: size,
        } => Some(SessionUpdate::UsageUpdate(UsageUpdate::new(used, size))),
        _ => None,
    }
}

async fn respond_prompt(
    runtime: AcpRuntime,
    request: PromptRequest,
    responder: Responder<PromptResponse>,
    connection: ConnectionTo<agent_client_protocol::Client>,
) -> agent_client_protocol::Result<()> {
    let session = runtime
        .sessions
        .lock()
        .await
        .get(&request.session_id)
        .cloned()
        .ok_or_else(Error::invalid_params)?;
    let text = prompt_text(&request.prompt)?;
    let message_id = Uuid::new_v4();
    let mut events = session.events.subscribe();
    session
        .commands
        .send(HostCommand::Prompt {
            session_id: session.id,
            message_id,
            text,
            attachments: Vec::new(),
            output_schema: None,
            delivery: PromptDelivery::Queue,
        })
        .await
        .map_err(|_| Error::internal_error())?;

    loop {
        let event = events.recv().await.map_err(|_| Error::internal_error())?;
        match event.kind {
            SessionEventKind::Message {
                actor: EventActor::Assistant,
                text,
                status: MessageStatus::Complete,
                ..
            } => {
                connection.send_notification(SessionNotification::new(
                    request.session_id.clone(),
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new(text),
                    ))),
                ))?;
            }
            SessionEventKind::ReasoningDelta { text } => {
                connection.send_notification(SessionNotification::new(
                    request.session_id.clone(),
                    SessionUpdate::AgentThoughtChunk(ContentChunk::new(ContentBlock::Text(
                        TextContent::new(text),
                    ))),
                ))?;
            }
            SessionEventKind::ToolStarted {
                tool_call_id,
                name,
                input,
                ..
            } => {
                connection.send_notification(SessionNotification::new(
                    request.session_id.clone(),
                    SessionUpdate::ToolCall(
                        ToolCall::new(tool_call_id, name)
                            .status(ToolCallStatus::InProgress)
                            .raw_input(input),
                    ),
                ))?;
            }
            SessionEventKind::ToolCompleted {
                tool_call_id,
                output,
                is_error,
                ..
            } => {
                let status = if is_error {
                    ToolCallStatus::Failed
                } else {
                    ToolCallStatus::Completed
                };
                connection.send_notification(SessionNotification::new(
                    request.session_id.clone(),
                    SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                        tool_call_id,
                        ToolCallUpdateFields::new()
                            .status(status)
                            .raw_output(serde_json::Value::String(output)),
                    )),
                ))?;
            }
            SessionEventKind::ApprovalRequested {
                approval_id,
                title,
                detail,
                ..
            } => {
                let tool_call = ToolCallUpdate::new(
                    approval_id.clone(),
                    ToolCallUpdateFields::new()
                        .title(format!("{title}: {detail}"))
                        .status(ToolCallStatus::Pending),
                );
                let response = connection
                    .send_request(RequestPermissionRequest::new(
                        request.session_id.clone(),
                        tool_call,
                        vec![
                            PermissionOption::new(
                                "allow_once",
                                "Allow once",
                                PermissionOptionKind::AllowOnce,
                            ),
                            PermissionOption::new(
                                "allow_session",
                                "Allow for session",
                                PermissionOptionKind::AllowAlways,
                            ),
                            PermissionOption::new("deny", "Deny", PermissionOptionKind::RejectOnce),
                        ],
                    ))
                    .block_task()
                    .await?;
                let decision = match response.outcome {
                    RequestPermissionOutcome::Selected(selected)
                        if selected.option_id.to_string() == "allow_once" =>
                    {
                        ApprovalDecision::AllowOnce
                    }
                    RequestPermissionOutcome::Selected(selected)
                        if selected.option_id.to_string() == "allow_session" =>
                    {
                        ApprovalDecision::AllowSession
                    }
                    _ => ApprovalDecision::Deny,
                };
                session
                    .commands
                    .send(HostCommand::Approve {
                        session_id: session.id,
                        approval_id,
                        decision,
                    })
                    .await
                    .map_err(|_| Error::internal_error())?;
            }
            SessionEventKind::PlanUpdated { items } => {
                let entries = items
                    .into_iter()
                    .map(|item| {
                        PlanEntry::new(
                            item.content,
                            PlanEntryPriority::Medium,
                            match item.status {
                                PlanItemStatus::Pending => PlanEntryStatus::Pending,
                                PlanItemStatus::InProgress => PlanEntryStatus::InProgress,
                                PlanItemStatus::Completed => PlanEntryStatus::Completed,
                            },
                        )
                    })
                    .collect();
                connection.send_notification(SessionNotification::new(
                    request.session_id.clone(),
                    SessionUpdate::Plan(AcpPlan::new(entries)),
                ))?;
            }
            SessionEventKind::UsageUpdated {
                context_tokens: Some(used),
                context_window_tokens: Some(size),
                ..
            }
            | SessionEventKind::ContextWindowUpdated {
                context_tokens: used,
                context_window_tokens: size,
            } => {
                connection.send_notification(SessionNotification::new(
                    request.session_id.clone(),
                    SessionUpdate::UsageUpdate(UsageUpdate::new(used, size)),
                ))?;
            }
            SessionEventKind::TurnCompleted {
                message_id: completed,
                error,
                ..
            } if completed == message_id => {
                let reason = if error
                    .as_deref()
                    .is_some_and(|message| message.contains("interrupted"))
                {
                    StopReason::Cancelled
                } else {
                    StopReason::EndTurn
                };
                return responder.respond(PromptResponse::new(reason));
            }
            _ => {}
        }
    }
}

async fn respond_close(
    runtime: AcpRuntime,
    request: CloseSessionRequest,
    responder: Responder<CloseSessionResponse>,
) -> agent_client_protocol::Result<()> {
    let session = runtime
        .sessions
        .lock()
        .await
        .remove(&request.session_id)
        .ok_or_else(Error::invalid_params)?;
    let _ = session
        .commands
        .send(HostCommand::Stop {
            session_id: session.id,
        })
        .await;
    responder.respond(CloseSessionResponse::new())
}

async fn cancel(
    runtime: AcpRuntime,
    notification: CancelNotification,
) -> agent_client_protocol::Result<()> {
    let session = runtime
        .sessions
        .lock()
        .await
        .get(&notification.session_id)
        .cloned()
        .ok_or_else(Error::invalid_params)?;
    session
        .commands
        .send(HostCommand::Interrupt {
            session_id: session.id,
        })
        .await
        .map_err(|_| Error::internal_error())
}

impl AcpRuntime {
    async fn start_session(&self, cwd: PathBuf) -> Result<AcpSession> {
        anyhow::ensure!(cwd.is_absolute(), "ACP session cwd must be absolute");
        let cwd = cwd
            .canonicalize()
            .with_context(|| format!("ACP session cwd does not exist: {}", cwd.display()))?;
        let id = Uuid::new_v4();
        self.store.create_session(id).await?;
        let lock_path = self.sessions_dir.join(format!("{id}.lock"));
        let writer = SessionWriterLease::try_acquire(&lock_path)?
            .context("new ACP session unexpectedly has another writer")?;
        let (commands, command_rx) = mpsc::channel(64);
        let (event_tx, mut event_rx) = mpsc::channel(256);
        let (events, _) = broadcast::channel(512);
        let event_bus = events.clone();
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                let _ = event_bus.send(event);
            }
        });
        let mut capabilities = SessionCapabilities::from(&self.config.capabilities);
        capabilities.provider_capabilities = probe_provider_capabilities().await;
        let launch = LaunchSession {
            request_id: id,
            cwd: cwd.clone(),
            provider: self.args.provider.into(),
            model: self.args.model.clone(),
            effort: self.args.effort.clone(),
            fast: Some(false),
            response_language: ResponseLanguage::default(),
            permission_mode: self.args.permission.into(),
            name: cwd
                .file_name()
                .and_then(|name| name.to_str())
                .map(str::to_owned),
            initial_prompt: None,
            capabilities,
            subagent_concurrency_limit: Some(self.config.subagent_concurrency_limit()),
            extension_skill_roots: Vec::new(),
            team_policy: None,
        };
        let actor_lock = lock_path;
        tokio::spawn(async move {
            if let Err(error) =
                run_agent_session_with_writer(&actor_lock, id, launch, command_rx, event_tx, writer)
                    .await
            {
                tracing::error!(session_id = %id, %error, "ACP session actor failed");
            }
        });
        let session = AcpSession {
            id,
            commands,
            events,
        };
        self.sessions
            .lock()
            .await
            .insert(SessionId::new(id.to_string()), session.clone());
        Ok(session)
    }

    async fn resume_session(
        &self,
        id: Uuid,
        configuration: SessionConfiguration,
    ) -> Result<AcpSession> {
        let protocol_id = SessionId::new(id.to_string());
        if let Some(session) = self.sessions.lock().await.get(&protocol_id).cloned() {
            return Ok(session);
        }
        let lock_path = self.sessions_dir.join(format!("{id}.lock"));
        let writer = SessionWriterLease::try_acquire(&lock_path)?
            .context("ACP session is already owned by another Borg process")?;
        let (commands, command_rx) = mpsc::channel(64);
        let (event_tx, mut event_rx) = mpsc::channel(256);
        let (events, _) = broadcast::channel(512);
        let event_bus = events.clone();
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                let _ = event_bus.send(event);
            }
        });
        let mut capabilities = SessionCapabilities::from(&self.config.capabilities);
        capabilities.provider_capabilities = probe_provider_capabilities().await;
        let launch = LaunchSession {
            request_id: id,
            cwd: configuration.cwd.clone(),
            provider: configuration.provider,
            model: configuration.model,
            effort: configuration.effort,
            fast: Some(configuration.fast),
            response_language: configuration.response_language,
            permission_mode: configuration.permission_mode,
            name: configuration
                .cwd
                .file_name()
                .and_then(|name| name.to_str())
                .map(str::to_owned),
            initial_prompt: None,
            capabilities,
            subagent_concurrency_limit: Some(self.config.subagent_concurrency_limit()),
            extension_skill_roots: Vec::new(),
            team_policy: None,
        };
        tokio::spawn(async move {
            if let Err(error) =
                run_agent_session_with_writer(&lock_path, id, launch, command_rx, event_tx, writer)
                    .await
            {
                tracing::error!(session_id = %id, %error, "resumed ACP session actor failed");
            }
        });
        let session = AcpSession {
            id,
            commands,
            events,
        };
        self.sessions
            .lock()
            .await
            .insert(protocol_id, session.clone());
        Ok(session)
    }
}

fn prompt_text(blocks: &[ContentBlock]) -> agent_client_protocol::Result<String> {
    let mut text = Vec::with_capacity(blocks.len());
    for block in blocks {
        match block {
            ContentBlock::Text(content) => text.push(content.text.clone()),
            ContentBlock::ResourceLink(resource) => {
                text.push(format!("[Context: {}]({})", resource.name, resource.uri))
            }
            ContentBlock::Resource(resource) => match &resource.resource {
                EmbeddedResourceResource::TextResourceContents(resource) => text.push(format!(
                    "<context uri=\"{}\">\n{}\n</context>",
                    resource.uri, resource.text
                )),
                _ => return Err(Error::invalid_params()),
            },
            _ => return Err(Error::invalid_params()),
        }
    }
    Ok(text.join("\n\n"))
}

fn internal_error(error: anyhow::Error) -> Error {
    tracing::error!(%error, "ACP request failed");
    Error::internal_error()
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::v1::{
        EmbeddedResource, EmbeddedResourceResource, ResourceLink, TextResourceContents,
    };

    #[test]
    fn prompt_conversion_preserves_text_links_and_embedded_context() {
        let prompt = prompt_text(&[
            ContentBlock::Text(TextContent::new("Review this")),
            ContentBlock::ResourceLink(ResourceLink::new("lib.rs", "file:///workspace/src/lib.rs")),
            ContentBlock::Resource(EmbeddedResource::new(
                EmbeddedResourceResource::TextResourceContents(TextResourceContents::new(
                    "fn main() {}",
                    "file:///workspace/src/main.rs",
                )),
            )),
        ])
        .unwrap();
        assert!(prompt.contains("Review this"));
        assert!(prompt.contains("[Context: lib.rs](file:///workspace/src/lib.rs)"));
        assert!(prompt.contains("fn main() {}"));
    }
}
