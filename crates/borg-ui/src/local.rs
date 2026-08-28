use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use borg_remote::{
    HostCommand, SessionConfigAction, SessionEventKind, SessionStore, SqliteSessionStore,
    SubagentAction, default_host_config_path, send_local_session_command,
    session_control_socket_path,
};
use uuid::Uuid;

use crate::{FrontendCommand, SessionPresentation, SessionView};

pub enum LocalSessionUpdate {
    Presentation(SessionPresentation),
    Sessions(Vec<LocalSessionOption>),
    Error(String),
}

#[derive(Clone, Debug)]
pub struct LocalSessionOption {
    pub session_id: Uuid,
    pub title: String,
    pub cwd: PathBuf,
    pub status: Option<crate::SessionStatus>,
}

pub struct LocalSessionWorker {
    commands: async_channel::Sender<FrontendCommand>,
    updates: async_channel::Receiver<LocalSessionUpdate>,
}

impl LocalSessionWorker {
    pub fn start(session_id: Option<Uuid>) -> Result<Option<Self>> {
        let (command_tx, command_rx) = async_channel::unbounded();
        let (update_tx, update_rx) = async_channel::unbounded();
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("borg-gui-session".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error.into()));
                        return;
                    }
                };
                runtime.block_on(async move {
                    let mut client = match LocalSessionClient::open(session_id).await {
                        Ok(Some(client)) => client,
                        Ok(None) => {
                            let _ = ready_tx.send(Ok(false));
                            return;
                        }
                        Err(error) => {
                            let _ = ready_tx.send(Err(error));
                            return;
                        }
                    };
                    if ready_tx.send(Ok(true)).is_err() {
                        return;
                    }
                    let mut root_session_id = client.view().session_id;
                    if let Ok(sessions) = client.list_sessions().await {
                        let _ = update_tx.send_blocking(LocalSessionUpdate::Sessions(sessions));
                    }
                    let _ = update_tx.send_blocking(LocalSessionUpdate::Presentation(
                        SessionPresentation::new(client.view().clone()),
                    ));
                    loop {
                        let wait = if matches!(
                            client.view().state.status,
                            Some(
                                crate::SessionStatus::Starting
                                    | crate::SessionStatus::Running
                                    | crate::SessionStatus::WaitingForApproval
                            )
                        ) {
                            std::time::Duration::from_millis(50)
                        } else {
                            std::time::Duration::from_millis(750)
                        };
                        if let Ok(command) = tokio::time::timeout(wait, command_rx.recv()).await {
                            let Ok(command) = command else { return };
                            let result = match command {
                                FrontendCommand::OpenSession(session_id) => {
                                    match LocalSessionClient::open(Some(session_id)).await {
                                        Ok(Some(next)) => {
                                            client = next;
                                            root_session_id = session_id;
                                            Ok(true)
                                        }
                                        Ok(None) => Ok(false),
                                        Err(error) => Err(error),
                                    }
                                }
                                FrontendCommand::FocusAgent(target) => {
                                    match LocalSessionClient::open_for_root(
                                        target.unwrap_or(root_session_id),
                                        root_session_id,
                                    )
                                    .await
                                    {
                                        Ok(Some(next)) => {
                                            client = next;
                                            Ok(true)
                                        }
                                        Ok(None) => Ok(false),
                                        Err(error) => Err(error),
                                    }
                                }
                                FrontendCommand::LoadOlderHistory => {
                                    client.load_older_history().await
                                }
                                command => client.dispatch(command).await.map(|_| false),
                            };
                            match result {
                                Ok(true) => {
                                    let _ =
                                        update_tx.send_blocking(LocalSessionUpdate::Presentation(
                                            SessionPresentation::new(client.view().clone()),
                                        ));
                                }
                                Ok(false) => {}
                                Err(error) => {
                                    let _ = update_tx.send_blocking(LocalSessionUpdate::Error(
                                        error.to_string(),
                                    ));
                                }
                            }
                        }
                        match client.refresh().await {
                            Ok(true) => {
                                if update_tx
                                    .send_blocking(LocalSessionUpdate::Presentation(
                                        SessionPresentation::new(client.view().clone()),
                                    ))
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            Ok(false) => {}
                            Err(error) => {
                                if update_tx
                                    .send_blocking(LocalSessionUpdate::Error(error.to_string()))
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        }
                    }
                });
            })
            .context("failed to start the Borg GUI session worker")?;
        match ready_rx
            .recv()
            .context("Borg GUI session worker stopped during startup")??
        {
            true => Ok(Some(Self {
                commands: command_tx,
                updates: update_rx,
            })),
            false => Ok(None),
        }
    }

    pub fn send(&self, command: FrontendCommand) -> Result<()> {
        self.commands
            .send_blocking(command)
            .context("Borg session worker is no longer running")
    }

    pub fn updates(&self) -> async_channel::Receiver<LocalSessionUpdate> {
        self.updates.clone()
    }
}

pub struct LocalSessionClient {
    store: Arc<SqliteSessionStore>,
    sessions_dir: PathBuf,
    view: SessionView,
    durable_history: Vec<borg_remote::SessionEvent>,
    live_events: HashMap<String, borg_remote::SessionEvent>,
    live_revision: u64,
    root_session_id: Uuid,
}

impl LocalSessionClient {
    pub async fn open(session_id: Option<Uuid>) -> Result<Option<Self>> {
        let sessions_dir = default_host_config_path()
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("sessions");
        let store =
            Arc::new(SqliteSessionStore::open(sessions_dir.join("sessions.sqlite3")).await?);
        let session_id = match session_id {
            Some(session_id) => session_id,
            None => match store.list_sessions(1).await?.first() {
                Some(session) => session.session_id,
                None => return Ok(None),
            },
        };
        Self::open_from_store(store, sessions_dir, session_id, session_id).await
    }

    async fn open_for_root(session_id: Uuid, root_session_id: Uuid) -> Result<Option<Self>> {
        let sessions_dir = default_host_config_path()
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("sessions");
        let store =
            Arc::new(SqliteSessionStore::open(sessions_dir.join("sessions.sqlite3")).await?);
        Self::open_from_store(store, sessions_dir, session_id, root_session_id).await
    }

    async fn open_from_store(
        store: Arc<SqliteSessionStore>,
        sessions_dir: PathBuf,
        session_id: Uuid,
        root_session_id: Uuid,
    ) -> Result<Option<Self>> {
        anyhow::ensure!(
            store.contains_session(session_id).await?,
            "Borg session {session_id} does not exist"
        );
        let state = store.state(session_id).await?;
        let after = state.latest_sequence.saturating_sub(2_000);
        let history = store.events_after(session_id, after, 2_000).await?;
        let cwd = state
            .configuration
            .as_ref()
            .map(|configuration| configuration.cwd.clone())
            .unwrap_or_else(|| PathBuf::from("."));
        let mut view = SessionView {
            session_id,
            goal: state.goal.clone(),
            state,
            history,
            agents: Vec::new(),
            cwd,
        };
        rebuild_agents(&mut view);
        let durable_history = view.history.clone();
        let mut client = Self {
            store,
            sessions_dir,
            view,
            durable_history,
            live_events: HashMap::new(),
            live_revision: 0,
            root_session_id,
        };
        client.refresh_live().await?;
        client.rebuild_history();
        Ok(Some(client))
    }

    pub fn view(&self) -> &SessionView {
        &self.view
    }

    pub async fn refresh(&mut self) -> Result<bool> {
        let events = self
            .store
            .events_after(self.view.session_id, self.view.state.latest_sequence, 1_024)
            .await?;
        let mut changed = !events.is_empty();
        for event in events {
            for key in event.kind.cleared_live_state_keys() {
                self.live_events.remove(&key);
            }
            self.view.state.apply(&event)?;
            self.durable_history.push(event);
        }
        if self.durable_history.len() > 2_000 {
            self.durable_history
                .drain(..self.durable_history.len() - 2_000);
        }
        changed |= self.refresh_live().await?;
        if !changed {
            return Ok(false);
        }
        self.rebuild_history();
        self.view.goal = self.view.state.goal.clone();
        if let Some(configuration) = &self.view.state.configuration {
            self.view.cwd = configuration.cwd.clone();
        }
        rebuild_agents(&mut self.view);
        Ok(true)
    }

    async fn list_sessions(&self) -> Result<Vec<LocalSessionOption>> {
        Ok(self
            .store
            .list_sessions(24)
            .await?
            .into_iter()
            .map(|summary| {
                let cwd = summary
                    .state
                    .configuration
                    .as_ref()
                    .map(|configuration| configuration.cwd.clone())
                    .unwrap_or_default();
                LocalSessionOption {
                    session_id: summary.session_id,
                    title: summary
                        .state
                        .first_prompt
                        .clone()
                        .unwrap_or_else(|| "Untitled session".into()),
                    cwd,
                    status: summary.state.status,
                }
            })
            .collect())
    }

    async fn refresh_live(&mut self) -> Result<bool> {
        let events = self
            .store
            .live_events_after(self.view.session_id, self.live_revision)
            .await?;
        if events.is_empty() {
            return Ok(false);
        }
        for live in events {
            self.live_revision = self.live_revision.max(live.revision);
            if let Some(key) = live.event.kind.live_state_key() {
                self.live_events.insert(key, live.event);
            }
        }
        Ok(true)
    }

    fn rebuild_history(&mut self) {
        self.view.history.clone_from(&self.durable_history);
        let mut live = self.live_events.values().cloned().collect::<Vec<_>>();
        live.sort_by_key(|event| event.created_at);
        self.view.history.extend(live);
    }

    async fn load_older_history(&mut self) -> Result<bool> {
        let Some(first) = self.durable_history.first().map(|event| event.sequence) else {
            return Ok(false);
        };
        if first <= 1 {
            return Ok(false);
        }
        let mut older = self
            .store
            .events_after(self.view.session_id, first.saturating_sub(501), 500)
            .await?;
        older.retain(|event| event.sequence < first);
        if older.is_empty() {
            return Ok(false);
        }
        older.append(&mut self.durable_history);
        self.durable_history = older;
        self.rebuild_history();
        rebuild_agents(&mut self.view);
        Ok(true)
    }

    pub async fn dispatch(&self, command: FrontendCommand) -> Result<()> {
        if self.view.session_id != self.root_session_id {
            return self.dispatch_to_child(command).await;
        }
        let session_id = self.root_session_id;
        let command = match command {
            FrontendCommand::SubmitPrompt {
                text,
                attachments,
                delivery,
            } => HostCommand::Prompt {
                session_id,
                message_id: Uuid::new_v4(),
                text,
                attachments,
                output_schema: None,
                delivery,
            },
            FrontendCommand::RecallQueuedPrompt(message_id) => HostCommand::RecallQueuedPrompt {
                session_id,
                message_id,
            },
            FrontendCommand::FlushPendingInput => HostCommand::FlushPendingInput { session_id },
            FrontendCommand::Interrupt => HostCommand::Interrupt { session_id },
            FrontendCommand::Approve(decision) => HostCommand::Approve {
                session_id,
                approval_id: self
                    .view
                    .state
                    .pending_approval_id
                    .clone()
                    .context("this session is not waiting for approval")?,
                decision,
            },
            FrontendCommand::RespondToProviderInteraction(response) => {
                HostCommand::RespondToProviderInteraction {
                    session_id,
                    interaction_id: self
                        .view
                        .state
                        .pending_provider_interaction_id
                        .clone()
                        .context("this session is not waiting for provider input")?,
                    response,
                }
            }
            FrontendCommand::ApplyGoal(action) => HostCommand::Goal { session_id, action },
            FrontendCommand::SetModel { provider, model } => HostCommand::Configure {
                session_id,
                action: SessionConfigAction::SetProvider {
                    provider,
                    model: Some(model),
                },
            },
            FrontendCommand::SetPermission(permission_mode) => HostCommand::Configure {
                session_id,
                action: SessionConfigAction::SetPermissionMode { permission_mode },
            },
            FrontendCommand::SetLanguage(language) => HostCommand::Configure {
                session_id,
                action: SessionConfigAction::SetResponseLanguage { language },
            },
            FrontendCommand::SetEffort(effort) => HostCommand::Configure {
                session_id,
                action: SessionConfigAction::SetEffort { effort },
            },
            FrontendCommand::SetFast(enabled) => HostCommand::Configure {
                session_id,
                action: SessionConfigAction::SetFast { enabled },
            },
            FrontendCommand::ClearContext => HostCommand::ClearContext { session_id },
            FrontendCommand::Compact => HostCommand::Compact { session_id },
            FrontendCommand::FocusAgent(_)
            | FrontendCommand::OpenSession(_)
            | FrontendCommand::LoadOlderHistory
            | FrontendCommand::Quit => return Ok(()),
        };
        send_local_session_command(
            &session_control_socket_path(&self.sessions_dir, session_id),
            session_id,
            command,
        )
        .await
    }

    async fn dispatch_to_child(&self, command: FrontendCommand) -> Result<()> {
        let session_id = self.root_session_id;
        let target = self.view.session_id.to_string();
        let request_id = Uuid::new_v4();
        let action = match command {
            FrontendCommand::SubmitPrompt {
                text,
                attachments,
                delivery,
            } => SubagentAction::Prompt {
                request_id,
                target,
                message_id: Uuid::new_v4(),
                text,
                attachments,
                delivery,
            },
            FrontendCommand::RecallQueuedPrompt(message_id) => SubagentAction::RecallPrompt {
                request_id,
                target,
                message_id,
            },
            FrontendCommand::FlushPendingInput => {
                SubagentAction::FlushPendingInput { request_id, target }
            }
            FrontendCommand::Interrupt => SubagentAction::Interrupt { request_id, target },
            FrontendCommand::Approve(decision) => SubagentAction::Approve {
                request_id,
                target,
                approval_id: self
                    .view
                    .state
                    .pending_approval_id
                    .clone()
                    .context("this agent is not waiting for approval")?,
                decision,
            },
            FrontendCommand::ClearContext => SubagentAction::ClearContext { request_id, target },
            FrontendCommand::Quit => SubagentAction::Stop { request_id, target },
            _ => anyhow::bail!("this setting can only be changed from the root session"),
        };
        send_local_session_command(
            &session_control_socket_path(&self.sessions_dir, session_id),
            session_id,
            HostCommand::Subagent { session_id, action },
        )
        .await
    }
}

fn rebuild_agents(view: &mut SessionView) {
    view.agents.clear();
    for event in &view.history {
        if let SessionEventKind::SubagentActivity { agent, .. } = &event.kind {
            if let Some(existing) = view
                .agents
                .iter_mut()
                .find(|existing| existing.session_id == agent.session_id)
            {
                *existing = agent.clone();
            } else {
                view.agents.push(agent.clone());
            }
        }
    }
}
