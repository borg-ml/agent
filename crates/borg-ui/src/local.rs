use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use borg_remote::{
    HostCommand, SessionConfigAction, SessionEventKind, SessionStore, SqliteSessionStore,
    default_host_config_path, send_local_session_command, session_control_socket_path,
};
use uuid::Uuid;

use crate::{FrontendCommand, SessionView};

pub enum LocalSessionUpdate {
    View(SessionView),
    Error(String),
}

pub struct LocalSessionWorker {
    commands: std::sync::mpsc::Sender<FrontendCommand>,
    updates: std::sync::mpsc::Receiver<LocalSessionUpdate>,
}

impl LocalSessionWorker {
    pub fn start(session_id: Option<Uuid>) -> Result<Option<Self>> {
        let (command_tx, command_rx) = std::sync::mpsc::channel();
        let (update_tx, update_rx) = std::sync::mpsc::channel();
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
                    let _ = update_tx.send(LocalSessionUpdate::View(client.view().clone()));
                    loop {
                        while let Ok(command) = command_rx.try_recv() {
                            if let Err(error) = client.dispatch(command).await {
                                let _ =
                                    update_tx.send(LocalSessionUpdate::Error(error.to_string()));
                            }
                        }
                        match client.refresh().await {
                            Ok(true) => {
                                if update_tx
                                    .send(LocalSessionUpdate::View(client.view().clone()))
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            Ok(false) => {}
                            Err(error) => {
                                if update_tx
                                    .send(LocalSessionUpdate::Error(error.to_string()))
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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
            .send(command)
            .context("Borg session worker is no longer running")
    }

    pub fn try_recv(&self) -> Option<LocalSessionUpdate> {
        self.updates.try_recv().ok()
    }
}

pub struct LocalSessionClient {
    store: Arc<SqliteSessionStore>,
    sessions_dir: PathBuf,
    view: SessionView,
    durable_history: Vec<borg_remote::SessionEvent>,
    live_events: HashMap<String, borg_remote::SessionEvent>,
    live_revision: u64,
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

    pub async fn dispatch(&self, command: FrontendCommand) -> Result<()> {
        let session_id = self.view.session_id;
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
            FrontendCommand::FocusAgent(_) | FrontendCommand::LoadOlderHistory => return Ok(()),
            FrontendCommand::Quit => HostCommand::Stop { session_id },
        };
        send_local_session_command(
            &session_control_socket_path(&self.sessions_dir, session_id),
            session_id,
            command,
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
