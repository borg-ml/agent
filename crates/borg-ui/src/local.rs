use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use borg_remote::{
    EventActor, HostCommand, MessageStatus, SessionConfigAction, SessionEvent, SessionEventKind,
    SessionStore, SqliteSessionStore, SubagentAction, default_host_config_path,
    local_session_owner_is_active, login_provider_with_output, send_local_session_command,
    session_control_socket_path,
};
use uuid::Uuid;

use crate::{
    FrontendCommand, FrontendInspection, PeerIntent, PeerTarget, PromptDelivery,
    SessionPresentation, SessionView, timeline::TimelineProjector,
};

pub enum LocalSessionUpdate {
    Presentation(Box<SessionPresentation>),
    Sessions(Vec<LocalSessionOption>),
    RestoreComposer {
        text: String,
        attachments: Vec<PathBuf>,
    },
    Info {
        title: String,
        body: String,
    },
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
        let (command_tx, command_rx) = async_channel::bounded(64);
        let (update_tx, update_rx) = async_channel::bounded(8);
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
                    let (mut client, mut _owner) = match LocalSessionClient::open(session_id).await
                    {
                        Ok(Some(client)) => {
                            let owner = match ensure_session_owner(
                                &client.sessions_dir,
                                client.view().session_id,
                            )
                            .await
                            {
                                Ok(owner) => owner,
                                Err(error) => {
                                    let _ = ready_tx.send(Err(error));
                                    return;
                                }
                            };
                            (client, owner)
                        }
                        Ok(None) if session_id.is_none() => {
                            match launch_new_session_owner().await {
                                Ok(value) => value,
                                Err(error) => {
                                    let _ = ready_tx.send(Err(error));
                                    return;
                                }
                            }
                        }
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
                    let _ = update_tx.send_blocking(LocalSessionUpdate::Presentation(Box::new(
                        client.presentation(),
                    )));
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
                            let result =
                                match command {
                                    FrontendCommand::NewSession => {
                                        match launch_new_session_owner().await {
                                            Ok((next, next_owner)) => {
                                                _owner = next_owner;
                                                root_session_id = next.view().session_id;
                                                client = next;
                                                if let Ok(sessions) = client.list_sessions().await {
                                                    let _ = update_tx.send_blocking(
                                                        LocalSessionUpdate::Sessions(sessions),
                                                    );
                                                }
                                                Ok(true)
                                            }
                                            Err(error) => Err(error),
                                        }
                                    }
                                    FrontendCommand::OpenSession(session_id) => {
                                        match LocalSessionClient::open(Some(session_id)).await {
                                            Ok(Some(next)) => {
                                                match ensure_session_owner(
                                                    &next.sessions_dir,
                                                    session_id,
                                                )
                                                .await
                                                {
                                                    Ok(next_owner) => _owner = next_owner,
                                                    Err(error) => {
                                                        let _ = update_tx.send_blocking(
                                                            LocalSessionUpdate::Error(
                                                                error.to_string(),
                                                            ),
                                                        );
                                                        continue;
                                                    }
                                                }
                                                client = next;
                                                root_session_id = session_id;
                                                if let Ok(sessions) = client.list_sessions().await {
                                                    let _ = update_tx.send_blocking(
                                                        LocalSessionUpdate::Sessions(sessions),
                                                    );
                                                }
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
                                    FrontendCommand::Inspect(kind) => {
                                        match inspect_frontend(kind, &client.view().cwd).await {
                                            Ok((title, body)) => {
                                                let _ = update_tx.send_blocking(
                                                    LocalSessionUpdate::Info { title, body },
                                                );
                                                Ok(false)
                                            }
                                            Err(error) => Err(error),
                                        }
                                    }
                                    FrontendCommand::LoginProvider(provider) => {
                                        if matches!(
                                            client.view().state.status,
                                            Some(
                                                crate::SessionStatus::Starting
                                                    | crate::SessionStatus::Running
                                                    | crate::SessionStatus::WaitingForApproval
                                            )
                                        ) {
                                            Err(anyhow::anyhow!(
                                                "Interrupt the current turn before reconnecting the provider"
                                            ))
                                        } else {
                                            let title = format!("{} LOGIN", provider.label().to_uppercase());
                                            let mut body = format!(
                                                "Starting {} login. Complete the browser or device flow shown below.\n",
                                                provider.label()
                                            );
                                            let _ = update_tx.send_blocking(LocalSessionUpdate::Info {
                                                title: title.clone(),
                                                body: body.clone(),
                                            });
                                            let login = login_provider_with_output(provider, |line| {
                                                if body.len() > 16_000 {
                                                    let boundary = body.floor_char_boundary(body.len() - 12_000);
                                                    body.drain(..boundary);
                                                }
                                                body.push_str(line);
                                                body.push('\n');
                                                let _ = update_tx.send_blocking(LocalSessionUpdate::Info {
                                                    title: title.clone(),
                                                    body: body.clone(),
                                                });
                                            })
                                            .await;
                                            match login {
                                                Ok(()) => {
                                                    body.push_str("\nLogin complete. This provider is ready.");
                                                    let _ = update_tx.send_blocking(LocalSessionUpdate::Info {
                                                        title,
                                                        body,
                                                    });
                                                    Ok(false)
                                                }
                                                Err(error) => Err(error),
                                            }
                                        }
                                    }
                                    command => client.dispatch(command).await.map(|_| false),
                                };
                            match result {
                                Ok(true) => {
                                    let _ =
                                        update_tx.send_blocking(LocalSessionUpdate::Presentation(
                                            Box::new(client.presentation()),
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
                                for (text, attachments) in client.take_recalled_prompts() {
                                    if update_tx
                                        .send_blocking(LocalSessionUpdate::RestoreComposer {
                                            text,
                                            attachments,
                                        })
                                        .is_err()
                                    {
                                        return;
                                    }
                                }
                                if update_tx
                                    .send_blocking(LocalSessionUpdate::Presentation(Box::new(
                                        client.presentation(),
                                    )))
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
            .try_send(command)
            .context("Borg session command queue is unavailable")
    }

    pub fn updates(&self) -> async_channel::Receiver<LocalSessionUpdate> {
        self.updates.clone()
    }
}

async fn ensure_session_owner(
    sessions_dir: &Path,
    session_id: Uuid,
) -> Result<Option<tokio::process::Child>> {
    let socket = session_control_socket_path(sessions_dir, session_id);
    if local_session_owner_is_active(sessions_dir, session_id)? {
        return Ok(None);
    }
    let borg = borg_executable()?;
    let mut child = tokio::process::Command::new(&borg)
        .arg("--gui-owner")
        .arg("--resume")
        .arg(session_id.to_string())
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("failed to start session owner {}", borg.display()))?;
    for _ in 0..200 {
        if socket.exists() {
            return Ok(Some(child));
        }
        if let Some(status) = child.try_wait()? {
            anyhow::bail!("Borg session owner exited during startup with {status}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    child.kill().await.ok();
    anyhow::bail!("timed out waiting for the Borg session owner")
}

async fn launch_new_session_owner() -> Result<(LocalSessionClient, Option<tokio::process::Child>)> {
    let borg = borg_executable()?;
    let previous_session_id = LocalSessionClient::open(None)
        .await?
        .map(|client| client.view().session_id);
    let mut child = tokio::process::Command::new(&borg)
        .arg("--gui-owner")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("failed to start session owner {}", borg.display()))?;
    for _ in 0..200 {
        if let Some(client) = LocalSessionClient::open(None).await?
            && Some(client.view().session_id) != previous_session_id
        {
            let socket =
                session_control_socket_path(&client.sessions_dir, client.view().session_id);
            if socket.exists() {
                return Ok((client, Some(child)));
            }
        }
        if let Some(status) = child.try_wait()? {
            anyhow::bail!("Borg session owner exited during startup with {status}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    child.kill().await.ok();
    anyhow::bail!("timed out waiting for a new Borg session owner")
}

fn borg_executable() -> Result<PathBuf> {
    let executable_name = if cfg!(windows) { "borg.exe" } else { "borg" };
    let borg = std::env::current_exe()
        .context("failed to locate the Borg GUI executable")?
        .with_file_name(executable_name);
    anyhow::ensure!(
        borg.is_file(),
        "the Borg owner executable was not found at {}",
        borg.display()
    );
    Ok(borg)
}

async fn inspect_frontend(kind: FrontendInspection, cwd: &Path) -> Result<(String, String)> {
    if kind == FrontendInspection::LanguageServers {
        return Ok(("LANGUAGE SERVERS".into(), crate::lsp_support_summary()));
    }
    let borg = borg_executable()?;
    let arguments: &[&str] = match kind {
        FrontendInspection::Extensions => &["extensions", "list"],
        FrontendInspection::Customization => &["customize", "inspect"],
        FrontendInspection::LanguageServers => unreachable!(),
    };
    let output = tokio::process::Command::new(&borg)
        .args(arguments)
        .current_dir(cwd)
        .stdin(std::process::Stdio::null())
        .output()
        .await
        .with_context(|| format!("failed to run {} {}", borg.display(), arguments.join(" ")))?;
    anyhow::ensure!(
        output.status.success(),
        "{} {} exited with {}: {}",
        borg.display(),
        arguments.join(" "),
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );
    let body = String::from_utf8(output.stdout).context("Borg inspection output was not UTF-8")?;
    Ok((
        match kind {
            FrontendInspection::Extensions => "BLU EXTENSIONS",
            FrontendInspection::Customization => "EFFECTIVE CUSTOMIZATION",
            FrontendInspection::LanguageServers => unreachable!(),
        }
        .into(),
        body.trim().to_string(),
    ))
}

pub struct LocalSessionClient {
    store: Arc<SqliteSessionStore>,
    sessions_dir: PathBuf,
    view: SessionView,
    durable_history: Vec<borg_remote::SessionEvent>,
    live_events: HashMap<String, borg_remote::SessionEvent>,
    live_revision: u64,
    root_session_id: Uuid,
    timeline: TimelineProjector,
    recalled_prompts: Vec<(String, Vec<PathBuf>)>,
    composer_history: Arc<Vec<String>>,
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
            history: Arc::new(history),
            agents: Vec::new(),
            cwd,
        };
        rebuild_agents(&mut view);
        let durable_history = view.history.as_ref().clone();
        let timeline = TimelineProjector::from_events(&durable_history);
        let composer_history = store
            .recent_user_messages(session_id, 100)
            .await?
            .into_iter()
            .filter_map(|event| match event.kind {
                SessionEventKind::Message { text, .. } => Some(text),
                _ => None,
            })
            .collect();
        let mut client = Self {
            store,
            sessions_dir,
            view,
            durable_history,
            live_events: HashMap::new(),
            live_revision: 0,
            root_session_id,
            timeline,
            recalled_prompts: Vec::new(),
            composer_history: Arc::new(composer_history),
        };
        client.refresh_live().await?;
        client.rebuild_history();
        Ok(Some(client))
    }

    pub fn view(&self) -> &SessionView {
        &self.view
    }

    fn presentation(&self) -> SessionPresentation {
        let mut timeline = self.timeline.clone();
        let mut live = self.live_events.values().cloned().collect::<Vec<_>>();
        live.sort_by_key(|event| event.created_at);
        timeline.extend(&live);
        SessionPresentation {
            view: self.view.clone(),
            root_session_id: self.root_session_id,
            timeline: Arc::new(timeline.into_shared_entries()),
            composer_history: Arc::clone(&self.composer_history),
        }
    }

    pub async fn refresh(&mut self) -> Result<bool> {
        let events = self
            .store
            .events_after(self.view.session_id, self.view.state.latest_sequence, 1_024)
            .await?;
        let mut changed = !events.is_empty();
        for event in events {
            if let SessionEventKind::Message {
                actor: EventActor::User,
                text,
                status: MessageStatus::Complete | MessageStatus::Failed,
                ..
            } = &event.kind
            {
                let history = Arc::make_mut(&mut self.composer_history);
                if history.last() != Some(text) {
                    history.push(text.clone());
                    if history.len() > 100 {
                        history.remove(0);
                    }
                }
            }
            if let SessionEventKind::PromptRecalled {
                text, attachments, ..
            } = &event.kind
            {
                self.recalled_prompts
                    .push((text.clone(), attachments.clone()));
            }
            for key in event.kind.cleared_live_state_keys() {
                self.live_events.remove(&key);
            }
            self.view.state.apply(&event)?;
            self.timeline.push(&event);
            self.durable_history.push(event);
        }
        if self.durable_history.len() > 2_000 {
            self.durable_history
                .drain(..self.durable_history.len() - 2_000);
            self.timeline = TimelineProjector::from_events(&self.durable_history);
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

    fn take_recalled_prompts(&mut self) -> Vec<(String, Vec<PathBuf>)> {
        std::mem::take(&mut self.recalled_prompts)
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
        let history = Arc::make_mut(&mut self.view.history);
        history.clone_from(&self.durable_history);
        let mut live = self.live_events.values().cloned().collect::<Vec<_>>();
        live.sort_by_key(|event| event.created_at);
        history.extend(live);
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
        self.timeline = TimelineProjector::from_events(&self.durable_history);
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
            FrontendCommand::ControlPeer {
                target,
                intent,
                attachments,
                delivery,
            } => {
                for command in peer_host_commands(
                    session_id,
                    target,
                    &intent,
                    Uuid::new_v4(),
                    &attachments,
                    delivery,
                ) {
                    send_local_session_command(
                        &session_control_socket_path(&self.sessions_dir, session_id),
                        session_id,
                        command,
                    )
                    .await?;
                }
                return Ok(());
            }
            command => command,
        };
        let command = match command {
            FrontendCommand::SubmitPrompt {
                text,
                attachments,
                delivery,
            } => {
                let message_id = Uuid::new_v4();
                self.store
                    .admit_prompt(SessionEvent::new(
                        session_id,
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
                HostCommand::Prompt {
                    session_id,
                    message_id,
                    text,
                    attachments,
                    output_schema: None,
                    delivery,
                }
            }
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
            FrontendCommand::ApplyTodo(action) => HostCommand::Todo { session_id, action },
            FrontendCommand::RunExtension { command, arguments } => HostCommand::ExtensionCommand {
                session_id,
                invocation_id: Uuid::new_v4(),
                command,
                arguments,
            },
            FrontendCommand::ControlPeer { .. } => unreachable!(),
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
            | FrontendCommand::NewSession
            | FrontendCommand::OpenSession(_)
            | FrontendCommand::LoadOlderHistory
            | FrontendCommand::Inspect(_)
            | FrontendCommand::LoginProvider(_) => return Ok(()),
            FrontendCommand::Quit => HostCommand::Stop { session_id },
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
            } => {
                let message_id = Uuid::new_v4();
                self.store
                    .admit_prompt(SessionEvent::new(
                        self.view.session_id,
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
                SubagentAction::Prompt {
                    request_id,
                    target,
                    message_id,
                    text,
                    attachments,
                    delivery,
                }
            }
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

pub fn peer_host_commands(
    session_id: Uuid,
    target: PeerTarget,
    intent: &PeerIntent,
    message_id: Uuid,
    attachments: &[PathBuf],
    delivery: PromptDelivery,
) -> Vec<HostCommand> {
    let ensure = HostCommand::Subagent {
        session_id,
        action: SubagentAction::Ensure {
            request_id: Uuid::new_v4(),
            task_name: target.task_name_argument().to_string(),
            provider: target.provider(),
            model: Some(target.default_model().to_string()),
            effort: Some(target.default_effort().to_string()),
        },
    };
    match intent {
        PeerIntent::Ensure => vec![ensure],
        PeerIntent::Clear => vec![
            ensure,
            HostCommand::Subagent {
                session_id,
                action: SubagentAction::ClearContext {
                    request_id: Uuid::new_v4(),
                    target: target.task_name().to_string(),
                },
            },
        ],
        PeerIntent::Rotate { model, effort } => vec![HostCommand::Subagent {
            session_id,
            action: SubagentAction::Rotate {
                request_id: Uuid::new_v4(),
                task_name: target.task_name_argument().to_string(),
                provider: target.provider(),
                model: Some(
                    model
                        .clone()
                        .unwrap_or_else(|| target.default_model().to_string()),
                ),
                effort: Some(
                    effort
                        .clone()
                        .unwrap_or_else(|| target.default_effort().to_string()),
                ),
            },
        }],
        PeerIntent::Prompt(text) => vec![
            ensure,
            HostCommand::Subagent {
                session_id,
                action: SubagentAction::Prompt {
                    request_id: Uuid::new_v4(),
                    target: target.task_name().to_string(),
                    message_id,
                    text: text.clone(),
                    attachments: attachments.to_vec(),
                    delivery,
                },
            },
        ],
    }
}

fn rebuild_agents(view: &mut SessionView) {
    view.agents.clear();
    for event in view.history.iter() {
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
