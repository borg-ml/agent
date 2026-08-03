use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, BufRead, IsTerminal, Write};
use std::panic::{self, AssertUnwindSafe, PanicHookInfo};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Context, Result};
use borg_remote::{
    AgentTurnExecutor, ApprovalDecision, CodingProvider, EventActor, GoalAction, GoalStatus,
    HostCommand, HostConfig, HostExecutorFactory, JsonlSessionStore, LaunchSession,
    LocalAgentSettings, LocalAgentTurnExecutor, LocalSessionControlServer, MessageStatus,
    PermissionMode, PlanItem, PlanItemStatus, PromptDelivery, ResponseLanguage,
    SessionConfigAction, SessionEvent, SessionEventKind, SessionGoal, SessionState, SessionStatus,
    SessionStore, SessionWriterLease, SpawnSubagent, SqliteSessionStore, SqliteWorkspaceStore,
    SubagentAction, SubagentSnapshot, SubagentStatus, TodoAction, default_host_config_path,
    enroll_host, local_session_owner_uses_current_binary, login_provider, mirror_local_session,
    probe_capabilities, provider_credentials_present, run_agent_session_with_store_and_writer,
    run_agent_session_with_store_writer_and_peers, run_attached_session,
    run_host_with_executor_factory, send_local_session_command, session_control_socket_path,
};
use chrono::{Local, Utc};
use futures_util::FutureExt;
use pulldown_cmark::{Event as MarkdownEvent, Parser as MarkdownParser};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::{Child, Command as TokioCommand};
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

use crate::agent_config::AgentConfig;
use crate::cli::{LocalAgentCliArgs, RemoteCommand};
use crate::editor_preferences::{ActiveMessageBehavior, EditorPreferences};
use crate::sleep_inhibitor::SleepInhibitor;
use crate::terminal_ui::{
    BorgTerminal, ProviderAuthChoice, ResumeSessionOption, TerminalInputEvent, UiAction,
};

const MIN_TUI_FPS: u64 = 15;
const MAX_TUI_FPS: u64 = 240;
const ACTIVITY_FRAME_INTERVAL: std::time::Duration = std::time::Duration::from_millis(120);
/// Keep first paint bounded while retaining enough context to include a real
/// recent exchange instead of an empty shell made only of projection events.
const RICH_TUI_HISTORY_EVENT_LIMIT: usize = 128;
// Keep first paint from deserializing a whole tool-heavy turn. The visible
// tail is still capped separately; this scan only supplies a few recent real
// conversation messages that may sit behind tool activity.
const RICH_TUI_HISTORY_BOOTSTRAP_SCAN_LIMIT: usize = 512;
const RICH_TUI_HISTORY_MESSAGE_LIMIT: usize = 8;
const RICH_TUI_HISTORY_PAGE_SIZE: usize = 512;
const RICH_TUI_PROMPT_HISTORY_LIMIT: usize = 64;
type BluDiscoveryResult = Result<(
    crate::extensions::ExtensionCatalog,
    Vec<borg_provider::mcp::ExternalMcpServer>,
)>;
type BluDiscoveryTask = tokio::task::JoinHandle<BluDiscoveryResult>;
/// Resume filtering is local and eager, so load enough history to make search useful while
/// keeping picker construction and per-keystroke filtering bounded.
const RESUME_PICKER_SESSION_LIMIT: usize = 1_000;

fn rich_terminal_can_prompt(stdin_is_terminal: bool, stdout_is_terminal: bool, json: bool) -> bool {
    stdin_is_terminal && stdout_is_terminal && !json
}

#[derive(Default)]
struct TuiCrashContext {
    session_id: Mutex<Option<Uuid>>,
    tui_active: Arc<AtomicBool>,
}

impl TuiCrashContext {
    fn set_session_id(&self, session_id: Uuid) {
        *self
            .session_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(session_id);
    }

    fn session_id(&self) -> Option<Uuid> {
        *self
            .session_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

type PanicHook = dyn for<'a> Fn(&PanicHookInfo<'a>) + Send + Sync + 'static;

/// Suppress the default panic report while the rich TUI is live. The terminal
/// is dropped during unwinding, and the caller prints the resume instructions
/// only after that cleanup has completed.
struct TuiPanicHook {
    previous: Arc<Mutex<Option<Box<PanicHook>>>>,
}

impl TuiPanicHook {
    fn install(tui_active: Arc<AtomicBool>) -> Self {
        let previous = Arc::new(Mutex::new(Some(panic::take_hook())));
        let hook_previous = Arc::clone(&previous);
        panic::set_hook(Box::new(move |info| {
            if !tui_active.load(Ordering::Acquire)
                && let Some(previous) = hook_previous
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .as_ref()
            {
                previous(info);
            }
        }));
        Self { previous }
    }
}

impl Drop for TuiPanicHook {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }
        let _ = panic::take_hook();
        let previous = self
            .previous
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(previous) = previous {
            panic::set_hook(previous);
        }
    }
}

pub(crate) async fn run_remote_command(command: RemoteCommand) -> Result<()> {
    match command {
        RemoteCommand::Connect {
            server,
            roots,
            name,
            config,
        } => {
            let config_path = config.unwrap_or_else(default_host_config_path);
            let roots = remote_roots_or_current(roots)?;
            connect_remote_account(&server, name.as_deref(), roots, &config_path).await?;
            install_host_service(&config_path).await?;
        }
        RemoteCommand::Enroll {
            server,
            token,
            name,
            roots,
            config,
        } => {
            let config_path = config.unwrap_or_else(default_host_config_path);
            let enrolled =
                enroll_host(&server, &token, name.as_deref(), roots, &config_path).await?;
            println!(
                "Enrolled {} as {}. Run `borg remote host` to connect.",
                enrolled.name, enrolled.host_id
            );
        }
        RemoteCommand::Host { config } => {
            let agent_config = AgentConfig::load(None)?;
            crate::updater::spawn_background(agent_config.updates);
            let config_path = config.unwrap_or_else(default_host_config_path);
            println!("Borg Remote host connected from {}", config_path.display());
            run_host_with_executor_factory(&config_path, blu_host_executor_factory()).await?;
        }
        RemoteCommand::Install { config } => {
            let config_path = config.unwrap_or_else(default_host_config_path);
            install_host_service(&config_path).await?;
        }
        RemoteCommand::Login { provider } => {
            login_provider(provider.into()).await?;
        }
        RemoteCommand::Status { roots } => {
            let roots = roots
                .into_iter()
                .map(|root| root.canonicalize())
                .collect::<io::Result<Vec<_>>>()
                .context("failed to resolve a remote root")?;
            println!(
                "{}",
                serde_json::to_string_pretty(&probe_capabilities(roots).await)?
            );
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct DeviceAuthorization {
    token: String,
    verification_url: String,
}

#[derive(Deserialize)]
struct DeviceAuthorizationStatus {
    status: String,
}

fn remote_roots_or_current(roots: Vec<PathBuf>) -> Result<Vec<PathBuf>> {
    if roots.is_empty() {
        Ok(vec![
            std::env::current_dir().context("failed to read the current directory")?,
        ])
    } else {
        Ok(roots)
    }
}

async fn connect_remote_account(
    server: &str,
    name: Option<&str>,
    roots: Vec<PathBuf>,
    config_path: &Path,
) -> Result<()> {
    let server = server.trim_end_matches('/');
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(15))
        .build()?;
    let response = client
        .post(format!("{server}/api/remote/device-authorizations"))
        .send()
        .await
        .context("failed to contact Borg")?;
    anyhow::ensure!(
        response.status().is_success(),
        "Borg rejected the remote connection request: {}",
        response.text().await.unwrap_or_default()
    );
    let authorization: DeviceAuthorization = response
        .json()
        .await
        .context("Borg returned an invalid remote connection response")?;
    println!(
        "Sign in to Borg and approve this machine:\n\n  {}\n",
        authorization.verification_url
    );
    if let Err(error) = open_browser(&authorization.verification_url).await {
        eprintln!("Could not open your browser automatically: {error}");
    }
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(600);
    loop {
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "remote connection approval expired; run /remote again"
        );
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let status = client
            .get(format!(
                "{server}/api/remote/device-authorizations/{}",
                urlencoding::encode(&authorization.token)
            ))
            .send()
            .await
            .context("lost contact with Borg while waiting for approval")?;
        if !status.status().is_success() {
            continue;
        }
        match status
            .json::<DeviceAuthorizationStatus>()
            .await?
            .status
            .as_str()
        {
            "approved" => break,
            "expired" => anyhow::bail!("remote connection approval expired; run /remote again"),
            _ => {}
        }
    }
    let enrolled = enroll_host(server, &authorization.token, name, roots, config_path).await?;
    println!(
        "Connected {} to your Borg account as {}.",
        enrolled.name, enrolled.host_id
    );
    Ok(())
}

fn blu_host_executor_factory() -> HostExecutorFactory {
    Arc::new(|host, launch| {
        let agent_config = AgentConfig::load(None)?;
        let (catalog, extension_servers) = crate::extensions::discover(
            &launch.cwd,
            &agent_config.capabilities,
            agent_config.extensions.allow_project_mcp,
        )?;
        let mut servers = agent_config.external_mcp_servers();
        servers.extend(extension_servers);
        let roots = catalog.active_skill_roots();
        let local_settings = LocalAgentSettings {
            approval_reviewer_model: agent_config.approvals.reviewer_model.clone(),
            approval_reviewer_effort: agent_config.approvals.reviewer_effort.clone(),
        };
        let reload_cwd = launch.cwd.clone();
        let reload = move || {
            let agent_config = AgentConfig::load(None)?;
            let (catalog, extension_servers) = crate::extensions::discover(
                &reload_cwd,
                &agent_config.capabilities,
                agent_config.extensions.allow_project_mcp,
            )?;
            anyhow::ensure!(
                !catalog.has_errors(),
                "Blu catalog has errors; keeping the remote session's last-known-good snapshot"
            );
            let mut servers = agent_config.external_mcp_servers();
            servers.extend(extension_servers);
            Ok((servers, catalog.active_skill_roots()))
        };
        let executor = LocalAgentTurnExecutor::with_model_gateway_and_settings(
            borg_provider::provider::ModelGateway {
                endpoint: format!(
                    "{}/api/remote/host/kimi/chat/completions",
                    host.server.trim_end_matches('/')
                ),
                bearer_token: host.host_token.clone(),
            },
            local_settings,
        )
        .with_external_mcp_servers(servers)
        .with_extension_skill_roots(roots)
        .with_runtime_extension_loader(reload);
        Ok(Arc::new(executor) as Arc<dyn AgentTurnExecutor>)
    })
}

async fn open_browser(url: &str) -> Result<()> {
    let (program, args): (&str, Vec<&str>) = if cfg!(target_os = "macos") {
        ("open", vec![url])
    } else if cfg!(target_os = "windows") {
        ("cmd", vec!["/C", "start", "", url])
    } else {
        ("xdg-open", vec![url])
    };
    let status = tokio::process::Command::new(program)
        .args(args)
        .status()
        .await
        .with_context(|| format!("failed to start {program}"))?;
    anyhow::ensure!(status.success(), "{program} exited with {status}");
    Ok(())
}

async fn install_host_service(config_path: &Path) -> Result<()> {
    anyhow::ensure!(
        cfg!(target_os = "linux"),
        "`borg remote install` currently supports Linux systemd user services; run `borg remote host` from your platform's login service"
    );
    anyhow::ensure!(
        config_path.is_file(),
        "host config does not exist at {}; run `borg remote enroll` first",
        config_path.display()
    );
    let executable = std::env::current_exe().context("failed to locate the borg executable")?;
    let service_dir = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .context("HOME or XDG_CONFIG_HOME is required to install the user service")?
        .join("systemd")
        .join("user");
    fs::create_dir_all(&service_dir)?;
    let service_path = service_dir.join("borg-remote.service");
    let path = std::env::var("PATH").unwrap_or_default();
    let service = format!(
        "[Unit]\nDescription=Borg Remote host\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=simple\nExecStart={} remote host --config {}\nEnvironment={}\nRestart=always\nRestartSec=2\n\n[Install]\nWantedBy=default.target\n",
        systemd_quote(&executable.to_string_lossy()),
        systemd_quote(&config_path.to_string_lossy()),
        systemd_quote(&format!("PATH={path}")),
    );
    fs::write(&service_path, service)
        .with_context(|| format!("failed to write {}", service_path.display()))?;
    for args in host_service_systemctl_commands() {
        let mut command = tokio::process::Command::new("systemctl");
        command.args(args);
        configure_systemd_user_bus(&mut command);
        let status = command
            .status()
            .await
            .context("failed to run systemctl --user")?;
        anyhow::ensure!(status.success(), "systemctl {} failed", args.join(" "));
    }
    println!(
        "Borg Remote is installed and running as a user service.\n  {}",
        service_path.display()
    );
    Ok(())
}

#[cfg(target_os = "linux")]
fn configure_systemd_user_bus(command: &mut tokio::process::Command) {
    use std::os::unix::fs::MetadataExt;

    let runtime_dir = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .or_else(|| {
            let home = std::env::var_os("HOME").map(PathBuf::from)?;
            let uid = fs::metadata(home).ok()?.uid();
            let candidate = PathBuf::from(format!("/run/user/{uid}"));
            candidate.is_dir().then_some(candidate)
        });
    let Some(runtime_dir) = runtime_dir else {
        return;
    };
    if std::env::var_os("XDG_RUNTIME_DIR").is_none() {
        command.env("XDG_RUNTIME_DIR", &runtime_dir);
    }
    if std::env::var_os("DBUS_SESSION_BUS_ADDRESS").is_none() {
        let bus = runtime_dir.join("bus");
        if bus.exists() {
            command.env(
                "DBUS_SESSION_BUS_ADDRESS",
                format!("unix:path={}", bus.display()),
            );
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn configure_systemd_user_bus(_command: &mut tokio::process::Command) {}

fn host_service_systemctl_commands() -> [&'static [&'static str]; 3] {
    [
        &["--user", "daemon-reload"],
        &["--user", "enable", "borg-remote.service"],
        &["--user", "restart", "borg-remote.service"],
    ]
}

fn systemd_quote(value: &str) -> String {
    format!(
        "\"{}\"",
        value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('%', "%%")
    )
}

pub(crate) async fn run_local_agent(args: LocalAgentCliArgs) -> Result<()> {
    let agent_config = AgentConfig::load(args.config.as_deref())?;
    crate::updater::spawn_background(agent_config.updates.clone());
    let ephemeral_sessions = args.ephemeral.then(tempfile::tempdir).transpose()?;
    let crash_context = Arc::new(TuiCrashContext::default());
    // Keep this guard outside the caught session future: terminal Drop runs
    // while unwinding, then this guard is restored after catch_unwind returns.
    let _tui_panic_hook = TuiPanicHook::install(Arc::clone(&crash_context.tui_active));
    let mut selected_session = None;
    let mut restored_prompt = None;
    loop {
        let result = AssertUnwindSafe(run_local_agent_session(
            &args,
            selected_session,
            restored_prompt.take(),
            ephemeral_sessions.as_ref().map(tempfile::TempDir::path),
            Arc::clone(&crash_context),
        ))
        .catch_unwind()
        .await;
        match result {
            Ok(Ok(Some((next_session, next_prompt)))) => {
                crash_context.tui_active.store(false, Ordering::Release);
                selected_session = Some(next_session);
                restored_prompt = next_prompt;
            }
            Ok(Ok(None)) => {
                crash_context.tui_active.store(false, Ordering::Release);
                return Ok(());
            }
            Ok(Err(error)) if crash_context.tui_active.swap(false, Ordering::AcqRel) => {
                let Some(session_id) = crash_context.session_id() else {
                    return Err(error);
                };
                println!("{}", resume_instructions(session_id, false));
                return Ok(());
            }
            Ok(Err(error)) => return Err(error),
            Err(payload) if crash_context.tui_active.swap(false, Ordering::AcqRel) => {
                let Some(session_id) = crash_context.session_id() else {
                    panic::resume_unwind(payload);
                };
                println!("{}", resume_instructions(session_id, false));
                return Ok(());
            }
            Err(payload) => panic::resume_unwind(payload),
        };
    }
}

pub(crate) async fn print_local_workspaces(json: bool) -> Result<()> {
    let host_config_path = default_host_config_path();
    let sessions_dir = host_config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("sessions");
    let workspace_path = sessions_dir.join("workspaces.sqlite3");
    let workspaces = if workspace_path.is_file() {
        let store = SqliteWorkspaceStore::open(&workspace_path).await?;
        let display_name = std::env::var("USER").unwrap_or_else(|_| "Local user".to_string());
        store
            .list_workspaces_for_participant(borg_remote::local_human_participant_id(&display_name))
            .await?
    } else {
        Vec::new()
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&workspaces)?);
    } else if workspaces.is_empty() {
        println!("No local multiplayer workspaces.");
    } else {
        for workspace in workspaces {
            println!("{}  {}", workspace.id, workspace.name);
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LocalSessionAccess {
    Owned,
    Attached,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionSwitch {
    StopOwnedSession,
    DetachViewer,
}

/// Durable session projection advanced by the same ordered event stream the
/// local TUI consumes.
///
/// The SQLite projection can be ahead of this stream because the session actor
/// commits an event before forwarding it. Keeping a delivery-aligned projection
/// prevents an asynchronous store read from moving the UI backward or forward
/// across events it has not rendered yet.
struct DeliveredSessionProjection {
    state: SessionState,
}

impl DeliveredSessionProjection {
    fn new(state: SessionState) -> Self {
        Self { state }
    }

    fn observe(&mut self, event: &SessionEvent) -> Result<()> {
        if event.sequence > 0 {
            self.state.apply(event)?;
        } else if let SessionEventKind::ContextWindowUpdated {
            context_tokens,
            context_window_tokens,
        } = &event.kind
        {
            // Context-window updates are coalesced live state. They deliberately
            // do not consume a durable sequence, but history reprojection still
            // needs the latest value already delivered to this TUI.
            self.state.usage.context_tokens = Some(*context_tokens);
            self.state.usage.context_window_tokens = Some(*context_window_tokens);
        }
        Ok(())
    }

    fn state(&self) -> &SessionState {
        &self.state
    }
}

impl LocalSessionAccess {
    fn is_attached(self) -> bool {
        self == Self::Attached
    }

    fn switch(self, status: SessionStatus) -> Result<SessionSwitch> {
        match self {
            Self::Attached => Ok(SessionSwitch::DetachViewer),
            Self::Owned
                if matches!(
                    status,
                    SessionStatus::Starting
                        | SessionStatus::Running
                        | SessionStatus::WaitingForApproval
                ) =>
            {
                anyhow::bail!("Interrupt the current turn before resuming another session.")
            }
            Self::Owned => Ok(SessionSwitch::StopOwnedSession),
        }
    }
}

fn stale_local_owner_can_handoff(status: Option<SessionStatus>) -> bool {
    matches!(status, Some(SessionStatus::Ready | SessionStatus::Stopped))
}

/// A stopped state is the durable result of the previous local process going
/// away, not the state of the new process the user just asked to resume. Keep
/// the owner UI responsive while the actor rehydrates a long session; the
/// actor's first real status event will replace this presentation state.
fn resume_display_state(
    mut state: SessionState,
    access: LocalSessionAccess,
    resuming: bool,
) -> SessionState {
    if resuming
        && access == LocalSessionAccess::Owned
        && state.status == Some(SessionStatus::Stopped)
    {
        state.status = Some(SessionStatus::Ready);
        state.status_detail = None;
    }
    state
}

fn owner_shutdown_should_handoff_to_viewer(
    access: LocalSessionAccess,
    status: SessionStatus,
    prompt_submission_pending: bool,
    control_server: Option<&LocalSessionControlServer>,
) -> bool {
    access == LocalSessionAccess::Owned
        && (prompt_submission_pending
            || matches!(
                status,
                SessionStatus::Starting
                    | SessionStatus::Running
                    | SessionStatus::WaitingForApproval
            ))
        && control_server.is_some_and(LocalSessionControlServer::has_attached_viewers)
}

fn local_prompt_submission_pending(
    pending_prompt_ids: &HashSet<Uuid>,
    local_prompt_admissions: &Mutex<HashSet<Uuid>>,
) -> bool {
    !pending_prompt_ids.is_empty()
        || !local_prompt_admissions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty()
}

async fn stop_stale_local_owner_and_acquire(
    journal_path: &Path,
    control_socket_path: &Path,
    session_id: Uuid,
) -> Result<SessionWriterLease> {
    send_local_session_command(
        control_socket_path,
        session_id,
        HostCommand::Stop { session_id },
    )
    .await
    .context("failed to ask the obsolete local session owner to stop")?;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if let Some(writer) = SessionWriterLease::try_acquire(journal_path)? {
            return Ok(writer);
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "obsolete local session owner did not release its writer lease"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

async fn run_local_agent_session(
    args: &LocalAgentCliArgs,
    selected_session: Option<Uuid>,
    restored_prompt: Option<(String, Vec<PathBuf>)>,
    session_root_override: Option<&Path>,
    crash_context: Arc<TuiCrashContext>,
) -> Result<Option<(Uuid, Option<(String, Vec<PathBuf>)>)>> {
    let mut agent_config = AgentConfig::load(args.config.as_deref())?;
    let agent_config_path = AgentConfig::path(args.config.as_deref());
    let mut agent_config_signature = agent_config_file_signature(agent_config_path.as_deref());
    let mut editor_preferences = EditorPreferences::load()?;
    let host_config_path = default_host_config_path();
    let sessions_dir = session_root_override.map_or_else(
        || {
            host_config_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("sessions")
        },
        Path::to_path_buf,
    );
    let sqlite_store =
        Arc::new(SqliteSessionStore::open(sessions_dir.join("sessions.sqlite3")).await?);
    let session_id = if let Some(session_id) = selected_session.or(args.resume) {
        session_id_if_present(&sessions_dir, sqlite_store.as_ref(), session_id).await?
    } else if args.continue_latest {
        latest_session_id(&sessions_dir, sqlite_store.as_ref())
            .await?
            .context("there are no local Borg sessions to continue")?
    } else {
        Uuid::new_v4()
    };
    crash_context.set_session_id(session_id);
    let journal_path = sessions_dir.join(format!("{session_id}.jsonl"));
    let control_socket_path = session_control_socket_path(&sessions_dir, session_id);
    let remote_launch_path = sessions_dir.join(format!("{session_id}.launch.json"));
    let mut writer = SessionWriterLease::try_acquire(&journal_path)?;
    let mut session_access = if writer.is_some() {
        LocalSessionAccess::Owned
    } else {
        LocalSessionAccess::Attached
    };
    let store: Arc<dyn SessionStore> = if sqlite_store.contains_session(session_id).await? {
        sqlite_store.clone()
    } else if journal_path.is_file() && !session_access.is_attached() {
        sqlite_store.import_jsonl(&journal_path).await?;
        sqlite_store.clone()
    } else if journal_path.is_file() {
        anyhow::bail!(
            "legacy JSONL session {session_id} is active in another Borg process; \
             stop that process and resume it once to migrate it to SQLite"
        )
    } else {
        if let Some(workspace_id) = args.workspace {
            sqlite_store
                .create_session_in_workspace(session_id, workspace_id)
                .await?;
        } else {
            sqlite_store.create_session(session_id).await?;
        }
        sqlite_store.clone()
    };
    let mut session_state = store.state(session_id).await?;
    let mut stale_local_owner = session_access.is_attached()
        && !remote_launch_path.is_file()
        && !local_session_owner_uses_current_binary(&sessions_dir, session_id)?;
    if stale_local_owner && stale_local_owner_can_handoff(session_state.status) {
        // Prompt admission intentionally precedes the durable Starting event.
        // Give an obsolete owner one short scheduling window to expose that
        // transition before deciding its apparently Ready state is safe to
        // hand off, so an update cannot eat a just-submitted prompt.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        session_state = store.state(session_id).await?;
        if stale_local_owner_can_handoff(session_state.status) {
            tracing::info!(%session_id, "replacing obsolete local session owner before attach");
            writer = Some(
                stop_stale_local_owner_and_acquire(&journal_path, &control_socket_path, session_id)
                    .await?,
            );
            session_access = LocalSessionAccess::Owned;
            session_state = store.state(session_id).await?;
            stale_local_owner = false;
        }
    }
    let resuming = session_state.latest_sequence > 0;
    let mut current_goal = session_state.goal.clone();
    let mut current_todos = session_state.todos.clone();
    let mut session_usage = SessionUsage::from_projection(&session_state.usage);
    let recorded_config = session_state.configuration.as_ref().map(|configuration| {
        (
            configuration.cwd.clone(),
            configuration.provider,
            configuration.model.clone(),
            configuration.effort.clone(),
            Some(configuration.fast),
            configuration.response_language,
            configuration.permission_mode,
        )
    });
    let requested_cwd = match args.cwd.as_deref() {
        Some(path) => Some(
            path.canonicalize()
                .with_context(|| format!("project directory does not exist: {}", path.display()))?,
        ),
        None if resuming => None,
        None => Some(
            Path::new(".")
                .canonicalize()
                .context("current project directory does not exist")?,
        ),
    };
    let requested_provider = args.provider.into();
    let requested_model = args.model.clone().or_else(|| match requested_provider {
        CodingProvider::Codex => Some(borg_provider::codex_product_model().to_string()),
        CodingProvider::Kimi => Some(borg_provider::kimi_product_model().to_string()),
        CodingProvider::OpenRouter => Some(borg_provider::openrouter_product_model().to_string()),
        CodingProvider::OpenAiCompatible => std::env::var("BORG_OPENAI_COMPATIBLE_MODEL")
            .ok()
            .filter(|model| !model.trim().is_empty()),
        CodingProvider::Claude | CodingProvider::OpenCode => None,
    });
    let requested_effort = args.effort.clone().or_else(|| match requested_provider {
        CodingProvider::Codex => Some(borg_provider::codex_default_effort().to_string()),
        CodingProvider::Kimi => Some(borg_provider::kimi_default_effort().to_string()),
        // OpenRouter spans reasoning and non-reasoning models. Only send its
        // optional reasoning parameter after an explicit user selection.
        CodingProvider::OpenRouter => None,
        CodingProvider::OpenAiCompatible => None,
        CodingProvider::Claude | CodingProvider::OpenCode => None,
    });
    let (recorded_cwd, mut provider, model, mut effort, fast, response_language, permission_mode) =
        if let Some(recorded_config) = recorded_config {
            recorded_config
        } else {
            (
                requested_cwd
                    .clone()
                    .context("new sessions require a project directory")?,
                requested_provider,
                requested_model,
                requested_effort,
                Some(args.fast),
                ResponseLanguage::Auto,
                args.permission.into(),
            )
        };
    anyhow::ensure!(
        !fast.unwrap_or(false) || provider.supports_fast(),
        "--fast is not supported by the {provider:?} transport"
    );
    anyhow::ensure!(
        !provider.uses_native_harness() || model.is_some(),
        "{provider:?} requires --model or BORG_OPENAI_COMPATIBLE_MODEL"
    );
    let cwd = requested_cwd.unwrap_or(recorded_cwd);
    let capabilities = borg_remote::SessionCapabilities::from(&agent_config.capabilities);
    let team_policy = agent_config.autonomous_team_policy(&capabilities, provider, session_id);
    if !resuming
        && args.effort.is_none()
        && let Some(policy) = &team_policy
        && let Some(director) = policy.topology.members.iter().find(|member| {
            member.participant_id == session_id && member.role == borg_remote::TeamRole::Director
        })
    {
        effort = director.profile.reasoning_effort.clone();
    }
    let mut current_model = model.clone();
    let mut current_effort = effort.clone();
    let mut current_fast = fast.unwrap_or(false);
    let mut current_response_language = response_language;
    anyhow::ensure!(
        cwd.is_dir(),
        "recorded project directory no longer exists: {}; pass --cwd to resume the session in its new location",
        cwd.display()
    );
    let local_settings = LocalAgentSettings {
        approval_reviewer_model: agent_config.approvals.reviewer_model.clone(),
        approval_reviewer_effort: agent_config.approvals.reviewer_effort.clone(),
    };
    let (mut extension_catalog, extension_servers) = crate::extensions::discover(
        &cwd,
        &agent_config.capabilities,
        agent_config.extensions.allow_project_mcp,
    )?;
    let extension_skill_roots = extension_catalog.active_skill_roots();
    let mut observed_extension_revision = extension_catalog.revision.clone();
    let mut last_blu_discovery_error: Option<String> = None;
    let local_executor = if provider == CodingProvider::Kimi && host_config_path.is_file() {
        let config: HostConfig = serde_json::from_slice(
            &fs::read(&host_config_path)
                .with_context(|| format!("failed to read {}", host_config_path.display()))?,
        )
        .with_context(|| format!("invalid host config {}", host_config_path.display()))?;
        LocalAgentTurnExecutor::with_model_gateway_and_settings(
            borg_provider::provider::ModelGateway {
                endpoint: format!(
                    "{}/api/remote/host/kimi/chat/completions",
                    config.server.trim_end_matches('/')
                ),
                bearer_token: config.host_token,
            },
            local_settings,
        )
    } else {
        LocalAgentTurnExecutor::with_settings(local_settings)
    }
    .with_external_mcp_servers({
        let mut servers = agent_config.external_mcp_servers();
        servers.extend(extension_servers);
        servers
    })
    .with_extension_skill_roots(extension_skill_roots);
    local_executor.prewarm(provider);
    let live_extension_executor = local_executor.clone();
    let executor: Arc<dyn AgentTurnExecutor> = Arc::new(local_executor);
    let mut rendered = HashMap::new();
    let stdin_is_terminal = io::stdin().is_terminal();
    let can_prompt = stdin_is_terminal && !args.json;
    let rich_tui_allowed =
        rich_terminal_can_prompt(stdin_is_terminal, io::stdout().is_terminal(), args.json)
            && !BorgTerminal::fallback_requested();
    let fallback_terminal = can_prompt && !rich_tui_allowed;
    let mut initial_prompt = if !args.prompt.is_empty() {
        Some(args.prompt.join(" "))
    } else if !stdin_is_terminal {
        let mut piped = String::new();
        tokio::io::stdin().read_to_string(&mut piped).await?;
        (!piped.trim().is_empty()).then(|| piped.trim().to_string())
    } else {
        None
    };
    let initial_peers = if let Some(peer_arg) = args.peer_provider {
        let topic = initial_prompt
            .as_deref()
            .context("--peer-provider requires an initial prompt")?
            .to_string();
        let peer_provider: CodingProvider = peer_arg.into();
        let task_name = format!("peer_{}", provider_name(peer_provider).replace('-', "_"));
        initial_prompt = Some(format!(
            "{topic}\n\nYou are the lead participant in a mixed-provider Borg thread. \
             Your peer `{task_name}` is already working on the same problem. Use the \
             team messaging tools to exchange arguments and evidence, coordinate any \
             workspace edits, and synthesize a final answer that incorporates both \
             participants' work."
        ));
        vec![SpawnSubagent {
            task_name: task_name.clone(),
            message: format!(
                "{topic}\n\nYou are `{task_name}`, a peer participant in a mixed-provider \
                 Borg thread. Investigate independently, then use send_message or \
                 followup_task to discuss findings with `/root`. Coordinate before \
                 editing shared files and keep working until the lead can synthesize \
                 the joint result."
            ),
            provider: Some(peer_provider),
            model: args.peer_model.clone(),
            effort: args.peer_effort.clone(),
        }]
    } else {
        Vec::new()
    };
    let has_initial_prompt = initial_prompt.is_some();
    let interactive = can_prompt;
    let mut history = if can_prompt && !fallback_terminal {
        recent_tui_history(store.as_ref(), session_id, session_state.latest_sequence).await?
    } else {
        store.read(session_id).await?
    };
    let mut history_start_reached = session_state.latest_sequence == 0
        || history.first().is_some_and(|event| event.sequence <= 1);
    let (team_history, mut team_snapshots) = if can_prompt && !fallback_terminal {
        subagent_state_from_history(&history)
    } else {
        (Vec::new(), Vec::new())
    };
    reconcile_subagent_snapshots(store.as_ref(), &sessions_dir, &mut team_snapshots).await;
    let request_id = initial_prompt
        .as_ref()
        .map_or(session_id, |_| Uuid::new_v4());
    let launch = LaunchSession {
        request_id,
        cwd: cwd.clone(),
        provider,
        model: model.clone(),
        effort: effort.clone(),
        fast,
        response_language,
        permission_mode,
        name: cwd
            .file_name()
            .and_then(|value| value.to_str())
            .map(str::to_string),
        initial_prompt,
        capabilities,
        subagent_concurrency_limit: Some(agent_config.subagent_concurrency_limit()),
        // The local executor owns the live snapshot. Keeping launch roots
        // empty avoids pinning or duplicating the startup catalog forever.
        extension_skill_roots: Vec::new(),
        team_policy,
    };
    if session_access.is_attached() {
        if remote_launch_path.is_file() {
            anyhow::bail!(
                "this session is still owned by the background Borg remote host; reopen it from the connected remote chat instead of starting a second local writer"
            );
        }
        #[cfg(unix)]
        tracing::info!(%session_id, "attaching to active local session owner");
        #[cfg(not(unix))]
        anyhow::bail!("cannot resume this session while its writer is active in another process");
    } else {
        tracing::info!(%session_id, "acquired local session ownership");
    }
    let (remote_command_tx, mut remote_commands) = mpsc::channel(64);
    let mut remote_open = !session_access.is_attached()
        && interactive
        && !args.local_only
        && !args.ephemeral
        && host_config_path.is_file();
    let mut mirror_shutdown = None;
    let mut mirror_task = None;
    let mut collab_child: Option<Child> = None;
    if remote_open {
        let mut registration = launch.clone();
        // Attached-session identity is stable across CLI resume and does not
        // inherit the current turn's message/idempotency key.
        registration.request_id = session_id;
        registration.initial_prompt = None;
        let config_path = host_config_path.clone();
        let mirror_store = Arc::clone(&store);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        mirror_shutdown = Some(shutdown_tx);
        let command_tx = remote_command_tx.clone();
        mirror_task = Some(tokio::spawn(async move {
            if let Err(error) = mirror_local_session(
                &config_path,
                mirror_store,
                session_id,
                registration,
                command_tx,
                shutdown_rx,
            )
            .await
            {
                tracing::warn!(%error, "remote mirror stopped");
            }
        }));
    }
    if !args.json && fallback_terminal {
        println!(
            "\n  BORG  {} · {} · {}{}\n  session {}\n",
            provider_name(provider),
            cwd.display(),
            permission_name(permission_mode),
            if fallback_terminal {
                " · fallback terminal"
            } else {
                ""
            },
            session_id,
        );
        if remote_open {
            println!("  Remote mirror enabled (journal-first and offline-safe).");
        }
        if resuming {
            if !can_prompt || fallback_terminal {
                print_history(&history);
            }
            println!(
                "\n  {}. Type /help for controls.\n",
                if session_access.is_attached() {
                    "Attached to active session"
                } else {
                    "Resumed"
                }
            );
        } else if interactive && !has_initial_prompt {
            println!("  Type a request. /help shows controls.\n");
        } else if interactive {
            println!("  Running the initial turn. Type to steer or queue.\n");
        } else {
            println!("  Running one turn.\n");
        }
    }

    let (session_command_tx, session_commands) = mpsc::channel(64);
    // The owned actor is the single ordered live source. A generous bounded
    // queue absorbs bursty child/tool traffic while preserving backpressure;
    // do not merge the same actor stream back through SQLite here, because a
    // durable boundary can delete its preceding live row before that second
    // reader observes it and permanently desynchronise the owner UI.
    let (session_event_tx, mut session_events) = mpsc::channel(4_096);
    let local_prompt_admissions = Arc::new(Mutex::new(HashSet::new()));
    let actor_journal_path = journal_path.clone();
    let registration_template = launch.clone();
    let control_server = if session_access.is_attached() {
        None
    } else {
        Some(LocalSessionControlServer::start_with_prompt_admissions(
            control_socket_path.clone(),
            session_id,
            writer.as_ref().expect("session owner holds writer lease"),
            session_command_tx.clone(),
            Some(Arc::clone(&local_prompt_admissions)),
        )?)
    };
    let actor = if session_access.is_attached() {
        tokio::spawn(run_attached_session(
            Arc::clone(&store),
            session_id,
            actor_journal_path,
            control_socket_path.clone(),
            session_state.latest_sequence,
            session_commands,
            session_event_tx,
        ))
    } else {
        let writer = writer.expect("session owner holds writer lease");
        let actor_store = Arc::clone(&sqlite_store);
        let actor_session_root = sessions_dir.clone();
        tokio::spawn(async move {
            if initial_peers.is_empty() {
                run_agent_session_with_store_and_writer(
                    &actor_session_root,
                    session_id,
                    launch,
                    session_commands,
                    session_event_tx,
                    executor,
                    actor_store,
                    writer,
                )
                .await
            } else {
                run_agent_session_with_store_writer_and_peers(
                    &actor_session_root,
                    session_id,
                    launch,
                    session_commands,
                    session_event_tx,
                    executor,
                    actor_store,
                    writer,
                    initial_peers,
                )
                .await
            }
        })
    };
    let mut shutdown_signals =
        ShutdownSignals::new().context("failed to install process shutdown handlers")?;
    let mut terminal = if rich_tui_allowed {
        match BorgTerminal::enter(
            &sessions_dir,
            session_id,
            cwd.clone(),
            &agent_config.keybindings,
        ) {
            Ok(terminal) => Some(terminal),
            Err(error) => {
                eprintln!("  Rich terminal unavailable ({error}); using line input.");
                if resuming {
                    print_history(&history);
                }
                None
            }
        }
    } else {
        None
    };
    crash_context
        .tui_active
        .store(terminal.is_some(), Ordering::Release);
    let display_session_state =
        resume_display_state(session_state.clone(), session_access, resuming);
    if let Some(terminal) = terminal.as_mut() {
        terminal.seed_history(&history);
        terminal.seed_team_roster(&team_snapshots);
        terminal.seed_session_state(&display_session_state);
        if stale_local_owner {
            terminal.set_notice(
                "This turn is owned by an older Borg build · upgrading automatically when it finishes"
                    .to_string(),
            );
        } else if extension_catalog.has_errors() {
            terminal.set_notice(format!(
                "Blu isolated {} invalid extension{} · run `borg extensions doctor`",
                extension_catalog.error_count(),
                if extension_catalog.error_count() == 1 {
                    ""
                } else {
                    "s"
                },
            ));
        }
        terminal.set_auto_expand_edits(editor_preferences.presentation.auto_expand_edits);
        terminal.set_auto_expand_tools(editor_preferences.presentation.auto_expand_tools);
        terminal.set_transcript_labels(
            editor_preferences.transcript.user_label.clone(),
            editor_preferences.transcript.assistant_label.clone(),
        );
        terminal.set_transcript_colors(&editor_preferences.transcript);
        if let Some((text, attachments)) = restored_prompt {
            terminal.restore_composer(text, attachments);
        }
        terminal.draw()?;
    }
    // Prompt recall is not part of first paint. The bounded transcript tail
    // already seeds the composer with the newest prompts; older prompts are
    // only needed when the user presses Up. Loading them here used to scan the
    // entire session event table before the first frame, which made long chats
    // feel like they were eagerly replaying their whole history.
    let mut composer_history_task = (resuming && terminal.is_some()).then(|| {
        let composer_store = Arc::clone(&store);
        tokio::spawn(async move {
            composer_store
                .recent_user_messages(session_id, RICH_TUI_PROMPT_HISTORY_LIMIT)
                .await
        })
    });
    // Team recovery can involve the entire root projection plus one child-tail
    // query per roster entry. Start it only after the latest root tail is on
    // screen so it can never delay the first frame.
    let mut team_state_task = (resuming && terminal.is_some()).then(|| {
        let team_store = Arc::clone(&store);
        let team_sessions_dir = sessions_dir.clone();
        tokio::spawn(async move {
            load_subagent_thread_state(team_store.as_ref(), &team_sessions_dir, session_id).await
        })
    });
    if team_state_task.is_none()
        && let Some(terminal) = terminal.as_mut()
    {
        terminal.finish_child_history_hydration();
    }
    // Warm one bounded older page immediately after first paint. Upward
    // navigation then reveals already-loaded rows instead of waiting behind
    // unrelated session/team hydration work.
    let mut history_page_task: Option<tokio::task::JoinHandle<Result<Vec<SessionEvent>>>> =
        (resuming && terminal.is_some() && !history_start_reached).then(|| {
            let before = history_page_before(&history, session_state.latest_sequence);
            let history_store = Arc::clone(&store);
            tokio::spawn(async move {
                older_tui_history(history_store.as_ref(), session_id, before).await
            })
        });
    let mut input = (can_prompt && terminal.is_none()).then(spawn_terminal_input);
    let mut input_open = can_prompt && terminal.is_none();
    let mut delivered_projection = DeliveredSessionProjection::new(display_session_state.clone());
    let mut status = display_session_state
        .status
        .unwrap_or(SessionStatus::Starting);
    let mut status_detail = display_session_state.status_detail.clone();
    let mut pending_approval = display_session_state.pending_approval_id.clone();
    let mut pending_provider_interaction = display_session_state
        .pending_provider_interaction_id
        .clone()
        .zip(
            display_session_state
                .pending_provider_interaction_kind
                .clone(),
        )
        .zip(
            display_session_state
                .pending_provider_interaction_payload
                .clone(),
        )
        .map(|((interaction_id, kind), payload)| (interaction_id, kind, payload));
    let mut child_pending_approvals = child_pending_approval_ids(&team_history);
    let mut saw_running = false;
    // A prompt can be accepted by the local command channel while the actor
    // still reports Ready. Preserve that handoff on terminal hangup instead
    // of mistaking the short admission window for an idle session.
    let mut pending_prompt_ids = HashSet::new();
    let mut stop_sent = false;
    let mut user_requested_exit = false;
    let mut exit_notice = None;
    let mut detached_from_terminal = false;
    let mut handoff_on_safe_boundary = false;
    let mut last_ctrl_c = None;
    let mut terminal_dirty = false;
    let mut tui_fps = tui_refresh_rate(u64::from(editor_preferences.presentation.refresh_rate_fps));
    let mut prevent_sleep = editor_preferences.interaction.prevent_sleep;
    let mut steer_active_codex =
        editor_preferences.interaction.active_messages == ActiveMessageBehavior::Steer;
    let mut resume_session = None;
    let mut rewind_prompt = None;
    let mut sleep_inhibitor = SleepInhibitor::new(prevent_sleep);
    let mut render_frame_interval = tui_frame_interval(tui_fps);
    let mut render_tick = tui_render_interval(render_frame_interval);
    let mut activity_tick = tokio::time::interval(ACTIVITY_FRAME_INTERVAL);
    activity_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut cache_tick = tokio::time::interval(std::time::Duration::from_secs(30));
    cache_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut agent_config_tick = tokio::time::interval(std::time::Duration::from_millis(500));
    agent_config_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut blu_tick = tokio::time::interval(std::time::Duration::from_millis(500));
    blu_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut blu_discovery_task: Option<BluDiscoveryTask> = None;
    let mut shutdown_signal_open = true;
    loop {
        tokio::select! {
            composer_history_result = async {
                composer_history_task
                    .as_mut()
                    .expect("composer-history task branch is guarded")
                    .await
            }, if composer_history_task.is_some() => {
                composer_history_task = None;
                match composer_history_result {
                    Ok(Ok(composer_history)) => {
                        if let Some(terminal) = terminal.as_mut() {
                            terminal.seed_composer_history(&composer_history);
                        }
                    }
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "could not hydrate composer history after first paint");
                    }
                    Err(error) => {
                        tracing::warn!(%error, "composer history hydration task failed");
                    }
                }
            }
            team_state_result = async {
                team_state_task
                    .as_mut()
                    .expect("team-state task branch is guarded")
                    .await
            }, if team_state_task.is_some() => {
                team_state_task = None;
                match team_state_result {
                    Ok(Ok((team_history, team_snapshots, child_histories))) => {
                        child_pending_approvals = child_pending_approval_ids(&team_history);
                        if let Some(terminal) = terminal.as_mut() {
                            seed_terminal_subagent_threads(
                                terminal,
                                &team_snapshots,
                                &child_histories,
                            );
                            terminal_dirty = true;
                        }
                    }
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "could not hydrate subagent history after first paint");
                        if let Some(terminal) = terminal.as_mut() {
                            terminal.finish_child_history_hydration();
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, "subagent history hydration task failed");
                        if let Some(terminal) = terminal.as_mut() {
                            terminal.finish_child_history_hydration();
                        }
                    }
                }
            }
            history_page_result = async {
                history_page_task
                    .as_mut()
                    .expect("history-page task branch is guarded")
                    .await
            }, if history_page_task.is_some() => {
                history_page_task = None;
                if let Some(terminal) = terminal.as_mut() {
                    terminal.set_history_page_loading(false);
                    terminal_dirty = true;
                }
                match history_page_result {
                    Ok(Ok(older)) if older.is_empty() => {
                        history_start_reached = true;
                    }
                    Ok(Ok(older)) => {
                        history.splice(0..0, older);
                        history_start_reached =
                            history.first().is_none_or(|event| event.sequence <= 1);
                        if let Some(terminal) = terminal.as_mut() {
                            terminal.replace_history(&history);
                            terminal.seed_session_state(delivered_projection.state());
                            terminal_dirty = true;
                        }
                    }
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "could not load an older transcript page");
                    }
                    Err(error) => {
                        tracing::warn!(%error, "older transcript page task failed");
                    }
                }
            }
            _ = render_tick.tick(), if terminal.is_some() && terminal_dirty => {
                let terminal = terminal.as_mut().expect("terminal");
                let interaction_frame = terminal.has_pending_scroll_frame();
                terminal.advance_scroll_frame();
                let history_requested = terminal.take_history_page_request();
                if history_requested && !history_start_reached {
                    terminal.set_history_page_loading(true);
                    if history_page_task.is_none() {
                        let before = history_page_before(
                            &history,
                            delivered_projection.state().latest_sequence,
                        );
                        let history_store = Arc::clone(&store);
                        history_page_task = Some(tokio::spawn(async move {
                            older_tui_history(history_store.as_ref(), session_id, before).await
                        }));
                    }
                }
                let draw_started = std::time::Instant::now();
                terminal.draw()?;
                let next_interval =
                    responsive_tui_frame_interval(tui_fps, draw_started.elapsed(), interaction_frame);
                if next_interval != render_frame_interval {
                    render_frame_interval = next_interval;
                    render_tick = tui_render_interval(render_frame_interval);
                }
                terminal_dirty = terminal.has_pending_scroll_frame();
            }
            _ = activity_tick.tick(), if terminal.as_ref().is_some_and(|terminal| {
                terminal_needs_activity_tick(
                    terminal.has_expiring_notice(),
                    terminal.has_blinking_cursor(),
                    status,
                ) || terminal.is_launch_screen()
                    || terminal.is_history_page_loading()
            }) => {
                terminal_dirty = true;
            }
            _ = cache_tick.tick(), if terminal.as_ref().is_some_and(
                crate::terminal_ui::BorgTerminal::has_cache_idle_timer
            ) => {
                terminal_dirty = true;
            }
            _ = agent_config_tick.tick(), if interactive => {
                let signature = agent_config_file_signature(agent_config_path.as_deref());
                if signature != agent_config_signature {
                    match AgentConfig::load(args.config.as_deref()) {
                        Ok(next) => {
                            let keybindings_changed = next.keybindings != agent_config.keybindings;
                            agent_config = next;
                            // Force the Blu snapshot to include updated base
                            // MCP servers, capability gates, and trust policy.
                            observed_extension_revision.clear();
                            if let Some(task) = blu_discovery_task.take() {
                                task.abort();
                            }
                            agent_config_signature = signature;
                            if let Some(terminal) = terminal.as_mut() {
                                if keybindings_changed {
                                    if let Err(error) = terminal.reload_keybindings(&agent_config.keybindings) {
                                        terminal.set_notice(format!("Settings changed, but keybindings were invalid: {error:#}"));
                                    } else {
                                        terminal.set_notice("Agent settings reloaded · aliases/keybindings are live · Blu/MCP apply next turn".to_string());
                                    }
                                } else {
                                    terminal.set_notice("Agent settings changed · Blu/MCP apply next turn · provider/session policy applies next session".to_string());
                                }
                                terminal_dirty = true;
                            }
                        }
                        Err(error) => {
                            agent_config_signature = signature;
                            if let Some(terminal) = terminal.as_mut() {
                                terminal.set_notice(format!("Agent settings not reloaded: {error:#}"));
                                terminal_dirty = true;
                            }
                        }
                    }
                }
            }
            _ = blu_tick.tick(), if interactive && blu_discovery_task.is_none() => {
                let discovery_cwd = cwd.clone();
                let discovery_capabilities = agent_config.capabilities.clone();
                let allow_project_mcp = agent_config.extensions.allow_project_mcp;
                blu_discovery_task = Some(tokio::task::spawn_blocking(move || {
                    crate::extensions::discover(
                        &discovery_cwd,
                        &discovery_capabilities,
                        allow_project_mcp,
                    )
                }));
            }
            blu_result = async {
                blu_discovery_task
                    .as_mut()
                    .expect("Blu discovery branch is guarded")
                    .await
            }, if blu_discovery_task.is_some() => {
                blu_discovery_task = None;
                match blu_result {
                    Ok(Ok((next_catalog, next_extension_servers)))
                        if next_catalog.revision != observed_extension_revision =>
                    {
                        observed_extension_revision = next_catalog.revision.clone();
                        if !next_catalog.has_errors() {
                            last_blu_discovery_error = None;
                            let mut servers = agent_config.external_mcp_servers();
                            servers.extend(next_extension_servers);
                            live_extension_executor.replace_runtime_extensions(
                                servers,
                                next_catalog.active_skill_roots(),
                            );
                            extension_catalog = next_catalog;
                            if let Some(terminal) = terminal.as_mut() {
                                terminal.set_notice(
                                    "Blu reloaded · extensions apply at the next turn boundary"
                                        .to_string(),
                                );
                                terminal_dirty = true;
                            }
                        } else {
                            let count = next_catalog.error_count();
                            let first = next_catalog
                                .diagnostics
                                .iter()
                                .find(|diagnostic| {
                                    diagnostic.level
                                        == crate::extensions::ExtensionDiagnosticLevel::Error
                                })
                                .map(|diagnostic| diagnostic.message.as_str());
                            let message = match first {
                                Some(first) => format!(
                                    "{count} diagnostic{} rejected the reload; first: {first}",
                                    if count == 1 { "" } else { "s" },
                                ),
                                None => format!(
                                    "{count} diagnostic{} rejected the reload",
                                    if count == 1 { "" } else { "s" },
                                ),
                            };
                            last_blu_discovery_error = Some(message);
                            if let Some(terminal) = terminal.as_mut() {
                                terminal.set_notice(format!(
                                    "Blu kept the last-known-good catalog · {count} diagnostic{} · run `borg extensions doctor`",
                                    if count == 1 { "" } else { "s" },
                                ));
                                terminal_dirty = true;
                            }
                        }
                    }
                    Ok(Ok((next_catalog, _))) => {
                        if !next_catalog.has_errors() {
                            last_blu_discovery_error = None;
                        }
                    }
                    Ok(Err(error)) => {
                        let message = format!("{error:#}");
                        if last_blu_discovery_error.as_deref() != Some(message.as_str())
                            && let Some(terminal) = terminal.as_mut()
                        {
                            terminal.set_notice(format!(
                                "Blu kept the last-known-good catalog · {message}"
                            ));
                            terminal_dirty = true;
                        }
                        last_blu_discovery_error = Some(message);
                    }
                    Err(error) => {
                        let message = format!("Blu discovery task failed: {error}");
                        if last_blu_discovery_error.as_deref() != Some(message.as_str())
                            && let Some(terminal) = terminal.as_mut()
                        {
                            terminal.set_notice(format!(
                                "Blu kept the last-known-good catalog · {message}"
                            ));
                            terminal_dirty = true;
                        }
                        last_blu_discovery_error = Some(message);
                    }
                }
            }
            event = session_events.recv() => {
                let Some(event) = event else {
                    break;
                };
                let handoff_stale_owner = stale_local_owner
                    && matches!(
                        event.kind,
                        SessionEventKind::StatusChanged {
                            status: SessionStatus::Ready,
                            ..
                        }
                    );
                delivered_projection.observe(&event)?;
                if let Some(message_id) = committed_prompt_id(&event.kind)
                    && matches!(
                        status,
                        SessionStatus::Starting
                            | SessionStatus::Running
                            | SessionStatus::WaitingForApproval
                    )
                {
                    pending_prompt_ids.remove(&message_id);
                    local_prompt_admissions
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .remove(&message_id);
                } else if let SessionEventKind::PromptRecalled { message_id, .. } = &event.kind {
                    pending_prompt_ids.remove(message_id);
                    local_prompt_admissions
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .remove(message_id);
                } else if matches!(event.kind, SessionEventKind::Error { .. }) {
                    pending_prompt_ids.clear();
                    local_prompt_admissions
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .clear();
                }
                if let SessionEventKind::StatusChanged { status: next, detail } = &event.kind {
                    status = *next;
                    status_detail = detail.clone();
                    saw_running |= *next == SessionStatus::Running;
                    if matches!(
                        next,
                        SessionStatus::Starting
                            | SessionStatus::Running
                            | SessionStatus::WaitingForApproval
                    ) {
                        // The durable status now protects terminal hangup. Any
                        // prompt-admission guard that bridged the earlier Ready
                        // window has served its purpose.
                        pending_prompt_ids.clear();
                        local_prompt_admissions
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .clear();
                    }
                    sleep_inhibitor.set_turn_active(matches!(
                        next,
                        SessionStatus::Starting
                            | SessionStatus::Running
                            | SessionStatus::WaitingForApproval
                    ));
                }
                match &event.kind {
                    SessionEventKind::ApprovalRequested { approval_id, .. } => {
                        pending_approval = Some(approval_id.clone());
                    }
                    SessionEventKind::ApprovalResolved { approval_id, .. }
                        if pending_approval.as_deref() == Some(approval_id.as_str()) =>
                    {
                        pending_approval = None;
                    }
                    SessionEventKind::ProviderInteractionRequested {
                        interaction_id,
                        kind,
                        payload,
                        ..
                    } => {
                        pending_provider_interaction =
                            Some((interaction_id.clone(), kind.clone(), payload.clone()));
                    }
                    SessionEventKind::ProviderInteractionResolved { interaction_id, .. }
                        if pending_provider_interaction
                            .as_ref()
                            .is_some_and(|(pending_id, _, _)| pending_id == interaction_id) =>
                    {
                        pending_provider_interaction = None;
                    }
                    SessionEventKind::GoalUpdated { goal } => {
                        current_goal = Some(goal.clone());
                    }
                    SessionEventKind::GoalCleared { .. } => {
                        current_goal = None;
                    }
                    SessionEventKind::PlanUpdated { items } => {
                        current_todos = items.clone();
                    }
                    SessionEventKind::SessionConfigured {
                        provider: configured_provider,
                        model,
                        effort,
                        fast,
                        response_language,
                        ..
                    } => {
                        provider = *configured_provider;
                        current_model = model.clone();
                        current_effort = effort.clone();
                        current_fast = *fast;
                        current_response_language = *response_language;
                    }
                    SessionEventKind::UsageUpdated {
                        input_tokens,
                        output_tokens,
                        cached_input_tokens,
                        cost_usd,
                        ..
                    } => session_usage.add(
                        *input_tokens,
                        *output_tokens,
                        *cached_input_tokens,
                        *cost_usd,
                    ),
                    SessionEventKind::SubagentActivity {
                        agent,
                        event: Some(child_event),
                        ..
                    } => match &child_event.kind {
                        SessionEventKind::ApprovalRequested { approval_id, .. } => {
                            child_pending_approvals
                                .insert(agent.session_id, approval_id.clone());
                        }
                        SessionEventKind::ApprovalResolved { approval_id, .. }
                            if child_pending_approvals
                                .get(&agent.session_id)
                                .is_some_and(|pending| pending == approval_id) =>
                        {
                            child_pending_approvals.remove(&agent.session_id);
                        }
                        _ => {}
                    },
                    _ => {}
                }
                if let Some(terminal) = terminal.as_mut() {
                    terminal_dirty |= terminal.apply_session_event(&event);
                    if history
                        .last()
                        .is_none_or(|loaded| loaded.sequence < event.sequence)
                    {
                        history.push(event.clone());
                    }
                } else if !detached_from_terminal {
                    render_event(&event, args.json, &mut rendered)?;
                }
                if handoff_stale_owner {
                    match send_local_session_command(
                        &control_socket_path,
                        session_id,
                        HostCommand::Stop { session_id },
                    )
                    .await
                    {
                        Ok(()) => {
                            stale_local_owner = false;
                            if let Some(terminal) = terminal.as_mut() {
                                terminal.set_notice(
                                    "Turn finished · restarting this session on the current Borg build"
                                        .to_string(),
                                );
                                terminal_dirty = true;
                            }
                        }
                        Err(error) => {
                            tracing::warn!(%error, %session_id, "could not replace obsolete local session owner");
                            if let Some(terminal) = terminal.as_mut() {
                                terminal.set_notice(format!(
                                    "Could not upgrade the older session owner: {error:#}"
                                ));
                                terminal_dirty = true;
                            }
                        }
                    }
                }
                if pending_approval.is_some() && !can_prompt {
                    let approval_id = pending_approval.take().expect("pending approval");
                    session_command_tx.send(HostCommand::Approve {
                        session_id,
                        approval_id,
                        decision: ApprovalDecision::Deny,
                    }).await.ok();
                } else if !detached_from_terminal
                    && pending_provider_interaction
                    .as_ref()
                    .is_some_and(|(_, _, payload)| {
                        terminal.is_none() && provider_interaction_payload_contains_secret(payload)
                    })
                {
                    let (interaction_id, kind, _) = pending_provider_interaction
                        .take()
                        .expect("pending secret provider interaction");
                    eprintln!(
                        "\n  Secret provider input requires Borg's rich terminal; request cancelled.\n"
                    );
                    session_command_tx
                        .send(HostCommand::RespondToProviderInteraction {
                            session_id,
                            interaction_id,
                            response: cancelled_provider_interaction_response(&kind),
                        })
                        .await
                        .ok();
                } else if pending_provider_interaction.is_some() && !can_prompt {
                    let (interaction_id, kind, _) = pending_provider_interaction
                        .take()
                        .expect("pending provider interaction");
                    session_command_tx
                        .send(HostCommand::RespondToProviderInteraction {
                            session_id,
                            interaction_id,
                            response: cancelled_provider_interaction_response(&kind),
                        })
                        .await
                        .ok();
                } else if !detached_from_terminal
                    && pending_approval.is_some()
                    && !args.json
                    && terminal.is_none()
                {
                    print!("\n  Allow · y   Deny · n › ");
                    io::stdout().flush()?;
                } else if !detached_from_terminal
                    && interactive
                    && status == SessionStatus::Ready
                    && !args.json
                    && terminal.is_none()
                {
                    print!("> ");
                    io::stdout().flush()?;
                }
                if !interactive
                    && status == SessionStatus::Ready
                    && (saw_running || !has_initial_prompt)
                    && !stop_sent
                {
                    stop_sent = true;
                    session_command_tx.send(HostCommand::Stop { session_id }).await.ok();
                }
                if handoff_on_safe_boundary
                    && status == SessionStatus::Ready
                    && !stop_sent
                {
                    // The owner has been asked to leave, but a viewer is
                    // attached. Let the in-flight turn finish before closing
                    // this actor so the viewer can acquire the writer lease
                    // without observing an interrupted turn.
                    stop_sent = true;
                    session_command_tx.send(HostCommand::Stop { session_id }).await.ok();
                }
            }
            line = recv_terminal_line(&mut input), if input_open => {
                let Some(line) = line else {
                    input_open = false;
                    if let Some(approval_id) = pending_approval.take() {
                        session_command_tx.send(HostCommand::Approve {
                            session_id,
                            approval_id,
                            decision: ApprovalDecision::Deny,
                        }).await.ok();
                    }
                    if let Some((interaction_id, kind, _)) =
                        pending_provider_interaction.take()
                    {
                        session_command_tx
                            .send(HostCommand::RespondToProviderInteraction {
                                session_id,
                                interaction_id,
                                response: cancelled_provider_interaction_response(&kind),
                            })
                            .await
                            .ok();
                    }
                    continue;
                };
                last_ctrl_c = None;
                let line = line?;
                let expanded = agent_config.expand_command(line.trim());
                let line = expanded.trim();
                if line.is_empty() {
                    continue;
                }
                if let Some(approval_id) = pending_approval.clone() {
                    let decision = match line {
                        "y" | "yes" => Some(ApprovalDecision::AllowOnce),
                        "n" | "no" | "deny" => Some(ApprovalDecision::Deny),
                        _ => None,
                    };
                    if let Some(decision) = decision {
                        session_command_tx.send(HostCommand::Approve {
                            session_id,
                            approval_id,
                            decision,
                        }).await.ok();
                    } else {
                        print!("  Choose y or n › ");
                        io::stdout().flush()?;
                    }
                    continue;
                }
                if let Some((interaction_id, kind, payload)) =
                    pending_provider_interaction.clone()
                {
                    match provider_interaction_response(&kind, &payload, line) {
                        Ok(response) => {
                            session_command_tx
                                .send(HostCommand::RespondToProviderInteraction {
                                    session_id,
                                    interaction_id,
                                    response,
                                })
                                .await
                                .ok();
                        }
                        Err(error) => {
                            eprintln!("\n  {error}\n");
                            print!("  Answer › ");
                            io::stdout().flush()?;
                        }
                    }
                    continue;
                }
                if line == "/model" {
                    println!(
                        "\n  Model: {}\n  Use /model NAME to change it.\n",
                        current_model.as_deref().unwrap_or("provider default")
                    );
                    continue;
                }
                if let Some(model) = line.strip_prefix("/model ") {
                    let model = model.trim().to_string();
                    let target = CodingProvider::for_model(&model).unwrap_or(provider);
                    send_model_selection(
                        &session_command_tx,
                        session_id,
                        provider,
                        target,
                        model,
                    )
                    .await;
                    continue;
                }
                if line == "/effort" {
                    println!(
                        "\n  Effort: {}\n  Available: {}\n",
                        current_effort.as_deref().unwrap_or("provider default"),
                        borg_provider::codex_effort_levels().join(", ")
                    );
                    continue;
                }
                if let Some(effort) = line.strip_prefix("/effort ") {
                    session_command_tx.send(HostCommand::Configure {
                        session_id,
                        action: SessionConfigAction::SetEffort {
                            effort: effort.trim().to_string(),
                        },
                    }).await.ok();
                    continue;
                }
                if line == "/language" {
                    println!(
                        "\n  Language: {} ({})\n  Available: {}\n",
                        current_response_language.name(),
                        current_response_language.code(),
                        ResponseLanguage::ALL
                            .map(|language| language.code())
                            .join(", ")
                    );
                    continue;
                }
                if let Some(value) = line.strip_prefix("/language ") {
                    if let Some(language) = ResponseLanguage::parse(value) {
                        session_command_tx
                            .send(HostCommand::Configure {
                                session_id,
                                action: SessionConfigAction::SetResponseLanguage { language },
                            })
                            .await
                            .ok();
                    } else {
                        println!("\n  Unknown language. Use /language to list choices.\n");
                    }
                    continue;
                }
                if line == "/fast" {
                    println!(
                        "\n  Fast mode: {}\n  Use /fast on or /fast off.\n",
                        if current_fast { "on" } else { "off" }
                    );
                    continue;
                }
                if let Some(value) = line.strip_prefix("/fast ") {
                    if let Some(enabled) = parse_on_off(value) {
                        session_command_tx.send(HostCommand::Configure {
                            session_id,
                            action: SessionConfigAction::SetFast { enabled },
                        }).await.ok();
                    } else {
                        println!("\n  Choose /fast on or /fast off.\n");
                    }
                    continue;
                }
                if line == "/usage" {
                    println!(
                        "\n  {}\n",
                        usage_summary(provider, &session_usage).await
                    );
                    continue;
                }
                if line == "/lsp" {
                    println!("\n{}\n", lsp_support_summary());
                    continue;
                }
                if matches!(
                    line,
                    "/settings" | "/followups" | "/refresh" | "/sleep"
                ) {
                    println!(
                        "\n  Settings\n  Model: {}\n  Effort: {}\n  Fast mode: {}\n  Active messages: {}\n  Refresh: {tui_fps} FPS\n  Prevent sleep: {}\n  User label: {}\n  Assistant label: {}\n  Use /model NAME, /effort LEVEL, /fast on|off, /followups steer|queue, /refresh FPS, /sleep on|off, /user-label TEXT, or /assistant-label TEXT.\n",
                        current_model.as_deref().unwrap_or("provider default"),
                        current_effort.as_deref().unwrap_or("provider default"),
                        if current_fast { "on" } else { "off" },
                        if steer_active_codex {
                            "steer current turn"
                        } else {
                            "queue next turn"
                        },
                        if prevent_sleep { "on" } else { "off" },
                        editor_preferences.transcript.user_label,
                        editor_preferences.transcript.assistant_label,
                    );
                    continue;
                }
                if line == "/colors" {
                    println!("\n{}\n", transcript_colors_summary(&editor_preferences));
                    continue;
                }
                if let Some(value) = line.strip_prefix("/color ") {
                    match set_transcript_color(&mut editor_preferences, value) {
                        Ok(()) => println!("\n{}\n", transcript_colors_summary(&editor_preferences)),
                        Err(error) => eprintln!("\n  {error}\n"),
                    }
                    continue;
                }
                if let Some(value) = line.strip_prefix("/followups ") {
                    match value.trim() {
                        "steer" => {
                            steer_active_codex = true;
                            editor_preferences.interaction.active_messages =
                                ActiveMessageBehavior::Steer;
                        }
                        "queue" => {
                            steer_active_codex = false;
                            editor_preferences.interaction.active_messages =
                                ActiveMessageBehavior::Queue;
                        }
                        _ => {
                            eprintln!("\n  Choose /followups steer or /followups queue.\n");
                            continue;
                        }
                    }
                    editor_preferences.save()?;
                    println!(
                        "\n  Messages sent while Codex works: {}.\n",
                        if steer_active_codex {
                            "steer current turn"
                        } else {
                            "queue next turn"
                        }
                    );
                    continue;
                }
                if let Some(fps) = line.strip_prefix("/refresh ") {
                    match fps.trim().parse::<u64>() {
                        Ok(fps @ MIN_TUI_FPS..=MAX_TUI_FPS) => {
                            tui_fps = fps;
                            render_frame_interval = tui_frame_interval(tui_fps);
                            render_tick = tui_render_interval(render_frame_interval);
                            editor_preferences.presentation.refresh_rate_fps =
                                u16::try_from(tui_fps).expect("bounded refresh rate fits u16");
                            editor_preferences.save()?;
                            println!("\n  Refresh rate set to {tui_fps} FPS.\n");
                        }
                        _ => eprintln!(
                            "\n  Refresh rate must be between {MIN_TUI_FPS} and {MAX_TUI_FPS} FPS.\n"
                        ),
                    }
                    continue;
                }
                if let Some(value) = line.strip_prefix("/sleep ") {
                    match value.trim() {
                        "on" => prevent_sleep = true,
                        "off" => prevent_sleep = false,
                        _ => {
                            eprintln!("\n  Choose /sleep on or /sleep off.\n");
                            continue;
                        }
                    }
                    sleep_inhibitor.set_enabled(prevent_sleep);
                    editor_preferences.interaction.prevent_sleep = prevent_sleep;
                    editor_preferences.save()?;
                    println!(
                        "\n  Prevent sleep during active turns: {}.\n",
                        if prevent_sleep { "on" } else { "off" }
                    );
                    continue;
                }
                if let Some(value) = line.strip_prefix("/user-label ") {
                    match set_transcript_label(&mut editor_preferences, true, value) {
                        Ok(()) => println!(
                            "\n  User transcript label: {}.\n",
                            editor_preferences.transcript.user_label
                        ),
                        Err(error) => eprintln!("\n  {error}\n"),
                    }
                    continue;
                }
                if let Some(value) = line.strip_prefix("/assistant-label ") {
                    match set_transcript_label(&mut editor_preferences, false, value) {
                        Ok(()) => println!(
                            "\n  Assistant transcript label: {}.\n",
                            editor_preferences.transcript.assistant_label
                        ),
                        Err(error) => eprintln!("\n  {error}\n"),
                    }
                    continue;
                }
                if line == "/goal" || line == "/goal view" {
                    print_goal(current_goal.as_ref());
                    continue;
                }
                if line.starts_with("/goal ") {
                    match parse_goal_action(line) {
                        Ok(action) => {
                            session_command_tx.send(HostCommand::Goal {
                                session_id,
                                action,
                            }).await.ok();
                        }
                        Err(error) => eprintln!("\n  {error}"),
                    }
                    continue;
                }
                if matches!(line, "/todo" | "/todos" | "/todo view" | "/todos view") {
                    print_todos(&current_todos);
                    continue;
                }
                if line.starts_with("/todo ") || line.starts_with("/todos ") {
                    match parse_todo_action(line, &current_todos) {
                        Ok(action) => {
                            session_command_tx.send(HostCommand::Todo {
                                session_id,
                                action,
                            }).await.ok();
                        }
                        Err(error) => eprintln!("\n  {error}"),
                    }
                    continue;
                }
                if line == "/resume" {
                    print_recent_sessions(&sessions_dir, sqlite_store.as_ref(), session_id, &cwd)
                        .await?;
                    continue;
                }
                if let Some(target) = line.strip_prefix("/resume ") {
                    match resolve_resume_switch(
                        &sessions_dir,
                        sqlite_store.as_ref(),
                        session_id,
                        target,
                        session_access,
                        status,
                    )
                    .await
                    {
                        Ok((target, switch)) => {
                            tracing::info!(?switch, from = %session_id, to = %target, "switching local session");
                            resume_session = Some(target);
                            stop_sent = true;
                            session_command_tx
                                .send(HostCommand::Stop { session_id })
                                .await
                                .ok();
                        }
                        Err(error) => eprintln!("\n  {error}\n"),
                    }
                    continue;
                }
                if let Some((sidecar, intent)) = persistent_sidecar_command(line) {
                    let active = matches!(
                        status,
                        SessionStatus::Starting
                            | SessionStatus::Running
                            | SessionStatus::WaitingForApproval
                    );
                    let delivery = if active {
                        running_input(
                            line,
                            provider,
                            steer_active_codex,
                            status_is_compacting(status_detail.as_deref()),
                        )
                        .0
                    } else {
                        PromptDelivery::Steer
                    };
                    for command in persistent_sidecar_commands(
                        session_id,
                        sidecar,
                        &intent,
                        Uuid::new_v4(),
                        &[],
                        delivery,
                    ) {
                        if session_command_tx.send(command).await.is_err() {
                            break;
                        }
                    }
                    continue;
                }
                match line {
                    "/help" => print_agent_help(),
                    "/compact" => {
                        session_command_tx
                            .send(HostCommand::Compact { session_id })
                            .await
                            .ok();
                    }
                    "/clear" => {
                        session_command_tx
                            .send(HostCommand::ClearContext { session_id })
                            .await
                            .ok();
                    }
                    "/login" => {
                        login_provider(provider).await?;
                        println!("  Signed in. Retry your message.");
                    }
                    "/quit" | "/exit" => {
                        user_requested_exit = true;
                        if owner_shutdown_should_handoff_to_viewer(
                            session_access,
                            status,
                            local_prompt_submission_pending(
                                &pending_prompt_ids,
                                &local_prompt_admissions,
                            ),
                            control_server.as_ref(),
                        ) {
                            handoff_on_safe_boundary = true;
                            detached_from_terminal = true;
                        } else {
                            stop_sent = true;
                            session_command_tx.send(HostCommand::Stop { session_id }).await.ok();
                        }
                    }
                    "/interrupt" | "/stop" => {
                        session_command_tx.send(HostCommand::Interrupt { session_id }).await.ok();
                    }
                    _ => {
                        let active = matches!(
                            status,
                            SessionStatus::Starting
                                | SessionStatus::Running
                                | SessionStatus::WaitingForApproval
                        );
                        let (delivery, text) = if active {
                            running_input(
                                line,
                                provider,
                                steer_active_codex,
                                status_is_compacting(status_detail.as_deref()),
                            )
                        } else {
                            idle_input(line)
                        };
                        if !text.is_empty() {
                            let message_id = Uuid::new_v4();
                            pending_prompt_ids.insert(message_id);
                            if session_command_tx.send(HostCommand::Prompt {
                                session_id,
                                message_id,
                                text,
                                attachments: Vec::new(),
                                output_schema: None,
                                delivery,
                            }).await.is_err() {
                                pending_prompt_ids.remove(&message_id);
                            }
                        }
                    }
                }
            }
            terminal_event = recv_terminal_event(&mut terminal), if terminal.is_some() => {
                let Some(terminal_event) = terminal_event else {
                    tracing::warn!(%session_id, "terminal input pump ended; restarting it");
                    terminal
                        .as_mut()
                        .expect("terminal")
                        .restart_input("Terminal input recovered")
                        .await;
                    terminal_dirty = true;
                    continue;
                };
                let terminal_event = match terminal_event {
                    Ok(event) => event,
                    Err(error) => {
                        tracing::warn!(%session_id, %error, "terminal input failed; restarting it");
                        terminal
                            .as_mut()
                            .expect("terminal")
                            .restart_input("Terminal input recovered")
                            .await;
                        terminal_dirty = true;
                        continue;
                    }
                };
                let scroll_was_active = terminal
                    .as_ref()
                    .expect("terminal")
                    .has_pending_scroll_frame();
                let action = terminal.as_mut().expect("terminal").handle_event(terminal_event)?;
                let scroll_is_active = terminal
                    .as_ref()
                    .expect("terminal")
                    .has_pending_scroll_frame();
                if scroll_is_active && !scroll_was_active {
                    render_frame_interval = tui_frame_interval(tui_fps);
                    render_tick = tui_render_interval(render_frame_interval);
                }
                let event_redraw_needed = terminal
                    .as_mut()
                    .expect("terminal")
                    .take_event_redraw_needed();
                terminal_dirty |= event_redraw_needed;
                if matches!(&action, UiAction::None)
                    && event_redraw_needed
                    && terminal.as_ref().expect("terminal").is_launch_screen()
                {
                    terminal.as_mut().expect("terminal").draw()?;
                    terminal_dirty = false;
                }
                match action {
                    UiAction::None => {}
                    UiAction::Approve { target, decision } => {
                        if let Some(target) = target {
                            if let Some(approval_id) =
                                child_pending_approvals.get(&target).cloned()
                            {
                                session_command_tx
                                    .send(HostCommand::Subagent {
                                        session_id,
                                        action: SubagentAction::Approve {
                                            request_id: Uuid::new_v4(),
                                            target: target.to_string(),
                                            approval_id,
                                            decision,
                                        },
                                    })
                                    .await
                                    .ok();
                            }
                        } else if let Some(approval_id) = pending_approval.clone() {
                            session_command_tx.send(HostCommand::Approve {
                                session_id,
                                approval_id,
                                decision,
                            }).await.ok();
                        }
                    }
                    UiAction::RecallQueuedPrompts { target } => {
                        if let Some(target) = target {
                            session_command_tx
                                .send(HostCommand::Subagent {
                                    session_id,
                                    action: SubagentAction::RecallPrompt {
                                        request_id: Uuid::new_v4(),
                                        target: target.to_string(),
                                        message_id: None,
                                    },
                                })
                                .await
                                .ok();
                        } else {
                            session_command_tx
                                .send(HostCommand::RecallQueuedPrompt {
                                    session_id,
                                    message_id: None,
                                })
                                .await
                                .ok();
                        }
                    }
                    UiAction::Rewind {
                        sequence,
                        text,
                        attachments,
                    } => {
                        if matches!(
                            status,
                            SessionStatus::Starting
                                | SessionStatus::Running
                                | SessionStatus::WaitingForApproval
                        ) {
                            terminal.as_mut().expect("terminal").set_notice(
                                "Interrupt the current turn before rewinding".to_string(),
                            );
                        } else {
                            let fork_id = Uuid::new_v4();
                            store
                                .fork_before(session_id, fork_id, sequence)
                                .await?;
                            resume_session = Some(fork_id);
                            rewind_prompt = Some((text, attachments));
                            stop_sent = true;
                            session_command_tx.send(HostCommand::Stop { session_id }).await.ok();
                        }
                    }
                    UiAction::SetModel(model) => {
                        let active = terminal
                            .as_ref()
                            .and_then(BorgTerminal::session_provider)
                            .unwrap_or(provider);
                        let target = CodingProvider::for_model(&model).unwrap_or(active);
                        if !provider_credentials_present(target) {
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .open_provider_auth_picker(target, model);
                        } else {
                            send_model_selection(
                                &session_command_tx,
                                session_id,
                                active,
                                target,
                                model,
                            )
                            .await;
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .open_effort_picker_for(Some(target));
                        }
                    }
                    UiAction::AuthenticateProvider {
                        provider: target,
                        model,
                        choice,
                    } => {
                        if matches!(
                            status,
                            SessionStatus::Starting
                                | SessionStatus::Running
                                | SessionStatus::WaitingForApproval
                        ) {
                            terminal.as_mut().expect("terminal").set_notice(
                                "Interrupt the current turn before connecting a provider."
                                    .to_string(),
                            );
                        } else {
                            // Both flows need a plain terminal: the subscription
                            // sign-in runs the provider's own device flow, and the
                            // key prompt reads from stdin with echo disabled.
                            shutdown_terminal(&mut terminal, &crash_context.tui_active).await;
                            let outcome = match choice {
                                ProviderAuthChoice::Subscription => {
                                    login_provider(target).await.map(|()| {
                                        format!("Connected {}.", target.label())
                                    })
                                }
                                ProviderAuthChoice::ApiKey => {
                                    prompt_and_store_api_key(target).map(|path| {
                                        format!(
                                            "{} API key saved to {}.",
                                            target.label(),
                                            path.display()
                                        )
                                    })
                                }
                            };
                            let latest_state = store.state(session_id).await?;
                            let latest = recent_tui_history(
                                store.as_ref(),
                                session_id,
                                latest_state.latest_sequence,
                            )
                            .await?;
                            let mut restored = BorgTerminal::enter(
                                &sessions_dir,
                                session_id,
                                cwd.clone(),
                                &agent_config.keybindings,
                            )?;
                            let composer_history = store
                                .recent_user_messages(session_id, RICH_TUI_PROMPT_HISTORY_LIMIT)
                                .await?;
                            restored.seed_composer_history(&composer_history);
                            restored.seed_history(&latest);
                            let (_, agents, histories) = load_subagent_thread_state(
                                store.as_ref(),
                                &sessions_dir,
                                session_id,
                            )
                            .await?;
                            seed_terminal_subagent_threads(&mut restored, &agents, &histories);
                            restored.seed_session_state(&latest_state);
                            let active = restored.session_provider().unwrap_or(provider);
                            restored.set_notice(match &outcome {
                                Ok(message) => format!("{message} Switching to {model}."),
                                Err(error) => format!("Provider not connected: {error:#}"),
                            });
                            terminal = Some(restored);
                            crash_context.tui_active.store(true, Ordering::Release);
                            if outcome.is_ok() {
                                send_model_selection(
                                    &session_command_tx,
                                    session_id,
                                    active,
                                    target,
                                    model,
                                )
                                .await;
                                terminal
                                    .as_mut()
                                    .expect("terminal")
                                    .open_effort_picker_for(Some(target));
                            }
                        }
                    }
                    UiAction::SetEffort(effort) => {
                        session_command_tx
                            .send(HostCommand::Configure {
                                session_id,
                                action: SessionConfigAction::SetEffort { effort },
                            })
                            .await
                            .ok();
                    }
                    UiAction::SetPermissionMode(permission_mode) => {
                        session_command_tx
                            .send(HostCommand::Configure {
                                session_id,
                                action: SessionConfigAction::SetPermissionMode {
                                    permission_mode,
                                },
                            })
                            .await
                            .ok();
                    }
                    UiAction::SetResponseLanguage(language) => {
                        session_command_tx
                            .send(HostCommand::Configure {
                                session_id,
                                action: SessionConfigAction::SetResponseLanguage { language },
                            })
                            .await
                            .ok();
                    }
                    UiAction::SetFast(enabled) => {
                        session_command_tx
                            .send(HostCommand::Configure {
                                session_id,
                                action: SessionConfigAction::SetFast { enabled },
                            })
                            .await
                            .ok();
                    }
                    UiAction::SetRefreshRate(fps) => {
                        tui_fps = fps.clamp(MIN_TUI_FPS, MAX_TUI_FPS);
                        render_frame_interval = tui_frame_interval(tui_fps);
                        render_tick = tui_render_interval(render_frame_interval);
                        editor_preferences.presentation.refresh_rate_fps =
                            u16::try_from(tui_fps).expect("bounded refresh rate fits u16");
                        editor_preferences.save()?;
                        terminal
                            .as_mut()
                            .expect("terminal")
                            .set_notice(format!("Refresh rate set to {tui_fps} FPS"));
                    }
                    UiAction::SetPreventSleep(enabled) => {
                        prevent_sleep = enabled;
                        sleep_inhibitor.set_enabled(enabled);
                        editor_preferences.interaction.prevent_sleep = enabled;
                        editor_preferences.save()?;
                        terminal.as_mut().expect("terminal").set_notice(format!(
                            "Prevent sleep during active turns: {}",
                            if enabled { "on" } else { "off" }
                        ));
                    }
                    UiAction::SetSteerActive(enabled) => {
                        steer_active_codex = enabled;
                        editor_preferences.interaction.active_messages = if enabled {
                            ActiveMessageBehavior::Steer
                        } else {
                            ActiveMessageBehavior::Queue
                        };
                        editor_preferences.save()?;
                        terminal.as_mut().expect("terminal").set_notice(format!(
                            "Messages sent while Codex works: {}",
                            if enabled {
                                "steer current turn"
                            } else {
                                "queue next turn"
                            }
                        ));
                    }
                    UiAction::SetAutoExpandEdits(enabled) => {
                        editor_preferences.presentation.auto_expand_edits = enabled;
                        editor_preferences.save()?;
                        let terminal = terminal.as_mut().expect("terminal");
                        terminal.set_auto_expand_edits(enabled);
                        terminal.set_notice(format!(
                            "Auto-expand edit diffs: {}",
                            if enabled { "on" } else { "off" }
                        ));
                    }
                    UiAction::SetAutoExpandTools(enabled) => {
                        editor_preferences.presentation.auto_expand_tools = enabled;
                        editor_preferences.save()?;
                        let terminal = terminal.as_mut().expect("terminal");
                        terminal.set_auto_expand_tools(enabled);
                        terminal.set_notice(format!(
                            "Auto-expand other tool details: {}",
                            if enabled { "on" } else { "off" }
                        ));
                    }
                    UiAction::LoadPayloads(payloads) => {
                        for payload in payloads {
                            match store.load_payload(&payload).await {
                                Ok(bytes) => terminal
                                    .as_mut()
                                    .expect("terminal")
                                    .hydrate_payload(&payload, bytes)?,
                                Err(error) => {
                                    terminal
                                        .as_mut()
                                        .expect("terminal")
                                        .set_notice(error.to_string());
                                    break;
                                }
                            }
                        }
                    }
                    UiAction::Interrupt { target } => {
                        if let Some(target) = target {
                            session_command_tx
                                .send(HostCommand::Subagent {
                                    session_id,
                                    action: SubagentAction::Interrupt {
                                        request_id: Uuid::new_v4(),
                                        target: target.to_string(),
                                    },
                                })
                                .await
                                .ok();
                        } else if matches!(
                            status,
                            SessionStatus::Starting
                                | SessionStatus::Running
                                | SessionStatus::WaitingForApproval
                        ) {
                            session_command_tx.send(HostCommand::Interrupt { session_id }).await.ok();
                        }
                    }
                    UiAction::Quit => {
                        user_requested_exit = true;
                        if owner_shutdown_should_handoff_to_viewer(
                            session_access,
                            status,
                            local_prompt_submission_pending(
                                &pending_prompt_ids,
                                &local_prompt_admissions,
                            ),
                            control_server.as_ref(),
                        ) {
                            handoff_on_safe_boundary = true;
                            detached_from_terminal = true;
                            shutdown_terminal(&mut terminal, &crash_context.tui_active).await;
                        } else {
                            stop_sent = true;
                            shutdown_terminal(&mut terminal, &crash_context.tui_active).await;
                            session_command_tx.send(HostCommand::Stop { session_id }).await.ok();
                        }
                    }
                    UiAction::Queue {
                        target,
                        message_id,
                        text,
                        attachments,
                    } => {
                        if let Some((sidecar, intent)) = persistent_sidecar_command(&text) {
                            let terminal = terminal.as_mut().expect("terminal");
                            terminal.discard_pending_prompt(target, message_id);
                            terminal.request_sidecar_focus(sidecar.task_name());
                            terminal.set_notice(sidecar_notice(sidecar, &intent));
                            for command in persistent_sidecar_commands(
                                session_id,
                                sidecar,
                                &intent,
                                message_id,
                                &attachments,
                                PromptDelivery::Queue,
                            ) {
                                if session_command_tx.send(command).await.is_err() {
                                    terminal.set_notice(format!(
                                        "Could not reach {} sidecar",
                                        sidecar.label()
                                    ));
                                    break;
                                }
                            }
                            continue;
                        }
                        let command = target.map_or_else(
                            || HostCommand::Prompt {
                                session_id,
                                message_id,
                                text: text.clone(),
                                attachments: attachments.clone(),
                                output_schema: None,
                                delivery: PromptDelivery::Queue,
                            },
                            |target| HostCommand::Subagent {
                                session_id,
                                action: SubagentAction::Prompt {
                                    request_id: Uuid::new_v4(),
                                    target: target.to_string(),
                                    message_id,
                                    text: text.clone(),
                                    attachments: attachments.clone(),
                                    delivery: PromptDelivery::Queue,
                                },
                            },
                        );
                        terminal.as_mut().expect("terminal").draw()?;
                        terminal_dirty = terminal
                            .as_ref()
                            .expect("terminal")
                            .has_pending_scroll_frame();
                        pending_prompt_ids.insert(message_id);
                        if session_command_tx.send(command).await.is_err() {
                            pending_prompt_ids.remove(&message_id);
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .reject_optimistic_prompt(
                                    target,
                                    message_id,
                                    text,
                                    attachments,
                                );
                            terminal.as_mut().expect("terminal").draw()?;
                            terminal_dirty = false;
                        }
                    }
                    UiAction::Submit {
                        target,
                        text,
                        attachments,
                    } => {
                        if let Some((sidecar, intent)) = persistent_sidecar_command(&text) {
                            let terminal = terminal.as_mut().expect("terminal");
                            terminal.request_sidecar_focus(sidecar.task_name());
                            terminal.set_notice(sidecar_notice(sidecar, &intent));
                            for command in persistent_sidecar_commands(
                                session_id,
                                sidecar,
                                &intent,
                                Uuid::new_v4(),
                                &attachments,
                                PromptDelivery::Steer,
                            ) {
                                if session_command_tx.send(command).await.is_err() {
                                    terminal.set_notice(format!(
                                        "Could not reach {} sidecar",
                                        sidecar.label()
                                    ));
                                    break;
                                }
                            }
                            continue;
                        }
                        if let Some(target) = target {
                            let message_id = Uuid::new_v4();
                            let active_provider = terminal
                                .as_ref()
                                .and_then(BorgTerminal::session_provider)
                                .unwrap_or(provider);
                            let delivery = if status_is_compacting(status_detail.as_deref()) {
                                PromptDelivery::Queue
                            } else {
                                default_active_delivery(active_provider, steer_active_codex)
                            };
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .project_pending_prompt(
                                    Some(target),
                                    message_id,
                                    text.clone(),
                                    delivery,
                                );
                            let command = HostCommand::Subagent {
                                session_id,
                                action: SubagentAction::Prompt {
                                    request_id: Uuid::new_v4(),
                                    target: target.to_string(),
                                    message_id,
                                    text: text.clone(),
                                    attachments: attachments.clone(),
                                    delivery,
                                },
                            };
                            terminal.as_mut().expect("terminal").draw()?;
                            terminal_dirty = terminal
                                .as_ref()
                                .expect("terminal")
                                .has_pending_scroll_frame();
                            pending_prompt_ids.insert(message_id);
                            if session_command_tx.send(command).await.is_err() {
                                pending_prompt_ids.remove(&message_id);
                                terminal
                                    .as_mut()
                                    .expect("terminal")
                                    .reject_optimistic_prompt(
                                        Some(target),
                                        message_id,
                                        text,
                                        attachments,
                                    );
                                terminal.as_mut().expect("terminal").draw()?;
                                terminal_dirty = false;
                            }
                            continue;
                        }
                        if let Some((interaction_id, kind, payload)) =
                            pending_provider_interaction.clone()
                        {
                            if !attachments.is_empty() {
                                terminal.as_mut().expect("terminal").restore_composer(
                                    text,
                                    attachments,
                                );
                                terminal.as_mut().expect("terminal").set_notice(
                                    "Provider input responses cannot include attachments"
                                        .to_string(),
                                );
                                continue;
                            }
                            match provider_interaction_response(&kind, &payload, text.trim()) {
                                Ok(response) => {
                                    session_command_tx
                                        .send(HostCommand::RespondToProviderInteraction {
                                            session_id,
                                            interaction_id,
                                            response,
                                        })
                                        .await
                                        .ok();
                                }
                                Err(error) => {
                                    terminal
                                        .as_mut()
                                        .expect("terminal")
                                        .restore_composer(text, Vec::new());
                                    terminal
                                        .as_mut()
                                        .expect("terminal")
                                        .set_notice(error.to_string());
                                }
                            }
                            continue;
                        }
                        let expanded = agent_config.expand_command(text.trim());
                        let expanded = normalize_consultation_command(&expanded);
                        let line = expanded.trim();
                        if line == "/model" && attachments.is_empty() {
                            terminal.as_mut().expect("terminal").open_model_picker();
                        } else if line == "/effort" && attachments.is_empty() {
                            terminal.as_mut().expect("terminal").open_effort_picker();
                        } else if line == "/language" && attachments.is_empty() {
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .open_language_picker();
                        } else if line == "/fast" && attachments.is_empty() {
                            terminal.as_mut().expect("terminal").open_fast_picker(current_fast);
                        } else if line == "/refresh" && attachments.is_empty() {
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .open_refresh_rate_picker(tui_fps);
                        } else if line == "/sleep" && attachments.is_empty() {
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .open_prevent_sleep_picker(prevent_sleep);
                        } else if line == "/expand-edits" && attachments.is_empty() {
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .open_auto_expand_edits_picker();
                        } else if line == "/expand-tools" && attachments.is_empty() {
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .open_auto_expand_tools_picker();
                        } else if line == "/followups" && attachments.is_empty() {
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .open_active_messages_picker(steer_active_codex);
                        } else if line == "/settings" && attachments.is_empty() {
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .open_settings_picker(
                                    &editor_preferences.transcript.user_label,
                                    &editor_preferences.transcript.assistant_label,
                                );
                        } else if line == "/lsp" && attachments.is_empty() {
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .set_notice(lsp_support_summary());
                        } else if line == "/extensions" && attachments.is_empty() {
                            terminal.as_mut().expect("terminal").show_info(
                                "Blu extensions",
                                live_extension_summary(
                                    &extension_catalog,
                                    last_blu_discovery_error.as_deref(),
                                ),
                            );
                        } else if line == "/colors" && attachments.is_empty() {
                            terminal.as_mut().expect("terminal").set_notice(
                                transcript_colors_summary(&editor_preferences),
                            );
                        } else if line == "/color" && attachments.is_empty() {
                            terminal.as_mut().expect("terminal").restore_composer(
                                "/color ".to_string(),
                                Vec::new(),
                            );
                            terminal.as_mut().expect("terminal").set_notice(
                                "Use /color user-label|user-message|assistant-label|assistant-message #RRGGBB",
                            );
                        } else if line == "/user-label" && attachments.is_empty() {
                            terminal.as_mut().expect("terminal").restore_composer(
                                "/user-label ".to_string(),
                                Vec::new(),
                            );
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .set_notice("Type the user transcript label and press Enter");
                        } else if line == "/assistant-label" && attachments.is_empty() {
                            terminal.as_mut().expect("terminal").restore_composer(
                                "/assistant-label ".to_string(),
                                Vec::new(),
                            );
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .set_notice("Type the assistant transcript label and press Enter");
                        } else if line == "/usage" && attachments.is_empty() {
                            let summary = usage_summary(provider, &session_usage).await;
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .show_info("Usage", summary);
                        } else if line == "/clear" && attachments.is_empty() {
                            session_command_tx
                                .send(HostCommand::ClearContext { session_id })
                                .await
                                .ok();
                        } else if line == "/compact" && attachments.is_empty() {
                            session_command_tx
                                .send(HostCommand::Compact { session_id })
                                .await
                                .ok();
                        } else if let Some(model) = line.strip_prefix("/model ")
                            && attachments.is_empty()
                        {
                            let model = model.trim().to_string();
                            let active_provider = terminal
                                .as_ref()
                                .and_then(BorgTerminal::session_provider)
                                .unwrap_or(provider);
                            let target =
                                CodingProvider::for_model(&model).unwrap_or(active_provider);
                            send_model_selection(
                                &session_command_tx,
                                session_id,
                                active_provider,
                                target,
                                model,
                            )
                            .await;
                        } else if let Some(effort) = line.strip_prefix("/effort ")
                            && attachments.is_empty()
                        {
                            session_command_tx.send(HostCommand::Configure {
                                session_id,
                                action: SessionConfigAction::SetEffort {
                                    effort: effort.trim().to_string(),
                                },
                            }).await.ok();
                        } else if let Some(value) = line.strip_prefix("/language ")
                            && attachments.is_empty()
                        {
                            if let Some(language) = ResponseLanguage::parse(value) {
                                session_command_tx.send(HostCommand::Configure {
                                    session_id,
                                    action: SessionConfigAction::SetResponseLanguage { language },
                                }).await.ok();
                            } else {
                                terminal.as_mut().expect("terminal").set_notice(
                                    "Unknown language. Use /language to choose one.",
                                );
                            }
                        } else if let Some(value) = line.strip_prefix("/fast ")
                            && attachments.is_empty()
                        {
                            if let Some(enabled) = parse_on_off(value) {
                                session_command_tx.send(HostCommand::Configure {
                                    session_id,
                                    action: SessionConfigAction::SetFast { enabled },
                                }).await.ok();
                            } else {
                                terminal.as_mut().expect("terminal").set_notice(
                                    "Choose /fast on or /fast off",
                                );
                            }
                        } else if let Some(value) = line.strip_prefix("/user-label ")
                            && attachments.is_empty()
                        {
                            match set_transcript_label(&mut editor_preferences, true, value) {
                                Ok(()) => {
                                    terminal.as_mut().expect("terminal").set_transcript_labels(
                                        editor_preferences.transcript.user_label.clone(),
                                        editor_preferences.transcript.assistant_label.clone(),
                                    );
                                    terminal.as_mut().expect("terminal").set_notice(format!(
                                        "User transcript label: {}",
                                        editor_preferences.transcript.user_label
                                    ));
                                }
                                Err(error) => terminal
                                    .as_mut()
                                    .expect("terminal")
                                    .set_notice(error.to_string()),
                            }
                        } else if let Some(value) = line.strip_prefix("/assistant-label ")
                            && attachments.is_empty()
                        {
                            match set_transcript_label(&mut editor_preferences, false, value) {
                                Ok(()) => {
                                    terminal.as_mut().expect("terminal").set_transcript_labels(
                                        editor_preferences.transcript.user_label.clone(),
                                        editor_preferences.transcript.assistant_label.clone(),
                                    );
                                    terminal.as_mut().expect("terminal").set_notice(format!(
                                        "Assistant transcript label: {}",
                                        editor_preferences.transcript.assistant_label
                                    ));
                                }
                                Err(error) => terminal
                                    .as_mut()
                                    .expect("terminal")
                                    .set_notice(error.to_string()),
                            }
                        } else if let Some(value) = line.strip_prefix("/color ")
                            && attachments.is_empty()
                        {
                            match set_transcript_color(&mut editor_preferences, value) {
                                Ok(()) => {
                                    terminal
                                        .as_mut()
                                        .expect("terminal")
                                        .set_transcript_colors(&editor_preferences.transcript);
                                    terminal.as_mut().expect("terminal").set_notice(
                                        transcript_colors_summary(&editor_preferences),
                                    );
                                }
                                Err(error) => terminal
                                    .as_mut()
                                    .expect("terminal")
                                    .set_notice(error.to_string()),
                            }
                        } else if let Some(value) = line.strip_prefix("/refresh ")
                            && attachments.is_empty()
                        {
                            match value.parse::<u64>() {
                                Ok(fps @ MIN_TUI_FPS..=MAX_TUI_FPS) => {
                                    tui_fps = fps;
                                    render_frame_interval = tui_frame_interval(tui_fps);
                                    render_tick = tui_render_interval(render_frame_interval);
                                    editor_preferences.presentation.refresh_rate_fps =
                                        u16::try_from(fps)
                                            .expect("bounded refresh rate fits u16");
                                    editor_preferences.save()?;
                                    terminal.as_mut().expect("terminal").set_notice(format!(
                                        "Refresh rate set to {tui_fps} FPS"
                                    ));
                                }
                                _ => terminal.as_mut().expect("terminal").set_notice(format!(
                                    "Refresh rate must be between {MIN_TUI_FPS} and {MAX_TUI_FPS} FPS"
                                )),
                            }
                        } else if let Some(value) = line.strip_prefix("/sleep ")
                            && attachments.is_empty()
                        {
                            if let Some(enabled) = parse_on_off(value) {
                                prevent_sleep = enabled;
                                sleep_inhibitor.set_enabled(enabled);
                                editor_preferences.interaction.prevent_sleep = enabled;
                                editor_preferences.save()?;
                                terminal.as_mut().expect("terminal").set_notice(format!(
                                    "Prevent sleep during active turns: {}",
                                    if enabled { "on" } else { "off" }
                                ));
                            } else {
                                terminal
                                    .as_mut()
                                    .expect("terminal")
                                    .set_notice("Choose /sleep on or /sleep off");
                            }
                        } else if let Some(value) = line.strip_prefix("/expand-edits ")
                            && attachments.is_empty()
                        {
                            if let Some(enabled) = parse_on_off(value) {
                                editor_preferences.presentation.auto_expand_edits = enabled;
                                editor_preferences.save()?;
                                let terminal = terminal.as_mut().expect("terminal");
                                terminal.set_auto_expand_edits(enabled);
                                terminal.set_notice(format!(
                                    "Auto-expand edit diffs: {}",
                                    if enabled { "on" } else { "off" }
                                ));
                            } else {
                                terminal.as_mut().expect("terminal").set_notice(
                                    "Choose /expand-edits on or /expand-edits off",
                                );
                            }
                        } else if let Some(value) = line.strip_prefix("/expand-tools ")
                            && attachments.is_empty()
                        {
                            if let Some(enabled) = parse_on_off(value) {
                                editor_preferences.presentation.auto_expand_tools = enabled;
                                editor_preferences.save()?;
                                let terminal = terminal.as_mut().expect("terminal");
                                terminal.set_auto_expand_tools(enabled);
                                terminal.set_notice(format!(
                                    "Auto-expand other tool details: {}",
                                    if enabled { "on" } else { "off" }
                                ));
                            } else {
                                terminal.as_mut().expect("terminal").set_notice(
                                    "Choose /expand-tools on or /expand-tools off",
                                );
                            }
                        } else if let Some(value) = line.strip_prefix("/followups ")
                            && attachments.is_empty()
                        {
                            match value.trim() {
                                "steer" => {
                                    steer_active_codex = true;
                                    editor_preferences.interaction.active_messages =
                                        ActiveMessageBehavior::Steer;
                                    editor_preferences.save()?;
                                    terminal.as_mut().expect("terminal").set_notice(
                                        "Messages sent while Codex works: steer current turn",
                                    );
                                }
                                "queue" => {
                                    steer_active_codex = false;
                                    editor_preferences.interaction.active_messages =
                                        ActiveMessageBehavior::Queue;
                                    editor_preferences.save()?;
                                    terminal.as_mut().expect("terminal").set_notice(
                                        "Messages sent while Codex works: queue next turn",
                                    );
                                }
                                _ => terminal.as_mut().expect("terminal").set_notice(
                                    "Choose /followups steer or /followups queue",
                                ),
                            }
                        } else if line == "/goal" || line == "/goal view" {
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .show_goal(current_goal.as_ref());
                        } else if line.starts_with("/goal ") && attachments.is_empty() {
                            match parse_goal_action(line) {
                                Ok(action) => {
                                    session_command_tx.send(HostCommand::Goal {
                                        session_id,
                                        action,
                                    }).await.ok();
                                }
                                Err(error) => terminal.as_mut().expect("terminal").set_notice(error.to_string()),
                            }
                        } else if matches!(line, "/todo" | "/todos" | "/todo view" | "/todos view")
                            && attachments.is_empty()
                        {
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .show_plan(&current_todos);
                        } else if (line.starts_with("/todo ") || line.starts_with("/todos "))
                            && attachments.is_empty()
                        {
                            match parse_todo_action(line, &current_todos) {
                                Ok(action) => {
                                    session_command_tx.send(HostCommand::Todo {
                                        session_id,
                                        action,
                                    }).await.ok();
                                }
                                Err(error) => terminal.as_mut().expect("terminal").set_notice(error.to_string()),
                            }
                        } else if line == "/resume" && attachments.is_empty() {
                            let sessions = recent_session_options(
                                &sessions_dir,
                                sqlite_store.as_ref(),
                                session_id,
                                &cwd,
                                RESUME_PICKER_SESSION_LIMIT,
                            )
                            .await?;
                            if sessions.is_empty() {
                                terminal.as_mut().expect("terminal").set_notice(
                                    "No other saved Borg sessions. Use /resume <session-id>.",
                                );
                            } else {
                                terminal
                                    .as_mut()
                                    .expect("terminal")
                                    .open_resume_picker(&sessions);
                            }
                        } else if let Some(target) = line.strip_prefix("/resume ")
                            && attachments.is_empty()
                        {
                            match resolve_resume_switch(
                                &sessions_dir,
                                sqlite_store.as_ref(),
                                session_id,
                                target,
                                session_access,
                                status,
                            )
                            .await
                            {
                                Ok((target, switch)) => {
                                    tracing::info!(?switch, from = %session_id, to = %target, "switching local session");
                                    resume_session = Some(target);
                                    stop_sent = true;
                                    session_command_tx
                                        .send(HostCommand::Stop { session_id })
                                        .await
                                        .ok();
                                }
                                Err(error) => terminal
                                    .as_mut()
                                    .expect("terminal")
                                    .set_notice(error.to_string()),
                            }
                        } else {
                            match line {
                                "/help" if attachments.is_empty() => terminal
                                    .as_mut()
                                    .expect("terminal")
                                    .show_info(
                                        "Commands",
                                        "/settings · /model · /effort · /extensions · /followups · /refresh · /sleep · /usage · /clear · /compact · /resume · /goal · /todo · /login · /collab · /remote · /quit",
                                    ),
                                "/collab" | "/collab view" if attachments.is_empty() => {
                                    if collab_child.is_some() {
                                        terminal.as_mut().expect("terminal").set_notice(
                                            "This session already has an active collaboration link. Use /collab stop first."
                                        );
                                    } else {
                                        match start_collaboration_host(session_id).await {
                                            Ok((child, view, control)) => {
                                                let notice = if line == "/collab view" {
                                                    format!("Read-only collaboration link:\n{view}")
                                                } else {
                                                    format!("Control link:\n{control}\n\nRead-only link:\n{view}")
                                                };
                                                collab_child = Some(child);
                                                terminal.as_mut().expect("terminal").show_info(
                                                    "Collaboration",
                                                    &notice,
                                                );
                                            }
                                            Err(error) => terminal
                                                .as_mut()
                                                .expect("terminal")
                                                .set_notice(format!("Collaboration failed: {error:#}")),
                                        }
                                    }
                                }
                                "/collab stop" if attachments.is_empty() => {
                                    if let Some(mut child) = collab_child.take() {
                                        child.kill().await.ok();
                                        terminal.as_mut().expect("terminal").set_notice(
                                            "Collaboration link stopped."
                                        );
                                    } else {
                                        terminal.as_mut().expect("terminal").set_notice(
                                            "No collaboration link is active."
                                        );
                                    }
                                }
                                "/remote" if attachments.is_empty() => {
                                    if matches!(
                                        status,
                                        SessionStatus::Starting
                                            | SessionStatus::Running
                                            | SessionStatus::WaitingForApproval
                                    ) {
                                        terminal.as_mut().expect("terminal").set_notice(
                                            "Interrupt the current turn before connecting Borg Remote."
                                        );
                                    } else if remote_open {
                                        terminal.as_mut().expect("terminal").set_notice(
                                            "This machine is already connected to Borg Remote."
                                        );
                                    } else {
                                        shutdown_terminal(&mut terminal, &crash_context.tui_active).await;
                                        let connected = match connect_remote_account(
                                            "https://borg.ml",
                                            None,
                                            vec![cwd.clone()],
                                            &host_config_path,
                                        )
                                        .await
                                        {
                                            Ok(()) => install_host_service(&host_config_path).await,
                                            Err(error) => Err(error),
                                        };
                                        if connected.is_ok() {
                                            let mut registration = registration_template.clone();
                                            registration.request_id = session_id;
                                            registration.initial_prompt = None;
                                            let config_path = host_config_path.clone();
                                            let mirror_store = Arc::clone(&store);
                                            let command_tx = remote_command_tx.clone();
                                            let (shutdown_tx, shutdown_rx) = watch::channel(false);
                                            mirror_shutdown = Some(shutdown_tx);
                                            mirror_task = Some(tokio::spawn(async move {
                                                if let Err(error) = mirror_local_session(
                                                    &config_path,
                                                    mirror_store,
                                                    session_id,
                                                    registration,
                                                    command_tx,
                                                    shutdown_rx,
                                                )
                                                .await
                                                {
                                                    tracing::warn!(%error, "remote mirror stopped");
                                                }
                                            }));
                                            remote_open = true;
                                        }
                                        let latest_state = store.state(session_id).await?;
                                        let latest = recent_tui_history(
                                            store.as_ref(),
                                            session_id,
                                            latest_state.latest_sequence,
                                        )
                                        .await?;
                                        let mut restored = BorgTerminal::enter(
                                            &sessions_dir,
                                            session_id,
                                            cwd.clone(),
                                            &agent_config.keybindings,
                                        )?;
                                        let composer_history = store
                                            .recent_user_messages(
                                                session_id,
                                                RICH_TUI_PROMPT_HISTORY_LIMIT,
                                            )
                                            .await?;
                                        restored.seed_composer_history(&composer_history);
                                        restored.seed_history(&latest);
                                        let (_, agents, histories) =
                                            load_subagent_thread_state(
                                                store.as_ref(),
                                                &sessions_dir,
                                                session_id,
                                            )
                                                .await?;
                                        seed_terminal_subagent_threads(
                                            &mut restored,
                                            &agents,
                                            &histories,
                                        );
                                        restored.seed_session_state(&latest_state);
                                        restored.set_notice(match connected {
                                            Ok(()) => "Remote connected. This chat is now available at borg.ml/remote.".to_string(),
                                            Err(error) => format!("Remote connection failed: {error:#}"),
                                        });
                                        terminal = Some(restored);
                                        crash_context.tui_active.store(true, Ordering::Release);
                                    }
                                }
                                "/login" if attachments.is_empty() => {
                                    if matches!(
                                        status,
                                        SessionStatus::Starting
                                            | SessionStatus::Running
                                            | SessionStatus::WaitingForApproval
                                    ) {
                                        terminal.as_mut().expect("terminal").set_notice(
                                            "Interrupt the current turn before reconnecting the provider."
                                        );
                                    } else {
                                        // The native device flow needs a normal terminal. Rebuild
                                        // the UI from its journal after the provider returns.
                                        shutdown_terminal(&mut terminal, &crash_context.tui_active).await;
                                        let login = login_provider(provider).await;
                                        let latest_state = store.state(session_id).await?;
                                        let latest = recent_tui_history(
                                            store.as_ref(),
                                            session_id,
                                            latest_state.latest_sequence,
                                        )
                                        .await?;
                                        let mut restored = BorgTerminal::enter(
                                            &sessions_dir,
                                            session_id,
                                            cwd.clone(),
                                            &agent_config.keybindings,
                                        )?;
                                        let composer_history = store
                                            .recent_user_messages(
                                                session_id,
                                                RICH_TUI_PROMPT_HISTORY_LIMIT,
                                            )
                                            .await?;
                                        restored.seed_composer_history(&composer_history);
                                        restored.seed_history(&latest);
                                        let (_, agents, histories) =
                                            load_subagent_thread_state(
                                                store.as_ref(),
                                                &sessions_dir,
                                                session_id,
                                            )
                                                .await?;
                                        seed_terminal_subagent_threads(
                                            &mut restored,
                                            &agents,
                                            &histories,
                                        );
                                        restored.seed_session_state(&latest_state);
                                        restored.set_notice(match login {
                                            Ok(()) => "Signed in. Retry your message.".to_string(),
                                            Err(error) => format!("Sign-in failed: {error:#}"),
                                        });
                                        terminal = Some(restored);
                                        crash_context.tui_active.store(true, Ordering::Release);
                                    }
                                }
                                "/quit" | "/exit" if attachments.is_empty() => {
                                    user_requested_exit = true;
                                    if owner_shutdown_should_handoff_to_viewer(
                                        session_access,
                                        status,
                                        local_prompt_submission_pending(
                                            &pending_prompt_ids,
                                            &local_prompt_admissions,
                                        ),
                                        control_server.as_ref(),
                                    ) {
                                        handoff_on_safe_boundary = true;
                                        detached_from_terminal = true;
                                        shutdown_terminal(&mut terminal, &crash_context.tui_active)
                                            .await;
                                    } else {
                                        stop_sent = true;
                                        shutdown_terminal(&mut terminal, &crash_context.tui_active)
                                            .await;
                                        session_command_tx
                                            .send(HostCommand::Stop { session_id })
                                            .await
                                            .ok();
                                    }
                                }
                                "/interrupt" | "/stop" if attachments.is_empty() => {
                                    session_command_tx.send(HostCommand::Interrupt { session_id }).await.ok();
                                }
                                _ => {
                                    let active = matches!(
                                        status,
                                        SessionStatus::Starting
                                            | SessionStatus::Running
                                            | SessionStatus::WaitingForApproval
                                    );
                                    let active_provider = terminal
                                        .as_ref()
                                        .and_then(BorgTerminal::session_provider)
                                        .unwrap_or(provider);
                                    let (delivery, text) = if active {
                                        running_input(
                                            &text,
                                            active_provider,
                                            steer_active_codex,
                                            status_is_compacting(status_detail.as_deref()),
                                        )
                                    } else {
                                        idle_input(&text)
                                    };
                                    if !text.is_empty() || !attachments.is_empty() {
                                        let message_id = Uuid::new_v4();
                                        if active {
                                            terminal
                                                .as_mut()
                                                .expect("terminal")
                                                .project_pending_prompt(
                                                    None,
                                                    message_id,
                                                    text.clone(),
                                                    delivery,
                                                );
                                        } else {
                                            terminal
                                                .as_mut()
                                                .expect("terminal")
                                                .project_submitted_prompt(
                                                    message_id,
                                                    text.clone(),
                                                    attachments.clone(),
                                                    delivery,
                                                );
                                            // The local projection is the
                                            // authoritative interaction state
                                            // until the durable TurnStarted
                                            // arrives. This also makes a
                                            // second immediate input queue or
                                            // steer instead of starting a
                                            // competing idle turn.
                                            status = SessionStatus::Starting;
                                            sleep_inhibitor.set_turn_active(true);
                                        }
                                        terminal.as_mut().expect("terminal").draw()?;
                                        terminal_dirty = terminal
                                            .as_ref()
                                            .expect("terminal")
                                            .has_pending_scroll_frame();
                                        pending_prompt_ids.insert(message_id);
                                        if let Err(error) = session_command_tx.send(HostCommand::Prompt {
                                            session_id,
                                            message_id,
                                            text,
                                            attachments,
                                            output_schema: None,
                                            delivery,
                                        }).await {
                                            pending_prompt_ids.remove(&message_id);
                                            let HostCommand::Prompt {
                                                text, attachments, ..
                                            } = error.0
                                            else {
                                                unreachable!("submission always sends a prompt command");
                                            };
                                            terminal
                                                .as_mut()
                                                .expect("terminal")
                                                .reject_optimistic_prompt(
                                                    None,
                                                    message_id,
                                                    text,
                                                    attachments,
                                                );
                                            if !active {
                                                status = SessionStatus::Ready;
                                                sleep_inhibitor.set_turn_active(false);
                                            }
                                            terminal.as_mut().expect("terminal").draw()?;
                                            terminal_dirty = false;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if let Some(terminal) = terminal.as_mut() {
                    terminal.draw()?;
                }
            }
            command = remote_commands.recv(), if remote_open => {
                match command {
                    Some(command) => {
                        if !args.json && terminal.is_none() {
                            println!("\n  ↳ remote {}", remote_command_name(&command));
                        }
                        session_command_tx.send(command).await.ok();
                    }
                    None => remote_open = false,
                }
            }
            _ = tokio::signal::ctrl_c(), if interactive => {
                if repeated_ctrl_c(&mut last_ctrl_c, std::time::Instant::now()) {
                    user_requested_exit = true;
                    if owner_shutdown_should_handoff_to_viewer(
                        session_access,
                        status,
                        local_prompt_submission_pending(
                            &pending_prompt_ids,
                            &local_prompt_admissions,
                        ),
                        control_server.as_ref(),
                    ) {
                        handoff_on_safe_boundary = true;
                        detached_from_terminal = true;
                        shutdown_terminal(&mut terminal, &crash_context.tui_active).await;
                    } else {
                        stop_sent = true;
                        shutdown_terminal(&mut terminal, &crash_context.tui_active).await;
                        session_command_tx.send(HostCommand::Stop { session_id }).await.ok();
                    }
                } else if let Some(terminal) = terminal.as_mut() {
                    terminal.handle_external_interrupt();
                    terminal_dirty = true;
                } else {
                    eprintln!("\n  Prompt cleared. Press Ctrl-C again to exit.");
                }
            }
            signal = shutdown_signals.recv(), if shutdown_signal_open => {
                shutdown_signal_open = false;
                let signal = signal.context("failed to listen for a process shutdown signal")?;
                tracing::warn!(%session_id, %signal, "restoring terminal before process shutdown");
                let attached_prompt_pending = local_prompt_submission_pending(
                    &pending_prompt_ids,
                    &local_prompt_admissions,
                );
                let handoff_to_viewer = owner_shutdown_should_handoff_to_viewer(
                    session_access,
                    status,
                    attached_prompt_pending,
                    control_server.as_ref(),
                );
                shutdown_terminal(&mut terminal, &crash_context.tui_active).await;
                if should_detach_on_terminal_hangup(
                    signal,
                    status,
                    attached_prompt_pending,
                ) {
                    // A terminal emulator can disappear independently of Borg (for
                    // example, a GPU/renderer crash). Keep the durable actor and
                    // control socket alive so a new `borg resume` can attach to the
                    // in-flight turn instead of cancelling it.
                    if handoff_to_viewer {
                        handoff_on_safe_boundary = true;
                    }
                    detached_from_terminal = true;
                    tracing::warn!(
                        %session_id,
                        %signal,
                        "terminal disappeared during an active turn; detached UI and preserved session"
                    );
                    continue;
                }
                if handoff_to_viewer {
                    handoff_on_safe_boundary = true;
                    detached_from_terminal = true;
                    tracing::warn!(
                        %session_id,
                        %signal,
                        "shutdown requested with an attached viewer; preserving the active turn until its boundary"
                    );
                    continue;
                }
                stop_sent = true;
                user_requested_exit = true;
                exit_notice = Some(format!(
                    "{signal} received; Borg restored the terminal and stopped the local session."
                ));
                session_command_tx
                    .send(HostCommand::Stop { session_id })
                    .await
                    .ok();
            }
        }
    }
    let tui_was_active = crash_context.tui_active.load(Ordering::Acquire);
    shutdown_terminal(&mut terminal, &crash_context.tui_active).await;
    drop(session_events);
    let actor_error = match actor.await {
        Ok(result) => result.err(),
        Err(join_error) if join_error.is_panic() && tui_was_active => {
            println!("{}", resume_instructions(session_id, false));
            return Ok(None);
        }
        Err(join_error) => Some(anyhow::anyhow!("agent session task failed: {join_error}")),
    };
    if let Some(error) = actor_error {
        if tui_was_active {
            tracing::error!(%session_id, error = %error, "local agent session crashed");
            println!("{}", resume_instructions(session_id, false));
            return Ok(None);
        }
        let active_elsewhere = error
            .to_string()
            .contains("session is already active in another Borg process");
        return Err(anyhow::anyhow!(
            "{error:#}\n\n{}",
            resume_instructions(session_id, active_elsewhere)
        ));
    }
    drop(control_server);
    if let Some(mut child) = collab_child {
        child.kill().await.ok();
    }
    if let Some(shutdown) = mirror_shutdown {
        shutdown.send(true).ok();
    }
    if let Some(task) = mirror_task {
        task.await.context("remote mirror task failed")?;
    }
    if should_print_exit_resume(user_requested_exit, resume_session, args.ephemeral) {
        if let Some(notice) = exit_notice {
            println!("\n  {notice}");
        }
        println!("\n{}", resume_instructions(session_id, false));
    }
    if session_access.is_attached() && !user_requested_exit && resume_session.is_none() {
        tracing::info!(%session_id, "active session owner exited; acquiring ownership");
        return Ok(Some((session_id, None)));
    }
    Ok(resume_session.map(|session| (session, rewind_prompt)))
}

async fn start_collaboration_host(session_id: Uuid) -> Result<(Child, String, String)> {
    let executable = std::env::current_exe().context("failed to locate the Borg executable")?;
    let relay =
        std::env::var("BORG_COLLAB_RELAY").unwrap_or_else(|_| "ws://127.0.0.1:8787".to_string());
    let mut child = TokioCommand::new(executable)
        .args(["collab", "host", &session_id.to_string(), "--relay", &relay])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("failed to start collaboration host")?;
    let stdout = child
        .stdout
        .take()
        .context("collaboration host has no stdout")?;
    let mut lines = BufReader::new(stdout).lines();
    let view = tokio::time::timeout(std::time::Duration::from_secs(10), lines.next_line())
        .await
        .context("collaboration relay connection timed out")??
        .context("collaboration host exited before returning a view link")?
        .strip_prefix("View: ")
        .context("collaboration host returned an invalid view link")?
        .to_string();
    let control = lines
        .next_line()
        .await?
        .context("collaboration host exited before returning a control link")?
        .strip_prefix("Control: ")
        .context("collaboration host returned an invalid control link")?
        .to_string();
    Ok((child, view, control))
}

/// Applies a model choice, switching the session's provider first when the
/// model belongs to a different one. The switch is live: the session keeps
/// running and the next turn goes to the new provider.
async fn send_model_selection(
    session_command_tx: &mpsc::Sender<HostCommand>,
    session_id: Uuid,
    active: CodingProvider,
    target: CodingProvider,
    model: String,
) {
    let action = if target == active {
        SessionConfigAction::SetModel { model }
    } else {
        SessionConfigAction::SetProvider {
            provider: target,
            model: Some(model),
        }
    };
    session_command_tx
        .send(HostCommand::Configure { session_id, action })
        .await
        .ok();
}

/// Reads an API key from the terminal without echoing it and stores it in the
/// borg credential store. Must run with the TUI torn down.
fn prompt_and_store_api_key(provider: CodingProvider) -> Result<PathBuf> {
    let credential = match provider {
        CodingProvider::Claude => borg_provider::credentials::ApiKeyCredential::Anthropic,
        other => anyhow::bail!("{} does not use a borg-managed API key", other.label()),
    };
    println!(
        "Paste your {} API key (input hidden), then press Enter:",
        provider.label()
    );
    io::stdout().flush().ok();
    let key = read_hidden_line().context("failed to read API key")?;
    borg_provider::credentials::store_api_key(credential, &key)
}

/// Line editor with echo suppressed. Only printable characters, backspace and
/// Enter are honoured; Esc and Ctrl-C abort so a mistyped key is never stored.
fn read_hidden_line() -> Result<String> {
    use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
    use crossterm::terminal::{disable_raw_mode, enable_raw_mode};

    enable_raw_mode().context("failed to enter raw mode for hidden input")?;
    let result = (|| -> Result<String> {
        let mut key = String::new();
        loop {
            match crossterm::event::read()? {
                Event::Key(KeyEvent {
                    code, modifiers, ..
                }) => match code {
                    KeyCode::Enter => break,
                    KeyCode::Backspace => {
                        key.pop();
                    }
                    KeyCode::Esc => anyhow::bail!("cancelled"),
                    KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => {
                        anyhow::bail!("cancelled");
                    }
                    KeyCode::Char(character) => key.push(character),
                    _ => {}
                },
                Event::Paste(text) => key.push_str(&text),
                _ => {}
            }
        }
        Ok(key)
    })();
    disable_raw_mode().ok();
    println!();
    result
}

async fn shutdown_terminal(terminal: &mut Option<BorgTerminal>, tui_active: &AtomicBool) {
    tui_active.store(false, Ordering::Release);
    if let Some(terminal) = terminal.take() {
        terminal.shutdown().await;
    }
}

struct ShutdownSignals {
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(unix)]
    hangup: tokio::signal::unix::Signal,
}

impl ShutdownSignals {
    fn new() -> io::Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};

            Ok(Self {
                terminate: signal(SignalKind::terminate())?,
                hangup: signal(SignalKind::hangup())?,
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    async fn recv(&mut self) -> io::Result<&'static str> {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = self.terminate.recv() => Ok("SIGTERM"),
                _ = self.hangup.recv() => Ok("SIGHUP"),
            }
        }
        #[cfg(not(unix))]
        {
            std::future::pending().await
        }
    }
}

const DOUBLE_CTRL_C_WINDOW: std::time::Duration = std::time::Duration::from_secs(1);

fn resume_instructions(session_id: Uuid, active_elsewhere: bool) -> String {
    let warning = if active_elsewhere {
        "Close the other Borg process before resuming.\n\n"
    } else {
        Default::default()
    };
    format!("{warning}Copy and paste the line below to resume:\nborg resume {session_id}")
}

fn should_print_exit_resume(
    user_requested_exit: bool,
    resume_session: Option<Uuid>,
    ephemeral: bool,
) -> bool {
    user_requested_exit && resume_session.is_none() && !ephemeral
}

fn should_detach_on_terminal_hangup(
    signal: &str,
    status: SessionStatus,
    prompt_submission_pending: bool,
) -> bool {
    signal == "SIGHUP"
        && (prompt_submission_pending
            || matches!(
                status,
                SessionStatus::Starting
                    | SessionStatus::Running
                    | SessionStatus::WaitingForApproval
            ))
}

fn committed_prompt_id(kind: &SessionEventKind) -> Option<Uuid> {
    match kind {
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            status: MessageStatus::Complete,
            ..
        } => Some(*message_id),
        _ => None,
    }
}

fn repeated_ctrl_c(last: &mut Option<std::time::Instant>, now: std::time::Instant) -> bool {
    let repeated = last
        .is_some_and(|previous| now.saturating_duration_since(previous) <= DOUBLE_CTRL_C_WINDOW);
    *last = (!repeated).then_some(now);
    repeated
}

async fn resolve_resume_target(
    sessions_dir: &Path,
    store: &SqliteSessionStore,
    current: Uuid,
    value: &str,
) -> Result<Uuid> {
    let value = value.trim();
    let target = if matches!(value, "--last" | "last") {
        latest_session_id_excluding(sessions_dir, store, current)
            .await?
            .context("there are no other local Borg sessions to resume")?
    } else {
        let target = value
            .parse::<Uuid>()
            .with_context(|| format!("invalid Borg session id: {value}"))?;
        session_id_if_present(sessions_dir, store, target).await?
    };
    anyhow::ensure!(target != current, "that Borg session is already active");
    Ok(target)
}

async fn resolve_resume_switch(
    sessions_dir: &Path,
    store: &SqliteSessionStore,
    current: Uuid,
    value: &str,
    access: LocalSessionAccess,
    status: SessionStatus,
) -> Result<(Uuid, SessionSwitch)> {
    let target = resolve_resume_target(sessions_dir, store, current, value).await?;
    let switch = access.switch(status)?;
    Ok((target, switch))
}

async fn latest_session_id_excluding(
    sessions_dir: &Path,
    store: &SqliteSessionStore,
    current: Uuid,
) -> Result<Option<Uuid>> {
    Ok(recent_session_ids(sessions_dir, store)
        .await?
        .into_iter()
        .find(|session| *session != current))
}

async fn recent_session_ids(sessions_dir: &Path, store: &SqliteSessionStore) -> Result<Vec<Uuid>> {
    fs::create_dir_all(sessions_dir)?;
    let stored = store
        .list_sessions(10_000)
        .await?
        .into_iter()
        .map(|session| session.session_id)
        .collect::<std::collections::HashSet<_>>();
    for entry in fs::read_dir(sessions_dir)?.filter_map(|entry| entry.ok()) {
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("jsonl")
            || entry
                .metadata()
                .map_or(true, |metadata| metadata.len() == 0)
        {
            continue;
        }
        let Some(session_id) = path
            .file_stem()
            .and_then(|value| value.to_str())
            .and_then(|value| value.parse::<Uuid>().ok())
        else {
            continue;
        };
        if !stored.contains(&session_id) {
            let Some(_writer) = SessionWriterLease::try_acquire(&path)? else {
                continue;
            };
            if let Err(error) = store.import_jsonl(&path).await {
                tracing::warn!(
                    %session_id,
                    path = %path.display(),
                    error = %format!("{error:#}"),
                    "skipping incompatible legacy session during discovery"
                );
            }
        }
    }
    Ok(store
        .list_sessions(10_000)
        .await?
        .into_iter()
        .filter(|session| session_has_resumable_activity(&session.state))
        .map(|session| session.session_id)
        .collect())
}

fn session_has_resumable_activity(state: &borg_remote::SessionState) -> bool {
    state.first_prompt.is_some()
        || state.latest_response.is_some()
        || state.provider_session_id.is_some()
        || state.goal.is_some()
        || !state.todos.is_empty()
        || state.usage.calls > 0
        // A prompt, interaction, context reset, or other durable user action
        // advances beyond the launch-only Started/Configured/Ready/Stopped
        // lifecycle emitted by automated terminal probes.
        || state.latest_sequence > 4
}

async fn recent_session_options(
    sessions_dir: &Path,
    store: &SqliteSessionStore,
    current: Uuid,
    current_dir: &Path,
    limit: usize,
) -> Result<Vec<ResumeSessionOption>> {
    let session_ids = recent_session_ids(sessions_dir, store).await?;
    let summaries = store
        .list_sessions(10_000)
        .await?
        .into_iter()
        .map(|summary| (summary.session_id, summary.state))
        .collect::<HashMap<_, _>>();
    let mut current_directory = Vec::new();
    let mut all_directories = Vec::new();
    for id in session_ids
        .into_iter()
        .filter(|session| *session != current)
    {
        let Some(state) = summaries.get(&id) else {
            continue;
        };
        if state
            .configuration
            .as_ref()
            .is_some_and(|configuration| configuration.cwd == current_dir)
        {
            current_directory.push(id);
        } else {
            all_directories.push(id);
        }
    }
    let current_limit =
        if limit >= 2 && !current_directory.is_empty() && !all_directories.is_empty() {
            limit - 1
        } else {
            limit
        };
    let mut selected = current_directory
        .into_iter()
        .take(current_limit)
        .map(|id| (id, true))
        .collect::<Vec<_>>();
    let remaining = limit.saturating_sub(selected.len());
    selected.extend(
        all_directories
            .into_iter()
            .take(remaining)
            .map(|id| (id, false)),
    );

    let mut options = Vec::with_capacity(selected.len());
    for (id, current_directory) in selected {
        let state = &summaries[&id];
        let first_prompt = state
            .first_prompt
            .clone()
            .unwrap_or_else(|| "No user prompt recorded".to_string());
        let last_prompt = state
            .latest_prompt
            .clone()
            .unwrap_or_else(|| first_prompt.clone());
        let latest_response = state.latest_response.clone();
        let primary_preview = latest_response
            .clone()
            .unwrap_or_else(|| last_prompt.clone());
        let (cwd, model) = state
            .configuration
            .as_ref()
            .map(|configuration| {
                (
                    Some(configuration.cwd.display().to_string()),
                    configuration.model.clone(),
                )
            })
            .unwrap_or_default();
        let started_at = state.started_at;
        let updated_at = state.activity_at;
        let timestamp = updated_at
            .or(started_at)
            .map(|time| {
                time.with_timezone(&Local)
                    .format("%b %-d %H:%M")
                    .to_string()
            })
            .unwrap_or_else(|| "Unknown time".to_string());
        let label = [
            Some(timestamp),
            model.clone(),
            Some(prompt_summary(&primary_preview, 56)),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" · ");
        let metadata = vec![
            started_at.map(|time| {
                format!(
                    "**Started:** {}",
                    time.with_timezone(&Local).format("%A, %B %-d at %H:%M")
                )
            }),
            updated_at.map(|time| {
                format!(
                    "**Last activity:** {}",
                    time.with_timezone(&Local).format("%A, %B %-d at %H:%M")
                )
            }),
            cwd.map(|cwd| format!("**Directory:** `{cwd}`")),
            model.map(|model| format!("**Model:** `{model}`")),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        let started_differently = last_prompt != first_prompt;
        let mut preview = primary_preview;
        if !metadata.is_empty() {
            preview.push_str("\n\n---\n");
            preview.push_str(
                &metadata
                    .into_iter()
                    .map(|line| format!("> {line}"))
                    .collect::<Vec<_>>()
                    .join("\n"),
            );
        }
        if latest_response.is_some() {
            preview.push_str(&format!(
                "\n> **Latest prompt:** {}",
                prompt_summary(&last_prompt, 120)
            ));
        } else if started_differently {
            preview.push_str(&format!(
                "\n> **Started with:** {}",
                prompt_summary(&first_prompt, 120)
            ));
        }
        options.push(ResumeSessionOption {
            id,
            label,
            preview,
            current_directory,
        });
    }
    Ok(options)
}

fn prompt_summary(value: &str, limit: usize) -> String {
    let plain = MarkdownParser::new(value)
        .filter_map(|event| match event {
            MarkdownEvent::Text(text)
            | MarkdownEvent::Code(text)
            | MarkdownEvent::InlineMath(text)
            | MarkdownEvent::DisplayMath(text) => Some(text.into_string()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(" ");
    let compact = plain.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() > limit {
        format!("{}…", compact.chars().take(limit).collect::<String>())
    } else {
        compact
    }
}

async fn recent_tui_history(
    store: &dyn SessionStore,
    session_id: Uuid,
    latest_sequence: u64,
) -> Result<Vec<SessionEvent>> {
    let scanned = store
        .events_after(
            session_id,
            recent_tui_history_after(latest_sequence),
            RICH_TUI_HISTORY_BOOTSTRAP_SCAN_LIMIT,
        )
        .await?;
    Ok(select_resume_bootstrap_history(scanned))
}

fn recent_tui_history_after(latest_sequence: u64) -> u64 {
    latest_sequence.saturating_sub(RICH_TUI_HISTORY_BOOTSTRAP_SCAN_LIMIT as u64)
}

fn select_resume_bootstrap_history(events: Vec<SessionEvent>) -> Vec<SessionEvent> {
    if events.len() <= RICH_TUI_HISTORY_EVENT_LIMIT {
        return events;
    }
    let floor = events.len() - RICH_TUI_HISTORY_EVENT_LIMIT;
    let message_indices = events
        .iter()
        .enumerate()
        .filter(|(_, event)| {
            matches!(
                event.kind,
                SessionEventKind::Message {
                    actor: EventActor::User | EventActor::Assistant,
                    status: MessageStatus::Complete | MessageStatus::InProgress,
                    ..
                }
            )
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let retained_messages = message_indices
        .into_iter()
        .rev()
        .take(RICH_TUI_HISTORY_MESSAGE_LIMIT)
        .collect::<HashSet<_>>();

    // Keep a bounded event tail for current lifecycle/tool state and splice in
    // the latest real conversation messages even when a high-volume child or
    // tool stream pushed them outside that tail. A contiguous slice starting
    // at the eighth message can contain thousands of irrelevant events and
    // makes the first render just as unusable as loading the whole history.
    events
        .into_iter()
        .enumerate()
        .filter_map(|(index, event)| {
            (index >= floor || retained_messages.contains(&index)).then_some(event)
        })
        .collect()
}

fn history_page_before(events: &[SessionEvent], latest_sequence: u64) -> u64 {
    events
        .first()
        .map(|event| event.sequence)
        .unwrap_or_else(|| latest_sequence.saturating_add(1))
}

async fn older_tui_history(
    store: &dyn SessionStore,
    session_id: Uuid,
    before_sequence: u64,
) -> Result<Vec<SessionEvent>> {
    let Some(after_sequence) = older_tui_history_after(before_sequence) else {
        return Ok(Vec::new());
    };
    let limit = older_tui_history_limit(before_sequence, after_sequence);
    store.events_after(session_id, after_sequence, limit).await
}

fn older_tui_history_after(before_sequence: u64) -> Option<u64> {
    (before_sequence > 1)
        .then(|| before_sequence.saturating_sub(RICH_TUI_HISTORY_PAGE_SIZE as u64 + 1))
}

fn older_tui_history_limit(before_sequence: u64, after_sequence: u64) -> usize {
    usize::try_from(before_sequence.saturating_sub(after_sequence.saturating_add(1)))
        .unwrap_or(RICH_TUI_HISTORY_PAGE_SIZE)
        .min(RICH_TUI_HISTORY_PAGE_SIZE)
}

fn child_pending_approval_ids(events: &[SessionEvent]) -> HashMap<Uuid, String> {
    let mut pending = HashMap::new();
    for event in events {
        let SessionEventKind::SubagentActivity {
            agent,
            event: Some(child_event),
            ..
        } = &event.kind
        else {
            continue;
        };
        match &child_event.kind {
            SessionEventKind::ApprovalRequested { approval_id, .. } => {
                pending.insert(agent.session_id, approval_id.clone());
            }
            SessionEventKind::ApprovalResolved { approval_id, .. }
                if pending
                    .get(&agent.session_id)
                    .is_some_and(|current| current == approval_id) =>
            {
                pending.remove(&agent.session_id);
            }
            _ => {}
        }
    }
    pending
}

fn latest_subagent_snapshots(events: &[SessionEvent]) -> Vec<SubagentSnapshot> {
    let mut latest = HashMap::new();
    for event in events {
        if let SessionEventKind::SubagentActivity { agent, .. } = &event.kind {
            latest.insert(agent.session_id, agent.clone());
        }
    }
    let mut agents = latest.into_values().collect::<Vec<_>>();
    agents.sort_by(|left, right| left.task_name.cmp(&right.task_name));
    agents
}

fn subagent_state_from_history(
    events: &[SessionEvent],
) -> (Vec<SessionEvent>, Vec<SubagentSnapshot>) {
    let team_history = events
        .iter()
        .filter(|event| matches!(&event.kind, SessionEventKind::SubagentActivity { .. }))
        .cloned()
        .collect::<Vec<_>>();
    let team_snapshots = latest_subagent_snapshots(&team_history);
    (team_history, team_snapshots)
}

fn seed_terminal_subagent_threads(
    terminal: &mut BorgTerminal,
    agents: &[SubagentSnapshot],
    histories: &HashMap<Uuid, Vec<SessionEvent>>,
) {
    terminal.seed_team_roster(agents);
    for (child_id, events) in histories {
        terminal.seed_child_history(*child_id, events);
    }
    terminal.finish_child_history_hydration();
}

async fn load_subagent_thread_state(
    store: &dyn SessionStore,
    sessions_dir: &Path,
    session_id: Uuid,
) -> Result<(
    Vec<SessionEvent>,
    Vec<SubagentSnapshot>,
    HashMap<Uuid, Vec<SessionEvent>>,
)> {
    let team_history = store.recovery(session_id).await?.subagent_events;
    let mut team_snapshots = latest_subagent_snapshots(&team_history);
    reconcile_subagent_snapshots(store, sessions_dir, &mut team_snapshots).await;
    let mut child_histories = HashMap::new();
    for agent in &mut team_snapshots {
        match child_authored_history(store, agent.session_id).await {
            Ok(events) => {
                child_histories.insert(agent.session_id, events);
            }
            Err(store_error) => {
                let legacy_path = sessions_dir
                    .join("subagents")
                    .join(format!("{}.jsonl", agent.session_id));
                if !legacy_path.is_file() {
                    tracing::warn!(
                        child_session_id = %agent.session_id,
                        %store_error,
                        "could not load subagent transcript history"
                    );
                    continue;
                }
                match JsonlSessionStore::open(&legacy_path) {
                    Ok(legacy) => match legacy.read(agent.session_id).await {
                        Ok(events) => {
                            child_histories.insert(agent.session_id, events);
                        }
                        Err(legacy_error) => {
                            tracing::warn!(
                                child_session_id = %agent.session_id,
                                %store_error,
                                %legacy_error,
                                "could not load subagent transcript history"
                            );
                        }
                    },
                    Err(legacy_error) => {
                        tracing::warn!(
                            child_session_id = %agent.session_id,
                            %store_error,
                            %legacy_error,
                            "could not load subagent transcript history"
                        );
                    }
                }
            }
        }
    }
    Ok((team_history, team_snapshots, child_histories))
}

async fn reconcile_subagent_snapshots(
    store: &dyn SessionStore,
    sessions_dir: &Path,
    agents: &mut [SubagentSnapshot],
) {
    for agent in agents {
        // Parent SubagentActivity is a durable mirror, not the child's status
        // authority. A crash can happen after the child journals Stopped but
        // before the parent mirrors it. Resolve the child ledger before first
        // paint so resume can never advertise stale background work.
        if let Ok(state) = store.state(agent.session_id).await {
            reconcile_subagent_snapshot(agent, &state);
        }
        reconcile_dormant_subagent_snapshot(sessions_dir, agent);
    }
}

fn reconcile_subagent_snapshot(agent: &mut SubagentSnapshot, state: &SessionState) {
    if let Some(status) = state.status {
        agent.status = match status {
            SessionStatus::Starting => SubagentStatus::Starting,
            SessionStatus::Running => SubagentStatus::Running,
            SessionStatus::WaitingForApproval => SubagentStatus::WaitingForApproval,
            SessionStatus::Ready | SessionStatus::Completed => SubagentStatus::Ready,
            SessionStatus::Failed => SubagentStatus::Failed,
            SessionStatus::Stopped => SubagentStatus::Stopped,
        };
        agent.detail = state.status_detail.clone();
    }
    if let Some(updated_at) = state.activity_at {
        agent.updated_at = updated_at;
    }
    agent.final_text = state.latest_response.clone();
    agent.usage.input_tokens = state.usage.input_tokens;
    agent.usage.output_tokens = state.usage.output_tokens;
    agent.usage.total_tokens = state.usage.total_tokens;
    agent.usage.cost_microusd = state.usage.cost_microusd;
}

fn reconcile_dormant_subagent_snapshot(sessions_dir: &Path, agent: &mut SubagentSnapshot) {
    if !matches!(
        agent.status,
        SubagentStatus::Starting | SubagentStatus::Running
    ) {
        return;
    }
    let path = sessions_dir
        .join("subagents")
        .join(format!("{}.jsonl", agent.session_id));
    if let Ok(Some(writer)) = SessionWriterLease::try_acquire(&path) {
        drop(writer);
        agent.status = SubagentStatus::Ready;
        agent.detail = Some("Paused with the parent session; follow up to wake".to_string());
    }
}

/// Return only the child's authored events for presentation. Forked session
/// storage can project a director prefix into a child session so the provider
/// retains its execution context, but that prefix must not be shown as if it
/// were the child's conversation. The child composer still receives the
/// director's task through the child session's own initial prompt.
async fn child_authored_history(
    store: &dyn SessionStore,
    child_id: Uuid,
) -> Result<Vec<SessionEvent>> {
    let inherited = store.inherited_event_count(child_id).await?;
    let latest = store.state(child_id).await?.latest_sequence;
    let after = recent_child_history_after(inherited, latest);
    store
        .events_after(child_id, after, RICH_TUI_HISTORY_EVENT_LIMIT)
        .await
}

fn recent_child_history_after(inherited: u64, latest: u64) -> u64 {
    inherited.max(latest.saturating_sub(RICH_TUI_HISTORY_EVENT_LIMIT as u64))
}

async fn recent_sessions_summary(
    sessions_dir: &Path,
    store: &SqliteSessionStore,
    current: Uuid,
    current_dir: &Path,
) -> Result<String> {
    let sessions = recent_session_options(sessions_dir, store, current, current_dir, 8).await?;
    if sessions.is_empty() {
        return Ok(
            "No other saved Borg sessions. Use /resume <session-id> or /resume --last.".to_string(),
        );
    }
    Ok(format!(
        "Recent Borg sessions:\n{}\nUse /resume <session-id> or /resume --last.",
        sessions
            .into_iter()
            .map(|session| format!("  {}", session.label))
            .collect::<Vec<_>>()
            .join("\n")
    ))
}

async fn print_recent_sessions(
    sessions_dir: &Path,
    store: &SqliteSessionStore,
    current: Uuid,
    current_dir: &Path,
) -> Result<()> {
    println!(
        "\n  {}\n",
        recent_sessions_summary(sessions_dir, store, current, current_dir).await?
    );
    Ok(())
}

fn tui_refresh_rate(default: u64) -> u64 {
    std::env::var("BORG_TUI_FPS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
        .clamp(MIN_TUI_FPS, MAX_TUI_FPS)
}

fn tui_frame_interval(fps: u64) -> std::time::Duration {
    std::time::Duration::from_nanos(1_000_000_000 / fps.clamp(MIN_TUI_FPS, MAX_TUI_FPS))
}

fn responsive_tui_frame_interval(
    fps: u64,
    last_draw: std::time::Duration,
    interaction_frame: bool,
) -> std::time::Duration {
    tui_frame_interval(fps)
        .max(if interaction_frame {
            last_draw
        } else {
            last_draw.saturating_mul(3)
        })
        .min(ACTIVITY_FRAME_INTERVAL)
}

fn parse_on_off(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "on" | "true" | "1" => Some(true),
        "off" | "false" | "0" => Some(false),
        _ => None,
    }
}

fn set_transcript_label(
    preferences: &mut EditorPreferences,
    user_label: bool,
    value: &str,
) -> Result<()> {
    let mut candidate = preferences.clone();
    if user_label {
        candidate.transcript.user_label = value.to_string();
    } else {
        candidate.transcript.assistant_label = value.to_string();
    }
    candidate.save()?;
    *preferences = candidate;
    Ok(())
}

fn set_transcript_color(preferences: &mut EditorPreferences, value: &str) -> Result<()> {
    let mut parts = value.split_whitespace();
    let target = parts.next().unwrap_or_default();
    let color = parts.next().unwrap_or_default();
    anyhow::ensure!(
        !target.is_empty() && !color.is_empty() && parts.next().is_none(),
        "Use /color user-label|user-message|assistant-label|assistant-message #RRGGBB"
    );
    let mut candidate = preferences.clone();
    let destination = match target {
        "user-label" => &mut candidate.transcript.user_label_color,
        "user-message" => &mut candidate.transcript.user_message_color,
        "assistant-label" => &mut candidate.transcript.assistant_label_color,
        "assistant-message" => &mut candidate.transcript.assistant_message_color,
        _ => anyhow::bail!(
            "Unknown colour target. Choose user-label, user-message, assistant-label, or assistant-message"
        ),
    };
    *destination = color.to_ascii_lowercase();
    candidate.save()?;
    *preferences = candidate;
    Ok(())
}

fn transcript_colors_summary(preferences: &EditorPreferences) -> String {
    format!(
        "Transcript colours · user label {} · user message {} · assistant label {} · assistant message {} · /color TARGET #RRGGBB",
        preferences.transcript.user_label_color,
        preferences.transcript.user_message_color,
        preferences.transcript.assistant_label_color,
        preferences.transcript.assistant_message_color,
    )
}

fn tui_render_interval(frame_interval: std::time::Duration) -> tokio::time::Interval {
    let mut interval =
        tokio::time::interval_at(tokio::time::Instant::now() + frame_interval, frame_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval
}

fn terminal_needs_activity_tick(
    has_expiring_notice: bool,
    has_blinking_cursor: bool,
    status: SessionStatus,
) -> bool {
    has_expiring_notice
        || has_blinking_cursor
        || matches!(status, SessionStatus::Starting | SessionStatus::Running)
}

fn spawn_terminal_input() -> mpsc::Receiver<io::Result<String>> {
    let (tx, rx) = mpsc::channel(32);
    std::thread::spawn(move || {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            if tx.blocking_send(line).is_err() {
                break;
            }
        }
    });
    rx
}

async fn recv_terminal_line(
    input: &mut Option<mpsc::Receiver<io::Result<String>>>,
) -> Option<io::Result<String>> {
    match input {
        Some(input) => input.recv().await,
        None => None,
    }
}

async fn recv_terminal_event(
    terminal: &mut Option<BorgTerminal>,
) -> Option<io::Result<TerminalInputEvent>> {
    match terminal {
        Some(terminal) => terminal.next_event().await,
        None => None,
    }
}

fn agent_config_file_signature(path: Option<&Path>) -> Option<(std::time::SystemTime, u64)> {
    let metadata = fs::metadata(path?).ok()?;
    Some((metadata.modified().ok()?, metadata.len()))
}

fn idle_input(line: &str) -> (PromptDelivery, String) {
    let line = normalize_consultation_command(line);
    if let Some(text) = line.strip_prefix("/queue ") {
        return (PromptDelivery::Queue, text.trim().to_string());
    }
    if let Some(text) = line.strip_prefix("/steer ") {
        return (PromptDelivery::Steer, text.trim().to_string());
    }
    (PromptDelivery::Steer, line.to_string())
}

fn running_input(
    line: &str,
    provider: CodingProvider,
    steer_active_turn: bool,
    compacting: bool,
) -> (PromptDelivery, String) {
    let line = normalize_consultation_command(line);
    if let Some(text) = line.strip_prefix("/queue ") {
        return (PromptDelivery::Queue, text.trim().to_string());
    }
    if let Some(text) = line.strip_prefix("/steer ") {
        return (
            if matches!(provider, CodingProvider::Codex | CodingProvider::Claude)
                || provider.uses_native_harness()
            {
                PromptDelivery::Steer
            } else {
                PromptDelivery::Queue
            },
            text.trim().to_string(),
        );
    }
    (
        if compacting {
            PromptDelivery::Queue
        } else {
            default_active_delivery(provider, steer_active_turn)
        },
        line.to_string(),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistentSidecar {
    Claude,
    Gpt,
}

impl PersistentSidecar {
    const fn alias(self) -> &'static str {
        match self {
            Self::Claude => "/claude",
            Self::Gpt => "/gpt",
        }
    }

    const fn task_name(self) -> &'static str {
        match self {
            Self::Claude => "/root/claude",
            Self::Gpt => "/root/gpt",
        }
    }

    const fn task_name_argument(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Gpt => "gpt",
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Claude => "Claude",
            Self::Gpt => "GPT",
        }
    }

    const fn provider(self) -> CodingProvider {
        match self {
            Self::Claude => CodingProvider::Claude,
            Self::Gpt => CodingProvider::Codex,
        }
    }

    const fn model(self) -> &'static str {
        match self {
            Self::Claude => "claude-opus-5",
            Self::Gpt => "gpt-5.6-sol",
        }
    }

    const fn effort(self) -> &'static str {
        match self {
            Self::Claude => "high",
            Self::Gpt => "xhigh",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PersistentSidecarIntent {
    Ensure,
    Clear,
    Prompt(String),
}

/// Parse the direct, durable peer lanes. `/ask` remains deliberately outside
/// this parser so it continues to be a one-shot briefing chosen by the main
/// model through `consult_model`.
fn persistent_sidecar_command(line: &str) -> Option<(PersistentSidecar, PersistentSidecarIntent)> {
    let trimmed = line.trim();
    for sidecar in [PersistentSidecar::Claude, PersistentSidecar::Gpt] {
        let alias = sidecar.alias();
        if trimmed == alias {
            return Some((sidecar, PersistentSidecarIntent::Ensure));
        }
        let Some(rest) = trimmed.strip_prefix(alias).filter(|rest| {
            rest.chars()
                .next()
                .is_some_and(|character| character.is_whitespace())
        }) else {
            continue;
        };
        let rest = rest.trim();
        if rest.is_empty() {
            return Some((sidecar, PersistentSidecarIntent::Ensure));
        }
        if matches!(rest.to_ascii_lowercase().as_str(), "clear" | "new") {
            return Some((sidecar, PersistentSidecarIntent::Clear));
        }
        return Some((sidecar, PersistentSidecarIntent::Prompt(rest.to_string())));
    }
    None
}

fn persistent_sidecar_commands(
    session_id: Uuid,
    sidecar: PersistentSidecar,
    intent: &PersistentSidecarIntent,
    message_id: Uuid,
    attachments: &[PathBuf],
    delivery: PromptDelivery,
) -> Vec<HostCommand> {
    let ensure = HostCommand::Subagent {
        session_id,
        action: SubagentAction::Ensure {
            request_id: Uuid::new_v4(),
            task_name: sidecar.task_name_argument().to_string(),
            provider: sidecar.provider(),
            model: Some(sidecar.model().to_string()),
            effort: Some(sidecar.effort().to_string()),
        },
    };
    match intent {
        PersistentSidecarIntent::Ensure => vec![ensure],
        PersistentSidecarIntent::Clear => vec![
            ensure,
            HostCommand::Subagent {
                session_id,
                action: SubagentAction::ClearContext {
                    request_id: Uuid::new_v4(),
                    target: sidecar.task_name().to_string(),
                },
            },
        ],
        PersistentSidecarIntent::Prompt(prompt) => vec![
            ensure,
            HostCommand::Subagent {
                session_id,
                action: SubagentAction::Prompt {
                    request_id: Uuid::new_v4(),
                    target: sidecar.task_name().to_string(),
                    message_id,
                    text: prompt.clone(),
                    attachments: attachments.to_vec(),
                    delivery,
                },
            },
        ],
    }
}

fn sidecar_notice(sidecar: PersistentSidecar, intent: &PersistentSidecarIntent) -> String {
    match intent {
        PersistentSidecarIntent::Ensure => format!("{} sidecar ready", sidecar.label()),
        PersistentSidecarIntent::Clear => format!("Clearing {} sidecar context", sidecar.label()),
        PersistentSidecarIntent::Prompt(_) => format!("Sending to {} sidecar", sidecar.label()),
    }
}

/// `/codex` remains a compatibility alias for the one-shot GPT consultation.
/// Persistent lanes are intentionally handled before this provider-neutral
/// normalizer.
pub(crate) fn normalize_consultation_command(line: &str) -> String {
    let trimmed = line.trim();
    for (alias, profile) in [("/codex", "gpt-5.6-sol@xhigh")] {
        if trimmed == alias {
            return format!("/ask {profile}");
        }
        if let Some(request) = trimmed.strip_prefix(alias).filter(|rest| {
            rest.chars()
                .next()
                .is_some_and(|character| character.is_whitespace())
        }) {
            return format!("/ask {profile}{}", request);
        }
    }
    line.to_string()
}

fn status_is_compacting(detail: Option<&str>) -> bool {
    detail.is_some_and(|detail| detail.to_ascii_lowercase().contains("compact"))
}

fn default_active_delivery(provider: CodingProvider, steer_active_turn: bool) -> PromptDelivery {
    if steer_active_turn
        && (matches!(provider, CodingProvider::Codex | CodingProvider::Claude)
            || provider.uses_native_harness())
    {
        PromptDelivery::Steer
    } else {
        PromptDelivery::Queue
    }
}

async fn session_id_if_present(
    sessions_dir: &Path,
    store: &SqliteSessionStore,
    session_id: Uuid,
) -> Result<Uuid> {
    anyhow::ensure!(
        store.contains_session(session_id).await?
            || sessions_dir.join(format!("{session_id}.jsonl")).is_file(),
        "local Borg session {session_id} does not exist"
    );
    Ok(session_id)
}

async fn latest_session_id(
    sessions_dir: &Path,
    store: &SqliteSessionStore,
) -> Result<Option<Uuid>> {
    Ok(recent_session_ids(sessions_dir, store)
        .await?
        .into_iter()
        .next())
}

fn print_history(events: &[SessionEvent]) {
    let mut order = Vec::new();
    let mut messages = HashMap::new();
    for event in events {
        if let SessionEventKind::Message {
            message_id,
            actor,
            text,
            ..
        } = &event.kind
        {
            if *actor == EventActor::System {
                continue;
            }
            if !messages.contains_key(message_id) {
                order.push(*message_id);
            }
            messages.insert(*message_id, (*actor, text.clone()));
        }
    }
    for message_id in order {
        let Some((actor, text)) = messages.remove(&message_id) else {
            continue;
        };
        match actor {
            EventActor::User => println!("\n› {text}"),
            EventActor::Assistant => println!("\n{text}"),
            _ => {}
        }
    }
}

fn print_agent_help() {
    println!(
        r#"
  /settings         show interactive settings
  /ask PROFILE TEXT ask another model for a second opinion
  /claude TEXT      persistent Claude Opus 5 sidecar (use /claude clear to reset)
  /gpt TEXT         persistent GPT 5.6 Sol sidecar (use /gpt clear to reset)
  /model            choose the model
  /effort           choose reasoning effort
  /followups        choose steer current turn or queue next turn
  /refresh          choose terminal refresh rate
  /sleep            prevent sleep during active turns
  /colors           show transcript colours
  /color TARGET HEX set a transcript colour
  /usage            show real Codex weekly limit and session tokens
  /clear            clear the conversation context
  /compact          compact the current provider context
  /goal             show the durable session goal
  /goal OBJECTIVE   set the goal and begin working
  /goal pause       pause automatic continuation
  /goal resume      resume automatic continuation
  /goal clear       clear the goal
  /todo             show the durable todo list
  /todo add TEXT    append a pending item
  /todo start ID    mark one item in progress
  /todo done ID     complete an item
  /todo remove ID   remove an item
  /todo clear       clear the list
  /login            reconnect the current provider
  /remote           connect this machine to your Borg account
  /queue TEXT       run after the current turn
  /steer TEXT       steer Codex now; queues on other providers
  /interrupt        stop the current turn and pause its goal
  /quit             end the session
"#
    );
}

pub(crate) fn parse_goal_action(line: &str) -> Result<GoalAction> {
    let value = line
        .strip_prefix("/goal ")
        .context("usage: /goal [OBJECTIVE|pause|resume|clear]")?
        .trim();
    match value {
        "pause" => return Ok(GoalAction::Pause),
        "resume" => return Ok(GoalAction::Resume),
        "clear" => return Ok(GoalAction::Clear),
        "" | "view" => anyhow::bail!("usage: /goal [OBJECTIVE|pause|resume|clear]"),
        _ => {}
    }
    let value = value.strip_prefix("set ").unwrap_or(value).trim();
    let (token_budget, objective) = if let Some(rest) = value.strip_prefix("--tokens ") {
        let (budget, objective) = rest
            .split_once(char::is_whitespace)
            .context("usage: /goal set --tokens NUMBER OBJECTIVE")?;
        let budget = budget
            .parse::<u64>()
            .context("goal token budget must be a positive integer")?;
        anyhow::ensure!(budget > 0, "goal token budget must be positive");
        (Some(budget), objective.trim())
    } else {
        (None, value)
    };
    anyhow::ensure!(!objective.is_empty(), "goal objective must not be empty");
    Ok(GoalAction::Set {
        objective: objective.to_string(),
        token_budget,
    })
}

#[derive(Default)]
struct SessionUsage {
    calls: u64,
    input_tokens: u64,
    output_tokens: u64,
    cached_input_tokens: u64,
    cost_usd: Option<f64>,
}

impl SessionUsage {
    fn from_projection(projected: &borg_remote::SessionUsage) -> Self {
        Self {
            calls: projected.calls,
            input_tokens: projected.input_tokens,
            output_tokens: projected.output_tokens,
            cached_input_tokens: projected.cached_input_tokens,
            cost_usd: projected.cost_usd,
        }
    }

    fn add(&mut self, input: u64, output: u64, cached: u64, cost: Option<f64>) {
        self.calls += 1;
        self.input_tokens += input;
        self.output_tokens += output;
        self.cached_input_tokens += cached;
        if let Some(cost) = cost {
            self.cost_usd = Some(self.cost_usd.unwrap_or_default() + cost);
        }
    }

    fn summary(&self) -> String {
        let mut summary = format!(
            "Session · {} calls · {} input · {} cached · {} output",
            self.calls, self.input_tokens, self.cached_input_tokens, self.output_tokens
        );
        if let Some(cost) = self.cost_usd {
            summary.push_str(&format!(" · ${cost:.4}"));
        }
        summary
    }
}

async fn usage_summary(provider: CodingProvider, session: &SessionUsage) -> String {
    let session = session.summary();
    if provider != CodingProvider::Codex {
        return session;
    }
    match codex_weekly_usage_summary().await {
        Ok(account) => format!("{account}\n{session}"),
        Err(error) => format!(
            "Codex weekly · unavailable: {}\n{session}",
            error.root_cause()
        ),
    }
}

async fn codex_weekly_usage_summary() -> Result<String> {
    let usage = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        tokio::task::spawn_blocking(|| {
            let mut client = borg_provider::provider::CodexAppServerClient::start(
                false,
                false,
                None,
                false,
                &[],
            )?;
            client.account_weekly_usage()
        }),
    )
    .await
    .context("Codex account usage request timed out")?
    .context("Codex account usage task failed")??;
    let reset = usage
        .resets_at
        .and_then(|timestamp| chrono::DateTime::<Utc>::from_timestamp(timestamp, 0))
        .map(|timestamp| {
            format!(
                " · resets {}",
                timestamp.with_timezone(&Local).format("%a %-d %b %H:%M %Z")
            )
        })
        .unwrap_or_default();
    Ok(format!(
        "Codex weekly · {}% left{reset}",
        usage.remaining_percent()
    ))
}

fn parse_todo_action(line: &str, items: &[PlanItem]) -> Result<TodoAction> {
    let value = line
        .strip_prefix("/todo ")
        .or_else(|| line.strip_prefix("/todos "))
        .context("usage: /todo [add|start|done|pending|remove|clear]")?
        .trim();
    if value == "clear" {
        return Ok(TodoAction::Clear);
    }
    let (command, argument) = value
        .split_once(char::is_whitespace)
        .context("usage: /todo [add TEXT|start ID|done ID|pending ID|remove ID|clear]")?;
    let argument = argument.trim();
    anyhow::ensure!(!argument.is_empty(), "todo command requires a value");
    match command {
        "add" => Ok(TodoAction::Add {
            content: argument.to_string(),
        }),
        "start" => Ok(TodoAction::SetStatus {
            id: resolve_todo_id(items, argument)?,
            status: PlanItemStatus::InProgress,
        }),
        "done" | "complete" => Ok(TodoAction::SetStatus {
            id: resolve_todo_id(items, argument)?,
            status: PlanItemStatus::Completed,
        }),
        "pending" | "reset" => Ok(TodoAction::SetStatus {
            id: resolve_todo_id(items, argument)?,
            status: PlanItemStatus::Pending,
        }),
        "remove" | "rm" => Ok(TodoAction::Remove {
            id: resolve_todo_id(items, argument)?,
        }),
        _ => anyhow::bail!("usage: /todo [add TEXT|start ID|done ID|pending ID|remove ID|clear]"),
    }
}

fn resolve_todo_id(items: &[PlanItem], value: &str) -> Result<Uuid> {
    let normalized = value.to_ascii_lowercase();
    let matches = items
        .iter()
        .filter(|item| item.id.to_string().starts_with(&normalized))
        .map(|item| item.id)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [id] => Ok(*id),
        [] => anyhow::bail!("no todo item matches ID {value}"),
        _ => anyhow::bail!("todo ID prefix {value} is ambiguous"),
    }
}

fn print_todos(items: &[PlanItem]) {
    if items.is_empty() {
        println!("\n  No todo items. Use /todo add TEXT.\n");
        return;
    }
    println!("\n  Todos");
    for item in items {
        let marker = match item.status {
            PlanItemStatus::Pending => "○",
            PlanItemStatus::InProgress => "◉",
            PlanItemStatus::Completed => "●",
        };
        println!("  {marker} {}  {}", &item.id.to_string()[..8], item.content);
    }
    println!();
}

fn print_goal(goal: Option<&SessionGoal>) {
    let Some(goal) = goal else {
        println!("\n  No goal is set. Use /goal OBJECTIVE to start one.\n");
        return;
    };
    println!(
        "\n  Goal\n  Status: {}\n  Objective: {}\n  Time used: {}\n  Tokens used: {}{}",
        goal_status_label(goal.status),
        goal.objective,
        format_goal_time(live_goal_time_seconds(goal)),
        goal.tokens_used,
        goal.token_budget
            .map(|budget| format!(" / {budget}"))
            .unwrap_or_default(),
    );
    let commands = match goal.status {
        GoalStatus::Active => "/goal pause · /goal clear",
        GoalStatus::Paused | GoalStatus::Blocked | GoalStatus::UsageLimited => {
            "/goal resume · /goal clear"
        }
        GoalStatus::BudgetLimited | GoalStatus::Complete => "/goal OBJECTIVE · /goal clear",
    };
    println!("  Commands: {commands}\n");
}

fn goal_status_label(status: GoalStatus) -> &'static str {
    match status {
        GoalStatus::Active => "active",
        GoalStatus::Paused => "paused",
        GoalStatus::Blocked => "blocked",
        GoalStatus::UsageLimited => "usage limited",
        GoalStatus::BudgetLimited => "limited by budget",
        GoalStatus::Complete => "complete",
    }
}

fn format_goal_time(seconds: u64) -> String {
    let days = seconds / 86_400;
    let hours = seconds % 86_400 / 3_600;
    let minutes = seconds % 3_600 / 60;
    let seconds = seconds % 60;
    let mut parts = Vec::new();
    if days > 0 {
        parts.push(format!("{days}d"));
    }
    if hours > 0 || days > 0 {
        parts.push(format!("{hours}h"));
    }
    if minutes > 0 || hours > 0 || days > 0 {
        parts.push(format!("{minutes}m"));
    }
    parts.push(format!("{seconds}s"));
    parts.join(" ")
}

fn live_goal_time_seconds(goal: &SessionGoal) -> u64 {
    if !goal.status.is_active() {
        return goal.time_used_seconds;
    }
    goal.time_used_seconds.saturating_add(
        Utc::now()
            .signed_duration_since(goal.updated_at)
            .num_seconds()
            .max(0) as u64,
    )
}

fn provider_name(provider: CodingProvider) -> &'static str {
    match provider {
        CodingProvider::Codex => "codex",
        CodingProvider::Claude => "claude",
        CodingProvider::OpenCode => "opencode",
        CodingProvider::Kimi => "kimi",
        CodingProvider::OpenRouter => "openrouter",
        CodingProvider::OpenAiCompatible => "openai-compatible",
    }
}

fn permission_name(permission: PermissionMode) -> &'static str {
    match permission {
        PermissionMode::FullAccess => "full-access",
        PermissionMode::Auto => "auto",
        PermissionMode::Manual => "manual",
    }
}

fn live_extension_summary(
    catalog: &crate::extensions::ExtensionCatalog,
    rejected_reload: Option<&str>,
) -> String {
    let mut lines = Vec::new();
    if catalog.extensions.is_empty() {
        lines.push("No Blu extensions installed.".to_string());
    } else {
        for extension in &catalog.extensions {
            let state = if extension.active {
                "active"
            } else {
                extension.reason.as_deref().unwrap_or("inactive")
            };
            lines.push(format!(
                "{} {} · {} · {}",
                extension.id,
                extension.version,
                extension.scope.label(),
                state,
            ));
        }
    }
    if catalog.has_errors() {
        lines.push(format!(
            "{} invalid package{} isolated · run `borg extensions doctor`",
            catalog.error_count(),
            if catalog.error_count() == 1 { "" } else { "s" },
        ));
    }
    if let Some(error) = rejected_reload {
        lines.push(format!(
            "Last reload rejected; the running revision is unchanged: {error}"
        ));
    }
    lines.push(format!(
        "{} active · revision {} · changes apply at the next turn boundary",
        catalog
            .extensions
            .iter()
            .filter(|extension| extension.active)
            .count(),
        &catalog.revision[..catalog.revision.len().min(12)],
    ));
    lines.join("\n")
}

fn lsp_support_summary() -> String {
    let servers = borg_remote::LspService::supported_status();
    let mut available = Vec::new();
    let mut missing = Vec::new();
    for server in servers.as_array().into_iter().flatten() {
        let language = server
            .get("language")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let command = server
            .get("command")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("language server");
        let extensions = server
            .get("extensions")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str)
            .collect::<Vec<_>>()
            .join(", ");
        let label = format!("{language} ({extensions}) · {command}");
        if server
            .get("available")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            available.push(label);
        } else {
            missing.push(label);
        }
    }
    let available = if available.is_empty() {
        "none detected".to_string()
    } else {
        available.join("; ")
    };
    let missing = if missing.is_empty() {
        "none".to_string()
    } else {
        missing.join("; ")
    };
    format!(
        "Language servers · available: {available}\nInstall on PATH to enable: {missing}\nServers start lazily when Borg inspects a matching source file."
    )
}

fn render_event(
    event: &SessionEvent,
    json: bool,
    rendered: &mut HashMap<Uuid, String>,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(event)?);
        return Ok(());
    }
    match &event.kind {
        SessionEventKind::Message {
            message_id,
            actor: EventActor::Assistant,
            text,
            status,
            ..
        } => {
            let prior = rendered.entry(*message_id).or_default();
            let normalized_prior = prior.trim_start();
            let normalized_text = text.trim_start();
            if *status == MessageStatus::Complete
                && !normalized_prior.is_empty()
                && normalized_text.starts_with(normalized_prior)
            {
                // Codex normalizes leading whitespace when it publishes the
                // completed item. Only render text not already streamed.
                print!("{}", &normalized_text[normalized_prior.len()..]);
                io::stdout().flush()?;
            } else if let Some(delta) = text.strip_prefix(prior.as_str()) {
                print!("{delta}");
                io::stdout().flush()?;
            } else {
                print!("\n{text}");
            }
            *prior = text.clone();
        }
        SessionEventKind::ToolStarted { name, input, .. } => {
            println!("\n  ↳ {name} {}", compact_json(input));
        }
        SessionEventKind::ToolCompleted {
            is_error: true,
            output,
            ..
        } => println!("  ! {}", output.lines().next().unwrap_or("tool failed")),
        SessionEventKind::ApprovalRequested {
            title,
            detail,
            command,
            ..
        } => {
            println!("\n  ? {title}\n    {detail}");
            if let Some(command) = command {
                println!("    {command}");
            }
        }
        SessionEventKind::ProviderInteractionRequested {
            title,
            detail,
            payload,
            ..
        } => {
            println!("\n  ? {title}\n    {detail}");
            let options = payload
                .get("questions")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .flat_map(|question| {
                    question
                        .get("options")
                        .and_then(serde_json::Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(|option| {
                            option.get("label").and_then(serde_json::Value::as_str)
                        })
                })
                .collect::<Vec<_>>();
            if !options.is_empty() {
                println!("    Options: {}", options.join(" · "));
            }
        }
        SessionEventKind::GoalUpdated { goal } => {
            println!(
                "\n  Goal {} · {} tokens · {}",
                goal_status_label(goal.status),
                goal.tokens_used,
                format_goal_time(live_goal_time_seconds(goal)),
            );
        }
        SessionEventKind::GoalCleared { .. } => println!("\n  Goal cleared"),
        SessionEventKind::ContextCleared => {
            print!("\x1b[2J\x1b[H");
            println!("  Conversation context cleared");
            io::stdout().flush()?;
        }
        SessionEventKind::PlanUpdated { items } => {
            let completed = items
                .iter()
                .filter(|item| item.status == PlanItemStatus::Completed)
                .count();
            println!("\n  Todos {completed}/{} complete", items.len());
        }
        SessionEventKind::SubagentActivity {
            activity,
            agent,
            event: child_event,
        } => {
            if let Some(summary) = crate::terminal_ui::subagent_activity_summary(
                *activity,
                agent,
                child_event.as_deref(),
            ) {
                println!("\n  {summary}");
            }
        }
        SessionEventKind::Error { message } => eprintln!("\n  Error: {message}"),
        _ => {}
    }
    Ok(())
}

fn remote_command_name(command: &HostCommand) -> &'static str {
    match command {
        HostCommand::Launch { .. } => "launch",
        HostCommand::Prompt { delivery, .. } => match delivery {
            PromptDelivery::Steer => "steer",
            PromptDelivery::Queue => "queue",
        },
        HostCommand::RecallQueuedPrompt { .. } => "recall queued prompt",
        HostCommand::Configure { .. } => "configure",
        HostCommand::Approve { .. } => "approval",
        HostCommand::RespondToProviderInteraction { .. } => "provider interaction response",
        HostCommand::Goal { .. } => "goal",
        HostCommand::Todo { .. } => "todo",
        HostCommand::Subagent { .. } => "subagent",
        HostCommand::Interrupt { .. } => "interrupt",
        HostCommand::Compact { .. } => "compact",
        HostCommand::ClearContext { .. } => "clear context",
        HostCommand::Stop { .. } => "stop",
        HostCommand::WorkspaceFilesystem { .. } => "workspace filesystem",
        HostCommand::CancelWorkspaceFilesystem { .. } => "cancel workspace filesystem",
        HostCommand::WorkspaceCommand { .. } => "workspace command",
        HostCommand::CancelWorkspaceCommand { .. } => "cancel workspace command",
    }
}

fn compact_json(value: &serde_json::Value) -> String {
    let value = value.to_string();
    if value.chars().count() > 120 {
        format!("{}…", value.chars().take(120).collect::<String>())
    } else {
        value
    }
}

fn cancelled_provider_interaction_response(kind: &str) -> serde_json::Value {
    if kind == "mcp_elicitation" {
        serde_json::json!({ "action": "cancel" })
    } else {
        serde_json::json!({ "answers": {} })
    }
}

fn provider_interaction_payload_contains_secret(payload: &serde_json::Value) -> bool {
    payload
        .get("questions")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|questions| {
            questions.iter().any(|question| {
                question
                    .get("isSecret")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false)
            })
        })
}

fn provider_interaction_response(
    kind: &str,
    payload: &serde_json::Value,
    input: &str,
) -> Result<serde_json::Value> {
    if input.eq_ignore_ascii_case("/cancel") {
        return Ok(cancelled_provider_interaction_response(kind));
    }
    if kind == "mcp_elicitation" {
        if input.eq_ignore_ascii_case("/decline") {
            return Ok(serde_json::json!({ "action": "decline" }));
        }
        let content = match serde_json::from_str::<serde_json::Value>(input) {
            Ok(value) => value,
            Err(_) => {
                let properties = payload
                    .get("requestedSchema")
                    .and_then(|schema| schema.get("properties"))
                    .and_then(serde_json::Value::as_object)
                    .context(
                        "Enter JSON matching the requested form, or use /decline or /cancel",
                    )?;
                anyhow::ensure!(
                    properties.len() == 1,
                    "Enter a JSON object matching the requested form, or use /decline or /cancel"
                );
                let key = properties.keys().next().expect("one property");
                serde_json::json!({ key: input })
            }
        };
        return Ok(serde_json::json!({ "action": "accept", "content": content }));
    }

    let questions = payload
        .get("questions")
        .and_then(serde_json::Value::as_array)
        .context("Provider user-input request did not include questions")?;
    if questions.len() == 1 {
        let id = questions[0]
            .get("id")
            .and_then(serde_json::Value::as_str)
            .context("Provider user-input question did not include an id")?;
        return Ok(serde_json::json!({
            "answers": {
                (id): { "answers": [input] }
            }
        }));
    }

    let value: serde_json::Value = serde_json::from_str(input).context(
        "Answer multiple questions with a JSON object keyed by question id, or use /cancel",
    )?;
    if value.get("answers").is_some() {
        return Ok(value);
    }
    let values = value
        .as_object()
        .context("Multiple answers must be a JSON object keyed by question id")?;
    let mut answers = serde_json::Map::new();
    for question in questions {
        let id = question
            .get("id")
            .and_then(serde_json::Value::as_str)
            .context("Provider user-input question did not include an id")?;
        let answer = values
            .get(id)
            .with_context(|| format!("Missing answer for question {id}"))?;
        let answer_values = match answer {
            serde_json::Value::Array(values) => values.clone(),
            value => vec![value.clone()],
        };
        anyhow::ensure!(
            answer_values.iter().all(serde_json::Value::is_string),
            "Answer for {id} must be a string or an array of strings"
        );
        answers.insert(
            id.to_string(),
            serde_json::json!({ "answers": answer_values }),
        );
    }
    Ok(serde_json::json!({ "answers": answers }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};
    use tempfile::tempdir;
    use tokio::io::AsyncReadExt;

    #[test]
    fn persistent_sidecar_aliases_keep_their_durable_lane_identity() {
        assert_eq!(
            persistent_sidecar_command("/claude review this"),
            Some((
                PersistentSidecar::Claude,
                PersistentSidecarIntent::Prompt("review this".to_string())
            ))
        );
        assert_eq!(
            persistent_sidecar_command("/gpt clear"),
            Some((PersistentSidecar::Gpt, PersistentSidecarIntent::Clear))
        );
        assert_eq!(
            persistent_sidecar_command("/claude"),
            Some((PersistentSidecar::Claude, PersistentSidecarIntent::Ensure))
        );
        assert_eq!(persistent_sidecar_command("/claudette review"), None);
    }

    #[test]
    fn consultation_aliases_leave_persistent_lanes_alone() {
        assert_eq!(
            normalize_consultation_command("/claude review this"),
            "/claude review this"
        );
        assert_eq!(
            normalize_consultation_command("/gpt compare these approaches"),
            "/gpt compare these approaches"
        );
        assert_eq!(
            normalize_consultation_command("/codex check the design"),
            "/ask gpt-5.6-sol@xhigh check the design"
        );
        assert_eq!(
            normalize_consultation_command("/ask claude review"),
            "/ask claude review"
        );
        assert_eq!(normalize_consultation_command("/claudette"), "/claudette");
    }

    #[test]
    fn persistent_sidecar_prompt_is_ensured_then_sent_to_the_same_child() {
        let session_id = Uuid::new_v4();
        let commands = persistent_sidecar_commands(
            session_id,
            PersistentSidecar::Claude,
            &PersistentSidecarIntent::Prompt("review the design".to_string()),
            Uuid::nil(),
            &[],
            PromptDelivery::Steer,
        );
        assert_eq!(commands.len(), 2);
        assert!(matches!(
            &commands[0],
            HostCommand::Subagent {
                action: SubagentAction::Ensure {
                    task_name,
                    provider: CodingProvider::Claude,
                    model: Some(model),
                    effort: Some(effort),
                    ..
                },
                ..
            } if task_name == "claude" && model == "claude-opus-5" && effort == "high"
        ));
        assert!(matches!(
            &commands[1],
            HostCommand::Subagent {
                action: SubagentAction::Prompt {
                    target,
                    text,
                    delivery: PromptDelivery::Steer,
                    ..
                },
                ..
            } if target == "/root/claude" && text == "review the design"
        ));
    }

    #[test]
    fn older_history_pages_end_immediately_before_the_loaded_tail() {
        assert_eq!(older_tui_history_after(1), None);
        assert_eq!(older_tui_history_after(513), Some(0));
        assert_eq!(older_tui_history_limit(513, 0), 512);
        assert_eq!(older_tui_history_after(2_000), Some(1_487));
        assert_eq!(older_tui_history_limit(2_000, 1_487), 512);
        assert_eq!(1_487 + RICH_TUI_HISTORY_PAGE_SIZE as u64, 1_999);
        assert_eq!(older_tui_history_after(37), Some(0));
        assert_eq!(older_tui_history_limit(37, 0), 36);
    }

    #[test]
    fn first_resume_scan_is_bounded_before_history_is_selected() {
        assert_eq!(recent_tui_history_after(10), 0);
        assert_eq!(recent_tui_history_after(100), 0);
        assert_eq!(recent_tui_history_after(60_000), 59_488);
    }

    #[test]
    fn extensions_view_keeps_a_rejected_reload_visible() {
        let catalog = crate::extensions::ExtensionCatalog {
            revision: "0123456789abcdef".to_string(),
            load_order: Vec::new(),
            extensions: Vec::new(),
            diagnostics: Vec::new(),
        };

        let summary = live_extension_summary(
            &catalog,
            Some("1 diagnostic rejected the reload; first: unknown field"),
        );

        assert!(summary.contains("No Blu extensions installed."));
        assert!(summary.contains(
            "Last reload rejected; the running revision is unchanged: 1 diagnostic rejected the reload; first: unknown field"
        ));
        assert!(summary.contains("revision 0123456789ab"));
    }

    #[test]
    fn first_resume_frame_keeps_real_recent_conversation_before_lazy_pages() {
        let session_id = Uuid::new_v4();
        let mut events = (1..=200)
            .map(|sequence| {
                SessionEvent::new(
                    session_id,
                    sequence,
                    SessionEventKind::UsageUpdated {
                        provider_duration_ms: 0,
                        input_tokens: 1,
                        output_tokens: 0,
                        total_tokens: 1,
                        cached_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        cost_usd: None,
                        cost_microusd: None,
                        cost_basis: String::new(),
                        context_tokens: None,
                        context_window_tokens: None,
                    },
                )
            })
            .collect::<Vec<_>>();
        events[20] = SessionEvent::new(
            session_id,
            21,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: "meaningful resume context".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        );

        let selected = select_resume_bootstrap_history(events);
        assert_eq!(selected.first().unwrap().sequence, 21);
        assert_eq!(selected.len(), RICH_TUI_HISTORY_EVENT_LIMIT + 1);
        assert!(selected.iter().any(|event| matches!(
            &event.kind,
            SessionEventKind::Message { text, .. } if text == "meaningful resume context"
        )));
    }

    #[test]
    fn history_reprojection_uses_delivered_durable_and_live_projection() {
        let session_id = Uuid::new_v4();
        let ready = SessionState {
            latest_sequence: 4,
            status: Some(SessionStatus::Ready),
            ..SessionState::default()
        };
        let stale_store_snapshot = ready.clone();
        let mut delivered = DeliveredSessionProjection::new(ready);

        delivered
            .observe(&SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ContextWindowUpdated {
                    context_tokens: 80,
                    context_window_tokens: 100,
                },
            ))
            .unwrap();
        assert_eq!(delivered.state().latest_sequence, 4);
        assert_eq!(delivered.state().usage.context_tokens, Some(80));
        assert_eq!(delivered.state().usage.context_window_tokens, Some(100));

        delivered
            .observe(&SessionEvent::new(
                session_id,
                5,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Running,
                    detail: None,
                },
            ))
            .unwrap();

        assert_eq!(stale_store_snapshot.status, Some(SessionStatus::Ready));
        assert_eq!(delivered.state().latest_sequence, 5);
        assert_eq!(delivered.state().status, Some(SessionStatus::Running));
    }

    #[test]
    fn child_history_tail_stays_bounded_after_long_runs_and_forks() {
        assert_eq!(recent_child_history_after(0, 100), 0);
        assert_eq!(recent_child_history_after(0, 60_000), 59_872);
        assert_eq!(recent_child_history_after(59_980, 60_000), 59_980);
    }

    #[test]
    fn initial_subagent_state_uses_only_the_loaded_root_tail() {
        let root = Uuid::new_v4();
        let child = Uuid::new_v4();
        let now = Utc::now();
        let snapshot = SubagentSnapshot {
            session_id: child,
            parent_session_id: root,
            task_name: "/root/worker".to_string(),
            status: borg_remote::SubagentStatus::Ready,
            provider: CodingProvider::Codex,
            model: Some("gpt-test".to_string()),
            effort: Some("low".to_string()),
            cwd: PathBuf::from("/workspace"),
            created_at: now,
            updated_at: now,
            detail: None,
            final_text: None,
            usage: borg_remote::SubagentUsage::default(),
        };
        let events = vec![
            SessionEvent::new(root, 1, SessionEventKind::SessionStarted),
            SessionEvent::new(
                root,
                2,
                SessionEventKind::SubagentActivity {
                    activity: borg_remote::SubagentActivityKind::Completed,
                    agent: snapshot,
                    event: None,
                },
            ),
        ];

        let (team_history, team_snapshots) = subagent_state_from_history(&events);

        assert_eq!(team_history.len(), 1);
        assert_eq!(team_snapshots.len(), 1);
        assert_eq!(team_snapshots[0].session_id, child);
    }

    #[test]
    fn resumed_roster_never_claims_an_unowned_child_is_running() {
        let directory = tempdir().unwrap();
        let root = Uuid::new_v4();
        let child = Uuid::new_v4();
        let now = Utc::now();
        let mut snapshot = SubagentSnapshot {
            session_id: child,
            parent_session_id: root,
            task_name: "/root/worker".to_string(),
            status: SubagentStatus::Running,
            provider: CodingProvider::Codex,
            model: Some("gpt-test".to_string()),
            effort: Some("low".to_string()),
            cwd: PathBuf::from("/workspace"),
            created_at: now,
            updated_at: now,
            detail: None,
            final_text: None,
            usage: borg_remote::SubagentUsage::default(),
        };

        reconcile_dormant_subagent_snapshot(directory.path(), &mut snapshot);
        assert_eq!(snapshot.status, SubagentStatus::Ready);
        assert!(
            snapshot
                .detail
                .as_deref()
                .unwrap()
                .contains("follow up to wake")
        );

        let child_path = directory
            .path()
            .join("subagents")
            .join(format!("{child}.jsonl"));
        let _writer = SessionWriterLease::try_acquire(&child_path)
            .unwrap()
            .expect("test owns child");
        snapshot.status = SubagentStatus::Running;
        snapshot.detail = None;
        reconcile_dormant_subagent_snapshot(directory.path(), &mut snapshot);
        assert_eq!(snapshot.status, SubagentStatus::Running);
    }

    #[tokio::test]
    async fn resumed_roster_prefers_the_child_terminal_ledger_over_a_stale_parent_mirror() {
        let directory = tempdir().unwrap();
        let root = Uuid::new_v4();
        let child = Uuid::new_v4();
        let now = Utc::now();
        let store = SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .unwrap();
        store.create_session(child).await.unwrap();
        store
            .append(SessionEvent::new(
                child,
                0,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Stopped,
                    detail: Some("crash cleanup completed".to_string()),
                },
            ))
            .await
            .unwrap();
        let mut snapshots = vec![SubagentSnapshot {
            session_id: child,
            parent_session_id: root,
            task_name: "/root/worker".to_string(),
            status: SubagentStatus::Running,
            provider: CodingProvider::Codex,
            model: Some("gpt-test".to_string()),
            effort: Some("low".to_string()),
            cwd: PathBuf::from("/workspace"),
            created_at: now,
            updated_at: now,
            detail: Some("turn phase: provider active".to_string()),
            final_text: None,
            usage: borg_remote::SubagentUsage::default(),
        }];

        reconcile_subagent_snapshots(&store, directory.path(), &mut snapshots).await;

        assert_eq!(snapshots[0].status, SubagentStatus::Stopped);
        assert_eq!(
            snapshots[0].detail.as_deref(),
            Some("crash cleanup completed")
        );
    }

    #[tokio::test]
    async fn child_history_excludes_fork_inherited_director_events() {
        let directory = tempdir().expect("tempdir");
        let store = SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .expect("session store");
        let parent = Uuid::new_v4();
        let child = Uuid::new_v4();
        store.create_session(parent).await.expect("parent");
        store
            .append(SessionEvent::new(
                parent,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
            .expect("director event");
        store.fork_before(parent, child, 2).await.expect("fork");
        store
            .append(SessionEvent::new(
                child,
                0,
                SessionEventKind::Error {
                    message: "child authored event".to_string(),
                },
            ))
            .await
            .expect("child event");

        let history = child_authored_history(&store, child)
            .await
            .expect("authored history");
        assert_eq!(history.len(), 1);
        assert!(matches!(
            history[0].kind,
            SessionEventKind::Error { ref message } if message == "child authored event"
        ));
    }

    #[test]
    fn reinstalling_the_remote_host_restarts_it_on_the_new_binary() {
        assert_eq!(
            host_service_systemctl_commands(),
            [
                &["--user", "daemon-reload"][..],
                &["--user", "enable", "borg-remote.service"][..],
                &["--user", "restart", "borg-remote.service"][..],
            ]
        );
    }

    #[test]
    fn team_history_restores_every_agent_and_child_approval() {
        let root = Uuid::new_v4();
        let child = Uuid::new_v4();
        let now = Utc::now();
        let mut agent = SubagentSnapshot {
            session_id: child,
            parent_session_id: root,
            task_name: "/root/worker".to_string(),
            status: borg_remote::SubagentStatus::Running,
            provider: CodingProvider::Codex,
            model: Some("gpt-test".to_string()),
            effort: Some("low".to_string()),
            cwd: PathBuf::from("/workspace"),
            created_at: now,
            updated_at: now,
            detail: None,
            final_text: None,
            usage: borg_remote::SubagentUsage::default(),
        };
        let approval_id = "approval-1".to_string();
        let approval = SessionEvent::new(
            root,
            1,
            SessionEventKind::SubagentActivity {
                activity: borg_remote::SubagentActivityKind::Updated,
                agent: agent.clone(),
                event: Some(Box::new(SessionEvent::new(
                    child,
                    1,
                    SessionEventKind::ApprovalRequested {
                        approval_id: approval_id.clone(),
                        title: "Run tests?".to_string(),
                        detail: String::new(),
                        command: None,
                    },
                ))),
            },
        );
        agent.status = borg_remote::SubagentStatus::Ready;
        let completed = SessionEvent::new(
            root,
            2,
            SessionEventKind::SubagentActivity {
                activity: borg_remote::SubagentActivityKind::Completed,
                agent: agent.clone(),
                event: None,
            },
        );

        let restored = latest_subagent_snapshots(&[approval.clone(), completed]);
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].session_id, child);
        assert_eq!(restored[0].status, borg_remote::SubagentStatus::Ready);
        assert_eq!(
            child_pending_approval_ids(&[approval]),
            HashMap::from([(child, approval_id)])
        );
    }

    #[test]
    fn provider_user_input_response_uses_question_ids() {
        let payload = serde_json::json!({
            "questions": [{
                "id": "scope",
                "header": "Scope",
                "question": "Which scope?",
                "options": [{"label": "Workspace", "description": "Current workspace"}]
            }]
        });

        assert_eq!(
            provider_interaction_response("user_input", &payload, "Workspace").unwrap(),
            serde_json::json!({
                "answers": {
                    "scope": { "answers": ["Workspace"] }
                }
            })
        );
    }

    #[test]
    fn provider_user_input_response_requires_all_multiple_answers() {
        let payload = serde_json::json!({
            "questions": [
                {"id": "scope", "header": "Scope", "question": "Which scope?"},
                {"id": "mode", "header": "Mode", "question": "Which mode?"}
            ]
        });

        let response = provider_interaction_response(
            "user_input",
            &payload,
            r#"{"scope":"Workspace","mode":["Fast","Safe"]}"#,
        )
        .unwrap();
        assert_eq!(
            response,
            serde_json::json!({
                "answers": {
                    "scope": { "answers": ["Workspace"] },
                    "mode": { "answers": ["Fast", "Safe"] }
                }
            })
        );
        assert!(
            provider_interaction_response("user_input", &payload, r#"{"scope":"Workspace"}"#)
                .unwrap_err()
                .to_string()
                .contains("mode")
        );
    }

    #[test]
    fn provider_mcp_elicitation_accepts_structured_content_and_cancellation() {
        let payload = serde_json::json!({
            "requestedSchema": {
                "type": "object",
                "properties": {"region": {"type": "string"}}
            }
        });

        assert_eq!(
            provider_interaction_response("mcp_elicitation", &payload, r#"{"region":"eu"}"#)
                .unwrap(),
            serde_json::json!({
                "action": "accept",
                "content": {"region": "eu"}
            })
        );
        assert_eq!(
            provider_interaction_response("mcp_elicitation", &payload, "/cancel").unwrap(),
            serde_json::json!({"action": "cancel"})
        );
    }

    #[test]
    fn active_message_delivery_respects_provider_capability_and_explicit_override() {
        assert_eq!(
            running_input("plain", CodingProvider::Codex, true, false).0,
            PromptDelivery::Steer
        );
        assert_eq!(
            running_input("plain", CodingProvider::Codex, false, false).0,
            PromptDelivery::Queue
        );
        assert_eq!(
            running_input("/queue later", CodingProvider::Codex, true, false).0,
            PromptDelivery::Queue
        );
        assert_eq!(
            running_input("/steer now", CodingProvider::Codex, false, false).0,
            PromptDelivery::Steer
        );
        assert_eq!(
            running_input("plain", CodingProvider::OpenRouter, true, false).0,
            PromptDelivery::Steer
        );
        assert_eq!(
            running_input("/steer now", CodingProvider::OpenAiCompatible, false, false).0,
            PromptDelivery::Steer
        );
        assert_eq!(
            running_input("plain", CodingProvider::Claude, true, false).0,
            PromptDelivery::Steer
        );
        assert_eq!(
            running_input("/steer now", CodingProvider::Claude, false, false).0,
            PromptDelivery::Steer
        );
    }

    #[test]
    fn ordinary_followups_queue_while_context_is_compacting() {
        assert_eq!(
            running_input("usage details", CodingProvider::Codex, true, true).0,
            PromptDelivery::Queue
        );
        assert_eq!(
            running_input("usage details", CodingProvider::Claude, true, true).0,
            PromptDelivery::Queue
        );
        assert_eq!(
            running_input("/steer now", CodingProvider::Codex, false, true).0,
            PromptDelivery::Steer
        );
        assert!(status_is_compacting(Some(
            "Automatically compacting context"
        )));
        assert!(!status_is_compacting(Some("turn phase: provider active")));
    }

    #[test]
    fn session_switch_distinguishes_owner_shutdown_from_viewer_detach() {
        assert_eq!(
            LocalSessionAccess::Attached
                .switch(SessionStatus::Running)
                .unwrap(),
            SessionSwitch::DetachViewer
        );
        assert_eq!(
            LocalSessionAccess::Owned
                .switch(SessionStatus::Ready)
                .unwrap(),
            SessionSwitch::StopOwnedSession
        );
        assert!(
            LocalSessionAccess::Owned
                .switch(SessionStatus::Running)
                .unwrap_err()
                .to_string()
                .contains("Interrupt the current turn")
        );
    }

    #[tokio::test]
    async fn owner_shutdown_hands_active_turn_to_an_attached_viewer() {
        let root = tempdir().unwrap();
        let session_id = Uuid::new_v4();
        let journal_path = root.path().join(format!("{session_id}.jsonl"));
        let socket_path = session_control_socket_path(root.path(), session_id);
        let presence_path =
            borg_remote::session_control_presence_socket_path(root.path(), session_id);
        let journal = borg_remote::SessionJournal::open(&journal_path).unwrap();
        let writer = journal.try_acquire_writer().unwrap().unwrap();
        let (commands, _received) = mpsc::channel(1);
        let server =
            LocalSessionControlServer::start(socket_path, session_id, &writer, commands).unwrap();

        assert!(!owner_shutdown_should_handoff_to_viewer(
            LocalSessionAccess::Owned,
            SessionStatus::Running,
            false,
            Some(&server),
        ));
        let mut presence = tokio::net::UnixStream::connect(presence_path)
            .await
            .unwrap();
        let mut acknowledgement = [0_u8; 1];
        presence.read_exact(&mut acknowledgement).await.unwrap();
        assert!(owner_shutdown_should_handoff_to_viewer(
            LocalSessionAccess::Owned,
            SessionStatus::Running,
            false,
            Some(&server),
        ));
        assert!(!owner_shutdown_should_handoff_to_viewer(
            LocalSessionAccess::Attached,
            SessionStatus::Running,
            false,
            Some(&server),
        ));
    }

    #[test]
    fn obsolete_owner_handoff_waits_for_a_safe_turn_boundary() {
        assert!(stale_local_owner_can_handoff(Some(SessionStatus::Ready)));
        assert!(stale_local_owner_can_handoff(Some(SessionStatus::Stopped)));
        assert!(!stale_local_owner_can_handoff(Some(
            SessionStatus::Starting
        )));
        assert!(!stale_local_owner_can_handoff(Some(SessionStatus::Running)));
        assert!(!stale_local_owner_can_handoff(Some(
            SessionStatus::WaitingForApproval
        )));
        assert!(!stale_local_owner_can_handoff(None));
    }

    #[test]
    fn owned_resume_presents_stopped_state_as_ready_while_rehydrating() {
        let state = SessionState {
            latest_sequence: 10_000,
            status: Some(SessionStatus::Stopped),
            status_detail: Some("process exited".to_string()),
            ..SessionState::default()
        };

        let displayed = resume_display_state(state.clone(), LocalSessionAccess::Owned, true);
        assert_eq!(displayed.status, Some(SessionStatus::Ready));
        assert_eq!(displayed.status_detail, None);
        assert_eq!(displayed.latest_sequence, state.latest_sequence);

        assert_eq!(
            resume_display_state(state.clone(), LocalSessionAccess::Attached, true).status,
            Some(SessionStatus::Stopped)
        );
        assert_eq!(
            resume_display_state(state, LocalSessionAccess::Owned, false).status,
            Some(SessionStatus::Stopped)
        );
    }

    #[tokio::test]
    async fn obsolete_owner_handoff_releases_and_reacquires_the_writer_lease() {
        let root = tempdir().unwrap();
        let session_id = Uuid::new_v4();
        let journal_path = root.path().join(format!("{session_id}.jsonl"));
        let socket_path = session_control_socket_path(root.path(), session_id);
        let journal = borg_remote::SessionJournal::open(&journal_path).unwrap();
        let writer = journal.try_acquire_writer().unwrap().unwrap();
        let (commands, mut received) = mpsc::channel(1);
        let _server =
            LocalSessionControlServer::start(socket_path.clone(), session_id, &writer, commands)
                .unwrap();
        let release = tokio::spawn(async move {
            assert!(matches!(
                received.recv().await,
                Some(HostCommand::Stop { session_id: target }) if target == session_id
            ));
            drop(writer);
        });

        let replacement =
            stop_stale_local_owner_and_acquire(&journal_path, &socket_path, session_id)
                .await
                .unwrap();
        release.await.unwrap();
        drop(replacement);
    }

    #[test]
    fn active_session_survives_terminal_hangup_but_idle_session_stops() {
        assert!(should_detach_on_terminal_hangup(
            "SIGHUP",
            SessionStatus::Running,
            false
        ));
        assert!(should_detach_on_terminal_hangup(
            "SIGHUP",
            SessionStatus::WaitingForApproval,
            false
        ));
        assert!(!should_detach_on_terminal_hangup(
            "SIGHUP",
            SessionStatus::Ready,
            false
        ));
        assert!(!should_detach_on_terminal_hangup(
            "SIGTERM",
            SessionStatus::Running,
            true
        ));
        assert!(should_detach_on_terminal_hangup(
            "SIGHUP",
            SessionStatus::Ready,
            true
        ));
    }

    #[test]
    fn tui_frame_interval_preserves_supported_high_refresh_and_caps_extremes() {
        assert_eq!(
            tui_frame_interval(165),
            std::time::Duration::from_nanos(6_060_606)
        );
        assert_eq!(tui_frame_interval(1), tui_frame_interval(MIN_TUI_FPS));
        assert_eq!(tui_frame_interval(1_000), tui_frame_interval(MAX_TUI_FPS));
    }

    #[test]
    fn expensive_draws_leave_time_for_input_and_animation_events() {
        assert_eq!(
            responsive_tui_frame_interval(165, std::time::Duration::from_millis(5), false),
            std::time::Duration::from_millis(15)
        );
        assert_eq!(
            responsive_tui_frame_interval(60, std::time::Duration::from_millis(40), false),
            ACTIVITY_FRAME_INTERVAL
        );
        assert_eq!(
            responsive_tui_frame_interval(60, std::time::Duration::ZERO, false),
            tui_frame_interval(60)
        );
        assert_eq!(
            responsive_tui_frame_interval(60, std::time::Duration::from_millis(40), true),
            std::time::Duration::from_millis(40)
        );
    }

    #[test]
    fn active_terminal_frames_do_not_depend_on_composer_text() {
        assert!(terminal_needs_activity_tick(
            false,
            false,
            SessionStatus::Starting
        ));
        assert!(terminal_needs_activity_tick(
            false,
            false,
            SessionStatus::Running
        ));
        assert!(terminal_needs_activity_tick(
            true,
            false,
            SessionStatus::Ready
        ));
        assert!(terminal_needs_activity_tick(
            false,
            true,
            SessionStatus::Ready
        ));
        assert!(!terminal_needs_activity_tick(
            false,
            false,
            SessionStatus::Ready
        ));
    }

    #[tokio::test]
    async fn resume_target_resolves_saved_session_and_skips_current_for_last() {
        let dir = tempdir().expect("session directory");
        let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3"))
            .await
            .unwrap();
        let current = Uuid::new_v4();
        let previous = Uuid::new_v4();
        for session_id in [previous, current] {
            let events = [
                SessionEvent::new(session_id, 1, SessionEventKind::SessionStarted),
                SessionEvent::new(
                    session_id,
                    2,
                    SessionEventKind::SessionConfigured {
                        cwd: dir.path().to_path_buf(),
                        provider: CodingProvider::Codex,
                        model: None,
                        effort: None,
                        fast: false,
                        response_language: ResponseLanguage::Auto,
                        permission_mode: PermissionMode::FullAccess,
                    },
                ),
                SessionEvent::new(
                    session_id,
                    3,
                    SessionEventKind::Message {
                        message_id: Uuid::new_v4(),
                        actor: EventActor::User,
                        text: "real resumable work".to_string(),
                        attachments: Vec::new(),
                        status: MessageStatus::Complete,
                        delivery: None,
                    },
                ),
            ];
            fs::write(
                dir.path().join(format!("{session_id}.jsonl")),
                format!(
                    "{}\n",
                    events
                        .iter()
                        .map(|event| serde_json::to_string(event).unwrap())
                        .collect::<Vec<_>>()
                        .join("\n")
                ),
            )
            .expect("saved session");
        }

        assert_eq!(
            resolve_resume_target(dir.path(), &store, current, &previous.to_string())
                .await
                .unwrap(),
            previous
        );
        assert_eq!(
            resolve_resume_target(dir.path(), &store, current, "--last")
                .await
                .unwrap(),
            previous
        );
        assert!(
            resolve_resume_target(dir.path(), &store, current, &current.to_string())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn recent_sessions_are_ordered_by_latest_persisted_activity() {
        let dir = tempdir().expect("session directory");
        let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3"))
            .await
            .unwrap();
        let recently_active = Uuid::new_v4();
        let recent_message = Uuid::new_v4();
        let now = Utc::now();

        let write_events = |session_id, events: Vec<(chrono::DateTime<Utc>, SessionEventKind)>| {
            let path = dir.path().join(format!("{session_id}.jsonl"));
            let started_at = events.first().map_or_else(Utc::now, |(created_at, _)| {
                *created_at - chrono::TimeDelta::nanoseconds(2)
            });
            let records = [
                (started_at, SessionEventKind::SessionStarted),
                (
                    started_at + chrono::TimeDelta::nanoseconds(1),
                    SessionEventKind::SessionConfigured {
                        cwd: dir.path().to_path_buf(),
                        provider: CodingProvider::Codex,
                        model: None,
                        effort: None,
                        fast: false,
                        response_language: ResponseLanguage::Auto,
                        permission_mode: PermissionMode::FullAccess,
                    },
                ),
            ]
            .into_iter()
            .chain(events)
            .enumerate()
            .map(|(index, (created_at, kind))| {
                serde_json::to_string(&SessionEvent {
                    id: Uuid::new_v4(),
                    session_id,
                    sequence: index as u64 + 1,
                    created_at,
                    kind,
                })
                .unwrap()
            })
            .collect::<Vec<_>>()
            .join("\n");
            fs::write(path, format!("{records}\n")).unwrap();
        };
        write_events(
            recently_active,
            vec![
                (
                    now - chrono::TimeDelta::minutes(5),
                    SessionEventKind::Message {
                        message_id: Uuid::new_v4(),
                        actor: EventActor::User,
                        text: "older prompt".to_string(),
                        attachments: Vec::new(),
                        status: MessageStatus::Complete,
                        delivery: None,
                    },
                ),
                (
                    now,
                    SessionEventKind::StatusChanged {
                        status: SessionStatus::Ready,
                        detail: None,
                    },
                ),
            ],
        );
        write_events(
            recent_message,
            vec![(
                now - chrono::TimeDelta::minutes(1),
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: EventActor::Assistant,
                    text: "newer message but older session activity".to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            )],
        );

        assert_eq!(
            recent_session_ids(dir.path(), &store).await.unwrap(),
            vec![recently_active, recent_message]
        );
    }

    #[tokio::test]
    async fn resume_picker_titles_and_previews_sessions_from_the_latest_response() {
        let dir = tempdir().expect("session directory");
        let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3"))
            .await
            .unwrap();
        let current = Uuid::new_v4();
        let target = Uuid::new_v4();
        store.create_session(current).await.unwrap();
        store.create_session(target).await.unwrap();
        for kind in [
            SessionEventKind::SessionStarted,
            SessionEventKind::SessionConfigured {
                cwd: dir.path().to_path_buf(),
                provider: CodingProvider::Codex,
                model: Some("gpt-resume-filter".to_string()),
                effort: Some("high".to_string()),
                fast: false,
                response_language: ResponseLanguage::Auto,
                permission_mode: PermissionMode::FullAccess,
            },
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: "First setup request".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: "Latest **formatted** request".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: "Latest **formatted** response".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ] {
            store
                .append(SessionEvent::new(target, 0, kind))
                .await
                .unwrap();
        }

        let options = recent_session_options(dir.path(), &store, current, dir.path(), 8)
            .await
            .unwrap();
        let target = options
            .iter()
            .find(|option| option.id == target)
            .expect("target session should be resumable");

        assert!(target.label.contains("Latest formatted response"));
        assert!(target.label.contains("gpt-resume-filter"));
        assert!(!target.label.contains("First setup request"));
        assert!(target.preview.starts_with("Latest **formatted** response"));
        assert!(target.preview.contains("**Model:** `gpt-resume-filter`"));
        assert!(target.preview.contains("Latest prompt:"));
        assert!(target.preview.contains("Latest formatted request"));
    }

    #[tokio::test]
    async fn resume_discovery_ignores_launch_only_probe_sessions() {
        let dir = tempdir().expect("session directory");
        let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3"))
            .await
            .unwrap();
        let probe = Uuid::new_v4();
        let real = Uuid::new_v4();
        for session_id in [probe, real] {
            store.create_session(session_id).await.unwrap();
            for kind in [
                SessionEventKind::SessionStarted,
                SessionEventKind::SessionConfigured {
                    cwd: dir.path().to_path_buf(),
                    provider: CodingProvider::Codex,
                    model: Some("gpt-5.6-sol".to_string()),
                    effort: Some("low".to_string()),
                    fast: false,
                    response_language: ResponseLanguage::Auto,
                    permission_mode: PermissionMode::FullAccess,
                },
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Ready,
                    detail: None,
                },
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Stopped,
                    detail: None,
                },
            ] {
                store
                    .append(SessionEvent::new(session_id, 0, kind))
                    .await
                    .unwrap();
            }
        }
        store
            .append(SessionEvent::new(
                real,
                0,
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: EventActor::User,
                    text: "real user work".to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: Some(PromptDelivery::Queue),
                },
            ))
            .await
            .unwrap();

        let sessions = recent_session_ids(dir.path(), &store).await.unwrap();
        assert_eq!(sessions, vec![real]);
        assert!(
            store.contains_session(probe).await.unwrap(),
            "filtering the resume surface must not destructively delete legacy rows"
        );
    }

    #[tokio::test]
    async fn resume_picker_prioritizes_current_directory_and_keeps_global_choices() {
        let dir = tempdir().expect("session directory");
        let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3"))
            .await
            .unwrap();
        let current = Uuid::new_v4();
        let local = Uuid::new_v4();
        let global = Uuid::new_v4();
        for session_id in [current, local, global] {
            store.create_session(session_id).await.unwrap();
        }
        for (session_id, cwd, prompt) in [
            (local, dir.path().to_path_buf(), "local session"),
            (global, dir.path().join("another-project"), "global session"),
        ] {
            for kind in [
                SessionEventKind::SessionStarted,
                SessionEventKind::SessionConfigured {
                    cwd,
                    provider: CodingProvider::Codex,
                    model: Some("gpt-5.6-sol".to_string()),
                    effort: Some("high".to_string()),
                    fast: false,
                    response_language: ResponseLanguage::Auto,
                    permission_mode: PermissionMode::FullAccess,
                },
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: EventActor::User,
                    text: prompt.to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ] {
                store
                    .append(SessionEvent::new(session_id, 0, kind))
                    .await
                    .unwrap();
            }
        }

        let options = recent_session_options(dir.path(), &store, current, dir.path(), 8)
            .await
            .unwrap();

        assert_eq!(
            options
                .iter()
                .map(|option| (option.id, option.current_directory))
                .collect::<Vec<_>>(),
            [(local, true), (global, false)]
        );
    }

    #[tokio::test]
    async fn resume_picker_loads_older_sessions_in_recent_first_order() {
        const SESSION_COUNT: usize = 12;

        let dir = tempdir().expect("session directory");
        let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3"))
            .await
            .unwrap();
        let current = Uuid::new_v4();
        store.create_session(current).await.unwrap();
        let mut session_ids = Vec::with_capacity(SESSION_COUNT);
        for index in 0..SESSION_COUNT {
            let session_id = Uuid::new_v4();
            session_ids.push(session_id);
            store.create_session(session_id).await.unwrap();
            for kind in [
                SessionEventKind::SessionStarted,
                SessionEventKind::SessionConfigured {
                    cwd: dir.path().to_path_buf(),
                    provider: CodingProvider::Codex,
                    model: Some(format!("picker-model-{index}")),
                    effort: None,
                    fast: false,
                    response_language: ResponseLanguage::Auto,
                    permission_mode: PermissionMode::FullAccess,
                },
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: EventActor::User,
                    text: format!("picker session {index}"),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ] {
                store
                    .append(SessionEvent::new(session_id, 0, kind))
                    .await
                    .unwrap();
            }
        }

        let options = recent_session_options(
            dir.path(),
            &store,
            current,
            dir.path(),
            RESUME_PICKER_SESSION_LIMIT,
        )
        .await
        .unwrap();

        assert_eq!(options.len(), SESSION_COUNT);
        assert_eq!(
            options.iter().map(|option| option.id).collect::<Vec<_>>(),
            session_ids.into_iter().rev().collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn incompatible_legacy_session_does_not_block_discovery_or_mutate_source() {
        let dir = tempdir().expect("session directory");
        let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3"))
            .await
            .unwrap();
        let valid = Uuid::new_v4();
        store.create_session(valid).await.unwrap();
        store
            .append(SessionEvent::new(
                valid,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
            .unwrap();
        store
            .append(SessionEvent::new(
                valid,
                0,
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: EventActor::User,
                    text: "valid resumable work".to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ))
            .await
            .unwrap();
        let incompatible = Uuid::new_v4();
        let path = dir.path().join(format!("{incompatible}.jsonl"));
        let bytes = format!(
            "{}\n",
            serde_json::to_string(&SessionEvent::new(
                incompatible,
                1,
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: EventActor::User,
                    text: "legacy partial stream".to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ))
            .unwrap()
        );
        fs::write(&path, &bytes).unwrap();

        assert_eq!(
            recent_session_ids(dir.path(), &store).await.unwrap(),
            vec![valid]
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), bytes);
        assert!(!path.with_extension("jsonl.bak").exists());
        assert!(!store.contains_session(incompatible).await.unwrap());
    }

    #[tokio::test]
    #[ignore = "explicit session-picker p95 performance gate"]
    async fn recent_session_picker_p95_gate() {
        const SESSION_COUNT: usize = 109;
        const SAMPLES: usize = 100;

        let dir = tempdir().expect("session directory");
        let store = SqliteSessionStore::open(dir.path().join("sessions.sqlite3"))
            .await
            .unwrap();
        let current = Uuid::new_v4();
        store.create_session(current).await.unwrap();
        for index in 0..SESSION_COUNT {
            let session_id = Uuid::new_v4();
            store.create_session(session_id).await.unwrap();
            for kind in [
                SessionEventKind::SessionStarted,
                SessionEventKind::SessionConfigured {
                    cwd: dir.path().to_path_buf(),
                    provider: CodingProvider::Codex,
                    model: Some("performance-fixture".to_string()),
                    effort: None,
                    fast: false,
                    response_language: ResponseLanguage::Auto,
                    permission_mode: PermissionMode::FullAccess,
                },
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: EventActor::User,
                    text: format!("performance fixture prompt {index}"),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ] {
                store
                    .append(SessionEvent::new(session_id, 0, kind))
                    .await
                    .unwrap();
            }
        }

        assert_eq!(
            recent_session_options(
                dir.path(),
                &store,
                current,
                dir.path(),
                RESUME_PICKER_SESSION_LIMIT,
            )
            .await
            .unwrap()
            .len(),
            SESSION_COUNT
        );
        let mut samples = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            let started = Instant::now();
            let options = recent_session_options(
                dir.path(),
                &store,
                current,
                dir.path(),
                RESUME_PICKER_SESSION_LIMIT,
            )
            .await
            .unwrap();
            assert_eq!(options.len(), SESSION_COUNT);
            samples.push(started.elapsed());
        }
        samples.sort_unstable();
        let p95 = samples[(samples.len() * 95).div_ceil(100).saturating_sub(1)];
        eprintln!("session picker p95: {p95:?}");
        assert!(
            p95 < Duration::from_millis(50),
            "session picker p95 exceeded 50 ms: {p95:?}"
        );
    }

    #[test]
    fn resume_instructions_end_with_copyable_command() {
        let session_id = Uuid::nil();
        let instructions = resume_instructions(session_id, true);

        assert!(instructions.contains("Close the other Borg process"));
        assert_eq!(
            instructions.lines().last(),
            Some("borg resume 00000000-0000-0000-0000-000000000000")
        );
    }

    #[test]
    fn ephemeral_exit_never_advertises_an_unresumable_session() {
        assert!(!should_print_exit_resume(true, None, true));
        assert!(should_print_exit_resume(true, None, false));
        assert!(!should_print_exit_resume(true, Some(Uuid::new_v4()), false));
        assert!(!should_print_exit_resume(false, None, false));
    }

    #[test]
    fn tui_crash_resume_output_is_only_the_two_requested_lines() {
        let session_id = Uuid::nil();
        let instructions = resume_instructions(session_id, false);

        assert_eq!(
            instructions,
            "Copy and paste the line below to resume:\nborg resume 00000000-0000-0000-0000-000000000000"
        );
        assert_eq!(instructions.lines().count(), 2);
        assert!(!instructions.contains("BORG"));
        assert!(!instructions.contains('\x1b'));
    }

    #[test]
    fn redirected_output_never_enters_rich_terminal_mode() {
        assert!(rich_terminal_can_prompt(true, true, false));
        assert!(!rich_terminal_can_prompt(true, false, false));
        assert!(!rich_terminal_can_prompt(true, true, true));
    }
}
