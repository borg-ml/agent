use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, BufRead, IsTerminal, Write};
use std::net::IpAddr;
use std::panic::{self, AssertUnwindSafe, PanicHookInfo};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Context, Result, ensure};
use borg_provider::provider::{
    CodexAccountRateLimits, CodexRateLimitWindow, read_codex_account_rate_limits,
};
use borg_remote::{
    AgentTurnExecutor, ApprovalDecision, CodingProvider, EventActor, GoalAction, GoalStatus,
    HostCommand, HostConfig, HostExecutionProfile, HostExecutorFactory, LaunchSession,
    LocalAgentSettings, LocalAgentTurnExecutor, LocalSessionControlServer, MessageStatus,
    PermissionMode, PlanItem, PlanItemStatus, PromptDelivery, ResponseLanguage,
    SessionConfigAction, SessionEvent, SessionEventKind, SessionGoal, SessionState, SessionStatus,
    SessionStore, SessionWriterLease, SpawnSubagent, SqliteSessionStore, SubagentAction,
    SubagentSnapshot, SubagentStatus, TodoAction, default_host_config_path, enroll_host,
    force_terminate_local_session_owner, local_session_owner_uses_current_binary, login_provider,
    mirror_local_session, obsolete_local_session_owner_pid, probe_capabilities,
    probe_provider_admission_capabilities, provider_credentials_present,
    run_agent_session_with_store_and_writer, run_agent_session_with_store_writer_and_peers,
    run_attached_session, run_host_with_executor_factory, send_local_session_command,
    session_control_socket_path,
};
use chrono::{Local, TimeZone, Utc};
use futures_util::FutureExt;
use pulldown_cmark::{Event as MarkdownEvent, Parser as MarkdownParser};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::{Child, Command as TokioCommand};
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

use crate::agent_config::AgentConfig;
use crate::cli::{LocalAgentCliArgs, RemoteCommand};
use crate::dictation::{
    LocalDictationBackend, LocalDictationConfig, LocalDictationRecorder, ensure_backend,
    parakeet_is_installed,
};
use crate::editor_preferences::{
    ActiveMessageBehavior, CompletionAlertPolicy, DictationIconStyle, EditorPreferences,
};
use crate::sleep_inhibitor::SleepInhibitor;
use crate::terminal_ui::{
    BorgTerminal, DictationState, ProviderAuthChoice, ResumeSessionOption, TerminalInputEvent,
    UiAction, dictation_icon_style_for_preference, discard_pending_terminal_input,
};

#[path = "local_server.rs"]
mod local_server;

const MIN_TUI_FPS: u64 = 15;
const MAX_TUI_FPS: u64 = 240;
const ACTIVITY_FRAME_INTERVAL: std::time::Duration = std::time::Duration::from_millis(20);
const TOOL_STARTED_FRAME_MIN_DURATION: std::time::Duration = std::time::Duration::from_millis(500);
const IDLE_FRAME_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);
const MAX_RENDER_BACKOFF_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
const LOCAL_RESUME_RETRY_INITIAL_DELAY: std::time::Duration = std::time::Duration::from_millis(250);
const LOCAL_RESUME_RETRY_MAX_DELAY: std::time::Duration = std::time::Duration::from_secs(5);
const EXTENSION_DISCOVERY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
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
    Vec<borg_remote::BluWorkflowDefinition>,
)>;
type BluDiscoveryTask = tokio::task::JoinHandle<BluDiscoveryResult>;
type RevertForkTask = tokio::task::JoinHandle<Result<Uuid>>;
/// Resume filtering is local and eager, so load enough history to make search useful while
/// keeping picker construction and per-keystroke filtering bounded.
const RESUME_PICKER_SESSION_LIMIT: usize = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RevertStartMode {
    ForkNow,
    StopThenFork,
}

fn revert_start_mode(status: SessionStatus) -> RevertStartMode {
    if status == SessionStatus::Stopped {
        RevertStartMode::ForkNow
    } else {
        RevertStartMode::StopThenFork
    }
}

fn spawn_revert_fork(
    store: Arc<dyn SessionStore>,
    session_id: Uuid,
    sequence: u64,
) -> RevertForkTask {
    let fork_id = Uuid::new_v4();
    tokio::spawn(async move {
        store
            .fork_before(session_id, fork_id, sequence)
            .await
            .map(|_| fork_id)
    })
}

fn take_revert_ready_to_fork(
    pending_sequence: &mut Option<u64>,
    status: SessionStatus,
    event_stream_closed: bool,
) -> Option<u64> {
    (event_stream_closed || status == SessionStatus::Stopped)
        .then(|| pending_sequence.take())
        .flatten()
}

fn rich_terminal_can_prompt(stdin_is_terminal: bool, stdout_is_terminal: bool, json: bool) -> bool {
    stdin_is_terminal && stdout_is_terminal && !json
}

#[derive(Default)]
struct TuiCrashContext {
    session_id: Mutex<Option<Uuid>>,
    retry_notice: Mutex<Option<String>>,
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

    fn set_retry_notice(&self, notice: String) {
        *self
            .retry_notice
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(notice);
    }

    fn take_retry_notice(&self) -> Option<String> {
        self.retry_notice
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
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
            token_stdin,
            name,
            roots,
            config,
        } => {
            let config_path = config.unwrap_or_else(default_host_config_path);
            let token = match token {
                Some(token) => token,
                None => {
                    anyhow::ensure!(token_stdin, "an enrollment token is required");
                    read_enrollment_token_from_stdin()?
                }
            };
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

fn read_enrollment_token_from_stdin() -> Result<String> {
    if io::stdin().is_terminal() {
        eprint!("Paste the one-time enrollment token, then press Enter: ");
        io::stderr().flush().ok();
    }
    let mut token = String::new();
    io::stdin()
        .read_line(&mut token)
        .context("failed to read the enrollment token from stdin")?;
    let token = token.trim().to_string();
    anyhow::ensure!(!token.is_empty(), "enrollment token from stdin is empty");
    Ok(token)
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
        let (catalog, extension_servers, extension_workflows) = crate::extensions::discover(
            &launch.cwd,
            &agent_config.capabilities,
            &agent_config.extensions,
        )?;
        let mut servers = agent_config.external_mcp_servers();
        servers.extend(extension_servers);
        let roots = catalog.active_skill_roots();
        let local_settings = LocalAgentSettings {
            approval_reviewer_model: agent_config.approvals.reviewer_model.clone(),
            approval_reviewer_effort: agent_config.approvals.reviewer_effort.clone(),
            configured_model_gateways: agent_config.configured_model_gateways(),
            tool_mode: agent_config.capabilities.tool_mode,
        };
        let reload_cwd = launch.cwd.clone();
        let reload = move || {
            let agent_config = AgentConfig::load(None)?;
            let (catalog, extension_servers, _extension_workflows) = crate::extensions::discover(
                &reload_cwd,
                &agent_config.capabilities,
                &agent_config.extensions,
            )?;
            anyhow::ensure!(
                !catalog.has_errors(),
                "Blu catalog has errors; keeping the remote session's last-known-good snapshot"
            );
            let mut servers = agent_config.external_mcp_servers();
            servers.extend(extension_servers);
            Ok((
                servers,
                catalog.active_skill_roots(),
                catalog.active_workflows(),
                catalog.api_snapshot(),
            ))
        };
        let executor = if launch.provider == CodingProvider::Kimi {
            LocalAgentTurnExecutor::with_model_gateway_and_settings(
                borg_provider::provider::ModelGateway::new(
                    format!(
                        "{}/api/remote/host/kimi/chat/completions",
                        host.server.trim_end_matches('/')
                    ),
                    host.host_token.clone(),
                ),
                local_settings,
            )
        } else {
            LocalAgentTurnExecutor::with_settings(local_settings)
        }
        .with_external_mcp_servers(servers)
        .with_extension_skill_roots(roots)
        .with_extension_workflows(extension_workflows)
        .with_extension_api(catalog.api_snapshot())
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
    ensure!(
        cfg!(target_os = "linux"),
        "`borg remote install` currently supports Linux systemd user services; run `borg remote host` from your platform's login service"
    );
    ensure!(
        config_path.is_file(),
        "host config does not exist at {}; run `borg remote enroll` first",
        config_path.display()
    );
    let config: HostConfig = serde_json::from_slice(
        &fs::read(config_path)
            .with_context(|| format!("failed to read {}", config_path.display()))?,
    )
    .with_context(|| format!("invalid host config {}", config_path.display()))?;
    let config_path = config_path
        .canonicalize()
        .with_context(|| format!("failed to resolve {}", config_path.display()))?;
    let allowed_networks = if config.execution_profile == HostExecutionProfile::IsolatedHosted {
        isolated_allowed_networks()?
    } else {
        Vec::new()
    };
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
    let service = host_service_unit(&executable, &config_path, &config, &path, &allowed_networks)?;
    fs::write(&service_path, service)
        .with_context(|| format!("failed to write {}", service_path.display()))?;
    if config.execution_profile == HostExecutionProfile::IsolatedHosted {
        let status = tokio::process::Command::new("systemd-analyze")
            .args(["verify", service_path.to_string_lossy().as_ref()])
            .status()
            .await
            .context("failed to run systemd-analyze verify for isolated hosted service")?;
        ensure!(
            status.success(),
            "systemd-analyze rejected the isolated hosted service unit"
        );
    }
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

fn host_service_unit(
    executable: &Path,
    config_path: &Path,
    config: &HostConfig,
    path: &str,
    allowed_networks: &[String],
) -> Result<String> {
    let mut service = format!(
        "[Unit]\nDescription=Borg Remote host\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=simple\nExecStart={} remote host --config {}\nEnvironment={}\nRestart=always\nRestartSec=2\n",
        systemd_quote(validated_systemd_value(&executable.to_string_lossy())?),
        systemd_quote(validated_systemd_value(&config_path.to_string_lossy())?),
        systemd_quote(validated_systemd_value(&format!("PATH={path}"))?),
    );
    for name in [
        "DISPLAY",
        "WAYLAND_DISPLAY",
        "DBUS_SESSION_BUS_ADDRESS",
        "XDG_RUNTIME_DIR",
        "XDG_CURRENT_DESKTOP",
        "XDG_SESSION_TYPE",
        "NIRI_SOCKET",
        "SWAYSOCK",
        "HYPRLAND_INSTANCE_SIGNATURE",
        "TERMINAL",
        "BORG_TERMINAL",
    ] {
        if let Some(value) = std::env::var_os(name)
            && let Some(value) = value.to_str()
        {
            let assignment = format!("{name}={value}");
            service.push_str("Environment=");
            service.push_str(&systemd_quote(validated_systemd_value(&assignment)?));
            service.push('\n');
        }
    }

    if config.execution_profile == HostExecutionProfile::IsolatedHosted {
        ensure!(
            !allowed_networks.is_empty(),
            "isolated hosted systemd service requires BORG_HOST_ALLOWED_NETWORKS"
        );
        service.push_str(&format!(
            "NoNewPrivileges=true\n\
PrivateTmp=true\n\
PrivateDevices=true\n\
ProtectSystem=strict\n\
ProtectKernelTunables=true\n\
ProtectKernelModules=true\n\
ProtectControlGroups=true\n\
LockPersonality=true\n\
RestrictSUIDSGID=true\n\
RestrictRealtime=true\n\
CapabilityBoundingSet=\n\
AmbientCapabilities=\n\
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6\n\
CPUAccounting=true\n\
CPUQuota={}%\n\
MemoryAccounting=true\n\
MemoryMax={}\n\
TasksAccounting=true\n\
TasksMax={}\n\
IPAddressDeny=any\n",
            config.resource_limits.max_cpu_percent,
            config.resource_limits.max_memory_bytes,
            config.resource_limits.max_processes,
        ));
        for network in allowed_networks {
            service.push_str("IPAddressAllow=");
            service.push_str(validated_systemd_value(network)?);
            service.push('\n');
        }
        let mut write_paths = vec![
            config_path
                .parent()
                .context("host config has no parent directory")?
                .to_path_buf(),
        ];
        write_paths.extend(config.roots.iter().cloned());
        for path in write_paths {
            ensure!(
                path.is_absolute() && path != Path::new("/"),
                "isolated hosted service refuses a missing, relative, or root filesystem write path: {}",
                path.display()
            );
            service.push_str("ReadWritePaths=");
            service.push_str(&systemd_quote(validated_systemd_value(
                &path.to_string_lossy(),
            )?));
            service.push('\n');
        }
        service.push_str(
            "Environment=\"BORG_HOST_EXECUTION_PROFILE=isolated_hosted\"\n\
Environment=\"BORG_HOST_ISOLATION_ATTESTATION=systemd-user-sandbox-v1\"\n",
        );
    }
    service.push_str("\n[Install]\nWantedBy=default.target\n");
    Ok(service)
}

fn isolated_allowed_networks() -> Result<Vec<String>> {
    let raw = std::env::var("BORG_HOST_ALLOWED_NETWORKS").map_err(|_| {
        anyhow::anyhow!(
            "isolated hosted service requires BORG_HOST_ALLOWED_NETWORKS with Borg/provider/DNS IPs or CIDRs"
        )
    })?;
    parse_isolated_allowed_networks(&raw)
}

fn parse_isolated_allowed_networks(raw: &str) -> Result<Vec<String>> {
    let mut networks = Vec::new();
    for item in raw
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        let (address, prefix) = item
            .split_once('/')
            .map_or((item, None), |(address, prefix)| (address, Some(prefix)));
        let parsed: IpAddr = address
            .parse()
            .with_context(|| format!("invalid isolated host network address `{item}`"))?;
        if let Some(prefix) = prefix {
            let prefix = prefix
                .parse::<u8>()
                .with_context(|| format!("invalid isolated host network prefix `{item}`"))?;
            let maximum = if parsed.is_ipv4() { 32 } else { 128 };
            ensure!(
                prefix <= maximum,
                "isolated host network prefix is outside the address family: {item}"
            );
            ensure!(
                prefix > 0,
                "isolated host network allowlist must not contain a default route: {item}"
            );
            networks.push(format!("{parsed}/{prefix}"));
        } else {
            networks.push(parsed.to_string());
        }
    }
    ensure!(
        !networks.is_empty(),
        "BORG_HOST_ALLOWED_NETWORKS must contain at least one IP address or CIDR"
    );
    Ok(networks)
}

fn validated_systemd_value(value: &str) -> Result<&str> {
    ensure!(
        !value.chars().any(char::is_control),
        "systemd service value contains control characters"
    );
    Ok(value)
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
    if args.print {
        print_mode_banner();
    }
    let agent_config = AgentConfig::load(args.config.as_deref())?;
    // Provider environment is applied inside each owned session, after resume
    // configuration is resolved and before the server/executor starts.
    crate::updater::spawn_background(agent_config.updates.clone());
    let ephemeral_sessions = args.ephemeral.then(tempfile::tempdir).transpose()?;
    let crash_context = Arc::new(TuiCrashContext::default());
    // Keep this guard outside the caught session future: terminal Drop runs
    // while unwinding, then this guard is restored after catch_unwind returns.
    let _tui_panic_hook = TuiPanicHook::install(Arc::clone(&crash_context.tui_active));
    let mut selected_session = None;
    let mut restored_prompt = None;
    let mut reusable_terminal = None;
    let mut resume_retry_delay = LOCAL_RESUME_RETRY_INITIAL_DELAY;
    loop {
        let resume_requested =
            args.resume.is_some() || args.continue_latest || selected_session.is_some();
        let result = AssertUnwindSafe(run_local_agent_session(
            &args,
            selected_session,
            restored_prompt.take(),
            ephemeral_sessions.as_ref().map(tempfile::TempDir::path),
            Arc::clone(&crash_context),
            reusable_terminal.take(),
        ))
        .catch_unwind()
        .await;
        match result {
            Ok(Ok(Some((next_session, next_prompt, next_terminal)))) => {
                crash_context
                    .tui_active
                    .store(next_terminal.is_some(), Ordering::Release);
                selected_session = Some(next_session);
                restored_prompt = next_prompt;
                reusable_terminal = next_terminal;
                if resume_requested {
                    tokio::time::sleep(resume_retry_delay).await;
                    resume_retry_delay = next_local_resume_retry_delay(resume_retry_delay);
                } else {
                    resume_retry_delay = LOCAL_RESUME_RETRY_INITIAL_DELAY;
                }
            }
            Ok(Ok(None)) => {
                crash_context.tui_active.store(false, Ordering::Release);
                return Ok(());
            }
            Ok(Err(error)) if resume_requested && local_resume_error_is_retryable(&error) => {
                crash_context.tui_active.store(false, Ordering::Release);
                crash_context.set_retry_notice(format!(
                    "The resumed session is waiting on its journal and Borg is retrying: {error:#}"
                ));
                if selected_session.is_none() {
                    selected_session = args.resume;
                }
                tokio::time::sleep(resume_retry_delay).await;
                resume_retry_delay = next_local_resume_retry_delay(resume_retry_delay);
                continue;
            }
            Ok(Err(error)) if crash_context.tui_active.swap(false, Ordering::AcqRel) => {
                let Some(session_id) = crash_context.session_id() else {
                    return Err(error);
                };
                crash_context.set_retry_notice(format!(
                    "The resumed session hit a transient error and Borg is reconnecting: {error:#}"
                ));
                tokio::time::sleep(resume_retry_delay).await;
                resume_retry_delay = next_local_resume_retry_delay(resume_retry_delay);
                selected_session = Some(session_id);
                continue;
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

fn print_mode_banner() {
    #[cfg(unix)]
    let terminal = "/dev/tty";
    #[cfg(windows)]
    let terminal = "CONOUT$";

    if let Ok(mut terminal) = fs::OpenOptions::new().write(true).open(terminal) {
        let _ = writeln!(terminal, "Borg Agent v{}", env!("CARGO_PKG_VERSION"));
    }
}

pub(crate) async fn print_local_workspaces(json: bool) -> Result<()> {
    let host_config_path = default_host_config_path();
    let sessions_dir = host_config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("sessions");
    let session_path = sessions_dir.join("sessions.sqlite3");
    let workspaces = if session_path.is_file() {
        let session_store = SqliteSessionStore::open(&session_path).await?;
        let store = session_store
            .workspace_store()
            .await?
            .context("canonical session database has no workspace projection")?;
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

    async fn observe_from_store(
        &mut self,
        store: &dyn SessionStore,
        event: &SessionEvent,
    ) -> Result<Vec<SessionEvent>> {
        let mut repaired_events = Vec::new();
        if event.sequence > 0 {
            let mut after = self.state.latest_sequence;
            while event.sequence > after.saturating_add(1) {
                let remaining = event.sequence - after - 1;
                let missing = store
                    .events_after(event.session_id, after, remaining.min(1_024) as usize)
                    .await?;
                anyhow::ensure!(
                    !missing.is_empty(),
                    "session projection could not repair the durable gap after {after} before {}",
                    event.sequence
                );
                for missing_event in missing {
                    anyhow::ensure!(
                        missing_event.sequence == after.saturating_add(1),
                        "session projection repair expected sequence {}, received {}",
                        after.saturating_add(1),
                        missing_event.sequence
                    );
                    self.observe(&missing_event)?;
                    after = missing_event.sequence;
                    repaired_events.push(missing_event);
                }
            }
        }
        self.observe(event)?;
        Ok(repaired_events)
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
    lock_path: &Path,
    control_socket_path: &Path,
    session_id: Uuid,
) -> Result<SessionWriterLease> {
    // A crashed owner releases the OS file lock even if its control socket and
    // owner metadata remain on disk. Prefer the lock as the source of truth so
    // a stale socket cannot make resume fail before the new actor starts.
    if let Some(writer) = SessionWriterLease::try_acquire(lock_path)? {
        return Ok(writer);
    }
    let sessions_dir = lock_path.parent().unwrap_or_else(|| Path::new("."));
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    if let Err(error) = send_local_session_command(
        control_socket_path,
        session_id,
        HostCommand::Stop { session_id },
    )
    .await
    {
        let control_endpoint_gone = !control_socket_path.exists()
            && error
                .root_cause()
                .downcast_ref::<io::Error>()
                .is_some_and(|error| error.kind() == io::ErrorKind::NotFound);
        let safely_obsolete =
            obsolete_local_session_owner_pid(sessions_dir, session_id, lock_path)?.is_some();
        if !control_endpoint_gone && !safely_obsolete {
            return Err(error).context("failed to ask the obsolete local session owner to stop");
        }
        if safely_obsolete {
            tracing::warn!(
                session_id = %session_id,
                "obsolete local session owner did not accept the graceful stop command; will escalate after the handoff grace period"
            );
        }
        // The owner can remove its control socket just before its process
        // releases the writer lease. Keep polling the authoritative lock
        // instead of turning that handoff race into a failed resume.
    }
    loop {
        if let Some(writer) = SessionWriterLease::try_acquire(lock_path)? {
            return Ok(writer);
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    if let Some(pid) = obsolete_local_session_owner_pid(sessions_dir, session_id, lock_path)? {
        tracing::warn!(
            session_id = %session_id,
            pid,
            "force-terminating the verified obsolete local session owner"
        );
        force_terminate_local_session_owner(pid)?;
        let force_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            if let Some(writer) = SessionWriterLease::try_acquire(lock_path)? {
                return Ok(writer);
            }
            anyhow::ensure!(
                tokio::time::Instant::now() < force_deadline,
                "verified obsolete local session owner did not release its writer lease after termination"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    anyhow::bail!("obsolete local session owner did not release its writer lease")
}

async fn run_local_agent_session(
    args: &LocalAgentCliArgs,
    selected_session: Option<Uuid>,
    restored_prompt: Option<(String, Vec<PathBuf>)>,
    session_root_override: Option<&Path>,
    crash_context: Arc<TuiCrashContext>,
    reusable_terminal: Option<BorgTerminal>,
) -> Result<Option<(Uuid, Option<(String, Vec<PathBuf>)>, Option<BorgTerminal>)>> {
    let mut agent_config = AgentConfig::load(args.config.as_deref())?;
    let _local_provider_env = agent_config.apply_local_provider_env();
    let dictation_config = LocalDictationConfig::from_env();
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
        session_id_if_present(sqlite_store.as_ref(), session_id).await?
    } else if args.continue_latest {
        let current_dir = args
            .cwd
            .as_deref()
            .unwrap_or_else(|| Path::new("."))
            .canonicalize()
            .context("current project directory does not exist")?;
        latest_session_id_in_directory(&sessions_dir, sqlite_store.as_ref(), &current_dir)
            .await?
            .context("there are no non-empty local Borg sessions to continue in this directory")?
    } else {
        Uuid::new_v4()
    };
    crash_context.set_session_id(session_id);
    let lock_path = sessions_dir.join(format!("{session_id}.lock"));
    let control_socket_path = session_control_socket_path(&sessions_dir, session_id);
    let remote_launch_present = sqlite_store
        .load_host_launch_metadata(session_id)
        .await?
        .is_some();
    let mut writer = SessionWriterLease::try_acquire(&lock_path)?;
    let mut session_access = if writer.is_some() {
        LocalSessionAccess::Owned
    } else {
        LocalSessionAccess::Attached
    };
    if !sqlite_store.contains_session(session_id).await? {
        anyhow::ensure!(
            !session_access.is_attached(),
            "local Borg session {session_id} has no SQLite state"
        );
        if let Some(workspace_id) = args.workspace {
            sqlite_store
                .create_session_in_workspace(session_id, workspace_id)
                .await?;
        } else {
            sqlite_store.create_session(session_id).await?;
        }
    }
    let store: Arc<dyn SessionStore> = sqlite_store.clone();
    let mut session_state = store.state(session_id).await?;
    // Context usage is intentionally coalesced live state rather than a
    // durable transcript event. Resume must seed the footer from that latest
    // snapshot too; otherwise every long session briefly (or permanently,
    // until the next provider event) advertises the misleading 100% headroom.
    let live_state_events = store.live_events_after(session_id, 0).await?;
    for live in &live_state_events {
        if let SessionEventKind::ContextWindowUpdated {
            context_tokens,
            context_window_tokens,
        } = &live.event.kind
        {
            session_state.usage.context_tokens = Some(*context_tokens);
            session_state.usage.context_window_tokens = Some(*context_window_tokens);
        }
    }
    let mut stale_local_owner = session_access.is_attached()
        && !remote_launch_present
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
                stop_stale_local_owner_and_acquire(&lock_path, &control_socket_path, session_id)
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
    let requested_provider = if args
        .model
        .as_deref()
        .is_some_and(|model| agent_config.has_configured_model(model))
    {
        CodingProvider::OpenAiCompatible
    } else {
        args.provider.into()
    };
    let requested_model = args.model.clone().or_else(|| match requested_provider {
        CodingProvider::Codex => Some(borg_provider::codex_product_model().to_string()),
        CodingProvider::Kimi => Some(borg_provider::kimi_product_model().to_string()),
        CodingProvider::OpenCode => None,
        CodingProvider::OpenRouter => Some(borg_provider::openrouter_product_model().to_string()),
        CodingProvider::OpenAiCompatible => std::env::var("BORG_OPENAI_COMPATIBLE_MODEL")
            .ok()
            .filter(|model| !model.trim().is_empty()),
        CodingProvider::Claude => None,
    });
    let requested_effort = args.effort.clone().or_else(|| match requested_provider {
        CodingProvider::Codex => Some(borg_provider::codex_default_effort().to_string()),
        // OpenRouter spans reasoning and non-reasoning models. Only send its
        // optional reasoning parameter after an explicit user selection.
        CodingProvider::OpenRouter => None,
        CodingProvider::OpenAiCompatible => None,
        CodingProvider::Claude | CodingProvider::OpenCode => None,
        CodingProvider::Kimi => Some(borg_provider::kimi_default_effort().to_string()),
    });
    let (
        recorded_cwd,
        mut provider,
        mut model,
        mut effort,
        fast,
        response_language,
        permission_mode,
    ) = if let Some(recorded_config) = recorded_config {
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
    let cwd = requested_cwd.unwrap_or(recorded_cwd);
    anyhow::ensure!(
        cwd.is_dir(),
        "recorded project directory no longer exists: {}; pass --cwd to resume the session in its new location",
        cwd.display()
    );
    let local_server =
        if provider == CodingProvider::OpenAiCompatible && !session_access.is_attached() {
            let lease = local_server::ensure(&agent_config.local, &cwd, model.as_deref()).await?;
            if model.is_none()
                && let Some(lease) = lease.as_ref()
                && !lease.model().is_empty()
            {
                model = Some(lease.model().to_string());
            }
            lease
        } else {
            None
        };
    anyhow::ensure!(
        !provider.uses_native_harness() || model.is_some(),
        "{provider:?} requires --model or BORG_OPENAI_COMPATIBLE_MODEL"
    );
    let mut capabilities = borg_remote::SessionCapabilities::from(&agent_config.capabilities);
    capabilities.provider_capabilities = probe_provider_admission_capabilities().await;
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
    let local_settings = LocalAgentSettings {
        approval_reviewer_model: agent_config.approvals.reviewer_model.clone(),
        approval_reviewer_effort: agent_config.approvals.reviewer_effort.clone(),
        configured_model_gateways: agent_config.configured_model_gateways(),
        tool_mode: agent_config.capabilities.tool_mode,
    };
    let (mut extension_catalog, extension_servers, extension_workflows) =
        crate::extensions::discover(&cwd, &agent_config.capabilities, &agent_config.extensions)?;
    extension_catalog.apply_editor_customization(&mut editor_preferences, &mut agent_config)?;
    let extension_skill_roots = extension_catalog.active_skill_roots();
    let mut observed_extension_revision = extension_catalog.revision.clone();
    let mut last_blu_discovery_error: Option<String> = None;
    let local_executor = LocalAgentTurnExecutor::with_settings(local_settings)
        .with_external_mcp_servers({
            let mut servers = agent_config.external_mcp_servers();
            servers.extend(extension_servers);
            servers
        })
        .with_extension_skill_roots(extension_skill_roots)
        .with_extension_workflows(extension_workflows)
        .with_extension_api(extension_catalog.api_snapshot());
    #[cfg(feature = "profiling")]
    let local_executor = if !session_access.is_attached() {
        match borg_remote::RuntimeProfiler::start(&sessions_dir, session_id)? {
            Some(profiler) => local_executor.with_profiler(profiler),
            None => local_executor,
        }
    } else {
        local_executor
    };
    let live_extension_executor = local_executor.clone();
    let executor: Arc<dyn AgentTurnExecutor> = Arc::new(local_executor);
    let lifecycle_executor = Arc::clone(&executor);
    let mut rendered = HashMap::new();
    let stdin_is_terminal = io::stdin().is_terminal();
    let machine_output = args.json || args.print || args.gui_owner;
    let can_prompt = stdin_is_terminal && !machine_output;
    let rich_tui_allowed = rich_terminal_can_prompt(
        stdin_is_terminal,
        io::stdout().is_terminal(),
        machine_output,
    ) && !BorgTerminal::fallback_requested();
    if rich_tui_allowed {
        tokio::spawn(async {
            if let Err(error) = borg_provider::refresh_openrouter_model_catalog().await {
                tracing::debug!(%error, "OpenRouter model catalog unavailable; keeping current/manual model fallback");
            }
        });
    }
    let fallback_terminal = can_prompt && !rich_tui_allowed;
    let mut initial_prompt = if !args.prompt.is_empty() {
        Some(args.prompt.join(" "))
    } else if !stdin_is_terminal && !args.gui_owner {
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
    let interactive = can_prompt || args.gui_owner;
    let (mut history, mut history_page_before_sequence) = if can_prompt && !fallback_terminal {
        let bootstrap =
            recent_tui_history(store.as_ref(), session_id, session_state.latest_sequence).await?;
        (bootstrap.events, bootstrap.page_before)
    } else {
        let history = store.read(session_id).await?;
        let page_before = history
            .first()
            .map(|event| event.sequence)
            .unwrap_or_else(|| session_state.latest_sequence.saturating_add(1));
        (history, page_before)
    };
    if can_prompt && !fallback_terminal {
        // Durable history is the transcript source of truth, while the latest
        // coalesced message/reasoning rows are the live tail of an in-flight
        // turn. Seed both in order so resume shows thinking immediately and
        // never treats the ephemeral snapshot as a second durable message.
        history.extend(
            live_state_events
                .into_iter()
                .map(|live| live.event)
                .filter(|event| {
                    matches!(
                        event.kind,
                        SessionEventKind::Message { .. } | SessionEventKind::ReasoningDelta { .. }
                    )
                }),
        );
    }
    let mut history_start_reached = session_state.latest_sequence == 0
        || history.first().is_some_and(|event| event.sequence <= 1);
    let (team_history, team_snapshots) = if can_prompt && !fallback_terminal {
        subagent_state_from_history(&history)
    } else {
        (Vec::new(), Vec::new())
    };
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
        if remote_launch_present {
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
            println!("  Remote mirror enabled (durable-store-first and offline-safe).");
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
    let actor_lock_path = lock_path.clone();
    let registration_template = launch.clone();
    let control_server = if session_access.is_attached() {
        None
    } else {
        Some(
            LocalSessionControlServer::start_with_durable_prompt_admissions(
                control_socket_path.clone(),
                session_id,
                writer.as_ref().expect("session owner holds writer lease"),
                session_command_tx.clone(),
                Some(Arc::clone(&local_prompt_admissions)),
                Arc::clone(&store),
            )?,
        )
    };
    let actor = if session_access.is_attached() {
        tokio::spawn(run_attached_session(
            Arc::clone(&store),
            session_id,
            actor_lock_path,
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
        if let Some(mut terminal) = reusable_terminal {
            terminal.retarget(
                &sessions_dir,
                session_id,
                cwd.clone(),
                &agent_config.keybindings,
            )?;
            terminal.set_configured_model_entries(agent_config.configured_model_entries());
            terminal.set_extension_commands(extension_catalog.api_snapshot().commands);
            Some(terminal)
        } else {
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
        }
    } else {
        None
    };
    crash_context
        .tui_active
        .store(terminal.is_some(), Ordering::Release);
    let startup_update_notice = crate::updater::manual_update_notice();
    let retry_notice = crash_context.take_retry_notice();
    let display_session_state =
        resume_display_state(session_state.clone(), session_access, resuming);
    if let Some(terminal) = terminal.as_mut() {
        terminal.set_configured_model_entries(agent_config.configured_model_entries());
        terminal.set_extension_commands(extension_catalog.api_snapshot().commands);
        terminal.seed_history(&history);
        terminal.seed_team_roster(&team_snapshots);
        terminal.seed_session_state(&display_session_state);
        if let Some(notice) = startup_update_notice.as_deref() {
            terminal.set_notice(notice);
        } else if let Some(notice) = retry_notice.as_deref() {
            terminal.set_notice(notice);
        } else if stale_local_owner {
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
        let icon =
            dictation_icon_style_for_preference(editor_preferences.presentation.dictation_icon);
        terminal.set_dictation_icon(icon);
        if editor_preferences.presentation.dictation_icon.is_none() {
            editor_preferences.presentation.dictation_icon = Some(icon);
            editor_preferences.save()?;
            terminal.open_dictation_icon_picker();
        }
        terminal.set_auto_expand_edits(editor_preferences.presentation.auto_expand_edits);
        terminal.set_auto_expand_tools(editor_preferences.presentation.auto_expand_tools);
        terminal.set_running_sweeps(editor_preferences.presentation.running_sweeps);
        terminal.set_layout_preferences(&editor_preferences.layout);
        terminal.set_completion_alerts(
            editor_preferences.interaction.completion_notifications,
            editor_preferences.interaction.completion_sound,
        );
        terminal.set_transcript_labels(
            editor_preferences.transcript.user_label.clone(),
            editor_preferences.transcript.assistant_label.clone(),
        );
        terminal.set_transcript_colors(&editor_preferences.transcript);
        if let Some((text, attachments)) = restored_prompt {
            terminal.restore_composer(text, attachments);
        }
        terminal.draw()?;
    } else if let Some(notice) = startup_update_notice.as_deref() {
        eprintln!("\n  {notice}\n");
    } else if let Some(notice) = retry_notice.as_deref() {
        eprintln!("\n  {notice}\n");
    }
    let mut displayed_update_notice = startup_update_notice;
    // Pending prompts are durable queue state, not part of the bounded
    // transcript bootstrap. Hydrate them after first paint so a long queue
    // cannot make resume wait on the full recovery projection.
    let mut pending_prompt_task = (resuming && terminal.is_some()).then(|| {
        let pending_store = Arc::clone(&store);
        tokio::spawn(async move {
            pending_store
                .recovery(session_id)
                .await
                .map(|recovery| recovery.queue_events)
        })
    });
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
            let before = history_page_before_sequence;
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
    let mut force_exit_requested = false;
    let mut exit_notice = None;
    let mut detached_from_terminal = false;
    let mut detached_prompt = None;
    let mut handoff_on_safe_boundary = false;
    let mut last_ctrl_c = None;
    let mut terminal_dirty = false;
    let mut tool_started_frame_hold_until = None;
    let mut tui_fps = tui_refresh_rate(u64::from(editor_preferences.presentation.refresh_rate_fps));
    let mut prevent_sleep = editor_preferences.interaction.prevent_sleep;
    let mut steer_active_turn =
        editor_preferences.interaction.active_messages == ActiveMessageBehavior::Steer;
    if let Some(terminal) = terminal.as_mut() {
        terminal.set_active_message_behavior(steer_active_turn);
    }
    let mut resume_session = None;
    let mut rewind_prompt = None;
    let mut pending_revert_sequence = None;
    let mut revert_fork_task: Option<RevertForkTask> = None;
    let mut sleep_inhibitor = SleepInhibitor::new(prevent_sleep);
    let mut dictation_recorder: Option<LocalDictationRecorder> = None;
    let mut dictation_task: Option<tokio::task::JoinHandle<Result<String>>> = None;
    let mut dictation_setup_task: Option<tokio::task::JoinHandle<Result<LocalDictationBackend>>> =
        None;
    let mut dictation_backend: Option<LocalDictationBackend> = None;
    sleep_inhibitor.set_turn_active(matches!(
        status,
        SessionStatus::Starting | SessionStatus::Running | SessionStatus::WaitingForApproval
    ));
    let mut render_frame_interval = tui_frame_interval(tui_fps);
    let mut render_tick = tui_render_interval(render_frame_interval);
    let mut activity_tick = tokio::time::interval(ACTIVITY_FRAME_INTERVAL);
    activity_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut idle_tick = tokio::time::interval(IDLE_FRAME_INTERVAL);
    idle_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut cache_tick = tokio::time::interval(std::time::Duration::from_secs(30));
    cache_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut agent_config_tick = tokio::time::interval(std::time::Duration::from_millis(500));
    agent_config_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut blu_tick = tokio::time::interval(EXTENSION_DISCOVERY_INTERVAL);
    blu_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut update_tick = tokio::time::interval(std::time::Duration::from_secs(1));
    update_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut blu_discovery_task: Option<BluDiscoveryTask> = None;
    let mut shutdown_signal_open = true;
    let mut session_event_stream_open = true;
    loop {
        tokio::select! {
            result = async {
                dictation_setup_task
                    .as_mut()
                    .expect("dictation setup branch is guarded")
                    .await
            }, if dictation_setup_task.is_some() => {
                dictation_setup_task = None;
                match result {
                    Ok(Ok(backend)) => {
                        let config = backend.config();
                        dictation_backend = Some(backend);
                        match LocalDictationRecorder::start(&config) {
                            Ok(recorder) => {
                                dictation_recorder = Some(recorder);
                                let terminal = terminal.as_mut().expect("terminal");
                                terminal.set_dictation_state(DictationState::Recording);
                                terminal.set_notice(
                                    "Recording locally · click stop or use the dictate keybinding"
                                        .to_string(),
                                );
                            }
                            Err(error) => terminal
                                .as_mut()
                                .expect("terminal")
                                .set_notice(format!("Could not start dictation: {error:#}")),
                        }
                    }
                    Ok(Err(error)) => {
                        if let Some(terminal) = terminal.as_mut() {
                            terminal.set_dictation_state(DictationState::Idle);
                            terminal.set_notice(format!("Could not prepare dictation: {error:#}"));
                        }
                    }
                    Err(error) => {
                        if let Some(terminal) = terminal.as_mut() {
                            terminal.set_dictation_state(DictationState::Idle);
                            terminal.set_notice(format!("Dictation setup task failed: {error}"));
                        }
                    }
                }
                terminal_dirty = true;
            }
            result = async {
                dictation_task
                    .as_mut()
                    .expect("dictation completion branch is guarded")
                    .await
            }, if dictation_task.is_some() => {
                dictation_task = None;
                if let Some(terminal) = terminal.as_mut() {
                    terminal.set_dictation_state(DictationState::Idle);
                    match result {
                        Ok(Ok(text)) if !text.is_empty() => terminal.insert_dictation(&text),
                        Ok(Ok(_)) => {}
                        Ok(Err(error)) => {
                            terminal.set_notice(format!("Dictation failed: {error:#}"));
                        }
                        Err(error) => {
                            terminal.set_notice(format!("Dictation task failed: {error}"));
                        }
                    }
                    terminal_dirty = true;
                }
            }
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
            pending_prompt_result = async {
                pending_prompt_task
                    .as_mut()
                    .expect("pending-prompt task branch is guarded")
                    .await
            }, if pending_prompt_task.is_some() => {
                pending_prompt_task = None;
                match pending_prompt_result {
                    Ok(Ok(pending_prompt_events)) => {
                        if let Some(terminal) = terminal.as_mut() {
                            terminal.seed_pending_prompt_events(&pending_prompt_events);
                            terminal_dirty = true;
                        }
                    }
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "could not hydrate pending prompts after first paint");
                    }
                    Err(error) => {
                        tracing::warn!(%error, "pending prompt hydration task failed");
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
                        if let Some(before) = older
                            .iter()
                            .map(|event| event.sequence)
                            .filter(|sequence| *sequence > 0)
                            .min()
                        {
                            history_page_before_sequence = before;
                        }
                        merge_tui_history_page(&mut history, older);
                        history_start_reached = history_page_before_sequence <= 1;
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
            revert_result = async {
                revert_fork_task
                    .as_mut()
                    .expect("revert-fork task branch is guarded")
                    .await
            }, if revert_fork_task.is_some() => {
                match revert_result {
                    Ok(Ok(fork_id)) => {
                        resume_session = Some(fork_id);
                        break;
                    }
                    Ok(Err(error)) => {
                        let message = format!("Revert failed; keeping the original session: {error:#}");
                        tracing::warn!(%session_id, %error, "session revert failed");
                        rewind_prompt = None;
                        resume_session = Some(session_id);
                        exit_notice = Some(message);
                        break;
                    }
                    Err(error) => {
                        let message = format!("Revert failed; keeping the original session: {error}");
                        tracing::warn!(%session_id, %error, "session revert task failed");
                        rewind_prompt = None;
                        resume_session = Some(session_id);
                        exit_notice = Some(message);
                        break;
                    }
                }
            }
            _ = render_tick.tick(), if terminal.is_some() && terminal_dirty => {
                if tool_started_frame_hold_until
                    .is_some_and(|until| tokio::time::Instant::now() < until)
                {
                    continue;
                }
                tool_started_frame_hold_until = None;
                let terminal = terminal.as_mut().expect("terminal");
                let interaction_frame = terminal.has_pending_scroll_frame();
                terminal.advance_scroll_frame();
                let history_requested = terminal.take_history_page_request();
                if history_requested && !history_start_reached {
                    terminal.set_history_page_loading(true);
                    if history_page_task.is_none() {
                        let before = history_page_before_sequence;
                        let history_store = Arc::clone(&store);
                        history_page_task = Some(tokio::spawn(async move {
                            older_tui_history(history_store.as_ref(), session_id, before).await
                        }));
                    }
                }
                let draw_started = std::time::Instant::now();
                terminal.draw()?;
                let activity_frame = terminal_needs_activity_tick(status)
                    || terminal.has_running_tool()
                    || terminal.has_active_subagents()
                    || terminal.has_active_splash_animation()
                    || terminal.is_history_page_loading();
                let next_interval = responsive_tui_frame_interval(
                    tui_fps,
                    draw_started.elapsed(),
                    interaction_frame,
                    activity_frame,
                );
                if next_interval != render_frame_interval {
                    render_frame_interval = next_interval;
                    render_tick = tui_render_interval(render_frame_interval);
                }
                terminal_dirty = terminal.has_pending_scroll_frame();
            }
            _ = activity_tick.tick(), if terminal.as_ref().is_some_and(|terminal| {
                terminal_needs_activity_tick(status)
                    || terminal.has_running_tool()
                    || terminal.has_active_subagents()
                    || terminal.has_active_splash_animation()
                    || terminal.is_history_page_loading()
            }) => {
                terminal_dirty = true;
            }
            _ = idle_tick.tick(), if terminal.as_ref().is_some_and(|terminal| {
                terminal_needs_idle_tick(
                    terminal.has_expiring_notice(),
                    terminal.has_blinking_cursor(),
                )
            }) => {
                terminal_dirty = true;
            }
            _ = cache_tick.tick(), if terminal.as_ref().is_some_and(
                crate::terminal_ui::BorgTerminal::has_cache_idle_timer
            ) => {
                terminal_dirty = true;
            }
            _ = update_tick.tick() => {
                sleep_inhibitor.refresh();
                if let Some(notice) = crate::updater::manual_update_notice()
                    && displayed_update_notice.as_deref() != Some(notice.as_str())
                {
                    displayed_update_notice = Some(notice.clone());
                    if let Some(terminal) = terminal.as_mut() {
                        terminal.set_notice(notice);
                        terminal_dirty = true;
                    } else {
                        eprintln!("\n  {notice}\n");
                    }
                }
            }
            _ = agent_config_tick.tick(), if interactive => {
                let signature = agent_config_file_signature(agent_config_path.as_deref());
                if signature != agent_config_signature {
                    match AgentConfig::load(args.config.as_deref()) {
                        Ok(next) => {
                            let keybindings_changed = next.keybindings != agent_config.keybindings;
                            agent_config = next;
                            if let Some(terminal) = terminal.as_mut() {
                                terminal.set_configured_model_entries(
                                    agent_config.configured_model_entries(),
                                );
                                terminal.set_extension_commands(
                                    extension_catalog.api_snapshot().commands,
                                );
                            }
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
                let extension_config = agent_config.extensions.clone();
                blu_discovery_task = Some(tokio::task::spawn_blocking(move || {
                    crate::extensions::discover(
                        &discovery_cwd,
                        &discovery_capabilities,
                        &extension_config,
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
                    Ok(Ok((next_catalog, next_extension_servers, next_extension_workflows)))
                        if next_catalog.revision != observed_extension_revision =>
                    {
                        observed_extension_revision = next_catalog.revision.clone();
                        if !next_catalog.has_errors() {
                            last_blu_discovery_error = None;
                            let mut next_agent_config =
                                AgentConfig::load(args.config.as_deref())?;
                            let mut next_editor_preferences = EditorPreferences::load()?;
                            next_catalog.apply_editor_customization(
                                &mut next_editor_preferences,
                                &mut next_agent_config,
                            )?;
                            let mut servers = agent_config.external_mcp_servers();
                            servers.extend(next_extension_servers);
                            live_extension_executor.replace_runtime_extensions_with_api(
                                servers,
                                next_catalog.active_skill_roots(),
                                next_extension_workflows,
                                next_catalog.api_snapshot(),
                            );
                            extension_catalog = next_catalog;
                            agent_config.keybindings = next_agent_config.keybindings;
                            agent_config.commands.aliases = next_agent_config.commands.aliases;
                            editor_preferences = next_editor_preferences;
                            tui_fps = tui_refresh_rate(u64::from(
                                editor_preferences.presentation.refresh_rate_fps,
                            ));
                            prevent_sleep = editor_preferences.interaction.prevent_sleep;
                            steer_active_turn = editor_preferences.interaction.active_messages
                                == ActiveMessageBehavior::Steer;
                            if let Some(terminal) = terminal.as_mut() {
                                terminal.reload_keybindings(&agent_config.keybindings)?;
                                terminal.set_auto_expand_edits(
                                    editor_preferences.presentation.auto_expand_edits,
                                );
                                terminal.set_auto_expand_tools(
                                    editor_preferences.presentation.auto_expand_tools,
                                );
                                terminal.set_running_sweeps(
                                    editor_preferences.presentation.running_sweeps,
                                );
                                terminal.set_layout_preferences(&editor_preferences.layout);
                                terminal.set_completion_alerts(
                                    editor_preferences.interaction.completion_notifications,
                                    editor_preferences.interaction.completion_sound,
                                );
                                terminal.set_transcript_labels(
                                    editor_preferences.transcript.user_label.clone(),
                                    editor_preferences.transcript.assistant_label.clone(),
                                );
                                terminal.set_transcript_colors(&editor_preferences.transcript);
                                terminal.set_extension_commands(
                                    extension_catalog.api_snapshot().commands,
                                );
                                terminal.set_notice(
                                    "Blu reloaded · editor and extension APIs are live · runtime applies next turn"
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
                    Ok(Ok((next_catalog, _, _))) => {
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
            event = session_events.recv(), if session_event_stream_open => {
                let Some(event) = event else {
                    session_event_stream_open = false;
                    if revert_fork_task.is_none()
                        && let Some(sequence) = take_revert_ready_to_fork(
                            &mut pending_revert_sequence,
                            status,
                            true,
                        )
                    {
                        // A closed event stream means the actor cannot append
                        // more events. Fork the immutable historical prefix
                        // even if its final Stopped projection was not seen.
                        revert_fork_task = Some(spawn_revert_fork(
                            Arc::clone(&store),
                            session_id,
                            sequence,
                        ));
                    }
                    if revert_fork_task.is_none() {
                        break;
                    }
                    continue;
                };
                let handoff_stale_owner = stale_local_owner
                    && matches!(
                        event.kind,
                        SessionEventKind::StatusChanged {
                            status: SessionStatus::Ready,
                            ..
                        }
                    );
                let repaired_events = delivered_projection
                    .observe_from_store(store.as_ref(), &event)
                    .await?;
                for repaired_event in repaired_events {
                    if let Some(terminal) = terminal.as_mut() {
                        terminal_dirty |= terminal.apply_session_event(&repaired_event);
                        if !history.iter().any(|loaded| {
                            loaded.sequence > 0 && loaded.sequence == repaired_event.sequence
                        }) {
                            let insertion = history
                                .iter()
                                .position(|loaded| loaded.sequence > repaired_event.sequence)
                                .unwrap_or(history.len());
                            history.insert(insertion, repaired_event);
                        }
                    } else if !detached_from_terminal {
                        render_event(&repaired_event, args.json, args.print, &mut rendered)?;
                    }
                }
                if let Some(message_id) = committed_prompt_id(&event.kind) {
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
                }
                if let SessionEventKind::StatusChanged { status: next, .. } = &event.kind {
                    status = *next;
                    saw_running |= *next == SessionStatus::Running;
                    sleep_inhibitor.set_turn_active(matches!(
                        next,
                        SessionStatus::Starting
                            | SessionStatus::Running
                            | SessionStatus::WaitingForApproval
                    ));
                    if revert_fork_task.is_none()
                        && let Some(sequence) = take_revert_ready_to_fork(
                            &mut pending_revert_sequence,
                            *next,
                            false,
                        )
                    {
                        revert_fork_task = Some(spawn_revert_fork(
                            Arc::clone(&store),
                            session_id,
                            sequence,
                        ));
                        if let Some(terminal) = terminal.as_mut() {
                            terminal.set_notice("Session stopped · creating reverted session…");
                            terminal_dirty = true;
                        }
                    }
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
                        total_tokens,
                        cost_usd,
                        ..
                    } => session_usage.add(
                        *input_tokens,
                        *output_tokens,
                        *cached_input_tokens,
                        *total_tokens,
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
                    if terminal_dirty && session_event_needs_immediate_frame(&event.kind) {
                        terminal.draw()?;
                        terminal_dirty = terminal.has_pending_scroll_frame();
                        tool_started_frame_hold_until = Some(
                            tokio::time::Instant::now() + TOOL_STARTED_FRAME_MIN_DURATION,
                        );
                    }
                } else if !detached_from_terminal {
                    render_event(&event, args.json, args.print, &mut rendered)?;
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
                            // Re-enter the same durable session with the
                            // existing terminal instead of tearing down the
                            // screen and making an automatic build handoff
                            // look like a user-requested resume.
                            resume_session = Some(session_id);
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
                if is_usage_command(line) {
                    let summary = usage_summary(provider, &session_usage).await;
                    println!(
                        "\n  Usage\n{}\n",
                        indent_usage_summary(&summary)
                    );
                    continue;
                }
                if line == "/lsp" {
                    println!("\n{}\n", lsp_support_summary());
                    continue;
                }
                if line == "/icons" {
                    println!("\n  Microphone icon: choose /icons nerd or /icons emoji.\n");
                    continue;
                }
                if let Some(value) = line.strip_prefix("/icons ") {
                    if let Some(style) = parse_dictation_icon_style(value) {
                        editor_preferences.presentation.dictation_icon = Some(style);
                        editor_preferences.save()?;
                        println!(
                            "\n  Microphone icon: {}.\n",
                            match style {
                                DictationIconStyle::NerdFont => "Nerd Font 󰍬",
                                DictationIconStyle::Emoji => "emoji 🎤",
                            }
                        );
                    } else {
                        eprintln!("\n  Choose /icons nerd or /icons emoji.\n");
                    }
                    continue;
                }
                if matches!(
                    line,
                    "/settings" | "/followups" | "/refresh" | "/sleep"
                ) {
                    println!(
                        "\n  Settings\n  Model: {}\n  Effort: {}\n  Fast mode: {}\n  Active messages: {}\n  Refresh: {tui_fps} FPS\n  Keep machine awake: {}\n  User label: {}\n  Assistant label: {}\n  Use /model NAME, /effort LEVEL, /fast on|off, /followups steer|queue, /refresh FPS, /sleep on|off, /user-label TEXT, or /assistant-label TEXT.\n",
                        current_model.as_deref().unwrap_or("provider default"),
                        current_effort.as_deref().unwrap_or("provider default"),
                        if current_fast { "on" } else { "off" },
                        if steer_active_turn {
                            "send now and redirect the current turn"
                        } else {
                            "send after the current turn finishes"
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
                            steer_active_turn = true;
                            editor_preferences.interaction.active_messages =
                                ActiveMessageBehavior::Steer;
                        }
                        "queue" => {
                            steer_active_turn = false;
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
                        "\n  Messages sent while Borg works: {}.\n",
                        if steer_active_turn {
                            "send now and redirect the current turn"
                        } else {
                            "send after the current turn finishes"
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
                        "\n  Keep machine awake during active turns: {}.\n",
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
                if let Some(prompt) = director_prompt_command(line) {
                    match prompt {
                        Ok(prompt) => {
                            let active = matches!(
                                status,
                                SessionStatus::Starting
                                    | SessionStatus::Running
                                    | SessionStatus::WaitingForApproval
                            );
                            let delivery = director_prompt_delivery(
                                active,
                                provider,
                                steer_active_turn,
                            );
                            let message_id = Uuid::new_v4();
                            if session_command_tx
                                .send(director_prompt_host_command(
                                    session_id,
                                    message_id,
                                    prompt,
                                    Vec::new(),
                                    delivery,
                                ))
                                .await
                                .is_err()
                            {
                                eprintln!("\n  Could not reach the director thread.\n");
                            }
                        }
                        Err(error) => eprintln!("\n  {error}\n"),
                    }
                    continue;
                }
                if let Some(command) = persistent_sidecar_command(line) {
                    match command {
                        Ok((sidecar, intent)) => {
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
                                    steer_active_turn,
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
                        }
                        Err(error) => eprintln!("\n  {error}\n"),
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
                        lifecycle_executor.stop_session(session_id).await?;
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
                                steer_active_turn,
                            )
                        } else {
                            idle_input(line)
                        };
                        if !text.is_empty() {
                            let message_id = Uuid::new_v4();
                            persist_prompt_admission(
                                store.as_ref(),
                                session_id,
                                message_id,
                                &text,
                                &[],
                                delivery,
                            )
                            .await?;
                            pending_prompt_ids.insert(message_id);
                            if session_command_tx.send(HostCommand::Prompt {
                                session_id,
                                message_id,
                                text,
                                attachments: Vec::new(),
                                output_schema: None,
                                delivery,
                            }).await.is_err() {
                                tracing::warn!(
                                    %message_id,
                                    "durably admitted prompt is waiting for the session actor to reconnect"
                                );
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
                // Resume hydration is deliberately deferred until after the
                // first paint. Up may depend on either deferred projection,
                // but an already hydrated local history or an in-progress
                // multiline edit must not wait on storage before being handled.
                if terminal_event.is_up() {
                    let should_wait_for_composer_history = terminal
                        .as_ref()
                        .is_some_and(|terminal| {
                            !terminal.has_composer_history() && terminal.up_may_recall_history()
                        });
                    if should_wait_for_composer_history
                        && let Some(task) = composer_history_task.take()
                    {
                        match task.await {
                            Ok(Ok(composer_history)) => {
                                if let Some(terminal) = terminal.as_mut() {
                                    terminal.seed_composer_history(&composer_history);
                                }
                            }
                            Ok(Err(error)) => {
                                tracing::warn!(%error, "could not hydrate composer history before recall");
                            }
                            Err(error) => {
                                tracing::warn!(%error, "composer history hydration task failed before recall");
                            }
                        }
                    }
                    let should_wait_for_pending_prompts = terminal.as_ref().is_some_and(|terminal| {
                        terminal.has_empty_composer_text() && !terminal.has_active_queued_prompts()
                    });
                    if should_wait_for_pending_prompts
                        && let Some(task) = pending_prompt_task.take()
                    {
                        match task.await {
                            Ok(Ok(pending_prompt_events)) => {
                                if let Some(terminal) = terminal.as_mut() {
                                    terminal.seed_pending_prompt_events(&pending_prompt_events);
                                }
                            }
                            Ok(Err(error)) => {
                                tracing::warn!(%error, "could not hydrate pending prompts before recall");
                            }
                            Err(error) => {
                                tracing::warn!(%error, "pending prompt hydration task failed before recall");
                            }
                        }
                    }
                }
                let input_is_keyboard = terminal_event.is_keyboard_input();
                let terminal_dirty_before_input = terminal_dirty;
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
                if should_draw_input_fast_path(
                    input_is_keyboard,
                    event_redraw_needed,
                    matches!(&action, UiAction::None),
                    terminal_dirty_before_input,
                    !terminal.as_ref().expect("terminal").is_launch_screen(),
                ) {
                    terminal
                        .as_mut()
                        .expect("terminal")
                        .draw_for_input()?;
                    terminal_dirty = terminal_dirty_before_input
                        || terminal
                            .as_ref()
                            .expect("terminal")
                            .has_pending_scroll_frame();
                }
                if matches!(&action, UiAction::None)
                    && event_redraw_needed
                    && terminal.as_ref().expect("terminal").is_launch_screen()
                {
                    terminal.as_mut().expect("terminal").draw()?;
                    terminal_dirty = false;
                }
                match action {
                    UiAction::None => {}
                    UiAction::ToggleGoal { action } => {
                        let terminal = terminal.as_mut().expect("terminal");
                        let changed = terminal.optimistically_apply_goal_action(&action);
                        if changed
                            && matches!(&action, GoalAction::Resume)
                            && !matches!(
                                status,
                                SessionStatus::Starting
                                    | SessionStatus::Running
                                    | SessionStatus::WaitingForApproval
                            )
                        {
                            status = SessionStatus::Starting;
                            sleep_inhibitor.set_turn_active(true);
                        }
                        terminal.draw()?;
                        terminal_dirty = terminal.has_pending_scroll_frame();
                        if !dispatch_host_command_without_blocking(
                            &session_command_tx,
                            HostCommand::Goal {
                                session_id,
                                action,
                            },
                        ) {
                            terminal.set_notice("Could not reach the session actor");
                        }
                    }
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
                    UiAction::FlushPendingInput { target, prompt } => {
                        if let Some((message_id, text, attachments)) = prompt {
                            if let Err(error) = persist_prompt_admission(
                                store.as_ref(),
                                target.unwrap_or(session_id),
                                message_id,
                                &text,
                                &attachments,
                                PromptDelivery::Steer,
                            )
                            .await
                            {
                                let terminal = terminal.as_mut().expect("terminal");
                                terminal.reject_optimistic_prompt(
                                    target,
                                    message_id,
                                    text,
                                    attachments,
                                );
                                terminal.set_notice(format!(
                                    "Could not durably send the pending input: {error:#}"
                                ));
                                terminal.draw()?;
                                terminal_dirty = false;
                            } else {
                                pending_prompt_ids.insert(message_id);
                                let command = target.map_or_else(
                                    || HostCommand::Prompt {
                                        session_id,
                                        message_id,
                                        text: text.clone(),
                                        attachments: attachments.clone(),
                                        output_schema: None,
                                        delivery: PromptDelivery::Steer,
                                    },
                                    |target| HostCommand::Subagent {
                                        session_id,
                                        action: SubagentAction::Prompt {
                                            request_id: Uuid::new_v4(),
                                            target: target.to_string(),
                                            message_id,
                                            text: text.clone(),
                                            attachments: attachments.clone(),
                                            delivery: PromptDelivery::Steer,
                                        },
                                    },
                                );
                                if session_command_tx.send(command).await.is_err() {
                                    terminal.as_mut().expect("terminal").set_notice(
                                        "Prompt saved durably · waiting for the session actor to reconnect"
                                            .to_string(),
                                    );
                                }
                            }
                        }
                        let command = target.map_or_else(
                            || HostCommand::FlushPendingInput { session_id },
                            |target| HostCommand::Subagent {
                                session_id,
                                action: SubagentAction::FlushPendingInput {
                                    request_id: Uuid::new_v4(),
                                    target: target.to_string(),
                                },
                            },
                        );
                        session_command_tx.send(command).await.ok();
                    }
                    UiAction::Rewind {
                        sequence,
                        text,
                        attachments,
                    } => {
                        if pending_revert_sequence.is_some() || revert_fork_task.is_some() {
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .set_notice("A revert is already in progress".to_string());
                        } else if revert_start_mode(status) == RevertStartMode::ForkNow {
                            rewind_prompt = Some((text, attachments));
                            revert_fork_task = Some(spawn_revert_fork(
                                Arc::clone(&store),
                                session_id,
                                sequence,
                            ));
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .set_notice("Creating reverted session…".to_string());
                        } else {
                            rewind_prompt = Some((text, attachments));
                            pending_revert_sequence = Some(sequence);
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .set_notice("Stopping session before reverting…".to_string());
                            if !stop_sent {
                                stop_sent = true;
                                if session_command_tx
                                    .send(HostCommand::Stop { session_id })
                                    .await
                                    .is_err()
                                    && let Some(sequence) = pending_revert_sequence.take()
                                {
                                    // The command receiver is gone, so the
                                    // actor cannot mutate the session further.
                                    revert_fork_task = Some(spawn_revert_fork(
                                        Arc::clone(&store),
                                        session_id,
                                        sequence,
                                    ));
                                    terminal.as_mut().expect("terminal").set_notice(
                                        "Session already stopped · creating reverted session…"
                                            .to_string(),
                                    );
                                }
                            }
                        }
                    }
                    UiAction::RevertTo { sequence } => {
                        if pending_revert_sequence.is_some() || revert_fork_task.is_some() {
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .set_notice("A revert is already in progress".to_string());
                        } else if revert_start_mode(status) == RevertStartMode::ForkNow {
                            rewind_prompt = None;
                            revert_fork_task = Some(spawn_revert_fork(
                                Arc::clone(&store),
                                session_id,
                                sequence,
                            ));
                            terminal.as_mut().expect("terminal").set_notice(
                                "Creating reverted session…".to_string(),
                            );
                        } else {
                            rewind_prompt = None;
                            pending_revert_sequence = Some(sequence);
                            terminal.as_mut().expect("terminal").set_notice(
                                "Stopping session before reverting…".to_string(),
                            );
                            if !stop_sent {
                                stop_sent = true;
                                if session_command_tx
                                    .send(HostCommand::Stop { session_id })
                                    .await
                                    .is_err()
                                    && let Some(sequence) = pending_revert_sequence.take()
                                {
                                    revert_fork_task = Some(spawn_revert_fork(
                                        Arc::clone(&store),
                                        session_id,
                                        sequence,
                                    ));
                                    terminal.as_mut().expect("terminal").set_notice(
                                        "Session already stopped · creating reverted session…"
                                            .to_string(),
                                    );
                                }
                            }
                        }
                    }
                    UiAction::SetModel(model) => {
                        let active = terminal
                            .as_ref()
                            .and_then(BorgTerminal::session_provider)
                            .unwrap_or(provider);
                        let configured = agent_config.has_configured_model(&model);
                        let target = if configured {
                            CodingProvider::OpenAiCompatible
                        } else {
                            CodingProvider::for_model(&model).unwrap_or(active)
                        };
                        if !configured && !provider_credentials_present(target) {
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
                            if outcome.is_ok() {
                                // Authentication changes are part of the
                                // provider lifecycle. Drop any retained
                                // native subscription process so the next
                                // turn cannot accidentally use the old
                                // credential context.
                                lifecycle_executor.stop_session(session_id).await?;
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
                            restored.set_dictation_icon(dictation_icon_style_for_preference(
                                editor_preferences.presentation.dictation_icon,
                            ));
                            restored.set_active_message_behavior(steer_active_turn);
                            restored.set_configured_model_entries(agent_config.configured_model_entries());
                            restored.set_extension_commands(extension_catalog.api_snapshot().commands);
                            let composer_history = store
                                .recent_user_messages(session_id, RICH_TUI_PROMPT_HISTORY_LIMIT)
                                .await?;
                            restored.seed_composer_history(&composer_history);
                            restored.seed_history(&latest.events);
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
                            "Keep machine awake during active turns: {}",
                            if enabled { "on" } else { "off" }
                        ));
                    }
                    UiAction::SetSteerActive(enabled) => {
                        steer_active_turn = enabled;
                        editor_preferences.interaction.active_messages = if enabled {
                            ActiveMessageBehavior::Steer
                        } else {
                            ActiveMessageBehavior::Queue
                        };
                        editor_preferences.save()?;
                        let terminal = terminal.as_mut().expect("terminal");
                        terminal.set_active_message_behavior(enabled);
                        terminal.set_notice(format!(
                            "Messages sent while Borg works: {}",
                            if enabled {
                                "send now and redirect the current turn"
                            } else {
                                "send after the current turn finishes"
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
                    UiAction::SetRunningSweeps(enabled) => {
                        editor_preferences.presentation.running_sweeps = enabled;
                        editor_preferences.save()?;
                        let terminal = terminal.as_mut().expect("terminal");
                        terminal.set_running_sweeps(enabled);
                        terminal.set_notice(format!(
                            "Running sweep animations: {}",
                            if enabled { "on" } else { "off" }
                        ));
                    }
                    UiAction::SetCompletionNotifications(policy) => {
                        editor_preferences.interaction.completion_notifications = policy;
                        editor_preferences.save()?;
                        let terminal = terminal.as_mut().expect("terminal");
                        terminal.set_completion_alerts(
                            policy,
                            editor_preferences.interaction.completion_sound,
                        );
                        terminal.set_notice(format!(
                            "Completion notifications: {}",
                            completion_alert_policy_name(policy)
                        ));
                    }
                    UiAction::SetCompletionSound(policy) => {
                        editor_preferences.interaction.completion_sound = policy;
                        editor_preferences.save()?;
                        let terminal = terminal.as_mut().expect("terminal");
                        terminal.set_completion_alerts(
                            editor_preferences.interaction.completion_notifications,
                            policy,
                        );
                        terminal.set_notice(format!(
                            "Completion sound: {}",
                            completion_alert_policy_name(policy)
                        ));
                    }
                    UiAction::SetDictationIcon(style) => {
                        editor_preferences.presentation.dictation_icon = Some(style);
                        editor_preferences.save()?;
                        let terminal = terminal.as_mut().expect("terminal");
                        terminal.set_dictation_icon(style);
                        terminal.set_notice(format!(
                            "Microphone icon: {}",
                            match style {
                                DictationIconStyle::NerdFont => "Nerd Font 󰍬",
                                DictationIconStyle::Emoji => "emoji 🎤",
                            }
                        ));
                    }
                    UiAction::ToggleDictation => {
                        if dictation_task.is_some() || dictation_setup_task.is_some() {
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .set_notice("Dictation is still preparing or transcribing".to_string());
                        } else if let Some(recorder) = dictation_recorder.take() {
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .set_dictation_state(DictationState::Transcribing);
                            let config = dictation_backend
                                .as_ref()
                                .map(LocalDictationBackend::config)
                                .unwrap_or_else(|| dictation_config.clone());
                            dictation_task = Some(tokio::spawn(async move {
                                recorder.finish_and_transcribe(config).await
                            }));
                        } else if dictation_backend.is_none() && dictation_config.requires_setup() {
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .set_dictation_state(DictationState::Installing);
                            terminal.as_mut().expect("terminal").set_notice(if parakeet_is_installed(&dictation_config) {
                                "Starting local dictation".to_string()
                            } else if dictation_config.uses_bundled_model() {
                                "Preparing local dictation · the default model downloads about 609 MiB"
                                    .to_string()
                            } else {
                                "Preparing local dictation runtime".to_string()
                            });
                            let config = dictation_config.clone();
                            dictation_setup_task = Some(tokio::spawn(async move {
                                ensure_backend(config).await
                            }));
                        } else {
                            let config = dictation_backend
                                .as_ref()
                                .map(LocalDictationBackend::config)
                                .unwrap_or_else(|| dictation_config.clone());
                            match LocalDictationRecorder::start(&config) {
                                Ok(recorder) => {
                                    dictation_recorder = Some(recorder);
                                    let terminal = terminal.as_mut().expect("terminal");
                                    terminal.set_dictation_state(DictationState::Recording);
                                    terminal.set_notice(
                                        "Recording locally · click stop or use the dictate keybinding"
                                            .to_string(),
                                    );
                                }
                                Err(error) => terminal
                                    .as_mut()
                                    .expect("terminal")
                                    .set_notice(format!("Could not start dictation: {error:#}")),
                            }
                        }
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
                    UiAction::ForceQuit => {
                        // Repeated Ctrl-C means "give the shell back now".
                        // In particular, never keep this foreground process
                        // alive merely because a remote viewer is attached.
                        user_requested_exit = true;
                        force_exit_requested = true;
                        shutdown_terminal(&mut terminal, &crash_context.tui_active).await;
                        let _ = session_command_tx.try_send(HostCommand::Stop { session_id });
                        break;
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
                        let text = normalize_consultation_command(&text);
                        if let Some(prompt) = director_prompt_command(&text) {
                            let terminal = terminal.as_mut().expect("terminal");
                            terminal.discard_pending_prompt(target, message_id);
                            match prompt {
                                Ok(prompt) => {
                                    terminal.focus_director();
                                    terminal.project_pending_prompt(
                                        None,
                                        message_id,
                                        prompt.clone(),
                                        PromptDelivery::Queue,
                                    );
                                    terminal.set_notice("Queued message for director".to_string());
                                    if let Err(error) = persist_prompt_admission(
                                        store.as_ref(),
                                        session_id,
                                        message_id,
                                        &prompt,
                                        &attachments,
                                        PromptDelivery::Queue,
                                    )
                                    .await
                                    {
                                        terminal.reject_optimistic_prompt(
                                            None,
                                            message_id,
                                            format!("/director {prompt}"),
                                            attachments,
                                        );
                                        terminal.set_notice(format!(
                                            "Could not durably queue the prompt: {error:#}"
                                        ));
                                        terminal.draw()?;
                                        terminal_dirty = false;
                                        continue;
                                    }
                                    terminal.draw()?;
                                    terminal_dirty = terminal.has_pending_scroll_frame();
                                    let command = director_prompt_host_command(
                                        session_id,
                                        message_id,
                                        prompt,
                                        attachments,
                                        PromptDelivery::Queue,
                                    );
                                    pending_prompt_ids.insert(message_id);
                                    if session_command_tx.send(command).await.is_err() {
                                        terminal.set_notice(
                                            "Prompt saved durably · waiting for the director thread to reconnect"
                                                .to_string(),
                                        );
                                        terminal.draw()?;
                                        terminal_dirty = false;
                                    }
                                }
                                Err(error) => {
                                    terminal.restore_composer(text, attachments);
                                    terminal.set_notice(error.to_string());
                                }
                            }
                            continue;
                        }
                        if let Some(command) = persistent_sidecar_command(&text) {
                            let terminal = terminal.as_mut().expect("terminal");
                            terminal.discard_pending_prompt(target, message_id);
                            match command {
                                Ok((sidecar, intent)) => {
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
                                                "Could not reach {} peer",
                                                sidecar.label()
                                            ));
                                            break;
                                        }
                                    }
                                }
                                Err(error) => {
                                    terminal.restore_composer(text, attachments);
                                    terminal.set_notice(error.to_string());
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
                        if let Err(error) = persist_prompt_admission(
                            store.as_ref(),
                            target.unwrap_or(session_id),
                            message_id,
                            &text,
                            &attachments,
                            PromptDelivery::Queue,
                        )
                        .await
                        {
                            let terminal = terminal.as_mut().expect("terminal");
                            terminal.reject_optimistic_prompt(
                                target,
                                message_id,
                                text,
                                attachments,
                            );
                            terminal.set_notice(format!(
                                "Could not durably queue the prompt: {error:#}"
                            ));
                            terminal.draw()?;
                            terminal_dirty = false;
                            continue;
                        }
                        pending_prompt_ids.insert(message_id);
                        if session_command_tx.send(command).await.is_err() {
                            let terminal = terminal.as_mut().expect("terminal");
                            terminal.set_notice(
                                "Prompt saved durably · waiting for the session coordinator to reconnect"
                                    .to_string(),
                            );
                            terminal.draw()?;
                            terminal_dirty = false;
                        }
                    }
                    UiAction::Submit {
                        target,
                        text,
                        attachments,
                    } => {
                        let text = normalize_consultation_command(&text);
                        if let Some(prompt) = director_prompt_command(&text) {
                            let terminal = terminal.as_mut().expect("terminal");
                            match prompt {
                                Ok(prompt) => {
                                    terminal.focus_director();
                                    let active = matches!(
                                        status,
                                        SessionStatus::Starting
                                            | SessionStatus::Running
                                            | SessionStatus::WaitingForApproval
                                    );
                                    let delivery = director_prompt_delivery(
                                        active,
                                        provider,
                                        steer_active_turn,
                                    );
                                    let message_id = Uuid::new_v4();
                                    if active {
                                        terminal.project_pending_prompt(
                                            None,
                                            message_id,
                                            prompt.clone(),
                                            delivery,
                                        );
                                    } else {
                                        terminal.project_submitted_prompt(
                                            message_id,
                                            prompt.clone(),
                                            attachments.clone(),
                                            delivery,
                                        );
                                        status = SessionStatus::Starting;
                                        sleep_inhibitor.set_turn_active(true);
                                    }
                                    terminal.set_notice("Sending to director".to_string());
                                    if let Err(error) = persist_prompt_admission(
                                        store.as_ref(),
                                        session_id,
                                        message_id,
                                        &prompt,
                                        &attachments,
                                        delivery,
                                    )
                                    .await
                                    {
                                        terminal.reject_optimistic_prompt(
                                            None,
                                            message_id,
                                            format!("/director {prompt}"),
                                            attachments,
                                        );
                                        terminal.set_notice(format!(
                                            "Could not durably send the prompt: {error:#}"
                                        ));
                                        if !active {
                                            status = SessionStatus::Ready;
                                            sleep_inhibitor.set_turn_active(false);
                                        }
                                        terminal.draw()?;
                                        terminal_dirty = false;
                                        continue;
                                    }
                                    terminal.draw()?;
                                    terminal_dirty = terminal.has_pending_scroll_frame();
                                    let command = director_prompt_host_command(
                                        session_id,
                                        message_id,
                                        prompt,
                                        attachments,
                                        delivery,
                                    );
                                    pending_prompt_ids.insert(message_id);
                                    if session_command_tx.send(command).await.is_err() {
                                        terminal.set_notice(
                                            "Prompt saved durably · waiting for the director thread to reconnect"
                                                .to_string(),
                                        );
                                        terminal.draw()?;
                                        terminal_dirty = false;
                                    }
                                }
                                Err(error) => {
                                    terminal.restore_composer(text, attachments);
                                    terminal.set_notice(error.to_string());
                                }
                            }
                            continue;
                        }
                        if let Some(command) = persistent_sidecar_command(&text) {
                            let terminal = terminal.as_mut().expect("terminal");
                            match command {
                                Ok((sidecar, intent)) => {
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
                                                "Could not reach {} peer",
                                                sidecar.label()
                                            ));
                                            break;
                                        }
                                    }
                                }
                                Err(error) => {
                                    terminal.restore_composer(text, attachments);
                                    terminal.set_notice(error.to_string());
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
                            let delivery =
                                default_active_delivery(active_provider, steer_active_turn);
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .project_pending_prompt(
                                    Some(target),
                                    message_id,
                                    text.clone(),
                                    delivery,
                                );
                            if let Err(error) = persist_prompt_admission(
                                store.as_ref(),
                                target,
                                message_id,
                                &text,
                                &attachments,
                                delivery,
                            )
                            .await
                            {
                                let terminal = terminal.as_mut().expect("terminal");
                                terminal.reject_optimistic_prompt(
                                    Some(target),
                                    message_id,
                                    text,
                                    attachments,
                                );
                                terminal.set_notice(format!(
                                    "Could not durably send the prompt: {error:#}"
                                ));
                                terminal.draw()?;
                                terminal_dirty = false;
                                continue;
                            }
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
                            pending_prompt_ids.insert(message_id);
                            if session_command_tx.send(command).await.is_err() {
                                let terminal = terminal.as_mut().expect("terminal");
                                terminal.set_notice(
                                    "Prompt saved durably · waiting for the session coordinator to reconnect"
                                        .to_string(),
                                );
                                terminal.draw()?;
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
                        match extension_command_request(
                            line,
                            &extension_catalog.api_snapshot().commands,
                        ) {
                            Ok(Some((command, arguments))) if attachments.is_empty() => {
                                let invocation_id = Uuid::new_v4();
                                let notice = format!("Running {line}");
                                if session_command_tx
                                    .send(HostCommand::ExtensionCommand {
                                        session_id,
                                        invocation_id,
                                        command,
                                        arguments,
                                    })
                                    .await
                                    .is_ok()
                                {
                                    terminal.as_mut().expect("terminal").set_notice(notice);
                                } else {
                                    terminal
                                        .as_mut()
                                        .expect("terminal")
                                        .set_notice("Extension command could not reach the session actor");
                                }
                                continue;
                            }
                            Ok(Some(_)) => {
                                terminal
                                    .as_mut()
                                    .expect("terminal")
                                    .set_notice("Extension commands do not accept attachments");
                                continue;
                            }
                            Err(error) => {
                                terminal.as_mut().expect("terminal").set_notice(error.to_string());
                                continue;
                            }
                            Ok(None) => {}
                        }
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
                        } else if line == "/animations" && attachments.is_empty() {
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .open_running_sweeps_picker();
                        } else if line == "/notifications" && attachments.is_empty() {
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .open_completion_notifications_picker();
                        } else if line == "/sound" && attachments.is_empty() {
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .open_completion_sound_picker();
                        } else if line == "/icons" && attachments.is_empty() {
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .open_dictation_icon_picker();
                        } else if let Some(value) = line.strip_prefix("/icons ")
                            && attachments.is_empty()
                        {
                            if let Some(style) = parse_dictation_icon_style(value) {
                                editor_preferences.presentation.dictation_icon = Some(style);
                                editor_preferences.save()?;
                                let terminal = terminal.as_mut().expect("terminal");
                                terminal.set_dictation_icon(style);
                                terminal.set_notice(format!(
                                    "Microphone icon: {}",
                                    match style {
                                        DictationIconStyle::NerdFont => "Nerd Font 󰍬",
                                        DictationIconStyle::Emoji => "emoji 🎤",
                                    }
                                ));
                            } else {
                                terminal.as_mut().expect("terminal").set_notice(
                                    "Choose /icons nerd or /icons emoji",
                                );
                            }
                        } else if line == "/followups" && attachments.is_empty() {
                            terminal
                                .as_mut()
                                .expect("terminal")
                                .open_active_messages_picker(steer_active_turn);
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
                        } else if line == "/customize" && attachments.is_empty() {
                            terminal.as_mut().expect("terminal").show_info(
                                "Effective customization",
                                live_customization_summary(
                                    &editor_preferences,
                                    &agent_config,
                                    &extension_catalog,
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
                        } else if is_usage_command(line) && attachments.is_empty() {
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
                                    "Keep machine awake during active turns: {}",
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
                        } else if let Some(value) = line.strip_prefix("/animations ")
                            && attachments.is_empty()
                        {
                            if let Some(enabled) = parse_on_off(value) {
                                editor_preferences.presentation.running_sweeps = enabled;
                                editor_preferences.save()?;
                                let terminal = terminal.as_mut().expect("terminal");
                                terminal.set_running_sweeps(enabled);
                                terminal.set_notice(format!(
                                    "Running sweep animations: {}",
                                    if enabled { "on" } else { "off" }
                                ));
                            } else {
                                terminal.as_mut().expect("terminal").set_notice(
                                    "Choose /animations on or /animations off",
                                );
                            }
                        } else if let Some(value) = line.strip_prefix("/notifications ")
                            && attachments.is_empty()
                        {
                            if let Some(policy) = parse_completion_alert_policy(value) {
                                editor_preferences.interaction.completion_notifications = policy;
                                editor_preferences.save()?;
                                let terminal = terminal.as_mut().expect("terminal");
                                terminal.set_completion_alerts(
                                    policy,
                                    editor_preferences.interaction.completion_sound,
                                );
                                terminal.set_notice(format!(
                                    "Completion notifications: {}",
                                    completion_alert_policy_name(policy)
                                ));
                            } else {
                                terminal.as_mut().expect("terminal").set_notice(
                                    "Choose /notifications off, unfocused, or always",
                                );
                            }
                        } else if let Some(value) = line.strip_prefix("/sound ")
                            && attachments.is_empty()
                        {
                            if let Some(policy) = parse_completion_alert_policy(value) {
                                editor_preferences.interaction.completion_sound = policy;
                                editor_preferences.save()?;
                                let terminal = terminal.as_mut().expect("terminal");
                                terminal.set_completion_alerts(
                                    editor_preferences.interaction.completion_notifications,
                                    policy,
                                );
                                terminal.set_notice(format!(
                                    "Completion sound: {}",
                                    completion_alert_policy_name(policy)
                                ));
                            } else {
                                terminal
                                    .as_mut()
                                    .expect("terminal")
                                    .set_notice("Choose /sound off, unfocused, or always");
                            }
                        } else if let Some(value) = line.strip_prefix("/followups ")
                            && attachments.is_empty()
                        {
                            match value.trim() {
                                "steer" => {
                                    steer_active_turn = true;
                                    editor_preferences.interaction.active_messages =
                                        ActiveMessageBehavior::Steer;
                                    editor_preferences.save()?;
                                    terminal.as_mut().expect("terminal").set_notice(
                                        "Messages sent while Borg works: send now and redirect the current turn",
                                    );
                                }
                                "queue" => {
                                    steer_active_turn = false;
                                    editor_preferences.interaction.active_messages =
                                        ActiveMessageBehavior::Queue;
                                    editor_preferences.save()?;
                                    terminal.as_mut().expect("terminal").set_notice(
                                        "Messages sent while Borg works: send after the current turn finishes",
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
                                    let optimistic = matches!(
                                        &action,
                                        GoalAction::Pause | GoalAction::Resume | GoalAction::Clear
                                    );
                                    let terminal = terminal.as_mut().expect("terminal");
                                    if optimistic {
                                        let changed =
                                            terminal.optimistically_apply_goal_action(&action);
                                        if changed
                                            && matches!(&action, GoalAction::Resume)
                                            && !matches!(
                                                status,
                                                SessionStatus::Starting
                                                    | SessionStatus::Running
                                                    | SessionStatus::WaitingForApproval
                                            )
                                        {
                                            status = SessionStatus::Starting;
                                            sleep_inhibitor.set_turn_active(true);
                                        }
                                        terminal.draw()?;
                                        terminal_dirty = terminal.has_pending_scroll_frame();
                                    }
                                    if !dispatch_host_command_without_blocking(
                                        &session_command_tx,
                                        HostCommand::Goal {
                                            session_id,
                                            action,
                                        },
                                    ) {
                                        terminal.set_notice("Could not reach the session actor");
                                    }
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
                                    .open_command_palette(),
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
                                        restored.set_dictation_icon(dictation_icon_style_for_preference(
                                            editor_preferences.presentation.dictation_icon,
                                        ));
                                        restored.set_configured_model_entries(agent_config.configured_model_entries());
                                        restored.set_extension_commands(extension_catalog.api_snapshot().commands);
                                        let composer_history = store
                                            .recent_user_messages(
                                                session_id,
                                                RICH_TUI_PROMPT_HISTORY_LIMIT,
                                            )
                                            .await?;
                                        restored.seed_composer_history(&composer_history);
                                        restored.seed_history(&latest.events);
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
                                        restored.set_dictation_icon(dictation_icon_style_for_preference(
                                            editor_preferences.presentation.dictation_icon,
                                        ));
                                        restored.set_configured_model_entries(agent_config.configured_model_entries());
                                        restored.set_extension_commands(extension_catalog.api_snapshot().commands);
                                        let composer_history = store
                                            .recent_user_messages(
                                                session_id,
                                                RICH_TUI_PROMPT_HISTORY_LIMIT,
                                            )
                                            .await?;
                                        restored.seed_composer_history(&composer_history);
                                        restored.seed_history(&latest.events);
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
                                            steer_active_turn,
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
                                        if let Err(error) = persist_prompt_admission(
                                            store.as_ref(),
                                            session_id,
                                            message_id,
                                            &text,
                                            &attachments,
                                            delivery,
                                        )
                                        .await
                                        {
                                            let terminal = terminal.as_mut().expect("terminal");
                                            terminal.reject_optimistic_prompt(
                                                None,
                                                message_id,
                                                text,
                                                attachments,
                                            );
                                            terminal.set_notice(format!(
                                                "Could not durably send the prompt: {error:#}"
                                            ));
                                            if !active {
                                                status = SessionStatus::Ready;
                                                sleep_inhibitor.set_turn_active(false);
                                            }
                                            terminal.draw()?;
                                            terminal_dirty = false;
                                            continue;
                                        }
                                        pending_prompt_ids.insert(message_id);
                                        if session_command_tx.send(HostCommand::Prompt {
                                            session_id,
                                            message_id,
                                            text,
                                            attachments,
                                            output_schema: None,
                                            delivery,
                                        }).await.is_err() {
                                            let terminal = terminal.as_mut().expect("terminal");
                                            terminal.set_notice(
                                                "Prompt saved durably · waiting for the session actor to reconnect"
                                                    .to_string(),
                                            );
                                            terminal.draw()?;
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
                    force_exit_requested = true;
                    shutdown_terminal(&mut terminal, &crash_context.tui_active).await;
                    let _ = session_command_tx.try_send(HostCommand::Stop { session_id });
                    break;
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
                let terminal_draft = terminal
                    .as_ref()
                    .and_then(BorgTerminal::composer_draft);
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
                    if detached_prompt.is_none() {
                        detached_prompt = terminal_draft;
                    }
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
    drop(dictation_recorder);
    if let Some(task) = dictation_task.take() {
        task.abort();
    }
    if let Some(task) = dictation_setup_task.take() {
        task.abort();
    }
    drop(dictation_backend);
    if let Some(terminal) = terminal.as_mut() {
        terminal.set_dictation_state(DictationState::Idle);
    }
    let tui_was_active = crash_context.tui_active.load(Ordering::Acquire);
    let preserve_terminal = resume_session.is_some() && !user_requested_exit && terminal.is_some();
    if !preserve_terminal {
        shutdown_terminal(&mut terminal, &crash_context.tui_active).await;
    }
    drop(session_events);
    let mut actor = actor;
    if force_exit_requested {
        actor.abort();
    }
    let actor_result = if force_exit_requested {
        match tokio::time::timeout(std::time::Duration::from_millis(500), &mut actor).await {
            Ok(result) => Some(result),
            Err(_) => {
                tracing::warn!(%session_id, "forced exit detached an actor that did not cancel within 500ms");
                None
            }
        }
    } else {
        Some(actor.await)
    };
    let mut actor_panicked = false;
    let actor_error = match actor_result {
        Some(Ok(result)) => result.err(),
        Some(Err(join_error)) if force_exit_requested && join_error.is_cancelled() => None,
        Some(Err(join_error)) => {
            actor_panicked = join_error.is_panic();
            Some(anyhow::anyhow!("agent session task failed: {join_error}"))
        }
        None => None,
    };
    let discarded_empty_session = if !force_exit_requested
        && session_access == LocalSessionAccess::Owned
        && !args.ephemeral
    {
        match sqlite_store.discard_empty_session(session_id).await {
            Ok(discarded) => discarded,
            Err(error) => {
                tracing::warn!(%session_id, %error, "failed to discard empty local session");
                false
            }
        }
    } else {
        false
    };
    if let Some(error) = actor_error {
        if discarded_empty_session {
            return Err(error);
        }
        let retry_prompt = detached_prompt.clone().or_else(|| rewind_prompt.clone());
        if actor_panicked && tui_was_active {
            if let Some(next_session) = resume_session {
                return Ok(Some((next_session, retry_prompt, terminal)));
            }
            if retry_prompt.is_some() {
                return Ok(Some((session_id, retry_prompt, terminal)));
            }
            println!("{}", resume_instructions(session_id, false));
            return Ok(None);
        }
        if tui_was_active {
            tracing::error!(%session_id, error = %error, "local agent session crashed");
            crash_context.set_retry_notice(format!(
                "The resumed session stopped unexpectedly and Borg is reconnecting: {error:#}"
            ));
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            let next_session = resume_session.unwrap_or(session_id);
            return Ok(Some((next_session, retry_prompt, terminal)));
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
    if let Some(shutdown) = mirror_shutdown {
        shutdown.send(true).ok();
    }
    if force_exit_requested {
        // The terminal is already restored. Do not keep the foreground shell
        // behind remote-mirror or child-process teardown after double Ctrl-C.
        drop(collab_child);
        if let Some(task) = mirror_task {
            task.abort();
        }
        drop(local_server);
    } else {
        if let Some(mut child) = collab_child {
            child.kill().await.ok();
        }
        if let Some(task) = mirror_task {
            task.await.context("remote mirror task failed")?;
        }
        if let Some(server) = local_server {
            server
                .shutdown()
                .await
                .context("failed to stop Borg-owned local llama-server")?;
        }
    }
    if resume_session == Some(session_id)
        && !user_requested_exit
        && let Some(notice) = exit_notice.take()
    {
        println!("\n  {notice}");
    }
    if should_print_exit_resume(user_requested_exit, resume_session, args.ephemeral) {
        if let Some(notice) = exit_notice {
            println!("\n  {notice}");
        }
        println!("\n{}", resume_instructions(session_id, false));
    }
    let reopen_after_detach =
        detached_prompt.is_some() && !user_requested_exit && resume_session.is_none();
    let next_prompt = detached_prompt.or(rewind_prompt);
    if reopen_after_detach {
        return Ok(Some((session_id, next_prompt, None)));
    }
    if session_access.is_attached() && !user_requested_exit && resume_session.is_none() {
        tracing::info!(%session_id, "active session owner exited; acquiring ownership");
        return Ok(Some((session_id, next_prompt, None)));
    }
    let next_terminal = if resume_session.is_some() && !user_requested_exit {
        terminal
    } else {
        None
    };
    Ok(resume_session.map(|session| (session, next_prompt, next_terminal)))
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
        CodingProvider::OpenRouter => borg_provider::credentials::ApiKeyCredential::OpenRouter,
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
    discard_pending_terminal_input();
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
            status: MessageStatus::Complete | MessageStatus::Failed,
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

fn local_resume_error_is_retryable(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    message.contains("database is locked")
        || message.contains("database table is locked")
        || message.contains("database is busy")
        || message.contains("pool timed out")
}

fn next_local_resume_retry_delay(delay: std::time::Duration) -> std::time::Duration {
    delay
        .checked_mul(2)
        .unwrap_or(LOCAL_RESUME_RETRY_MAX_DELAY)
        .min(LOCAL_RESUME_RETRY_MAX_DELAY)
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
        session_id_if_present(store, target).await?
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
    if store.load_host_launch_metadata(target).await?.is_some() {
        anyhow::bail!(
            "this session is still owned by the background Borg remote host; reopen it from the connected remote chat instead of starting a second local writer"
        );
    }
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
    Ok(store
        .list_sessions(10_000)
        .await?
        .into_iter()
        .filter(|session| session_has_resumable_activity(&session.state))
        .map(|session| session.session_id)
        .collect())
}

fn session_has_resumable_activity(state: &borg_remote::SessionState) -> bool {
    state.has_resumable_activity()
}

async fn latest_session_id_in_directory(
    sessions_dir: &Path,
    store: &SqliteSessionStore,
    current_dir: &Path,
) -> Result<Option<Uuid>> {
    fs::create_dir_all(sessions_dir)?;
    Ok(store
        .list_sessions(10_000)
        .await?
        .into_iter()
        .find(|session| {
            session_has_resumable_activity(&session.state)
                && session
                    .state
                    .configuration
                    .as_ref()
                    .is_some_and(|configuration| configuration.cwd == current_dir)
        })
        .map(|session| session.session_id))
}

async fn recent_session_options(
    sessions_dir: &Path,
    store: &SqliteSessionStore,
    current: Uuid,
    current_dir: &Path,
    limit: usize,
) -> Result<Vec<ResumeSessionOption>> {
    fs::create_dir_all(sessions_dir)?;
    let summaries = store.list_sessions(10_000).await?;
    let session_ids = summaries
        .iter()
        .filter(|summary| session_has_resumable_activity(&summary.state))
        .map(|summary| summary.session_id)
        .collect::<Vec<_>>();
    let summaries = summaries
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
) -> Result<ResumeBootstrapHistory> {
    let (checkpoint, latest_user_messages, scanned) = tokio::try_join!(
        store.latest_completed_context_compaction(session_id),
        store.recent_user_messages(session_id, 1),
        store.events_after(
            session_id,
            recent_tui_history_after(latest_sequence),
            RICH_TUI_HISTORY_BOOTSTRAP_SCAN_LIMIT,
        ),
    )?;
    let mut scanned = scanned;
    if let Some(latest_user) = latest_user_messages.into_iter().next()
        && !scanned.iter().any(|event| event.id == latest_user.id)
    {
        let index = scanned.partition_point(|event| event.sequence < latest_user.sequence);
        scanned.insert(index, latest_user);
    }
    let selection = select_resume_bootstrap_history(scanned);
    let mut selected = selection.events;
    if let Some(checkpoint) = checkpoint
        && !selected.iter().any(|event| event.id == checkpoint.id)
    {
        let index = selected.partition_point(|event| event.sequence < checkpoint.sequence);
        selected.insert(index, checkpoint);
    }
    Ok(ResumeBootstrapHistory {
        events: selected,
        page_before: selection
            .page_before
            .unwrap_or_else(|| latest_sequence.saturating_add(1)),
    })
}

fn recent_tui_history_after(latest_sequence: u64) -> u64 {
    latest_sequence.saturating_sub(RICH_TUI_HISTORY_BOOTSTRAP_SCAN_LIMIT as u64)
}

#[derive(Debug)]
struct ResumeBootstrapHistory {
    events: Vec<SessionEvent>,
    page_before: u64,
}

#[derive(Debug)]
struct ResumeBootstrapSelection {
    events: Vec<SessionEvent>,
    page_before: Option<u64>,
}

fn select_resume_bootstrap_history(events: Vec<SessionEvent>) -> ResumeBootstrapSelection {
    // Queued input is not rendered as a conversation row, but its durable
    // admission marker is part of the ordering contract. A later completion
    // can arrive out of order when several steer prompts are pending; the
    // transcript uses these markers to restore the order in which the user
    // submitted them. Applying a queued event only updates that projection;
    // it never wakes or re-runs the prompt.
    let terminal_user_ids = events
        .iter()
        .filter_map(|event| match &event.kind {
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                status: MessageStatus::Complete | MessageStatus::Failed,
                ..
            } => Some(*message_id),
            _ => None,
        })
        .collect::<HashSet<_>>();
    let events = events
        .into_iter()
        .filter(|event| match &event.kind {
            SessionEventKind::Message {
                message_id,
                status: MessageStatus::Queued,
                ..
            } => terminal_user_ids.contains(message_id),
            kind @ SessionEventKind::ProviderEvent {
                kind: event_kind, ..
            } if event_kind == "context_compaction" && !kind.is_completed_context_compaction() => {
                false
            }
            _ => true,
        })
        .collect::<Vec<_>>();
    if events.len() <= RICH_TUI_HISTORY_EVENT_LIMIT {
        let events = trim_resume_bootstrap_to_user_boundary(events);
        return ResumeBootstrapSelection {
            page_before: events
                .iter()
                .map(|event| event.sequence)
                .filter(|sequence| *sequence > 0)
                .min(),
            events,
        };
    }
    let floor = events.len() - RICH_TUI_HISTORY_EVENT_LIMIT;
    let tail_start = events.get(floor).map(|event| event.sequence);
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
    let mut retained_messages = message_indices
        .into_iter()
        .rev()
        .take(RICH_TUI_HISTORY_MESSAGE_LIMIT)
        .collect::<HashSet<_>>();
    if let Some(latest_user) = events.iter().rposition(|event| {
        matches!(
            event.kind,
            SessionEventKind::Message {
                actor: EventActor::User,
                status: MessageStatus::Complete | MessageStatus::Failed | MessageStatus::InProgress,
                ..
            }
        )
    }) {
        retained_messages.insert(latest_user);
    }

    // Keep a bounded event tail for current lifecycle/tool state and splice in
    // the latest real conversation messages even when a high-volume child or
    // tool stream pushed them outside that tail. A contiguous slice starting
    // at the eighth message can contain thousands of irrelevant events and
    // makes the first render just as unusable as loading the whole history.
    let selected = events
        .into_iter()
        .enumerate()
        .filter_map(|(index, event)| {
            (index >= floor || retained_messages.contains(&index)).then_some(event)
        })
        .collect::<Vec<_>>();
    let selected = trim_resume_bootstrap_to_user_boundary(selected);
    // Trimming can move the first contiguous tail event forward. Paging must
    // end immediately before that retained event, or the removed interval is
    // never loaded and completed messages/tools silently disappear on resume.
    let page_before = tail_start.and_then(|tail_start| {
        selected
            .iter()
            .find(|event| event.sequence >= tail_start)
            .map(|event| event.sequence)
    });
    ResumeBootstrapSelection {
        events: selected,
        page_before,
    }
}

fn trim_resume_bootstrap_to_user_boundary(events: Vec<SessionEvent>) -> Vec<SessionEvent> {
    let retained_user_ids = events
        .iter()
        .filter_map(|event| match &event.kind {
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                status: MessageStatus::Complete | MessageStatus::Failed | MessageStatus::InProgress,
                ..
            } => Some(*message_id),
            _ => None,
        })
        .collect::<HashSet<_>>();
    let Some(first_user) = events.iter().position(|event| {
        matches!(
            event.kind,
            SessionEventKind::Message {
                actor: EventActor::User,
                status: MessageStatus::Complete | MessageStatus::Failed | MessageStatus::InProgress,
                ..
            }
        )
    }) else {
        return Vec::new();
    };
    let first_admission_marker = events.iter().position(|event| {
        matches!(
            &event.kind,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                status: MessageStatus::Queued,
                ..
            } if retained_user_ids.contains(message_id)
        )
    });
    events
        .into_iter()
        .skip(first_admission_marker.unwrap_or(first_user))
        .collect()
}

fn merge_tui_history_page(history: &mut Vec<SessionEvent>, older: Vec<SessionEvent>) {
    let mut seen_ids = HashSet::new();
    let mut seen_sequences = HashSet::new();
    let mut merged = Vec::with_capacity(history.len() + older.len());
    for event in older.into_iter().chain(std::mem::take(history)) {
        let unseen = if event.sequence > 0 {
            seen_sequences.insert(event.sequence)
        } else {
            seen_ids.insert(event.id)
        };
        if unseen {
            merged.push(event);
        }
    }
    merged.sort_by_key(|event| (event.sequence == 0, event.sequence));
    *history = merged;
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
    Ok(store
        .events_after(session_id, after_sequence, limit)
        .await?
        .into_iter()
        .filter(|event| {
            !matches!(
                event.kind,
                SessionEventKind::Message {
                    status: MessageStatus::Queued,
                    ..
                }
            )
        })
        .collect())
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
                tracing::warn!(
                    child_session_id = %agent.session_id,
                    %store_error,
                    "could not load subagent transcript history"
                );
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
        // before the parent mirrors it. Resolve the child ledger before the
        // hydrated roster is exposed so it cannot advertise stale work.
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
    agent.usage.context_tokens = state.usage.context_tokens;
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
        .join(format!("{}.lock", agent.session_id));
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
    activity_frame: bool,
) -> std::time::Duration {
    let max_interval = if activity_frame {
        ACTIVITY_FRAME_INTERVAL
    } else {
        MAX_RENDER_BACKOFF_INTERVAL
    };
    tui_frame_interval(fps)
        .max(if interaction_frame {
            last_draw
        } else {
            last_draw.saturating_mul(3)
        })
        .min(max_interval)
}

fn parse_on_off(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "on" | "true" | "1" => Some(true),
        "off" | "false" | "0" => Some(false),
        _ => None,
    }
}

fn parse_completion_alert_policy(value: &str) -> Option<CompletionAlertPolicy> {
    match value.trim() {
        "off" => Some(CompletionAlertPolicy::Off),
        "unfocused" => Some(CompletionAlertPolicy::Unfocused),
        "always" => Some(CompletionAlertPolicy::Always),
        _ => None,
    }
}

fn completion_alert_policy_name(policy: CompletionAlertPolicy) -> &'static str {
    match policy {
        CompletionAlertPolicy::Off => "off",
        CompletionAlertPolicy::Unfocused => "when unfocused",
        CompletionAlertPolicy::Always => "always",
    }
}

fn parse_dictation_icon_style(value: &str) -> Option<DictationIconStyle> {
    match value.trim().to_ascii_lowercase().as_str() {
        "nerd" | "nerd_font" => Some(DictationIconStyle::NerdFont),
        "emoji" => Some(DictationIconStyle::Emoji),
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

fn terminal_needs_activity_tick(status: SessionStatus) -> bool {
    matches!(status, SessionStatus::Starting | SessionStatus::Running)
}

fn terminal_needs_idle_tick(has_expiring_notice: bool, has_blinking_cursor: bool) -> bool {
    has_expiring_notice || has_blinking_cursor
}

fn session_event_needs_immediate_frame(kind: &SessionEventKind) -> bool {
    matches!(
        kind,
        SessionEventKind::ToolStarted { .. } | SessionEventKind::ToolUpdated { .. }
    ) || matches!(kind, SessionEventKind::ProviderEvent { kind, .. } if kind == "tool_call_started")
}

fn should_draw_input_fast_path(
    input_is_keyboard: bool,
    event_redraw_needed: bool,
    action_is_none: bool,
    transcript_dirty: bool,
    not_launch_screen: bool,
) -> bool {
    input_is_keyboard
        && event_redraw_needed
        && action_is_none
        && !transcript_dirty
        && not_launch_screen
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

async fn persist_prompt_admission(
    store: &dyn SessionStore,
    session_id: Uuid,
    message_id: Uuid,
    text: &str,
    attachments: &[PathBuf],
    delivery: PromptDelivery,
) -> Result<()> {
    store
        .admit_prompt(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: text.to_string(),
                attachments: attachments.to_vec(),
                status: MessageStatus::Queued,
                delivery: Some(delivery),
            },
        ))
        .await?;
    Ok(())
}

fn running_input(
    line: &str,
    provider: CodingProvider,
    steer_active_turn: bool,
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
        default_active_delivery(provider, steer_active_turn),
        line.to_string(),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistentSidecar {
    Claude,
    Gpt,
}

impl PersistentSidecar {
    const fn task_name(self) -> &'static str {
        match self {
            Self::Claude => "/root/claude",
            Self::Gpt => "/root/gpt",
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Claude => "Claude",
            Self::Gpt => "GPT",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PersistentSidecarIntent {
    Ensure,
    Clear,
    Rotate {
        model: Option<String>,
        effort: Option<String>,
    },
    Prompt(String),
}

/// Parse explicit direct control of a durable peer. `/claude` and `/gpt`
/// remain deliberately outside this parser: they are friendly aliases that
/// let the active model make a context-aware `consult_peer` call.
fn persistent_sidecar_command(
    line: &str,
) -> Option<Result<(PersistentSidecar, PersistentSidecarIntent)>> {
    borg_ui::parse_peer_command(line).map(|result| {
        result.map(|(target, intent)| {
            let sidecar = match target {
                borg_ui::PeerTarget::Claude => PersistentSidecar::Claude,
                borg_ui::PeerTarget::Gpt => PersistentSidecar::Gpt,
            };
            let intent = match intent {
                borg_ui::PeerIntent::Ensure => PersistentSidecarIntent::Ensure,
                borg_ui::PeerIntent::Clear => PersistentSidecarIntent::Clear,
                borg_ui::PeerIntent::Rotate { model, effort } => {
                    PersistentSidecarIntent::Rotate { model, effort }
                }
                borg_ui::PeerIntent::Prompt(prompt) => PersistentSidecarIntent::Prompt(prompt),
            };
            (sidecar, intent)
        })
    })
}

fn persistent_sidecar_commands(
    session_id: Uuid,
    sidecar: PersistentSidecar,
    intent: &PersistentSidecarIntent,
    message_id: Uuid,
    attachments: &[PathBuf],
    delivery: PromptDelivery,
) -> Vec<HostCommand> {
    let target = match sidecar {
        PersistentSidecar::Claude => borg_ui::PeerTarget::Claude,
        PersistentSidecar::Gpt => borg_ui::PeerTarget::Gpt,
    };
    let intent = match intent {
        PersistentSidecarIntent::Ensure => borg_ui::PeerIntent::Ensure,
        PersistentSidecarIntent::Clear => borg_ui::PeerIntent::Clear,
        PersistentSidecarIntent::Rotate { model, effort } => borg_ui::PeerIntent::Rotate {
            model: model.clone(),
            effort: effort.clone(),
        },
        PersistentSidecarIntent::Prompt(prompt) => borg_ui::PeerIntent::Prompt(prompt.clone()),
    };
    borg_ui::local::peer_host_commands(
        session_id,
        target,
        &intent,
        message_id,
        attachments,
        delivery,
    )
}

fn sidecar_notice(sidecar: PersistentSidecar, intent: &PersistentSidecarIntent) -> String {
    match intent {
        PersistentSidecarIntent::Ensure => format!("{} peer ready", sidecar.label()),
        PersistentSidecarIntent::Clear => format!("Clearing {} peer context", sidecar.label()),
        PersistentSidecarIntent::Rotate { model, effort } => {
            let profile = match (model.as_deref(), effort.as_deref()) {
                (Some(model), Some(effort)) => format!("{model}@{effort}"),
                (Some(model), None) => model.to_string(),
                (None, Some(effort)) => format!("@{effort}"),
                (None, None) => "provider default".to_string(),
            };
            format!("Replacing {} peer with {profile}", sidecar.label())
        }
        PersistentSidecarIntent::Prompt(_) => format!("Sending to {} peer", sidecar.label()),
    }
}

/// `/claude` and `/gpt` are the ergonomic aliases for a primary-agent-driven
/// persistent consultation. `/codex` remains a compatibility alias for an
/// explicit GPT peer consultation; the active model still chooses the useful
/// briefing. Direct sidecar controls are handled before this normalizer.
pub(crate) fn normalize_consultation_command(line: &str) -> String {
    borg_ui::normalize_consultation_command(line)
}

fn director_prompt_command(line: &str) -> Option<Result<String>> {
    let trimmed = line.trim();
    let rest = trimmed.strip_prefix("/director")?;
    if !rest.is_empty()
        && !rest
            .chars()
            .next()
            .is_some_and(|character| character.is_whitespace())
    {
        return None;
    }
    let prompt = rest.trim();
    if prompt.is_empty() {
        Some(Err(anyhow::anyhow!("usage: /director TEXT")))
    } else {
        Some(Ok(prompt.to_string()))
    }
}

fn extension_command_request(
    line: &str,
    commands: &[borg_remote::ExtensionApiCommand],
) -> Result<Option<(String, serde_json::Value)>> {
    let Some(rest) = line.strip_prefix("/ext:") else {
        return Ok(None);
    };
    let (qualified, argument_text) = rest
        .split_once(char::is_whitespace)
        .map_or((rest, ""), |(qualified, arguments)| {
            (qualified, arguments.trim())
        });
    let (extension_id, name) = qualified
        .split_once(':')
        .context("usage: /ext:EXTENSION:COMMAND [JSON_OBJECT|TEXT]")?;
    let command = commands
        .iter()
        .find(|command| command.extension_id == extension_id && command.name == name)
        .with_context(|| format!("extension command /ext:{extension_id}:{name} is not active"))?;
    let arguments = if argument_text.is_empty() {
        serde_json::json!({})
    } else if argument_text.starts_with('{') {
        let value: serde_json::Value = serde_json::from_str(argument_text)
            .context("extension command arguments must be valid JSON")?;
        anyhow::ensure!(
            value.is_object(),
            "extension command arguments must be a JSON object"
        );
        value
    } else {
        serde_json::json!({"arguments": argument_text})
    };
    Ok(Some((
        borg_remote::ExtensionApiSnapshot::command_wire_name(command),
        arguments,
    )))
}

fn director_prompt_delivery(
    active: bool,
    provider: CodingProvider,
    steer_active_turn: bool,
) -> PromptDelivery {
    if active {
        default_active_delivery(provider, steer_active_turn)
    } else {
        PromptDelivery::Steer
    }
}

fn director_prompt_host_command(
    session_id: Uuid,
    message_id: Uuid,
    text: String,
    attachments: Vec<PathBuf>,
    delivery: PromptDelivery,
) -> HostCommand {
    HostCommand::Prompt {
        session_id,
        message_id,
        text,
        attachments,
        output_schema: None,
        delivery,
    }
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

async fn session_id_if_present(store: &SqliteSessionStore, session_id: Uuid) -> Result<Uuid> {
    anyhow::ensure!(
        store.contains_session(session_id).await?,
        "local Borg session {session_id} does not exist"
    );
    Ok(session_id)
}

fn print_history(events: &[SessionEvent]) {
    let mut order = Vec::new();
    let mut messages = HashMap::new();
    for event in events {
        if matches!(
            &event.kind,
            SessionEventKind::Message {
                status: MessageStatus::Queued,
                ..
            }
        ) {
            continue;
        }
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
  /customize        inspect effective settings and extension authority
  /ask PROFILE TEXT ask another model through its persistent peer thread
  /director TEXT    send a message to the persistent director thread
  /claude TEXT      ask the active model to consult its persistent Claude peer
  /gpt TEXT         ask the active model to consult its persistent GPT peer
  /peer PROVIDER    direct peer control; add TEXT, clear, or new MODEL@EFFORT
  /model            choose the model
  /effort           choose reasoning effort
  /followups        choose message delivery: redirect now or wait for this turn to finish
  /refresh          choose terminal refresh rate
  /sleep            keep the machine awake during active turns
  /colors           show transcript colours
  /color TARGET HEX set a transcript colour
  /icons            choose the dictation icon (nerd or emoji)
  /usage            view account limits and session usage
  /status           alias for /usage
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
    borg_ui::parse_goal_action(line)
}

fn dispatch_host_command_without_blocking(
    sender: &mpsc::Sender<HostCommand>,
    command: HostCommand,
) -> bool {
    match sender.try_send(command) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(command)) => {
            let sender = sender.clone();
            tokio::spawn(async move {
                let _ = sender.send(command).await;
            });
            true
        }
        Err(mpsc::error::TrySendError::Closed(_)) => false,
    }
}

#[derive(Default)]
struct SessionUsage {
    calls: u64,
    input_tokens: u64,
    output_tokens: u64,
    cached_input_tokens: u64,
    total_tokens: u64,
    cost_usd: Option<f64>,
}

impl SessionUsage {
    fn from_projection(projected: &borg_remote::SessionUsage) -> Self {
        Self {
            calls: projected.calls,
            input_tokens: projected.input_tokens,
            output_tokens: projected.output_tokens,
            cached_input_tokens: projected.cached_input_tokens,
            total_tokens: projected.total_tokens,
            cost_usd: projected.cost_usd,
        }
    }

    fn add(&mut self, input: u64, output: u64, cached: u64, total: u64, cost: Option<f64>) {
        self.calls += 1;
        self.input_tokens += input;
        self.output_tokens += output;
        self.cached_input_tokens += cached;
        self.total_tokens += total;
        if let Some(cost) = cost {
            self.cost_usd = Some(self.cost_usd.unwrap_or_default() + cost);
        }
    }
}

async fn usage_summary(provider: CodingProvider, session: &SessionUsage) -> String {
    let limits = if provider == CodingProvider::Codex {
        match read_codex_account_rate_limits().await {
            Ok(limits) => Some(limits),
            Err(error) => {
                tracing::debug!(%error, "Codex account limits unavailable for usage view");
                None
            }
        }
    } else {
        None
    };
    format_usage_summary(provider, session, limits.as_ref())
}

fn format_usage_summary(
    provider: CodingProvider,
    session: &SessionUsage,
    limits: Option<&CodexAccountRateLimits>,
) -> String {
    let mut lines = vec![format!("Account limits · {}", provider.label())];
    if provider == CodingProvider::Codex {
        if let Some(limits) = limits {
            if let Some(plan) = limits.plan_type.as_deref() {
                lines.push(format!("  {:<16} {}", "Plan", title_case(plan)));
            }
            for (label, window) in [
                ("Primary", limits.primary.as_ref()),
                ("Secondary", limits.secondary.as_ref()),
            ] {
                if let Some(window) = window {
                    append_rate_limit_window(&mut lines, label, window);
                }
            }
            if limits.primary.is_none() && limits.secondary.is_none() {
                lines.push("  No active account limit reported.".to_string());
            }
        } else {
            lines.push("  Unavailable      Could not read current account limits.".to_string());
            lines.push("  Details          https://chatgpt.com/codex/settings/usage".to_string());
        }
    } else {
        lines.push(format!(
            "  Unavailable      Account limits are not exposed by {}.",
            provider.label()
        ));
    }

    lines.extend([String::new(), "Session".to_string()]);
    if session.calls == 0 {
        lines.push(
            "  Usage unavailable No provider token usage was reported for this session."
                .to_string(),
        );
        return lines.join("\n");
    }
    lines.extend([
        format!("  {:<16} {}", "Calls", format_count(session.calls)),
        format!(
            "  {:<16} {}",
            "Input tokens",
            format_count(session.input_tokens)
        ),
        format!(
            "  {:<16} {}",
            "Cached input",
            format_count(session.cached_input_tokens)
        ),
        format!(
            "  {:<16} {}",
            "Output tokens",
            format_count(session.output_tokens)
        ),
        format!(
            "  {:<16} {}",
            "Total tokens",
            format_count(session.total_tokens)
        ),
    ]);
    if let Some(cost) = session.cost_usd {
        lines.push(format!("  {:<16} ${cost:.4}", "Estimated cost"));
    }
    lines.join("\n")
}

fn indent_usage_summary(summary: &str) -> String {
    summary
        .lines()
        .map(|line| format!("  {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn append_rate_limit_window(lines: &mut Vec<String>, label: &str, window: &CodexRateLimitWindow) {
    lines.push(format!(
        "  {:<16} {}",
        window_label(label, window.window_duration_mins),
        rate_limit_bar(window.used_percent)
    ));
    if let Some(resets_at) = window.resets_at
        && let Some(reset) = Local.timestamp_opt(resets_at, 0).single()
    {
        lines.push(format!(
            "  {:<16} resets {}",
            "",
            reset.format("%H:%M on %d %b")
        ));
    }
}

fn window_label(fallback: &str, duration_mins: u64) -> String {
    match duration_mins {
        0 => fallback.to_string(),
        10_080 => "Weekly".to_string(),
        1_440 => "Daily".to_string(),
        300 => "5-hour".to_string(),
        60 => "Hourly".to_string(),
        mins if mins % 1_440 == 0 => format!("{}-day", mins / 1_440),
        mins if mins % 60 == 0 => format!("{}-hour", mins / 60),
        mins => format!("{mins}-minute"),
    }
}

fn rate_limit_bar(used_percent: u8) -> String {
    let left_percent = 100_u8.saturating_sub(used_percent.min(100));
    let filled = usize::from(left_percent) * 20 / 100;
    format!(
        "[{}{}] {left_percent}% left",
        "█".repeat(filled),
        "░".repeat(20 - filled)
    )
}

fn title_case(value: &str) -> String {
    let mut chars = value.chars();
    chars.next().map_or_else(String::new, |first| {
        format!("{}{}", first.to_uppercase(), chars.as_str())
    })
}

fn format_count(value: u64) -> String {
    let raw = value.to_string();
    let mut formatted = String::with_capacity(raw.len() + raw.len() / 3);
    for (index, character) in raw.chars().enumerate() {
        if index > 0 && (raw.len() - index).is_multiple_of(3) {
            formatted.push(',');
        }
        formatted.push(character);
    }
    formatted
}

fn is_usage_command(line: &str) -> bool {
    matches!(line, "/usage" | "/status")
}

fn parse_todo_action(line: &str, items: &[PlanItem]) -> Result<TodoAction> {
    borg_ui::parse_todo_action(line, items)
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
        CodingProvider::OpenCode => "open-code",
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
                "{} {} · {} · {} · {}",
                extension.id,
                extension.version,
                extension.scope.label(),
                extension.requested_access.label(),
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
    let workflow_count = catalog
        .extensions
        .iter()
        .filter(|extension| extension.active)
        .map(|extension| extension.workflows.len())
        .sum::<usize>();
    lines.push(format!(
        "{} active · {} workflows · revision {} · changes apply at the next turn boundary",
        catalog
            .extensions
            .iter()
            .filter(|extension| extension.active)
            .count(),
        workflow_count,
        &catalog.revision[..catalog.revision.len().min(12)],
    ));
    lines.join("\n")
}

fn live_customization_summary(
    editor: &EditorPreferences,
    agent: &AgentConfig,
    catalog: &crate::extensions::ExtensionCatalog,
) -> String {
    let mut lines = vec![
        format!(
            "layout: margin {} · composer {} rows · footer {}",
            editor.layout.horizontal_margin,
            editor.layout.composer_max_height,
            if editor.layout.show_footer {
                "shown"
            } else {
                "hidden"
            }
        ),
        format!(
            "rendering: {} FPS · edits {} · tools {} · sweeps {}",
            editor.presentation.refresh_rate_fps,
            editor.presentation.auto_expand_edits,
            editor.presentation.auto_expand_tools,
            editor.presentation.running_sweeps
        ),
        format!(
            "theme: user {} / {} · assistant {} / {}",
            editor.transcript.user_label_color,
            editor.transcript.user_message_color,
            editor.transcript.assistant_label_color,
            editor.transcript.assistant_message_color
        ),
        format!(
            "alerts: notifications {:?} · sound {:?}",
            editor.interaction.completion_notifications, editor.interaction.completion_sound
        )
        .to_lowercase(),
        format!(
            "commands: {} aliases · keymaps: {} actions",
            agent.commands.aliases.len(),
            agent.keybindings.entries().len()
        ),
    ];
    lines.push(live_extension_summary(catalog, None));
    lines.push(
        "Export/import: `borg customize export|import`; full JSON: `borg customize inspect --json`"
            .to_string(),
    );
    lines.join("\n")
}

fn lsp_support_summary() -> String {
    borg_ui::lsp_support_summary()
}

fn render_event(
    event: &SessionEvent,
    json: bool,
    print: bool,
    rendered: &mut HashMap<Uuid, String>,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(event)?);
        return Ok(());
    }
    if print {
        if let SessionEventKind::TurnCompleted {
            final_text, error, ..
        } = &event.kind
        {
            if let Some(error) = error {
                anyhow::bail!("{error}");
            }
            println!("{final_text}");
        }
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
        HostCommand::Prompt { delivery, .. } | HostCommand::TeamPrompt { delivery, .. } => {
            match delivery {
                PromptDelivery::Steer => "steer",
                PromptDelivery::Queue => "queue",
            }
        }
        HostCommand::RecallQueuedPrompt { .. } => "recall queued prompt",
        HostCommand::FlushPendingInput { .. } => "flush pending input",
        HostCommand::Configure { .. } => "configure",
        HostCommand::Approve { .. } => "approval",
        HostCommand::RespondToProviderInteraction { .. } => "provider interaction response",
        HostCommand::Goal { .. } => "goal",
        HostCommand::Todo { .. } => "todo",
        HostCommand::ExtensionCommand { .. } => "extension command",
        HostCommand::Subagent { .. } => "subagent",
        HostCommand::Interrupt { .. } => "interrupt",
        HostCommand::Compact { .. } => "compact",
        HostCommand::ClearContext { .. } => "clear context",
        HostCommand::Stop { .. } => "stop",
        HostCommand::WorkspaceFilesystem { .. } => "workspace filesystem",
        HostCommand::CancelWorkspaceFilesystem { .. } => "cancel workspace filesystem",
        HostCommand::WorkspaceCommand { .. } => "workspace command",
        HostCommand::CancelWorkspaceCommand { .. } => "cancel workspace command",
        HostCommand::ShellCommand { .. } => "shell command",
        HostCommand::OpenTerminal { .. } => "open terminal",
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
    borg_ui::cancelled_provider_interaction_response(kind)
}

fn provider_interaction_payload_contains_secret(payload: &serde_json::Value) -> bool {
    borg_ui::provider_interaction_contains_secret(payload)
}

fn provider_interaction_response(
    kind: &str,
    payload: &serde_json::Value,
    input: &str,
) -> Result<serde_json::Value> {
    borg_ui::provider_interaction_response(kind, payload, input)
}

#[cfg(test)]
mod tests;
