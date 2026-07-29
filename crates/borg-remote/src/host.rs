use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use tokio::sync::{Mutex, mpsc, watch};
use uuid::Uuid;

use crate::receipt::{ReceiptState, ReceiptStore, atomic_write_secure};
use crate::{
    CodingProvider, HostCapabilities, HostCommand, HostCommandEnvelope, HostHeartbeat,
    LaunchSession, ProviderCapability, REMOTE_PROTOCOL_VERSION, RemoteHost, RemoteHostIdentity,
    SessionEvent, SessionLiveEvent, SessionStore, SessionWriterLease, SqliteSessionStore,
    WorkspaceAttachment, WorkspaceCommandErrorCode, WorkspaceCommandOutcome,
    WorkspaceCommandRequest, WorkspaceCommandResponse, WorkspaceFilesystemErrorCode,
    WorkspaceFilesystemOutcome, WorkspaceFilesystemRequest, WorkspaceFilesystemResponse,
    execute_workspace_command, execute_workspace_filesystem,
    run_agent_session_with_store_and_writer,
};

#[derive(Clone, Serialize, Deserialize)]
pub struct HostConfig {
    pub server: String,
    pub host_id: Uuid,
    pub host_token: String,
    pub name: String,
    pub roots: Vec<PathBuf>,
}

#[derive(Serialize)]
struct EnrollRequest {
    token: String,
    name: String,
    hostname: String,
    platform: String,
    capabilities: HostCapabilities,
}

#[derive(Deserialize)]
struct EnrollResponse {
    host: RemoteHost,
    host_token: String,
}

#[derive(Deserialize)]
struct CommandsResponse {
    commands: Vec<HostCommandEnvelope>,
}

#[derive(Deserialize)]
struct RegisterSessionResponse {
    command_cursor: u64,
    #[serde(default)]
    event_cursor: u64,
    #[serde(default)]
    live_revision: u64,
}

#[derive(Deserialize)]
struct SessionSyncResponse {
    event_cursor: u64,
    live_revision: u64,
}

/// Reconnect cursors are lower bounds, never instructions to rewind.  Taking
/// the component-wise maximum prevents duplicate command delivery and event
/// uploads after either side restarts.
fn merge_reconnect_cursors(
    remote: (u64, u64, u64),
    attachment: Option<&crate::RemoteReconnectSyncCursors>,
) -> (u64, u64, u64) {
    match attachment {
        Some(cursor) => (
            remote.0.max(cursor.command_cursor),
            remote.1.max(cursor.event_cursor),
            remote.2.max(cursor.live_revision),
        ),
        None => remote,
    }
}

fn presence_lease_is_active(lease: &crate::RemotePresenceLease, now: DateTime<Utc>) -> bool {
    lease.expires_at > now
}

#[derive(Serialize)]
struct EventBatch<'a> {
    events: &'a [SessionEvent],
}

#[derive(Serialize)]
struct LiveStateBatch<'a> {
    session_id: Uuid,
    events: &'a [SessionLiveEvent],
}

#[derive(Serialize, Deserialize)]
struct PersistedLaunchMetadata {
    request: LaunchSession,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    attachment: Option<WorkspaceAttachment>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum StoredLaunchMetadata {
    Current(PersistedLaunchMetadata),
    Legacy(LaunchSession),
}

impl StoredLaunchMetadata {
    fn into_current(self) -> PersistedLaunchMetadata {
        match self {
            Self::Current(metadata) => metadata,
            Self::Legacy(request) => PersistedLaunchMetadata {
                request,
                attachment: None,
            },
        }
    }
}

async fn upload_event_payloads(
    client: &Client,
    config: &HostConfig,
    store: &dyn SessionStore,
    events: &[SessionEvent],
) -> Result<bool> {
    let payloads = events
        .iter()
        .flat_map(|event| {
            event
                .kind
                .payload_refs()
                .into_iter()
                .map(move |payload| (event.session_id, payload.clone()))
        })
        .collect::<Vec<_>>();
    for (session_id, payload) in payloads {
        let bytes = store.load_payload(&payload).await?;
        let response = client
            .post(endpoint(
                &config.server,
                &format!(
                    "/api/remote/host/sessions/{session_id}/payloads/{}",
                    payload.id
                ),
            ))
            .bearer_auth(&config.host_token)
            .timeout(Duration::from_secs(30))
            .query(&[
                ("kind", payload.kind.as_str().to_string()),
                ("byte_len", payload.byte_len.to_string()),
            ])
            .body(bytes)
            .send()
            .await;
        match response {
            Ok(response) if response.status().is_success() => {}
            Ok(response) if response.status() == StatusCode::UNAUTHORIZED => {
                bail!("remote host token was rejected; enroll this host again");
            }
            Ok(response) => {
                tracing::warn!(
                    status = %response.status(),
                    payload_id = %payload.id,
                    "remote session payload upload failed; retained for replay"
                );
                return Ok(false);
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    payload_id = %payload.id,
                    "remote session payload upload failed; retained for replay"
                );
                return Ok(false);
            }
        }
    }
    Ok(true)
}

async fn upload_live_state(
    client: &Client,
    config: &HostConfig,
    store: &dyn SessionStore,
    session_id: Uuid,
    uploaded_revision: &mut u64,
) -> Result<bool> {
    let live = store
        .live_events_after(session_id, *uploaded_revision)
        .await?;
    if live.is_empty() {
        return Ok(true);
    }
    let response = client
        .post(endpoint(&config.server, "/api/remote/host/live-state"))
        .bearer_auth(&config.host_token)
        .json(&LiveStateBatch {
            session_id,
            events: &live,
        })
        .send()
        .await;
    match response {
        Ok(response) if response.status().is_success() => {
            *uploaded_revision = live
                .last()
                .map_or(*uploaded_revision, |event| event.revision);
            Ok(true)
        }
        Ok(response) if response.status() == StatusCode::UNAUTHORIZED => {
            bail!("remote host token was rejected; enroll this host again");
        }
        Ok(response) => {
            tracing::warn!(
                status = %response.status(),
                "remote live-state upload failed; retained for replay"
            );
            Ok(false)
        }
        Err(error) => {
            tracing::warn!(%error, "remote live-state upload failed; retained for replay");
            Ok(false)
        }
    }
}

pub fn default_host_config_path() -> PathBuf {
    std::env::var_os("BORG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".borg")))
        .unwrap_or_else(|| PathBuf::from(".borg"))
        .join("remote")
        .join("host.json")
}

pub async fn enroll_host(
    server: &str,
    token: &str,
    name: Option<&str>,
    roots: Vec<PathBuf>,
    config_path: &Path,
) -> Result<HostConfig> {
    let roots = canonical_roots(roots)?;
    let name = name
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(hostname);
    let capabilities = probe_capabilities(roots.clone()).await;
    let response = Client::new()
        .post(endpoint(server, "/api/remote/hosts/enroll"))
        .json(&EnrollRequest {
            token: token.to_string(),
            name: name.clone(),
            hostname: hostname(),
            platform: platform(),
            capabilities,
        })
        .send()
        .await
        .context("failed to contact Borg for host enrollment")?;
    if !response.status().is_success() {
        bail!(
            "host enrollment failed with {}: {}",
            response.status(),
            response.text().await.unwrap_or_default()
        );
    }
    let enrolled: EnrollResponse = response
        .json()
        .await
        .context("Borg returned an invalid host enrollment response")?;
    let config = HostConfig {
        server: server.trim_end_matches('/').to_string(),
        host_id: enrolled.host.id,
        host_token: enrolled.host_token,
        name,
        roots,
    };
    write_config(config_path, &config)?;
    Ok(config)
}

pub async fn probe_capabilities(roots: Vec<PathBuf>) -> HostCapabilities {
    probe_capabilities_with_managed_kimi(roots, false).await
}

async fn probe_capabilities_with_managed_kimi(
    roots: Vec<PathBuf>,
    managed_kimi: bool,
) -> HostCapabilities {
    let mut providers = Vec::new();
    for provider in [
        CodingProvider::Codex,
        CodingProvider::Claude,
        CodingProvider::OpenCode,
        CodingProvider::Kimi,
        CodingProvider::OpenRouter,
        CodingProvider::OpenAiCompatible,
    ] {
        providers.push(probe_provider(provider, managed_kimi).await);
    }
    HostCapabilities {
        protocol_version: REMOTE_PROTOCOL_VERSION,
        providers,
        roots,
        can_launch: true,
        workspace_attachment: Some(workspace_attachment_capabilities()),
    }
}

fn workspace_attachment_capabilities() -> crate::WorkspaceAttachmentCapabilities {
    crate::WorkspaceAttachmentCapabilities {
        presence_leases: true,
        approval_provenance: true,
        reconnect_sync_cursors: true,
        participant_scoped_command_authority: true,
    }
}

async fn probe_provider(provider: CodingProvider, managed_kimi: bool) -> ProviderCapability {
    let version = command_output(provider.executable(), &["--version"])
        .await
        .ok()
        .filter(|value| !value.is_empty());
    let auth = match provider {
        CodingProvider::Codex => command_output("codex", &["login", "status"]).await,
        CodingProvider::Claude => command_output("claude", &["auth", "status"]).await,
        CodingProvider::OpenCode => command_output("opencode", &["providers", "list"]).await,
        CodingProvider::Kimi if managed_kimi => {
            Ok("Borg gateway credentials available".to_string())
        }
        CodingProvider::Kimi => std::env::var("BORG_KIMI_API_KEY")
            .or_else(|_| std::env::var("MOONSHOT_API_KEY"))
            .map(|_| "Local Kimi credentials available".to_string())
            .map_err(anyhow::Error::from),
        CodingProvider::OpenRouter => std::env::var("OPENROUTER_API_KEY")
            .map(|_| "OpenRouter credentials available".to_string())
            .map_err(anyhow::Error::from),
        CodingProvider::OpenAiCompatible => Ok("OpenAI-compatible endpoint available".to_string()),
    };
    let authenticated = auth.as_ref().is_ok_and(|output| match provider {
        // OpenCode prints a non-empty table header even with zero credentials.
        // A bullet represents either a stored credential or a usable provider
        // environment variable.
        CodingProvider::OpenCode => output.lines().any(|line| line.contains('●')),
        _ => !output.trim().is_empty(),
    });
    ProviderCapability {
        provider,
        installed: version.is_some(),
        version,
        authenticated,
        auth_detail: authenticated.then(|| "Provider credentials available".to_string()),
    }
}

async fn command_output(executable: &str, args: &[&str]) -> Result<String> {
    let mut command = Command::new(executable);
    command.args(args).stdin(Stdio::null());
    command_output_from(&mut command, executable).await
}

async fn command_output_from(command: &mut Command, executable: &str) -> Result<String> {
    let output = tokio::time::timeout(Duration::from_secs(8), command.output())
        .await
        .context("provider probe timed out")?
        .with_context(|| format!("{executable} is not installed"))?;
    if !output.status.success() {
        bail!("{executable} command failed");
    }
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    Ok(if stdout.is_empty() { stderr } else { stdout })
}

pub async fn login_provider(provider: CodingProvider) -> Result<()> {
    if provider.uses_native_harness() {
        bail!(
            "{provider:?} uses API credentials from the environment; configure the provider key and endpoint variables"
        );
    }
    let status = match provider {
        CodingProvider::Codex => Command::new("codex")
            .args(["login", "--device-auth"])
            .status(),
        CodingProvider::Claude => Command::new("claude").args(["auth", "login"]).status(),
        CodingProvider::OpenCode => Command::new("opencode")
            .args(["providers", "login"])
            .status(),
        CodingProvider::Kimi | CodingProvider::OpenRouter | CodingProvider::OpenAiCompatible => {
            unreachable!("handled above")
        }
    }
    .await
    .with_context(|| format!("failed to start {} login", provider.executable()))?;
    if !status.success() {
        bail!("{} login exited with {status}", provider.executable());
    }
    Ok(())
}

pub async fn run_host(config_path: &Path) -> Result<()> {
    let config: HostConfig = serde_json::from_slice(
        &fs::read(config_path)
            .with_context(|| format!("failed to read {}", config_path.display()))?,
    )
    .with_context(|| format!("invalid host config {}", config_path.display()))?;
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(35))
        .build()?;
    let acknowledged = Arc::new(AtomicU64::new(0));
    let sessions: Arc<Mutex<HashMap<Uuid, mpsc::Sender<HostCommand>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let session_root = config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("sessions");
    let mut capabilities = probe_capabilities_with_managed_kimi(config.roots.clone(), true).await;
    let mut capabilities_probed_at = Instant::now();
    loop {
        if capabilities_probed_at.elapsed() >= Duration::from_secs(300) {
            capabilities = probe_capabilities_with_managed_kimi(config.roots.clone(), true).await;
            capabilities_probed_at = Instant::now();
        }
        if let Err(error) = heartbeat(
            &client,
            &config,
            capabilities.clone(),
            acknowledged.load(Ordering::Relaxed),
        )
        .await
        {
            tracing::warn!(%error, "remote host heartbeat failed; retrying");
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }
        let response = client
            .get(endpoint(&config.server, "/api/remote/host/commands"))
            .bearer_auth(&config.host_token)
            .query(&[
                ("after", acknowledged.load(Ordering::Relaxed).to_string()),
                ("wait_seconds", "20".to_string()),
            ])
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(%error, "remote command poll failed; retrying");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        if response.status() == StatusCode::UNAUTHORIZED {
            bail!("remote host token was rejected; enroll this host again");
        }
        if !response.status().is_success() {
            tracing::warn!(status = %response.status(), "remote command poll failed");
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }
        let commands: CommandsResponse = response
            .json()
            .await
            .context("Borg returned invalid remote commands")?;
        for envelope in commands.commands {
            let handled = dispatch(
                client.clone(),
                config.clone(),
                session_root.clone(),
                sessions.clone(),
                envelope.command,
            )
            .await;
            if !handled {
                tokio::time::sleep(Duration::from_secs(2)).await;
                break;
            }
            acknowledged.fetch_max(envelope.sequence, Ordering::Relaxed);
        }
    }
}

/// Mirror a terminal-owned session through the enrolled host relay.
///
/// The terminal remains the process owner and journal authority. This task
/// only registers the session, uploads its durable suffix, and forwards typed
/// commands from web/mobile clients.
pub async fn mirror_local_session(
    config_path: &Path,
    store: Arc<dyn crate::SessionStore>,
    session_id: Uuid,
    request: LaunchSession,
    commands: mpsc::Sender<HostCommand>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let config: HostConfig = serde_json::from_slice(
        &fs::read(config_path)
            .with_context(|| format!("failed to read {}", config_path.display()))?,
    )
    .with_context(|| format!("invalid host config {}", config_path.display()))?;
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(5))
        .build()?;
    let registration = serde_json::json!({
        "session_id": session_id,
        "request": request,
    });
    let command_cursor = loop {
        let response = client
            .post(endpoint(&config.server, "/api/remote/host/sessions"))
            .bearer_auth(&config.host_token)
            .json(&registration)
            .send()
            .await;
        match response {
            Ok(response) if response.status().is_success() => {
                let registered: RegisterSessionResponse = response
                    .json()
                    .await
                    .context("Borg returned an invalid local session registration")?;
                break merge_reconnect_cursors(
                    (
                        registered.command_cursor,
                        registered.event_cursor,
                        registered.live_revision,
                    ),
                    None,
                );
            }
            Ok(response) if response.status() == StatusCode::UNAUTHORIZED => {
                bail!("remote host token was rejected; enroll this host again");
            }
            Ok(response)
                if response.status().is_server_error()
                    || response.status() == StatusCode::TOO_MANY_REQUESTS =>
            {
                tracing::warn!(
                    status = %response.status(),
                    "local session registration failed; retrying"
                );
            }
            Ok(response) => {
                let status = response.status();
                let detail = response.text().await.unwrap_or_default().trim().to_string();
                if detail.is_empty() {
                    bail!("remote session registration was rejected ({status})");
                }
                bail!("remote session registration was rejected ({status}): {detail}");
            }
            Err(error) => {
                tracing::warn!(%error, "local session registration failed; retrying");
            }
        }
        if wait_for_mirror_shutdown(&mut shutdown, Duration::from_secs(2)).await {
            return Ok(());
        }
    };

    let capabilities = probe_capabilities(config.roots.clone()).await;
    let mut last_heartbeat = Instant::now() - Duration::from_secs(30);
    let (mut command_cursor, mut uploaded_sequence, mut uploaded_live_revision) = command_cursor;
    loop {
        if last_heartbeat.elapsed() >= Duration::from_secs(15) {
            if let Err(error) =
                heartbeat(&client, &config, capabilities.clone(), command_cursor).await
            {
                tracing::warn!(%error, "local session heartbeat failed");
            } else {
                last_heartbeat = Instant::now();
            }
        }

        let pending = store
            .events_after(session_id, uploaded_sequence, 1_024)
            .await?;
        let mut upload_failed = false;
        for events in pending.chunks(256) {
            if !upload_event_payloads(&client, &config, store.as_ref(), events).await? {
                upload_failed = true;
                break;
            }
            let response = client
                .post(endpoint(&config.server, "/api/remote/host/events"))
                .bearer_auth(&config.host_token)
                .json(&EventBatch { events })
                .send()
                .await;
            match response {
                Ok(response) if response.status().is_success() => {
                    uploaded_sequence = events
                        .last()
                        .map_or(uploaded_sequence, |event| event.sequence);
                }
                Ok(response) if response.status() == StatusCode::UNAUTHORIZED => {
                    bail!("remote host token was rejected; enroll this host again");
                }
                Ok(response) if response.status() == StatusCode::CONFLICT => {
                    bail!(
                        "local session event replay conflicted with the durable remote journal: {}",
                        response.text().await.unwrap_or_default()
                    );
                }
                Ok(response) => {
                    tracing::warn!(
                        status = %response.status(),
                        "local session event upload failed; journaled for replay"
                    );
                    upload_failed = true;
                    break;
                }
                Err(error) => {
                    tracing::warn!(%error, "local session event upload failed; journaled for replay");
                    upload_failed = true;
                    break;
                }
            }
        }
        if !upload_failed
            && !upload_live_state(
                &client,
                &config,
                store.as_ref(),
                session_id,
                &mut uploaded_live_revision,
            )
            .await?
        {
            upload_failed = true;
        }
        if upload_failed {
            if wait_for_mirror_shutdown(&mut shutdown, Duration::from_secs(2)).await {
                return Ok(());
            }
            continue;
        }
        if *shutdown.borrow() {
            return Ok(());
        }
        if pending.len() < 1_024
            && wait_for_mirror_shutdown(&mut shutdown, Duration::from_millis(250)).await
        {
            return Ok(());
        }

        let request = client
            .get(endpoint(&config.server, "/api/remote/host/commands"))
            .bearer_auth(&config.host_token)
            .query(&[
                ("after", command_cursor.to_string()),
                ("wait_seconds", "1".to_string()),
            ])
            .send();
        let response = tokio::select! {
            response = request => Some(response),
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    None
                } else {
                    continue;
                }
            }
        };
        let Some(response) = response else {
            // Run one final loop so every journaled event, including the
            // terminal status, is durably uploaded before this task exits.
            continue;
        };
        let response = match response {
            Ok(response) if response.status().is_success() => response,
            Ok(response) if response.status() == StatusCode::UNAUTHORIZED => {
                bail!("remote host token was rejected; enroll this host again");
            }
            Ok(response) if response.status() == StatusCode::CONFLICT => {
                bail!(
                    "remote event replay conflicted with the durable remote journal: {}",
                    response.text().await.unwrap_or_default()
                );
            }
            Ok(response) => {
                tracing::warn!(status = %response.status(), "local session command poll failed");
                if wait_for_mirror_shutdown(&mut shutdown, Duration::from_secs(2)).await {
                    continue;
                }
                continue;
            }
            Err(error) => {
                tracing::warn!(%error, "local session command poll failed");
                if wait_for_mirror_shutdown(&mut shutdown, Duration::from_secs(2)).await {
                    continue;
                }
                continue;
            }
        };
        let response: CommandsResponse = response
            .json()
            .await
            .context("Borg returned invalid remote commands")?;
        for envelope in response.commands {
            command_cursor = command_cursor.max(envelope.sequence);
            if envelope.command.session_id() == Some(session_id)
                && commands.send(envelope.command).await.is_err()
            {
                return Ok(());
            }
        }
    }
}

async fn wait_for_mirror_shutdown(shutdown: &mut watch::Receiver<bool>, delay: Duration) -> bool {
    if *shutdown.borrow() {
        return true;
    }
    tokio::select! {
        _ = tokio::time::sleep(delay) => false,
        changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
    }
}

async fn heartbeat(
    client: &Client,
    config: &HostConfig,
    capabilities: HostCapabilities,
    acknowledged_command_sequence: u64,
) -> Result<()> {
    let response = client
        .post(endpoint(&config.server, "/api/remote/host/heartbeat"))
        .bearer_auth(&config.host_token)
        .json(&HostHeartbeat {
            name: config.name.clone(),
            platform: platform(),
            hostname: hostname(),
            capabilities,
            acknowledged_command_sequence,
            identity: Some(RemoteHostIdentity {
                host_id: config.host_id,
                hostname: hostname(),
                platform: platform(),
            }),
        })
        .send()
        .await
        .context("failed to send remote host heartbeat")?;
    if !response.status().is_success() {
        bail!("remote host heartbeat failed with {}", response.status());
    }
    Ok(())
}

async fn load_session_sync(
    client: &Client,
    config: &HostConfig,
    session_id: Uuid,
) -> Result<SessionSyncResponse> {
    let response = client
        .get(endpoint(
            &config.server,
            &format!("/api/remote/host/sessions/{session_id}/sync"),
        ))
        .bearer_auth(&config.host_token)
        .send()
        .await
        .context("failed to load remote session sync cursor")?;
    if response.status() == StatusCode::UNAUTHORIZED {
        bail!("remote host token was rejected; enroll this host again");
    }
    if !response.status().is_success() {
        bail!(
            "remote session sync cursor failed with {}",
            response.status()
        );
    }
    response
        .json()
        .await
        .context("Borg returned an invalid session sync cursor")
}

async fn dispatch(
    client: Client,
    config: HostConfig,
    session_root: PathBuf,
    sessions: Arc<Mutex<HashMap<Uuid, mpsc::Sender<HostCommand>>>>,
    command: HostCommand,
) -> bool {
    if let HostCommand::WorkspaceFilesystem { request } = &command {
        let receipts = ReceiptStore::new(
            session_root
                .parent()
                .unwrap_or(&session_root)
                .join("filesystem-receipts"),
        );
        let Some(response) = filesystem_response(&receipts, &config, request).await else {
            return false;
        };
        let result = client
            .post(endpoint(
                &config.server,
                "/api/remote/host/filesystem-results",
            ))
            .bearer_auth(&config.host_token)
            .json(&response)
            .send()
            .await;
        let uploaded = match result {
            Ok(result) if result.status().is_success() => true,
            Ok(result) if matches!(result.status(), StatusCode::NOT_FOUND | StatusCode::GONE) => {
                tracing::warn!(
                    status = %result.status(),
                    request_id = %response.request_id,
                    "filesystem request no longer exists on the relay; acknowledging terminal result"
                );
                true
            }
            Ok(result) => {
                tracing::warn!(
                    status = %result.status(),
                    request_id = %response.request_id,
                    "filesystem result upload failed"
                );
                false
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    request_id = %response.request_id,
                    "filesystem result upload failed"
                );
                false
            }
        };
        return uploaded;
    }
    if let HostCommand::CancelWorkspaceFilesystem { .. } = &command {
        return true;
    }
    if let HostCommand::WorkspaceCommand { request } = &command {
        let receipts = ReceiptStore::new(
            session_root
                .parent()
                .unwrap_or(&session_root)
                .join("command-receipts"),
        );
        let Some(response) = workspace_command_response(&receipts, &config, request).await else {
            return false;
        };
        let result = client
            .post(endpoint(
                &config.server,
                "/api/remote/host/workspace-command-results",
            ))
            .bearer_auth(&config.host_token)
            .json(&response)
            .send()
            .await;
        let uploaded = match result {
            Ok(result) if result.status().is_success() => true,
            Ok(result) if matches!(result.status(), StatusCode::NOT_FOUND | StatusCode::GONE) => {
                tracing::warn!(
                    status = %result.status(),
                    request_id = %response.request_id,
                    "workspace command no longer exists on the relay; acknowledging terminal result"
                );
                true
            }
            Ok(result) => {
                tracing::warn!(
                    status = %result.status(),
                    request_id = %response.request_id,
                    "workspace command result upload failed"
                );
                false
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    request_id = %response.request_id,
                    "workspace command result upload failed"
                );
                false
            }
        };
        return uploaded;
    }
    if let HostCommand::CancelWorkspaceCommand { .. } = &command {
        return true;
    }
    if let HostCommand::Launch {
        session_id,
        request,
        attachment,
    } = command
    {
        let metadata_path = session_root.join(format!("{session_id}.launch.json"));
        if let Err(error) = validate_workspace_attachment(&config, session_id, attachment.as_ref())
            .and_then(|()| persist_launch_metadata(&metadata_path, &request, attachment.as_ref()))
        {
            tracing::error!(%error, %session_id, "failed to persist remote session launch");
            return false;
        }
        if sessions.lock().await.contains_key(&session_id) {
            return true;
        }
        spawn_host_session(client, config, session_root, sessions, session_id, request).await;
        return true;
    }
    if let Some(session_id) = command.session_id() {
        let metadata_path = session_root.join(format!("{session_id}.launch.json"));
        let attachment = fs::read(&metadata_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<StoredLaunchMetadata>(&bytes).ok())
            .map(StoredLaunchMetadata::into_current)
            .and_then(|metadata| metadata.attachment);
        if let Some(attachment) = attachment.as_ref()
            && let Err(error) = authorize_workspace_command(attachment, &command)
        {
            tracing::warn!(%error, %session_id, "rejected unauthorized workspace command");
            // The command was handled by rejecting it. Leaving its envelope
            // unacknowledged would make the relay retry the same denied
            // command forever and head-of-line block every later command.
            return true;
        }
        let existing = { sessions.lock().await.get(&session_id).cloned() };
        let session = match existing {
            Some(session) => Some(session),
            None => {
                let metadata_path = session_root.join(format!("{session_id}.launch.json"));
                let request = fs::read(&metadata_path)
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<StoredLaunchMetadata>(&bytes).ok())
                    .map(StoredLaunchMetadata::into_current);
                if let Some(metadata) = request {
                    if let Err(error) = validate_workspace_attachment(
                        &config,
                        session_id,
                        metadata.attachment.as_ref(),
                    ) {
                        tracing::error!(%error, %session_id, "stored launch attachment is invalid");
                        return false;
                    }
                    Some(
                        spawn_host_session(
                            client,
                            config,
                            session_root,
                            sessions,
                            session_id,
                            metadata.request,
                        )
                        .await,
                    )
                } else {
                    None
                }
            }
        };
        if let Some(session) = session {
            session.send(command).await.ok();
        }
    }
    true
}

async fn filesystem_response(
    receipts: &ReceiptStore,
    config: &HostConfig,
    request: &WorkspaceFilesystemRequest,
) -> Option<WorkspaceFilesystemResponse> {
    let state = match receipts.load(request.request_id, request) {
        Ok(state) => state,
        Err(error) => {
            tracing::warn!(%error, request_id = %request.request_id, "failed to read filesystem receipt");
            return None;
        }
    };
    match state {
        ReceiptState::Terminal(response) | ReceiptState::Legacy(response) => Some(response),
        ReceiptState::Started => {
            let response = indeterminate_filesystem_response(
                request,
                "the host restarted after this mutation began; Borg will not replay it because its outcome cannot be proven",
            );
            persist_filesystem_terminal(receipts, request, &response)
        }
        ReceiptState::Conflict => Some(indeterminate_filesystem_response(
            request,
            "request_id was already used for a different filesystem request",
        )),
        ReceiptState::Corrupt if request.operation.is_mutating() => {
            Some(indeterminate_filesystem_response(
                request,
                "the durable mutation receipt is unreadable; Borg will not replay the operation",
            ))
        }
        ReceiptState::Missing | ReceiptState::Corrupt => {
            if request.operation.is_mutating()
                && let Err(error) = receipts.begin(request.request_id, request)
            {
                tracing::warn!(%error, request_id = %request.request_id, "failed to durably begin filesystem mutation");
                return None;
            }
            let response = execute_workspace_filesystem(&config.roots, request.clone()).await;
            persist_filesystem_terminal(receipts, request, &response)
        }
    }
}

fn persist_filesystem_terminal(
    receipts: &ReceiptStore,
    request: &WorkspaceFilesystemRequest,
    response: &WorkspaceFilesystemResponse,
) -> Option<WorkspaceFilesystemResponse> {
    if let Err(error) = receipts.finish(request.request_id, request, response) {
        tracing::warn!(%error, request_id = %request.request_id, "failed to persist terminal filesystem receipt");
        return None;
    }
    Some(response.clone())
}

fn indeterminate_filesystem_response(
    request: &WorkspaceFilesystemRequest,
    message: &str,
) -> WorkspaceFilesystemResponse {
    WorkspaceFilesystemResponse {
        request_id: request.request_id,
        workspace_id: request.workspace_id,
        outcome: WorkspaceFilesystemOutcome::Failure {
            code: WorkspaceFilesystemErrorCode::Indeterminate,
            message: message.to_string(),
            retryable: false,
        },
    }
}

async fn workspace_command_response(
    receipts: &ReceiptStore,
    config: &HostConfig,
    request: &WorkspaceCommandRequest,
) -> Option<WorkspaceCommandResponse> {
    let state = match receipts.load(request.request_id, request) {
        Ok(state) => state,
        Err(error) => {
            tracing::warn!(%error, request_id = %request.request_id, "failed to read workspace command receipt");
            return None;
        }
    };
    match state {
        ReceiptState::Terminal(response) | ReceiptState::Legacy(response) => Some(response),
        ReceiptState::Started => {
            let response = indeterminate_command_response(
                request,
                "the host restarted after this command began; Borg will not replay it because its side effects cannot be proven",
            );
            persist_command_terminal(receipts, request, &response)
        }
        ReceiptState::Conflict => Some(indeterminate_command_response(
            request,
            "request_id was already used for a different workspace command",
        )),
        ReceiptState::Corrupt => Some(indeterminate_command_response(
            request,
            "the durable command receipt is unreadable; Borg will not replay the command",
        )),
        ReceiptState::Missing => {
            if let Err(error) = receipts.begin(request.request_id, request) {
                tracing::warn!(%error, request_id = %request.request_id, "failed to durably begin workspace command");
                return None;
            }
            let response = execute_workspace_command(&config.roots, request.clone()).await;
            persist_command_terminal(receipts, request, &response)
        }
    }
}

fn persist_command_terminal(
    receipts: &ReceiptStore,
    request: &WorkspaceCommandRequest,
    response: &WorkspaceCommandResponse,
) -> Option<WorkspaceCommandResponse> {
    if let Err(error) = receipts.finish(request.request_id, request, response) {
        tracing::warn!(%error, request_id = %request.request_id, "failed to persist terminal workspace command receipt");
        return None;
    }
    Some(response.clone())
}

fn indeterminate_command_response(
    request: &WorkspaceCommandRequest,
    message: &str,
) -> WorkspaceCommandResponse {
    WorkspaceCommandResponse {
        request_id: request.request_id,
        workspace_id: request.workspace_id,
        outcome: WorkspaceCommandOutcome::Failure {
            code: WorkspaceCommandErrorCode::Indeterminate,
            message: message.to_string(),
            retryable: false,
        },
    }
}

fn validate_workspace_attachment(
    config: &HostConfig,
    session_id: Uuid,
    attachment: Option<&WorkspaceAttachment>,
) -> Result<()> {
    let Some(attachment) = attachment else {
        return Ok(());
    };
    if attachment.workspace_id.is_some() != attachment.participant_id.is_some() {
        bail!("workspace attachment requires workspace_id and participant_id together");
    }
    if let Some(authority) = &attachment.command_authority {
        let participant_id = attachment
            .participant_id
            .context("participant command authority requires a workspace participant")?;
        ensure!(
            authority.participant_id == participant_id,
            "participant command authority does not match workspace participant"
        );
    }
    if let Some(identity) = &attachment.host_identity
        && identity.host_id != config.host_id
    {
        bail!("workspace attachment host identity does not match enrolled host");
    }
    if let Some(lease) = &attachment.presence_lease
        && !presence_lease_is_active(lease, Utc::now())
    {
        bail!("workspace presence lease has expired");
    }
    if attachment.reconnect_sync_cursors.is_some() && session_id.is_nil() {
        bail!("workspace reconnect cursors require a non-nil session id");
    }
    Ok(())
}

fn authorize_workspace_command(
    attachment: &WorkspaceAttachment,
    command: &HostCommand,
) -> Result<()> {
    let Some(authority) = &attachment.command_authority else {
        return Ok(());
    };
    let kind = match command {
        HostCommand::Prompt { .. } => crate::ParticipantCommandKind::Prompt,
        HostCommand::RecallQueuedPrompt { .. } => crate::ParticipantCommandKind::RecallQueuedPrompt,
        HostCommand::Configure { .. } => crate::ParticipantCommandKind::Configure,
        HostCommand::Approve { .. } => crate::ParticipantCommandKind::Approve,
        HostCommand::RespondToProviderInteraction { .. } => {
            crate::ParticipantCommandKind::RespondToProviderInteraction
        }
        HostCommand::Goal { .. } => crate::ParticipantCommandKind::Goal,
        HostCommand::Todo { .. } => crate::ParticipantCommandKind::Todo,
        HostCommand::Subagent { .. } => crate::ParticipantCommandKind::Subagent,
        HostCommand::Interrupt { .. } => crate::ParticipantCommandKind::Interrupt,
        HostCommand::Compact { .. } => crate::ParticipantCommandKind::Compact,
        HostCommand::ClearContext { .. } => crate::ParticipantCommandKind::ClearContext,
        HostCommand::Stop { .. } => crate::ParticipantCommandKind::Stop,
        HostCommand::Launch { .. }
        | HostCommand::WorkspaceFilesystem { .. }
        | HostCommand::CancelWorkspaceFilesystem { .. }
        | HostCommand::WorkspaceCommand { .. }
        | HostCommand::CancelWorkspaceCommand { .. } => return Ok(()),
    };
    ensure!(
        authority.allowed.contains(&kind),
        "workspace participant {} is not authorized for {kind:?}",
        authority.participant_id
    );
    Ok(())
}

fn persist_launch_metadata(
    path: &Path,
    request: &LaunchSession,
    attachment: Option<&WorkspaceAttachment>,
) -> Result<()> {
    let current = PersistedLaunchMetadata {
        request: request.clone(),
        attachment: attachment.cloned(),
    };
    if path.exists() {
        let bytes = fs::read(path)
            .with_context(|| format!("failed to read launch metadata {}", path.display()))?;
        let existing: StoredLaunchMetadata = serde_json::from_slice(&bytes)
            .with_context(|| format!("invalid launch metadata {}", path.display()))?;
        if serde_json::to_value(existing.into_current())? != serde_json::to_value(&current)? {
            bail!(
                "session launch metadata already exists for a different launch request: {}",
                path.display()
            );
        }
        return Ok(());
    }
    atomic_write_secure(path, &serde_json::to_vec_pretty(&current)?)
        .with_context(|| format!("failed to atomically write {}", path.display()))
}

async fn spawn_host_session(
    client: Client,
    config: HostConfig,
    session_root: PathBuf,
    sessions: Arc<Mutex<HashMap<Uuid, mpsc::Sender<HostCommand>>>>,
    session_id: Uuid,
    request: LaunchSession,
) -> mpsc::Sender<HostCommand> {
    let (tx, rx) = mpsc::channel(64);
    sessions.lock().await.insert(session_id, tx.clone());
    let sessions_for_cleanup = sessions.clone();
    tokio::spawn(async move {
        if let Err(error) = run_session(client, config, session_root, session_id, request, rx).await
        {
            tracing::error!(session_id = %session_id, %error, "remote agent session failed");
        }
        sessions_for_cleanup.lock().await.remove(&session_id);
    });
    tx
}

async fn run_session(
    client: Client,
    config: HostConfig,
    session_root: PathBuf,
    session_id: Uuid,
    mut launch: LaunchSession,
    commands: mpsc::Receiver<HostCommand>,
) -> Result<()> {
    launch.cwd = validate_host_cwd(&config.roots, &launch.cwd)?;
    let journal_path = session_root.join(format!("{session_id}.jsonl"));
    let writer = SessionWriterLease::acquire(&journal_path)?;
    let sqlite_store =
        Arc::new(SqliteSessionStore::open(session_root.join("sessions.sqlite3")).await?);
    if !sqlite_store.contains_session(session_id).await? {
        if journal_path.is_file() {
            sqlite_store.import_jsonl(&journal_path).await?;
        } else {
            sqlite_store.create_session(session_id).await?;
        }
    }
    let store: Arc<dyn SessionStore> = sqlite_store.clone();
    let cursor = load_session_sync(&client, &config, session_id).await?;
    let mut sync = JournalSync {
        uploaded_sequence: cursor.event_cursor,
        uploaded_live_revision: cursor.live_revision,
        retry_at: Instant::now(),
    };
    flush_pending(&client, &config, store.as_ref(), session_id, &mut sync).await?;
    let (event_tx, mut event_rx) = mpsc::channel(256);
    let actor_session_root = session_root.clone();
    let actor_store = Arc::clone(&sqlite_store);
    let executor = Arc::new(crate::LocalAgentTurnExecutor::with_model_gateway(
        borg_provider::provider::ModelGateway {
            endpoint: endpoint(&config.server, "/api/remote/host/kimi/chat/completions"),
            bearer_token: config.host_token.clone(),
        },
    ));
    let actor = tokio::spawn(async move {
        run_agent_session_with_store_and_writer(
            &actor_session_root,
            session_id,
            launch,
            commands,
            event_tx,
            executor,
            actor_store,
            writer,
        )
        .await
    });
    loop {
        tokio::select! {
            event = event_rx.recv() => {
                if event.is_none() {
                    break;
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
        flush_pending(&client, &config, store.as_ref(), session_id, &mut sync).await?;
    }
    actor.await.context("agent session task failed")??;
    flush_pending(&client, &config, store.as_ref(), session_id, &mut sync).await
}

struct JournalSync {
    uploaded_sequence: u64,
    uploaded_live_revision: u64,
    retry_at: Instant,
}

async fn flush_pending(
    client: &Client,
    config: &HostConfig,
    store: &dyn SessionStore,
    session_id: Uuid,
    sync: &mut JournalSync,
) -> Result<()> {
    if Instant::now() < sync.retry_at {
        return Ok(());
    }
    const PAGE_SIZE: usize = 1_024;
    loop {
        let pending = store
            .events_after(session_id, sync.uploaded_sequence, PAGE_SIZE)
            .await?;
        let caught_up = pending.len() < PAGE_SIZE;
        for events in pending.chunks(256) {
            if !upload_event_payloads(client, config, store, events).await? {
                sync.retry_at = Instant::now() + Duration::from_secs(2);
                return Ok(());
            }
            let response = client
                .post(endpoint(&config.server, "/api/remote/host/events"))
                .bearer_auth(&config.host_token)
                .timeout(Duration::from_secs(3))
                .json(&EventBatch { events })
                .send()
                .await;
            match response {
                Ok(response) if response.status().is_success() => {}
                Ok(response) if response.status() == StatusCode::UNAUTHORIZED => {
                    bail!("remote host token was rejected; enroll this host again");
                }
                Ok(response) => {
                    tracing::warn!(
                        status = %response.status(),
                        "remote event upload failed; journaled for replay"
                    );
                    sync.retry_at = Instant::now() + Duration::from_secs(2);
                    return Ok(());
                }
                Err(error) => {
                    tracing::warn!(%error, "remote event upload failed; journaled for replay");
                    sync.retry_at = Instant::now() + Duration::from_secs(2);
                    return Ok(());
                }
            }
            if let Some(last) = events.last() {
                sync.uploaded_sequence = last.sequence;
            }
        }
        if caught_up {
            break;
        }
    }
    if !upload_live_state(
        client,
        config,
        store,
        session_id,
        &mut sync.uploaded_live_revision,
    )
    .await?
    {
        sync.retry_at = Instant::now() + Duration::from_secs(2);
        return Ok(());
    }
    sync.retry_at = Instant::now();
    Ok(())
}

fn validate_host_cwd(roots: &[PathBuf], requested: &Path) -> Result<PathBuf> {
    let cwd = requested
        .canonicalize()
        .with_context(|| format!("session directory does not exist: {}", requested.display()))?;
    if !cwd.is_dir() {
        bail!("session path is not a directory: {}", cwd.display());
    }
    if !roots.is_empty() && !roots.iter().any(|root| cwd.starts_with(root)) {
        bail!("session directory is outside this host's enrolled roots");
    }
    Ok(cwd)
}

fn canonical_roots(roots: Vec<PathBuf>) -> Result<Vec<PathBuf>> {
    roots
        .into_iter()
        .map(|root| {
            root.canonicalize()
                .with_context(|| format!("host root does not exist: {}", root.display()))
        })
        .collect()
}

fn write_config(path: &Path, config: &HostConfig) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(config)?;
    fs::write(path, bytes).with_context(|| format!("failed to write {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn endpoint(server: &str, path: &str) -> String {
    format!("{}{}", server.trim_end_matches('/'), path)
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| fs::read_to_string("/etc/hostname").ok())
        .map(|value| value.trim().to_string())
        .unwrap_or_else(|| "Borg host".to_string())
}

fn platform() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

#[cfg(test)]
mod tests {
    use chrono::Duration as ChronoDuration;
    use tempfile::tempdir;

    use super::*;
    use crate::WorkspaceFilesystemOperation;

    #[test]
    fn workspace_attachment_requires_coherent_identity_and_live_lease() {
        let root = tempdir().unwrap();
        let config = test_config(root.path());
        let attachment = WorkspaceAttachment {
            workspace_id: Some(Uuid::new_v4()),
            participant_id: Some(Uuid::new_v4()),
            command_authority: None,
            host_identity: Some(RemoteHostIdentity {
                host_id: config.host_id,
                hostname: "test".to_string(),
                platform: "test".to_string(),
            }),
            host_capabilities: None,
            presence_lease: Some(crate::RemotePresenceLease {
                lease_id: Uuid::new_v4(),
                expires_at: Utc::now() + ChronoDuration::minutes(1),
            }),
            approval_provenance: None,
            reconnect_sync_cursors: Some(crate::RemoteReconnectSyncCursors {
                command_cursor: 3,
                event_cursor: 5,
                live_revision: 8,
            }),
        };
        assert!(validate_workspace_attachment(&config, Uuid::new_v4(), Some(&attachment)).is_ok());

        let mut expired = attachment;
        expired.presence_lease.as_mut().unwrap().expires_at =
            Utc::now() - ChronoDuration::seconds(1);
        assert!(validate_workspace_attachment(&config, Uuid::new_v4(), Some(&expired)).is_err());
    }

    #[test]
    fn participant_command_authority_rejects_commands_outside_the_grant() {
        let participant_id = Uuid::new_v4();
        let attachment = WorkspaceAttachment {
            workspace_id: Some(Uuid::new_v4()),
            participant_id: Some(participant_id),
            command_authority: Some(crate::ParticipantCommandAuthority {
                participant_id,
                allowed: vec![crate::ParticipantCommandKind::Prompt],
            }),
            host_identity: None,
            host_capabilities: None,
            presence_lease: None,
            approval_provenance: None,
            reconnect_sync_cursors: None,
        };
        let session_id = Uuid::new_v4();
        assert!(
            authorize_workspace_command(
                &attachment,
                &HostCommand::Prompt {
                    session_id,
                    message_id: Uuid::new_v4(),
                    text: "allowed".to_string(),
                    attachments: Vec::new(),
                    output_schema: None,
                    delivery: crate::PromptDelivery::Queue,
                },
            )
            .is_ok()
        );
        assert!(
            authorize_workspace_command(&attachment, &HostCommand::Stop { session_id }).is_err()
        );
    }

    #[tokio::test]
    async fn denied_session_command_is_consumed_without_blocking_the_host_queue() {
        let root = tempdir().unwrap();
        let config = test_config(root.path());
        let session_id = Uuid::new_v4();
        let participant_id = Uuid::new_v4();
        let attachment = WorkspaceAttachment {
            workspace_id: Some(Uuid::new_v4()),
            participant_id: Some(participant_id),
            command_authority: Some(crate::ParticipantCommandAuthority {
                participant_id,
                allowed: vec![crate::ParticipantCommandKind::Prompt],
            }),
            host_identity: None,
            host_capabilities: None,
            presence_lease: None,
            approval_provenance: None,
            reconnect_sync_cursors: None,
        };
        let launch = LaunchSession {
            request_id: Uuid::new_v4(),
            cwd: root.path().to_path_buf(),
            provider: CodingProvider::Codex,
            model: None,
            effort: None,
            fast: Some(false),
            response_language: crate::ResponseLanguage::Auto,
            permission_mode: crate::PermissionMode::Manual,
            name: None,
            initial_prompt: None,
            capabilities: Default::default(),
            subagent_concurrency_limit: None,
            extension_skill_roots: Vec::new(),
            team_policy: None,
        };
        persist_launch_metadata(
            &root.path().join(format!("{session_id}.launch.json")),
            &launch,
            Some(&attachment),
        )
        .unwrap();
        let sessions = Arc::new(Mutex::new(HashMap::new()));

        assert!(
            dispatch(
                Client::new(),
                config,
                root.path().to_path_buf(),
                Arc::clone(&sessions),
                HostCommand::Stop { session_id },
            )
            .await,
            "a denied command must be acknowledged as handled"
        );
        assert!(sessions.lock().await.is_empty());
    }

    #[test]
    fn participant_command_authority_must_match_the_attachment_identity() {
        let root = tempdir().unwrap();
        let config = test_config(root.path());
        let attachment = WorkspaceAttachment {
            workspace_id: Some(Uuid::new_v4()),
            participant_id: Some(Uuid::new_v4()),
            command_authority: Some(crate::ParticipantCommandAuthority {
                participant_id: Uuid::new_v4(),
                allowed: Vec::new(),
            }),
            host_identity: None,
            host_capabilities: None,
            presence_lease: None,
            approval_provenance: None,
            reconnect_sync_cursors: None,
        };
        assert!(validate_workspace_attachment(&config, Uuid::new_v4(), Some(&attachment)).is_err());
    }

    #[test]
    fn host_declares_participant_scoped_attachment_authority() {
        assert!(workspace_attachment_capabilities().participant_scoped_command_authority);
    }

    #[test]
    fn reconnect_cursors_never_rewind_after_restart() {
        let attachment = crate::RemoteReconnectSyncCursors {
            command_cursor: 9,
            event_cursor: 12,
            live_revision: 7,
        };
        assert_eq!(
            merge_reconnect_cursors((4, 15, 2), Some(&attachment)),
            (9, 15, 7)
        );
        assert_eq!(merge_reconnect_cursors((4, 15, 2), None), (4, 15, 2));
    }

    #[test]
    fn presence_lease_expiry_is_not_durable_offline_presence() {
        let lease = crate::RemotePresenceLease {
            lease_id: Uuid::new_v4(),
            expires_at: Utc::now() + ChronoDuration::seconds(1),
        };
        assert!(presence_lease_is_active(&lease, Utc::now()));
        assert!(!presence_lease_is_active(
            &lease,
            Utc::now() + ChronoDuration::seconds(2)
        ));
    }

    #[test]
    fn host_cwd_must_stay_inside_enrolled_root() {
        let root = tempdir().unwrap();
        let child = root.path().join("repo");
        fs::create_dir(&child).unwrap();
        assert!(validate_host_cwd(&[root.path().to_path_buf()], &child).is_ok());

        let outside = tempdir().unwrap();
        assert!(validate_host_cwd(&[root.path().to_path_buf()], outside.path()).is_err());
    }

    #[tokio::test]
    async fn started_filesystem_mutation_is_not_replayed() {
        let root = tempdir().unwrap();
        let target = root.path().join("keep.txt");
        fs::write(&target, "still here").unwrap();
        let request = WorkspaceFilesystemRequest {
            request_id: Uuid::new_v4(),
            workspace_id: Uuid::new_v4(),
            root_path: root.path().to_path_buf(),
            timeout_ms: 1_000,
            operation: WorkspaceFilesystemOperation::Delete {
                path: PathBuf::from("keep.txt"),
                archive: false,
                recursive: false,
            },
        };
        let receipts = ReceiptStore::new(root.path().join("receipts"));
        receipts.begin(request.request_id, &request).unwrap();

        let response = filesystem_response(&receipts, &test_config(root.path()), &request)
            .await
            .unwrap();

        assert!(target.exists(), "indeterminate delete must not be replayed");
        assert!(matches!(
            response.outcome,
            WorkspaceFilesystemOutcome::Failure {
                code: WorkspaceFilesystemErrorCode::Indeterminate,
                retryable: false,
                ..
            }
        ));
        assert!(matches!(
            receipts
                .load::<_, WorkspaceFilesystemResponse>(request.request_id, &request)
                .unwrap(),
            ReceiptState::Terminal(_)
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn started_workspace_command_is_not_replayed() {
        let root = tempdir().unwrap();
        let marker = root.path().join("must-not-exist");
        let request = WorkspaceCommandRequest {
            request_id: Uuid::new_v4(),
            workspace_id: Uuid::new_v4(),
            root_path: root.path().to_path_buf(),
            cwd: PathBuf::from("."),
            command: vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                format!("touch {}", marker.display()),
            ],
            timeout_ms: 1_000,
            output_max_bytes: 1_024,
        };
        let receipts = ReceiptStore::new(root.path().join("receipts"));
        receipts.begin(request.request_id, &request).unwrap();

        let response = workspace_command_response(&receipts, &test_config(root.path()), &request)
            .await
            .unwrap();

        assert!(
            !marker.exists(),
            "indeterminate command must not be replayed"
        );
        assert!(matches!(
            response.outcome,
            WorkspaceCommandOutcome::Failure {
                code: WorkspaceCommandErrorCode::Indeterminate,
                retryable: false,
                ..
            }
        ));
    }

    fn test_config(root: &Path) -> HostConfig {
        HostConfig {
            server: "http://localhost".to_string(),
            host_id: Uuid::new_v4(),
            host_token: "test".to_string(),
            name: "test".to_string(),
            roots: vec![root.to_path_buf()],
        }
    }
}
