use std::collections::{HashMap, VecDeque};
use std::fs;
use std::ops::Range;
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
    AgentTurnExecutor, CodingProvider, HostCapabilities, HostCommand, HostCommandEnvelope,
    HostHeartbeat, LaunchSession, ProviderCapability, REMOTE_PROTOCOL_VERSION, RemoteHost,
    RemoteHostIdentity, SessionEvent, SessionLiveEvent, SessionPayloadRef, SessionStore,
    SessionWriterLease, SqliteSessionStore, WorkspaceAttachment, WorkspaceCommandErrorCode,
    WorkspaceCommandOutcome, WorkspaceCommandRequest, WorkspaceCommandResponse,
    WorkspaceFilesystemErrorCode, WorkspaceFilesystemOutcome, WorkspaceFilesystemRequest,
    WorkspaceFilesystemResponse, execute_workspace_command, execute_workspace_filesystem,
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

// Keep ordinary uploads comfortably below the relay's request-body ceiling.
// The adaptive 413 path below is still authoritative when a deployment has a
// smaller limit or a single event is unusually large.
const TARGET_EVENT_BATCH_BYTES: usize = 384 * 1024;
const MAX_EVENT_BATCH_EVENTS: usize = 256;

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
    Current(Box<PersistedLaunchMetadata>),
    Legacy(Box<LaunchSession>),
}

impl StoredLaunchMetadata {
    fn into_current(self) -> PersistedLaunchMetadata {
        match self {
            Self::Current(metadata) => *metadata,
            Self::Legacy(request) => PersistedLaunchMetadata {
                request: *request,
                attachment: None,
            },
        }
    }
}

async fn upload_event_payloads(
    client: &Client,
    config: &HostConfig,
    store: &dyn SessionStore,
    source_events: &[SessionEvent],
    remote_events: &[SessionEvent],
) -> Result<bool> {
    ensure!(
        source_events.len() == remote_events.len(),
        "remote payload preparation changed the event count"
    );
    let mut payloads = Vec::new();
    for (source_event, remote_event) in source_events.iter().zip(remote_events) {
        let target_session_id = remote_event.session_id;
        let source_refs = scoped_payload_refs(source_event, target_session_id);
        let remote_refs = scoped_payload_refs(remote_event, target_session_id);
        ensure!(
            source_refs.len() == remote_refs.len(),
            "remote payload preparation changed the payload count for event {}",
            source_event.id
        );
        for ((source_session_id, source), (remote_session_id, remote)) in
            source_refs.into_iter().zip(remote_refs)
        {
            ensure!(
                source_session_id == remote_session_id
                    && source.kind == remote.kind
                    && source.byte_len == remote.byte_len,
                "remote payload preparation changed payload metadata for event {}",
                source_event.id
            );
            payloads.push((remote_session_id, source, remote));
        }
    }
    for (session_id, source, remote) in payloads {
        let bytes = store.load_payload(&source).await?;
        let response = client
            .post(endpoint(
                &config.server,
                &format!(
                    "/api/remote/host/sessions/{session_id}/payloads/{}",
                    remote.id
                ),
            ))
            .bearer_auth(&config.host_token)
            .timeout(Duration::from_secs(30))
            .query(&[
                ("kind", remote.kind.as_str().to_string()),
                ("byte_len", remote.byte_len.to_string()),
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
                    %session_id,
                    payload_id = %remote.id,
                    source_payload_id = %source.id,
                    "remote session payload upload failed; retained for replay"
                );
                return Ok(false);
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    %session_id,
                    payload_id = %remote.id,
                    source_payload_id = %source.id,
                    "remote session payload upload failed; retained for replay"
                );
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn compact_event_payloads_for_upload(event: &SessionEvent) -> Result<SessionEvent> {
    let mut compact = event.clone();
    compact_event_payloads_in_place(&mut compact)?;
    Ok(compact)
}

fn compact_event_payloads_in_place(event: &mut SessionEvent) -> Result<()> {
    let event_id = event.id;
    match &mut event.kind {
        crate::SessionEventKind::ToolStarted {
            input, input_ref, ..
        } if input_ref.is_none() => {
            let bytes = serde_json::to_vec(input)?;
            if bytes.len() > crate::session_store::INLINE_SESSION_PAYLOAD_BYTES {
                let payload = derived_payload_ref(
                    event_id,
                    crate::SessionPayloadKind::ToolInput,
                    bytes.len(),
                );
                *input = crate::session_store::deferred_json_payload(&payload);
                *input_ref = Some(payload);
            }
        }
        crate::SessionEventKind::ToolCompleted {
            output,
            output_ref,
            input,
            input_ref,
            ..
        } => {
            if output_ref.is_none()
                && output.len() > crate::session_store::INLINE_SESSION_PAYLOAD_BYTES
            {
                let payload = derived_payload_ref(
                    event_id,
                    crate::SessionPayloadKind::ToolOutput,
                    output.len(),
                );
                *output = crate::session_store::deferred_text_payload(output, &payload);
                *output_ref = Some(payload);
            }
            if input_ref.is_none()
                && let Some(value) = input
            {
                let bytes = serde_json::to_vec(&*value)?;
                if bytes.len() > crate::session_store::INLINE_SESSION_PAYLOAD_BYTES {
                    let payload = derived_payload_ref(
                        event_id,
                        crate::SessionPayloadKind::ToolResultInput,
                        bytes.len(),
                    );
                    *value = crate::session_store::deferred_json_payload(&payload);
                    *input_ref = Some(payload);
                }
            }
        }
        crate::SessionEventKind::SubagentActivity {
            event: Some(child_event),
            ..
        } => compact_event_payloads_in_place(child_event)?,
        _ => {}
    }
    Ok(())
}

fn derived_payload_ref(
    event_id: Uuid,
    kind: crate::SessionPayloadKind,
    byte_len: usize,
) -> SessionPayloadRef {
    SessionPayloadRef {
        id: Uuid::new_v5(&event_id, kind.as_str().as_bytes()),
        kind,
        byte_len: u64::try_from(byte_len).unwrap_or(u64::MAX),
    }
}

fn prepare_event_upload(compact_event: &SessionEvent) -> SessionEvent {
    let mut remote_event = compact_event.clone();
    rekey_event_payloads(&mut remote_event, compact_event.session_id);
    remote_event
}

fn rekey_event_payloads(event: &mut SessionEvent, target_session_id: Uuid) {
    match &mut event.kind {
        crate::SessionEventKind::ToolStarted { input_ref, .. } => {
            rekey_remote_payload(target_session_id, input_ref);
        }
        crate::SessionEventKind::ToolCompleted {
            output_ref,
            input_ref,
            ..
        } => {
            rekey_remote_payload(target_session_id, output_ref);
            rekey_remote_payload(target_session_id, input_ref);
        }
        crate::SessionEventKind::SubagentActivity {
            event: Some(child_event),
            ..
        } => rekey_event_payloads(child_event, target_session_id),
        _ => {}
    }
}

fn rekey_remote_payload(session_id: Uuid, payload: &mut Option<SessionPayloadRef>) {
    if let Some(payload) = payload {
        payload.id = Uuid::new_v5(&session_id, payload.id.as_bytes());
    }
}

fn scoped_payload_refs(
    event: &SessionEvent,
    target_session_id: Uuid,
) -> Vec<(Uuid, SessionPayloadRef)> {
    let mut refs = event
        .kind
        .payload_refs()
        .into_iter()
        .map(|payload| (target_session_id, payload.clone()))
        .collect::<Vec<_>>();
    if let crate::SessionEventKind::SubagentActivity {
        event: Some(child_event),
        ..
    } = &event.kind
    {
        refs.extend(scoped_payload_refs(child_event, target_session_id));
    }
    refs
}

fn event_upload_ranges(events: &[SessionEvent]) -> Result<VecDeque<Range<usize>>> {
    let empty_batch_bytes = serde_json::to_vec(&EventBatch { events: &[] })?.len();
    let mut ranges = VecDeque::new();
    let mut start = 0;
    let mut batch_bytes = empty_batch_bytes;

    for (index, event) in events.iter().enumerate() {
        let separator_bytes = usize::from(index > start);
        let event_bytes = serde_json::to_vec(event)?.len();
        if index > start
            && (index - start >= MAX_EVENT_BATCH_EVENTS
                || batch_bytes
                    .saturating_add(separator_bytes)
                    .saturating_add(event_bytes)
                    > TARGET_EVENT_BATCH_BYTES)
        {
            ranges.push_back(start..index);
            start = index;
            batch_bytes = empty_batch_bytes;
        }
        batch_bytes = batch_bytes
            .saturating_add(usize::from(index > start))
            .saturating_add(event_bytes);
    }
    if start < events.len() {
        ranges.push_back(start..events.len());
    }
    Ok(ranges)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EventUploadOutcome {
    Complete,
    Retryable,
    Blocked {
        session_id: Uuid,
        sequence: u64,
        event_kind: String,
        event_bytes: usize,
    },
}

async fn upload_event_page(
    client: &Client,
    config: &HostConfig,
    store: &dyn SessionStore,
    events: &[SessionEvent],
    uploaded_sequence: &mut u64,
    timeout: Duration,
) -> Result<EventUploadOutcome> {
    let compact_events = events
        .iter()
        .map(compact_event_payloads_for_upload)
        .collect::<Result<Vec<_>>>()?;
    let remote_events = compact_events
        .iter()
        .map(prepare_event_upload)
        .collect::<Vec<_>>();
    let mut ranges = event_upload_ranges(&remote_events)?;
    while let Some(range) = ranges.pop_front() {
        let batch = &remote_events[range.clone()];
        if !upload_event_payloads(client, config, store, &compact_events[range.clone()], batch)
            .await?
        {
            return Ok(EventUploadOutcome::Retryable);
        }
        let response = client
            .post(endpoint(&config.server, "/api/remote/host/events"))
            .bearer_auth(&config.host_token)
            .timeout(timeout)
            .json(&EventBatch { events: batch })
            .send()
            .await;
        match response {
            Ok(response) if response.status().is_success() => {
                if let Some(last) = batch.last() {
                    *uploaded_sequence = last.sequence;
                }
            }
            Ok(response)
                if response.status() == StatusCode::PAYLOAD_TOO_LARGE && batch.len() > 1 =>
            {
                let middle = range.start + range.len() / 2;
                tracing::warn!(
                    session_id = %batch[0].session_id,
                    first_sequence = batch[0].sequence,
                    last_sequence = batch[batch.len() - 1].sequence,
                    events = batch.len(),
                    "remote event batch was too large; splitting in order"
                );
                ranges.push_front(middle..range.end);
                ranges.push_front(range.start..middle);
            }
            Ok(response) if response.status() == StatusCode::PAYLOAD_TOO_LARGE => {
                let event_bytes = serde_json::to_vec(&EventBatch { events: batch })?.len();
                let event_kind = serde_json::to_value(&batch[0].kind)?
                    .get("type")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown")
                    .to_string();
                tracing::error!(
                    session_id = %batch[0].session_id,
                    sequence = batch[0].sequence,
                    %event_kind,
                    event_bytes,
                    "remote relay rejected irreducible session event; durable event was not skipped and sync is blocked"
                );
                return Ok(EventUploadOutcome::Blocked {
                    session_id: batch[0].session_id,
                    sequence: batch[0].sequence,
                    event_kind,
                    event_bytes,
                });
            }
            Ok(response) if response.status() == StatusCode::UNAUTHORIZED => {
                bail!("remote host token was rejected; enroll this host again");
            }
            Ok(response) if response.status() == StatusCode::CONFLICT => {
                bail!(
                    "remote event replay conflicted with the durable journal for session {}: {}",
                    batch[0].session_id,
                    response.text().await.unwrap_or_default()
                );
            }
            Ok(response) => {
                let status = response.status();
                let detail = response
                    .text()
                    .await
                    .unwrap_or_default()
                    .chars()
                    .take(512)
                    .collect::<String>();
                tracing::warn!(
                    %status,
                    %detail,
                    session_id = %batch[0].session_id,
                    first_sequence = batch[0].sequence,
                    last_sequence = batch[batch.len() - 1].sequence,
                    events = batch.len(),
                    "remote event upload failed; journaled for replay"
                );
                return Ok(EventUploadOutcome::Retryable);
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    session_id = %batch[0].session_id,
                    first_sequence = batch[0].sequence,
                    last_sequence = batch[batch.len() - 1].sequence,
                    events = batch.len(),
                    "remote event upload failed; journaled for replay"
                );
                return Ok(EventUploadOutcome::Retryable);
            }
        }
    }
    Ok(EventUploadOutcome::Complete)
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
        CodingProvider::Codex | CodingProvider::Claude | CodingProvider::OpenCode => {
            provider_auth_status(provider).await
        }
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
        CodingProvider::Claude => claude_auth_status_authenticated(output),
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

async fn provider_auth_status(provider: CodingProvider) -> Result<String> {
    match provider {
        CodingProvider::Codex => command_output("codex", &["login", "status"]).await,
        CodingProvider::Claude => command_output("claude", &["auth", "status", "--json"]).await,
        CodingProvider::OpenCode => command_output("opencode", &["providers", "list"]).await,
        _ => bail!("{provider:?} does not use an interactive provider login"),
    }
}

fn claude_auth_status_authenticated(output: &str) -> bool {
    match serde_json::from_str::<serde_json::Value>(output) {
        Ok(value) => value
            .get("loggedIn")
            .or_else(|| value.get("logged_in"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        Err(_) => {
            let normalized = output.to_ascii_lowercase();
            !normalized.trim().is_empty()
                && !normalized.contains("not logged in")
                && !normalized.contains("logged out")
        }
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

/// Whether `provider` already has credentials on this machine — an OAuth
/// session file written by its CLI, or an API key borg holds. Providers whose
/// credentials come from the environment are treated as configured; they fail
/// loudly at call time with their own message.
pub fn provider_credentials_present(provider: CodingProvider) -> bool {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    match provider {
        CodingProvider::Claude => {
            borg_provider::credentials::api_key(
                borg_provider::credentials::ApiKeyCredential::Anthropic,
            )
            .is_some()
                || home.is_some_and(|home| home.join(".claude/.credentials.json").is_file())
        }
        CodingProvider::Codex => home.is_some_and(|home| home.join(".codex/auth.json").is_file()),
        _ => true,
    }
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
    let auth = provider_auth_status(provider).await.with_context(|| {
        format!(
            "{} login completed but auth status failed",
            provider.executable()
        )
    })?;
    anyhow::ensure!(
        match provider {
            CodingProvider::Claude => claude_auth_status_authenticated(&auth),
            CodingProvider::OpenCode => auth.lines().any(|line| line.contains('●')),
            _ => !auth.trim().is_empty(),
        },
        "{} login completed but no authenticated session is available",
        provider.executable()
    );
    Ok(())
}

pub type HostExecutorFactory =
    Arc<dyn Fn(&HostConfig, &LaunchSession) -> Result<Arc<dyn AgentTurnExecutor>> + Send + Sync>;

fn default_host_executor_factory() -> HostExecutorFactory {
    Arc::new(|config, _launch| {
        Ok(Arc::new(crate::LocalAgentTurnExecutor::with_model_gateway(
            borg_provider::provider::ModelGateway {
                endpoint: endpoint(&config.server, "/api/remote/host/kimi/chat/completions"),
                bearer_token: config.host_token.clone(),
            },
        )))
    })
}

pub async fn run_host(config_path: &Path) -> Result<()> {
    run_host_with_executor_factory(config_path, default_host_executor_factory()).await
}

pub async fn run_host_with_executor_factory(
    config_path: &Path,
    executor_factory: HostExecutorFactory,
) -> Result<()> {
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
                Arc::clone(&executor_factory),
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
    let mut event_retry_at = Instant::now();
    let mut shutdown_flush_pending = false;
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

        let mut caught_up = false;
        if Instant::now() >= event_retry_at {
            let pending = store
                .events_after(session_id, uploaded_sequence, 1_024)
                .await?;
            match upload_event_page(
                &client,
                &config,
                store.as_ref(),
                &pending,
                &mut uploaded_sequence,
                Duration::from_secs(5),
            )
            .await?
            {
                EventUploadOutcome::Complete => {
                    if upload_live_state(
                        &client,
                        &config,
                        store.as_ref(),
                        session_id,
                        &mut uploaded_live_revision,
                    )
                    .await?
                    {
                        caught_up = pending.len() < 1_024;
                        event_retry_at = Instant::now();
                    } else {
                        event_retry_at = Instant::now() + Duration::from_secs(2);
                    }
                }
                EventUploadOutcome::Retryable => {
                    event_retry_at = Instant::now() + Duration::from_secs(2);
                }
                EventUploadOutcome::Blocked { .. } => {
                    // Keep heartbeats and remote interrupt/stop commands alive
                    // without rereading the same irreducible event in a loop.
                    event_retry_at = Instant::now() + Duration::from_secs(300);
                }
            }
        }
        // A shutdown can arrive after this iteration read an empty suffix but
        // before the actor commits its terminal status. Only finish after one
        // complete upload pass that began after shutdown was observed.
        if shutdown_flush_pending && caught_up {
            return Ok(());
        }
        if *shutdown.borrow() {
            shutdown_flush_pending = true;
            let retry_delay = event_retry_at.saturating_duration_since(Instant::now());
            if retry_delay.is_zero() {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(retry_delay).await;
            }
            continue;
        }
        if caught_up && wait_for_mirror_shutdown(&mut shutdown, Duration::from_millis(250)).await {
            shutdown_flush_pending = true;
            continue;
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
            shutdown_flush_pending = true;
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
    executor_factory: HostExecutorFactory,
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
        spawn_host_session(
            client,
            config,
            session_root,
            sessions,
            Arc::clone(&executor_factory),
            session_id,
            *request,
        )
        .await;
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
                            Arc::clone(&executor_factory),
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

fn discard_serialized_extension_roots(launch: &mut LaunchSession) {
    if !launch.extension_skill_roots.is_empty() {
        tracing::debug!(
            count = launch.extension_skill_roots.len(),
            "discarding controller-supplied extension roots at host boundary"
        );
        // A remote controller may only request a session. The host-side
        // executor factory discovers its own active/trusted Blu catalog; no
        // serialized path is allowed to become a host filesystem capability.
        launch.extension_skill_roots.clear();
    }
}

async fn spawn_host_session(
    client: Client,
    config: HostConfig,
    session_root: PathBuf,
    sessions: Arc<Mutex<HashMap<Uuid, mpsc::Sender<HostCommand>>>>,
    executor_factory: HostExecutorFactory,
    session_id: Uuid,
    request: LaunchSession,
) -> mpsc::Sender<HostCommand> {
    let (tx, rx) = mpsc::channel(64);
    sessions.lock().await.insert(session_id, tx.clone());
    let sessions_for_cleanup = sessions.clone();
    tokio::spawn(async move {
        if let Err(error) = run_session(
            client,
            config,
            session_root,
            executor_factory,
            session_id,
            request,
            rx,
        )
        .await
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
    executor_factory: HostExecutorFactory,
    session_id: Uuid,
    mut launch: LaunchSession,
    commands: mpsc::Receiver<HostCommand>,
) -> Result<()> {
    launch.cwd = validate_host_cwd(&config.roots, &launch.cwd)?;
    discard_serialized_extension_roots(&mut launch);
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
    let executor = executor_factory(&config, &launch)?;
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
        match upload_event_page(
            client,
            config,
            store,
            &pending,
            &mut sync.uploaded_sequence,
            Duration::from_secs(3),
        )
        .await?
        {
            EventUploadOutcome::Complete => {}
            EventUploadOutcome::Retryable => {
                sync.retry_at = Instant::now() + Duration::from_secs(2);
                return Ok(());
            }
            EventUploadOutcome::Blocked { .. } => {
                // Preserve the actor and command receiver. A later binary or
                // relay limit change can resume from this exact cursor.
                sync.retry_at = Instant::now() + Duration::from_secs(300);
                return Ok(());
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::*;
    use crate::WorkspaceFilesystemOperation;

    #[test]
    fn host_boundary_discards_controller_supplied_extension_roots() {
        let mut launch: LaunchSession = serde_json::from_value(serde_json::json!({
            "request_id": Uuid::new_v4(),
            "cwd": "/workspace",
            "provider": "codex",
            "permission_mode": "manual",
            "extension_skill_roots": ["/workspace/.borg/extensions/untrusted/skills"]
        }))
        .expect("launch payload");

        discard_serialized_extension_roots(&mut launch);

        assert!(launch.extension_skill_roots.is_empty());
    }

    fn error_events(session_id: Uuid, count: usize, message_bytes: usize) -> Vec<SessionEvent> {
        (1..=count)
            .map(|sequence| {
                SessionEvent::new(
                    session_id,
                    sequence as u64,
                    crate::SessionEventKind::Error {
                        message: "x".repeat(message_bytes),
                    },
                )
            })
            .collect()
    }

    async fn read_http_request(stream: &mut TcpStream) -> (String, Vec<u8>) {
        let mut request = Vec::new();
        let header_end = loop {
            let mut buffer = [0_u8; 4_096];
            let read = stream.read(&mut buffer).await.unwrap();
            assert!(read > 0);
            request.extend_from_slice(&buffer[..read]);
            if let Some(offset) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                break offset + 4;
            }
        };
        let headers = std::str::from_utf8(&request[..header_end]).unwrap();
        let path = headers
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .to_string();
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.split_once(':').and_then(|(name, value)| {
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
            })
            .unwrap();
        while request.len() - header_end < content_length {
            let mut buffer = [0_u8; 4_096];
            let read = stream.read(&mut buffer).await.unwrap();
            assert!(read > 0);
            request.extend_from_slice(&buffer[..read]);
        }
        (
            path,
            request[header_end..header_end + content_length].to_vec(),
        )
    }

    async fn event_server(
        statuses: Vec<&'static str>,
    ) -> (String, tokio::task::JoinHandle<Vec<Vec<u64>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut delivered = Vec::new();
            for status in statuses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (_, request) = read_http_request(&mut stream).await;
                let body: serde_json::Value = serde_json::from_slice(&request).unwrap();
                delivered.push(
                    body["events"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|event| event["sequence"].as_u64().unwrap())
                        .collect::<Vec<_>>(),
                );
                stream
                    .write_all(
                        format!(
                            "HTTP/1.1 {status}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
            delivered
        });
        (format!("http://{address}"), server)
    }

    #[tokio::test]
    async fn mirror_shutdown_flushes_a_terminal_event_committed_during_the_caught_up_wait() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (first_upload_tx, first_upload_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut first_upload_tx = Some(first_upload_tx);
            let mut uploads = Vec::new();
            while uploads.len() < 2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (path, body) = read_http_request(&mut stream).await;
                let response = match path.as_str() {
                    "/api/remote/host/sessions" => {
                        let body = r#"{"command_cursor":0,"event_cursor":0,"live_revision":0}"#;
                        format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                            body.len()
                        )
                    }
                    "/api/remote/host/heartbeat" => {
                        "HTTP/1.1 204 No Content\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                            .to_string()
                    }
                    "/api/remote/host/events" => {
                        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
                        uploads.push(
                            body["events"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .map(|event| event["sequence"].as_u64().unwrap())
                                .collect::<Vec<_>>(),
                        );
                        if uploads.len() == 1
                            && let Some(first_upload_tx) = first_upload_tx.take()
                        {
                            first_upload_tx.send(()).unwrap();
                        }
                        "HTTP/1.1 204 No Content\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                            .to_string()
                    }
                    unexpected => panic!("unexpected mirror request: {unexpected}"),
                };
                stream.write_all(response.as_bytes()).await.unwrap();
            }
            uploads
        });

        let root = tempdir().unwrap();
        let store = Arc::new(
            crate::SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
                .await
                .unwrap(),
        );
        let session_id = Uuid::new_v4();
        store.create_session(session_id).await.unwrap();
        store
            .append(SessionEvent::new(
                session_id,
                0,
                crate::SessionEventKind::SessionStarted,
            ))
            .await
            .unwrap();
        let config_path = root.path().join("host.json");
        write_config(
            &config_path,
            &HostConfig {
                server: format!("http://{address}"),
                ..test_config(root.path())
            },
        )
        .unwrap();
        let launch = LaunchSession {
            request_id: session_id,
            cwd: root.path().to_path_buf(),
            provider: CodingProvider::Codex,
            model: None,
            effort: None,
            fast: Some(false),
            response_language: crate::ResponseLanguage::Auto,
            permission_mode: crate::PermissionMode::FullAccess,
            name: None,
            initial_prompt: None,
            capabilities: Default::default(),
            subagent_concurrency_limit: None,
            extension_skill_roots: Vec::new(),
            team_policy: None,
        };
        let (command_tx, _command_rx) = mpsc::channel(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mirror_store: Arc<dyn SessionStore> = store.clone();
        let mirror = tokio::spawn(async move {
            mirror_local_session(
                &config_path,
                mirror_store,
                session_id,
                launch,
                command_tx,
                shutdown_rx,
            )
            .await
        });

        first_upload_rx.await.unwrap();
        store
            .append(SessionEvent::new(
                session_id,
                0,
                crate::SessionEventKind::StatusChanged {
                    status: crate::SessionStatus::Stopped,
                    detail: None,
                },
            ))
            .await
            .unwrap();
        shutdown_tx.send(true).unwrap();

        tokio::time::timeout(Duration::from_secs(3), mirror)
            .await
            .expect("mirror should finish after its shutdown flush")
            .unwrap()
            .unwrap();
        assert_eq!(server.await.unwrap(), vec![vec![1], vec![2]]);
    }

    #[test]
    fn event_upload_ranges_bound_the_exact_serialized_request_size() {
        let events = error_events(Uuid::new_v4(), 256, 2_500);
        let ranges = event_upload_ranges(&events).unwrap();

        assert!(
            ranges.len() > 1,
            "the regression page must exceed one batch"
        );
        assert_eq!(ranges.front().unwrap().start, 0);
        assert_eq!(ranges.back().unwrap().end, events.len());
        for (previous, next) in ranges.iter().zip(ranges.iter().skip(1)) {
            assert_eq!(previous.end, next.start);
        }
        for range in ranges {
            let bytes = serde_json::to_vec(&EventBatch {
                events: &events[range.clone()],
            })
            .unwrap()
            .len();
            assert!(
                bytes <= TARGET_EVENT_BATCH_BYTES || range.len() == 1,
                "multi-event request was {bytes} bytes"
            );
        }
    }

    #[test]
    fn event_upload_ranges_preserve_the_relay_event_count_limit() {
        let events = error_events(Uuid::new_v4(), 600, 1);
        let ranges = event_upload_ranges(&events).unwrap();

        assert_eq!(ranges.len(), 3);
        assert!(
            ranges
                .iter()
                .all(|range| range.len() <= MAX_EVENT_BATCH_EVENTS)
        );
        assert_eq!(ranges.front().unwrap().start, 0);
        assert_eq!(ranges.back().unwrap().end, 600);
    }

    #[test]
    fn remote_payload_ids_are_stable_and_session_scoped() {
        let source_payload_id = Uuid::new_v4();
        let source = SessionEvent::new(
            Uuid::new_v4(),
            1,
            crate::SessionEventKind::ToolCompleted {
                tool_call_id: "tool".to_string(),
                output: String::new(),
                output_ref: Some(SessionPayloadRef {
                    id: source_payload_id,
                    kind: crate::SessionPayloadKind::ToolOutput,
                    byte_len: 42,
                }),
                is_error: false,
                input: None,
                input_ref: None,
            },
        );

        let first = prepare_event_upload(&source);
        let repeated = prepare_event_upload(&source);
        let mut other_session = source.clone();
        other_session.session_id = Uuid::new_v4();
        let other = prepare_event_upload(&other_session);
        let first_id = first.kind.payload_refs()[0].id;

        assert_ne!(first_id, source_payload_id);
        assert_eq!(first_id, repeated.kind.payload_refs()[0].id);
        assert_ne!(first_id, other.kind.payload_refs()[0].id);
        assert_eq!(source.kind.payload_refs()[0].id, source_payload_id);
    }

    #[test]
    fn nested_child_tool_payloads_are_compacted_into_the_enclosing_remote_session() {
        let child_session_id = Uuid::new_v4();
        let child_event = SessionEvent::new(
            child_session_id,
            7,
            crate::SessionEventKind::ToolCompleted {
                tool_call_id: "large-child-tool".to_string(),
                output: "x".repeat(crate::session_store::INLINE_SESSION_PAYLOAD_BYTES + 1),
                output_ref: None,
                is_error: false,
                input: None,
                input_ref: None,
            },
        );
        let child_event_id = child_event.id;
        let now = Utc::now();
        let root_session_id = Uuid::new_v4();
        let root_event = SessionEvent::new(
            root_session_id,
            1,
            crate::SessionEventKind::SubagentActivity {
                activity: crate::SubagentActivityKind::Updated,
                agent: crate::SubagentSnapshot {
                    session_id: child_session_id,
                    parent_session_id: Uuid::new_v4(),
                    task_name: "/root/child".to_string(),
                    status: crate::SubagentStatus::Running,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    cwd: PathBuf::from("/tmp"),
                    created_at: now,
                    updated_at: now,
                    detail: None,
                    final_text: None,
                    usage: crate::SubagentUsage::default(),
                },
                event: Some(Box::new(child_event)),
            },
        );

        let compact = compact_event_payloads_for_upload(&root_event).unwrap();
        let remote = prepare_event_upload(&compact);
        let source_refs = scoped_payload_refs(&compact, root_session_id);
        let remote_refs = scoped_payload_refs(&remote, root_session_id);

        assert_eq!(source_refs.len(), 1);
        assert_eq!(source_refs[0].0, root_session_id);
        assert_eq!(
            source_refs[0].1.id,
            Uuid::new_v5(
                &child_event_id,
                crate::SessionPayloadKind::ToolOutput.as_str().as_bytes()
            )
        );
        assert_eq!(remote_refs[0].0, root_session_id);
        assert_eq!(
            remote_refs[0].1.id,
            Uuid::new_v5(&root_session_id, source_refs[0].1.id.as_bytes())
        );
        assert!(serde_json::to_vec(&compact).unwrap().len() < TARGET_EVENT_BATCH_BYTES);
    }

    #[tokio::test]
    async fn nested_child_payload_upload_uses_the_enclosing_session_and_original_bytes() {
        let root = tempdir().unwrap();
        let store = SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
            .await
            .unwrap();
        let root_session_id = Uuid::new_v4();
        let child_session_id = Uuid::new_v4();
        store.create_session(root_session_id).await.unwrap();
        store.create_session(child_session_id).await.unwrap();
        let large_output = "z".repeat(crate::session_store::INLINE_SESSION_PAYLOAD_BYTES + 1);
        let mut child_event = SessionEvent::new(
            child_session_id,
            0,
            crate::SessionEventKind::ToolCompleted {
                tool_call_id: "nested".to_string(),
                output: large_output.clone(),
                output_ref: None,
                is_error: false,
                input: None,
                input_ref: None,
            },
        );
        let stored_child = store.append(child_event.clone()).await.unwrap();
        child_event.sequence = stored_child.sequence;
        let compact_child = store.events_after(child_session_id, 0, 1).await.unwrap();
        let source_payload_id = compact_child[0].kind.payload_refs()[0].id;
        let remote_payload_id = Uuid::new_v5(&root_session_id, source_payload_id.as_bytes());
        let now = Utc::now();
        let root_event = SessionEvent::new(
            root_session_id,
            1,
            crate::SessionEventKind::SubagentActivity {
                activity: crate::SubagentActivityKind::Updated,
                agent: crate::SubagentSnapshot {
                    session_id: child_session_id,
                    parent_session_id: root_session_id,
                    task_name: "/root/child".to_string(),
                    status: crate::SubagentStatus::Running,
                    provider: CodingProvider::Codex,
                    model: None,
                    effort: None,
                    cwd: root.path().to_path_buf(),
                    created_at: now,
                    updated_at: now,
                    detail: None,
                    final_text: None,
                    usage: crate::SubagentUsage::default(),
                },
                event: Some(Box::new(child_event)),
            },
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut payload_stream, _) = listener.accept().await.unwrap();
            let (payload_path, payload_body) = read_http_request(&mut payload_stream).await;
            payload_stream
                .write_all(
                    b"HTTP/1.1 204 No Content\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            let (mut event_stream, _) = listener.accept().await.unwrap();
            let (_, event_body) = read_http_request(&mut event_stream).await;
            event_stream
                .write_all(
                    b"HTTP/1.1 204 No Content\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            (payload_path, payload_body, event_body)
        });
        let config = HostConfig {
            server: format!("http://{address}"),
            ..test_config(root.path())
        };
        let mut uploaded_sequence = 0;

        assert_eq!(
            upload_event_page(
                &Client::new(),
                &config,
                &store,
                &[root_event.clone()],
                &mut uploaded_sequence,
                Duration::from_secs(3),
            )
            .await
            .unwrap(),
            EventUploadOutcome::Complete
        );
        let (payload_path, payload_body, event_body) = server.await.unwrap();
        assert!(payload_path.starts_with(&format!(
            "/api/remote/host/sessions/{root_session_id}/payloads/{remote_payload_id}"
        )));
        assert_eq!(payload_body, large_output.as_bytes());
        let uploaded: serde_json::Value = serde_json::from_slice(&event_body).unwrap();
        assert_eq!(
            uploaded["events"][0]["kind"]["event"]["kind"]["output_ref"]["id"],
            remote_payload_id.to_string()
        );
        assert!(event_body.len() < TARGET_EVENT_BATCH_BYTES);
        assert_eq!(uploaded_sequence, 1);
        assert_eq!(root_event.kind.payload_refs().len(), 0);
    }

    #[tokio::test]
    async fn payload_too_large_is_split_without_skipping_or_reordering_events() {
        let (server_url, server) = event_server(vec![
            "413 Payload Too Large",
            "204 No Content",
            "204 No Content",
        ])
        .await;

        let root = tempdir().unwrap();
        let store = SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
            .await
            .unwrap();
        let session_id = Uuid::new_v4();
        let events = error_events(session_id, 4, 16);
        let config = HostConfig {
            server: server_url,
            ..test_config(root.path())
        };
        let mut uploaded_sequence = 0;

        assert_eq!(
            upload_event_page(
                &Client::new(),
                &config,
                &store,
                &events,
                &mut uploaded_sequence,
                Duration::from_secs(3),
            )
            .await
            .unwrap(),
            EventUploadOutcome::Complete
        );
        assert_eq!(uploaded_sequence, 4);
        assert_eq!(
            server.await.unwrap(),
            vec![vec![1, 2, 3, 4], vec![1, 2], vec![3, 4]]
        );
    }

    #[tokio::test]
    async fn split_retry_preserves_the_last_contiguous_cursor() {
        let (server_url, server) = event_server(vec![
            "413 Payload Too Large",
            "204 No Content",
            "500 Internal Server Error",
        ])
        .await;
        let root = tempdir().unwrap();
        let store = SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
            .await
            .unwrap();
        let events = error_events(Uuid::new_v4(), 4, 16);
        let config = HostConfig {
            server: server_url,
            ..test_config(root.path())
        };
        let mut uploaded_sequence = 0;

        assert_eq!(
            upload_event_page(
                &Client::new(),
                &config,
                &store,
                &events,
                &mut uploaded_sequence,
                Duration::from_secs(3),
            )
            .await
            .unwrap(),
            EventUploadOutcome::Retryable
        );
        assert_eq!(uploaded_sequence, 2);
        assert_eq!(
            server.await.unwrap(),
            vec![vec![1, 2, 3, 4], vec![1, 2], vec![3, 4]]
        );
    }

    #[tokio::test]
    async fn singleton_payload_too_large_is_irreducible_and_never_skipped() {
        let (server_url, server) = event_server(vec!["413 Payload Too Large"]).await;
        let root = tempdir().unwrap();
        let store = SqliteSessionStore::open(root.path().join("sessions.sqlite3"))
            .await
            .unwrap();
        let events = error_events(Uuid::new_v4(), 1, 16);
        let config = HostConfig {
            server: server_url,
            ..test_config(root.path())
        };
        let mut uploaded_sequence = 0;

        let outcome = upload_event_page(
            &Client::new(),
            &config,
            &store,
            &events,
            &mut uploaded_sequence,
            Duration::from_secs(3),
        )
        .await
        .unwrap();
        assert!(matches!(
            outcome,
            EventUploadOutcome::Blocked {
                sequence: 1,
                event_kind,
                ..
            } if event_kind == "error"
        ));
        assert_eq!(uploaded_sequence, 0);
        assert_eq!(server.await.unwrap(), vec![vec![1]]);
    }

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
                default_host_executor_factory(),
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

    #[test]
    fn claude_auth_status_requires_an_authenticated_json_state() {
        assert!(claude_auth_status_authenticated(
            r#"{"loggedIn":true,"authMethod":"claude.ai"}"#
        ));
        assert!(!claude_auth_status_authenticated(
            r#"{"loggedIn":false,"authMethod":null}"#
        ));
        assert!(!claude_auth_status_authenticated("{}"));
        assert!(!claude_auth_status_authenticated("Not logged in"));
    }

    #[test]
    fn claude_auth_status_accepts_legacy_positive_text_only() {
        assert!(claude_auth_status_authenticated(
            "Logged in with a Claude account"
        ));
        assert!(!claude_auth_status_authenticated(""));
        assert!(!claude_auth_status_authenticated("Logged out"));
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
