use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use borg_remote::{
    CodingProvider, EventActor, MessageStatus, PermissionMode, PromptDelivery, ResponseLanguage,
    SessionConfiguration, SessionEvent, SessionEventKind, SessionStore, SessionSummary,
    SqliteSessionStore, WorkspaceSnapshot, default_host_config_path,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::fs;
use uuid::Uuid;

use crate::cli::SessionCommand;

const SESSION_EXPORT_VERSION: u32 = 1;
const MAX_SESSION_EXPORT_BYTES: usize = 64 * 1024 * 1024;
const MAX_SESSION_EXPORT_MESSAGES: usize = 100_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionExport {
    version: u32,
    session_id: Uuid,
    configuration: Option<SessionConfiguration>,
    messages: Vec<ExportMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExportMessage {
    message_id: Uuid,
    actor: EventActor,
    text: String,
    #[serde(default)]
    attachments: Vec<PathBuf>,
    status: MessageStatus,
    delivery: Option<PromptDelivery>,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct TreeEntry {
    session_id: Uuid,
    parent_session_id: Option<Uuid>,
    parent_cut_sequence: Option<u64>,
    inherited_event_count: u64,
    latest_sequence: u64,
    status: Option<borg_remote::SessionStatus>,
    cwd: Option<PathBuf>,
    first_prompt: Option<String>,
    latest_response: Option<String>,
}

pub(crate) async fn run(command: SessionCommand) -> Result<()> {
    match command {
        SessionCommand::Snapshot { cwd, output } => capture_snapshot(cwd, output).await,
        SessionCommand::Restore {
            input,
            cwd,
            prune,
            json,
        } => restore_snapshot(input, cwd, prune, json).await,
        SessionCommand::Import { input, cwd, json } => import_session(input, cwd, json).await,
        command => {
            let store = open_store().await?;
            match command {
                SessionCommand::Tree { session, json } => show_tree(&store, session, json).await,
                SessionCommand::Fork {
                    session,
                    before,
                    json,
                } => fork_session(&store, session, before, json).await,
                SessionCommand::Undo {
                    session,
                    before,
                    json,
                } => undo_session(&store, session, before, json).await,
                SessionCommand::Redo { session, json } => redo_session(&store, session, json).await,
                SessionCommand::Export {
                    session,
                    output,
                    json,
                } => export_session(&store, session, output, json).await,
                SessionCommand::Import { .. }
                | SessionCommand::Snapshot { .. }
                | SessionCommand::Restore { .. } => unreachable!("handled above"),
            }
        }
    }
}

async fn open_store() -> Result<SqliteSessionStore> {
    let path = default_host_config_path()
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("sessions/sessions.sqlite3");
    Ok(SqliteSessionStore::open(path).await?)
}

async fn resolve_session(store: &SqliteSessionStore, requested: Option<Uuid>) -> Result<Uuid> {
    if let Some(session_id) = requested {
        store
            .state(session_id)
            .await
            .with_context(|| format!("local session {session_id} does not exist"))?;
        return Ok(session_id);
    }
    store
        .list_sessions(10_000)
        .await?
        .into_iter()
        .find(|summary| summary.state.has_resumable_activity())
        .map(|summary| summary.session_id)
        .context("there are no resumable local Borg sessions")
}

async fn show_tree(store: &SqliteSessionStore, requested: Option<Uuid>, json: bool) -> Result<()> {
    let summaries = store.list_sessions(10_000).await?;
    let entries = summaries.iter().map(tree_entry).collect::<Vec<_>>();
    if json {
        if let Some(session_id) = requested {
            let mut ancestry = Vec::new();
            let mut current = Some(session_id);
            while let Some(id) = current {
                let entry = entries
                    .iter()
                    .find(|entry| entry.session_id == id)
                    .with_context(|| format!("session {id} does not exist"))?;
                current = entry.parent_session_id;
                ancestry.push(entry);
            }
            ancestry.reverse();
            println!("{}", serde_json::to_string_pretty(&ancestry)?);
        } else {
            println!("{}", serde_json::to_string_pretty(&entries)?);
        }
        return Ok(());
    }
    let selected = requested.map(|session_id| {
        let mut chain = BTreeMap::new();
        let mut current = Some(session_id);
        while let Some(id) = current {
            if let Some(entry) = entries.iter().find(|entry| entry.session_id == id) {
                current = entry.parent_session_id;
                chain.insert(id, entry);
            } else {
                current = None;
            }
        }
        chain
    });
    for entry in &entries {
        if selected
            .as_ref()
            .is_some_and(|chain| !chain.contains_key(&entry.session_id))
        {
            continue;
        }
        let depth = tree_depth(entry, &entries);
        println!(
            "{}{} {} · {}",
            "  ".repeat(depth),
            if entry.parent_session_id.is_some() {
                "└"
            } else {
                "•"
            },
            entry.session_id,
            entry
                .cwd
                .as_deref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "unconfigured".to_string())
        );
    }
    Ok(())
}

fn tree_depth(entry: &TreeEntry, entries: &[TreeEntry]) -> usize {
    let mut depth = 0;
    let mut parent = entry.parent_session_id;
    while let Some(parent_id) = parent {
        depth += 1;
        parent = entries
            .iter()
            .find(|candidate| candidate.session_id == parent_id)
            .and_then(|candidate| candidate.parent_session_id);
    }
    depth
}

fn tree_entry(summary: &SessionSummary) -> TreeEntry {
    TreeEntry {
        session_id: summary.session_id,
        parent_session_id: summary.parent_session_id,
        parent_cut_sequence: summary.parent_cut_sequence,
        inherited_event_count: summary.inherited_event_count,
        latest_sequence: summary.state.latest_sequence,
        status: summary.state.status,
        cwd: summary
            .state
            .configuration
            .as_ref()
            .map(|configuration| configuration.cwd.clone()),
        first_prompt: summary.state.first_prompt.clone(),
        latest_response: summary.state.latest_response.clone(),
    }
}

async fn fork_session(
    store: &SqliteSessionStore,
    parent: Uuid,
    before: u64,
    json: bool,
) -> Result<()> {
    let child = Uuid::new_v4();
    let fork = store.fork_before(parent, child, before).await?;
    print_fork(fork, json)
}

async fn undo_session(
    store: &SqliteSessionStore,
    session: Uuid,
    before: Option<u64>,
    json: bool,
) -> Result<()> {
    let events = store.read(session).await?;
    let before = match before {
        Some(sequence) => sequence,
        None => events
            .iter()
            .rev()
            .find_map(|event| match &event.kind {
                SessionEventKind::Message {
                    actor: EventActor::User,
                    status: MessageStatus::Complete | MessageStatus::Failed,
                    ..
                } => Some(event.sequence),
                _ => None,
            })
            .context("session has no completed user prompt to undo")?,
    };
    ensure!(before > 0, "undo sequence must be positive");
    let child = Uuid::new_v4();
    let fork = store.fork_before(session, child, before).await?;
    print_fork(fork, json)
}

async fn redo_session(store: &SqliteSessionStore, session: Uuid, json: bool) -> Result<()> {
    let summaries = store.list_sessions(10_000).await?;
    let current = summaries
        .iter()
        .find(|summary| summary.session_id == session)
        .with_context(|| format!("session {session} does not exist"))?;
    let parent = current
        .parent_session_id
        .context("redo requires a forked session with a parent")?;
    let parent_summary = summaries
        .iter()
        .find(|summary| summary.session_id == parent)
        .with_context(|| format!("parent session {parent} is unavailable"))?;
    let child = Uuid::new_v4();
    let fork = store
        .fork_before(
            parent,
            child,
            parent_summary.state.latest_sequence.saturating_add(1),
        )
        .await?;
    print_fork(fork, json)
}

fn print_fork(fork: borg_remote::SessionStoreFork, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(&fork_json(&fork))?);
    } else {
        println!(
            "Created session {} from {} before sequence {}.",
            fork.session_id,
            fork.parent_session_id,
            fork.parent_cut_sequence + 1
        );
    }
    Ok(())
}

fn fork_json(fork: &borg_remote::SessionStoreFork) -> serde_json::Value {
    serde_json::json!({
        "session_id": fork.session_id,
        "parent_session_id": fork.parent_session_id,
        "parent_cut_sequence": fork.parent_cut_sequence,
        "inherited_event_count": fork.inherited_event_count,
    })
}

async fn export_session(
    store: &SqliteSessionStore,
    requested: Option<Uuid>,
    output: Option<PathBuf>,
    json: bool,
) -> Result<()> {
    let session_id = resolve_session(store, requested).await?;
    let events = store.read(session_id).await?;
    let state = store.state(session_id).await?;
    let messages = events
        .into_iter()
        .filter_map(|event| match event.kind {
            SessionEventKind::Message {
                message_id,
                actor: actor @ (EventActor::User | EventActor::Assistant),
                text,
                attachments,
                status: status @ (MessageStatus::Complete | MessageStatus::Failed),
                delivery,
            } => Some(ExportMessage {
                message_id,
                actor,
                text,
                attachments,
                status,
                delivery,
                created_at: event.created_at,
            }),
            _ => None,
        })
        .collect::<Vec<_>>();
    ensure!(
        messages.len() <= MAX_SESSION_EXPORT_MESSAGES,
        "session export contains too many messages"
    );
    let archive = SessionExport {
        version: SESSION_EXPORT_VERSION,
        session_id,
        configuration: state.configuration,
        messages,
    };
    let serialized = serde_json::to_string_pretty(&archive)?;
    ensure!(
        serialized.len() <= MAX_SESSION_EXPORT_BYTES,
        "session export is too large"
    );
    if let Some(output) = output {
        if let Some(parent) = output.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(&output, serialized).await?;
        if json {
            println!(
                "{}",
                serde_json::json!({ "session_id": session_id, "output": output })
            );
        } else {
            println!(
                "Exported conversation {session_id} to {}.",
                output.display()
            );
        }
    } else {
        println!("{serialized}");
    }
    Ok(())
}

async fn import_session(input: PathBuf, cwd: Option<PathBuf>, json: bool) -> Result<()> {
    let bytes = fs::read(&input).await?;
    ensure!(
        bytes.len() <= MAX_SESSION_EXPORT_BYTES,
        "session import is too large"
    );
    let archive: SessionExport = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse session archive {}", input.display()))?;
    ensure!(
        archive.version == SESSION_EXPORT_VERSION,
        "unsupported session archive version {}",
        archive.version
    );
    ensure!(
        archive.messages.len() <= MAX_SESSION_EXPORT_MESSAGES,
        "session import contains too many messages"
    );
    let store = open_store().await?;
    let session_id = Uuid::new_v4();
    store.create_session(session_id).await?;
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::SessionStarted,
        ))
        .await?;
    let configuration = archive
        .configuration
        .unwrap_or_else(|| SessionConfiguration {
            cwd: cwd
                .clone()
                .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))),
            provider: CodingProvider::Codex,
            model: None,
            effort: None,
            fast: false,
            response_language: ResponseLanguage::Auto,
            permission_mode: PermissionMode::FullAccess,
        });
    let configuration = if let Some(cwd) = cwd {
        SessionConfiguration {
            cwd,
            ..configuration
        }
    } else {
        configuration
    };
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::SessionConfigured {
                cwd: configuration.cwd,
                provider: configuration.provider,
                model: configuration.model,
                effort: configuration.effort,
                fast: configuration.fast,
                response_language: configuration.response_language,
                permission_mode: configuration.permission_mode,
            },
        ))
        .await?;
    for message in archive.messages {
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::Message {
                    message_id: message.message_id,
                    actor: message.actor,
                    text: message.text,
                    attachments: message.attachments,
                    status: message.status,
                    delivery: message.delivery,
                },
            ))
            .await?;
    }
    if json {
        println!("{}", serde_json::json!({ "session_id": session_id }));
    } else {
        println!("Imported conversation as session {session_id}.");
    }
    Ok(())
}

async fn capture_snapshot(cwd: Option<PathBuf>, output: PathBuf) -> Result<()> {
    let root = cwd.unwrap_or(std::env::current_dir()?);
    let snapshot = tokio::task::spawn_blocking(move || WorkspaceSnapshot::capture(root)).await??;
    let serialized = serde_json::to_vec_pretty(&snapshot)?;
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).await?;
    }
    fs::write(&output, serialized).await?;
    println!(
        "Captured {} files to {}.",
        snapshot.files.len(),
        output.display()
    );
    Ok(())
}

async fn restore_snapshot(
    input: PathBuf,
    cwd: Option<PathBuf>,
    prune: bool,
    json: bool,
) -> Result<()> {
    let bytes = fs::read(&input).await?;
    ensure!(
        bytes.len() <= MAX_SESSION_EXPORT_BYTES,
        "workspace snapshot is too large"
    );
    let snapshot: WorkspaceSnapshot = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse workspace snapshot {}", input.display()))?;
    let root = cwd.unwrap_or(std::env::current_dir()?);
    let report = tokio::task::spawn_blocking(move || snapshot.restore(root, prune)).await??;
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!(
            "Restored {} files{}.",
            report.restored_files,
            if report.removed_files == 0 {
                String::new()
            } else {
                format!(" and removed {} extra files", report.removed_files)
            }
        );
    }
    Ok(())
}
