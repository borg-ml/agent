mod formats;

use anyhow::{Context, Result, bail, ensure};
use borg_remote::{
    CodingProvider, EventActor, ImportedMemory, MessageStatus, PermissionMode, ResponseLanguage,
    SessionEvent, SessionEventKind, SessionStatus, SqliteSessionStore,
};
use chrono::{DateTime, Utc};
use clap::{Args, ValueEnum};
use serde::{Deserialize, Serialize};
use std::{
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum Source {
    #[value(alias = "codex-cli", alias = "codex-desktop")]
    Codex,
    ClaudeCode,
    ClaudeDesktop,
    Portable,
}
impl Source {
    pub fn key(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::ClaudeCode => "claude-code",
            Self::ClaudeDesktop => "claude-desktop",
            Self::Portable => "portable",
        }
    }
    fn default_path(self) -> Option<PathBuf> {
        match self {
            Self::Codex => std::env::var_os("CODEX_HOME")
                .map(PathBuf::from)
                .or_else(|| dirs::home_dir().map(|p| p.join(".codex"))),
            Self::ClaudeCode => std::env::var_os("CLAUDE_CONFIG_DIR")
                .map(PathBuf::from)
                .or_else(|| dirs::home_dir().map(|p| p.join(".claude"))),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Args)]
pub(crate) struct ImportArgs {
    #[arg(value_enum)]
    pub source: Option<Source>,
    /// Source directory, exported JSON, or ZIP archive.
    #[arg(long)]
    pub path: Option<PathBuf>,
    /// Do not copy threads.
    #[arg(long)]
    pub no_threads: bool,
    /// Do not copy memory or project instructions.
    #[arg(long)]
    pub no_memory: bool,
    /// Inspect the source without importing.
    #[arg(long)]
    pub preview: bool,
    /// Import the selected categories without the interactive preview.
    #[arg(long, short = 'y')]
    pub yes: bool,
    /// Emit preview or result as JSON; use --yes to import.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct Message {
    pub role: String,
    pub text: String,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub attachments: Vec<Attachment>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct Attachment {
    pub name: String,
    #[serde(default)]
    pub path: Option<PathBuf>,
    #[serde(default)]
    pub data_base64: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct Thread {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub cwd: Option<PathBuf>,
    pub messages: Vec<Message>,
}
#[derive(Debug)]
pub(crate) struct PreparedImport {
    source: Source,
    threads: Vec<Thread>,
    memory: Vec<ImportedMemory>,
    pub warnings: Vec<String>,
}
impl PreparedImport {
    pub fn counts(&self) -> (usize, usize) {
        (self.threads.len(), self.memory.len())
    }
}
pub(crate) enum ImportTaskResult {
    Prepared(PreparedImport, bool, bool),
    Complete(ImportReport),
}

#[derive(Debug, Default, Serialize)]
pub(crate) struct ImportReport {
    threads_copied: usize,
    memory_copied: usize,
    duplicates_skipped: usize,
    session_ids: Vec<Uuid>,
    warnings: Vec<String>,
}
impl ImportReport {
    pub fn summary(&self) -> String {
        format!(
            "Imported {} threads and {} memory entries · {} duplicates skipped · {} warnings. Use /resume to open imported threads.{}",
            self.threads_copied,
            self.memory_copied,
            self.duplicates_skipped,
            self.warnings.len(),
            if self.warnings.is_empty() {
                String::new()
            } else {
                format!("\n{}", self.warnings.join("\n"))
            }
        )
    }
}

pub(crate) fn parse_args(text: &str) -> Result<ImportArgs> {
    use clap::Parser;
    #[derive(Parser)]
    struct ImportCommand {
        #[command(flatten)]
        args: ImportArgs,
    }
    let words = shlex::split(text).context("unclosed quote in import options")?;
    Ok(ImportCommand::try_parse_from(std::iter::once("import".to_string()).chain(words))?.args)
}

pub(crate) async fn prepare(args: &ImportArgs) -> Result<PreparedImport> {
    ensure!(
        !args.no_threads || !args.no_memory,
        "select Threads, Memory, or both"
    );
    let source = args
        .source
        .context("choose codex, claude-code, claude-desktop, or portable")?;
    let path = args.path.clone().or_else(|| source.default_path()).context(
        "this source needs --path to its export. For Claude Desktop, export data from Settings > Privacy, then choose the downloaded ZIP")?;
    let threads = !args.no_threads;
    let memory = !args.no_memory;
    tokio::task::spawn_blocking(move || formats::read(source, &path, threads, memory)).await?
}

pub(crate) async fn execute(
    plan: PreparedImport,
    threads: bool,
    memory: bool,
) -> Result<ImportReport> {
    let database = borg_remote::default_host_config_path()
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("sessions/sessions.sqlite3");
    let store = SqliteSessionStore::open(database).await?;
    execute_into(
        plan,
        threads,
        memory,
        &store,
        &borg_remote::imported_memory_directory(),
    )
    .await
}

async fn execute_into(
    plan: PreparedImport,
    threads: bool,
    memory: bool,
    store: &SqliteSessionStore,
    memory_dir: &Path,
) -> Result<ImportReport> {
    ensure!(threads || memory, "select Threads, Memory, or both");
    let mut report = ImportReport {
        warnings: plan.warnings,
        ..Default::default()
    };
    if threads {
        for thread in plan.threads {
            let id = Uuid::new_v5(
                &Uuid::NAMESPACE_URL,
                format!("borg-import:{}:thread:{}", plan.source.key(), thread.id).as_bytes(),
            );
            match copy_thread(
                store,
                memory_dir,
                plan.source,
                id,
                thread,
                &mut report.warnings,
            )
            .await
            {
                Ok(true) => {
                    report.threads_copied += 1;
                    report.session_ids.push(id);
                }
                Ok(false) => report.duplicates_skipped += 1,
                Err(error) => report.warnings.push(format!("Thread {id}: {error:#}")),
            }
        }
    }
    if memory {
        for entry in plan.memory {
            match borg_remote::copy_imported_memory(memory_dir, &entry).await {
                Ok(true) => report.memory_copied += 1,
                Ok(false) => report.duplicates_skipped += 1,
                Err(error) => report
                    .warnings
                    .push(format!("Memory {}: {error:#}", entry.title)),
            }
        }
    }
    Ok(report)
}

async fn copy_thread(
    store: &SqliteSessionStore,
    memory_dir: &Path,
    source: Source,
    id: Uuid,
    thread: Thread,
    warnings: &mut Vec<String>,
) -> Result<bool> {
    use base64::Engine;
    use borg_remote::SessionStore;
    if store.state(id).await.is_ok() {
        return Ok(false);
    }
    ensure!(
        !thread.messages.is_empty(),
        "thread contains no supported messages"
    );
    let start = thread.messages[0].created_at;
    let end = thread.messages.last().unwrap().created_at;
    let cwd = thread
        .cwd
        .clone()
        .filter(|p| p.is_dir())
        .unwrap_or(std::env::current_dir()?);
    let mut events = Vec::new();
    let mut push = |kind, timestamp| {
        let mut event = SessionEvent::new(id, 0, kind);
        event.created_at = timestamp;
        events.push(event);
    };
    push(SessionEventKind::SessionStarted, start);
    push(
        SessionEventKind::SessionConfigured {
            cwd,
            provider: if matches!(source, Source::ClaudeCode | Source::ClaudeDesktop) {
                CodingProvider::Claude
            } else {
                CodingProvider::Codex
            },
            model: None,
            effort: None,
            fast: false,
            response_language: ResponseLanguage::Auto,
            permission_mode: PermissionMode::Manual,
        },
        start,
    );
    push(
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "conversation_imported".into(),
            payload: serde_json::json!({"source": source.key(), "source_id": thread.id, "title": thread.title, "original_cwd": thread.cwd}),
        },
        start,
    );
    push(
        SessionEventKind::TurnStarted {
            message_id: Uuid::new_v5(&id, b"0"),
            provider: if matches!(source, Source::ClaudeCode | Source::ClaudeDesktop) {
                CodingProvider::Claude
            } else {
                CodingProvider::Codex
            },
            model: None,
            effort: None,
            fast: false,
        },
        start,
    );
    for (index, message) in thread.messages.into_iter().enumerate() {
        let mut attachments = Vec::new();
        let mut text = message.text;
        for (attachment_index, attachment) in message.attachments.into_iter().enumerate() {
            let result: Result<PathBuf> = async {
                let bytes = if let Some(data) = attachment.data_base64 {
                    ensure!(data.len() <= 90 * 1024 * 1024, "attachment exceeds 64 MiB");
                    base64::engine::general_purpose::STANDARD.decode(data)?
                } else {
                    let path = attachment
                        .path
                        .context("attachment bytes were not included in the source export")?;
                    ensure!(
                        tokio::fs::metadata(&path).await?.len() <= 64 * 1024 * 1024,
                        "attachment exceeds 64 MiB"
                    );
                    tokio::fs::read(path).await?
                };
                let name = Path::new(&attachment.name)
                    .file_name()
                    .context("attachment has no filename")?;
                let directory = memory_dir
                    .parent()
                    .context("import directory missing")?
                    .join("attachments")
                    .join(id.to_string())
                    .join(format!("{index}-{attachment_index}"));
                tokio::fs::create_dir_all(&directory).await?;
                let path = directory.join(name);
                tokio::fs::write(&path, bytes).await?;
                Ok(path)
            }
            .await;
            match result {
                Ok(path) => attachments.push(path),
                Err(error) => {
                    warnings.push(format!(
                        "{}: attachment {}: {error:#}",
                        thread.title, attachment.name
                    ));
                    text.push_str(&format!(
                        "\n[Imported attachment unavailable: {}]",
                        attachment.name
                    ));
                }
            }
        }
        let actor = match message.role.as_str() {
            "user" | "human" => EventActor::User,
            "assistant" => EventActor::Assistant,
            other => {
                text = format!("[Imported {other} record — historical context]\n{text}");
                EventActor::Assistant
            }
        };
        push(
            SessionEventKind::Message {
                message_id: Uuid::new_v5(&id, index.to_string().as_bytes()),
                actor,
                text,
                attachments,
                status: MessageStatus::Complete,
                delivery: None,
            },
            message.created_at,
        );
    }
    push(
        SessionEventKind::TurnCompleted {
            message_id: Uuid::new_v5(&id, b"0"),
            provider_session_id: None,
            final_text: String::new(),
            error: None,
        },
        end,
    );
    push(
        SessionEventKind::StatusChanged {
            status: SessionStatus::Ready,
            detail: Some(format!("Imported from {} · {}", source.key(), thread.title)),
        },
        end,
    );
    store.import_session_events(id, events).await
}

pub(crate) async fn run(mut args: ImportArgs) -> Result<()> {
    let interactive = io::stdin().is_terminal() && io::stdout().is_terminal();
    if args.source.is_none() && interactive {
        println!(
            "Import into Borg\n1. Codex CLI / Desktop\n2. Claude Code\n3. Claude Desktop export\n4. Portable JSON"
        );
        args.source = Some(match read_line("Source [1]: ")?.as_str() {
            "" | "1" => Source::Codex,
            "2" => Source::ClaudeCode,
            "3" => Source::ClaudeDesktop,
            "4" => Source::Portable,
            _ => bail!("unknown source"),
        });
    }
    if args.path.is_none() && args.source.is_some_and(|s| s.default_path().is_none()) && interactive
    {
        args.path = Some(PathBuf::from(read_line("Export path: ")?));
    }
    let plan = prepare(&args).await?;
    let (thread_count, memory_count) = plan.counts();
    if args.json && !args.yes {
        println!(
            "{}",
            serde_json::json!({"threads": thread_count, "memory": memory_count, "warnings": plan.warnings})
        );
        return Ok(());
    }
    if !args.json {
        println!("Found {thread_count} threads and {memory_count} memory entries.");
    }
    for warning in &plan.warnings {
        eprintln!("{warning}");
    }
    if args.preview {
        return Ok(());
    }
    if !args.yes {
        ensure!(
            interactive,
            "use --preview to inspect or --yes to import without a terminal"
        );
        loop {
            println!(
                "1. [{}] Threads\n2. [{}] Memory",
                if args.no_threads { " " } else { "x" },
                if args.no_memory { " " } else { "x" }
            );
            match read_line("Enter to Import · 1/2 to toggle · q to cancel: ")?.as_str() {
                "1" => args.no_threads = !args.no_threads,
                "2" => args.no_memory = !args.no_memory,
                "q" | "Q" => return Ok(()),
                "" if !args.no_threads || !args.no_memory => break,
                _ => println!("Select at least one category."),
            }
        }
    }
    let report = execute(plan, !args.no_threads, !args.no_memory).await?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("{}", report.summary());
    }
    Ok(())
}
fn read_line(prompt: &str) -> Result<String> {
    print!("{prompt}");
    io::stdout().flush()?;
    let mut line = String::new();
    ensure!(io::stdin().read_line(&mut line)? != 0, "import cancelled");
    Ok(line.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use borg_remote::SessionStore;

    #[tokio::test]
    async fn portable_import_preserves_history_copies_attachments_and_skips_duplicates() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        std::fs::create_dir(&source).unwrap();
        std::fs::write(source.join("note.txt"), "original attachment").unwrap();
        let at = DateTime::parse_from_rfc3339("2025-01-02T03:04:05Z")
            .unwrap()
            .with_timezone(&Utc);
        let archive = serde_json::json!({"version":1,"threads":[{"id":"source-1","title":"Old work","messages":[
            {"role":"user","text":"Question","created_at":at,"attachments":[{"name":"note.txt","path":"note.txt"}]},
            {"role":"assistant","text":"Answer","created_at":at}]}],"memory":[{"source":"portable","source_id":"preference-1","title":"Preference","content":"Use concise answers","updated_at":at}]});
        let path = source.join("export.json");
        std::fs::write(&path, serde_json::to_vec(&archive).unwrap()).unwrap();
        let args = ImportArgs {
            source: Some(Source::Portable),
            path: Some(path),
            no_threads: false,
            no_memory: false,
            preview: false,
            yes: true,
            json: false,
        };
        let store = SqliteSessionStore::open(root.path().join("borg.sqlite3"))
            .await
            .unwrap();
        let memories = root.path().join("imports/memory");
        let plan = prepare(&args).await.unwrap();
        assert_eq!(plan.counts(), (1, 1));
        let report = execute_into(plan, true, true, &store, &memories)
            .await
            .unwrap();
        assert_eq!((report.threads_copied, report.memory_copied), (1, 1));
        let id = report.session_ids[0];
        let events = store.read(id).await.unwrap();
        let messages = events
            .iter()
            .filter_map(|event| match &event.kind {
                SessionEventKind::Message {
                    text, attachments, ..
                } => Some((event.created_at, text, attachments)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].0, at);
        assert_eq!(messages[1].1, "Answer");
        let copied = &messages[0].2[0];
        assert!(!copied.starts_with(&source));
        std::fs::remove_file(source.join("note.txt")).unwrap();
        assert_eq!(
            std::fs::read_to_string(copied).unwrap(),
            "original attachment"
        );
        let state = store.state(id).await.unwrap();
        assert!(state.has_resumable_activity());
        assert!(state.provider_session_id.is_none());
        assert_eq!(
            state.configuration.unwrap().permission_mode,
            PermissionMode::Manual
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e.kind, SessionEventKind::TurnStarted { .. }))
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e.kind, SessionEventKind::TurnCompleted { .. }))
        );
        let report = execute_into(prepare(&args).await.unwrap(), true, true, &store, &memories)
            .await
            .unwrap();
        assert_eq!(report.duplicates_skipped, 2);
        assert_eq!(store.read(id).await.unwrap().len(), events.len());
    }

    #[tokio::test]
    async fn import_category_opt_outs_do_not_copy_excluded_data() {
        let root = tempfile::tempdir().unwrap();
        for (threads, memory) in [(true, false), (false, true)] {
            let destination = root.path().join(format!("{threads}-{memory}"));
            let store = SqliteSessionStore::open(destination.join("sessions.sqlite3"))
                .await
                .unwrap();
            let memories = destination.join("memory");
            let plan = PreparedImport {
                source: Source::Portable,
                threads: vec![Thread {
                    id: "test".into(),
                    title: "test".into(),
                    cwd: None,
                    messages: vec![Message {
                        role: "user".into(),
                        text: "history".into(),
                        created_at: Utc::now(),
                        attachments: Vec::new(),
                    }],
                }],
                memory: vec![ImportedMemory {
                    source: "test".into(),
                    source_id: "test".into(),
                    title: "memory".into(),
                    content: "preference".into(),
                    cwd: None,
                    project: None,
                    updated_at: Utc::now(),
                }],
                warnings: Vec::new(),
            };
            let report = execute_into(plan, threads, memory, &store, &memories)
                .await
                .unwrap();
            assert_eq!(report.threads_copied, usize::from(threads));
            assert_eq!(report.memory_copied, usize::from(memory));
            assert_eq!(
                store.list_sessions(10).await.unwrap().len(),
                usize::from(threads)
            );
            assert_eq!(memories.exists(), memory);
        }
    }
}
