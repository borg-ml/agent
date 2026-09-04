use super::{Attachment, ImportedMemory, Message, PreparedImport, Source, Thread};
use anyhow::{Context, Result, ensure};
use base64::Engine;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    fs,
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
};

const MAX_FILE: u64 = 256 * 1024 * 1024;
const MAX_TOTAL: u64 = 1024 * 1024 * 1024;

pub(super) fn read(
    source: Source,
    path: &Path,
    threads: bool,
    memory: bool,
) -> Result<PreparedImport> {
    ensure!(path.exists(), "source does not exist: {}", path.display());
    let mut plan = PreparedImport {
        source,
        threads: Vec::new(),
        memory: Vec::new(),
        warnings: Vec::new(),
    };
    let unpacked;
    let path = if path.extension().is_some_and(|v| v == "zip") {
        unpacked = tempfile::tempdir()?;
        let mut archive = zip::ZipArchive::new(fs::File::open(path)?)?;
        ensure!(archive.len() <= 100_000, "archive contains too many files");
        let mut total = 0;
        for index in 0..archive.len() {
            let mut file = archive.by_index(index)?;
            let Some(relative) = file.enclosed_name() else {
                plan.warnings.push("Skipped unsafe archive path".into());
                continue;
            };
            ensure!(
                file.unix_mode()
                    .is_none_or(|mode| mode & 0o170000 != 0o120000),
                "archive contains a symbolic link"
            );
            if file.is_dir() {
                continue;
            }
            total += file.size();
            ensure!(
                file.size() <= MAX_FILE && total <= MAX_TOTAL,
                "archive exceeds import size limits"
            );
            let target = unpacked.path().join(relative);
            fs::create_dir_all(target.parent().unwrap())?;
            let mut output = fs::File::create(target)?;
            let copied = std::io::copy(&mut (&mut file).take(MAX_FILE + 1), &mut output)?;
            ensure!(
                copied <= MAX_FILE,
                "archive entry exceeds import size limit"
            );
        }
        unpacked.path()
    } else {
        path
    };
    let mut files = Vec::new();
    if path.is_file() {
        files.push(path.to_path_buf());
    } else {
        match source {
            Source::Codex => {
                for dir in ["sessions", "archived_sessions"] {
                    collect(&path.join(dir), &mut files)?;
                }
            }
            Source::ClaudeCode => collect(&path.join("projects"), &mut files)?,
            _ => collect(path, &mut files)?,
        }
    }
    files.sort();
    let mut total = 0;
    let mut project_roots = HashMap::new();
    let mut thread_scopes = HashMap::new();
    for file in &files {
        if matches!(source, Source::Codex | Source::ClaudeCode)
            && file.extension().is_some_and(|ext| ext == "jsonl")
        {
            let size = fs::metadata(file)?.len();
            if size > MAX_FILE {
                plan.warnings.push(format!(
                    "{}: skipped; source exceeds import size limit",
                    file.display()
                ));
                continue;
            }
            match read_jsonl(source, file, &mut plan.warnings) {
                Ok(Some(thread)) => {
                    if let Some(cwd) = &thread.cwd {
                        thread_scopes.insert(thread.id.clone(), cwd.clone());
                    }
                    if let (Some(parent), Some(cwd)) = (file.parent(), thread.cwd.as_ref()) {
                        project_roots.insert(parent.to_path_buf(), cwd.clone());
                    }
                    if threads {
                        let extracted = thread
                            .messages
                            .iter()
                            .map(|message| {
                                message.text.len() as u64
                                    + message
                                        .attachments
                                        .iter()
                                        .filter_map(|a| a.data_base64.as_ref())
                                        .map(|data| data.len() as u64)
                                        .sum::<u64>()
                            })
                            .sum::<u64>();
                        if total + extracted > MAX_TOTAL {
                            plan.warnings.push(format!("{}: extracted conversations exceed 1 GiB; import this source file separately", file.display()));
                        } else {
                            total += extracted;
                            plan.threads.push(thread);
                        }
                    }
                }
                Ok(None) => {}
                Err(error) => plan.warnings.push(format!("{}: {error:#}", file.display())),
            }
        }
    }
    if matches!(source, Source::Codex) && threads {
        let index = path.join("session_index.jsonl");
        if let Ok(file) = fs::File::open(index) {
            let mut names = HashMap::new();
            for line in BufReader::new(file).lines() {
                if let Ok(value) = serde_json::from_str::<Value>(&line?) {
                    if let (Some(id), Some(name)) =
                        (value["id"].as_str(), value["thread_name"].as_str())
                    {
                        names.insert(id.to_string(), name.to_string());
                    }
                }
            }
            for thread in &mut plan.threads {
                if let Some(name) = names.get(&thread.id) {
                    thread.title.clone_from(name);
                }
            }
        }
    }
    match source {
        Source::Codex | Source::ClaudeCode if memory => {
            let instruction = if matches!(source, Source::Codex) {
                "AGENTS.md"
            } else {
                "CLAUDE.md"
            };
            add_markdown(&mut plan, &path.join(instruction), None, instruction)?;
            let mut memory_files = Vec::new();
            if matches!(source, Source::Codex) {
                collect(&path.join("memories"), &mut memory_files)?;
            } else {
                memory_files.extend(
                    files
                        .iter()
                        .filter(|p| p.components().any(|part| part.as_os_str() == "memory"))
                        .cloned(),
                );
            }
            for file in memory_files {
                if file
                    .extension()
                    .is_none_or(|ext| ext != "md" && ext != "txt")
                {
                    continue;
                }
                let cwd = file
                    .ancestors()
                    .find_map(|ancestor| project_roots.get(ancestor))
                    .cloned();
                let key = file
                    .strip_prefix(path)
                    .unwrap_or(&file)
                    .to_string_lossy()
                    .into_owned();
                let unresolved = matches!(source, Source::ClaudeCode) && cwd.is_none();
                let before = plan.memory.len();
                add_markdown(&mut plan, &file, cwd, &key)?;
                if unresolved && plan.memory.len() > before {
                    plan.memory.last_mut().unwrap().project = Some(key.clone());
                }
            }
            for cwd in project_roots.values().collect::<HashSet<_>>() {
                add_markdown(
                    &mut plan,
                    &cwd.join(instruction),
                    Some(cwd.clone()),
                    &format!("{}:{instruction}", cwd.display()),
                )?;
            }
            if matches!(source, Source::Codex) && path.is_dir() {
                codex_memories(path, &thread_scopes, &mut plan)?;
            }
        }
        Source::ClaudeDesktop => {
            for file in &files {
                if file.extension().is_none_or(|ext| ext != "json") {
                    continue;
                }
                let name = file.file_name().unwrap_or_default().to_string_lossy();
                if threads
                    && (name == "conversations.json"
                        || (path.is_file() && name != "memories.json" && name != "projects.json"))
                {
                    match read_json(file).and_then(|value| claude_export(value, &mut plan)) {
                        Ok(()) => {}
                        Err(error) => plan.warnings.push(format!("{}: {error:#}", file.display())),
                    }
                } else if memory && name == "memories.json" {
                    match read_json(file) {
                        Ok(value) => export_memories(&value, "memories", &mut plan),
                        Err(error) => plan.warnings.push(format!("{}: {error:#}", file.display())),
                    }
                } else if memory && name == "projects.json" {
                    if let Ok(Value::Array(projects)) = read_json(file) {
                        for project in projects {
                            if let Some(content) = project
                                .get("prompt_template")
                                .and_then(Value::as_str)
                                .filter(|text| !text.is_empty())
                            {
                                let id = string(&project, "uuid");
                                plan.memory.push(ImportedMemory {
                                    source: source.key().into(),
                                    source_id: format!("project:{id}"),
                                    title: format!(
                                        "Project instructions: {}",
                                        string(&project, "name")
                                    ),
                                    content: content.into(),
                                    cwd: None,
                                    project: Some(id),
                                    updated_at: timestamp(project.get("updated_at")),
                                });
                                plan.warnings.push(format!("Project {} instructions were copied but need a local cwd set in the memory file before use", string(&project, "name")));
                            }
                        }
                    }
                }
            }
            if memory && plan.memory.is_empty() {
                plan.warnings.push("No memory found in the export. Include memories.json or use a portable memory export.".into());
            }
        }
        Source::Portable => {
            #[derive(Deserialize)]
            struct Archive {
                version: u32,
                #[serde(default)]
                threads: Vec<Thread>,
                #[serde(default)]
                memory: Vec<ImportedMemory>,
            }
            for file in &files {
                if file.extension().is_none_or(|ext| ext != "json") {
                    continue;
                }
                let archive: Archive = serde_json::from_value(read_json(file)?)?;
                ensure!(archive.version == 1, "unsupported portable import version");
                if threads {
                    plan.threads.extend(archive.threads);
                }
                if memory {
                    plan.memory.extend(archive.memory);
                }
            }
        }
        _ => {}
    }
    for entry in &plan.memory {
        if entry.project.is_some() && entry.cwd.is_none() {
            plan.warnings.push(format!("Memory {} will be copied inactive: set cwd in its imported memory file to map the source project.", entry.title));
        }
    }
    if threads && plan.threads.is_empty() {
        plan.warnings
            .push("No supported threads found in this source.".into());
    }
    let mut ids = HashSet::new();
    plan.threads.retain(|thread| ids.insert(thread.id.clone()));
    for thread in &mut plan.threads {
        ensure!(
            !thread.id.is_empty() && thread.messages.len() <= 100_000,
            "invalid thread id or too many messages"
        );
        for message in &mut thread.messages {
            for attachment in &mut message.attachments {
                if attachment.data_base64.is_some() {
                    continue;
                }
                if let Some(relative) = attachment.path.as_ref() {
                    let resolved = if relative.is_absolute() {
                        relative.clone()
                    } else {
                        path.parent()
                            .filter(|_| path.is_file())
                            .unwrap_or(path)
                            .join(relative)
                    };
                    let trusted_local = matches!(source, Source::Codex | Source::ClaudeCode);
                    let source_root = path
                        .parent()
                        .filter(|_| path.is_file())
                        .unwrap_or(path)
                        .canonicalize()?;
                    let within_export = resolved
                        .canonicalize()
                        .is_ok_and(|p| p.starts_with(&source_root));
                    if !trusted_local && !within_export {
                        plan.warnings.push(format!(
                            "Attachment {} points outside the export and was not copied",
                            attachment.name
                        ));
                        attachment.path = None;
                        continue;
                    }
                    if resolved
                        .metadata()
                        .is_ok_and(|m| m.is_file() && m.len() <= 64 * 1024 * 1024)
                    {
                        attachment.data_base64 = Some(
                            base64::engine::general_purpose::STANDARD.encode(fs::read(&resolved)?),
                        );
                    }
                    attachment.path = None;
                }
            }
        }
    }
    Ok(plan)
}

fn collect(root: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_symlink() {
                continue;
            }
            if kind.is_dir() {
                dirs.push(entry.path());
            } else if kind.is_file() {
                files.push(entry.path());
            }
            ensure!(
                files.len() + dirs.len() <= 100_000,
                "source contains too many files"
            );
        }
    }
    Ok(())
}
fn read_json(path: &Path) -> Result<Value> {
    ensure!(
        fs::metadata(path)?.len() <= MAX_FILE,
        "JSON exceeds 256 MiB"
    );
    Ok(serde_json::from_reader(BufReader::new(fs::File::open(
        path,
    )?))?)
}
fn timestamp(value: Option<&Value>) -> DateTime<Utc> {
    value
        .and_then(|v| {
            v.as_str()
                .and_then(|s| {
                    DateTime::parse_from_rfc3339(s)
                        .ok()
                        .map(|d| d.with_timezone(&Utc))
                })
                .or_else(|| {
                    v.as_i64().and_then(|n| {
                        if n > 10_000_000_000 {
                            DateTime::from_timestamp_millis(n)
                        } else {
                            DateTime::from_timestamp(n, 0)
                        }
                    })
                })
        })
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}
fn string(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn content(value: &Value) -> (String, Vec<Attachment>) {
    if let Some(text) = value.as_str() {
        return (text.into(), Vec::new());
    }
    let mut text = Vec::new();
    let mut attachments = Vec::new();
    if let Some(blocks) = value.as_array() {
        for block in blocks {
            if let Some(part) = block.get("text").and_then(Value::as_str) {
                text.push(part.to_string());
            } else if matches!(
                block.get("type").and_then(Value::as_str),
                Some("tool_use" | "tool_result")
            ) {
                text.push(format!("[Imported tool record]\n{block}"));
            } else if matches!(
                block.get("type").and_then(Value::as_str),
                Some("image" | "input_image" | "localImage" | "file" | "document")
            ) {
                let url = block
                    .get("image_url")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let data = block
                    .pointer("/source/data")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .or_else(|| {
                        url.strip_prefix("data:")
                            .and_then(|s| s.split_once(";base64,"))
                            .map(|(_, data)| data.to_string())
                    });
                let mime = block
                    .pointer("/source/media_type")
                    .and_then(Value::as_str)
                    .unwrap_or("image/png");
                let extension = if mime == "image/jpeg" || url.starts_with("data:image/jpeg") {
                    "jpg"
                } else if mime == "application/pdf" {
                    "pdf"
                } else {
                    "png"
                };
                attachments.push(Attachment {
                    name: block
                        .get("file_name")
                        .or_else(|| block.get("name"))
                        .and_then(Value::as_str)
                        .map(str::to_string)
                        .unwrap_or_else(|| {
                            format!("attachment-{}.{}", attachments.len(), extension)
                        }),
                    path: block.get("path").and_then(Value::as_str).map(PathBuf::from),
                    data_base64: data,
                });
            }
        }
    }
    (text.join("\n\n"), attachments)
}

fn read_jsonl(source: Source, file: &Path, warnings: &mut Vec<String>) -> Result<Option<Thread>> {
    let mut thread = Thread {
        id: String::new(),
        title: String::new(),
        cwd: None,
        messages: Vec::new(),
    };
    let mut completed = Vec::new();
    let mut fallback = Vec::new();
    let mut seen = HashMap::new();
    for (line_number, line) in BufReader::new(fs::File::open(file)?).lines().enumerate() {
        let line = line?;
        let value: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(_) => {
                warnings.push(format!(
                    "{}:{}: skipped invalid JSON record",
                    file.display(),
                    line_number + 1
                ));
                continue;
            }
        };
        let time = timestamp(value.get("timestamp"));
        let kind = string(&value, "type");
        let payload = &value["payload"];
        if matches!(source, Source::Codex) {
            if kind == "session_meta" {
                thread.id = payload
                    .get("id")
                    .or_else(|| payload.get("session_id"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .into();
                thread.cwd = payload
                    .get("cwd")
                    .and_then(Value::as_str)
                    .map(PathBuf::from);
            } else if kind == "response_item" {
                let role = string(payload, "role");
                if payload["type"] == "message" && matches!(role.as_str(), "user" | "assistant") {
                    let (text, attachments) = content(&payload["content"]);
                    if !text.is_empty() || !attachments.is_empty() {
                        thread.messages.push(Message {
                            role,
                            text,
                            attachments,
                            created_at: time,
                        });
                    }
                } else if matches!(
                    payload["type"].as_str(),
                    Some(
                        "function_call"
                            | "function_call_output"
                            | "custom_tool_call"
                            | "custom_tool_call_output"
                    )
                ) {
                    thread.messages.push(Message {
                        role: "tool".into(),
                        text: payload.to_string(),
                        attachments: Vec::new(),
                        created_at: time,
                    });
                }
            } else if kind == "event_msg" {
                if payload["type"] == "item_completed" {
                    if let Some(message) = codex_item(&payload["item"], time) {
                        completed.push(message);
                    }
                } else if matches!(
                    payload["type"].as_str(),
                    Some("user_message" | "agent_message")
                ) {
                    fallback.push(Message {
                        role: if payload["type"] == "user_message" {
                            "user"
                        } else {
                            "assistant"
                        }
                        .into(),
                        text: string(payload, "message"),
                        attachments: Vec::new(),
                        created_at: time,
                    });
                }
            }
        } else {
            if let Some(id) = value.get("sessionId").and_then(Value::as_str) {
                thread.id = id.into();
            }
            if let Some(cwd) = value.get("cwd").and_then(Value::as_str) {
                thread.cwd = Some(cwd.into());
            }
            if kind == "ai-title" {
                thread.title = string(&value, "aiTitle");
            }
            if matches!(kind.as_str(), "user" | "assistant") {
                let (text, attachments) = content(&value["message"]["content"]);
                if text.is_empty() && attachments.is_empty() {
                    continue;
                }
                let message = Message {
                    role: kind,
                    text,
                    attachments,
                    created_at: time,
                };
                let id = string(&value, "uuid");
                if let Some(index) = seen.get(&id).copied().filter(|_| !id.is_empty()) {
                    thread.messages[index] = message;
                } else {
                    seen.insert(id, thread.messages.len());
                    thread.messages.push(message);
                }
            }
        }
    }
    if matches!(source, Source::Codex) {
        if completed.iter().any(|m| m.role == "user")
            && completed.iter().any(|m| m.role == "assistant")
        {
            thread.messages = completed;
        } else if !thread.messages.iter().any(|m| m.role == "user") {
            thread.messages = fallback;
        }
    }
    if matches!(source, Source::ClaudeCode)
        && file
            .components()
            .any(|part| part.as_os_str() == "subagents")
    {
        thread.id = format!(
            "{}:subagent:{}",
            thread.id,
            file.file_stem().unwrap_or_default().to_string_lossy()
        );
    }
    if thread.messages.is_empty() {
        return Ok(None);
    }
    ensure!(!thread.id.is_empty(), "transcript has no stable session id");
    if thread.title.is_empty() {
        thread.title = thread
            .messages
            .iter()
            .find(|m| m.role == "user")
            .map(|m| m.text.chars().take(100).collect())
            .unwrap_or_else(|| thread.id.clone());
    }
    Ok(Some(thread))
}
fn codex_item(item: &Value, created_at: DateTime<Utc>) -> Option<Message> {
    let (role, text, attachments) = match item["type"].as_str()? {
        "userMessage" => {
            let (text, attachments) = content(&item["content"]);
            ("user", text, attachments)
        }
        "agentMessage" => ("assistant", string(item, "text"), Vec::new()),
        "commandExecution" | "fileChange" | "mcpToolCall" | "webSearch" | "imageView" => {
            ("tool", item.to_string(), Vec::new())
        }
        _ => return None,
    };
    Some(Message {
        role: role.into(),
        text,
        attachments,
        created_at,
    })
}
fn claude_export(value: Value, plan: &mut PreparedImport) -> Result<()> {
    let conversations = value
        .as_array()
        .context("expected an array of Claude conversations")?;
    for conversation in conversations {
        let id = string(conversation, "uuid");
        ensure!(!id.is_empty(), "conversation has no uuid");
        let mut messages = Vec::new();
        for message in conversation["chat_messages"]
            .as_array()
            .context("conversation has no chat_messages")?
        {
            let (mut text, mut attachments) = content(&message["content"]);
            if text.is_empty() {
                text = string(message, "text");
            }
            for attachment in message["attachments"].as_array().into_iter().flatten() {
                let extracted = string(attachment, "extracted_content");
                if !extracted.is_empty() {
                    text.push_str(&format!(
                        "\n[Attachment: {}]\n{extracted}",
                        string(attachment, "file_name")
                    ));
                } else {
                    attachments.push(Attachment {
                        name: string(attachment, "file_name"),
                        path: attachment
                            .get("path")
                            .and_then(Value::as_str)
                            .map(PathBuf::from),
                        data_base64: None,
                    });
                }
            }
            messages.push(Message {
                role: if message["sender"] == "human" {
                    "user"
                } else {
                    "assistant"
                }
                .into(),
                text,
                attachments,
                created_at: timestamp(message.get("created_at")),
            });
        }
        if !messages.is_empty() {
            plan.threads.push(Thread {
                id,
                title: string(conversation, "name"),
                cwd: None,
                messages,
            });
        }
    }
    Ok(())
}
fn export_memories(value: &Value, key: &str, plan: &mut PreparedImport) {
    match value {
        Value::String(content) if !content.trim().is_empty() => plan.memory.push(ImportedMemory {
            source: plan.source.key().into(),
            source_id: key.into(),
            title: key.into(),
            content: content.clone(),
            cwd: None,
            project: key.contains("project_memories/").then(|| key.to_string()),
            updated_at: DateTime::UNIX_EPOCH,
        }),
        Value::Array(values) => {
            for (i, value) in values.iter().enumerate() {
                export_memories(value, &format!("{key}/{i}"), plan);
            }
        }
        Value::Object(values) => {
            if let Some(text) = values.get("content").and_then(Value::as_str) {
                let name = values
                    .get("path")
                    .or_else(|| values.get("name"))
                    .and_then(Value::as_str)
                    .unwrap_or(key);
                plan.memory.push(ImportedMemory {
                    source: plan.source.key().into(),
                    source_id: name.into(),
                    title: name.into(),
                    content: text.into(),
                    cwd: None,
                    project: key.contains("project_memories/").then(|| key.to_string()),
                    updated_at: timestamp(values.get("updated_at")),
                });
            } else {
                for (name, value) in values {
                    if !matches!(
                        name.as_str(),
                        "uuid" | "id" | "created_at" | "updated_at" | "account_uuid"
                    ) {
                        export_memories(value, &format!("{key}/{name}"), plan);
                    }
                }
            }
        }
        _ => {}
    }
}
fn add_markdown(
    plan: &mut PreparedImport,
    file: &Path,
    cwd: Option<PathBuf>,
    key: &str,
) -> Result<()> {
    if !file.is_file() {
        return Ok(());
    }
    if fs::metadata(file)?.len() > 4 * 1024 * 1024 {
        plan.warnings
            .push(format!("{}: memory exceeds 4 MiB", file.display()));
        return Ok(());
    }
    let content = fs::read_to_string(file)?;
    if content.trim().is_empty() {
        return Ok(());
    }
    plan.memory.push(ImportedMemory {
        project: None,
        source: plan.source.key().into(),
        source_id: key.into(),
        title: file
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned(),
        content,
        cwd,
        updated_at: fs::metadata(file)?
            .modified()
            .map(DateTime::<Utc>::from)
            .unwrap_or(DateTime::UNIX_EPOCH),
    });
    Ok(())
}
fn codex_memories(
    root: &Path,
    scopes: &HashMap<String, PathBuf>,
    plan: &mut PreparedImport,
) -> Result<()> {
    let mut databases = fs::read_dir(root)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("memories_"))
                && p.extension().is_some_and(|ext| ext == "sqlite")
        })
        .collect::<Vec<_>>();
    databases.sort();
    let Some(database) = databases.last() else {
        return Ok(());
    };
    let result: Result<Vec<(String, String, i64)>> = tokio::runtime::Handle::current().block_on(async {
        let options = sqlx::sqlite::SqliteConnectOptions::new().filename(database).read_only(true);
        let mut connection = sqlx::SqliteConnection::connect_with(&options).await?;
        Ok(sqlx::query_as("select thread_id, raw_memory, generated_at from stage1_outputs where raw_memory != ''").fetch_all(&mut connection).await?)
    });
    match result {
        Ok(rows) => {
            for (id, content, generated_at) in rows {
                let cwd = scopes.get(&id).cloned();
                plan.memory.push(ImportedMemory {
                    source: "codex".into(),
                    source_id: format!("memory:{id}"),
                    title: format!("Codex thread {id}"),
                    content,
                    project: Some(id),
                    cwd,
                    updated_at: DateTime::from_timestamp(generated_at, 0)
                        .unwrap_or(DateTime::UNIX_EPOCH),
                });
            }
        }
        Err(error) => plan.warnings.push(format!(
            "Codex memory database could not be read: {error:#}"
        )),
    }
    Ok(())
}
use sqlx::Connection;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_transcripts_keep_tool_history_without_duplicating_display_events() {
        let root = tempfile::tempdir().unwrap();
        let codex = root.path().join("codex.jsonl");
        let records = [
            serde_json::json!({"type":"session_meta","payload":{"id":"codex-id","cwd":"/project"}}),
            serde_json::json!({"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"Question"}]}}),
            serde_json::json!({"type":"event_msg","payload":{"type":"user_message","message":"Question"}}),
            serde_json::json!({"type":"response_item","payload":{"type":"function_call","name":"exec","arguments":"pwd"}}),
            serde_json::json!({"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Answer"}]}}),
        ];
        fs::write(
            &codex,
            records
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        let mut warnings = Vec::new();
        let thread = read_jsonl(Source::Codex, &codex, &mut warnings)
            .unwrap()
            .unwrap();
        assert_eq!(thread.messages.len(), 3);
        assert_eq!(thread.messages[1].role, "tool");
        assert_eq!(thread.messages[2].text, "Answer");
        let claude = root.path().join("claude.jsonl");
        let records = [
            serde_json::json!({"type":"user","sessionId":"claude-id","uuid":"user","cwd":"/project","message":{"content":"Question"}}),
            serde_json::json!({"type":"assistant","sessionId":"claude-id","uuid":"assistant","message":{"content":[{"type":"text","text":"Partial"}]}}),
            serde_json::json!({"type":"assistant","sessionId":"claude-id","uuid":"assistant","message":{"content":[{"type":"text","text":"Complete"}]}}),
        ];
        fs::write(
            &claude,
            records
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        let thread = read_jsonl(Source::ClaudeCode, &claude, &mut warnings)
            .unwrap()
            .unwrap();
        assert_eq!(thread.messages.len(), 2);
        assert_eq!(thread.messages[1].text, "Complete");
        assert!(warnings.is_empty());
    }

    #[test]
    fn claude_export_copies_conversations_and_keeps_project_memory_scoped() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("conversations.json"), serde_json::to_vec(&serde_json::json!([
            {"uuid":"chat-id","name":"My chat","chat_messages":[{"sender":"human","text":"Hi","created_at":"2025-01-01T00:00:00Z"},{"sender":"assistant","content":[{"type":"text","text":"Hello"}],"created_at":"2025-01-01T00:00:01Z"}]}
        ])).unwrap()).unwrap();
        fs::write(root.path().join("memories.json"), serde_json::to_vec(&serde_json::json!({"conversations_memory":"Global preference", "project_memories":{"project-id":"Project preference"}})).unwrap()).unwrap();
        let plan = read(Source::ClaudeDesktop, root.path(), true, true).unwrap();
        assert_eq!(plan.counts(), (1, 2));
        assert_eq!(plan.threads[0].messages[1].text, "Hello");
        assert!(
            plan.memory
                .iter()
                .find(|m| m.content == "Project preference")
                .unwrap()
                .project
                .is_some()
        );
        assert!(
            plan.memory
                .iter()
                .find(|m| m.content == "Global preference")
                .unwrap()
                .project
                .is_none()
        );
    }

    #[test]
    fn portable_export_cannot_copy_attachments_outside_the_source() {
        let root = tempfile::tempdir().unwrap();
        let export = root.path().join("export");
        fs::create_dir(&export).unwrap();
        fs::write(root.path().join("private.txt"), "do not import").unwrap();
        fs::write(export.join("archive.json"), serde_json::to_vec(&serde_json::json!({"version":1,"threads":[{"id":"id","title":"test","messages":[{"role":"user","text":"Hi","created_at":"2025-01-01T00:00:00Z","attachments":[{"name":"private.txt","path":"../private.txt"}]}]}]})).unwrap()).unwrap();
        let plan = read(Source::Portable, &export, true, false).unwrap();
        let attachment = &plan.threads[0].messages[0].attachments[0];
        assert!(attachment.path.is_none() && attachment.data_base64.is_none());
        assert!(!plan.warnings.is_empty());
    }
}
