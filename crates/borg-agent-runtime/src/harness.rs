use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use uuid::Uuid;

const MAX_ENTRIES: usize = 128;
const MAX_REFINEMENTS: usize = 64;
const MAX_ENTRY_TITLE_BYTES: usize = 512;
const MAX_ENTRY_CONTENT_BYTES: usize = 64 * 1024;
const MAX_PROMPT_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HarnessKind {
    Prompt,
    Memory,
    Skill,
    Subagent,
}

impl HarnessKind {
    fn label(self) -> &'static str {
        match self {
            Self::Prompt => "prompt",
            Self::Memory => "memory",
            Self::Skill => "skill",
            Self::Subagent => "subagent",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HarnessScope {
    Local,
    Global,
}

impl HarnessScope {
    fn label(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Global => "global",
        }
    }
}

fn empty_object() -> Value {
    json!({})
}

fn now() -> DateTime<Utc> {
    Utc::now()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HarnessEntry {
    pub id: String,
    pub kind: HarnessKind,
    pub title: String,
    pub content: String,
    #[serde(default = "default_path")]
    pub path: String,
    pub scope: HarnessScope,
    #[serde(default = "empty_object")]
    pub reference: Value,
    #[serde(default = "empty_object")]
    pub arguments: Value,
    #[serde(default = "empty_object")]
    pub metadata: Value,
    #[serde(default = "default_source")]
    pub source: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub version: u64,
}

fn default_path() -> String {
    "general".to_string()
}

fn default_source() -> String {
    "agent".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RefinementEvent {
    pub id: String,
    pub trigger: String,
    pub changes: Vec<String>,
    #[serde(default)]
    pub evidence: String,
    #[serde(default)]
    pub outcome: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HarnessState {
    #[serde(default = "default_schema")]
    pub schema: u32,
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub entries: Vec<HarnessEntry>,
    #[serde(default)]
    pub refinements: Vec<RefinementEvent>,
}

impl Default for HarnessState {
    fn default() -> Self {
        Self {
            schema: 1,
            revision: 0,
            entries: Vec::new(),
            refinements: Vec::new(),
        }
    }
}

fn default_schema() -> u32 {
    1
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HarnessRequest {
    op: String,
    #[serde(default)]
    kind: Option<HarnessKind>,
    #[serde(default)]
    scope: Option<HarnessScope>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    reference: Option<Value>,
    #[serde(default)]
    arguments: Option<Value>,
    #[serde(default)]
    metadata: Option<Value>,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    trigger: Option<String>,
    #[serde(default)]
    changes: Vec<String>,
    #[serde(default)]
    evidence: Option<String>,
    #[serde(default)]
    outcome: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    steps: Option<usize>,
}

fn state_path(root: &Path) -> PathBuf {
    root.join(".borg").join("harness_state.json")
}

async fn load_state(
    scope: HarnessScope,
    session_id: Uuid,
    root: &Path,
    store: Option<&crate::SqliteSessionStore>,
) -> Result<HarnessState> {
    let value = match scope {
        HarnessScope::Local => {
            let store = store.context("local harness state requires a durable session store")?;
            store.load_harness_state(session_id).await?
        }
        HarnessScope::Global => match tokio::fs::read(state_path(root)).await {
            Ok(bytes) => Some(
                serde_json::from_slice(&bytes).context("global harness state is invalid JSON")?,
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error).context("failed to read global harness state"),
        },
    };
    value
        .map(|value| {
            serde_json::from_value(value).context("stored harness state has an invalid shape")
        })
        .transpose()
        .map(|state| state.unwrap_or_default())
}

async fn save_state(
    scope: HarnessScope,
    state: &HarnessState,
    session_id: Uuid,
    root: &Path,
    store: Option<&crate::SqliteSessionStore>,
) -> Result<()> {
    let value = serde_json::to_value(state)?;
    match scope {
        HarnessScope::Local => {
            let store = store.context("local harness state requires a durable session store")?;
            store.save_harness_state(session_id, &value).await
        }
        HarnessScope::Global => {
            let path = state_path(root);
            let parent = path
                .parent()
                .context("global harness state has no parent")?;
            tokio::fs::create_dir_all(parent).await?;
            let temporary = parent.join(format!(".harness_state.{}.tmp", Uuid::new_v4()));
            tokio::fs::write(&temporary, serde_json::to_vec_pretty(&value)?).await?;
            if let Err(error) = tokio::fs::rename(&temporary, &path).await {
                let _ = tokio::fs::remove_file(&temporary).await;
                return Err(error).context("failed to replace global harness state");
            }
            Ok(())
        }
    }
}

fn validate_text(label: &str, value: &str, max_bytes: usize) -> Result<()> {
    ensure!(!value.trim().is_empty(), "harness {label} is empty");
    ensure!(
        value.len() <= max_bytes,
        "harness {label} exceeds {max_bytes} bytes"
    );
    Ok(())
}

fn validate_id(id: &str) -> Result<()> {
    validate_text("id", id, 128)?;
    ensure!(
        id.chars()
            .all(|character| character.is_ascii_alphanumeric() || "-_:.".contains(character)),
        "harness id contains unsupported characters"
    );
    Ok(())
}

fn validate_object(label: &str, value: &Value) -> Result<()> {
    ensure!(value.is_object(), "harness {label} must be a JSON object");
    ensure!(
        serde_json::to_vec(value)?.len() <= 32 * 1024,
        "harness {label} exceeds 32768 bytes"
    );
    Ok(())
}

fn validate_request_fields(request: &HarnessRequest) -> Result<()> {
    if let Some(id) = &request.id {
        validate_id(id)?;
    }
    if let Some(title) = &request.title {
        validate_text("title", title, MAX_ENTRY_TITLE_BYTES)?;
    }
    if let Some(content) = &request.content {
        validate_text("content", content, MAX_ENTRY_CONTENT_BYTES)?;
    }
    if let Some(path) = &request.path {
        validate_text("path", path, 256)?;
    }
    if let Some(source) = &request.source {
        validate_text("source", source, 128)?;
    }
    for (label, value) in [
        ("reference", request.reference.as_ref()),
        ("arguments", request.arguments.as_ref()),
        ("metadata", request.metadata.as_ref()),
    ] {
        if let Some(value) = value {
            validate_object(label, value)?;
        }
    }
    Ok(())
}

fn find_entry<'a>(
    state: &'a HarnessState,
    kind: Option<HarnessKind>,
    id: &str,
) -> Option<&'a HarnessEntry> {
    state
        .entries
        .iter()
        .find(|entry| entry.id == id && kind.is_none_or(|kind| entry.kind == kind))
}

fn find_entry_mut<'a>(
    state: &'a mut HarnessState,
    kind: Option<HarnessKind>,
    id: &str,
) -> Option<&'a mut HarnessEntry> {
    state
        .entries
        .iter_mut()
        .find(|entry| entry.id == id && kind.is_none_or(|kind| entry.kind == kind))
}

fn sorted_entries(mut entries: Vec<HarnessEntry>, limit: usize) -> Vec<HarnessEntry> {
    entries.sort_by(|left, right| {
        (
            left.kind.label(),
            left.path.as_str(),
            left.title.as_str(),
            left.id.as_str(),
        )
            .cmp(&(
                right.kind.label(),
                right.path.as_str(),
                right.title.as_str(),
                right.id.as_str(),
            ))
    });
    entries.truncate(limit.clamp(1, MAX_ENTRIES));
    entries
}

async fn list_entries(
    request: &HarnessRequest,
    session_id: Uuid,
    root: &Path,
    store: Option<&crate::SqliteSessionStore>,
) -> Result<Value> {
    let scopes = request
        .scope
        .map(|scope| vec![scope])
        .unwrap_or_else(|| vec![HarnessScope::Local, HarnessScope::Global]);
    let mut entries = Vec::new();
    for scope in scopes {
        let state = load_state(scope, session_id, root, store).await?;
        entries.extend(
            state
                .entries
                .into_iter()
                .filter(|entry| request.kind.is_none_or(|kind| entry.kind == kind)),
        );
    }
    Ok(json!({
        "entries": sorted_entries(entries, request.limit.unwrap_or(MAX_ENTRIES)),
        "limit": request.limit.unwrap_or(MAX_ENTRIES).clamp(1, MAX_ENTRIES),
    }))
}

async fn overview(
    request: &HarnessRequest,
    session_id: Uuid,
    root: &Path,
    store: Option<&crate::SqliteSessionStore>,
) -> Result<Value> {
    let local = load_state(HarnessScope::Local, session_id, root, store).await?;
    let global = load_state(HarnessScope::Global, session_id, root, store).await?;
    let count = |state: &HarnessState, kind| {
        state
            .entries
            .iter()
            .filter(|entry| entry.kind == kind)
            .count()
    };
    Ok(json!({
        "schema": 1,
        "revision": local.revision.max(global.revision),
        "scopes": {
            "local": {"entries": local.entries.len(), "refinements": local.refinements.len()},
            "global": {"entries": global.entries.len(), "refinements": global.refinements.len()},
        },
        "counts": {
            "prompt": count(&local, HarnessKind::Prompt) + count(&global, HarnessKind::Prompt),
            "memory": count(&local, HarnessKind::Memory) + count(&global, HarnessKind::Memory),
            "skill": count(&local, HarnessKind::Skill) + count(&global, HarnessKind::Skill),
            "subagent": count(&local, HarnessKind::Subagent) + count(&global, HarnessKind::Subagent),
        },
        "recent_refinements": local.refinements.into_iter().chain(global.refinements).rev().take(request.limit.unwrap_or(8).clamp(1, 16)).collect::<Vec<_>>(),
    }))
}

async fn get_entry(
    request: &HarnessRequest,
    session_id: Uuid,
    root: &Path,
    store: Option<&crate::SqliteSessionStore>,
) -> Result<Value> {
    let id = request
        .id
        .as_deref()
        .context("harness entry id is required")?;
    let scope = request.scope.unwrap_or(HarnessScope::Local);
    let state = load_state(scope, session_id, root, store).await?;
    let entry = find_entry(&state, request.kind, id).cloned();
    Ok(json!({"entry": entry}))
}

fn next_id(state: &HarnessState, kind: HarnessKind, title: &str) -> String {
    let slug = title
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .chars()
        .take(48)
        .collect::<String>();
    let base = if slug.is_empty() { kind.label() } else { &slug };
    let mut id = base.to_string();
    let mut suffix = 2;
    while state
        .entries
        .iter()
        .any(|entry| entry.id == id && entry.kind == kind)
    {
        id = format!("{base}-{suffix}");
        suffix += 1;
    }
    id
}

fn make_entry(
    request: &HarnessRequest,
    state: &HarnessState,
    scope: HarnessScope,
) -> Result<HarnessEntry> {
    let kind = request.kind.context("harness kind is required")?;
    let title = request
        .title
        .as_deref()
        .context("harness title is required")?;
    let content = request
        .content
        .as_deref()
        .context("harness content is required")?;
    validate_text("title", title, MAX_ENTRY_TITLE_BYTES)?;
    validate_text("content", content, MAX_ENTRY_CONTENT_BYTES)?;
    let id = request
        .id
        .clone()
        .unwrap_or_else(|| next_id(state, kind, title));
    validate_id(&id)?;
    let timestamp = now();
    Ok(HarnessEntry {
        id,
        kind,
        title: title.to_string(),
        content: content.to_string(),
        path: request.path.clone().unwrap_or_else(default_path),
        scope,
        reference: request.reference.clone().unwrap_or_else(empty_object),
        arguments: request.arguments.clone().unwrap_or_else(empty_object),
        metadata: request.metadata.clone().unwrap_or_else(empty_object),
        source: request.source.clone().unwrap_or_else(default_source),
        created_at: timestamp,
        updated_at: timestamp,
        version: 1,
    })
}

async fn mutate(
    request: &HarnessRequest,
    session_id: Uuid,
    root: &Path,
    store: Option<&crate::SqliteSessionStore>,
    allow_effects: bool,
) -> Result<Value> {
    ensure!(
        allow_effects,
        "harness mutation requires Full Access or an approved runtime call"
    );
    let scope = request.scope.unwrap_or(HarnessScope::Local);
    let mut state = load_state(scope, session_id, root, store).await?;
    let result = match request.op.as_str() {
        "create" => {
            ensure!(
                state.entries.len() < MAX_ENTRIES,
                "harness entry limit reached"
            );
            let entry = make_entry(request, &state, scope)?;
            ensure!(
                find_entry(&state, Some(entry.kind), &entry.id).is_none(),
                "harness entry already exists"
            );
            state.entries.push(entry.clone());
            json!({"entry": entry})
        }
        "update" => {
            let id = request
                .id
                .as_deref()
                .context("harness entry id is required")?;
            let entry = find_entry_mut(&mut state, request.kind, id)
                .context("harness entry does not exist")?;
            if let Some(title) = &request.title {
                validate_text("title", title, MAX_ENTRY_TITLE_BYTES)?;
                entry.title = title.clone();
            }
            if let Some(content) = &request.content {
                validate_text("content", content, MAX_ENTRY_CONTENT_BYTES)?;
                entry.content = content.clone();
            }
            if let Some(path) = &request.path {
                entry.path = path.clone();
            }
            if let Some(reference) = &request.reference {
                validate_object("reference", reference)?;
                entry.reference = reference.clone();
            }
            if let Some(arguments) = &request.arguments {
                validate_object("arguments", arguments)?;
                entry.arguments = arguments.clone();
            }
            if let Some(metadata) = &request.metadata {
                validate_object("metadata", metadata)?;
                entry.metadata = metadata.clone();
            }
            if let Some(source) = &request.source {
                validate_text("source", source, 128)?;
                entry.source = source.clone();
            }
            entry.updated_at = now();
            entry.version = entry.version.saturating_add(1);
            json!({"entry": entry})
        }
        "delete" => {
            let id = request
                .id
                .as_deref()
                .context("harness entry id is required")?;
            let before = state.entries.len();
            state.entries.retain(|entry| {
                !(entry.id == id && request.kind.is_none_or(|kind| entry.kind == kind))
            });
            json!({"deleted": state.entries.len() != before, "id": id})
        }
        "refine" => {
            let trigger = request
                .trigger
                .as_deref()
                .context("refinement trigger is required")?;
            validate_text("refinement trigger", trigger, 4 * 1024)?;
            ensure!(request.changes.len() <= 32, "too many refinement changes");
            for change in &request.changes {
                validate_text("refinement change", change, 4 * 1024)?;
            }
            let event = RefinementEvent {
                id: format!("refine-{}", Uuid::new_v4().simple()),
                trigger: trigger.to_string(),
                changes: request.changes.clone(),
                evidence: request.evidence.clone().unwrap_or_default(),
                outcome: request.outcome.clone().unwrap_or_default(),
                created_at: now(),
            };
            state.refinements.push(event.clone());
            if state.refinements.len() > MAX_REFINEMENTS {
                let overflow = state.refinements.len() - MAX_REFINEMENTS;
                state.refinements.drain(..overflow);
            }
            json!({"refinement": event})
        }
        "rollback" => {
            let steps = request.steps.unwrap_or(1);
            ensure!(
                scope == HarnessScope::Local,
                "global harness rollback is not available"
            );
            let store = store.context("local harness state requires a durable session store")?;
            return Ok(json!({
                "state": store.rollback_harness_state(session_id, steps).await?
            }));
        }
        other => bail!("unknown harness mutation '{other}'"),
    };
    state.revision = state.revision.saturating_add(1);
    save_state(scope, &state, session_id, root, store).await?;
    Ok(result)
}

pub(crate) async fn call(
    arguments: Value,
    session_id: Uuid,
    root: &Path,
    store: Option<&crate::SqliteSessionStore>,
    global_lock: &Arc<Mutex<()>>,
    allow_effects: bool,
) -> Result<Value> {
    let request: HarnessRequest = serde_json::from_value(arguments)?;
    validate_request_fields(&request)?;
    let _lock = global_lock.lock().await;
    match request.op.as_str() {
        "list" => list_entries(&request, session_id, root, store).await,
        "overview" => overview(&request, session_id, root, store).await,
        "get" => get_entry(&request, session_id, root, store).await,
        "plan_refinement" => {
            let observation = request
                .content
                .as_deref()
                .context("refinement observation is required")?;
            Ok(json!({
                "steps": [
                    format!("Diagnose the repeated failure or opportunity: {observation}"),
                    "Update the smallest useful prompt, memory, skill, or subagent entry.",
                    "Run the next action with the changed harness state and record the outcome."
                ]
            }))
        }
        "create" | "update" | "delete" | "refine" | "rollback" => {
            mutate(&request, session_id, root, store, allow_effects).await
        }
        other => bail!("unknown harness operation '{other}'"),
    }
}

pub(crate) async fn prompt_appendix(
    session_id: Uuid,
    root: &Path,
    store: Option<&crate::SqliteSessionStore>,
    global_lock: &Arc<Mutex<()>>,
) -> Result<String> {
    let _lock = global_lock.lock().await;
    let local = if store.is_some() {
        load_state(HarnessScope::Local, session_id, root, store).await?
    } else {
        HarnessState::default()
    };
    let global = load_state(HarnessScope::Global, session_id, root, store).await?;
    let entries = sorted_entries(
        local.entries.into_iter().chain(global.entries).collect(),
        MAX_ENTRIES,
    );
    let refinements = local
        .refinements
        .into_iter()
        .chain(global.refinements)
        .rev()
        .take(4)
        .collect::<Vec<_>>();
    if entries.is_empty() && refinements.is_empty() {
        return Ok(String::new());
    }
    let mut appendix = String::from(
        "\n\n## Continual harness state\nThe following entries are persistent state explicitly created by the agent or user. Use them as working context; update or remove stale entries through borg.harness.\n",
    );
    for entry in entries {
        let block = format!(
            "\n### [{}:{}] {}\n{}\n",
            entry.scope.label(),
            entry.id,
            entry.title,
            entry.content
        );
        if appendix.len() + block.len() > MAX_PROMPT_BYTES {
            break;
        }
        appendix.push_str(&block);
    }
    if !refinements.is_empty() && appendix.len() < MAX_PROMPT_BYTES {
        appendix.push_str("\nRecent harness refinement evidence:\n");
        for event in refinements {
            let line = format!(
                "- {}: {}; outcome: {}\n",
                event.trigger, event.evidence, event.outcome
            );
            if appendix.len() + line.len() > MAX_PROMPT_BYTES {
                break;
            }
            appendix.push_str(&line);
        }
    }
    Ok(appendix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SessionStore;
    use tempfile::tempdir;

    #[test]
    fn generated_ids_are_stable_and_scoped() {
        let state = HarnessState::default();
        let id = next_id(&state, HarnessKind::Memory, "Surf ramp contact");
        assert_eq!(id, "surf_ramp_contact");
        assert_eq!(HarnessKind::Memory.label(), "memory");
        assert_eq!(HarnessScope::Global.label(), "global");
    }

    #[test]
    fn invalid_ids_cannot_escape_the_harness_namespace() {
        assert!(validate_id("surf/memory").is_err());
        assert!(validate_id("surf-memory").is_ok());
    }

    #[tokio::test]
    async fn crud_is_persisted_and_injected_into_the_next_turn() {
        let directory = tempdir().unwrap();
        let store = crate::SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .unwrap();
        let session_id = Uuid::new_v4();
        store.create_session(session_id).await.unwrap();
        let lock = Arc::new(Mutex::new(()));

        let created = call(
            json!({
                "op": "create",
                "kind": "memory",
                "title": "Ramp contact",
                "content": "Preserve tangent velocity on the ramp",
                "scope": "local"
            }),
            session_id,
            directory.path(),
            Some(&store),
            &lock,
            true,
        )
        .await
        .unwrap();
        let id = created["entry"]["id"].as_str().unwrap();
        let appendix = prompt_appendix(session_id, directory.path(), Some(&store), &lock)
            .await
            .unwrap();
        assert!(appendix.contains("Preserve tangent velocity on the ramp"));

        call(
            json!({
                "op": "update",
                "kind": "memory",
                "id": id,
                "content": "Preserve tangent velocity and inspect contact normals",
                "scope": "local"
            }),
            session_id,
            directory.path(),
            Some(&store),
            &lock,
            true,
        )
        .await
        .unwrap();
        call(
            json!({
                "op": "refine",
                "trigger": "repeated ramp divergence",
                "changes": ["Added contact-normal inspection"],
                "evidence": "Trace diverged at tick 240",
                "outcome": "Next run records the first divergent contact",
                "scope": "local"
            }),
            session_id,
            directory.path(),
            Some(&store),
            &lock,
            true,
        )
        .await
        .unwrap();
        let overview = call(
            json!({"op": "overview"}),
            session_id,
            directory.path(),
            Some(&store),
            &lock,
            false,
        )
        .await
        .unwrap();
        assert_eq!(overview["counts"]["memory"], 1);
        assert_eq!(overview["scopes"]["local"]["refinements"], 1);
    }
}
