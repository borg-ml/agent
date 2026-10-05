use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail, ensure};
use borg_provider::provider::ModelMessage;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc};
use uuid::Uuid;

use crate::{CodingProvider, SessionEventKind};

#[derive(Clone, Default)]
pub(crate) struct ContextEditor {
    state: Arc<Mutex<ContextState>>,
    next_epoch: Arc<AtomicU64>,
    active_epoch: Arc<AtomicU64>,
}

#[derive(Default)]
struct ContextState {
    entries: Vec<Entry>,
    visible: usize,
    revision: u64,
    editing_tools: bool,
    // Index in the harness's unedited vector, not in the staged replacement.
    pending_anchor: Option<usize>,
    pending_system_override: Option<String>,
    events: Option<mpsc::WeakSender<SessionEventKind>>,
    provider: Option<CodingProvider>,
}

#[derive(Clone)]
struct Entry {
    id: Uuid,
    message: ModelMessage,
}

pub(crate) struct ActiveContext {
    epoch: u64,
    active: Arc<AtomicU64>,
}

impl Drop for ActiveContext {
    fn drop(&mut self) {
        let _ = self
            .active
            .compare_exchange(self.epoch, 0, Ordering::AcqRel, Ordering::Acquire);
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Args {
    op: String,
    #[serde(default)]
    offset: usize,
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default = "default_chars")]
    max_chars: usize,
    #[serde(default)]
    text_offset: usize,
    id: Option<Uuid>,
    revision: Option<String>,
    #[serde(default)]
    edits: Vec<Edit>,
}
fn default_limit() -> usize {
    20
}
fn default_chars() -> usize {
    4_000
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Edit {
    Replace {
        id: Uuid,
        text: Option<String>,
        message: Option<ModelMessage>,
    },
    Drop {
        ids: Vec<Uuid>,
    },
    Insert {
        before: Option<Uuid>,
        after: Option<Uuid>,
        role: Option<String>,
        text: Option<String>,
        message: Option<ModelMessage>,
    },
    Move {
        ids: Vec<Uuid>,
        before: Option<Uuid>,
        after: Option<Uuid>,
    },
}

impl ContextEditor {
    pub(crate) async fn begin(
        &self,
        messages: &[ModelMessage],
        provider: CodingProvider,
        events: &mpsc::Sender<SessionEventKind>,
    ) -> ActiveContext {
        let mut state = self.state.lock().await;
        let entries = reuse_entries(&state.entries, messages);
        *state = ContextState {
            entries,
            visible: messages.len(),
            events: Some(events.downgrade()),
            provider: Some(provider),
            ..Default::default()
        };
        let epoch = self.next_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        self.active_epoch.store(epoch, Ordering::Release);
        ActiveContext {
            epoch,
            active: self.active_epoch.clone(),
        }
    }

    // A tool may re-enter through exec/runtime_exec. Keep its view current,
    // but never let it rewrite the model response/tools still being executed.
    pub(crate) async fn publish(&self, messages: &[ModelMessage], request_boundary: bool) {
        let mut state = self.state.lock().await;
        if let Some(anchor) = state.pending_anchor {
            let mut entries = state.entries[..state.visible].to_vec();
            entries.extend(reuse_entries(
                &state.entries[state.visible..],
                &messages[anchor..],
            ));
            state.entries = entries;
        } else {
            if request_boundary
                && (messages.len() < state.visible
                    || state.entries[..state.visible]
                        .iter()
                        .zip(messages)
                        .any(|(entry, message)| entry.message != *message))
            {
                state.revision += 1;
            }
            state.entries = reuse_entries(&state.entries, messages);
            if request_boundary {
                state.visible = messages.len();
            }
        }
    }

    pub(crate) async fn open_tools(&self) {
        self.state.lock().await.editing_tools = true;
    }

    pub(crate) async fn close_tools(&self) {
        self.state.lock().await.editing_tools = false;
    }

    // None means no edit; Some(None) changes history without pinning the
    // dynamically rebuilt system; Some(Some(text)) explicitly overrides it.
    pub(crate) async fn apply_pending(
        &self,
        messages: &mut Vec<ModelMessage>,
    ) -> Option<Option<String>> {
        let mut state = self.state.lock().await;
        let anchor = state.pending_anchor.take()?;
        let mut edited = state.entries[..state.visible]
            .iter()
            .map(|entry| entry.message.clone())
            .collect::<Vec<_>>();
        edited.extend_from_slice(&messages[anchor..]);
        *messages = edited;
        state.entries = reuse_entries(&state.entries, messages);
        Some(state.pending_system_override.take())
    }

    pub(crate) async fn call(&self, mut arguments: Value, allow_edit: bool) -> Result<Value> {
        if let Some(arguments) = arguments.as_object_mut() {
            arguments.remove("action");
        }
        let args: Args = serde_json::from_value(arguments)?;
        let epoch = self.active_epoch.load(Ordering::Acquire);
        ensure!(
            epoch != 0,
            "context is available only during a Borg-owned model turn"
        );
        let mut state = self.state.lock().await;
        ensure!(
            self.active_epoch.load(Ordering::Acquire) == epoch,
            "context turn ended; read a fresh view"
        );
        let revision = format!("{epoch}:{}", state.revision);
        match args.op.as_str() {
            "read" => {
                ensure!(
                    args.limit > 0 && args.limit <= 100 && args.max_chars <= 64_000,
                    "limit must be 1..100 and max_chars 0..64000"
                );
                let entries = &state.entries[..state.visible];
                let groups = tool_groups(entries);
                let selected = entries
                    .iter()
                    .enumerate()
                    .filter(|(_, entry)| args.id.is_none_or(|id| id == entry.id))
                    .skip(args.offset)
                    .take(args.limit);
                let rows = selected.map(|(index, entry)| {
                    let (role, text) = message_text(&entry.message);
                    let text_length = text.chars().count();
                    let text_offset = args.text_offset.min(text_length);
                    let excerpt = text.chars().skip(text_offset).take(args.max_chars).collect::<String>();
                    let text_end = text_offset + excerpt.chars().count();
                    let (attachments, opaque, calls) = match &entry.message {
                        ModelMessage::User { attachments, .. } | ModelMessage::Tool { attachments, .. } => (attachments.len(), false, Vec::new()),
                        ModelMessage::Assistant { reasoning_details, provider_state, tool_calls, .. } => (0, reasoning_details.is_some() || provider_state.is_some(), tool_calls.iter().map(|call| json!({"id":call.id, "name":call.function.name})).collect()),
                        _ => (0, false, Vec::new()),
                    };
                    json!({"id":entry.id, "index":index, "role":role, "text":excerpt, "truncated":text_offset > 0 || text_end < text_length, "text_length":text_length, "text_offset":text_offset, "next_text_offset":if args.max_chars > 0 && text_end < text_length { Some(text_end) } else { None }, "group_id":groups.get(&entry.id), "attachments":attachments, "opaque_reasoning":opaque, "tool_calls":calls})
                }).collect::<Vec<_>>();
                let next = args.offset + rows.len();
                Ok(
                    json!({"revision":revision, "entries":rows, "message_count":entries.len(), "next_offset":if args.id.is_none() && next < entries.len() { Some(next) } else { None }, "locked_tail_messages":state.entries.len()-state.visible}),
                )
            }
            "edit" => {
                ensure!(
                    allow_edit,
                    "context editing requires Full Access or explicit approval"
                );
                ensure!(
                    state.editing_tools,
                    "submit context edits from a model tool batch, not while a request or compaction is in flight"
                );
                ensure!(
                    args.revision.as_deref() == Some(&revision),
                    "context revision changed; read a fresh view before editing"
                );
                ensure!(
                    !args.edits.is_empty() && args.edits.len() <= 100,
                    "edit batch must contain 1..100 operations"
                );
                let mut edited = state.entries[..state.visible].to_vec();
                for edit in &args.edits {
                    edit_entries(&mut edited, edit)?;
                }
                validate_entries(&edited, false)?;
                let mut canonical = edited
                    .iter()
                    .map(|entry| entry.message.clone())
                    .collect::<Vec<_>>();
                crate::native_harness::canonicalize_native_messages(&mut canonical);
                edited = reuse_entries(&edited, &canonical);
                let mut complete = edited.clone();
                complete.extend_from_slice(&state.entries[state.visible..]);
                // The protected batch may still await results, but its call
                // IDs must not collide with raw calls inserted into history.
                validate_entries(&complete, true)?;
                if state.entries.len() > state.visible {
                    ensure!(
                        !edited
                            .iter()
                            .rev()
                            .find(|entry| !matches!(entry.message, ModelMessage::System { .. }))
                            .is_some_and(|entry| matches!(
                                entry.message,
                                ModelMessage::Assistant { .. }
                            )),
                        "edited history must end with a user message or tool result so it cannot merge into the protected assistant response"
                    );
                }
                let ModelMessage::System {
                    content: system_prompt,
                } = &complete[0].message
                else {
                    unreachable!()
                };
                // Journaled results may be ahead of this runtime snapshot.
                // Replace only the prefix; replay takes the suffix from the journal.
                let system_changed = complete[0].message != state.entries[0].message;
                let system_override = system_changed.then(|| system_prompt.clone());
                let mut payload = json!({"messages":edited[1..].iter().map(|entry| &entry.message).collect::<Vec<_>>(), "preserve_tail_from":state.visible - 1, "operations":args.edits.len()});
                if let Some(system_prompt) = &system_override {
                    payload["system_prompt"] = json!(system_prompt);
                }
                ensure!(
                    serde_json::to_vec(&payload)?.len() <= 64 * 1024 * 1024,
                    "context checkpoint exceeds 64 MiB; remove unneeded attachments or split the edit"
                );
                let events = state
                    .events
                    .as_ref()
                    .and_then(mpsc::WeakSender::upgrade)
                    .context("context turn ended")?;
                events
                    .send(SessionEventKind::ProviderEvent {
                        provider: state.provider.context("context provider missing")?,
                        kind: crate::session::NATIVE_CONTEXT_EDIT_EVENT.into(),
                        payload,
                    })
                    .await
                    .context("record context edit")?;
                let visible = state.visible;
                if system_override.is_some() {
                    state.pending_system_override = system_override;
                }
                state.pending_anchor.get_or_insert(visible);
                state.visible = edited.len();
                state.entries = complete;
                state.revision += 1;
                Ok(
                    json!({"revision":format!("{epoch}:{}", state.revision), "operations":args.edits.len(), "applies":"next_model_request", "journal_preserved":true, "cache_prefix_changed":system_changed}),
                )
            }
            other => bail!("unknown context operation {other}; use read or edit"),
        }
    }
}

fn reuse_entries(previous: &[Entry], messages: &[ModelMessage]) -> Vec<Entry> {
    let mut used = HashSet::new();
    messages
        .iter()
        .enumerate()
        .map(|(index, message)| {
            let old = previous
                .get(index)
                .filter(|entry| entry.message == *message && !used.contains(&entry.id))
                .or_else(|| {
                    previous
                        .iter()
                        .find(|entry| entry.message == *message && !used.contains(&entry.id))
                });
            let id = old.map(|entry| entry.id).unwrap_or_else(Uuid::new_v4);
            used.insert(id);
            Entry {
                id,
                message: message.clone(),
            }
        })
        .collect()
}

fn message_text(message: &ModelMessage) -> (&'static str, &str) {
    match message {
        ModelMessage::System { content } => ("system", content),
        ModelMessage::User { content, .. } => ("user", content),
        ModelMessage::Assistant { content, .. } => {
            ("assistant", content.as_deref().unwrap_or_default())
        }
        ModelMessage::Tool { content, .. } => ("tool", content),
    }
}

fn tool_groups(entries: &[Entry]) -> HashMap<Uuid, Uuid> {
    let calls = entries
        .iter()
        .flat_map(|entry| match &entry.message {
            ModelMessage::Assistant { tool_calls, .. } => tool_calls
                .iter()
                .map(|call| (call.id.as_str(), entry.id))
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        })
        .collect::<HashMap<_, _>>();
    entries
        .iter()
        .filter_map(|entry| match &entry.message {
            ModelMessage::Assistant { tool_calls, .. } if !tool_calls.is_empty() => {
                Some((entry.id, entry.id))
            }
            ModelMessage::Tool { tool_call_id, .. } => calls
                .get(tool_call_id.as_str())
                .map(|group| (entry.id, *group)),
            _ => None,
        })
        .collect()
}

fn selected_ids(entries: &[Entry], ids: &[Uuid]) -> Result<HashSet<Uuid>> {
    ensure!(!ids.is_empty(), "ids must not be empty");
    let groups = tool_groups(entries);
    let mut selected = HashSet::new();
    for id in ids {
        ensure!(
            entries.iter().any(|entry| entry.id == *id),
            "unknown context message {id}; read a fresh view"
        );
        selected.insert(*id);
        if let Some(group) = groups.get(id) {
            selected.extend(
                groups
                    .iter()
                    .filter_map(|(member, candidate)| (candidate == group).then_some(*member)),
            );
        }
    }
    ensure!(
        !selected.contains(&entries[0].id),
        "the leading system slot cannot be removed or moved; replace its text (an empty string disables it)"
    );
    Ok(selected)
}

fn insertion_index(entries: &[Entry], before: Option<Uuid>, after: Option<Uuid>) -> Result<usize> {
    ensure!(
        before.is_none() || after.is_none(),
        "supply before or after, not both"
    );
    let position = match before.or(after) {
        Some(id) => {
            entries
                .iter()
                .position(|entry| entry.id == id)
                .with_context(|| format!("unknown insertion anchor {id}"))?
                + usize::from(after.is_some())
        }
        None => entries.len(),
    };
    ensure!(
        position > 0,
        "the leading system slot stays first; replace its text instead"
    );
    Ok(position)
}

fn edit_entries(entries: &mut Vec<Entry>, edit: &Edit) -> Result<()> {
    match edit {
        Edit::Replace { id, text, message } => {
            ensure!(
                text.is_some() != message.is_some(),
                "replace requires exactly one of text or message"
            );
            let entry = entries
                .iter_mut()
                .find(|entry| entry.id == *id)
                .with_context(|| format!("unknown context message {id}"))?;
            if let Some(message) = message {
                entry.message = message.clone();
            } else if let Some(text) = text {
                match &mut entry.message {
                    ModelMessage::System { content }
                    | ModelMessage::User { content, .. }
                    | ModelMessage::Tool { content, .. } => *content = text.clone(),
                    ModelMessage::Assistant {
                        content,
                        reasoning_content,
                        reasoning_details,
                        provider_state,
                        ..
                    } => {
                        *content = Some(text.clone());
                        // The old signed/opaque response no longer describes
                        // the edited assistant text. Untouched entries keep it.
                        *reasoning_content = None;
                        *reasoning_details = None;
                        *provider_state = None;
                    }
                }
            }
        }
        Edit::Drop { ids } => {
            let selected = selected_ids(entries, ids)?;
            entries.retain(|entry| !selected.contains(&entry.id));
        }
        Edit::Insert {
            before,
            after,
            role,
            text,
            message,
        } => {
            ensure!(
                text.is_some() != message.is_some(),
                "insert requires exactly one of text or message"
            );
            ensure!(
                message.is_none() || role.is_none(),
                "role applies only to text inserts"
            );
            let message = if let Some(message) = message {
                message.clone()
            } else {
                let text = text.clone().expect("validated text");
                match role.as_deref().unwrap_or("user") {
                    "system" => ModelMessage::System { content: text },
                    "user" => ModelMessage::user(text),
                    "assistant" => ModelMessage::assistant(Some(text), None, None, Vec::new()),
                    other => {
                        bail!("cannot insert text role {other}; use system, user or assistant")
                    }
                }
            };
            let position = insertion_index(entries, *before, *after)?;
            entries.insert(
                position,
                Entry {
                    id: Uuid::new_v4(),
                    message,
                },
            );
        }
        Edit::Move { ids, before, after } => {
            let selected = selected_ids(entries, ids)?;
            let moved = entries
                .iter()
                .filter(|entry| selected.contains(&entry.id))
                .cloned()
                .collect::<Vec<_>>();
            entries.retain(|entry| !selected.contains(&entry.id));
            let position = insertion_index(entries, *before, *after)?;
            entries.splice(position..position, moved);
        }
    }
    Ok(())
}

fn validate_entries(entries: &[Entry], allow_pending_tail: bool) -> Result<()> {
    ensure!(
        matches!(
            entries.first().map(|entry| &entry.message),
            Some(ModelMessage::System { .. })
        ),
        "context must start with its editable system slot"
    );
    if let Some(first) = entries
        .iter()
        .find(|entry| !matches!(entry.message, ModelMessage::System { .. }))
    {
        ensure!(
            matches!(first.message, ModelMessage::User { .. }),
            "context must begin with a user message after the system slots"
        );
    }
    let mut seen = HashSet::new();
    let mut pending = HashSet::new();
    let mut previous_assistant = false;
    for entry in entries {
        if let ModelMessage::Tool { tool_call_id, .. } = &entry.message {
            ensure!(
                pending.remove(tool_call_id),
                "orphan or duplicate tool result {tool_call_id}"
            );
            previous_assistant = false;
            continue;
        }
        ensure!(
            pending.is_empty(),
            "tool calls and all their results must stay adjacent; move or drop the complete group"
        );
        if matches!(entry.message, ModelMessage::System { .. }) {
            continue;
        }
        ensure!(
            !previous_assistant
                || !matches!(
                    &entry.message,
                    ModelMessage::Assistant {
                        provider_state: Some(
                            borg_core::model::ModelProviderState::AnthropicMessages { .. }
                        ),
                        ..
                    }
                ),
            "signed assistant reasoning cannot follow another assistant message without a user boundary"
        );
        previous_assistant = matches!(entry.message, ModelMessage::Assistant { .. });
        if let ModelMessage::Assistant { tool_calls, .. } = &entry.message {
            for call in tool_calls {
                ensure!(
                    !call.id.is_empty() && seen.insert(call.id.clone()),
                    "duplicate or empty tool call id {}",
                    call.id
                );
                ensure!(
                    call.kind == "function",
                    "unsupported tool call type {}",
                    call.kind
                );
                let _: Value = serde_json::from_str(&call.function.arguments)
                    .context("invalid tool call arguments")?;
                pending.insert(call.id.clone());
            }
        }
    }
    ensure!(
        allow_pending_tail || pending.is_empty(),
        "context contains tool calls without results"
    );
    Ok(())
}

pub(crate) fn tool_spec() -> Value {
    json!({
        "name":"context",
        "description":"Inspect and edit your own model context on Borg's loop. The harness manages context capacity and automatic compaction; do not trim or rewrite history to manage context size. Read returns stable message IDs and editable text, with opaque replay/images retained internally. Edit applies an atomic batch to the latest request's context while preserving the in-flight tool batch and newly appended messages. Supply the revision from read; edits require Full Access or approval. Replace system text, including an empty string; insert/drop/move history. Moving or dropping a tool call/result selects its whole group. Replacing assistant text clears its stale signed reasoning. A raw provider-neutral message is an optional escape hatch instead of text. Submit edits from a model tool batch; the API rejects edits during model requests or compaction. The append-only journal and tool permissions are unchanged. Edits are durable and affect the next model request; auto-compaction may subsequently reshape the view. Read again after restart or compaction. When editing history, preserve whether retained human requests were already answered or completed (keep the reply or a concise status summary), so old requests do not become new obligations.",
        "inputSchema":{
            "type":"object", "properties":{
                "op":{"type":"string","enum":["read","edit"]},
                "offset":{"type":"integer","minimum":0},
                "limit":{"type":"integer","minimum":1,"maximum":100},
                "max_chars":{"type":"integer","minimum":0,"maximum":64000},
                "text_offset":{"type":"integer","minimum":0,"description":"Character offset within message text; use id to page a long message."},
                "id":{"type":"string","format":"uuid"},
                "revision":{"type":"string"},
                "edits":{"type":"array","minItems":1,"maxItems":100,"items":{
                    "type":"object","properties":{
                        "op":{"type":"string","enum":["replace","drop","insert","move"]},
                        "id":{"type":"string","format":"uuid"},
                        "ids":{"type":"array","items":{"type":"string","format":"uuid"}},
                        "before":{"type":"string","format":"uuid"},
                        "after":{"type":"string","format":"uuid"},
                        "role":{"type":"string","enum":["system","user","assistant"]},
                        "text":{"type":"string"}, "message":{"type":"object"}
                    },"required":["op"],"additionalProperties":false
                }}
            },"required":["op"],"additionalProperties":false
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SessionEvent, session_store::EventPersistence};
    use borg_core::model::ModelProviderState;
    use borg_provider::provider::ModelToolCall;

    fn call(id: &str, name: &str) -> ModelMessage {
        ModelMessage::assistant(
            None,
            None,
            None,
            vec![ModelToolCall::function(id.into(), name.into(), "{}".into())],
        )
    }

    // Context edits are a replay/persistence contract, not just string edits:
    // a lagging tool snapshot must preserve already-journaled results and steers.
    #[tokio::test]
    async fn text_edit_preserves_opaque_groups_and_the_journaled_inflight_tail() {
        let editor = ContextEditor::default();
        let (tx, mut rx) = mpsc::channel(16);
        let mut old_call = call("old", "read_file");
        if let ModelMessage::Assistant { provider_state, .. } = &mut old_call {
            *provider_state = Some(ModelProviderState::AnthropicMessages {
                content: vec![
                    json!({"type":"thinking", "thinking":"kept", "signature":"opaque-test-signature"}),
                ],
                account_identity: None,
            });
        }
        let prefix = vec![
            ModelMessage::System {
                content: "original system".into(),
            },
            ModelMessage::user("old context"),
            old_call.clone(),
            ModelMessage::tool("old", "evidence"),
            ModelMessage::user("latest request"),
        ];
        let _active = editor.begin(&prefix, CodingProvider::OpenRouter, &tx).await;
        let view = editor.call(json!({"op":"read"}), false).await.unwrap();
        let user_id = view["entries"][1]["id"].clone();
        let result_id = view["entries"][3]["id"].clone();
        let latest_id = view["entries"][4]["id"].clone();
        let mut live = prefix.clone();
        live.push(ModelMessage::assistant(
            None,
            None,
            None,
            vec![
                ModelToolCall::function("finished".into(), "exec".into(), "{}".into()),
                ModelToolCall::function("context".into(), "context".into(), "{}".into()),
            ],
        ));
        editor.publish(&live, false).await;
        editor.open_tools().await;
        editor
            .call(
                json!({"op":"edit", "revision":view["revision"], "edits":[
                    {"op":"replace", "id":user_id, "text":"curated context"},
                    {"op":"move", "ids":[result_id], "after":latest_id}
                ]}),
                true,
            )
            .await
            .unwrap();
        let marker = rx.recv().await.unwrap();
        assert_eq!(marker.persistence(), EventPersistence::Durable);
        assert!(marker.is_context_relevant());
        // This result reached the actor before the edit; the editor had not
        // published it yet. The prefix marker must retain it from the journal.
        live.push(ModelMessage::tool("finished", "side effect completed once"));
        let mut events = live[1..]
            .iter()
            .map(|message| SessionEventKind::ProviderEvent {
                provider: CodingProvider::OpenRouter,
                kind: "native_model_message".into(),
                payload: serde_json::to_value(message).unwrap(),
            })
            .collect::<Vec<_>>();
        events.push(marker);
        let own_result = ModelMessage::tool("context", "edit accepted");
        let steer = ModelMessage::user("new human steer");
        for message in [&own_result, &steer] {
            events.push(SessionEventKind::ProviderEvent {
                provider: CodingProvider::OpenRouter,
                kind: "native_model_message".into(),
                payload: serde_json::to_value(message).unwrap(),
            });
        }
        events.push(SessionEventKind::ProviderEvent {
            provider: CodingProvider::OpenRouter,
            kind: "native_tool_round_completed".into(),
            payload: json!({"round":1}),
        });
        let session_id = Uuid::new_v4();
        let journal = events
            .into_iter()
            .enumerate()
            .map(|(index, kind)| SessionEvent::new(session_id, index as u64 + 1, kind))
            .collect::<Vec<_>>();
        live.extend([own_result, steer]);
        editor.close_tools().await;
        assert!(editor.apply_pending(&mut live).await.is_some());
        assert_eq!(
            live[1],
            ModelMessage::user("curated context\nlatest request")
        );
        assert_eq!(live[2], old_call);
        assert_eq!(
            crate::session::native_conversation(&journal, CodingProvider::OpenRouter).unwrap(),
            live[1..]
        );
        assert_eq!(crate::session::native_system_override(journal.iter()), None);
    }

    #[tokio::test]
    async fn long_message_text_can_be_paged_without_splitting_unicode() {
        let editor = ContextEditor::default();
        let (tx, _rx) = mpsc::channel(16);
        let messages = vec![ModelMessage::System {
            content: "🧩é文abc".into(),
        }];
        let _active = editor
            .begin(&messages, CodingProvider::OpenRouter, &tx)
            .await;
        let first = editor
            .call(json!({"op":"read", "max_chars":2}), false)
            .await
            .unwrap();
        assert_eq!(first["entries"][0]["text"], "🧩é");
        assert_eq!(first["entries"][0]["next_text_offset"], 2);
        let next = editor.call(json!({"op":"read", "id":first["entries"][0]["id"], "text_offset":2, "max_chars":4}), false).await.unwrap();
        assert_eq!(next["entries"][0]["text"], "文abc");
        assert!(next["entries"][0]["next_text_offset"].is_null());
        assert_eq!(next["entries"][0]["text_length"], 6);
        assert_eq!(next["revision"], first["revision"]);
    }

    #[tokio::test]
    async fn leading_system_can_be_empty_and_closed_phases_refuse_edits() {
        let editor = ContextEditor::default();
        let (tx, _rx) = mpsc::channel(16);
        let mut messages = vec![
            ModelMessage::System {
                content: "old".into(),
            },
            ModelMessage::user("request"),
        ];
        let _active = editor
            .begin(&messages, CodingProvider::OpenRouter, &tx)
            .await;
        editor.open_tools().await;
        let view = editor.call(json!({"op":"read"}), false).await.unwrap();
        editor.call(json!({"op":"edit", "revision":view["revision"], "edits":[{"op":"replace", "id":view["entries"][0]["id"], "text":""}]}), true).await.unwrap();
        assert!(editor.apply_pending(&mut messages).await.is_some());
        assert_eq!(
            messages[0],
            ModelMessage::System {
                content: String::new()
            }
        );
        editor.close_tools().await;
        let view = editor.call(json!({"op":"read"}), false).await.unwrap();
        assert!(editor.call(json!({"op":"edit", "revision":view["revision"], "edits":[{"op":"drop", "ids":[view["entries"][1]["id"]]}]}), true).await.is_err());
    }

    #[test]
    fn assistant_text_replacement_clears_stale_signed_reasoning() {
        let id = Uuid::new_v4();
        let mut message = ModelMessage::assistant(
            Some("old".into()),
            Some("old reasoning".into()),
            Some(json!([{"signature":"old"}])),
            Vec::new(),
        );
        if let ModelMessage::Assistant { provider_state, .. } = &mut message {
            *provider_state = Some(ModelProviderState::AnthropicMessages {
                content: vec![json!({"type":"thinking", "signature":"old"})],
                account_identity: None,
            });
        }
        let mut entries = vec![Entry { id, message }];
        edit_entries(
            &mut entries,
            &Edit::Replace {
                id,
                text: Some("new".into()),
                message: None,
            },
        )
        .unwrap();
        assert_eq!(
            entries[0].message,
            ModelMessage::assistant(Some("new".into()), None, None, Vec::new())
        );
    }

    #[tokio::test]
    async fn history_only_edits_do_not_pin_the_dynamic_system_prompt() {
        let editor = ContextEditor::default();
        let (tx, mut rx) = mpsc::channel(16);
        let mut messages = vec![
            ModelMessage::System {
                content: "dynamic instructions".into(),
            },
            ModelMessage::user("old"),
        ];
        let _active = editor
            .begin(&messages, CodingProvider::OpenRouter, &tx)
            .await;
        editor.open_tools().await;
        let view = editor.call(json!({"op":"read"}), false).await.unwrap();
        editor.call(json!({"op":"edit", "revision":view["revision"], "edits":[{"op":"replace", "id":view["entries"][1]["id"], "text":"new"}]}), true).await.unwrap();
        let SessionEventKind::ProviderEvent { payload, .. } = rx.recv().await.unwrap() else {
            panic!("expected edit marker")
        };
        assert!(payload.get("system_prompt").is_none());
        assert_eq!(editor.apply_pending(&mut messages).await, Some(None));
    }

    #[tokio::test]
    async fn edits_cannot_merge_into_the_protected_assistant_or_remove_the_user_boundary() {
        let editor = ContextEditor::default();
        let (tx, mut rx) = mpsc::channel(16);
        let prefix = vec![
            ModelMessage::System {
                content: "instructions".into(),
            },
            ModelMessage::user("request"),
        ];
        let _active = editor.begin(&prefix, CodingProvider::Claude, &tx).await;
        let view = editor.call(json!({"op":"read"}), false).await.unwrap();
        let mut live = prefix;
        live.push(call("current", "context"));
        editor.publish(&live, false).await;
        editor.open_tools().await;
        for edit in [
            json!({"op":"insert", "role":"assistant", "text":"would precede signed thinking"}),
            json!({"op":"drop", "ids":[view["entries"][1]["id"]]}),
        ] {
            assert!(
                editor
                    .call(
                        json!({"op":"edit", "revision":view["revision"], "edits":[edit]}),
                        true
                    )
                    .await
                    .is_err()
            );
        }
        assert!(rx.try_recv().is_err());
        assert_eq!(
            editor.call(json!({"op":"read"}), false).await.unwrap()["entries"],
            view["entries"]
        );
    }

    #[tokio::test]
    async fn raw_edits_cannot_duplicate_protected_tool_call_ids() {
        let editor = ContextEditor::default();
        let (tx, mut rx) = mpsc::channel(16);
        let prefix = vec![
            ModelMessage::System {
                content: "instructions".into(),
            },
            ModelMessage::user("request"),
        ];
        let _active = editor.begin(&prefix, CodingProvider::OpenRouter, &tx).await;
        let view = editor.call(json!({"op":"read"}), false).await.unwrap();
        let mut live = prefix;
        live.push(call("active", "context"));
        editor.publish(&live, false).await;
        editor.open_tools().await;
        let error = editor.call(json!({"op":"edit", "revision":view["revision"], "edits":[
            {"op":"insert", "message":call("active", "read_file")},
            {"op":"insert", "message":ModelMessage::tool("active", "invented historical result")}
        ]}), true).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("duplicate or empty tool call id active")
        );
        let unchanged = editor.call(json!({"op":"read"}), false).await.unwrap();
        assert_eq!(unchanged["entries"], view["entries"]);
        assert_eq!(unchanged["revision"], view["revision"]);
        assert!(rx.try_recv().is_err());
        assert!(editor.apply_pending(&mut live).await.is_none());
    }

    #[tokio::test]
    async fn edits_are_atomic_permission_checked_and_scoped_to_a_live_tool_batch() {
        let editor = ContextEditor::default();
        let (tx, mut rx) = mpsc::channel(16);
        let prefix = vec![
            ModelMessage::System {
                content: "instructions".into(),
            },
            ModelMessage::user("request"),
        ];
        let active = editor.begin(&prefix, CodingProvider::OpenRouter, &tx).await;
        let view = editor.call(json!({"op":"read"}), false).await.unwrap();
        let edits = json!({"op":"edit", "revision":view["revision"], "edits":[{"op":"replace", "id":view["entries"][0]["id"], "text":"new instructions"}]});
        assert!(editor.call(edits.clone(), true).await.is_err());
        editor.open_tools().await;
        assert!(editor.call(edits.clone(), false).await.is_err());
        let mut invalid = edits.clone();
        invalid["edits"]
            .as_array_mut()
            .unwrap()
            .push(json!({"op":"drop", "ids":[Uuid::new_v4()]}));
        assert!(editor.call(invalid, true).await.is_err());
        assert_eq!(
            editor.call(json!({"op":"read"}), false).await.unwrap()["entries"],
            view["entries"]
        );
        assert!(rx.try_recv().is_err());
        editor.call(edits.clone(), true).await.unwrap();
        assert!(editor.call(edits, true).await.is_err());
        drop(active);
        assert!(editor.call(json!({"op":"read"}), false).await.is_err());
    }
}
