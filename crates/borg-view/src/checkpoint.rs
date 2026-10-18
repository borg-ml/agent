use crate::{
    ViewReducer,
    core::TranscriptCore,
    entry::TranscriptEntry,
    wire::{Cursor, SNAPSHOT_BYTES},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointTextField {
    Message,
    ToolInput,
    ToolOutput,
    ActionBody,
    Activity,
    Information,
    Compaction,
    Queue,
    HeldTail,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckpointTextRef {
    pub row_id: String,
    pub revision: String,
    pub field: CheckpointTextField,
    pub bytes: usize,
}

#[derive(Serialize)]
struct Save<'a> {
    version: u32,
    core: &'a TranscriptCore,
    cursor: &'a Cursor,
    session_id: &'a str,
    row_revisions: &'a HashMap<String, u64>,
    text_offsets: &'a HashMap<String, usize>,
    history_positions: &'a HashMap<String, String>,
    group_membership: &'a HashMap<String, String>,
    group_totals: &'a HashMap<String, usize>,
    texts: Vec<CheckpointTextRef>,
}
#[derive(Deserialize)]
struct Saved {
    version: u32,
    core: TranscriptCore,
    cursor: Cursor,
    session_id: String,
    row_revisions: HashMap<String, u64>,
    text_offsets: HashMap<String, usize>,
    history_positions: HashMap<String, String>,
    group_membership: HashMap<String, String>,
    group_totals: HashMap<String, usize>,
    texts: Vec<CheckpointTextRef>,
}

fn read_checkpoint(bytes: &[u8]) -> Result<Saved> {
    let mut value: serde_json::Value = serde_json::from_slice(bytes)?;
    let version = value.get("version").and_then(serde_json::Value::as_u64);
    ensure!(matches!(version, Some(1 | 2)), "unknown checkpoint version");
    if version == Some(1) {
        let child_owners = value
            .pointer("/core/subagent_entries")
            .and_then(serde_json::Value::as_object)
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(|(child, index)| {
                        index.as_u64().map(|index| (index as usize, child.clone()))
                    })
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();
        let entries = value
            .pointer_mut("/core/order/entries")
            .and_then(serde_json::Value::as_array_mut)
            .context("checkpoint has no transcript entries")?;
        for (index, entry) in entries.iter_mut().enumerate() {
            for kind in ["Action", "Compaction", "Message"] {
                let Some(row) = entry
                    .get_mut(kind)
                    .and_then(serde_json::Value::as_object_mut)
                else {
                    continue;
                };
                let started_at = row
                    .get("time")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|time| chrono::DateTime::parse_from_rfc3339(time).ok())
                    .map(|time| time.with_timezone(&chrono::Utc))
                    .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
                let state = if kind == "Action" {
                    row.insert(
                        "source_session_id".into(),
                        serde_json::to_value(child_owners.get(&index))?,
                    );
                    row.remove("state").context("legacy action has no state")?
                } else if kind == "Message" {
                    let complete = row
                        .remove("complete")
                        .and_then(|value| value.as_bool())
                        .context("legacy message has no completion state")?;
                    let interrupted = row
                        .remove("user_interrupted")
                        .and_then(|value| value.as_bool())
                        .unwrap_or(false);
                    let status = row
                        .get("status")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("");
                    serde_json::json!(if interrupted {
                        "Stopped"
                    } else if complete && status.eq_ignore_ascii_case("failed") {
                        "Failed"
                    } else if complete {
                        "Complete"
                    } else if status.eq_ignore_ascii_case("queued") {
                        "Waiting"
                    } else {
                        "Running"
                    })
                } else {
                    let complete = row
                        .remove("complete")
                        .and_then(|value| value.as_bool())
                        .context("legacy compaction has no completion state")?;
                    serde_json::json!(if complete { "Complete" } else { "Running" })
                };
                let state: crate::entry::ExecutionState = serde_json::from_value(state)?;
                row.insert(
                    "execution".into(),
                    serde_json::to_value(crate::entry::Execution::new(started_at, state))?,
                );
            }
            let Some(tool) = entry
                .get_mut("Tool")
                .and_then(serde_json::Value::as_object_mut)
            else {
                continue;
            };
            let flag = |name: &str| {
                tool.get(name)
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false)
            };
            let failed_outcome = tool
                .get("outcome")
                .and_then(serde_json::Value::as_str)
                .is_some_and(crate::semantics::outcome_is_failure);
            let state = if flag("error") || failed_outcome {
                "Failed"
            } else if flag("user_interrupted") {
                "Stopped"
            } else if flag("backgrounded") {
                "Backgrounded"
            } else if flag("complete") {
                "Complete"
            } else {
                "Running"
            };
            let started_at = tool
                .remove("started_at")
                .context("legacy tool has no start time")?;
            let ended_at = tool
                .remove("completed_at")
                .unwrap_or(serde_json::Value::Null);
            let completed_at = if matches!(state, "Failed" | "Stopped" | "Complete") {
                if ended_at.is_null() {
                    started_at.clone()
                } else {
                    ended_at
                }
            } else {
                serde_json::Value::Null
            };
            for name in ["complete", "error", "user_interrupted", "backgrounded"] {
                tool.remove(name);
            }
            tool.insert(
                "execution".into(),
                serde_json::json!({
                    "state": state, "started_at": started_at, "completed_at": completed_at,
                }),
            );
        }
        value["version"] = serde_json::json!(2);
    }
    let saved: Saved = serde_json::from_value(value)?;
    ensure!(saved.version == 2, "unknown checkpoint version");
    Ok(saved)
}

impl ViewReducer {
    pub fn checkpoint_text_refs(bytes: &[u8]) -> Result<Vec<CheckpointTextRef>> {
        ensure!(
            bytes.len() <= SNAPSHOT_BYTES,
            "checkpoint exceeds byte budget"
        );
        let saved = read_checkpoint(bytes)?;
        Ok(saved.texts)
    }

    /// Borrow text without copying: a host compares stored byte offsets and persists only new suffixes.
    pub fn checkpoint_texts<'a>(&'a self) -> Vec<(CheckpointTextRef, &'a str)> {
        use CheckpointTextField::*;
        let mut texts = Vec::new();
        for (index, entry) in self.core.order.entries.iter().enumerate() {
            let (event_id, ordinal) = self.core.order.identities[index];
            let row_id = format!("{event_id}:{ordinal}");
            let revision = self
                .row_revisions
                .get(&row_id)
                .copied()
                .unwrap_or(1)
                .to_string();
            let mut add = |field, text: &'a str| {
                if !text.is_empty() {
                    texts.push((
                        CheckpointTextRef {
                            row_id: row_id.clone(),
                            revision: revision.clone(),
                            field,
                            bytes: text.len(),
                        },
                        text,
                    ));
                }
            };
            match entry {
                TranscriptEntry::Message { text, .. } => add(Message, text),
                TranscriptEntry::Tool {
                    code_view,
                    output_view,
                    ..
                } => {
                    if let Some((_, text)) = code_view {
                        add(ToolInput, text)
                    }
                    if let Some((_, text)) = output_view {
                        add(ToolOutput, text)
                    }
                }
                TranscriptEntry::Action {
                    body: Some(text), ..
                } => add(ActionBody, text),
                TranscriptEntry::Activity { text, .. } => add(Activity, text),
                TranscriptEntry::Info { text, .. } => add(Information, text),
                TranscriptEntry::Compaction { summary, .. } => add(Compaction, summary),
                _ => {}
            }
        }
        for item in &self.core.meta.queue {
            texts.push((
                CheckpointTextRef {
                    row_id: item.message_id.clone(),
                    revision: "0".into(),
                    field: Queue,
                    bytes: item.text.len(),
                },
                item.text.as_str(),
            ));
        }
        for (id, text) in &self.core.streaming_tails {
            texts.push((
                CheckpointTextRef {
                    row_id: id.to_string(),
                    revision: "0".into(),
                    field: HeldTail,
                    bytes: text.len(),
                },
                text.as_str(),
            ));
        }
        texts
    }

    /// Constant-size active state: transcript bodies remain in normalized host storage.
    pub fn checkpoint(&mut self) -> Result<Vec<u8>> {
        let mut texts = Vec::new();
        for (index, entry) in self.core.order.entries.iter_mut().enumerate() {
            let (event_id, ordinal) = self.core.order.identities[index];
            let id = format!("{event_id}:{ordinal}");
            let revision = self
                .row_revisions
                .get(&id)
                .copied()
                .unwrap_or(1)
                .to_string();
            let mut take = |field, value: &mut String| {
                if !value.is_empty() {
                    texts.push((
                        CheckpointTextRef {
                            row_id: id.clone(),
                            revision: revision.clone(),
                            field,
                            bytes: value.len(),
                        },
                        std::mem::take(value),
                    ));
                }
            };
            match entry {
                TranscriptEntry::Message { text, .. } => take(CheckpointTextField::Message, text),
                TranscriptEntry::Tool {
                    code_view,
                    output_view,
                    ..
                } => {
                    if let Some((_, text)) = code_view {
                        take(CheckpointTextField::ToolInput, text)
                    }
                    if let Some((_, text)) = output_view {
                        take(CheckpointTextField::ToolOutput, text)
                    }
                }
                TranscriptEntry::Action {
                    body: Some(text), ..
                } => take(CheckpointTextField::ActionBody, text),
                TranscriptEntry::Activity { text, .. } => take(CheckpointTextField::Activity, text),
                TranscriptEntry::Info { text, .. } => take(CheckpointTextField::Information, text),
                TranscriptEntry::Compaction { summary, .. } => {
                    take(CheckpointTextField::Compaction, summary)
                }
                _ => {}
            }
        }
        for item in &mut self.core.meta.queue {
            let text = std::mem::take(&mut item.text);
            texts.push((
                CheckpointTextRef {
                    row_id: item.message_id.clone(),
                    revision: "0".into(),
                    field: CheckpointTextField::Queue,
                    bytes: text.len(),
                },
                text,
            ));
        }
        for (message_id, text) in &mut self.core.streaming_tails {
            let value = std::mem::take(text);
            texts.push((
                CheckpointTextRef {
                    row_id: message_id.to_string(),
                    revision: "0".into(),
                    field: CheckpointTextField::HeldTail,
                    bytes: value.len(),
                },
                value,
            ));
        }
        let encoded = serde_json::to_vec(&Save {
            version: 2,
            core: &self.core,
            cursor: &self.cursor,
            session_id: &self.session_id,
            row_revisions: &self.row_revisions,
            text_offsets: &self.text_offsets,
            history_positions: &self.history_positions,
            group_membership: &self.group_membership,
            group_totals: &self.group_totals,
            texts: texts
                .iter()
                .map(|(reference, _)| reference.clone())
                .collect(),
        });
        // Put owned text back even when serialization fails; checkpointing is not a semantic mutation.
        for (reference, text) in texts {
            self.hydrate_text(&reference, text)?;
        }
        let encoded = encoded?;
        ensure!(
            encoded.len() <= SNAPSHOT_BYTES,
            "active checkpoint exceeds byte budget"
        );
        Ok(encoded)
    }

    pub fn restore(
        bytes: &[u8],
        mut resolve: impl FnMut(&CheckpointTextRef) -> Result<String>,
    ) -> Result<Self> {
        ensure!(
            bytes.len() <= SNAPSHOT_BYTES,
            "checkpoint exceeds byte budget"
        );
        let saved = read_checkpoint(bytes)?;
        ensure!(
            saved.cursor.revision.parse::<u64>().is_ok(),
            "invalid checkpoint cursor"
        );
        ensure!(
            saved.core.order.entries.len() == saved.core.order.identities.len(),
            "invalid checkpoint identities"
        );
        let mut reducer = Self {
            core: saved.core,
            cursor: saved.cursor,
            session_id: saved.session_id,
            row_revisions: saved.row_revisions,
            text_offsets: saved.text_offsets,
            history_positions: saved.history_positions,
            group_membership: saved.group_membership,
            group_totals: saved.group_totals,
        };
        for reference in saved.texts {
            let text = resolve(&reference)?;
            ensure!(
                text.len() == reference.bytes,
                "checkpoint text byte length mismatch"
            );
            reducer.hydrate_text(&reference, text)?;
        }
        Ok(reducer)
    }

    fn hydrate_text(&mut self, reference: &CheckpointTextRef, text: String) -> Result<()> {
        use CheckpointTextField::*;
        if reference.field == Queue {
            let item = self
                .core
                .meta
                .queue
                .iter_mut()
                .find(|item| item.message_id == reference.row_id)
                .context("missing queued text reference")?;
            item.text = text;
            return Ok(());
        }
        if reference.field == HeldTail {
            let id = uuid::Uuid::parse_str(&reference.row_id)?;
            *self
                .core
                .streaming_tails
                .get_mut(&id)
                .context("missing held text reference")? = text;
            return Ok(());
        }
        let index = self
            .core
            .order
            .identities
            .iter()
            .position(|(id, ordinal)| format!("{id}:{ordinal}") == reference.row_id)
            .context("missing row text reference")?;
        ensure!(
            self.row_revisions
                .get(&reference.row_id)
                .copied()
                .unwrap_or(1)
                .to_string()
                == reference.revision,
            "checkpoint row revision mismatch"
        );
        let value = match (&mut self.core.order.entries[index], &reference.field) {
            (TranscriptEntry::Message { text, .. }, Message) => text,
            (
                TranscriptEntry::Tool {
                    code_view: Some((_, text)),
                    ..
                },
                ToolInput,
            ) => text,
            (
                TranscriptEntry::Tool {
                    output_view: Some((_, text)),
                    ..
                },
                ToolOutput,
            ) => text,
            (
                TranscriptEntry::Action {
                    body: Some(text), ..
                },
                ActionBody,
            ) => text,
            (TranscriptEntry::Activity { text, .. }, Activity) => text,
            (TranscriptEntry::Info { text, .. }, Information) => text,
            (TranscriptEntry::Compaction { summary, .. }, Compaction) => summary,
            _ => anyhow::bail!("checkpoint text field does not match row kind"),
        };
        *value = text;
        Ok(())
    }
}

#[cfg(test)]
mod lifecycle_migration_tests {
    use super::*;
    use crate::entry::ExecutionState;
    use chrono::Utc;
    use uuid::Uuid;

    // Existing saved sessions must retain terminal clocks and live background work
    // through the schema migration; type checking cannot validate legacy JSON.
    #[test]
    fn legacy_tool_checkpoints_restore_execution_state_and_clock() {
        for (complete, error, interrupted, backgrounded, outcome, expected) in [
            (false, false, false, false, None, ExecutionState::Running),
            (true, false, false, false, None, ExecutionState::Complete),
            (true, false, false, true, None, ExecutionState::Backgrounded),
            (true, true, false, true, None, ExecutionState::Failed),
            (true, false, true, false, None, ExecutionState::Stopped),
            (
                true,
                false,
                false,
                false,
                Some("exit 1"),
                ExecutionState::Failed,
            ),
        ] {
            let mut reducer = ViewReducer::new(Uuid::new_v4(), "epoch".into());
            let mut value: serde_json::Value =
                serde_json::from_slice(&reducer.checkpoint().unwrap()).unwrap();
            let start = Utc::now();
            let end = start + chrono::Duration::seconds(5);
            value["version"] = serde_json::json!(1);
            value["core"]["order"]["entries"] = serde_json::json!([{"Tool": {
                "source_name": "exec", "name": "Run", "detail": "", "code_view": null,
                "output_view": null, "payload_refs": [], "time": start.to_rfc3339(),
                "started_at": start, "completed_at": end, "complete": complete,
                "error": error, "user_interrupted": interrupted, "backgrounded": backgrounded,
                "expanded": false, "outcome": outcome, "cwd": null
            }}]);
            value["core"]["order"]["identities"] = serde_json::json!([[Uuid::new_v4(), 0]]);
            let bytes = serde_json::to_vec(&value).unwrap();
            let restored = ViewReducer::restore(&bytes, |_| unreachable!()).unwrap();
            let TranscriptEntry::Tool { execution, .. } = &restored.core.order[0] else {
                panic!("missing tool");
            };
            assert_eq!(execution.state(), expected);
            assert_eq!(execution.started_at(), start);
            assert_eq!(
                execution.completed_at(),
                expected.is_terminal().then_some(end)
            );
        }
    }
    #[test]
    fn legacy_non_tool_checkpoints_restore_execution_state() {
        let time = Utc::now();
        for (row, expected) in [
            (
                serde_json::json!({"Action": {"kind":"Approval", "label":"Approval", "detail":"", "body":null, "time":time.to_rfc3339(), "state":"Waiting", "expanded":false}}),
                ExecutionState::Waiting,
            ),
            (
                serde_json::json!({"Action": {"kind":"Agent", "label":"Agent", "detail":"", "body":null, "time":time.to_rfc3339(), "state":"Complete", "expanded":false}}),
                ExecutionState::Complete,
            ),
            (
                serde_json::json!({"Compaction": {"summary":"", "time":time.to_rfc3339(), "sequence":1, "complete":true, "expanded":false}}),
                ExecutionState::Complete,
            ),
            (
                serde_json::json!({"Message": {"actor":"assistant", "text":"partial", "attachments":[], "model":null, "effort":null, "time":time.to_rfc3339(), "status":"failed", "complete":false, "user_interrupted":false, "redirected":false}}),
                ExecutionState::Running,
            ),
            (
                serde_json::json!({"Message": {"actor":"assistant", "text":"done", "attachments":[], "model":null, "effort":null, "time":time.to_rfc3339(), "status":"complete", "complete":true, "user_interrupted":false, "redirected":false}}),
                ExecutionState::Complete,
            ),
        ] {
            let mut reducer = ViewReducer::new(Uuid::new_v4(), "epoch".into());
            let mut value: serde_json::Value =
                serde_json::from_slice(&reducer.checkpoint().unwrap()).unwrap();
            let owner = (row
                .pointer("/Action/kind")
                .and_then(serde_json::Value::as_str)
                == Some("Agent"))
            .then(Uuid::new_v4);
            if let Some(owner) = owner {
                value["core"]["subagent_entries"] = serde_json::json!({(owner.to_string()):0});
            }
            value["version"] = serde_json::json!(1);
            value["core"]["order"]["entries"] = serde_json::json!([row]);
            value["core"]["order"]["identities"] = serde_json::json!([[Uuid::new_v4(), 0]]);
            let restored =
                ViewReducer::restore(&serde_json::to_vec(&value).unwrap(), |_| unreachable!())
                    .unwrap();
            let (TranscriptEntry::Action { execution, .. }
            | TranscriptEntry::Compaction { execution, .. }
            | TranscriptEntry::Message { execution, .. }) = &restored.core.order[0]
            else {
                panic!("missing action");
            };
            if let TranscriptEntry::Action {
                source_session_id, ..
            } = &restored.core.order[0]
            {
                assert_eq!(*source_session_id, owner);
            }
            assert_eq!(execution.state(), expected);
            assert_eq!(execution.started_at(), time);
            assert_eq!(
                execution.completed_at(),
                expected.is_terminal().then_some(time)
            );
        }
    }
}
