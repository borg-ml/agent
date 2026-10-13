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

impl ViewReducer {
    pub fn checkpoint_text_refs(bytes: &[u8]) -> Result<Vec<CheckpointTextRef>> {
        ensure!(
            bytes.len() <= SNAPSHOT_BYTES,
            "checkpoint exceeds byte budget"
        );
        let saved: Saved = serde_json::from_slice(bytes)?;
        ensure!(saved.version == 1, "unknown checkpoint version");
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
            version: 1,
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
        let saved: Saved = serde_json::from_slice(bytes)?;
        ensure!(saved.version == 1, "unknown checkpoint version");
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
