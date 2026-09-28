use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use uuid::Uuid;

/// Unsaved composer text for one session.
///
/// A draft outlives the process that typed it: quitting mid-sentence and
/// resuming the same session has to bring the prompt back, so the text is
/// mirrored to a sidecar file instead of living only in the terminal.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ComposerDraft {
    pub text: String,
    pub attachments: Vec<PathBuf>,
}

impl ComposerDraft {
    pub fn is_empty(&self) -> bool {
        self.text.is_empty() && self.attachments.is_empty()
    }

    fn to_value(&self) -> serde_json::Value {
        serde_json::json!({
            "text": self.text,
            "attachments": self.attachments,
        })
    }

    fn from_value(value: serde_json::Value) -> Option<Self> {
        let text = value.get("text")?.as_str()?.to_string();
        let attachments = value
            .get("attachments")
            .and_then(serde_json::Value::as_array)
            .map(|paths| {
                paths
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(PathBuf::from)
                    .collect()
            })
            .unwrap_or_default();
        Some(Self { text, attachments })
    }
}

/// Per-session draft sidecar, kept beside the other session-local files.
#[derive(Clone, Debug)]
pub struct ComposerDraftStore {
    path: PathBuf,
}

impl ComposerDraftStore {
    pub fn for_session(sessions_dir: &Path, session_id: Uuid) -> Self {
        Self {
            path: sessions_dir.join(format!("{session_id}.draft.json")),
        }
    }

    /// Read the stored draft, treating an unreadable or corrupt sidecar as no
    /// draft. Losing a draft is recoverable; failing to resume because one is
    /// malformed is not.
    pub fn load(&self) -> Option<ComposerDraft> {
        let raw = fs::read_to_string(&self.path).ok()?;
        let value = serde_json::from_str::<serde_json::Value>(&raw).ok()?;
        ComposerDraft::from_value(value).filter(|draft| !draft.is_empty())
    }

    /// Persist the draft, or remove the sidecar once there is nothing to keep.
    /// Callers treat failure as non-fatal: a draft that cannot be written must
    /// not interrupt the session.
    pub fn save(&self, draft: &ComposerDraft) -> Result<()> {
        if draft.is_empty() {
            return self.clear();
        }
        let raw = serde_json::to_vec(&draft.to_value()).context("failed to encode composer draft")?;
        // Write-then-rename so an interrupted save cannot leave a half-written
        // sidecar that reads back as a truncated prompt.
        let temporary = self.path.with_extension("draft.json.tmp");
        fs::write(&temporary, raw)
            .with_context(|| format!("failed to write composer draft {}", temporary.display()))?;
        fs::rename(&temporary, &self.path)
            .with_context(|| format!("failed to store composer draft {}", self.path.display()))
    }

    pub fn clear(&self) -> Result<()> {
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => {
                Err(error).with_context(|| format!("failed to remove composer draft {}", self.path.display()))
            }
        }
    }
}
