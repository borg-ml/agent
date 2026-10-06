use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

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
///
/// Saves run on a writer thread so a slow disk never stalls typing. Only the
/// newest draft is kept: a write still pending when the next keystroke lands
/// is replaced, not queued. Dropping the store flushes that last draft, so a
/// terminal rebuilt for the same session reads back what was typed.
#[derive(Debug)]
pub struct ComposerDraftStore {
    path: PathBuf,
    writer: Option<DraftWriter>,
}

#[derive(Debug)]
struct DraftWriter {
    pending: Arc<(Mutex<PendingDraft>, Condvar)>,
    thread: Option<JoinHandle<()>>,
}

#[derive(Debug, Default)]
struct PendingDraft {
    draft: Option<ComposerDraft>,
    closed: bool,
}

impl ComposerDraftStore {
    pub fn for_session(sessions_dir: &Path, session_id: Uuid) -> Self {
        Self {
            path: sessions_dir.join(format!("{session_id}.draft.json")),
            writer: None,
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

    /// Hand the draft to the writer thread, superseding any unwritten one.
    /// An empty draft removes the sidecar. Failures are logged by the writer:
    /// a draft that cannot be written must not interrupt the session.
    pub fn save(&mut self, draft: ComposerDraft) {
        let path = self.path.clone();
        let writer = self.writer.get_or_insert_with(|| DraftWriter::spawn(path));
        let (lock, wake) = &*writer.pending;
        lock.lock().unwrap_or_else(|error| error.into_inner()).draft = Some(draft);
        wake.notify_one();
    }

    fn write(path: &Path, draft: &ComposerDraft) -> Result<()> {
        if draft.is_empty() {
            return Self::remove(path);
        }
        let raw =
            serde_json::to_vec(&draft.to_value()).context("failed to encode composer draft")?;
        // Write-then-rename so an interrupted save cannot leave a half-written
        // sidecar that reads back as a truncated prompt.
        let temporary = path.with_extension("draft.json.tmp");
        fs::write(&temporary, raw)
            .with_context(|| format!("failed to write composer draft {}", temporary.display()))?;
        fs::rename(&temporary, path)
            .with_context(|| format!("failed to store composer draft {}", path.display()))
    }

    fn remove(path: &Path) -> Result<()> {
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error)
                .with_context(|| format!("failed to remove composer draft {}", path.display())),
        }
    }
}

impl DraftWriter {
    fn spawn(path: PathBuf) -> Self {
        let pending = Arc::new((Mutex::new(PendingDraft::default()), Condvar::new()));
        let shared = Arc::clone(&pending);
        let thread = std::thread::Builder::new()
            .name("composer-draft".into())
            .spawn(move || {
                let (lock, wake) = &*shared;
                loop {
                    let draft = {
                        let mut state = lock.lock().unwrap_or_else(|error| error.into_inner());
                        while state.draft.is_none() && !state.closed {
                            state = wake.wait(state).unwrap_or_else(|error| error.into_inner());
                        }
                        match state.draft.take() {
                            Some(draft) => draft,
                            None => return,
                        }
                    };
                    if let Err(error) = ComposerDraftStore::write(&path, &draft) {
                        tracing::warn!(%error, "failed to persist composer draft");
                    }
                }
            })
            .ok();
        if thread.is_none() {
            tracing::warn!("could not start the composer draft writer");
        }
        Self { pending, thread }
    }
}

impl Drop for DraftWriter {
    fn drop(&mut self) {
        let (lock, wake) = &*self.pending;
        lock.lock()
            .unwrap_or_else(|error| error.into_inner())
            .closed = true;
        wake.notify_one();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
