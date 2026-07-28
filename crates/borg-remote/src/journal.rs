use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use uuid::Uuid;

use crate::{PlanItem, SessionEvent, SessionEventKind, SessionGoal};

/// Durable append-only event journal used by both attached and headless hosts.
///
/// JSONL keeps recovery and export simple. Sequence continuity is validated on
/// read and append so remote clients never render an ambiguous timeline.
pub struct SessionJournal {
    path: PathBuf,
    next_sequence: u64,
    append_file: Option<File>,
}

/// Exclusive ownership of a session journal's writer.
///
/// Readers remain lock-free so mirrors can upload events while the actor is
/// running. The session actor holds this lease for its whole lifetime to
/// prevent two resumed processes from appending the same sequence.
#[derive(Debug)]
pub struct SessionWriterLease {
    journal_path: PathBuf,
    _file: File,
}

impl SessionJournal {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(parent, fs::Permissions::from_mode(0o700))
                    .with_context(|| format!("failed to secure {}", parent.display()))?;
            }
        }
        let events = read_events(&path)?;
        let next_sequence = events.last().map_or(1, |event| event.sequence + 1);
        Ok(Self {
            path,
            next_sequence,
            append_file: None,
        })
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Try to become the sole writer for this session.
    ///
    /// Contention is normal and means a caller should attach to the active
    /// owner. Other lock failures remain errors.
    pub fn try_acquire_writer(&self) -> Result<Option<SessionWriterLease>> {
        let Some(lease) = SessionWriterLease::try_acquire(&self.path)? else {
            return Ok(None);
        };
        repair_torn_tail(&self.path)?;
        Ok(Some(lease))
    }

    #[cfg(test)]
    pub(crate) fn acquire_writer(&self) -> Result<SessionWriterLease> {
        self.try_acquire_writer()?.with_context(|| {
            format!(
                "session is already active in another Borg process ({})",
                self.path.display()
            )
        })
    }

    pub fn validate_session(&self, session_id: Uuid) -> Result<()> {
        let events = self.read()?;
        anyhow::ensure!(
            !events.is_empty(),
            "session journal {} is empty",
            self.path.display()
        );
        for event in &events {
            anyhow::ensure!(
                event.session_id == session_id,
                "session journal {} contains event {} for session {}, expected {}",
                self.path.display(),
                event.id,
                event.session_id,
                session_id
            );
        }
        anyhow::ensure!(
            matches!(
                events.first().map(|event| &event.kind),
                Some(SessionEventKind::SessionStarted)
            ),
            "session journal {} does not start with session_started",
            self.path.display()
        );
        anyhow::ensure!(
            events
                .iter()
                .any(|event| matches!(event.kind, SessionEventKind::SessionConfigured { .. })),
            "session journal {} is missing session configuration",
            self.path.display()
        );
        Ok(())
    }

    pub fn append(&mut self, mut event: SessionEvent) -> Result<SessionEvent> {
        if event.sequence == 0 {
            event.sequence = self.next_sequence;
        }
        if event.sequence != self.next_sequence {
            bail!(
                "session event sequence must be {}, received {}",
                self.next_sequence,
                event.sequence
            );
        }
        let existed = self.path.exists();
        if self.append_file.is_none() {
            let mut options = OpenOptions::new();
            options.create(true).append(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let file = options
                .open(&self.path)
                .with_context(|| format!("failed to open {}", self.path.display()))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                file.set_permissions(fs::Permissions::from_mode(0o600))
                    .with_context(|| format!("failed to secure {}", self.path.display()))?;
            }
            self.append_file = Some(file);
        }
        // Publish a complete JSONL record with one write. `read()` may run in
        // the relay uploader while the actor is appending; streaming JSON
        // directly into the file lets that reader mistake an in-progress
        // record for a torn final append and truncate it.
        let mut record = serde_json::to_vec(&event)?;
        record.push(b'\n');
        let file = self.append_file.as_mut().expect("opened above");
        file.write_all(&record)?;
        if event_requires_immediate_sync(&event.kind) {
            file.sync_data()?;
        }
        if !existed && let Some(parent) = self.path.parent() {
            sync_directory(parent)?;
        }
        self.next_sequence += 1;
        Ok(event)
    }

    pub fn read(&self) -> Result<Vec<SessionEvent>> {
        read_events(&self.path)
    }

    /// Create a source-preserving branch immediately before `sequence`.
    ///
    /// Provider links are deliberately omitted: the branch owns a new provider
    /// conversation and can reconstruct the retained visible messages when it
    /// sends its first turn.
    pub fn fork_before(
        &self,
        destination: impl Into<PathBuf>,
        session_id: Uuid,
        sequence: u64,
    ) -> Result<Self> {
        let destination = destination.into();
        anyhow::ensure!(
            !destination.exists(),
            "fork destination already exists: {}",
            destination.display()
        );
        let mut fork = Self::open(destination)?;
        for event in self
            .read()?
            .into_iter()
            .filter(|event| event.sequence < sequence)
        {
            if matches!(
                event.kind,
                SessionEventKind::ProviderSessionLinked { .. }
                    | SessionEventKind::StatusChanged { .. }
                    | SessionEventKind::SubagentActivity { .. }
                    | SessionEventKind::SubagentControl { .. }
            ) {
                continue;
            }
            fork.append(SessionEvent {
                id: Uuid::new_v4(),
                session_id,
                sequence: 0,
                created_at: event.created_at,
                kind: event.kind,
            })?;
        }
        fork.validate_session(session_id)?;
        Ok(fork)
    }

    pub fn contains_message(&self, message_id: Uuid) -> Result<bool> {
        Ok(self.read()?.iter().any(|event| {
            matches!(
                event.kind,
                SessionEventKind::Message {
                    message_id: existing,
                    ..
                } if existing == message_id
            )
        }))
    }

    pub fn provider_session_id(&self) -> Result<Option<String>> {
        Ok(self
            .read()?
            .into_iter()
            .rev()
            .find_map(|event| match event.kind {
                SessionEventKind::ProviderSessionLinked {
                    provider_session_id,
                } => Some(provider_session_id),
                _ => None,
            }))
    }

    /// Project the current goal from the durable event stream.
    pub fn goal(&self) -> Result<Option<SessionGoal>> {
        let mut goal = None;
        for event in self.read()? {
            match event.kind {
                SessionEventKind::GoalUpdated { goal: updated } => goal = Some(updated),
                SessionEventKind::GoalCleared { .. } => goal = None,
                _ => {}
            }
        }
        Ok(goal)
    }

    /// Project the current todo list from the durable event stream.
    pub fn todos(&self) -> Result<Vec<PlanItem>> {
        Ok(self
            .read()?
            .into_iter()
            .rev()
            .find_map(|event| match event.kind {
                SessionEventKind::PlanUpdated { items } => Some(items),
                _ => None,
            })
            .unwrap_or_default())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl SessionWriterLease {
    /// Acquire only the per-session process ownership boundary.
    ///
    /// This deliberately avoids decoding the legacy JSONL payload. SQLite
    /// callers retain the existing ownership contract without paying a
    /// full-history read on every startup or attachment.
    pub fn try_acquire(journal_path: impl Into<PathBuf>) -> Result<Option<Self>> {
        let journal_path = journal_path.into();
        if let Some(parent) = journal_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let lock_path = journal_path.with_extension("jsonl.lock");
        let mut options = OpenOptions::new();
        options.create(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options
            .open(&lock_path)
            .with_context(|| format!("failed to open session lock {}", lock_path.display()))?;
        match file.try_lock() {
            Ok(()) => Ok(Some(Self {
                journal_path,
                _file: file,
            })),
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(std::fs::TryLockError::Error(error)) => Err(error)
                .with_context(|| format!("failed to lock session {}", journal_path.display())),
        }
    }

    pub(crate) fn acquire(journal_path: impl Into<PathBuf>) -> Result<Self> {
        let journal_path = journal_path.into();
        Self::try_acquire(journal_path.clone())?.with_context(|| {
            format!(
                "session is already active in another Borg process ({})",
                journal_path.display()
            )
        })
    }

    pub(crate) fn ensure_journal(&self, journal_path: &Path) -> Result<()> {
        anyhow::ensure!(
            self.journal_path == journal_path,
            "session writer lease belongs to {}, not {}",
            self.journal_path.display(),
            journal_path.display()
        );
        Ok(())
    }
}

fn event_requires_immediate_sync(kind: &SessionEventKind) -> bool {
    !matches!(
        kind,
        SessionEventKind::ProviderEvent { .. }
            | SessionEventKind::ReasoningDelta { .. }
            | SessionEventKind::Message {
                status: crate::MessageStatus::InProgress,
                ..
            }
    )
}

fn read_events(path: &Path) -> Result<Vec<SessionEvent>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to open {}", path.display()));
        }
    };
    decode_events(bytes, path)
}

pub(crate) fn decode_events(mut bytes: Vec<u8>, source: &Path) -> Result<Vec<SessionEvent>> {
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        bytes.truncate(committed_len(&bytes));
    }
    let mut events = Vec::new();
    for (index, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let event: SessionEvent = serde_json::from_slice(line)
            .with_context(|| format!("invalid event at {} line {}", source.display(), index + 1))?;
        let expected = events
            .last()
            .map_or(1, |prior: &SessionEvent| prior.sequence + 1);
        if event.sequence != expected {
            bail!(
                "session event sequence gap at {} line {}: expected {}, received {}",
                source.display(),
                index + 1,
                expected,
                event.sequence
            );
        }
        events.push(event);
    }
    Ok(events)
}

fn repair_torn_tail(path: &Path) -> Result<()> {
    let bytes = match fs::read(path) {
        Ok(bytes) if !bytes.is_empty() && !bytes.ends_with(b"\n") => bytes,
        Ok(_) => return Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to open {}", path.display()));
        }
    };
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .with_context(|| format!("failed to repair torn journal {}", path.display()))?;
    file.set_len(committed_len(&bytes) as u64)
        .with_context(|| format!("failed to truncate torn journal {}", path.display()))?;
    file.sync_data()
        .with_context(|| format!("failed to sync repaired journal {}", path.display()))?;
    Ok(())
}

fn committed_len(bytes: &[u8]) -> usize {
    bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1)
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(path)
        .with_context(|| format!("failed to open {}", path.display()))?
        .sync_all()
        .with_context(|| format!("failed to sync {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;
    use crate::SessionEventKind;

    #[test]
    fn journal_round_trips_contiguous_events() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        let session_id = Uuid::new_v4();
        let mut journal = SessionJournal::open(&path).unwrap();
        let first = journal
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::SessionStarted,
            ))
            .unwrap();
        let second = journal
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: crate::EventActor::User,
                    text: "hello".into(),
                    attachments: Vec::new(),
                    status: crate::MessageStatus::Complete,
                    delivery: Some(crate::PromptDelivery::Steer),
                },
            ))
            .unwrap();

        assert_eq!((first.sequence, second.sequence), (1, 2));
        assert_eq!(SessionJournal::open(path).unwrap().next_sequence(), 3);
    }

    #[test]
    fn resumed_journal_must_belong_to_the_requested_session() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        let persisted_session_id = Uuid::new_v4();
        let mut journal = SessionJournal::open(&path).unwrap();
        journal
            .append(SessionEvent::new(
                persisted_session_id,
                0,
                SessionEventKind::SessionStarted,
            ))
            .unwrap();
        journal
            .append(SessionEvent::new(
                persisted_session_id,
                0,
                SessionEventKind::SessionConfigured {
                    cwd: directory.path().to_path_buf(),
                    provider: crate::CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast: false,
                    response_language: crate::ResponseLanguage::Auto,
                    permission_mode: crate::PermissionMode::FullAccess,
                },
            ))
            .unwrap();

        journal.validate_session(persisted_session_id).unwrap();
        let error = journal.validate_session(Uuid::new_v4()).unwrap_err();
        assert!(error.to_string().contains("expected"));
    }

    #[test]
    fn fork_keeps_the_committed_prefix_without_reusing_provider_state() {
        let directory = tempdir().unwrap();
        let source_id = Uuid::new_v4();
        let fork_id = Uuid::new_v4();
        let mut source = SessionJournal::open(directory.path().join("source.jsonl")).unwrap();
        for kind in [
            SessionEventKind::SessionStarted,
            SessionEventKind::SessionConfigured {
                cwd: directory.path().to_path_buf(),
                provider: crate::CodingProvider::Codex,
                model: None,
                effort: None,
                fast: false,
                response_language: crate::ResponseLanguage::Auto,
                permission_mode: crate::PermissionMode::FullAccess,
            },
            SessionEventKind::ProviderSessionLinked {
                provider_session_id: "original-provider-thread".into(),
            },
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: crate::EventActor::User,
                text: "keep me".into(),
                attachments: Vec::new(),
                status: crate::MessageStatus::Complete,
                delivery: Some(crate::PromptDelivery::Steer),
            },
        ] {
            source
                .append(SessionEvent::new(source_id, 0, kind))
                .unwrap();
        }
        let cut_sequence = source.next_sequence();
        source
            .append(SessionEvent::new(
                source_id,
                0,
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: crate::EventActor::User,
                    text: "edit me".into(),
                    attachments: Vec::new(),
                    status: crate::MessageStatus::Complete,
                    delivery: Some(crate::PromptDelivery::Steer),
                },
            ))
            .unwrap();

        let fork = source
            .fork_before(directory.path().join("fork.jsonl"), fork_id, cut_sequence)
            .unwrap();
        let events = fork.read().unwrap();

        assert!(events.iter().all(|event| event.session_id == fork_id));
        assert!(events.iter().any(|event| matches!(
            &event.kind,
            SessionEventKind::Message { text, .. } if text == "keep me"
        )));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event.kind, SessionEventKind::ProviderSessionLinked { .. }))
        );
    }

    #[test]
    fn only_one_session_actor_can_own_the_journal_writer() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        let journal = SessionJournal::open(&path).unwrap();
        let lease = journal.try_acquire_writer().unwrap().unwrap();

        let competing_journal = SessionJournal::open(&path).unwrap();
        assert!(competing_journal.try_acquire_writer().unwrap().is_none());

        drop(lease);
        assert!(competing_journal.try_acquire_writer().unwrap().is_some());
    }

    #[test]
    fn journal_replays_latest_todo_list_with_stable_ids() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        let session_id = Uuid::new_v4();
        let item_id = Uuid::new_v4();
        let mut journal = SessionJournal::open(&path).unwrap();
        journal
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::PlanUpdated {
                    items: vec![crate::PlanItem {
                        id: item_id,
                        content: "Ship the todo contract".into(),
                        status: crate::PlanItemStatus::InProgress,
                    }],
                },
            ))
            .unwrap();

        let resumed = SessionJournal::open(path).unwrap().todos().unwrap();
        assert_eq!(resumed.len(), 1);
        assert_eq!(resumed[0].id, item_id);
        assert_eq!(resumed[0].status, crate::PlanItemStatus::InProgress);
    }

    #[test]
    fn readers_ignore_torn_tail_and_writer_repairs_it_before_append() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        let session_id = Uuid::new_v4();
        let mut journal = SessionJournal::open(&path).unwrap();
        journal
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::SessionStarted,
            ))
            .unwrap();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(br#"{"id":"torn""#).unwrap();
        file.sync_data().unwrap();
        let torn_len = fs::metadata(&path).unwrap().len();

        let mut recovered = SessionJournal::open(&path).unwrap();
        assert_eq!(recovered.read().unwrap().len(), 1);
        assert_eq!(fs::metadata(&path).unwrap().len(), torn_len);
        assert_eq!(recovered.next_sequence(), 2);
        let _lease = recovered.try_acquire_writer().unwrap().unwrap();
        assert!(fs::metadata(&path).unwrap().len() < torn_len);
        recovered
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::StatusChanged {
                    status: crate::SessionStatus::Ready,
                    detail: None,
                },
            ))
            .unwrap();
        assert_eq!(recovered.read().unwrap().len(), 2);
    }
}
