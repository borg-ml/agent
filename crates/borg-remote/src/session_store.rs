use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Row, Sqlite, SqlitePool, Transaction};
use uuid::Uuid;

use crate::{
    CodingProvider, MessageStatus, PermissionMode, PlanItem, ResponseLanguage, SessionEvent,
    SessionEventKind, SessionGoal, SessionJournal, SessionPayloadKind, SessionPayloadRef,
    SessionStatus,
};

const INLINE_SESSION_PAYLOAD_BYTES: usize = 64 * 1024;
const SESSION_PAYLOAD_PREVIEW_BYTES: usize = 4 * 1024;
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const SQLITE_WRITE_TRANSACTION: &str = "BEGIN IMMEDIATE";
pub const SESSION_PROJECTION_VERSION: i32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventPersistence {
    Durable,
    Coalesced,
    Ephemeral,
}

impl SessionEventKind {
    pub fn persistence(&self) -> EventPersistence {
        match self {
            Self::ProviderEvent { provider, kind, .. }
                if provider.uses_native_harness()
                    && matches!(
                        kind.as_str(),
                        "native_model_message" | "native_tool_round_completed"
                    ) =>
            {
                EventPersistence::Durable
            }
            Self::ProviderEvent { kind, .. } if kind == "context_compaction" => {
                EventPersistence::Durable
            }
            Self::ProviderEvent { .. } => EventPersistence::Ephemeral,
            Self::ReasoningDelta { .. }
            | Self::ContextWindowUpdated { .. }
            | Self::Message {
                status: MessageStatus::InProgress,
                ..
            } => EventPersistence::Coalesced,
            _ => EventPersistence::Durable,
        }
    }

    pub fn is_fork_inheritable(&self) -> bool {
        !matches!(
            self,
            Self::ProviderSessionLinked { .. }
                | Self::TurnStarted { .. }
                | Self::StatusChanged { .. }
                | Self::SubagentActivity { .. }
                | Self::SubagentControl { .. }
        )
    }

    pub fn is_context_relevant(&self) -> bool {
        match self {
            Self::Message {
                actor: crate::EventActor::User | crate::EventActor::Assistant,
                status: MessageStatus::Complete,
                ..
            }
            | Self::TurnCompleted { .. }
            | Self::ContextCleared => true,
            Self::ProviderEvent { provider, kind, .. } if provider.uses_native_harness() => {
                matches!(
                    kind.as_str(),
                    "native_model_message" | "native_tool_round_completed" | "context_compaction"
                )
            }
            _ => false,
        }
    }

    pub fn is_queue_relevant(&self) -> bool {
        matches!(
            self,
            Self::Message {
                actor: crate::EventActor::User,
                ..
            } | Self::PromptRecalled { .. }
        )
    }

    pub fn is_subagent_relevant(&self) -> bool {
        matches!(self, Self::SubagentActivity { .. })
    }

    fn is_recovery_relevant(&self) -> bool {
        self.is_context_relevant() || self.is_queue_relevant() || self.is_subagent_relevant()
    }

    pub fn live_state_key(&self) -> Option<String> {
        match self {
            Self::Message {
                message_id,
                status: MessageStatus::InProgress,
                ..
            } => Some(format!("message:{message_id}")),
            Self::ReasoningDelta { .. } => Some("reasoning".to_string()),
            Self::ContextWindowUpdated { .. } => Some("context_window".to_string()),
            _ => None,
        }
    }

    pub fn cleared_live_state_keys(&self) -> Vec<String> {
        match self {
            Self::Message {
                message_id,
                actor,
                status: MessageStatus::Complete,
                ..
            } => {
                let mut keys = vec![format!("message:{message_id}")];
                if *actor == crate::EventActor::Assistant {
                    keys.push("reasoning".to_string());
                }
                keys
            }
            Self::TurnCompleted { .. }
            | Self::StatusChanged {
                status: SessionStatus::Ready | SessionStatus::Stopped,
                ..
            } => vec!["reasoning".to_string()],
            Self::ContextCleared => vec!["reasoning".to_string(), "context_window".to_string()],
            _ => Vec::new(),
        }
    }

    pub fn payload_refs(&self) -> Vec<&SessionPayloadRef> {
        match self {
            Self::ToolStarted { input_ref, .. } => input_ref.iter().collect(),
            Self::ToolCompleted {
                output_ref,
                input_ref,
                ..
            } => output_ref.iter().chain(input_ref.iter()).collect(),
            _ => Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionConfiguration {
    pub cwd: PathBuf,
    pub provider: CodingProvider,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub fast: bool,
    pub response_language: ResponseLanguage,
    pub permission_mode: PermissionMode,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SessionUsage {
    pub calls: u64,
    pub provider_duration_ms: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_input_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub total_tokens: u64,
    pub cost_microusd: Option<u64>,
    pub cost_basis: String,
    pub cost_usd: Option<f64>,
    pub context_tokens: Option<u64>,
    pub context_window_tokens: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SessionState {
    pub latest_sequence: u64,
    pub started_at: Option<DateTime<Utc>>,
    pub activity_at: Option<DateTime<Utc>>,
    pub configuration: Option<SessionConfiguration>,
    pub status: Option<SessionStatus>,
    pub status_detail: Option<String>,
    pub provider_session_id: Option<String>,
    pub pending_approval_id: Option<String>,
    pub pending_provider_interaction_id: Option<String>,
    pub pending_provider_interaction_kind: Option<String>,
    pub pending_provider_interaction_payload: Option<serde_json::Value>,
    pub goal: Option<SessionGoal>,
    pub todos: Vec<PlanItem>,
    pub usage: SessionUsage,
    pub first_prompt: Option<String>,
    pub latest_prompt: Option<String>,
    pub latest_response: Option<String>,
}

impl SessionState {
    pub fn reduce(events: &[SessionEvent]) -> Result<Self> {
        let mut state = Self::default();
        for event in events {
            state.apply(event)?;
        }
        Ok(state)
    }

    pub fn apply(&mut self, event: &SessionEvent) -> Result<()> {
        let expected = self.latest_sequence.saturating_add(1);
        anyhow::ensure!(
            event.sequence == expected,
            "session projection expected sequence {expected}, received {}",
            event.sequence
        );
        self.latest_sequence = event.sequence;
        self.activity_at = Some(event.created_at);
        match &event.kind {
            SessionEventKind::SessionStarted => self.started_at = Some(event.created_at),
            SessionEventKind::SessionConfigured {
                cwd,
                provider,
                model,
                effort,
                fast,
                response_language,
                permission_mode,
            } => {
                self.configuration = Some(SessionConfiguration {
                    cwd: cwd.clone(),
                    provider: *provider,
                    model: model.clone(),
                    effort: effort.clone(),
                    fast: *fast,
                    response_language: *response_language,
                    permission_mode: *permission_mode,
                });
            }
            SessionEventKind::StatusChanged { status, detail } => {
                self.status = Some(*status);
                self.status_detail = detail.clone();
            }
            SessionEventKind::ProviderSessionLinked {
                provider_session_id,
            } => self.provider_session_id = Some(provider_session_id.clone()),
            SessionEventKind::ApprovalRequested { approval_id, .. } => {
                self.pending_approval_id = Some(approval_id.clone());
            }
            SessionEventKind::ApprovalResolved { approval_id, .. }
                if self.pending_approval_id.as_deref() == Some(approval_id) =>
            {
                self.pending_approval_id = None;
            }
            SessionEventKind::ProviderInteractionRequested {
                interaction_id,
                kind,
                payload,
                ..
            } => {
                self.pending_provider_interaction_id = Some(interaction_id.clone());
                self.pending_provider_interaction_kind = Some(kind.clone());
                self.pending_provider_interaction_payload = Some(payload.clone());
            }
            SessionEventKind::ProviderInteractionResolved { interaction_id, .. }
                if self.pending_provider_interaction_id.as_deref() == Some(interaction_id) =>
            {
                self.pending_provider_interaction_id = None;
                self.pending_provider_interaction_kind = None;
                self.pending_provider_interaction_payload = None;
            }
            SessionEventKind::GoalUpdated { goal } => self.goal = Some(goal.clone()),
            SessionEventKind::GoalCleared { .. } => self.goal = None,
            SessionEventKind::PlanUpdated { items } => self.todos = items.clone(),
            SessionEventKind::UsageUpdated {
                provider_duration_ms,
                input_tokens,
                output_tokens,
                cached_input_tokens,
                cache_creation_input_tokens,
                total_tokens,
                cost_microusd,
                cost_basis,
                cost_usd,
                context_tokens,
                context_window_tokens,
            } => {
                self.usage.calls = self.usage.calls.saturating_add(1);
                self.usage.provider_duration_ms = self
                    .usage
                    .provider_duration_ms
                    .saturating_add(*provider_duration_ms);
                self.usage.input_tokens = self.usage.input_tokens.saturating_add(*input_tokens);
                self.usage.output_tokens = self.usage.output_tokens.saturating_add(*output_tokens);
                self.usage.cached_input_tokens = self
                    .usage
                    .cached_input_tokens
                    .saturating_add(*cached_input_tokens);
                self.usage.cache_creation_input_tokens = self
                    .usage
                    .cache_creation_input_tokens
                    .saturating_add(*cache_creation_input_tokens);
                self.usage.total_tokens = self.usage.total_tokens.saturating_add(*total_tokens);
                self.usage.cost_microusd = match (self.usage.cost_microusd, cost_microusd) {
                    (Some(current), Some(additional)) => Some(current.saturating_add(*additional)),
                    (None, Some(value)) => Some(*value),
                    (current, None) => current,
                };
                self.usage.cost_basis = cost_basis.clone();
                self.usage.cost_usd = match (self.usage.cost_usd, cost_usd) {
                    (Some(current), Some(additional)) => Some(current + additional),
                    (None, Some(value)) => Some(*value),
                    (current, None) => current,
                };
                self.usage.context_tokens = *context_tokens;
                self.usage.context_window_tokens = *context_window_tokens;
            }
            SessionEventKind::ContextWindowUpdated {
                context_tokens,
                context_window_tokens,
            } => {
                self.usage.context_tokens = Some(*context_tokens);
                self.usage.context_window_tokens = Some(*context_window_tokens);
            }
            SessionEventKind::ContextCleared => {
                self.provider_session_id = None;
                self.usage.context_tokens = Some(0);
            }
            SessionEventKind::Message {
                actor: crate::EventActor::User,
                text,
                status: MessageStatus::Complete,
                ..
            } if !text.trim().is_empty() => {
                let prompt = text.trim().to_string();
                self.first_prompt.get_or_insert_with(|| prompt.clone());
                self.latest_prompt = Some(prompt);
            }
            SessionEventKind::Message {
                actor: crate::EventActor::Assistant,
                text,
                status: MessageStatus::Complete,
                ..
            } if !text.trim().is_empty() => {
                self.latest_response = Some(text.trim().to_string());
            }
            _ => {}
        }
        Ok(())
    }

    fn for_fork(&self, inherited_event_count: u64) -> Self {
        let mut state = self.clone();
        state.latest_sequence = inherited_event_count;
        state.status = None;
        state.status_detail = None;
        state.provider_session_id = None;
        state.pending_approval_id = None;
        state.pending_provider_interaction_id = None;
        state.pending_provider_interaction_kind = None;
        state.pending_provider_interaction_payload = None;
        state
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionStoreFork {
    pub session_id: Uuid,
    pub parent_session_id: Uuid,
    pub parent_cut_sequence: u64,
    pub inherited_event_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionImport {
    pub session_id: Uuid,
    pub event_count: u64,
    pub source_fingerprint: String,
    pub backup_path: PathBuf,
    pub imported: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SessionSummary {
    pub session_id: Uuid,
    pub parent_session_id: Option<Uuid>,
    pub parent_cut_sequence: Option<u64>,
    pub inherited_event_count: u64,
    pub state: SessionState,
}

#[derive(Debug, Clone, Default)]
pub struct SessionRecovery {
    pub context_events: Vec<SessionEvent>,
    pub queue_events: Vec<SessionEvent>,
    pub subagent_events: Vec<SessionEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionLiveEvent {
    pub revision: u64,
    pub event: SessionEvent,
}

impl SessionRecovery {
    fn from_events(events: Vec<SessionEvent>) -> Self {
        let mut recovery = Self::default();
        for event in events {
            if matches!(event.kind, SessionEventKind::ContextCleared) {
                recovery.context_events.clear();
            }
            if event.kind.is_context_relevant() {
                recovery.context_events.push(event.clone());
            }
            if event.kind.is_queue_relevant() {
                recovery.queue_events.push(event.clone());
            }
            if event.kind.is_subagent_relevant() {
                recovery.subagent_events.push(event);
            }
        }
        recovery
    }
}

#[async_trait]
pub trait SessionStore: Send + Sync {
    async fn create_session(&self, session_id: Uuid) -> Result<()>;
    async fn register_child_session(
        &self,
        _owner_session_id: Uuid,
        session_id: Uuid,
        _legacy_jsonl: &Path,
        _writer: &crate::SessionWriterLease,
    ) -> Result<()> {
        anyhow::bail!("session store cannot register child session {session_id}")
    }
    async fn append(&self, event: SessionEvent) -> Result<SessionEvent>;
    async fn read(&self, session_id: Uuid) -> Result<Vec<SessionEvent>>;
    async fn events_after(
        &self,
        session_id: Uuid,
        sequence: u64,
        limit: usize,
    ) -> Result<Vec<SessionEvent>>;
    async fn state(&self, session_id: Uuid) -> Result<SessionState>;
    async fn recovery(&self, session_id: Uuid) -> Result<SessionRecovery>;
    async fn live_events_after(
        &self,
        session_id: Uuid,
        revision: u64,
    ) -> Result<Vec<SessionLiveEvent>>;
    async fn load_payload(&self, payload: &SessionPayloadRef) -> Result<Vec<u8>>;
    async fn contains_message(&self, session_id: Uuid, message_id: Uuid) -> Result<bool>;
    async fn fork_before(
        &self,
        parent_session_id: Uuid,
        session_id: Uuid,
        sequence: u64,
    ) -> Result<SessionStoreFork>;
    async fn list_sessions(&self, limit: usize) -> Result<Vec<SessionSummary>>;
}

#[derive(Clone)]
pub struct SqliteSessionStore {
    pool: SqlitePool,
}

pub struct JsonlSessionStore {
    journal: Mutex<SessionJournal>,
}

impl JsonlSessionStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        Ok(Self::from_journal(SessionJournal::open(path)?))
    }

    pub(crate) fn from_journal(journal: SessionJournal) -> Self {
        Self {
            journal: Mutex::new(journal),
        }
    }

    fn journal(&self) -> Result<std::sync::MutexGuard<'_, SessionJournal>> {
        self.journal
            .lock()
            .map_err(|_| anyhow::anyhow!("JSONL session store lock was poisoned"))
    }
}

#[async_trait]
impl SessionStore for JsonlSessionStore {
    async fn create_session(&self, _session_id: Uuid) -> Result<()> {
        Ok(())
    }

    async fn append(&self, event: SessionEvent) -> Result<SessionEvent> {
        self.journal()?.append(event)
    }

    async fn read(&self, session_id: Uuid) -> Result<Vec<SessionEvent>> {
        let events = self.journal()?.read()?;
        anyhow::ensure!(
            events.iter().all(|event| event.session_id == session_id),
            "JSONL session store contains events for another session"
        );
        Ok(events)
    }

    async fn events_after(
        &self,
        session_id: Uuid,
        sequence: u64,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        Ok(self
            .read(session_id)
            .await?
            .into_iter()
            .skip(usize::try_from(sequence).unwrap_or(usize::MAX))
            .take(limit)
            .collect())
    }

    async fn state(&self, session_id: Uuid) -> Result<SessionState> {
        SessionState::reduce(&self.read(session_id).await?)
    }

    async fn recovery(&self, session_id: Uuid) -> Result<SessionRecovery> {
        Ok(SessionRecovery::from_events(self.read(session_id).await?))
    }

    async fn live_events_after(
        &self,
        _session_id: Uuid,
        _revision: u64,
    ) -> Result<Vec<SessionLiveEvent>> {
        Ok(Vec::new())
    }

    async fn load_payload(&self, payload: &SessionPayloadRef) -> Result<Vec<u8>> {
        anyhow::bail!(
            "payload {} is not available in the JSONL compatibility store",
            payload.id
        )
    }

    async fn contains_message(&self, session_id: Uuid, message_id: Uuid) -> Result<bool> {
        Ok(self.read(session_id).await?.iter().any(|event| {
            matches!(
                event.kind,
                SessionEventKind::Message {
                    message_id: existing,
                    ..
                } if existing == message_id
            )
        }))
    }

    async fn fork_before(
        &self,
        _parent_session_id: Uuid,
        session_id: Uuid,
        sequence: u64,
    ) -> Result<SessionStoreFork> {
        anyhow::bail!(
            "JSONL compatibility store cannot create metadata lineage for session {session_id} at {sequence}"
        )
    }

    async fn list_sessions(&self, limit: usize) -> Result<Vec<SessionSummary>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let events = self.journal()?.read()?;
        let Some(session_id) = events.first().map(|event| event.session_id) else {
            return Ok(Vec::new());
        };
        Ok(vec![SessionSummary {
            session_id,
            parent_session_id: None,
            parent_cut_sequence: None,
            inherited_event_count: 0,
            state: SessionState::reduce(&events)?,
        }])
    }
}

impl SqliteSessionStore {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(SQLITE_BUSY_TIMEOUT)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await
            .with_context(|| format!("failed to open SQLite session store {}", path.display()))?;
        let store = Self { pool };
        store.ensure_schema().await?;
        Ok(store)
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    async fn begin_write(&self) -> Result<Transaction<'static, Sqlite>, sqlx::Error> {
        self.pool.begin_with(SQLITE_WRITE_TRANSACTION).await
    }

    pub async fn contains_session(&self, session_id: Uuid) -> Result<bool> {
        let found: i64 = sqlx::query_scalar("select exists(select 1 from sessions where id = ?)")
            .bind(session_id.to_string())
            .fetch_one(&self.pool)
            .await?;
        Ok(found != 0)
    }

    async fn ensure_schema(&self) -> Result<()> {
        sqlx::raw_sql(
            r#"
            create table if not exists sessions (
                id text primary key,
                parent_session_id text references sessions(id),
                parent_cut_sequence integer,
                owner_session_id text references sessions(id),
                inherited_event_count integer not null default 0,
                next_sequence integer not null default 1,
                live_revision integer not null default 0,
                state_json text not null,
                projection_version integer not null default 3,
                source_fingerprint text,
                created_at text not null,
                updated_at text not null
            );

            create table if not exists session_events (
                session_id text not null references sessions(id) on delete cascade,
                sequence integer not null,
                event_id text not null,
                event_kind text not null,
                event_json text not null,
                projection_json text not null,
                fork_inheritable integer not null,
                recovery_relevant integer not null,
                message_id text,
                created_at text not null,
                primary key (session_id, sequence),
                unique (session_id, event_id)
            );

            create table if not exists session_live_state (
                session_id text not null references sessions(id) on delete cascade,
                live_key text not null,
                revision integer not null,
                event_json text not null,
                updated_at text not null,
                primary key (session_id, live_key)
            );

            create index if not exists idx_session_live_revision
                on session_live_state (session_id, revision);

            create table if not exists session_payloads (
                id text primary key,
                session_id text not null references sessions(id) on delete cascade,
                event_id text not null,
                payload_kind text not null,
                payload blob not null,
                byte_len integer not null,
                created_at text not null
            );

            create index if not exists idx_session_payloads_event
                on session_payloads (session_id, event_id);

            create index if not exists idx_session_events_message
                on session_events (session_id, message_id)
                where message_id is not null;

            create index if not exists idx_sessions_activity
                on sessions (updated_at desc);
            "#,
        )
        .execute(&self.pool)
        .await?;
        let columns = sqlx::query("pragma table_info(sessions)")
            .fetch_all(&self.pool)
            .await?;
        if !columns
            .iter()
            .any(|row| row.get::<&str, _>("name") == "source_fingerprint")
        {
            sqlx::query("alter table sessions add column source_fingerprint text")
                .execute(&self.pool)
                .await?;
        }
        if !columns
            .iter()
            .any(|row| row.get::<&str, _>("name") == "projection_version")
        {
            sqlx::query(
                "alter table sessions add column projection_version integer not null default 0",
            )
            .execute(&self.pool)
            .await?;
        }
        if !columns
            .iter()
            .any(|row| row.get::<&str, _>("name") == "live_revision")
        {
            sqlx::query("alter table sessions add column live_revision integer not null default 0")
                .execute(&self.pool)
                .await?;
        }
        if !columns
            .iter()
            .any(|row| row.get::<&str, _>("name") == "owner_session_id")
        {
            sqlx::query(
                "alter table sessions add column owner_session_id text references sessions(id)",
            )
            .execute(&self.pool)
            .await?;
        }
        sqlx::query(
            "create index if not exists idx_sessions_root_activity \
             on sessions (owner_session_id, updated_at desc)",
        )
        .execute(&self.pool)
        .await?;
        self.ensure_recovery_index().await?;
        self.rebuild_stale_projections().await?;
        Ok(())
    }

    async fn rebuild_stale_projections(&self) -> Result<()> {
        let rows =
            sqlx::query("select id, parent_session_id from sessions where projection_version < 3")
                .fetch_all(&self.pool)
                .await?;
        for row in &rows {
            if row
                .try_get::<Option<&str>, _>("parent_session_id")?
                .is_some()
            {
                continue;
            }
            let session_id = parse_uuid(row.try_get("id")?)?;
            let event_rows = sqlx::query(
                "select sequence, event_json from session_events \
                 where session_id = ? order by sequence",
            )
            .bind(session_id.to_string())
            .fetch_all(&self.pool)
            .await?;
            let mut state = SessionState::default();
            let mut transaction = self.begin_write().await?;
            for event_row in event_rows {
                let event: SessionEvent = serde_json::from_str(event_row.try_get("event_json")?)?;
                state.apply(&event)?;
                sqlx::query(
                    "update session_events set projection_json = ? \
                     where session_id = ? and sequence = ?",
                )
                .bind(serde_json::to_string(&state)?)
                .bind(session_id.to_string())
                .bind(event_row.try_get::<i64, _>("sequence")?)
                .execute(&mut *transaction)
                .await?;
            }
            transaction.commit().await?;
        }
        for row in rows {
            let session_id = parse_uuid(row.try_get("id")?)?;
            let state = SessionState::reduce(&self.projected_events(session_id, None).await?)?;
            sqlx::query("update sessions set state_json = ?, projection_version = 3 where id = ?")
                .bind(serde_json::to_string(&state)?)
                .bind(session_id.to_string())
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    async fn ensure_recovery_index(&self) -> Result<()> {
        let columns = sqlx::query("pragma table_info(session_events)")
            .fetch_all(&self.pool)
            .await?;
        let added = !columns
            .iter()
            .any(|row| row.get::<&str, _>("name") == "recovery_relevant");
        if added {
            sqlx::query("alter table session_events add column recovery_relevant integer")
                .execute(&self.pool)
                .await?;
            let rows = sqlx::query("select session_id, sequence, event_json from session_events")
                .fetch_all(&self.pool)
                .await?;
            let mut transaction = self.begin_write().await?;
            for row in rows {
                let event: SessionEvent = serde_json::from_str(row.try_get("event_json")?)?;
                sqlx::query(
                    "update session_events set recovery_relevant = ? \
                     where session_id = ? and sequence = ?",
                )
                .bind(i64::from(event.kind.is_recovery_relevant()))
                .bind(row.try_get::<&str, _>("session_id")?)
                .bind(row.try_get::<i64, _>("sequence")?)
                .execute(&mut *transaction)
                .await?;
            }
            transaction.commit().await?;
        }
        sqlx::query(
            "create index if not exists idx_session_events_recovery \
             on session_events (session_id, sequence) where recovery_relevant = 1",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn import_jsonl(&self, path: impl AsRef<Path>) -> Result<SessionImport> {
        let path = path.as_ref();
        let bytes = tokio::fs::read(path)
            .await
            .with_context(|| format!("failed to read {}", path.display()))?;
        let source_fingerprint = format!("{:x}", Sha256::digest(&bytes));
        let events = crate::journal::decode_events(bytes.clone(), path)?;
        let session_id = validate_import_events(path, &events)?;
        let backup_path = preserve_journal_backup(path, &bytes, &source_fingerprint).await?;

        let mut transaction = self.begin_write().await?;
        if let Some(existing_fingerprint) = sqlx::query_scalar::<_, Option<String>>(
            "select source_fingerprint from sessions where id = ?",
        )
        .bind(session_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .flatten()
        {
            anyhow::ensure!(
                existing_fingerprint == source_fingerprint,
                "session {session_id} was already imported from a different JSONL source"
            );
            transaction.rollback().await?;
            return Ok(SessionImport {
                session_id,
                event_count: events.len() as u64,
                source_fingerprint,
                backup_path,
                imported: false,
            });
        }
        let already_exists: i64 =
            sqlx::query_scalar("select exists(select 1 from sessions where id = ?)")
                .bind(session_id.to_string())
                .fetch_one(&mut *transaction)
                .await?;
        anyhow::ensure!(
            already_exists == 0,
            "session {session_id} already exists without matching migration provenance"
        );

        let mut state = SessionState::default();
        let created_at = events
            .first()
            .map(|event| event.created_at)
            .unwrap_or_else(Utc::now);
        let updated_at = events
            .last()
            .map(|event| event.created_at)
            .unwrap_or(created_at);
        sqlx::query(
            "insert into sessions \
             (id, next_sequence, state_json, projection_version, source_fingerprint, \
              created_at, updated_at) values (?, ?, ?, 3, ?, ?, ?)",
        )
        .bind(session_id.to_string())
        .bind(
            i64::try_from(events.len())
                .unwrap_or(i64::MAX)
                .saturating_add(1),
        )
        .bind(serde_json::to_string(&state)?)
        .bind(&source_fingerprint)
        .bind(created_at.to_rfc3339())
        .bind(updated_at.to_rfc3339())
        .execute(&mut *transaction)
        .await?;
        for event in &events {
            state.apply(event)?;
            let compact_event = self.compact_payloads(&mut transaction, event).await?;
            let message_id = match &event.kind {
                SessionEventKind::Message { message_id, .. } => Some(message_id.to_string()),
                _ => None,
            };
            sqlx::query(
                "insert into session_events \
                 (session_id, sequence, event_id, event_kind, event_json, projection_json, \
                  fork_inheritable, recovery_relevant, message_id, created_at) \
                  values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(session_id.to_string())
            .bind(i64::try_from(event.sequence).context("session sequence exceeds SQLite integer")?)
            .bind(event.id.to_string())
            .bind(event_kind(&event.kind)?)
            .bind(serde_json::to_string(&compact_event)?)
            .bind(serde_json::to_string(&state)?)
            .bind(if event.kind.is_fork_inheritable() {
                1_i64
            } else {
                0_i64
            })
            .bind(i64::from(event.kind.is_recovery_relevant()))
            .bind(message_id)
            .bind(event.created_at.to_rfc3339())
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query("update sessions set state_json = ? where id = ?")
            .bind(serde_json::to_string(&state)?)
            .bind(session_id.to_string())
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(SessionImport {
            session_id,
            event_count: events.len() as u64,
            source_fingerprint,
            backup_path,
            imported: true,
        })
    }

    pub async fn register_child_session(
        &self,
        owner_session_id: Uuid,
        session_id: Uuid,
        legacy_jsonl: &Path,
        writer: &crate::SessionWriterLease,
    ) -> Result<()> {
        writer.ensure_journal(legacy_jsonl)?;
        anyhow::ensure!(
            self.contains_session(owner_session_id).await?,
            "owner session {owner_session_id} does not exist"
        );
        if !self.contains_session(session_id).await? {
            if legacy_jsonl.is_file() {
                let imported = self.import_jsonl(legacy_jsonl).await?;
                anyhow::ensure!(
                    imported.session_id == session_id,
                    "child journal {} contains session {}, expected {session_id}",
                    legacy_jsonl.display(),
                    imported.session_id
                );
            } else {
                self.create_session(session_id).await?;
            }
        }
        let existing_owner: Option<String> =
            sqlx::query_scalar("select owner_session_id from sessions where id = ?")
                .bind(session_id.to_string())
                .fetch_one(&self.pool)
                .await?;
        if let Some(existing_owner) = existing_owner {
            anyhow::ensure!(
                existing_owner == owner_session_id.to_string(),
                "child session {session_id} already belongs to {existing_owner}"
            );
        } else {
            sqlx::query("update sessions set owner_session_id = ? where id = ?")
                .bind(owner_session_id.to_string())
                .bind(session_id.to_string())
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    async fn session_row(&self, session_id: Uuid) -> Result<StoredSession> {
        let row = sqlx::query(
            "select parent_session_id, parent_cut_sequence, inherited_event_count, \
             next_sequence, state_json from sessions where id = ?",
        )
        .bind(session_id.to_string())
        .fetch_optional(&self.pool)
        .await?
        .with_context(|| format!("session {session_id} does not exist"))?;
        StoredSession::from_row(&row)
    }

    fn composed_events<'a>(
        &'a self,
        session_id: Uuid,
        before_or_at: Option<u64>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<StoredEvent>>> + Send + 'a>> {
        Box::pin(async move {
            let session = self.session_row(session_id).await?;
            let logical_limit = before_or_at.unwrap_or(session.next_sequence.saturating_sub(1));
            let inherited_limit = logical_limit.min(session.inherited_event_count);
            let mut events = if let (Some(parent), Some(cut)) =
                (session.parent_session_id, session.parent_cut_sequence)
            {
                let mut inherited = self.composed_events(parent, Some(cut)).await?;
                inherited.retain(|event| event.fork_inheritable);
                inherited.truncate(usize::try_from(inherited_limit).unwrap_or(usize::MAX));
                inherited
            } else {
                Vec::new()
            };
            if logical_limit > session.inherited_event_count {
                let rows = sqlx::query(
                    "select event_json, fork_inheritable from session_events \
                     where session_id = ? and sequence > ? and sequence <= ? order by sequence",
                )
                .bind(session_id.to_string())
                .bind(i64::try_from(session.inherited_event_count).unwrap_or(i64::MAX))
                .bind(i64::try_from(logical_limit).unwrap_or(i64::MAX))
                .fetch_all(&self.pool)
                .await?;
                for row in rows {
                    events.push(StoredEvent {
                        event: serde_json::from_str(row.try_get("event_json")?)?,
                        fork_inheritable: row.try_get::<i64, _>("fork_inheritable")? != 0,
                    });
                }
            }
            Ok(events)
        })
    }

    async fn projected_events(
        &self,
        session_id: Uuid,
        before_or_at: Option<u64>,
    ) -> Result<Vec<SessionEvent>> {
        self.composed_events(session_id, before_or_at)
            .await?
            .into_iter()
            .enumerate()
            .map(|(index, stored)| {
                let sequence = u64::try_from(index).unwrap_or(u64::MAX) + 1;
                let mut event = stored.event;
                if event.session_id != session_id {
                    event.id = inherited_event_id(session_id, event.id);
                    event.session_id = session_id;
                }
                event.sequence = sequence;
                Ok(event)
            })
            .collect()
    }

    fn composed_recovery_events<'a>(
        &'a self,
        session_id: Uuid,
        before_or_at: Option<u64>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<StoredEvent>>> + Send + 'a>> {
        Box::pin(async move {
            let session = self.session_row(session_id).await?;
            let logical_limit = before_or_at.unwrap_or(session.next_sequence.saturating_sub(1));
            let inherited_limit = logical_limit.min(session.inherited_event_count);
            let mut events = if let (Some(parent), Some(cut)) =
                (session.parent_session_id, session.parent_cut_sequence)
            {
                if inherited_limit < session.inherited_event_count {
                    let mut inherited = self.composed_events(parent, Some(cut)).await?;
                    inherited.retain(|event| event.fork_inheritable);
                    inherited.truncate(usize::try_from(inherited_limit).unwrap_or(usize::MAX));
                    inherited.retain(|event| event.event.kind.is_recovery_relevant());
                    inherited
                } else {
                    let mut inherited = self.composed_recovery_events(parent, Some(cut)).await?;
                    inherited.retain(|event| event.fork_inheritable);
                    inherited
                }
            } else {
                Vec::new()
            };
            if logical_limit > session.inherited_event_count {
                let rows = sqlx::query(
                    "select event_json, fork_inheritable from session_events \
                     where session_id = ? and sequence > ? and sequence <= ? \
                     and recovery_relevant = 1 order by sequence",
                )
                .bind(session_id.to_string())
                .bind(i64::try_from(session.inherited_event_count).unwrap_or(i64::MAX))
                .bind(i64::try_from(logical_limit).unwrap_or(i64::MAX))
                .fetch_all(&self.pool)
                .await?;
                for row in rows {
                    events.push(StoredEvent {
                        event: serde_json::from_str(row.try_get("event_json")?)?,
                        fork_inheritable: row.try_get::<i64, _>("fork_inheritable")? != 0,
                    });
                }
            }
            Ok(events)
        })
    }

    async fn recovery_events(&self, session_id: Uuid) -> Result<Vec<SessionEvent>> {
        Ok(self
            .composed_recovery_events(session_id, None)
            .await?
            .into_iter()
            .map(|stored| {
                let mut event = stored.event;
                if event.session_id != session_id {
                    event.id = inherited_event_id(session_id, event.id);
                    event.session_id = session_id;
                }
                event
            })
            .collect())
    }

    async fn fork_projection(
        &self,
        parent_session_id: Uuid,
        sequence: u64,
        parent: &StoredSession,
    ) -> Result<(u64, SessionState)> {
        if parent.parent_session_id.is_none() {
            let cut = i64::try_from(sequence.saturating_sub(1)).unwrap_or(i64::MAX);
            let inherited_event_count: i64 = sqlx::query_scalar(
                "select count(*) from session_events \
                 where session_id = ? and sequence <= ? and fork_inheritable = 1",
            )
            .bind(parent_session_id.to_string())
            .bind(cut)
            .fetch_one(&self.pool)
            .await?;
            let projection = sqlx::query(
                "select projection_json, created_at from session_events \
                 where session_id = ? and sequence <= ? and fork_inheritable = 1 \
                 order by sequence desc limit 1",
            )
            .bind(parent_session_id.to_string())
            .bind(cut)
            .fetch_optional(&self.pool)
            .await?;
            let mut projection = match projection {
                Some(row) => {
                    let mut state: SessionState =
                        serde_json::from_str(row.try_get("projection_json")?)?;
                    state.activity_at = Some(
                        DateTime::parse_from_rfc3339(row.try_get("created_at")?)?
                            .with_timezone(&Utc),
                    );
                    state
                }
                None => SessionState::default(),
            };
            projection.latest_sequence =
                u64::try_from(inherited_event_count).context("negative inherited event count")?;
            return Ok((projection.latest_sequence, projection));
        }
        let events = self
            .projected_events(parent_session_id, sequence.checked_sub(1))
            .await?;
        let inherited_event_count = events
            .iter()
            .filter(|event| event.kind.is_fork_inheritable())
            .count() as u64;
        Ok((inherited_event_count, SessionState::reduce(&events)?))
    }

    fn contains_message_before<'a>(
        &'a self,
        session_id: Uuid,
        message_id: Uuid,
        before_or_at: Option<u64>,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>> {
        Box::pin(async move {
            let session = self.session_row(session_id).await?;
            let limit = before_or_at.unwrap_or(session.next_sequence.saturating_sub(1));
            let found: i64 = sqlx::query_scalar(
                "select exists(select 1 from session_events \
                 where session_id = ? and message_id = ? and sequence <= ?)",
            )
            .bind(session_id.to_string())
            .bind(message_id.to_string())
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .fetch_one(&self.pool)
            .await?;
            if found != 0 {
                return Ok(true);
            }
            let inherited_limit = limit.min(session.inherited_event_count);
            if inherited_limit == 0 {
                return Ok(false);
            }
            let (Some(parent), Some(cut)) =
                (session.parent_session_id, session.parent_cut_sequence)
            else {
                return Ok(false);
            };
            // Message events are always fork-inheritable. A cut within the
            // compacted inherited prefix is uncommon; resolve that bounded
            // prefix to preserve exact lineage semantics.
            if inherited_limit < session.inherited_event_count {
                return Ok(self
                    .projected_events(session_id, Some(inherited_limit))
                    .await?
                    .iter()
                    .any(|event| {
                        matches!(
                            event.kind,
                            SessionEventKind::Message {
                                message_id: existing,
                                ..
                            } if existing == message_id
                        )
                    }));
            }
            self.contains_message_before(parent, message_id, Some(cut))
                .await
        })
    }

    async fn compact_payloads(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        event: &SessionEvent,
    ) -> Result<SessionEvent> {
        let mut compact = event.clone();
        match &mut compact.kind {
            SessionEventKind::ToolStarted {
                input, input_ref, ..
            } if input_ref.is_none() => {
                let bytes = serde_json::to_vec(input)?;
                if bytes.len() > INLINE_SESSION_PAYLOAD_BYTES {
                    let payload =
                        store_payload(transaction, event, SessionPayloadKind::ToolInput, &bytes)
                            .await?;
                    *input = deferred_json_payload(&payload);
                    *input_ref = Some(payload);
                }
            }
            SessionEventKind::ToolCompleted {
                output,
                output_ref,
                input,
                input_ref,
                ..
            } => {
                if output_ref.is_none() && output.len() > INLINE_SESSION_PAYLOAD_BYTES {
                    let payload = store_payload(
                        transaction,
                        event,
                        SessionPayloadKind::ToolOutput,
                        output.as_bytes(),
                    )
                    .await?;
                    *output = deferred_text_payload(output, &payload);
                    *output_ref = Some(payload);
                }
                if input_ref.is_none()
                    && let Some(value) = input
                {
                    let bytes = serde_json::to_vec(value)?;
                    if bytes.len() > INLINE_SESSION_PAYLOAD_BYTES {
                        let payload = store_payload(
                            transaction,
                            event,
                            SessionPayloadKind::ToolResultInput,
                            &bytes,
                        )
                        .await?;
                        *value = deferred_json_payload(&payload);
                        *input_ref = Some(payload);
                    }
                }
            }
            _ => {}
        }
        Ok(compact)
    }

    async fn append_live(&self, mut event: SessionEvent) -> Result<SessionEvent> {
        let live_key = event
            .kind
            .live_state_key()
            .context("coalesced session event has no live-state key")?;
        event.sequence = 0;
        let mut transaction = self.begin_write().await?;
        let current_revision: i64 =
            sqlx::query_scalar("select live_revision from sessions where id = ?")
                .bind(event.session_id.to_string())
                .fetch_optional(&mut *transaction)
                .await?
                .with_context(|| format!("session {} does not exist", event.session_id))?;
        let revision = current_revision.saturating_add(1);
        let stored_event = if let SessionEventKind::ReasoningDelta { text } = &event.kind {
            let prior = sqlx::query_scalar::<_, String>(
                "select event_json from session_live_state \
                 where session_id = ? and live_key = ?",
            )
            .bind(event.session_id.to_string())
            .bind(&live_key)
            .fetch_optional(&mut *transaction)
            .await?;
            let mut accumulated = prior
                .map(|value| serde_json::from_str::<SessionEvent>(&value))
                .transpose()?
                .and_then(|event| match event.kind {
                    SessionEventKind::ReasoningDelta { text } => Some(text),
                    _ => None,
                })
                .unwrap_or_default();
            accumulated.push_str(text);
            let mut snapshot = event.clone();
            snapshot.kind = SessionEventKind::ReasoningDelta { text: accumulated };
            snapshot
        } else {
            event.clone()
        };
        sqlx::query(
            "insert into session_live_state \
             (session_id, live_key, revision, event_json, updated_at) values (?, ?, ?, ?, ?) \
             on conflict(session_id, live_key) do update set \
             revision = excluded.revision, event_json = excluded.event_json, \
             updated_at = excluded.updated_at",
        )
        .bind(event.session_id.to_string())
        .bind(live_key)
        .bind(revision)
        .bind(serde_json::to_string(&stored_event)?)
        .bind(event.created_at.to_rfc3339())
        .execute(&mut *transaction)
        .await?;
        sqlx::query("update sessions set live_revision = ?, updated_at = ? where id = ?")
            .bind(revision)
            .bind(event.created_at.to_rfc3339())
            .bind(event.session_id.to_string())
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(event)
    }
}

#[async_trait]
impl SessionStore for SqliteSessionStore {
    async fn create_session(&self, session_id: Uuid) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "insert into sessions \
             (id, state_json, projection_version, created_at, updated_at) values (?, ?, 3, ?, ?) \
             on conflict(id) do nothing",
        )
        .bind(session_id.to_string())
        .bind(serde_json::to_string(&SessionState::default())?)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn register_child_session(
        &self,
        owner_session_id: Uuid,
        session_id: Uuid,
        legacy_jsonl: &Path,
        writer: &crate::SessionWriterLease,
    ) -> Result<()> {
        SqliteSessionStore::register_child_session(
            self,
            owner_session_id,
            session_id,
            legacy_jsonl,
            writer,
        )
        .await
    }

    async fn append(&self, mut event: SessionEvent) -> Result<SessionEvent> {
        match event.kind.persistence() {
            EventPersistence::Ephemeral => {
                event.sequence = 0;
                return Ok(event);
            }
            EventPersistence::Coalesced => return self.append_live(event).await,
            EventPersistence::Durable => {}
        }
        let mut transaction = self.begin_write().await?;
        let row = sqlx::query("select next_sequence, state_json from sessions where id = ?")
            .bind(event.session_id.to_string())
            .fetch_optional(&mut *transaction)
            .await?
            .with_context(|| format!("session {} does not exist", event.session_id))?;
        let next_sequence = u64::try_from(row.try_get::<i64, _>("next_sequence")?)
            .context("negative SQLite session sequence")?;
        if event.sequence == 0 {
            event.sequence = next_sequence;
        }
        anyhow::ensure!(
            event.sequence == next_sequence,
            "session event sequence must be {next_sequence}, received {}",
            event.sequence
        );
        let mut state: SessionState = serde_json::from_str(row.try_get("state_json")?)?;
        state.apply(&event)?;
        let compact_event = self.compact_payloads(&mut transaction, &event).await?;
        let event_json = serde_json::to_string(&compact_event)?;
        let projection_json = serde_json::to_string(&state)?;
        let message_id = match &event.kind {
            SessionEventKind::Message { message_id, .. } => Some(message_id.to_string()),
            _ => None,
        };
        sqlx::query(
            "insert into session_events \
             (session_id, sequence, event_id, event_kind, event_json, projection_json, \
              fork_inheritable, recovery_relevant, message_id, created_at) \
             values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(event.session_id.to_string())
        .bind(i64::try_from(event.sequence).context("session sequence exceeds SQLite integer")?)
        .bind(event.id.to_string())
        .bind(event_kind(&event.kind)?)
        .bind(event_json)
        .bind(&projection_json)
        .bind(i64::from(event.kind.is_fork_inheritable()))
        .bind(i64::from(event.kind.is_recovery_relevant()))
        .bind(message_id)
        .bind(event.created_at.to_rfc3339())
        .execute(&mut *transaction)
        .await?;
        for live_key in event.kind.cleared_live_state_keys() {
            sqlx::query("delete from session_live_state where session_id = ? and live_key = ?")
                .bind(event.session_id.to_string())
                .bind(live_key)
                .execute(&mut *transaction)
                .await?;
        }
        sqlx::query(
            "update sessions set next_sequence = ?, state_json = ?, projection_version = 3, \
             updated_at = ? where id = ?",
        )
        .bind(i64::try_from(event.sequence.saturating_add(1)).unwrap_or(i64::MAX))
        .bind(projection_json)
        .bind(event.created_at.to_rfc3339())
        .bind(event.session_id.to_string())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(event)
    }

    async fn read(&self, session_id: Uuid) -> Result<Vec<SessionEvent>> {
        self.projected_events(session_id, None).await
    }

    async fn events_after(
        &self,
        session_id: Uuid,
        sequence: u64,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let session = self.session_row(session_id).await?;
        if sequence >= session.inherited_event_count {
            let rows = sqlx::query(
                "select event_json from session_events where session_id = ? and sequence > ? \
                 order by sequence limit ?",
            )
            .bind(session_id.to_string())
            .bind(i64::try_from(sequence).unwrap_or(i64::MAX))
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .fetch_all(&self.pool)
            .await?;
            return rows
                .into_iter()
                .map(|row| serde_json::from_str(row.try_get("event_json")?).map_err(Into::into))
                .collect();
        }
        Ok(self
            .projected_events(session_id, None)
            .await?
            .into_iter()
            .skip(usize::try_from(sequence).unwrap_or(usize::MAX))
            .take(limit)
            .collect())
    }

    async fn state(&self, session_id: Uuid) -> Result<SessionState> {
        Ok(self.session_row(session_id).await?.state)
    }

    async fn recovery(&self, session_id: Uuid) -> Result<SessionRecovery> {
        Ok(SessionRecovery::from_events(
            self.recovery_events(session_id).await?,
        ))
    }

    async fn live_events_after(
        &self,
        session_id: Uuid,
        revision: u64,
    ) -> Result<Vec<SessionLiveEvent>> {
        let rows = sqlx::query(
            "select revision, event_json from session_live_state \
             where session_id = ? and revision > ? order by revision",
        )
        .bind(session_id.to_string())
        .bind(i64::try_from(revision).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(SessionLiveEvent {
                    revision: u64::try_from(row.try_get::<i64, _>("revision")?)
                        .context("negative live-state revision")?,
                    event: serde_json::from_str(row.try_get("event_json")?)?,
                })
            })
            .collect()
    }

    async fn load_payload(&self, payload: &SessionPayloadRef) -> Result<Vec<u8>> {
        let row = sqlx::query(
            "select payload_kind, payload, byte_len from session_payloads where id = ?",
        )
        .bind(payload.id.to_string())
        .fetch_optional(&self.pool)
        .await?
        .with_context(|| format!("session payload {} does not exist", payload.id))?;
        anyhow::ensure!(
            row.try_get::<&str, _>("payload_kind")? == payload_kind_name(payload.kind),
            "session payload {} has a different typed kind",
            payload.id
        );
        let bytes: Vec<u8> = row.try_get("payload")?;
        let stored_len =
            u64::try_from(row.try_get::<i64, _>("byte_len")?).context("negative payload length")?;
        anyhow::ensure!(
            stored_len == payload.byte_len && bytes.len() as u64 == payload.byte_len,
            "session payload {} length does not match its reference",
            payload.id
        );
        Ok(bytes)
    }

    async fn contains_message(&self, session_id: Uuid, message_id: Uuid) -> Result<bool> {
        self.contains_message_before(session_id, message_id, None)
            .await
    }

    async fn fork_before(
        &self,
        parent_session_id: Uuid,
        session_id: Uuid,
        sequence: u64,
    ) -> Result<SessionStoreFork> {
        let parent = self.session_row(parent_session_id).await?;
        anyhow::ensure!(
            sequence > 0 && sequence <= parent.next_sequence,
            "fork sequence {sequence} is outside session {parent_session_id}"
        );
        let (inherited_event_count, parent_state) = self
            .fork_projection(parent_session_id, sequence, &parent)
            .await?;
        let state = parent_state.for_fork(inherited_event_count);
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "insert into sessions \
             (id, parent_session_id, parent_cut_sequence, inherited_event_count, next_sequence, \
              state_json, projection_version, created_at, updated_at) \
             values (?, ?, ?, ?, ?, ?, 3, ?, ?)",
        )
        .bind(session_id.to_string())
        .bind(parent_session_id.to_string())
        .bind(i64::try_from(sequence.saturating_sub(1)).unwrap_or(i64::MAX))
        .bind(i64::try_from(inherited_event_count).unwrap_or(i64::MAX))
        .bind(i64::try_from(inherited_event_count.saturating_add(1)).unwrap_or(i64::MAX))
        .bind(serde_json::to_string(&state)?)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(SessionStoreFork {
            session_id,
            parent_session_id,
            parent_cut_sequence: sequence.saturating_sub(1),
            inherited_event_count,
        })
    }

    async fn list_sessions(&self, limit: usize) -> Result<Vec<SessionSummary>> {
        let rows = sqlx::query(
            "select id, parent_session_id, parent_cut_sequence, inherited_event_count, state_json \
             from sessions where owner_session_id is null order by updated_at desc limit ?",
        )
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(SessionSummary {
                    session_id: parse_uuid(row.try_get("id")?)?,
                    parent_session_id: row
                        .try_get::<Option<&str>, _>("parent_session_id")?
                        .map(parse_uuid)
                        .transpose()?,
                    parent_cut_sequence: row
                        .try_get::<Option<i64>, _>("parent_cut_sequence")?
                        .map(u64::try_from)
                        .transpose()
                        .context("negative parent cut sequence")?,
                    inherited_event_count: u64::try_from(
                        row.try_get::<i64, _>("inherited_event_count")?,
                    )
                    .context("negative inherited event count")?,
                    state: serde_json::from_str(row.try_get("state_json")?)?,
                })
            })
            .collect()
    }
}

async fn store_payload(
    transaction: &mut Transaction<'_, Sqlite>,
    event: &SessionEvent,
    kind: SessionPayloadKind,
    bytes: &[u8],
) -> Result<SessionPayloadRef> {
    let id = Uuid::new_v5(&event.id, payload_kind_name(kind).as_bytes());
    let payload = SessionPayloadRef {
        id,
        kind,
        byte_len: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
    };
    sqlx::query(
        "insert into session_payloads \
         (id, session_id, event_id, payload_kind, payload, byte_len, created_at) \
         values (?, ?, ?, ?, ?, ?, ?) on conflict(id) do nothing",
    )
    .bind(id.to_string())
    .bind(event.session_id.to_string())
    .bind(event.id.to_string())
    .bind(payload_kind_name(kind))
    .bind(bytes)
    .bind(i64::try_from(bytes.len()).context("session payload exceeds SQLite integer")?)
    .bind(event.created_at.to_rfc3339())
    .execute(&mut **transaction)
    .await?;
    Ok(payload)
}

fn payload_kind_name(kind: SessionPayloadKind) -> &'static str {
    kind.as_str()
}

fn deferred_json_payload(payload: &SessionPayloadRef) -> serde_json::Value {
    serde_json::json!({
        "borg_payload_deferred": true,
        "payload_id": payload.id,
        "byte_len": payload.byte_len,
    })
}

fn deferred_text_payload(value: &str, payload: &SessionPayloadRef) -> String {
    let mut end = value.len().min(SESSION_PAYLOAD_PREVIEW_BYTES);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}\n\n… {} byte payload deferred; expand to load …",
        &value[..end],
        payload.byte_len
    )
}

struct StoredSession {
    parent_session_id: Option<Uuid>,
    parent_cut_sequence: Option<u64>,
    inherited_event_count: u64,
    next_sequence: u64,
    state: SessionState,
}

impl StoredSession {
    fn from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Self> {
        Ok(Self {
            parent_session_id: row
                .try_get::<Option<&str>, _>("parent_session_id")?
                .map(parse_uuid)
                .transpose()?,
            parent_cut_sequence: row
                .try_get::<Option<i64>, _>("parent_cut_sequence")?
                .map(u64::try_from)
                .transpose()
                .context("negative parent cut sequence")?,
            inherited_event_count: u64::try_from(row.try_get::<i64, _>("inherited_event_count")?)
                .context("negative inherited event count")?,
            next_sequence: u64::try_from(row.try_get::<i64, _>("next_sequence")?)
                .context("negative next sequence")?,
            state: serde_json::from_str(row.try_get("state_json")?)?,
        })
    }
}

struct StoredEvent {
    event: SessionEvent,
    fork_inheritable: bool,
}

fn inherited_event_id(session_id: Uuid, source_event_id: Uuid) -> Uuid {
    Uuid::new_v5(&session_id, source_event_id.as_bytes())
}

fn parse_uuid(value: &str) -> Result<Uuid> {
    Uuid::parse_str(value).with_context(|| format!("invalid stored UUID {value}"))
}

fn event_kind(kind: &SessionEventKind) -> Result<String> {
    serde_json::to_value(kind)?
        .get("type")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .context("session event kind has no typed discriminant")
}

fn validate_import_events(path: &Path, events: &[SessionEvent]) -> Result<Uuid> {
    let first = events
        .first()
        .with_context(|| format!("session journal {} is empty", path.display()))?;
    let session_id = first.session_id;
    anyhow::ensure!(
        matches!(first.kind, SessionEventKind::SessionStarted),
        "session journal {} does not start with session_started",
        path.display()
    );
    anyhow::ensure!(
        events.iter().all(|event| event.session_id == session_id),
        "session journal {} contains multiple session IDs",
        path.display()
    );
    anyhow::ensure!(
        events
            .iter()
            .any(|event| matches!(event.kind, SessionEventKind::SessionConfigured { .. })),
        "session journal {} is missing session configuration",
        path.display()
    );
    SessionState::reduce(events)?;
    Ok(session_id)
}

async fn preserve_journal_backup(path: &Path, bytes: &[u8], fingerprint: &str) -> Result<PathBuf> {
    let backup_path = path.with_extension("jsonl.bak");
    match tokio::fs::read(&backup_path).await {
        Ok(existing) => {
            anyhow::ensure!(
                format!("{:x}", Sha256::digest(&existing)) == fingerprint,
                "preserved backup {} differs from {}",
                backup_path.display(),
                path.display()
            );
            return Ok(backup_path);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to read backup {}", backup_path.display()));
        }
    }
    let temporary = backup_path.with_extension(format!("jsonl.bak.tmp-{}", Uuid::new_v4()));
    tokio::fs::write(&temporary, bytes)
        .await
        .with_context(|| format!("failed to write backup {}", temporary.display()))?;
    let file = tokio::fs::OpenOptions::new()
        .read(true)
        .open(&temporary)
        .await?;
    file.sync_all().await?;
    tokio::fs::rename(&temporary, &backup_path)
        .await
        .with_context(|| {
            format!(
                "failed to publish backup {} from {}",
                backup_path.display(),
                temporary.display()
            )
        })?;
    if let Some(parent) = backup_path.parent() {
        let directory = tokio::fs::OpenOptions::new()
            .read(true)
            .open(parent)
            .await?;
        directory.sync_all().await?;
    }
    Ok(backup_path)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use tempfile::tempdir;

    use super::*;
    use crate::{EventActor, PromptDelivery};

    async fn store() -> (tempfile::TempDir, SqliteSessionStore) {
        let directory = tempdir().unwrap();
        let store = SqliteSessionStore::open(directory.path().join("sessions.sqlite3"))
            .await
            .unwrap();
        (directory, store)
    }

    #[tokio::test]
    async fn session_writes_wait_for_short_cross_connection_contention() {
        let (_directory, store) = store().await;
        let session_id = Uuid::new_v4();
        store.create_session(session_id).await.unwrap();
        let blocker = store.begin_write().await.unwrap();
        let contender = store.clone();
        let append = tokio::spawn(async move {
            contender
                .append(SessionEvent::new(
                    session_id,
                    0,
                    SessionEventKind::SessionStarted,
                ))
                .await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!append.is_finished());
        blocker.commit().await.unwrap();

        let event = tokio::time::timeout(Duration::from_secs(1), append)
            .await
            .expect("contending append should resume")
            .expect("append task should not panic")
            .expect("append should succeed after lock release");
        assert_eq!(event.sequence, 1);
    }

    #[tokio::test]
    async fn existing_sqlite_schema_adds_child_ownership_before_indexing_it() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sessions.sqlite3");
        let pool = SqlitePoolOptions::new()
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true),
            )
            .await
            .unwrap();
        sqlx::query(
            "create table sessions (
                id text primary key,
                parent_session_id text references sessions(id),
                parent_cut_sequence integer,
                inherited_event_count integer not null default 0,
                next_sequence integer not null default 1,
                live_revision integer not null default 0,
                state_json text not null,
                projection_version integer not null default 1,
                source_fingerprint text,
                created_at text not null,
                updated_at text not null
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        let store = SqliteSessionStore::open(&path).await.unwrap();
        let owner_column: i64 = sqlx::query_scalar(
            "select count(*) from pragma_table_info('sessions') where name = 'owner_session_id'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        let root_activity_index: i64 = sqlx::query_scalar(
            "select count(*) from sqlite_master \
             where type = 'index' and name = 'idx_sessions_root_activity'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(owner_column, 1);
        assert_eq!(root_activity_index, 1);
    }

    #[tokio::test]
    async fn stale_projection_rebuilds_latest_completed_assistant_response() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sessions.sqlite3");
        let store = SqliteSessionStore::open(&path).await.unwrap();
        let session_id = Uuid::new_v4();
        store.create_session(session_id).await.unwrap();
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
            .unwrap();
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: EventActor::Assistant,
                    text: "persisted response".to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ))
            .await
            .unwrap();
        sqlx::query("update sessions set state_json = ?, projection_version = 2 where id = ?")
            .bind(serde_json::to_string(&SessionState::default()).unwrap())
            .bind(session_id.to_string())
            .execute(store.pool())
            .await
            .unwrap();
        store.pool().close().await;

        let reopened = SqliteSessionStore::open(&path).await.unwrap();
        assert_eq!(
            reopened
                .state(session_id)
                .await
                .unwrap()
                .latest_response
                .as_deref(),
            Some("persisted response")
        );
    }

    fn configured(directory: &Path) -> SessionEventKind {
        SessionEventKind::SessionConfigured {
            cwd: directory.to_path_buf(),
            provider: CodingProvider::Codex,
            model: Some("gpt-test".to_string()),
            effort: Some("high".to_string()),
            fast: false,
            response_language: ResponseLanguage::Auto,
            permission_mode: PermissionMode::FullAccess,
        }
    }

    fn message(message_id: Uuid, text: &str) -> SessionEventKind {
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: text.to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: Some(PromptDelivery::Steer),
        }
    }

    #[tokio::test]
    async fn sqlite_store_appends_projects_and_reads_indexed_suffixes() {
        let (directory, store) = store().await;
        let session_id = Uuid::new_v4();
        let message_id = Uuid::new_v4();
        store.create_session(session_id).await.unwrap();
        for kind in [
            SessionEventKind::SessionStarted,
            configured(directory.path()),
            message(message_id, "hello"),
        ] {
            store
                .append(SessionEvent::new(session_id, 0, kind))
                .await
                .unwrap();
        }

        let state = store.state(session_id).await.unwrap();
        assert_eq!(state.latest_sequence, 3);
        assert_eq!(
            state.configuration.as_ref().unwrap().model.as_deref(),
            Some("gpt-test")
        );
        assert!(
            store
                .contains_message(session_id, message_id)
                .await
                .unwrap()
        );
        let suffix = store.events_after(session_id, 1, 10).await.unwrap();
        assert_eq!(
            suffix
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            [2, 3]
        );
        let recovery = store.recovery(session_id).await.unwrap();
        assert_eq!(recovery.context_events.len(), 1);
        assert_eq!(recovery.queue_events.len(), 1);
        assert!(recovery.subagent_events.is_empty());
        assert_eq!(store.list_sessions(10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn context_clear_resets_provider_projection_and_recovery_prefix() {
        let (directory, store) = store().await;
        let session_id = Uuid::new_v4();
        store.create_session(session_id).await.unwrap();
        for kind in [
            SessionEventKind::SessionStarted,
            configured(directory.path()),
            SessionEventKind::ProviderSessionLinked {
                provider_session_id: "old-provider-thread".to_string(),
            },
            message(Uuid::new_v4(), "old context"),
            SessionEventKind::ContextWindowUpdated {
                context_tokens: 40_000,
                context_window_tokens: 100_000,
            },
            SessionEventKind::ContextCleared,
            message(Uuid::new_v4(), "new context"),
        ] {
            store
                .append(SessionEvent::new(session_id, 0, kind))
                .await
                .unwrap();
        }

        let state = store.state(session_id).await.unwrap();
        assert!(state.provider_session_id.is_none());
        assert_eq!(state.usage.context_tokens, Some(0));
        let recovery = store.recovery(session_id).await.unwrap();
        assert_eq!(recovery.context_events.len(), 2);
        assert!(matches!(
            recovery.context_events[0].kind,
            SessionEventKind::ContextCleared
        ));
        assert!(matches!(
            &recovery.context_events[1].kind,
            SessionEventKind::Message { text, .. } if text == "new context"
        ));
    }

    #[tokio::test]
    async fn state_projects_pending_approval_and_cumulative_usage() {
        let (directory, store) = store().await;
        let session_id = Uuid::new_v4();
        store.create_session(session_id).await.unwrap();
        for kind in [
            SessionEventKind::SessionStarted,
            configured(directory.path()),
            SessionEventKind::ApprovalRequested {
                approval_id: "approval-1".to_string(),
                title: "Run command".to_string(),
                detail: "Needs permission".to_string(),
                command: Some("cargo test".to_string()),
            },
        ] {
            store
                .append(SessionEvent::new(session_id, 0, kind))
                .await
                .unwrap();
        }
        assert_eq!(
            store.state(session_id).await.unwrap().pending_approval_id,
            Some("approval-1".to_string())
        );
        for kind in [
            SessionEventKind::ApprovalResolved {
                approval_id: "approval-1".to_string(),
                decision: crate::ApprovalDecision::AllowOnce,
            },
            SessionEventKind::UsageUpdated {
                provider_duration_ms: 10,
                input_tokens: 100,
                output_tokens: 20,
                cached_input_tokens: 40,
                cache_creation_input_tokens: 5,
                total_tokens: 120,
                cost_microusd: Some(100),
                cost_basis: "provider".to_string(),
                cost_usd: Some(0.0001),
                context_tokens: Some(100),
                context_window_tokens: Some(1_000),
            },
            SessionEventKind::UsageUpdated {
                provider_duration_ms: 20,
                input_tokens: 200,
                output_tokens: 30,
                cached_input_tokens: 80,
                cache_creation_input_tokens: 7,
                total_tokens: 230,
                cost_microusd: Some(200),
                cost_basis: "provider".to_string(),
                cost_usd: Some(0.0002),
                context_tokens: Some(200),
                context_window_tokens: Some(1_000),
            },
        ] {
            store
                .append(SessionEvent::new(session_id, 0, kind))
                .await
                .unwrap();
        }

        let state = store.state(session_id).await.unwrap();
        assert_eq!(state.pending_approval_id, None);
        assert_eq!(state.usage.calls, 2);
        assert_eq!(state.usage.provider_duration_ms, 30);
        assert_eq!(state.usage.input_tokens, 300);
        assert_eq!(state.usage.output_tokens, 50);
        assert_eq!(state.usage.cached_input_tokens, 120);
        assert_eq!(state.usage.cache_creation_input_tokens, 12);
        assert_eq!(state.usage.total_tokens, 350);
        assert_eq!(state.usage.cost_microusd, Some(300));
        assert!((state.usage.cost_usd.unwrap() - 0.0003).abs() < f64::EPSILON);
        assert_eq!(state.usage.context_tokens, Some(200));
    }

    #[tokio::test]
    async fn fork_records_lineage_without_copying_events() {
        let (directory, store) = store().await;
        let parent_id = Uuid::new_v4();
        let fork_id = Uuid::new_v4();
        let retained_message_id = Uuid::new_v4();
        let discarded_message_id = Uuid::new_v4();
        store.create_session(parent_id).await.unwrap();
        for kind in [
            SessionEventKind::SessionStarted,
            configured(directory.path()),
            SessionEventKind::ProviderSessionLinked {
                provider_session_id: "provider-thread".to_string(),
            },
            message(retained_message_id, "keep"),
            message(discarded_message_id, "discard"),
        ] {
            store
                .append(SessionEvent::new(parent_id, 0, kind))
                .await
                .unwrap();
        }

        let fork = store.fork_before(parent_id, fork_id, 5).await.unwrap();
        assert_eq!(fork.inherited_event_count, 3);
        let copied_rows: i64 =
            sqlx::query_scalar("select count(*) from session_events where session_id = ?")
                .bind(fork_id.to_string())
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(copied_rows, 0);

        let events = store.read(fork_id).await.unwrap();
        assert_eq!(events.len(), 3);
        assert!(events.iter().all(|event| event.session_id == fork_id));
        assert_eq!(
            events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            [1, 2, 3]
        );
        assert!(
            events
                .iter()
                .all(|event| !matches!(event.kind, SessionEventKind::ProviderSessionLinked { .. }))
        );
        assert!(
            store
                .contains_message(fork_id, retained_message_id)
                .await
                .unwrap()
        );
        assert!(
            !store
                .contains_message(fork_id, discarded_message_id)
                .await
                .unwrap()
        );
        let state = store.state(fork_id).await.unwrap();
        assert_eq!(state.latest_sequence, 3);
        assert!(state.provider_session_id.is_none());
        let recovery = store.recovery(fork_id).await.unwrap();
        assert_eq!(recovery.context_events.len(), 1);
        assert_eq!(recovery.queue_events.len(), 1);
    }

    #[tokio::test]
    async fn live_state_coalesces_without_consuming_durable_sequences() {
        let (directory, store) = store().await;
        let session_id = Uuid::new_v4();
        let message_id = Uuid::new_v4();
        store.create_session(session_id).await.unwrap();
        for kind in [
            SessionEventKind::SessionStarted,
            configured(directory.path()),
        ] {
            store
                .append(SessionEvent::new(session_id, 0, kind))
                .await
                .unwrap();
        }
        for text in ["a", "a much longer snapshot"] {
            let event = store
                .append(SessionEvent::new(
                    session_id,
                    0,
                    SessionEventKind::Message {
                        message_id,
                        actor: EventActor::Assistant,
                        text: text.to_string(),
                        attachments: Vec::new(),
                        status: MessageStatus::InProgress,
                        delivery: None,
                    },
                ))
                .await
                .unwrap();
            assert_eq!(event.sequence, 0);
        }
        for text in ["thinking ", "carefully"] {
            store
                .append(SessionEvent::new(
                    session_id,
                    0,
                    SessionEventKind::ReasoningDelta {
                        text: text.to_string(),
                    },
                ))
                .await
                .unwrap();
        }
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ProviderEvent {
                    provider: CodingProvider::Codex,
                    kind: "telemetry".to_string(),
                    payload: serde_json::json!({"large": "discarded"}),
                },
            ))
            .await
            .unwrap();

        assert_eq!(store.read(session_id).await.unwrap().len(), 2);
        let live = store.live_events_after(session_id, 0).await.unwrap();
        assert_eq!(live.len(), 2);
        assert!(live.iter().any(|live| matches!(
            &live.event.kind,
            SessionEventKind::Message { text, .. } if text == "a much longer snapshot"
        )));
        assert!(live.iter().any(|live| matches!(
            &live.event.kind,
            SessionEventKind::ReasoningDelta { text } if text == "thinking carefully"
        )));

        let completed = store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::Message {
                    message_id,
                    actor: EventActor::Assistant,
                    text: "done".to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ))
            .await
            .unwrap();
        assert_eq!(completed.sequence, 3);
        assert!(
            store
                .live_events_after(session_id, 0)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn large_tool_payloads_are_loaded_only_by_reference() {
        let (directory, store) = store().await;
        let session_id = Uuid::new_v4();
        let input = serde_json::json!({"text": "x".repeat(INLINE_SESSION_PAYLOAD_BYTES)});
        store.create_session(session_id).await.unwrap();
        for kind in [
            SessionEventKind::SessionStarted,
            configured(directory.path()),
        ] {
            store
                .append(SessionEvent::new(session_id, 0, kind))
                .await
                .unwrap();
        }
        let appended = store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ToolStarted {
                    tool_call_id: "large-tool".to_string(),
                    name: "large".to_string(),
                    input: input.clone(),
                    input_ref: None,
                },
            ))
            .await
            .unwrap();
        assert!(matches!(
            appended.kind,
            SessionEventKind::ToolStarted {
                input_ref: None,
                ..
            }
        ));

        let persisted = store.events_after(session_id, 2, 1).await.unwrap();
        let SessionEventKind::ToolStarted {
            input: preview,
            input_ref: Some(payload),
            ..
        } = &persisted[0].kind
        else {
            panic!("large tool input should be stored by reference");
        };
        assert_ne!(preview, &input);
        assert_eq!(
            store.load_payload(payload).await.unwrap(),
            serde_json::to_vec(&input).unwrap()
        );
    }

    #[tokio::test]
    async fn jsonl_import_is_atomic_backed_up_and_idempotent() {
        let (directory, store) = store().await;
        let session_id = Uuid::new_v4();
        let journal_path = directory.path().join(format!("{session_id}.jsonl"));
        let mut journal = crate::SessionJournal::open(&journal_path).unwrap();
        for kind in [
            SessionEventKind::SessionStarted,
            configured(directory.path()),
            message(Uuid::new_v4(), "migrate me"),
        ] {
            journal
                .append(SessionEvent::new(session_id, 0, kind))
                .unwrap();
        }

        let imported = store.import_jsonl(&journal_path).await.unwrap();
        assert!(imported.imported);
        assert_eq!(imported.session_id, session_id);
        assert_eq!(imported.event_count, 3);
        assert_eq!(
            tokio::fs::read(&imported.backup_path).await.unwrap(),
            tokio::fs::read(&journal_path).await.unwrap()
        );
        assert_eq!(store.read(session_id).await.unwrap().len(), 3);

        let repeated = store.import_jsonl(&journal_path).await.unwrap();
        assert!(!repeated.imported);
        assert_eq!(repeated.source_fingerprint, imported.source_fingerprint);
    }

    #[tokio::test]
    async fn invalid_jsonl_does_not_create_a_session_or_backup() {
        let (directory, store) = store().await;
        let session_id = Uuid::new_v4();
        let journal_path = directory.path().join(format!("{session_id}.jsonl"));
        let event = SessionEvent::new(
            session_id,
            1,
            message(Uuid::new_v4(), "missing session start"),
        );
        tokio::fs::write(
            &journal_path,
            format!("{}\n", serde_json::to_string(&event).unwrap()),
        )
        .await
        .unwrap();

        assert!(store.import_jsonl(&journal_path).await.is_err());
        assert!(!journal_path.with_extension("jsonl.bak").exists());
        assert!(store.list_sessions(10).await.unwrap().is_empty());
    }

    #[test]
    fn persistence_and_fork_rules_are_typed_rust_contracts() {
        assert_eq!(
            SessionEventKind::ReasoningDelta {
                text: "working".to_string()
            }
            .persistence(),
            EventPersistence::Coalesced
        );
        assert_eq!(
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "noise".to_string(),
                payload: serde_json::Value::Null,
            }
            .persistence(),
            EventPersistence::Ephemeral
        );
        assert!(
            !SessionEventKind::ProviderSessionLinked {
                provider_session_id: "provider".to_string()
            }
            .is_fork_inheritable()
        );
    }

    #[tokio::test]
    #[ignore = "explicit large-session p95 performance gate"]
    async fn large_session_lineage_and_tail_p95_gates() {
        const EVENT_COUNT: u64 = 38_272;
        const SAMPLES: usize = 100;

        let (directory, store) = store().await;
        let parent_id = Uuid::new_v4();
        store.create_session(parent_id).await.unwrap();
        let mut state = SessionState::default();
        let mut transaction = store.pool().begin().await.unwrap();
        for sequence in 1..=EVENT_COUNT {
            let kind = match sequence {
                1 => SessionEventKind::SessionStarted,
                2 => configured(directory.path()),
                _ => SessionEventKind::Error {
                    message: "bounded performance fixture".to_string(),
                },
            };
            let event = SessionEvent::new(parent_id, sequence, kind);
            state.apply(&event).unwrap();
            sqlx::query(
                "insert into session_events \
                 (session_id, sequence, event_id, event_kind, event_json, projection_json, \
                  fork_inheritable, recovery_relevant, message_id, created_at) \
                 values (?, ?, ?, ?, ?, ?, ?, ?, null, ?)",
            )
            .bind(parent_id.to_string())
            .bind(i64::try_from(sequence).unwrap())
            .bind(event.id.to_string())
            .bind(event_kind(&event.kind).unwrap())
            .bind(serde_json::to_string(&event).unwrap())
            .bind(serde_json::to_string(&state).unwrap())
            .bind(i64::from(event.kind.is_fork_inheritable()))
            .bind(i64::from(event.kind.is_recovery_relevant()))
            .bind(event.created_at.to_rfc3339())
            .execute(&mut *transaction)
            .await
            .unwrap();
        }
        sqlx::query(
            "update sessions set next_sequence = ?, state_json = ?, updated_at = ? where id = ?",
        )
        .bind(i64::try_from(EVENT_COUNT + 1).unwrap())
        .bind(serde_json::to_string(&state).unwrap())
        .bind(Utc::now().to_rfc3339())
        .bind(parent_id.to_string())
        .execute(&mut *transaction)
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let mut fork_samples = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            let started = Instant::now();
            store
                .fork_before(parent_id, Uuid::new_v4(), EVENT_COUNT + 1)
                .await
                .unwrap();
            fork_samples.push(started.elapsed());
        }
        let mut tail_samples = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            let started = Instant::now();
            let tail = store
                .events_after(parent_id, EVENT_COUNT - 100, 100)
                .await
                .unwrap();
            assert_eq!(tail.len(), 100);
            tail_samples.push(started.elapsed());
        }
        let fork_p95 = duration_p95(&mut fork_samples);
        let tail_p95 = duration_p95(&mut tail_samples);
        eprintln!("lineage fork p95: {fork_p95:?}; indexed tail p95: {tail_p95:?}");
        assert!(
            fork_p95 < Duration::from_millis(200),
            "lineage fork p95 exceeded 200 ms: {fork_p95:?}"
        );
        assert!(
            tail_p95 < Duration::from_millis(50),
            "indexed tail p95 exceeded 50 ms: {tail_p95:?}"
        );
    }

    fn duration_p95(samples: &mut [Duration]) -> Duration {
        samples.sort_unstable();
        samples[(samples.len() * 95).div_ceil(100).saturating_sub(1)]
    }
}
