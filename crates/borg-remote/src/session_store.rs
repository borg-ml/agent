use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
};
use sqlx::{Row, Sqlite, SqlitePool, Transaction};
use uuid::Uuid;

use crate::session_action::{SessionAction, SessionActionState, SessionActionTransition};
use crate::{
    CodingProvider, MessageStatus, PermissionMode, PlanItem, ResponseLanguage, SessionEvent,
    SessionEventKind, SessionGoal, SessionPayloadKind, SessionPayloadRef, SessionStatus,
};

pub(crate) const INLINE_SESSION_PAYLOAD_BYTES: usize = 64 * 1024;
pub(crate) const SESSION_PAYLOAD_PREVIEW_BYTES: usize = 4 * 1024;
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const SQLITE_WRITE_TRANSACTION: &str = "BEGIN IMMEDIATE";
const MAX_HOST_LAUNCH_METADATA_BYTES: usize = 512 * 1024;
pub const SESSION_PROJECTION_VERSION: i32 = 3;
const SESSION_SCHEMA_VERSION: i64 = 5;

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
            Self::ProviderEvent { kind, .. }
                if matches!(
                    kind.as_str(),
                    "context_compaction" | "context_compaction_failed"
                ) =>
            {
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
                | Self::ProviderCapabilitiesUpdated { .. }
                | Self::TurnStarted { .. }
                | Self::StatusChanged { .. }
                | Self::SubagentActivity { .. }
                | Self::SubagentControl { .. }
                | Self::RuntimeProcessStarted { .. }
                | Self::RuntimeProcessOutput { .. }
                | Self::RuntimeProcessCompleted { .. }
                | Self::BluWorkflowStarted { .. }
                | Self::BluWorkflowCallRequested { .. }
                | Self::BluWorkflowCallCompleted { .. }
                | Self::BluWorkflowCompleted { .. }
                // A fork cuts immediately before the admission of the prompt it
                // rewinds to, which would otherwise leave that prompt's earlier
                // queue entry inside the inherited history: the fork would then
                // recover it as pending and immediately re-run the very prompt
                // the rewind discarded. Only admitted history is inheritable.
                | Self::Message {
                    status: MessageStatus::Queued,
                    ..
                }
        )
    }

    pub fn is_context_relevant(&self) -> bool {
        match self {
            Self::Message {
                actor: crate::EventActor::User | crate::EventActor::Assistant,
                status: MessageStatus::Complete,
                ..
            } => true,
            Self::Message {
                actor: crate::EventActor::System,
                status: MessageStatus::Complete,
                ..
            }
            | Self::ToolStarted { .. }
            | Self::ToolCompleted { .. }
            | Self::ApprovalRequested { .. }
            | Self::ApprovalResolved { .. }
            | Self::ProviderInteractionRequested { .. }
            | Self::ProviderInteractionResolved { .. }
            | Self::PlanUpdated { .. }
            | Self::GoalUpdated { .. }
            | Self::GoalCleared { .. } => true,
            // TurnStarted is not itself model content, but it is the durable
            // boundary that tells replay which provider produced the generic
            // message/tool events that follow. Pi keeps the equivalent branch
            // structure in its session entries; Borg needs this metadata in
            // the recovered context slice for cross-provider replay.
            Self::TurnStarted { .. } | Self::TurnCompleted { .. } | Self::ContextCleared => true,
            Self::ProviderEvent { kind, payload, .. } if kind == "context_compaction" => {
                payload.get("status").and_then(serde_json::Value::as_str) != Some("started")
            }
            Self::ProviderEvent { provider, kind, .. } if provider.uses_native_harness() => {
                matches!(
                    kind.as_str(),
                    "native_model_message" | "native_tool_round_completed"
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
                | Self::Message {
                    actor: crate::EventActor::System,
                    ..
                }
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
            Self::ReasoningCompleted
            | Self::ToolStarted { .. }
            | Self::ToolCompleted { .. }
            | Self::ApprovalRequested { .. }
            | Self::ProviderInteractionRequested { .. }
            | Self::StatusChanged {
                status: SessionStatus::WaitingForApproval,
                ..
            } => vec!["reasoning".to_string()],
            Self::ContextCleared => vec!["reasoning".to_string(), "context_window".to_string()],
            _ => Vec::new(),
        }
    }

    /// Terminal boundaries end the current streamed turn. The context-window
    /// snapshot is session metadata and intentionally survives, but streamed
    /// assistant messages and reasoning must not outlive their turn.
    pub fn clears_live_turn_state(&self) -> bool {
        matches!(
            self,
            Self::TurnCompleted { .. }
                | Self::StatusChanged {
                    status: SessionStatus::Ready
                        | SessionStatus::Completed
                        | SessionStatus::Failed
                        | SessionStatus::Stopped,
                    ..
                }
        )
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
    /// Monotonic identity for the canonical provider-context prefix. A
    /// compaction, explicit context clear, or provider/model change starts a
    /// new cache epoch; reconnects and ordinary turns retain it.
    #[serde(default)]
    pub context_generation: u64,
    pub started_at: Option<DateTime<Utc>>,
    pub activity_at: Option<DateTime<Utc>>,
    pub configuration: Option<SessionConfiguration>,
    /// Latest host-local provider admission snapshot. Authentication metadata
    /// is durable for recovery and UI inspection but excluded from model
    /// context and fork inheritance.
    #[serde(default)]
    pub provider_capabilities: Vec<crate::ProviderCapability>,
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
                let context_identity_changed = self.configuration.as_ref().is_some_and(|old| {
                    old.provider != *provider || old.model.as_ref() != model.as_ref()
                });
                self.configuration = Some(SessionConfiguration {
                    cwd: cwd.clone(),
                    provider: *provider,
                    model: model.clone(),
                    effort: effort.clone(),
                    fast: *fast,
                    response_language: *response_language,
                    permission_mode: *permission_mode,
                });
                if context_identity_changed {
                    self.context_generation = self.context_generation.saturating_add(1);
                    self.provider_session_id = None;
                    self.usage.context_tokens = Some(0);
                }
            }
            SessionEventKind::ProviderCapabilitiesUpdated { providers } => {
                self.provider_capabilities = providers.clone();
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
                ..
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
                self.context_generation = self.context_generation.saturating_add(1);
            }
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if kind == "context_compaction"
                    && payload.get("status").and_then(serde_json::Value::as_str)
                        == Some("completed") =>
            {
                self.context_generation = self.context_generation.saturating_add(1);
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

#[derive(Debug, Clone, PartialEq)]
pub struct SessionSummary {
    pub session_id: Uuid,
    pub parent_session_id: Option<Uuid>,
    pub parent_cut_sequence: Option<u64>,
    pub inherited_event_count: u64,
    pub state: SessionState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionWorkspaceBinding {
    pub session_id: Uuid,
    pub workspace_id: Uuid,
    pub participant_id: Uuid,
    pub host_id: Option<Uuid>,
    pub attached_at: DateTime<Utc>,
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
    ) -> Result<()> {
        anyhow::bail!("session store cannot register child session {session_id}")
    }
    async fn append(&self, event: SessionEvent) -> Result<SessionEvent>;
    async fn enqueue_action(&self, action: SessionAction) -> Result<SessionAction>;
    async fn transition_action(
        &self,
        session_id: Uuid,
        action_id: Uuid,
        expected: Option<SessionActionState>,
        next: SessionActionState,
        error: Option<String>,
    ) -> Result<SessionAction>;
    /// Atomically reserve one queued or expired non-terminal action for a
    /// worker. Repeating the call with the same owner while its lease is live
    /// returns the existing claim; another owner receives `None`.
    async fn claim_action(
        &self,
        session_id: Uuid,
        action_id: Uuid,
        lease_owner: &str,
        lease_duration: Duration,
    ) -> Result<Option<SessionAction>>;
    /// Extend a live lease. The token fences a worker that was paused past
    /// expiry and then resumed after another worker reclaimed the action.
    async fn heartbeat_action(
        &self,
        session_id: Uuid,
        action_id: Uuid,
        lease_owner: &str,
        lease_token: Uuid,
        lease_duration: Duration,
    ) -> Result<SessionAction>;
    /// Transition an action only while the caller still owns its live lease.
    async fn transition_claimed_action(
        &self,
        session_id: Uuid,
        action_id: Uuid,
        lease_owner: &str,
        lease_token: Uuid,
        expected: Option<SessionActionState>,
        next: SessionActionState,
        error: Option<String>,
    ) -> Result<SessionAction>;
    /// Requeue expired work left in the in-flight states by a crashed worker.
    /// The update and its audit transition are committed as one transaction.
    async fn recover_expired_actions(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<SessionAction>>;
    async fn action(&self, session_id: Uuid, action_id: Uuid) -> Result<Option<SessionAction>>;
    async fn action_transitions(
        &self,
        session_id: Uuid,
        action_id: Uuid,
    ) -> Result<Vec<SessionActionTransition>>;
    async fn pending_actions(&self, session_id: Uuid, limit: usize) -> Result<Vec<SessionAction>>;
    async fn read(&self, session_id: Uuid) -> Result<Vec<SessionEvent>>;
    async fn events_after(
        &self,
        session_id: Uuid,
        sequence: u64,
        limit: usize,
    ) -> Result<Vec<SessionEvent>>;
    /// Return the newest completed user messages authored in this session,
    /// ordered from oldest to newest.
    ///
    /// This is intentionally separate from transcript paging: interactive
    /// clients need durable prompt recall across resumes without loading the
    /// entire event stream or coupling Up-arrow history to the visible tail.
    async fn recent_user_messages(
        &self,
        session_id: Uuid,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        let mut messages = self
            .read(session_id)
            .await?
            .into_iter()
            .filter(|event| {
                matches!(
                    event.kind,
                    SessionEventKind::Message {
                        actor: crate::EventActor::User,
                        status: crate::MessageStatus::Complete,
                        ..
                    }
                )
            })
            .rev()
            .take(limit)
            .collect::<Vec<_>>();
        messages.reverse();
        Ok(messages)
    }
    async fn state(&self, session_id: Uuid) -> Result<SessionState>;
    /// Number of leading events this session inherited from a fork parent.
    ///
    /// Reads renumber inherited events into the child's own sequence space, so
    /// this is the only way to tell what the session actually authored.
    async fn inherited_event_count(&self, _session_id: Uuid) -> Result<u64> {
        Ok(0)
    }
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
    async fn attach_workspace(
        &self,
        binding: SessionWorkspaceBinding,
    ) -> Result<SessionWorkspaceBinding> {
        anyhow::bail!(
            "session store cannot attach session {} to workspace {}",
            binding.session_id,
            binding.workspace_id
        )
    }
    async fn workspace_binding(
        &self,
        _session_id: Uuid,
    ) -> Result<Option<SessionWorkspaceBinding>> {
        Ok(None)
    }
    /// Return the durable autonomous runtime journal when this session store
    /// is backed by SQLite. Other implementations may leave it unavailable.
    async fn autonomy_store(&self) -> Result<Option<crate::SqliteAutonomyStore>> {
        Ok(None)
    }
    /// Return the workspace projection on the same durable authority when the
    /// store supports it. Keeping this optional preserves the trait's small
    /// in-memory test seam without allowing production sessions to silently
    /// create a second workspace database.
    async fn workspace_store(&self) -> Result<Option<crate::SqliteWorkspaceStore>> {
        Ok(None)
    }
}

#[derive(Clone)]
pub struct SqliteSessionStore {
    pool: SqlitePool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionStoreHealth {
    pub integrity: String,
    pub journal_mode: String,
    pub synchronous: i64,
    pub foreign_keys: bool,
    pub wal_busy: i64,
    pub wal_log_frames: i64,
    pub wal_checkpointed_frames: i64,
    pub sessions: i64,
    pub events: i64,
    pub actions: i64,
    pub payloads: i64,
    pub projection_version: i32,
}

impl SessionStoreHealth {
    pub fn is_ready(&self) -> bool {
        self.integrity == "ok"
            && self.journal_mode.eq_ignore_ascii_case("wal")
            && self.synchronous >= 2
            && self.foreign_keys
            && self.wal_busy == 0
    }
}

impl SqliteSessionStore {
    pub(crate) fn from_pool(pool: SqlitePool) -> Self {
        Self { pool }
    }

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
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(SQLITE_BUSY_TIMEOUT)
            .foreign_keys(true);
        let open_error_context =
            || format!("failed to open SQLite session store {}", path.display());
        let schema_deadline = std::time::Instant::now() + SQLITE_BUSY_TIMEOUT;
        let pool = loop {
            match SqlitePoolOptions::new()
                .max_connections(8)
                .connect_with(options.clone())
                .await
            {
                Ok(pool) => break pool,
                Err(error)
                    if sqlite_lock_text(&error.to_string())
                        && std::time::Instant::now() < schema_deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(error) => return Err(anyhow::Error::new(error).context(open_error_context())),
            }
        };
        let store = Self { pool };
        let schema_deadline = std::time::Instant::now() + SQLITE_BUSY_TIMEOUT;
        loop {
            match store.ensure_schema().await {
                Ok(()) => break,
                Err(error)
                    if sqlite_schema_lock(&error)
                        && std::time::Instant::now() < schema_deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(store)
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Open the provider-neutral team policy journal on this same SQLite
    /// authority. Keeping one pool makes policy decisions and session/event
    /// recovery share the same WAL, foreign-key, and durability settings.
    pub async fn team_policy_store(&self) -> Result<crate::SqliteTeamPolicyStore> {
        crate::SqliteTeamPolicyStore::open(self.pool.clone()).await
    }

    /// Open the provider-neutral autonomous runtime journal on this same
    /// SQLite authority. Jobs, checkpoints, sessions, and their event log
    /// therefore share one WAL and one crash-recovery boundary.
    pub async fn autonomy_store(&self) -> Result<crate::SqliteAutonomyStore> {
        crate::SqliteAutonomyStore::open(self.pool.clone()).await
    }

    /// Persist host launch authorization in the canonical SQLite authority.
    /// A launch id is immutable: retries with the same metadata are harmless,
    /// while reuse with different metadata is rejected.
    pub async fn persist_host_launch_metadata(
        &self,
        session_id: Uuid,
        metadata: &serde_json::Value,
    ) -> Result<()> {
        ensure!(
            metadata.is_object(),
            "host launch metadata must be an object"
        );
        let metadata_json = serde_json::to_string(metadata)?;
        ensure!(
            metadata_json.len() <= MAX_HOST_LAUNCH_METADATA_BYTES,
            "host launch metadata exceeds {MAX_HOST_LAUNCH_METADATA_BYTES} bytes"
        );
        let mut transaction = self.begin_write().await?;
        let existing: Option<String> =
            sqlx::query_scalar("select metadata_json from host_launches where session_id=?")
                .bind(session_id.to_string())
                .fetch_optional(&mut *transaction)
                .await?;
        if let Some(existing) = existing {
            ensure!(
                existing == metadata_json,
                "session launch metadata already exists for a different launch request"
            );
        } else {
            let now = Utc::now().to_rfc3339();
            sqlx::query(
                "insert into host_launches \
                 (session_id, metadata_json, created_at, updated_at) values (?, ?, ?, ?)",
            )
            .bind(session_id.to_string())
            .bind(&metadata_json)
            .bind(&now)
            .bind(&now)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    /// Load immutable host launch authorization, rejecting malformed rows
    /// instead of treating them as a missing session.
    pub async fn load_host_launch_metadata(
        &self,
        session_id: Uuid,
    ) -> Result<Option<serde_json::Value>> {
        let metadata: Option<String> =
            sqlx::query_scalar("select metadata_json from host_launches where session_id=?")
                .bind(session_id.to_string())
                .fetch_optional(&self.pool)
                .await?;
        metadata
            .map(|metadata| {
                ensure!(
                    metadata.len() <= MAX_HOST_LAUNCH_METADATA_BYTES,
                    "host launch metadata exceeds {MAX_HOST_LAUNCH_METADATA_BYTES} bytes"
                );
                serde_json::from_str(&metadata)
                    .context("host launch metadata contains invalid JSON")
            })
            .transpose()
    }

    /// Check the durable authority without reading prompt or tool payload data.
    pub async fn health(&self) -> Result<SessionStoreHealth> {
        let integrity: String = sqlx::query_scalar("pragma quick_check")
            .fetch_one(&self.pool)
            .await?;
        let journal_mode: String = sqlx::query_scalar("pragma journal_mode")
            .fetch_one(&self.pool)
            .await?;
        let synchronous: i64 = sqlx::query_scalar("pragma synchronous")
            .fetch_one(&self.pool)
            .await?;
        let foreign_keys: i64 = sqlx::query_scalar("pragma foreign_keys")
            .fetch_one(&self.pool)
            .await?;
        let (wal_busy, wal_log_frames, wal_checkpointed_frames): (i64, i64, i64) =
            sqlx::query_as("pragma wal_checkpoint(passive)")
                .fetch_one(&self.pool)
                .await?;
        let sessions: i64 = sqlx::query_scalar("select count(*) from sessions")
            .fetch_one(&self.pool)
            .await?;
        let events: i64 = sqlx::query_scalar("select count(*) from session_events")
            .fetch_one(&self.pool)
            .await?;
        let actions: i64 = sqlx::query_scalar("select count(*) from session_actions")
            .fetch_one(&self.pool)
            .await?;
        let payloads: i64 = sqlx::query_scalar("select count(*) from session_payloads")
            .fetch_one(&self.pool)
            .await?;
        Ok(SessionStoreHealth {
            integrity,
            journal_mode,
            synchronous,
            foreign_keys: foreign_keys != 0,
            wal_busy,
            wal_log_frames,
            wal_checkpointed_frames,
            sessions,
            events,
            actions,
            payloads,
            projection_version: SESSION_PROJECTION_VERSION,
        })
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

    /// Create a new execution session directly inside an existing or
    /// caller-selected workspace. This is only valid before the session has
    /// events, so a resumed transcript can never silently move workspaces.
    pub async fn create_session_in_workspace(
        &self,
        session_id: Uuid,
        workspace_id: Uuid,
    ) -> Result<SessionWorkspaceBinding> {
        self.create_session(session_id).await?;
        let attached_at = Utc::now();
        sqlx::query(
            "update session_workspace_bindings \
             set workspace_id=?, participant_id=?, host_id=null, attached_at=? \
             where session_id=? and workspace_id=? and participant_id=?",
        )
        .bind(workspace_id.to_string())
        .bind(session_id.to_string())
        .bind(attached_at.to_rfc3339())
        .bind(session_id.to_string())
        .bind(session_id.to_string())
        .bind(session_id.to_string())
        .execute(&self.pool)
        .await?
        .rows_affected()
        .eq(&1)
        .then_some(())
        .context("new session workspace binding was not in its default state")?;
        Ok(SessionWorkspaceBinding {
            session_id,
            workspace_id,
            participant_id: session_id,
            host_id: None,
            attached_at,
        })
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

            create table if not exists session_actions (
                action_id text primary key,
                session_id text not null references sessions(id) on delete cascade,
                action_kind text not null,
                state text not null,
                delivery_policy text not null,
                wake_policy text not null,
                payload_json text not null,
                attempt integer not null default 0,
                error text,
                created_at text not null,
                updated_at text not null,
                accepted_at text,
                delivered_at text,
                completed_at text,
                lease_owner text,
                lease_token text,
                lease_heartbeat_at text,
                lease_expires_at text
            );

            create index if not exists idx_session_actions_pending
                on session_actions (session_id, state, created_at);

            create table if not exists session_action_transitions (
                action_id text not null references session_actions(action_id) on delete cascade,
                session_id text not null references sessions(id) on delete cascade,
                transition_no integer not null,
                from_state text,
                to_state text not null,
                error text,
                created_at text not null,
                primary key (action_id, transition_no)
            );

            create index if not exists idx_session_action_transitions_session
                on session_action_transitions (session_id, created_at, action_id);

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

            create index if not exists idx_session_events_fork_inheritable
                on session_events (session_id, sequence)
                where fork_inheritable = 1;

            create index if not exists idx_session_events_recovery
                on session_events (session_id, sequence)
                where recovery_relevant = 1;

            create index if not exists idx_session_events_subagent_recovery
                on session_events (
                    session_id,
                    event_kind,
                    json_extract(event_json, '$.kind.agent.session_id'),
                    sequence desc
                ) where event_kind = 'subagent_activity';

            create index if not exists idx_sessions_activity
                on sessions (updated_at desc);

            create table if not exists session_workspace_bindings (
                session_id text primary key references sessions(id) on delete cascade,
                workspace_id text not null,
                participant_id text not null,
                host_id text,
                attached_at text not null
            );

            create index if not exists idx_session_workspace_bindings_workspace
                on session_workspace_bindings (workspace_id, session_id);

            create table if not exists host_launches (
                session_id text primary key,
                metadata_json text not null,
                created_at text not null,
                updated_at text not null
            );
            "#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "create table if not exists borg_session_schema (\
                id integer primary key check(id=1),\
                version integer not null\
            )",
        )
        .execute(&self.pool)
        .await?;
        // Keep the only supported forward migration at the same SQLite
        // boundary as its schema marker. The lease fields are nullable and
        // additive, so this is safe for existing action rows and idempotent if
        // a previous process was interrupted after ALTER TABLE but before the
        // marker update.
        let mut schema_transaction = self.begin_write().await?;
        let schema_version: Option<i64> =
            sqlx::query_scalar("select version from borg_session_schema where id=1")
                .fetch_optional(&mut *schema_transaction)
                .await?;
        if let Some(version) = schema_version {
            ensure!(
                version <= SESSION_SCHEMA_VERSION,
                "unsupported future Borg session schema version {version}; current is {SESSION_SCHEMA_VERSION}"
            );
        }
        Self::ensure_session_action_lease_columns(&mut schema_transaction).await?;
        match schema_version {
            Some(version) if version < SESSION_SCHEMA_VERSION => {
                sqlx::query("update borg_session_schema set version=? where id=1")
                    .bind(SESSION_SCHEMA_VERSION)
                    .execute(&mut *schema_transaction)
                    .await?;
            }
            Some(_) => {}
            None => {
                sqlx::query("insert into borg_session_schema(id,version) values(1,?)")
                    .bind(SESSION_SCHEMA_VERSION)
                    .execute(&mut *schema_transaction)
                    .await?;
            }
        }
        schema_transaction.commit().await?;
        self.validate_current_schema().await?;
        sqlx::query(
            "create index if not exists idx_session_actions_lease_expiry \
             on session_actions (state, lease_expires_at, created_at)",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "create index if not exists idx_sessions_root_activity \
             on sessions (owner_session_id, updated_at desc)",
        )
        .execute(&self.pool)
        .await?;
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "insert into session_workspace_bindings \
             (session_id, workspace_id, participant_id, attached_at) \
             select id, coalesce(owner_session_id, parent_session_id, id), id, ? from sessions \
             where true \
             on conflict(session_id) do nothing",
        )
        .bind(now)
        .execute(&self.pool)
        .await?;
        self.clear_terminal_live_state().await?;
        Ok(())
    }

    async fn ensure_session_action_lease_columns(
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<()> {
        let existing = sqlx::query("pragma table_info(session_actions)")
            .fetch_all(&mut **transaction)
            .await?
            .into_iter()
            .map(|row| row.get::<String, _>("name"))
            .collect::<HashSet<_>>();
        for (name, statement) in [
            (
                "lease_owner",
                "alter table session_actions add column lease_owner text",
            ),
            (
                "lease_token",
                "alter table session_actions add column lease_token text",
            ),
            (
                "lease_heartbeat_at",
                "alter table session_actions add column lease_heartbeat_at text",
            ),
            (
                "lease_expires_at",
                "alter table session_actions add column lease_expires_at text",
            ),
        ] {
            if !existing.contains(name) {
                sqlx::query(statement).execute(&mut **transaction).await?;
            }
        }
        Ok(())
    }

    /// The store rejects unsupported future schemas and schemas missing
    /// non-additive invariants. The one supported additive repair happens in
    /// `ensure_schema` before this validation runs.
    async fn validate_current_schema(&self) -> Result<()> {
        for (table, required_columns) in [
            (
                "sessions",
                &[
                    "id",
                    "parent_session_id",
                    "parent_cut_sequence",
                    "owner_session_id",
                    "inherited_event_count",
                    "next_sequence",
                    "live_revision",
                    "state_json",
                    "projection_version",
                    "created_at",
                    "updated_at",
                ][..],
            ),
            (
                "session_events",
                &[
                    "session_id",
                    "sequence",
                    "event_id",
                    "event_kind",
                    "event_json",
                    "projection_json",
                    "fork_inheritable",
                    "recovery_relevant",
                    "message_id",
                    "created_at",
                ][..],
            ),
            (
                "session_actions",
                &[
                    "action_id",
                    "session_id",
                    "action_kind",
                    "state",
                    "delivery_policy",
                    "wake_policy",
                    "payload_json",
                    "attempt",
                    "error",
                    "created_at",
                    "updated_at",
                    "accepted_at",
                    "delivered_at",
                    "completed_at",
                    "lease_owner",
                    "lease_token",
                    "lease_heartbeat_at",
                    "lease_expires_at",
                ][..],
            ),
        ] {
            let columns = sqlx::query(&format!("pragma table_info({table})"))
                .fetch_all(&self.pool)
                .await?
                .into_iter()
                .map(|row| row.get::<String, _>("name"))
                .collect::<HashSet<_>>();
            let missing = required_columns
                .iter()
                .filter(|column| !columns.contains(**column))
                .copied()
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                bail!(
                    "Borg session database schema is stale for {table}; missing columns: {}",
                    missing.join(", ")
                );
            }
        }
        Ok(())
    }

    /// Repair stores written before terminal boundaries cleared every streamed
    /// turn key. Keep the context-window snapshot because it remains useful
    /// while idle.
    async fn clear_terminal_live_state(&self) -> Result<()> {
        sqlx::query(
            "delete from session_live_state \
             where live_key <> 'context_window' \
               and session_id in ( \
                 select id from sessions \
                 where json_extract(state_json, '$.status') in \
                       ('ready', 'completed', 'failed', 'stopped') \
               )",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn register_child_session(
        &self,
        owner_session_id: Uuid,
        session_id: Uuid,
    ) -> Result<()> {
        anyhow::ensure!(
            self.contains_session(owner_session_id).await?,
            "owner session {owner_session_id} does not exist"
        );
        if !self.contains_session(session_id).await? {
            self.create_session(session_id).await?;
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
        let owner_workspace: Option<String> = sqlx::query_scalar(
            "select workspace_id from session_workspace_bindings where session_id=?",
        )
        .bind(owner_session_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        let owner_workspace = owner_workspace
            .with_context(|| format!("owner session {owner_session_id} has no workspace"))?;
        sqlx::query(
            "update session_workspace_bindings set workspace_id=?, participant_id=?, \
             attached_at=? where session_id=? and workspace_id=session_id and participant_id=session_id",
        )
        .bind(&owner_workspace)
        .bind(session_id.to_string())
        .bind(Utc::now().to_rfc3339())
        .bind(session_id.to_string())
        .execute(&self.pool)
        .await?;
        // Attach the child before the resumed actor can accept durable team
        // messages, including when the registration was retried.
        sqlx::query(
            "insert into session_workspace_bindings \
             (session_id, workspace_id, participant_id, attached_at) values (?, ?, ?, ?) \
             on conflict(session_id) do nothing",
        )
        .bind(session_id.to_string())
        .bind(&owner_workspace)
        .bind(session_id.to_string())
        .bind(Utc::now().to_rfc3339())
        .execute(&self.pool)
        .await?;
        let binding = self
            .workspace_binding(session_id)
            .await?
            .with_context(|| format!("child session {session_id} has no workspace"))?;
        anyhow::ensure!(
            binding.workspace_id.to_string() == owner_workspace
                && binding.participant_id == session_id,
            "child session {session_id} is attached outside owner workspace {owner_workspace}"
        );
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

    fn composed_event_slice<'a>(
        &'a self,
        session_id: Uuid,
        before_or_at: Option<u64>,
        fork_inheritable_only: bool,
        skip: u64,
        limit: usize,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<StoredEvent>>> + Send + 'a>> {
        Box::pin(async move {
            if limit == 0 {
                return Ok(Vec::new());
            }
            let session = self.session_row(session_id).await?;
            let logical_limit = before_or_at
                .unwrap_or(session.next_sequence.saturating_sub(1))
                .min(session.next_sequence.saturating_sub(1));
            let inherited_limit = logical_limit.min(session.inherited_event_count);
            let mut events = Vec::with_capacity(limit);

            if skip < inherited_limit
                && let (Some(parent), Some(cut)) =
                    (session.parent_session_id, session.parent_cut_sequence)
            {
                let inherited_take = usize::try_from(inherited_limit - skip)
                    .unwrap_or(usize::MAX)
                    .min(limit);
                events.extend(
                    self.composed_event_slice(parent, Some(cut), true, skip, inherited_take)
                        .await?,
                );
            }

            let remaining = limit.saturating_sub(events.len());
            if remaining == 0 || logical_limit <= session.inherited_event_count {
                return Ok(events);
            }

            let rows = if fork_inheritable_only {
                let local_skip = skip.saturating_sub(inherited_limit);
                sqlx::query(
                    "select event_json, fork_inheritable from session_events \
                     where session_id = ? and sequence > ? and sequence <= ? \
                     and fork_inheritable = 1 order by sequence limit ? offset ?",
                )
                .bind(session_id.to_string())
                .bind(i64::try_from(session.inherited_event_count).unwrap_or(i64::MAX))
                .bind(i64::try_from(logical_limit).unwrap_or(i64::MAX))
                .bind(i64::try_from(remaining).unwrap_or(i64::MAX))
                .bind(i64::try_from(local_skip).unwrap_or(i64::MAX))
                .fetch_all(&self.pool)
                .await?
            } else {
                let first_sequence = skip.max(inherited_limit).saturating_add(1);
                sqlx::query(
                    "select event_json, fork_inheritable from session_events \
                     where session_id = ? and sequence >= ? and sequence <= ? \
                     order by sequence limit ?",
                )
                .bind(session_id.to_string())
                .bind(i64::try_from(first_sequence).unwrap_or(i64::MAX))
                .bind(i64::try_from(logical_limit).unwrap_or(i64::MAX))
                .bind(i64::try_from(remaining).unwrap_or(i64::MAX))
                .fetch_all(&self.pool)
                .await?
            };
            for row in rows {
                events.push(StoredEvent {
                    event: serde_json::from_str(row.try_get("event_json")?)?,
                    fork_inheritable: row.try_get::<i64, _>("fork_inheritable")? != 0,
                });
            }
            Ok(events)
        })
    }

    async fn projected_event_slice(
        &self,
        session_id: Uuid,
        sequence: u64,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        self.composed_event_slice(session_id, None, false, sequence, limit)
            .await?
            .into_iter()
            .enumerate()
            .map(|(index, stored)| {
                let mut event = stored.event;
                if event.session_id != session_id {
                    event.id = inherited_event_id(session_id, event.id);
                    event.session_id = session_id;
                }
                event.sequence = sequence
                    .saturating_add(u64::try_from(index).unwrap_or(u64::MAX))
                    .saturating_add(1);
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
                    // The parent cut already identifies the exact inherited
                    // prefix. Replaying `composed_events` here materialized
                    // every tool/subagent payload in that prefix just to
                    // discard the non-recovery rows below. On large forked
                    // sessions that turns resume into a multi-gigabyte scan.
                    // Recurse through the recovery-only path instead; it
                    // preserves the same cut while letting each generation
                    // apply its indexed recovery filter.
                    let mut inherited = self.composed_recovery_events(parent, Some(cut)).await?;
                    inherited.retain(|event| event.fork_inheritable);
                    inherited.truncate(usize::try_from(inherited_limit).unwrap_or(usize::MAX));
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
                     and recovery_relevant = 1 \
                     and (event_kind != 'subagent_activity' or sequence in ( \
                       select max(sequence) from session_events \
                       where session_id = ? and sequence > ? and sequence <= ? \
                       and event_kind = 'subagent_activity' \
                       group by json_extract(event_json, '$.kind.agent.session_id') \
                     )) order by sequence",
                )
                .bind(session_id.to_string())
                .bind(i64::try_from(session.inherited_event_count).unwrap_or(i64::MAX))
                .bind(i64::try_from(logical_limit).unwrap_or(i64::MAX))
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
        // A compaction checkpoint in a resumed/forked session is normally a
        // local durable event. Its projection_json is already the exact
        // SessionState at that boundary, so do not rebuild the entire
        // inherited transcript just to fork after it. The old recursive path
        // is retained for checkpoints that fall inside inherited ancestry.
        let cut = sequence.saturating_sub(1);
        if cut > parent.inherited_event_count {
            let projection = sqlx::query(
                "select projection_json, created_at from session_events \
                 where session_id = ? and sequence > ? and sequence <= ? \
                 order by sequence desc limit 1",
            )
            .bind(parent_session_id.to_string())
            .bind(i64::try_from(parent.inherited_event_count).unwrap_or(i64::MAX))
            .bind(i64::try_from(cut).unwrap_or(i64::MAX))
            .fetch_optional(&self.pool)
            .await?;
            if let Some(row) = projection {
                let local_inherited: i64 = sqlx::query_scalar(
                    "select count(*) from session_events \
                     where session_id = ? and sequence > ? and sequence <= ? \
                     and fork_inheritable = 1",
                )
                .bind(parent_session_id.to_string())
                .bind(i64::try_from(parent.inherited_event_count).unwrap_or(i64::MAX))
                .bind(i64::try_from(cut).unwrap_or(i64::MAX))
                .fetch_one(&self.pool)
                .await?;
                let inherited_event_count = parent
                    .inherited_event_count
                    .saturating_add(u64::try_from(local_inherited).unwrap_or(0));
                let mut state: SessionState =
                    serde_json::from_str(row.try_get("projection_json")?)?;
                state.activity_at = Some(
                    DateTime::parse_from_rfc3339(row.try_get("created_at")?)?.with_timezone(&Utc),
                );
                state.latest_sequence = inherited_event_count;
                return Ok((inherited_event_count, state));
            }
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
        self.contains_message_in(session_id, message_id, before_or_at, false)
    }

    /// `inherited_only` restricts the search to what a descendant actually
    /// inherits.  A session owns every event it appended, but a fork inherits
    /// only the fork-inheritable ones, so a queue entry left below the cut is
    /// not part of the fork's history.
    fn contains_message_in<'a>(
        &'a self,
        session_id: Uuid,
        message_id: Uuid,
        before_or_at: Option<u64>,
        inherited_only: bool,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>> {
        Box::pin(async move {
            let session = self.session_row(session_id).await?;
            let limit = before_or_at.unwrap_or(session.next_sequence.saturating_sub(1));
            let found: i64 = sqlx::query_scalar(if inherited_only {
                "select exists(select 1 from session_events \
                 where session_id = ? and message_id = ? and sequence <= ? \
                   and fork_inheritable = 1)"
            } else {
                "select exists(select 1 from session_events \
                 where session_id = ? and message_id = ? and sequence <= ?)"
            })
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
            // A cut within the compacted inherited prefix is uncommon; resolve
            // that bounded prefix to preserve exact lineage semantics. The
            // composed view already drops what the fork did not inherit.
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
            self.contains_message_in(parent, message_id, Some(cut), true)
                .await
        })
    }

    fn compact_payloads<'a>(
        &'a self,
        transaction: &'a mut Transaction<'_, Sqlite>,
        event: &'a SessionEvent,
    ) -> Pin<Box<dyn Future<Output = Result<SessionEvent>> + Send + 'a>> {
        Box::pin(async move {
            let mut compact = event.clone();
            match &mut compact.kind {
                SessionEventKind::ToolStarted {
                    input, input_ref, ..
                } if input_ref.is_none() => {
                    let bytes = serde_json::to_vec(input)?;
                    if bytes.len() > INLINE_SESSION_PAYLOAD_BYTES {
                        let payload = store_payload(
                            transaction,
                            event,
                            SessionPayloadKind::ToolInput,
                            &bytes,
                        )
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
                SessionEventKind::SubagentActivity {
                    event: Some(child_event),
                    ..
                } => {
                    **child_event = self.compact_payloads(transaction, child_event).await?;
                }
                _ => {}
            }
            Ok(compact)
        })
    }

    async fn append_live(&self, mut event: SessionEvent) -> Result<SessionEvent> {
        let live_key = event
            .kind
            .live_state_key()
            .context("coalesced session event has no live-state key")?;
        event.sequence = 0;
        let mut transaction = self.begin_write().await?;
        let row = sqlx::query("select live_revision, state_json from sessions where id = ?")
            .bind(event.session_id.to_string())
            .fetch_optional(&mut *transaction)
            .await?
            .with_context(|| format!("session {} does not exist", event.session_id))?;
        let current_revision = row.try_get::<i64, _>("live_revision")?;
        let state: SessionState = serde_json::from_str(row.try_get("state_json")?)?;
        let turn_live_allowed = matches!(
            state.status,
            Some(SessionStatus::Running | SessionStatus::WaitingForApproval)
        );
        if !turn_live_allowed {
            // A provider task can finish after the actor has published its
            // terminal status. Do not let that delayed snapshot resurrect a
            // responding message or reasoning disclosure in an idle session.
            sqlx::query(
                "delete from session_live_state \
                 where session_id = ? and live_key <> 'context_window'",
            )
            .bind(event.session_id.to_string())
            .execute(&mut *transaction)
            .await?;
            if live_key != "context_window" {
                transaction.commit().await?;
                return Ok(event);
            }
        }
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
            if text.starts_with(accumulated.as_str()) {
                accumulated.clear();
                accumulated.push_str(text);
            } else if !accumulated.starts_with(text) {
                accumulated.push_str(text);
            }
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
        // Coalesced events are delivered from the same canonical snapshot
        // that was committed to SQLite. This keeps local and reconnecting
        // clients on one wire representation and lets the UI replace a
        // cumulative reasoning snapshot instead of appending it twice.
        Ok(stored_event)
    }

    /// Ensure the durable action projection for an embedded workflow exists.
    /// Older event journals may predate the action projection, so this is an
    /// idempotent repair at the same SQLite boundary rather than a migration
    /// or a second source of workflow truth.
    pub(crate) async fn ensure_workflow_action(
        &self,
        session_id: Uuid,
        workflow_id: Uuid,
        payload: &serde_json::Value,
    ) -> Result<SessionAction> {
        let mut transaction = self.begin_write().await?;
        if let Some(row) =
            sqlx::query("select * from session_actions where action_id=? and session_id=?")
                .bind(workflow_id.to_string())
                .bind(session_id.to_string())
                .fetch_optional(&mut *transaction)
                .await?
        {
            let action = decode_action(&row)?;
            ensure!(
                action.kind == crate::SessionActionKind::Workflow && action.payload == *payload,
                "workflow action {workflow_id} has conflicting durable metadata"
            );
            ensure!(
                !action.state.is_terminal(),
                "workflow action {workflow_id} is terminal without a workflow completion record"
            );
            transaction.commit().await?;
            return Ok(action);
        }
        let exists: i64 = sqlx::query_scalar("select exists(select 1 from sessions where id=?)")
            .bind(session_id.to_string())
            .fetch_one(&mut *transaction)
            .await?;
        ensure!(exists != 0, "session {session_id} does not exist");
        let action = SessionAction::new(
            workflow_id,
            session_id,
            crate::SessionActionKind::Workflow,
            crate::ActionDeliveryPolicy::WhenRunIdle,
            crate::ActionWakePolicy::Immediate,
            payload.clone(),
        );
        create_action_and_advance(&mut transaction, action, SessionActionState::Running, None)
            .await?;
        let row = sqlx::query("select * from session_actions where action_id=? and session_id=?")
            .bind(workflow_id.to_string())
            .bind(session_id.to_string())
            .fetch_one(&mut *transaction)
            .await?;
        let action = decode_action(&row)?;
        transaction.commit().await?;
        Ok(action)
    }

    /// Append a durable event and its action transition only while the caller
    /// owns the workflow lease. This is the fenced commit point: a stale
    /// worker cannot publish a terminal workflow event after another worker
    /// has reclaimed the execution.
    pub(crate) async fn append_with_action_lease(
        &self,
        event: SessionEvent,
        action_id: Uuid,
        lease_owner: &str,
        lease_token: Uuid,
    ) -> Result<SessionEvent> {
        ensure!(
            event.kind.persistence() == EventPersistence::Durable,
            "leased workflow events must be durable"
        );
        let mut transaction = self.begin_write().await?;
        let action = load_action_for_update(&mut transaction, event.session_id, action_id).await?;
        validate_live_lease(&action, lease_owner, lease_token, Utc::now())?;
        let event = self
            .append_durable_in_transaction(&mut transaction, event)
            .await?;
        transaction.commit().await?;
        Ok(event)
    }

    async fn append_durable_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        mut event: SessionEvent,
    ) -> Result<SessionEvent> {
        let row = sqlx::query("select next_sequence, state_json from sessions where id = ?")
            .bind(event.session_id.to_string())
            .fetch_optional(&mut **transaction)
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
        let compact_event = self.compact_payloads(transaction, &event).await?;
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
        .execute(&mut **transaction)
        .await?;
        // Keep the action journal and the event proving its admission or
        // terminal boundary in the same SQLite transaction.
        sync_session_action(transaction, &event).await?;
        if event.kind.clears_live_turn_state() {
            sqlx::query(
                "delete from session_live_state \
                 where session_id = ? and live_key <> 'context_window'",
            )
            .bind(event.session_id.to_string())
            .execute(&mut **transaction)
            .await?;
        } else {
            for live_key in event.kind.cleared_live_state_keys() {
                sqlx::query("delete from session_live_state where session_id = ? and live_key = ?")
                    .bind(event.session_id.to_string())
                    .bind(live_key)
                    .execute(&mut **transaction)
                    .await?;
            }
        }
        sqlx::query(
            "update sessions set next_sequence = ?, state_json = ?, projection_version = 3, \
             updated_at = ? where id = ?",
        )
        .bind(i64::try_from(event.sequence.saturating_add(1)).unwrap_or(i64::MAX))
        .bind(projection_json)
        .bind(event.created_at.to_rfc3339())
        .bind(event.session_id.to_string())
        .execute(&mut **transaction)
        .await?;
        Ok(event)
    }
}

fn sqlite_schema_lock(error: &anyhow::Error) -> bool {
    sqlite_lock_text(&error.to_string())
}

fn enum_text<T: Serialize>(value: &T) -> Result<String> {
    Ok(serde_json::to_value(value)?
        .as_str()
        .context("session action enum did not serialize as a string")?
        .to_string())
}

fn parse_enum<T: DeserializeOwned>(value: &str) -> Result<T> {
    Ok(serde_json::from_value(serde_json::Value::String(
        value.to_string(),
    ))?)
}

fn parse_timestamp(value: Option<&str>) -> Result<Option<DateTime<Utc>>> {
    value
        .map(|value| {
            DateTime::parse_from_rfc3339(value)
                .map(|timestamp| timestamp.with_timezone(&Utc))
                .map_err(Into::into)
        })
        .transpose()
}

fn decode_action(row: &SqliteRow) -> Result<SessionAction> {
    Ok(SessionAction {
        action_id: parse_uuid(row.try_get("action_id")?)?,
        session_id: parse_uuid(row.try_get("session_id")?)?,
        kind: parse_enum(row.try_get("action_kind")?)?,
        state: parse_enum(row.try_get("state")?)?,
        delivery: parse_enum(row.try_get("delivery_policy")?)?,
        wake: parse_enum(row.try_get("wake_policy")?)?,
        payload: serde_json::from_str(row.try_get("payload_json")?)?,
        attempt: u32::try_from(row.try_get::<i64, _>("attempt")?)
            .context("negative session action attempt")?,
        error: row.try_get("error")?,
        created_at: parse_timestamp(row.try_get("created_at")?)?
            .context("missing action created_at")?,
        updated_at: parse_timestamp(row.try_get("updated_at")?)?
            .context("missing action updated_at")?,
        accepted_at: parse_timestamp(row.try_get("accepted_at")?)?,
        delivered_at: parse_timestamp(row.try_get("delivered_at")?)?,
        completed_at: parse_timestamp(row.try_get("completed_at")?)?,
        lease_owner: row.try_get("lease_owner")?,
        lease_token: row
            .try_get::<Option<&str>, _>("lease_token")?
            .map(parse_uuid)
            .transpose()?,
        lease_heartbeat_at: parse_timestamp(row.try_get("lease_heartbeat_at")?)?,
        lease_expires_at: parse_timestamp(row.try_get("lease_expires_at")?)?,
    })
}

fn decode_action_transition(row: &SqliteRow) -> Result<SessionActionTransition> {
    Ok(SessionActionTransition {
        action_id: parse_uuid(row.try_get("action_id")?)?,
        session_id: parse_uuid(row.try_get("session_id")?)?,
        transition_no: u64::try_from(row.try_get::<i64, _>("transition_no")?)
            .context("negative action transition number")?,
        from: row
            .try_get::<Option<&str>, _>("from_state")?
            .map(parse_enum)
            .transpose()?,
        to: parse_enum(row.try_get("to_state")?)?,
        error: row.try_get("error")?,
        created_at: parse_timestamp(Some(row.try_get("created_at")?))?
            .context("missing action transition created_at")?,
    })
}

async fn sync_session_action(
    transaction: &mut Transaction<'_, Sqlite>,
    event: &SessionEvent,
) -> Result<()> {
    match &event.kind {
        SessionEventKind::SessionConfigured { .. } => {
            let action = SessionAction::new(
                event.id,
                event.session_id,
                crate::SessionActionKind::ProviderChange,
                crate::ActionDeliveryPolicy::WhenRunIdle,
                crate::ActionWakePolicy::Immediate,
                serde_json::to_value(&event.kind)?,
            );
            create_action_and_advance(transaction, action, SessionActionState::Completed, None)
                .await?;
        }
        SessionEventKind::Message {
            message_id,
            actor,
            text,
            attachments,
            status: MessageStatus::Queued,
            delivery,
        } if matches!(actor, crate::EventActor::User | crate::EventActor::System) => {
            let delivery = delivery.unwrap_or(crate::PromptDelivery::Queue);
            let (kind, delivery_policy, wake_policy) = match (*actor, delivery) {
                (crate::EventActor::System, _) => (
                    crate::SessionActionKind::AgentMessage,
                    crate::ActionDeliveryPolicy::NextTurnBoundary,
                    crate::ActionWakePolicy::OnLowerBoundary,
                ),
                (_, crate::PromptDelivery::Steer) => (
                    crate::SessionActionKind::Steering,
                    crate::ActionDeliveryPolicy::WhenRunIdle,
                    crate::ActionWakePolicy::Immediate,
                ),
                (_, crate::PromptDelivery::Queue) => (
                    crate::SessionActionKind::Prompt,
                    crate::ActionDeliveryPolicy::NextTurnBoundary,
                    crate::ActionWakePolicy::OnLowerBoundary,
                ),
            };
            let mut action = SessionAction::new(
                *message_id,
                event.session_id,
                kind,
                delivery_policy,
                wake_policy,
                serde_json::json!({
                    "message_id": message_id,
                    "text": text,
                    "attachments": attachments,
                    "delivery": delivery,
                }),
            );
            action.created_at = event.created_at;
            action.updated_at = event.created_at;
            action.transition(
                Some(SessionActionState::Queued),
                SessionActionState::Admitted,
                None,
            )?;
            action.created_at = event.created_at;
            insert_action_row(transaction, &action).await?;
        }
        SessionEventKind::TurnStarted { message_id, .. } => {
            advance_action(
                transaction,
                event.session_id,
                *message_id,
                SessionActionState::Running,
                None,
            )
            .await?;
        }
        SessionEventKind::TurnCompleted {
            message_id, error, ..
        } => {
            let target = if error.is_some() {
                SessionActionState::Failed
            } else {
                SessionActionState::Completed
            };
            advance_action(
                transaction,
                event.session_id,
                *message_id,
                target,
                error.clone(),
            )
            .await?;
        }
        SessionEventKind::PromptRecalled { message_id, .. } => {
            let Some(row) = sqlx::query("select * from session_actions where action_id = ?")
                .bind(message_id.to_string())
                .fetch_optional(&mut **transaction)
                .await?
            else {
                return Ok(());
            };
            let mut action = decode_action(&row)?;
            if !action.state.is_terminal() {
                let current = action.state;
                action.transition(
                    Some(current),
                    SessionActionState::Cancelled,
                    Some("recalled before execution".to_string()),
                )?;
                append_action_transition(transaction, &action, current, action.error.clone())
                    .await?;
                update_action_row(transaction, &action).await?;
            }
        }
        SessionEventKind::ProviderEvent { kind, payload, .. } if kind == "context_compaction" => {
            match payload.get("status").and_then(serde_json::Value::as_str) {
                Some("started") => {
                    let action = SessionAction::new(
                        event.id,
                        event.session_id,
                        crate::SessionActionKind::Compaction,
                        crate::ActionDeliveryPolicy::WhenRunIdle,
                        crate::ActionWakePolicy::Immediate,
                        payload.clone(),
                    );
                    create_action_and_advance(
                        transaction,
                        action,
                        SessionActionState::Running,
                        None,
                    )
                    .await?;
                }
                Some("completed") => {
                    if let Some(action_id) = latest_action_id(
                        transaction,
                        event.session_id,
                        crate::SessionActionKind::Compaction,
                    )
                    .await?
                    {
                        advance_action(
                            transaction,
                            event.session_id,
                            action_id,
                            SessionActionState::Completed,
                            None,
                        )
                        .await?;
                    }
                }
                _ => {}
            }
        }
        SessionEventKind::ProviderEvent { kind, payload, .. }
            if kind == "context_compaction_failed" =>
        {
            if let Some(action_id) = latest_action_id(
                transaction,
                event.session_id,
                crate::SessionActionKind::Compaction,
            )
            .await?
            {
                advance_action(
                    transaction,
                    event.session_id,
                    action_id,
                    SessionActionState::Failed,
                    payload
                        .get("error")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string),
                )
                .await?;
            }
        }
        SessionEventKind::BluWorkflowStarted {
            workflow_id,
            source_hash,
            name,
        } => {
            let mut action = SessionAction::new(
                *workflow_id,
                event.session_id,
                crate::SessionActionKind::Workflow,
                crate::ActionDeliveryPolicy::WhenRunIdle,
                crate::ActionWakePolicy::Immediate,
                serde_json::json!({
                    "workflow_id": workflow_id,
                    "source_hash": source_hash,
                    "name": name,
                }),
            );
            action.created_at = event.created_at;
            action.updated_at = event.created_at;
            create_action_and_advance(transaction, action, SessionActionState::Running, None)
                .await?;
        }
        SessionEventKind::BluWorkflowCompleted {
            workflow_id,
            success,
            error,
            ..
        } => {
            advance_action(
                transaction,
                event.session_id,
                *workflow_id,
                if *success {
                    SessionActionState::Completed
                } else {
                    SessionActionState::Failed
                },
                error.clone(),
            )
            .await?;
        }
        SessionEventKind::ContextCleared
        | SessionEventKind::GoalUpdated { .. }
        | SessionEventKind::GoalCleared { .. }
        | SessionEventKind::PlanUpdated { .. }
        | SessionEventKind::SubagentControl { .. } => {
            let kind = if matches!(event.kind, SessionEventKind::ContextCleared) {
                crate::SessionActionKind::Revert
            } else {
                crate::SessionActionKind::Command
            };
            let action = SessionAction::new(
                event.id,
                event.session_id,
                kind,
                crate::ActionDeliveryPolicy::WhenRunIdle,
                crate::ActionWakePolicy::Immediate,
                serde_json::to_value(&event.kind)?,
            );
            create_action_and_advance(transaction, action, SessionActionState::Completed, None)
                .await?;
        }
        _ => {}
    }
    Ok(())
}

async fn create_action_and_advance(
    transaction: &mut Transaction<'_, Sqlite>,
    action: SessionAction,
    target: SessionActionState,
    error: Option<String>,
) -> Result<()> {
    let action_id = action.action_id;
    let session_id = action.session_id;
    insert_action_row(transaction, &action).await?;
    advance_action(transaction, session_id, action_id, target, error).await
}

async fn latest_action_id(
    transaction: &mut Transaction<'_, Sqlite>,
    session_id: Uuid,
    kind: crate::SessionActionKind,
) -> Result<Option<Uuid>> {
    sqlx::query_scalar(
        "select action_id from session_actions \
         where session_id=? and action_kind=? \
           and state not in ('completed', 'failed', 'cancelled') \
         order by created_at desc, action_id desc limit 1",
    )
    .bind(session_id.to_string())
    .bind(enum_text(&kind)?)
    .fetch_optional(&mut **transaction)
    .await?
    .map(|value: String| parse_uuid(&value))
    .transpose()
}

async fn insert_action_row(
    transaction: &mut Transaction<'_, Sqlite>,
    action: &SessionAction,
) -> Result<()> {
    if let Some(row) = sqlx::query("select * from session_actions where action_id = ?")
        .bind(action.action_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
    {
        let existing = decode_action(&row)?;
        anyhow::ensure!(
            existing.session_id == action.session_id,
            "action {} was reused by a different session",
            action.action_id
        );
        if existing.kind == crate::SessionActionKind::Steering
            && action.kind == crate::SessionActionKind::Prompt
            && same_prompt_payload_ignoring_delivery(&existing.payload, &action.payload)
        {
            // A rejected or interrupted active-turn steer is deliberately
            // promoted into the next-turn FIFO. It remains one user action;
            // only its delivery class changes. Keep the immutable message
            // identity/content check above and update the durable routing
            // projection in place so replay cannot duplicate the action.
            sqlx::query(
                "update session_actions set action_kind=?, delivery_policy=?, wake_policy=?, \
                 payload_json=?, updated_at=? where action_id=? and session_id=?",
            )
            .bind(enum_text(&action.kind)?)
            .bind(enum_text(&action.delivery)?)
            .bind(enum_text(&action.wake)?)
            .bind(serde_json::to_string(&action.payload)?)
            .bind(action.updated_at.to_rfc3339())
            .bind(action.action_id.to_string())
            .bind(action.session_id.to_string())
            .execute(&mut **transaction)
            .await?;
            return Ok(());
        }
        anyhow::ensure!(
            existing.kind == action.kind && existing.payload == action.payload,
            "action {} was reused with a different immutable payload",
            action.action_id
        );
        return Ok(());
    }
    sqlx::query(
        "insert into session_actions \
         (action_id, session_id, action_kind, state, delivery_policy, wake_policy, \
          payload_json, attempt, error, created_at, updated_at, accepted_at, delivered_at, \
          completed_at, lease_owner, lease_token, lease_heartbeat_at, lease_expires_at) \
         values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(action.action_id.to_string())
    .bind(action.session_id.to_string())
    .bind(enum_text(&action.kind)?)
    .bind(enum_text(&action.state)?)
    .bind(enum_text(&action.delivery)?)
    .bind(enum_text(&action.wake)?)
    .bind(serde_json::to_string(&action.payload)?)
    .bind(i64::from(action.attempt))
    .bind(action.error.as_deref())
    .bind(action.created_at.to_rfc3339())
    .bind(action.updated_at.to_rfc3339())
    .bind(action.accepted_at.map(|value| value.to_rfc3339()))
    .bind(action.delivered_at.map(|value| value.to_rfc3339()))
    .bind(action.completed_at.map(|value| value.to_rfc3339()))
    .bind(action.lease_owner.as_deref())
    .bind(action.lease_token.map(|value| value.to_string()))
    .bind(action.lease_heartbeat_at.map(|value| value.to_rfc3339()))
    .bind(action.lease_expires_at.map(|value| value.to_rfc3339()))
    .execute(&mut **transaction)
    .await?;
    insert_initial_action_transitions(transaction, action).await?;
    Ok(())
}

async fn insert_initial_action_transitions(
    transaction: &mut Transaction<'_, Sqlite>,
    action: &SessionAction,
) -> Result<()> {
    let queued = SessionActionState::Queued;
    insert_action_transition(
        transaction,
        action,
        None,
        queued,
        None,
        action.created_at,
        0,
    )
    .await?;
    if action.state != queued {
        insert_action_transition(
            transaction,
            action,
            Some(queued),
            action.state,
            action.error.clone(),
            action.accepted_at.unwrap_or(action.updated_at),
            1,
        )
        .await?;
    }
    Ok(())
}

async fn insert_action_transition(
    transaction: &mut Transaction<'_, Sqlite>,
    action: &SessionAction,
    from_state: Option<SessionActionState>,
    to_state: SessionActionState,
    error: Option<String>,
    created_at: DateTime<Utc>,
    transition_no: i64,
) -> Result<()> {
    sqlx::query(
        "insert into session_action_transitions \
         (action_id, session_id, transition_no, from_state, to_state, error, created_at) \
         values (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(action.action_id.to_string())
    .bind(action.session_id.to_string())
    .bind(transition_no)
    .bind(from_state.map(|value| enum_text(&value)).transpose()?)
    .bind(enum_text(&to_state)?)
    .bind(error.as_deref())
    .bind(created_at.to_rfc3339())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn append_action_transition(
    transaction: &mut Transaction<'_, Sqlite>,
    action: &SessionAction,
    from_state: SessionActionState,
    error: Option<String>,
) -> Result<()> {
    let next_no: i64 = sqlx::query_scalar(
        "select coalesce(max(transition_no) + 1, 0) \
         from session_action_transitions where action_id=? and session_id=?",
    )
    .bind(action.action_id.to_string())
    .bind(action.session_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    insert_action_transition(
        transaction,
        action,
        Some(from_state),
        action.state,
        error,
        action.updated_at,
        next_no,
    )
    .await
}

fn same_prompt_payload_ignoring_delivery(
    left: &serde_json::Value,
    right: &serde_json::Value,
) -> bool {
    left.get("message_id") == right.get("message_id")
        && left.get("text") == right.get("text")
        && left.get("attachments") == right.get("attachments")
}

async fn update_action_row(
    transaction: &mut Transaction<'_, Sqlite>,
    action: &SessionAction,
) -> Result<()> {
    sqlx::query(
        "update session_actions set state=?, attempt=?, error=?, updated_at=?, \
         accepted_at=?, delivered_at=?, completed_at=?, lease_owner=?, lease_token=?, \
         lease_heartbeat_at=?, lease_expires_at=? where action_id=? and session_id=?",
    )
    .bind(enum_text(&action.state)?)
    .bind(i64::from(action.attempt))
    .bind(action.error.as_deref())
    .bind(action.updated_at.to_rfc3339())
    .bind(action.accepted_at.map(|value| value.to_rfc3339()))
    .bind(action.delivered_at.map(|value| value.to_rfc3339()))
    .bind(action.completed_at.map(|value| value.to_rfc3339()))
    .bind(action.lease_owner.as_deref())
    .bind(action.lease_token.map(|value| value.to_string()))
    .bind(action.lease_heartbeat_at.map(|value| value.to_rfc3339()))
    .bind(action.lease_expires_at.map(|value| value.to_rfc3339()))
    .bind(action.action_id.to_string())
    .bind(action.session_id.to_string())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn load_action_for_update(
    transaction: &mut Transaction<'_, Sqlite>,
    session_id: Uuid,
    action_id: Uuid,
) -> Result<SessionAction> {
    sqlx::query("select * from session_actions where action_id = ? and session_id = ?")
        .bind(action_id.to_string())
        .bind(session_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .with_context(|| format!("action {action_id} does not exist in session {session_id}"))
        .and_then(|row| decode_action(&row))
}

fn validate_live_lease(
    action: &SessionAction,
    lease_owner: &str,
    lease_token: Uuid,
    now: DateTime<Utc>,
) -> Result<()> {
    anyhow::ensure!(
        action.lease_owner.as_deref() == Some(lease_owner)
            && action.lease_token == Some(lease_token),
        "action {} lease is not owned by {lease_owner}",
        action.action_id
    );
    anyhow::ensure!(
        !action.lease_expired_at(now),
        "action {} lease has expired",
        action.action_id
    );
    anyhow::ensure!(
        !action.state.is_terminal(),
        "action {} is already terminal",
        action.action_id
    );
    Ok(())
}

async fn transition_action_in_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    session_id: Uuid,
    action_id: Uuid,
    expected: Option<SessionActionState>,
    next: SessionActionState,
    error: Option<String>,
) -> Result<SessionAction> {
    let mut action = load_action_for_update(transaction, session_id, action_id).await?;
    let from_state = action.state;
    action.transition(expected, next, error)?;
    append_action_transition(transaction, &action, from_state, action.error.clone()).await?;
    update_action_row(transaction, &action).await?;
    Ok(action)
}

async fn advance_action(
    transaction: &mut Transaction<'_, Sqlite>,
    session_id: Uuid,
    action_id: Uuid,
    target: SessionActionState,
    error: Option<String>,
) -> Result<()> {
    let Some(row) =
        sqlx::query("select * from session_actions where action_id = ? and session_id = ?")
            .bind(action_id.to_string())
            .bind(session_id.to_string())
            .fetch_optional(&mut **transaction)
            .await?
    else {
        return Ok(());
    };
    let mut action = decode_action(&row)?;
    if action.state.is_terminal() {
        return Ok(());
    }
    while action.state != target {
        let from_state = action.state;
        let next = match action.state {
            SessionActionState::Queued => SessionActionState::Admitted,
            SessionActionState::Admitted => {
                if target == SessionActionState::Failed {
                    SessionActionState::Failed
                } else {
                    SessionActionState::Delivered
                }
            }
            SessionActionState::Delivered => {
                if target == SessionActionState::Failed {
                    SessionActionState::Failed
                } else {
                    SessionActionState::Preparing
                }
            }
            SessionActionState::Preparing => {
                if target == SessionActionState::Failed {
                    SessionActionState::Failed
                } else {
                    SessionActionState::Committing
                }
            }
            SessionActionState::Committing => SessionActionState::Running,
            SessionActionState::Running => target,
            SessionActionState::Completed
            | SessionActionState::Failed
            | SessionActionState::Cancelled => break,
        };
        action.transition(Some(action.state), next, error.clone())?;
        append_action_transition(transaction, &action, from_state, error.clone()).await?;
    }
    update_action_row(transaction, &action).await
}

fn sqlite_lock_text(error: &str) -> bool {
    let message = error.to_ascii_lowercase();
    message.contains("database is locked") || message.contains("database table is locked")
}

#[async_trait]
impl SessionStore for SqliteSessionStore {
    async fn autonomy_store(&self) -> Result<Option<crate::SqliteAutonomyStore>> {
        Ok(Some(
            crate::SqliteAutonomyStore::open(self.pool.clone()).await?,
        ))
    }

    async fn workspace_store(&self) -> Result<Option<crate::SqliteWorkspaceStore>> {
        Ok(Some(
            crate::SqliteWorkspaceStore::from_pool(self.pool.clone()).await?,
        ))
    }

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
        sqlx::query(
            "insert into session_workspace_bindings \
             (session_id, workspace_id, participant_id, attached_at) values (?, ?, ?, ?) \
             on conflict(session_id) do nothing",
        )
        .bind(session_id.to_string())
        .bind(session_id.to_string())
        .bind(session_id.to_string())
        .bind(Utc::now().to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn register_child_session(&self, owner_session_id: Uuid, session_id: Uuid) -> Result<()> {
        SqliteSessionStore::register_child_session(self, owner_session_id, session_id).await
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
        event = self
            .append_durable_in_transaction(&mut transaction, event)
            .await?;
        transaction.commit().await?;
        Ok(event)
    }

    async fn enqueue_action(&self, action: SessionAction) -> Result<SessionAction> {
        anyhow::ensure!(
            action.state == SessionActionState::Queued,
            "newly enqueued action {} must start queued",
            action.action_id
        );
        let mut transaction = self.begin_write().await?;
        if let Some(row) = sqlx::query("select * from session_actions where action_id = ?")
            .bind(action.action_id.to_string())
            .fetch_optional(&mut *transaction)
            .await?
        {
            let existing = decode_action(&row)?;
            anyhow::ensure!(
                existing.session_id == action.session_id
                    && existing.kind == action.kind
                    && existing.delivery == action.delivery
                    && existing.wake == action.wake
                    && existing.payload == action.payload,
                "action {} was reused with different immutable payload",
                action.action_id
            );
            transaction.commit().await?;
            return Ok(existing);
        }
        let exists: i64 = sqlx::query_scalar("select exists(select 1 from sessions where id = ?)")
            .bind(action.session_id.to_string())
            .fetch_one(&mut *transaction)
            .await?;
        anyhow::ensure!(exists != 0, "session {} does not exist", action.session_id);
        sqlx::query(
            "insert into session_actions \
             (action_id, session_id, action_kind, state, delivery_policy, wake_policy, \
              payload_json, attempt, error, created_at, updated_at, accepted_at, delivered_at, \
              completed_at, lease_owner, lease_token, lease_heartbeat_at, lease_expires_at) \
             values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(action.action_id.to_string())
        .bind(action.session_id.to_string())
        .bind(enum_text(&action.kind)?)
        .bind(enum_text(&action.state)?)
        .bind(enum_text(&action.delivery)?)
        .bind(enum_text(&action.wake)?)
        .bind(serde_json::to_string(&action.payload)?)
        .bind(i64::from(action.attempt))
        .bind(action.error.as_deref())
        .bind(action.created_at.to_rfc3339())
        .bind(action.updated_at.to_rfc3339())
        .bind(action.accepted_at.map(|value| value.to_rfc3339()))
        .bind(action.delivered_at.map(|value| value.to_rfc3339()))
        .bind(action.completed_at.map(|value| value.to_rfc3339()))
        .bind(action.lease_owner.as_deref())
        .bind(action.lease_token.map(|value| value.to_string()))
        .bind(action.lease_heartbeat_at.map(|value| value.to_rfc3339()))
        .bind(action.lease_expires_at.map(|value| value.to_rfc3339()))
        .execute(&mut *transaction)
        .await?;
        insert_initial_action_transitions(&mut transaction, &action).await?;
        transaction.commit().await?;
        Ok(action)
    }

    async fn transition_action(
        &self,
        session_id: Uuid,
        action_id: Uuid,
        expected: Option<SessionActionState>,
        next: SessionActionState,
        error: Option<String>,
    ) -> Result<SessionAction> {
        let mut transaction = self.begin_write().await?;
        let action = transition_action_in_transaction(
            &mut transaction,
            session_id,
            action_id,
            expected,
            next,
            error,
        )
        .await?;
        transaction.commit().await?;
        Ok(action)
    }

    async fn claim_action(
        &self,
        session_id: Uuid,
        action_id: Uuid,
        lease_owner: &str,
        lease_duration: Duration,
    ) -> Result<Option<SessionAction>> {
        anyhow::ensure!(
            !lease_owner.trim().is_empty(),
            "action lease owner is empty"
        );
        anyhow::ensure!(!lease_duration.is_zero(), "action lease duration is zero");
        let mut transaction = self.begin_write().await?;
        let Some(row) =
            sqlx::query("select * from session_actions where action_id = ? and session_id = ?")
                .bind(action_id.to_string())
                .bind(session_id.to_string())
                .fetch_optional(&mut *transaction)
                .await?
        else {
            transaction.commit().await?;
            return Ok(None);
        };
        let mut action = decode_action(&row)?;
        let now = Utc::now();
        if action.state.is_terminal() {
            transaction.commit().await?;
            return Ok(None);
        }
        if action.lease_owner.as_deref() == Some(lease_owner) && !action.lease_expired_at(now) {
            transaction.commit().await?;
            return Ok(Some(action));
        }
        if !action.lease_expired_at(now) {
            transaction.commit().await?;
            return Ok(None);
        }
        action.claim(lease_owner.to_string(), Uuid::new_v4(), now, lease_duration)?;
        update_action_row(&mut transaction, &action).await?;
        transaction.commit().await?;
        Ok(Some(action))
    }

    async fn heartbeat_action(
        &self,
        session_id: Uuid,
        action_id: Uuid,
        lease_owner: &str,
        lease_token: Uuid,
        lease_duration: Duration,
    ) -> Result<SessionAction> {
        let mut transaction = self.begin_write().await?;
        let mut action = load_action_for_update(&mut transaction, session_id, action_id).await?;
        action.heartbeat(lease_owner, lease_token, Utc::now(), lease_duration)?;
        update_action_row(&mut transaction, &action).await?;
        transaction.commit().await?;
        Ok(action)
    }

    async fn transition_claimed_action(
        &self,
        session_id: Uuid,
        action_id: Uuid,
        lease_owner: &str,
        lease_token: Uuid,
        expected: Option<SessionActionState>,
        next: SessionActionState,
        error: Option<String>,
    ) -> Result<SessionAction> {
        let mut transaction = self.begin_write().await?;
        let mut action = load_action_for_update(&mut transaction, session_id, action_id).await?;
        validate_live_lease(&action, lease_owner, lease_token, Utc::now())?;
        let from_state = action.state;
        action.transition(expected, next, error)?;
        append_action_transition(&mut transaction, &action, from_state, action.error.clone())
            .await?;
        update_action_row(&mut transaction, &action).await?;
        transaction.commit().await?;
        Ok(action)
    }

    async fn recover_expired_actions(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<SessionAction>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut transaction = self.begin_write().await?;
        let rows = sqlx::query(
            "select * from session_actions \
             where state in ('running', 'committing') \
               and (lease_expires_at is null or lease_expires_at <= ?) \
             order by lease_expires_at, created_at, action_id limit ?",
        )
        .bind(now.to_rfc3339())
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&mut *transaction)
        .await?;
        let mut recovered = Vec::with_capacity(rows.len());
        for row in rows {
            let mut action = decode_action(&row)?;
            if !action.lease_expired_at(now) {
                continue;
            }
            let from_state = action.state;
            action.transition(
                Some(from_state),
                SessionActionState::Queued,
                Some("action lease expired; requeued for recovery".to_string()),
            )?;
            append_action_transition(&mut transaction, &action, from_state, action.error.clone())
                .await?;
            update_action_row(&mut transaction, &action).await?;
            recovered.push(action);
        }
        transaction.commit().await?;
        Ok(recovered)
    }

    async fn action(&self, session_id: Uuid, action_id: Uuid) -> Result<Option<SessionAction>> {
        sqlx::query("select * from session_actions where action_id = ? and session_id = ?")
            .bind(action_id.to_string())
            .bind(session_id.to_string())
            .fetch_optional(&self.pool)
            .await?
            .map(|row| decode_action(&row))
            .transpose()
    }

    async fn action_transitions(
        &self,
        session_id: Uuid,
        action_id: Uuid,
    ) -> Result<Vec<SessionActionTransition>> {
        let rows = sqlx::query(
            "select * from session_action_transitions \
             where session_id=? and action_id=? order by transition_no",
        )
        .bind(session_id.to_string())
        .bind(action_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(decode_action_transition).collect()
    }

    async fn pending_actions(&self, session_id: Uuid, limit: usize) -> Result<Vec<SessionAction>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "select * from session_actions where session_id = ? \
             and state not in ('completed', 'failed', 'cancelled') \
             order by created_at, action_id limit ?",
        )
        .bind(session_id.to_string())
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(decode_action).collect()
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
        self.projected_event_slice(session_id, sequence, limit)
            .await
    }

    async fn recent_user_messages(
        &self,
        session_id: Uuid,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        // Use the existing message-id partial index to avoid walking every
        // tool/reasoning event in a long session. We sort the much smaller
        // message set in memory because the message index is keyed by message
        // id rather than sequence.
        let rows = sqlx::query(
            "select event_json from session_events \
             where session_id = ? and message_id is not null",
        )
        .bind(session_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        let mut messages = rows
            .into_iter()
            .map(|row| serde_json::from_str(row.try_get("event_json")?).map_err(Into::into))
            .collect::<Result<Vec<SessionEvent>>>()?
            .into_iter()
            .filter(|event| {
                matches!(
                    event.kind,
                    SessionEventKind::Message {
                        actor: crate::EventActor::User,
                        status: crate::MessageStatus::Complete,
                        ..
                    }
                )
            })
            .collect::<Vec<_>>();
        messages.sort_unstable_by(|left, right| right.sequence.cmp(&left.sequence));
        messages.truncate(limit);
        messages.reverse();
        Ok(messages)
    }

    async fn state(&self, session_id: Uuid) -> Result<SessionState> {
        Ok(self.session_row(session_id).await?.state)
    }

    async fn inherited_event_count(&self, session_id: Uuid) -> Result<u64> {
        Ok(self.session_row(session_id).await?.inherited_event_count)
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
        let parent_workspace: Option<String> = sqlx::query_scalar(
            "select workspace_id from session_workspace_bindings where session_id=?",
        )
        .bind(parent_session_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        sqlx::query(
            "insert into session_workspace_bindings \
             (session_id, workspace_id, participant_id, attached_at) values (?, ?, ?, ?)",
        )
        .bind(session_id.to_string())
        .bind(parent_workspace.unwrap_or_else(|| parent_session_id.to_string()))
        .bind(session_id.to_string())
        .bind(Utc::now().to_rfc3339())
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
        let mut sessions = Vec::with_capacity(rows.len());
        for row in rows {
            let session_id = parse_uuid(row.try_get("id")?)?;
            let state_json: &str = row.try_get("state_json")?;
            let state = match serde_json::from_str(state_json) {
                Ok(state) => state,
                Err(error) => {
                    // A provider removed from the runtime can still be present
                    // in an older session projection. It is not resumable by
                    // this binary, but it must not make `/resume` take down the
                    // entire TUI. Keep the row intact for explicit inspection.
                    tracing::warn!(
                        %session_id,
                        %error,
                        "skipping session with incompatible stored provider state"
                    );
                    continue;
                }
            };
            sessions.push(SessionSummary {
                session_id,
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
                state,
            });
        }
        Ok(sessions)
    }

    async fn attach_workspace(
        &self,
        binding: SessionWorkspaceBinding,
    ) -> Result<SessionWorkspaceBinding> {
        anyhow::ensure!(
            self.contains_session(binding.session_id).await?,
            "session {} does not exist",
            binding.session_id
        );
        let existing = self.workspace_binding(binding.session_id).await?;
        if let Some(existing) = &existing {
            anyhow::ensure!(
                existing.workspace_id == binding.workspace_id
                    && existing.participant_id == binding.participant_id,
                "session {} is already attached to workspace {} as participant {}",
                binding.session_id,
                existing.workspace_id,
                existing.participant_id
            );
        }
        sqlx::query(
            "insert into session_workspace_bindings \
             (session_id, workspace_id, participant_id, host_id, attached_at) \
             values (?, ?, ?, ?, ?) \
             on conflict(session_id) do update set host_id=excluded.host_id, \
             attached_at=excluded.attached_at",
        )
        .bind(binding.session_id.to_string())
        .bind(binding.workspace_id.to_string())
        .bind(binding.participant_id.to_string())
        .bind(binding.host_id.map(|id| id.to_string()))
        .bind(binding.attached_at.to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(binding)
    }

    async fn workspace_binding(&self, session_id: Uuid) -> Result<Option<SessionWorkspaceBinding>> {
        let row = sqlx::query(
            "select workspace_id, participant_id, host_id, attached_at \
             from session_workspace_bindings where session_id=?",
        )
        .bind(session_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            Ok(SessionWorkspaceBinding {
                session_id,
                workspace_id: parse_uuid(row.try_get("workspace_id")?)?,
                participant_id: parse_uuid(row.try_get("participant_id")?)?,
                host_id: row
                    .try_get::<Option<&str>, _>("host_id")?
                    .map(parse_uuid)
                    .transpose()?,
                attached_at: DateTime::parse_from_rfc3339(row.try_get("attached_at")?)?
                    .with_timezone(&Utc),
            })
        })
        .transpose()
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

pub(crate) fn deferred_json_payload(payload: &SessionPayloadRef) -> serde_json::Value {
    serde_json::json!({
        "borg_payload_deferred": true,
        "payload_id": payload.id,
        "byte_len": payload.byte_len,
    })
}

pub(crate) fn deferred_text_payload(value: &str, payload: &SessionPayloadRef) -> String {
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
    async fn opening_schema_v4_repairs_missing_action_lease_columns() {
        let (directory, store) = store().await;
        let path = directory.path().join("sessions.sqlite3");
        sqlx::query("drop index if exists idx_session_actions_lease_expiry")
            .execute(store.pool())
            .await
            .unwrap();
        for column in [
            "lease_owner",
            "lease_token",
            "lease_heartbeat_at",
            "lease_expires_at",
        ] {
            sqlx::query(&format!("alter table session_actions drop column {column}"))
                .execute(store.pool())
                .await
                .unwrap();
        }
        sqlx::query("update borg_session_schema set version=4 where id=1")
            .execute(store.pool())
            .await
            .unwrap();
        store.pool().close().await;

        let reopened = SqliteSessionStore::open(path).await.unwrap();
        let columns = sqlx::query("pragma table_info(session_actions)")
            .fetch_all(reopened.pool())
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.get::<String, _>("name"))
            .collect::<HashSet<_>>();
        assert!(
            [
                "lease_owner",
                "lease_token",
                "lease_heartbeat_at",
                "lease_expires_at",
            ]
            .into_iter()
            .all(|column| columns.contains(column))
        );
        let version: i64 = sqlx::query_scalar("select version from borg_session_schema where id=1")
            .fetch_one(reopened.pool())
            .await
            .unwrap();
        assert_eq!(version, SESSION_SCHEMA_VERSION);
    }

    #[tokio::test]
    async fn list_sessions_skips_state_for_removed_providers() {
        let (_directory, store) = store().await;
        let valid = Uuid::new_v4();
        let incompatible = Uuid::new_v4();
        store.create_session(valid).await.unwrap();
        store.create_session(incompatible).await.unwrap();
        sqlx::query("update sessions set state_json = ? where id = ?")
            .bind(r#"{"configuration":{"provider":"open_code"}}"#)
            .bind(incompatible.to_string())
            .execute(store.pool())
            .await
            .unwrap();

        let sessions = store.list_sessions(10).await.unwrap();
        assert_eq!(
            sessions
                .into_iter()
                .map(|session| session.session_id)
                .collect::<Vec<_>>(),
            vec![valid]
        );
        assert!(store.contains_session(incompatible).await.unwrap());
    }

    #[tokio::test]
    async fn provider_capability_snapshot_is_durable_metadata_not_context() {
        let (_directory, store) = store().await;
        let session_id = Uuid::new_v4();
        store.create_session(session_id).await.unwrap();
        let providers = vec![crate::ProviderCapability {
            provider: CodingProvider::Codex,
            installed: true,
            version: Some("test".to_string()),
            authenticated: true,
            auth_detail: Some("Codex subscription authenticated".to_string()),
            auth_methods: vec![crate::ProviderAuthMethod::Subscription],
            can_spawn: true,
        }];
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ProviderCapabilitiesUpdated {
                    providers: providers.clone(),
                },
            ))
            .await
            .unwrap();

        assert_eq!(
            store.state(session_id).await.unwrap().provider_capabilities,
            providers
        );
        assert!(
            store
                .recovery(session_id)
                .await
                .unwrap()
                .context_events
                .is_empty()
        );
        assert_eq!(store.read(session_id).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn sessions_have_stable_workspace_bindings_and_children_inherit_the_team_workspace() {
        let (directory, store) = store().await;
        let root = Uuid::new_v4();
        store.create_session(root).await.unwrap();
        let root_binding = store.workspace_binding(root).await.unwrap().unwrap();
        assert_eq!(root_binding.workspace_id, root);
        assert_eq!(root_binding.participant_id, root);

        let host_id = Uuid::new_v4();
        let reattached = store
            .attach_workspace(SessionWorkspaceBinding {
                host_id: Some(host_id),
                attached_at: Utc::now(),
                ..root_binding.clone()
            })
            .await
            .unwrap();
        assert_eq!(reattached.host_id, Some(host_id));
        assert_eq!(
            store
                .workspace_binding(root)
                .await
                .unwrap()
                .unwrap()
                .host_id,
            Some(host_id)
        );
        assert!(
            store
                .attach_workspace(SessionWorkspaceBinding {
                    workspace_id: Uuid::new_v4(),
                    ..reattached
                })
                .await
                .unwrap_err()
                .to_string()
                .contains("already attached")
        );

        let child = Uuid::new_v4();
        let child_journal = directory
            .path()
            .join("subagents")
            .join(format!("{child}.lock"));
        let _writer = crate::SessionWriterLease::acquire(&child_journal).unwrap();
        store.register_child_session(root, child).await.unwrap();
        let child_binding = store.workspace_binding(child).await.unwrap().unwrap();
        assert_eq!(child_binding.workspace_id, root);
        assert_eq!(child_binding.participant_id, child);
    }

    #[tokio::test]
    async fn new_session_can_start_in_a_selected_workspace_without_becoming_rebindable() {
        let (_directory, store) = store().await;
        let session_id = Uuid::new_v4();
        let workspace_id = Uuid::new_v4();
        let binding = store
            .create_session_in_workspace(session_id, workspace_id)
            .await
            .unwrap();
        assert_eq!(binding.workspace_id, workspace_id);
        assert_eq!(binding.participant_id, session_id);
        assert_eq!(
            store.workspace_binding(session_id).await.unwrap().unwrap(),
            binding
        );
        assert!(
            store
                .attach_workspace(SessionWorkspaceBinding {
                    workspace_id: Uuid::new_v4(),
                    ..binding
                })
                .await
                .unwrap_err()
                .to_string()
                .contains("already attached")
        );
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
    async fn prompt_event_boundaries_drive_one_atomic_action_lifecycle() {
        let (directory, store) = store().await;
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
                configured(directory.path()),
            ))
            .await
            .unwrap();
        let message_id = Uuid::new_v4();
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::Message {
                    message_id,
                    actor: EventActor::User,
                    text: "do the work".to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Queued,
                    delivery: Some(PromptDelivery::Queue),
                },
            ))
            .await
            .unwrap();
        assert_eq!(
            store
                .action(session_id, message_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            SessionActionState::Admitted
        );
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::TurnStarted {
                    message_id,
                    provider: CodingProvider::Codex,
                    model: Some("gpt-test".to_string()),
                    effort: Some("high".to_string()),
                    fast: false,
                },
            ))
            .await
            .unwrap();
        assert_eq!(
            store
                .action(session_id, message_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            SessionActionState::Running
        );
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::TurnCompleted {
                    message_id,
                    provider_session_id: Some("provider-session".to_string()),
                    final_text: "done".to_string(),
                    error: None,
                },
            ))
            .await
            .unwrap();
        let completed = store.action(session_id, message_id).await.unwrap().unwrap();
        assert_eq!(completed.state, SessionActionState::Completed);
        assert!(completed.accepted_at.is_some());
        assert!(completed.delivered_at.is_some());
        assert!(completed.completed_at.is_some());
        let transitions = store
            .action_transitions(session_id, message_id)
            .await
            .unwrap();
        assert_eq!(
            transitions
                .iter()
                .map(|transition| (transition.from, transition.to))
                .collect::<Vec<_>>(),
            [
                (None, SessionActionState::Queued),
                (
                    Some(SessionActionState::Queued),
                    SessionActionState::Admitted
                ),
                (
                    Some(SessionActionState::Admitted),
                    SessionActionState::Delivered
                ),
                (
                    Some(SessionActionState::Delivered),
                    SessionActionState::Preparing
                ),
                (
                    Some(SessionActionState::Preparing),
                    SessionActionState::Committing
                ),
                (
                    Some(SessionActionState::Committing),
                    SessionActionState::Running
                ),
                (
                    Some(SessionActionState::Running),
                    SessionActionState::Completed
                ),
            ]
        );
        assert!(
            store
                .pending_actions(session_id, 10)
                .await
                .unwrap()
                .is_empty()
        );

        // Re-admission with the same id/payload is an idempotent read of the
        // durable terminal action, not a duplicate action.
        let replay = SessionAction::new(
            message_id,
            session_id,
            completed.kind,
            completed.delivery,
            completed.wake,
            completed.payload.clone(),
        );
        assert_eq!(
            store.enqueue_action(replay).await.unwrap().state,
            completed.state
        );
    }

    #[tokio::test]
    async fn concurrent_claims_have_one_winner_and_same_owner_claim_is_idempotent() {
        let (_directory, store) = store().await;
        let session_id = Uuid::new_v4();
        let action_id = Uuid::new_v4();
        store.create_session(session_id).await.unwrap();
        store
            .enqueue_action(SessionAction::new(
                action_id,
                session_id,
                crate::SessionActionKind::Prompt,
                crate::ActionDeliveryPolicy::NextTurnBoundary,
                crate::ActionWakePolicy::Immediate,
                serde_json::json!({"text": "claim once"}),
            ))
            .await
            .unwrap();

        let mut tasks = Vec::new();
        for worker_number in 0..8 {
            let contender = store.clone();
            tasks.push(tokio::spawn(async move {
                contender
                    .claim_action(
                        session_id,
                        action_id,
                        &format!("worker-{worker_number}"),
                        Duration::from_secs(30),
                    )
                    .await
                    .unwrap()
            }));
        }
        let mut winner = None;
        for task in tasks {
            if let Some(action) = task.await.unwrap() {
                assert!(winner.is_none(), "two workers claimed the action");
                winner = Some(action);
            }
        }
        let winner = winner.expect("one worker should claim the queued action");
        let replay = store
            .claim_action(
                session_id,
                action_id,
                winner.lease_owner.as_deref().unwrap(),
                Duration::from_secs(30),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay.lease_token, winner.lease_token);
        assert_eq!(replay.lease_heartbeat_at, winner.lease_heartbeat_at);
        assert_eq!(
            store
                .action_transitions(session_id, action_id)
                .await
                .unwrap()
                .len(),
            1,
            "claiming must not create duplicate lifecycle transitions"
        );
    }

    #[tokio::test]
    async fn expired_leases_requeue_once_and_fence_stale_workers() {
        let (_directory, store) = store().await;
        let session_id = Uuid::new_v4();
        let action_id = Uuid::new_v4();
        store.create_session(session_id).await.unwrap();
        store
            .enqueue_action(SessionAction::new(
                action_id,
                session_id,
                crate::SessionActionKind::Prompt,
                crate::ActionDeliveryPolicy::NextTurnBoundary,
                crate::ActionWakePolicy::Immediate,
                serde_json::json!({"text": "recover me"}),
            ))
            .await
            .unwrap();
        let claimed = store
            .claim_action(session_id, action_id, "worker-a", Duration::from_millis(40))
            .await
            .unwrap()
            .unwrap();
        let token = claimed.lease_token.unwrap();
        store
            .transition_claimed_action(
                session_id,
                action_id,
                "worker-a",
                token,
                Some(SessionActionState::Queued),
                SessionActionState::Admitted,
                None,
            )
            .await
            .unwrap();
        store
            .transition_claimed_action(
                session_id,
                action_id,
                "worker-a",
                token,
                Some(SessionActionState::Admitted),
                SessionActionState::Delivered,
                None,
            )
            .await
            .unwrap();
        store
            .transition_claimed_action(
                session_id,
                action_id,
                "worker-a",
                token,
                Some(SessionActionState::Delivered),
                SessionActionState::Preparing,
                None,
            )
            .await
            .unwrap();
        store
            .transition_claimed_action(
                session_id,
                action_id,
                "worker-a",
                token,
                Some(SessionActionState::Preparing),
                SessionActionState::Committing,
                None,
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await;
        let recovered = store.recover_expired_actions(Utc::now(), 10).await.unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].state, SessionActionState::Queued);
        assert!(recovered[0].lease_owner.is_none());
        assert!(
            store
                .heartbeat_action(
                    session_id,
                    action_id,
                    "worker-a",
                    token,
                    Duration::from_secs(30),
                )
                .await
                .is_err()
        );
        assert!(
            store
                .recover_expired_actions(Utc::now(), 10)
                .await
                .unwrap()
                .is_empty()
        );
        let reclaimed = store
            .claim_action(session_id, action_id, "worker-b", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_ne!(reclaimed.lease_token, Some(token));
        assert!(
            store
                .transition_claimed_action(
                    session_id,
                    action_id,
                    "worker-a",
                    token,
                    Some(SessionActionState::Queued),
                    SessionActionState::Admitted,
                    None,
                )
                .await
                .is_err()
        );
        let transitions = store
            .action_transitions(session_id, action_id)
            .await
            .unwrap();
        assert!(transitions.iter().any(|transition| {
            transition.from == Some(SessionActionState::Committing)
                && transition.to == SessionActionState::Queued
        }));
    }

    #[tokio::test]
    async fn compaction_events_drive_one_replayable_action_lifecycle() {
        let (directory, store) = store().await;
        let session_id = Uuid::new_v4();
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
        let mut started = SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::OpenRouter,
                kind: "context_compaction".to_string(),
                payload: serde_json::json!({"status": "started"}),
            },
        );
        started = store.append(started).await.unwrap();
        assert_eq!(
            store
                .action(session_id, started.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            SessionActionState::Running
        );
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ProviderEvent {
                    provider: CodingProvider::OpenRouter,
                    kind: "context_compaction".to_string(),
                    payload: serde_json::json!({
                        "status": "completed",
                        "summary": "keep the durable decisions"
                    }),
                },
            ))
            .await
            .unwrap();
        let action = store.action(session_id, started.id).await.unwrap().unwrap();
        assert_eq!(action.state, SessionActionState::Completed);
        assert_eq!(
            store
                .action_transitions(session_id, started.id)
                .await
                .unwrap()
                .last()
                .unwrap()
                .to,
            SessionActionState::Completed
        );
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
    async fn recent_user_messages_are_bounded_ordered_and_ignore_non_completed_prompts() {
        let (_directory, store) = store().await;
        let session_id = Uuid::new_v4();
        store.create_session(session_id).await.unwrap();
        for kind in [
            message(Uuid::new_v4(), "first"),
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: "assistant".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: "still queued".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Queue),
            },
            message(Uuid::new_v4(), "second"),
            message(Uuid::new_v4(), "third"),
        ] {
            store
                .append(SessionEvent::new(session_id, 0, kind))
                .await
                .unwrap();
        }

        let prompts = store.recent_user_messages(session_id, 2).await.unwrap();
        let texts = prompts
            .iter()
            .filter_map(|event| match &event.kind {
                SessionEventKind::Message { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(texts, vec!["second", "third"]);
        assert!(
            store
                .recent_user_messages(session_id, 0)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn durable_store_health_requires_full_sync_wal_and_foreign_keys() {
        let (_directory, store) = store().await;
        let health = store.health().await.unwrap();
        assert_eq!(health.integrity, "ok");
        assert_eq!(health.journal_mode.to_ascii_lowercase(), "wal");
        assert!(health.synchronous >= 2);
        assert!(health.foreign_keys);
        assert!(health.is_ready());
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
                turn_id: None,
                provider_context_reused: None,
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
                turn_id: None,
                provider_context_reused: None,
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
    async fn inherited_event_pages_match_the_full_projection_across_lineage_boundaries() {
        let (directory, store) = store().await;
        let parent_id = Uuid::new_v4();
        let child_id = Uuid::new_v4();
        let grandchild_id = Uuid::new_v4();
        store.create_session(parent_id).await.unwrap();
        for kind in [
            SessionEventKind::SessionStarted,
            configured(directory.path()),
            SessionEventKind::ProviderSessionLinked {
                provider_session_id: "must-not-fork".to_string(),
            },
            message(Uuid::new_v4(), "parent-a"),
            message(Uuid::new_v4(), "parent-b"),
        ] {
            store
                .append(SessionEvent::new(parent_id, 0, kind))
                .await
                .unwrap();
        }
        store.fork_before(parent_id, child_id, 6).await.unwrap();
        for text in ["child-a", "child-b", "child-c", "child-d"] {
            store
                .append(SessionEvent::new(
                    child_id,
                    0,
                    message(Uuid::new_v4(), text),
                ))
                .await
                .unwrap();
        }
        store.fork_before(child_id, grandchild_id, 8).await.unwrap();
        for text in ["grandchild-a", "grandchild-b"] {
            store
                .append(SessionEvent::new(
                    grandchild_id,
                    0,
                    message(Uuid::new_v4(), text),
                ))
                .await
                .unwrap();
        }

        let full = store.read(grandchild_id).await.unwrap();
        for sequence in 0..=u64::try_from(full.len() + 1).unwrap() {
            for limit in [1, 2, 4, 100] {
                let expected = full
                    .iter()
                    .skip(usize::try_from(sequence).unwrap())
                    .take(limit)
                    .map(|event| serde_json::to_value(event).unwrap())
                    .collect::<Vec<_>>();
                let actual = store
                    .events_after(grandchild_id, sequence, limit)
                    .await
                    .unwrap()
                    .iter()
                    .map(|event| serde_json::to_value(event).unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected, "sequence={sequence}, limit={limit}");
            }
        }
    }

    /// A rewind cuts immediately before the admission of the prompt it targets.
    /// That prompt's earlier queue entry sits below the cut, so inheriting it
    /// would hand the fork a pending prompt and re-run exactly what the user
    /// just discarded.
    #[tokio::test]
    async fn a_rewind_does_not_inherit_the_queue_entry_of_the_discarded_prompt() {
        let (directory, store) = store().await;
        let parent_id = Uuid::new_v4();
        let fork_id = Uuid::new_v4();
        let discarded_message_id = Uuid::new_v4();
        store.create_session(parent_id).await.unwrap();
        for kind in [
            SessionEventKind::SessionStarted,
            configured(directory.path()),
            SessionEventKind::Message {
                message_id: discarded_message_id,
                actor: EventActor::User,
                text: "discard".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Queued,
                delivery: Some(PromptDelivery::Queue),
            },
            SessionEventKind::Message {
                message_id: discarded_message_id,
                actor: EventActor::User,
                text: "discard".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: Some(PromptDelivery::Queue),
            },
        ] {
            store
                .append(SessionEvent::new(parent_id, 0, kind))
                .await
                .unwrap();
        }

        // The UI rewinds to the admission at sequence 4; the queue entry it was
        // admitted from is at sequence 3, below the cut.
        store.fork_before(parent_id, fork_id, 4).await.unwrap();
        assert!(
            !store
                .contains_message(fork_id, discarded_message_id)
                .await
                .unwrap()
        );
        let recovery = store.recovery(fork_id).await.unwrap();
        assert!(
            recovery.queue_events.is_empty(),
            "the discarded prompt must not come back as pending work"
        );
        assert!(
            store
                .read(fork_id)
                .await
                .unwrap()
                .iter()
                .all(|event| !matches!(
                    event.kind,
                    SessionEventKind::Message {
                        status: MessageStatus::Queued,
                        ..
                    }
                ))
        );
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
            SessionEventKind::StatusChanged {
                status: SessionStatus::Running,
                detail: None,
            },
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
        let first_reasoning = store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ReasoningDelta {
                    text: "thinking ".to_string(),
                },
            ))
            .await
            .unwrap();
        assert!(matches!(
            first_reasoning.kind,
            SessionEventKind::ReasoningDelta { ref text } if text == "thinking "
        ));
        let second_reasoning = store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ReasoningDelta {
                    text: "carefully".to_string(),
                },
            ))
            .await
            .unwrap();
        assert!(matches!(
            second_reasoning.kind,
            SessionEventKind::ReasoningDelta { ref text } if text == "thinking carefully"
        ));
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ReasoningDelta {
                    text: "thinking carefully".to_string(),
                },
            ))
            .await
            .unwrap();
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

        assert_eq!(store.read(session_id).await.unwrap().len(), 3);
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
        assert_eq!(completed.sequence, 4);
        assert!(
            store
                .live_events_after(session_id, 0)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn reasoning_boundaries_clear_the_snapshot_before_the_next_thought() {
        let (directory, store) = store().await;
        let session_id = Uuid::new_v4();
        store.create_session(session_id).await.unwrap();
        for kind in [
            SessionEventKind::SessionStarted,
            configured(directory.path()),
            SessionEventKind::StatusChanged {
                status: SessionStatus::Running,
                detail: None,
            },
        ] {
            store
                .append(SessionEvent::new(session_id, 0, kind))
                .await
                .unwrap();
        }

        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ReasoningDelta {
                    text: "previous thought".to_string(),
                },
            ))
            .await
            .unwrap();
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ReasoningCompleted,
            ))
            .await
            .unwrap();
        let next = store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ReasoningDelta {
                    text: "next thought".to_string(),
                },
            ))
            .await
            .unwrap();
        assert!(matches!(
            next.kind,
            SessionEventKind::ReasoningDelta { ref text } if text == "next thought"
        ));

        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ToolStarted {
                    tool_call_id: "tool-1".to_string(),
                    name: "read_file".to_string(),
                    input: serde_json::json!({"path": "src/lib.rs"}),
                    input_ref: None,
                },
            ))
            .await
            .unwrap();
        let after_tool = store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ReasoningDelta {
                    text: "thought after tool".to_string(),
                },
            ))
            .await
            .unwrap();
        assert!(matches!(
            after_tool.kind,
            SessionEventKind::ReasoningDelta { ref text } if text == "thought after tool"
        ));
    }

    #[tokio::test]
    async fn terminal_boundaries_clear_all_turn_live_state_but_keep_context_window() {
        let (directory, store) = store().await;
        let session_id = Uuid::new_v4();
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

        let live_message = |message_id| SessionEventKind::Message {
            message_id,
            actor: EventActor::Assistant,
            text: "partial".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        };
        store
            .append(SessionEvent::new(
                session_id,
                0,
                live_message(Uuid::new_v4()),
            ))
            .await
            .unwrap();
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ReasoningDelta {
                    text: "thinking".to_string(),
                },
            ))
            .await
            .unwrap();
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ContextWindowUpdated {
                    context_tokens: 80,
                    context_window_tokens: 100,
                },
            ))
            .await
            .unwrap();

        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::TurnCompleted {
                    message_id: Uuid::new_v4(),
                    provider_session_id: None,
                    final_text: String::new(),
                    error: Some("turn interrupted".to_string()),
                },
            ))
            .await
            .unwrap();
        let live = store.live_events_after(session_id, 0).await.unwrap();
        assert_eq!(live.len(), 1);
        assert!(matches!(
            live[0].event.kind,
            SessionEventKind::ContextWindowUpdated {
                context_tokens: 80,
                context_window_tokens: 100,
            }
        ));

        store
            .append(SessionEvent::new(
                session_id,
                0,
                live_message(Uuid::new_v4()),
            ))
            .await
            .unwrap();
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Ready,
                    detail: None,
                },
            ))
            .await
            .unwrap();
        let live = store.live_events_after(session_id, 0).await.unwrap();
        assert_eq!(live.len(), 1);
        assert!(matches!(
            live[0].event.kind,
            SessionEventKind::ContextWindowUpdated { .. }
        ));

        // A delayed coalesced event must not recreate turn state after the
        // session has become idle.
        store
            .append(SessionEvent::new(
                session_id,
                0,
                live_message(Uuid::new_v4()),
            ))
            .await
            .unwrap();
        let live = store.live_events_after(session_id, 0).await.unwrap();
        assert_eq!(live.len(), 1);
        assert!(matches!(
            live[0].event.kind,
            SessionEventKind::ContextWindowUpdated { .. }
        ));
    }

    #[tokio::test]
    async fn reopening_repairs_turn_live_state_left_on_a_terminal_session() {
        let (directory, store) = store().await;
        let path = directory.path().join("sessions.sqlite3");
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
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Ready,
                    detail: None,
                },
            ))
            .await
            .unwrap();

        let message_id = Uuid::new_v4();
        let event = SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::Assistant,
                text: "stale response".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::InProgress,
                delivery: None,
            },
        );
        sqlx::query(
            "insert into session_live_state \
             (session_id, live_key, revision, event_json, updated_at) values (?, ?, ?, ?, ?)",
        )
        .bind(session_id.to_string())
        .bind(format!("message:{message_id}"))
        .bind(99_i64)
        .bind(serde_json::to_string(&event).unwrap())
        .bind(event.created_at.to_rfc3339())
        .execute(store.pool())
        .await
        .unwrap();
        drop(store);

        let reopened = SqliteSessionStore::open(&path).await.unwrap();
        assert!(
            reopened
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
        let compaction_started = SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "context_compaction".to_string(),
            payload: serde_json::json!({"status": "started"}),
        };
        assert_eq!(compaction_started.persistence(), EventPersistence::Durable);
        assert!(!compaction_started.is_context_relevant());
        let compaction_completed = SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "context_compaction".to_string(),
            payload: serde_json::json!({"status": "completed", "summary": "done"}),
        };
        assert_eq!(
            compaction_completed.persistence(),
            EventPersistence::Durable
        );
        assert!(compaction_completed.is_context_relevant());
        assert!(
            !SessionEventKind::ProviderSessionLinked {
                provider_session_id: "provider".to_string()
            }
            .is_fork_inheritable()
        );
    }

    #[test]
    fn context_generation_changes_only_at_explicit_prefix_boundaries() {
        let session_id = Uuid::new_v4();
        let mut state = SessionState::default();
        state
            .apply(&SessionEvent::new(
                session_id,
                1,
                SessionEventKind::SessionStarted,
            ))
            .unwrap();
        state
            .apply(&SessionEvent::new(
                session_id,
                2,
                configured(Path::new("/tmp")),
            ))
            .unwrap();
        assert_eq!(state.context_generation, 0);
        state
            .apply(&SessionEvent::new(
                session_id,
                3,
                SessionEventKind::ProviderEvent {
                    provider: CodingProvider::Codex,
                    kind: "context_compaction".to_string(),
                    payload: serde_json::json!({"status": "completed"}),
                },
            ))
            .unwrap();
        assert_eq!(state.context_generation, 1);
        state
            .apply(&SessionEvent::new(
                session_id,
                4,
                SessionEventKind::ContextCleared,
            ))
            .unwrap();
        assert_eq!(state.context_generation, 2);
        state
            .apply(&SessionEvent::new(
                session_id,
                5,
                SessionEventKind::SessionConfigured {
                    cwd: PathBuf::from("/tmp"),
                    provider: CodingProvider::Claude,
                    model: Some("claude-test".to_string()),
                    effort: Some("high".to_string()),
                    fast: false,
                    response_language: ResponseLanguage::Auto,
                    permission_mode: PermissionMode::FullAccess,
                },
            ))
            .unwrap();
        assert_eq!(state.context_generation, 3);
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
