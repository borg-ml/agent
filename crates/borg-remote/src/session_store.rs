use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use regex::{Regex, RegexBuilder};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
};
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool, Transaction};
use tracing::warn;
use uuid::Uuid;

use crate::session_action::{SessionAction, SessionActionState, SessionActionTransition};
use crate::{
    CodingProvider, MessageStatus, PermissionMode, PlanItem, ResponseLanguage, SessionEvent,
    SessionEventKind, SessionGoal, SessionPayloadKind, SessionPayloadRef, SessionStatus,
};

pub(crate) const INLINE_SESSION_PAYLOAD_BYTES: usize = 64 * 1024;
pub(crate) const SESSION_PAYLOAD_PREVIEW_BYTES: usize = 4 * 1024;
// A blocked SQLite connection must return to the pool quickly. The writer
// admission loop below owns the longer wait; keeping that wait in SQLite
// would strand a pooled connection and starve unrelated reads.
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(1);
const SQLITE_SCHEMA_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const SQLITE_WRITE_WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const SQLITE_WRITE_TRANSACTION: &str = "BEGIN IMMEDIATE";
const SQLITE_JOURNAL_SIZE_LIMIT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_HOST_LAUNCH_METADATA_BYTES: usize = 512 * 1024;
// Cap fork replay at 255 local events without duplicating SessionState in every row.
const FORK_PROJECTION_CHECKPOINT_INTERVAL: u64 = 256;
pub const SESSION_PROJECTION_VERSION: i32 = 3;
const SESSION_SCHEMA_VERSION: i64 = 5;
const DISPOSABLE_SCHEMA_ERROR: &str = "Borg session database schema is incompatible";
pub(crate) const DEFAULT_HISTORY_LIMIT: usize = 50;
pub(crate) const MAX_HISTORY_LIMIT: usize = 200;
pub(crate) const DEFAULT_HISTORY_SCAN_LIMIT: usize = 10_000;
pub(crate) const MAX_HISTORY_SCAN_LIMIT: usize = 100_000;
pub(crate) const DEFAULT_HISTORY_PAYLOAD_BYTES: usize = 256 * 1024;
pub(crate) const MAX_HISTORY_PAYLOAD_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_HISTORY_QUERY_BYTES: usize = 8 * 1024;
pub(crate) const MAX_RUNTIME_CHECKPOINT_BYTES: usize = 512 * 1024;
pub(crate) const HARNESS_CHECKPOINT_PREFIX: &str = "__borg_harness__";

fn historical_projection_json(
    sequence: u64,
    inherited_event_count: u64,
    projection_json: &str,
) -> &str {
    let local_sequence = sequence.saturating_sub(inherited_event_count);
    if local_sequence == 1 || local_sequence.is_multiple_of(FORK_PROJECTION_CHECKPOINT_INTERVAL) {
        projection_json
    } else {
        ""
    }
}

/// Version of the durable runtime namespace manifest. The manifest describes
/// how to reconnect to a trusted runtime; it is not a second semantic-memory
/// store and never makes arbitrary code replay implicit.
pub(crate) const RUNTIME_MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RuntimeManifestStatus {
    Running,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RuntimeManifest {
    pub manifest_version: u32,
    pub session_id: Uuid,
    pub runtime: String,
    pub root: String,
    pub command: String,
    pub worker_id: Uuid,
    pub status: RuntimeManifestStatus,
    pub execution_count: u64,
    pub last_code_hash: Option<String>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct RuntimeCheckpoint {
    pub session_id: Uuid,
    pub key: String,
    pub state: serde_json::Value,
    pub content_hash: String,
    pub revision: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeManifestActivation {
    pub manifest: RuntimeManifest,
    pub recovered_from_previous_worker: bool,
}

/// The canonical session journal remains the authority; this selects only the
/// derived discovery path used to find event ids inside it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionHistorySearchMode {
    #[default]
    Lexical,
    Regex,
}

/// One bounded query over a session's lossless event history.
///
/// Sequence bounds are inclusive. An empty `text` performs an indexed typed
/// or range read without consulting the text-search projection.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SessionHistoryQuery {
    pub text: Option<String>,
    pub mode: SessionHistorySearchMode,
    /// Optional literal FTS narrowing applied before a regex. Supplying a
    /// term known to occur in every desired match avoids a full bounded scan.
    pub prefilter: Option<String>,
    pub event_id: Option<Uuid>,
    pub start_sequence: Option<u64>,
    pub end_sequence: Option<u64>,
    pub event_kinds: Vec<String>,
    pub actors: Vec<crate::EventActor>,
    pub newest_first: bool,
    pub case_sensitive: bool,
    pub limit: Option<usize>,
    /// Maximum canonical candidates inspected by regex or lineage fallback.
    pub scan_limit: Option<usize>,
    pub expand_payloads: bool,
    /// Aggregate byte budget for expanded payloads in the response.
    pub max_payload_bytes: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionHistoryPayload {
    pub reference: SessionPayloadRef,
    pub text: String,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionHistoryHit {
    /// Always rehydrated from the canonical journal, never returned directly
    /// from FTS or a future semantic index.
    pub event: SessionEvent,
    pub snippet: Option<String>,
    pub score: Option<f64>,
    pub payloads: Vec<SessionHistoryPayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionHistoryPage {
    pub hits: Vec<SessionHistoryHit>,
    pub backend: String,
    pub scanned_events: usize,
    pub truncated: bool,
}

/// Rebuildable feed record for an external lexical/vector index such as
/// BorgSearch/Vespa. The stable ids are locators only; callers must resolve a
/// search hit through `query_history(event_id=...)` before treating it as
/// canonical evidence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionHistoryIndexDocument {
    pub schema_version: u32,
    pub document_id: String,
    pub session_id: Uuid,
    pub event_id: Uuid,
    pub sequence: u64,
    pub event_kind: String,
    pub actor: Option<String>,
    pub created_at: DateTime<Utc>,
    pub content: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventPersistence {
    Durable,
    Coalesced,
    Ephemeral,
}

impl SessionEventKind {
    pub fn is_completed_context_compaction(&self) -> bool {
        matches!(
            self,
            Self::ProviderEvent { kind, payload, .. }
                if kind == "context_compaction"
                    && payload
                        .get("provider_context_preserved")
                        .and_then(serde_json::Value::as_bool)
                        != Some(true)
                    && matches!(
                        payload.get("status").and_then(serde_json::Value::as_str),
                        None | Some("completed")
                    )
        )
    }

    pub fn is_completed_provider_recovery_checkpoint(&self) -> bool {
        matches!(
            self,
            Self::ProviderEvent { kind, payload, .. }
                if kind == "context_compaction"
                    && payload
                        .get("provider_recovery_checkpoint")
                        .and_then(serde_json::Value::as_bool)
                        == Some(true)
                    && matches!(
                        payload.get("status").and_then(serde_json::Value::as_str),
                        None | Some("completed")
                    )
        )
    }

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
            Self::Message {
                actor: crate::EventActor::User | crate::EventActor::System,
                status: MessageStatus::InProgress,
                ..
            } => EventPersistence::Durable,
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
                | Self::EffectiveCapabilitiesUpdated { .. }
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
                | Self::RuntimeWorkflowStarted { .. }
                | Self::RuntimeWorkflowCallRequested { .. }
                | Self::RuntimeWorkflowCallCompleted { .. }
                | Self::RuntimeWorkflowCompleted { .. }
                // A fork cuts immediately before the admission of the prompt it
                // rewinds to, which would otherwise leave that prompt's earlier
                // queue entry inside the inherited history: the fork would then
                // recover it as pending and immediately re-run the very prompt
                // the rewind discarded. Only admitted history is inheritable.
                | Self::Message {
                    status: MessageStatus::Queued | MessageStatus::InProgress,
                    ..
                }
        )
    }

    pub fn is_context_relevant(&self) -> bool {
        match self {
            Self::Message {
                actor: crate::EventActor::User | crate::EventActor::System,
                status: MessageStatus::Complete | MessageStatus::Failed,
                ..
            } => true,
            Self::Message {
                actor: crate::EventActor::Assistant,
                status: MessageStatus::Complete,
                ..
            } => true,
            Self::ToolStarted { .. }
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
            kind if kind.is_completed_context_compaction()
                || kind.is_completed_provider_recovery_checkpoint() =>
            {
                true
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
    /// Host-normalized capability intersection and explicit denials.
    #[serde(default)]
    pub effective_capabilities: Option<crate::EffectiveCapabilities>,
    pub status: Option<SessionStatus>,
    pub status_detail: Option<String>,
    pub provider_session_id: Option<String>,
    /// Last provider turn committed by a durable `TurnCompleted` boundary.
    pub provider_turn_id: Option<String>,
    /// Codex reports its native checkpoint just before the session actor
    /// commits `TurnCompleted`; keep it pending until that boundary lands.
    pub pending_provider_turn_id: Option<String>,
    pub pending_provider_turn_session_id: Option<String>,
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

    pub fn has_resumable_activity(&self) -> bool {
        self.first_prompt.is_some()
            || self.latest_response.is_some()
            || self.provider_session_id.is_some()
            || self.goal.is_some()
            || !self.todos.is_empty()
            || self.usage.calls > 0
            || matches!(
                self.status,
                Some(SessionStatus::Running | SessionStatus::WaitingForApproval)
            )
            || self.pending_approval_id.is_some()
            || self.pending_provider_interaction_id.is_some()
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
                let provider_changed = self
                    .configuration
                    .as_ref()
                    .is_some_and(|old| old.provider != *provider);
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
                    // A model change starts a new Borg context generation, but
                    // an acknowledged Codex thread remains resumable under
                    // the new provider lifecycle key. Other providers do not
                    // have this durable resume path, and provider changes
                    // necessarily invalidate the old session id.
                    if provider_changed || *provider != CodingProvider::Codex {
                        self.provider_session_id = None;
                        self.provider_turn_id = None;
                        self.pending_provider_turn_id = None;
                        self.pending_provider_turn_session_id = None;
                    }
                    self.usage.context_tokens = Some(0);
                }
            }
            SessionEventKind::ProviderCapabilitiesUpdated { providers } => {
                self.provider_capabilities = providers.clone();
            }
            SessionEventKind::EffectiveCapabilitiesUpdated { capabilities } => {
                self.effective_capabilities = Some(capabilities.clone());
            }
            SessionEventKind::StatusChanged { status, detail } => {
                self.status = Some(*status);
                self.status_detail = detail.clone();
            }
            SessionEventKind::ProviderSessionLinked {
                provider_session_id,
                provider_turn_id,
            } => {
                if let Some(provider_turn_id) = provider_turn_id {
                    self.pending_provider_turn_id = Some(provider_turn_id.clone());
                    self.pending_provider_turn_session_id = Some(provider_session_id.clone());
                } else {
                    self.provider_session_id = Some(provider_session_id.clone());
                    self.provider_turn_id = None;
                    self.pending_provider_turn_id = None;
                    self.pending_provider_turn_session_id = None;
                }
            }
            SessionEventKind::TurnCompleted {
                provider_session_id,
                error,
                ..
            } => {
                // A provider id is resumable only at a durable terminal
                // boundary. Successful turns and acknowledged interrupts are
                // valid checkpoints; uncertain failures explicitly unlink the
                // native thread so recovery replays Borg's journal instead.
                if error.is_none() || error.as_deref() == Some("turn interrupted") {
                    self.provider_turn_id = provider_session_id.as_ref().and_then(|session_id| {
                        (self.pending_provider_turn_session_id.as_ref() == Some(session_id))
                            .then(|| self.pending_provider_turn_id.clone())
                            .flatten()
                    });
                    self.provider_session_id = provider_session_id.clone();
                } else {
                    self.provider_session_id = None;
                    self.provider_turn_id = None;
                }
                self.pending_provider_turn_id = None;
                self.pending_provider_turn_session_id = None;
            }
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
                self.provider_turn_id = None;
                self.pending_provider_turn_id = None;
                self.pending_provider_turn_session_id = None;
                self.usage.context_tokens = Some(0);
                self.context_generation = self.context_generation.saturating_add(1);
            }
            kind if kind.is_completed_context_compaction() => {
                self.context_generation = self.context_generation.saturating_add(1);
                self.provider_session_id = None;
                self.provider_turn_id = None;
                self.pending_provider_turn_id = None;
                self.pending_provider_turn_session_id = None;
                self.usage.context_tokens = Some(0);
            }
            SessionEventKind::Message {
                actor: crate::EventActor::User,
                text,
                status: MessageStatus::Complete | MessageStatus::Failed,
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
        state.provider_turn_id = None;
        state.pending_provider_turn_id = None;
        state.pending_provider_turn_session_id = None;
        // A fork keeps the canonical conversation, but it always starts a
        // fresh provider context. Carrying the parent's near-full usage into
        // the child makes the next prompt pass the pre-turn auto-compaction
        // check before the new provider context has been built.
        state.context_generation = state.context_generation.saturating_add(1);
        state.usage.context_tokens = Some(0);
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

/// Inputs for a lease-fenced action transition.
///
/// Keeping the fence and lifecycle fields together makes the store boundary
/// harder to call with a mismatched lease token or expected state.
#[derive(Debug, Clone)]
pub struct ClaimedActionTransition {
    pub session_id: Uuid,
    pub action_id: Uuid,
    pub lease_owner: String,
    pub lease_token: Uuid,
    pub expected: Option<SessionActionState>,
    pub next: SessionActionState,
    pub error: Option<String>,
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
        transition: ClaimedActionTransition,
    ) -> Result<SessionAction>;
    /// Requeue expired work left in the in-flight states by a crashed worker.
    /// The update and its audit transition are committed as one transaction.
    async fn recover_expired_actions(
        &self,
        session_id: Uuid,
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
    async fn latest_completed_context_compaction(
        &self,
        session_id: Uuid,
    ) -> Result<Option<SessionEvent>>;
    /// Return the newest recallable user messages authored in this session,
    /// including failed prompts, ordered from oldest to newest.
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
                        status: crate::MessageStatus::Complete | crate::MessageStatus::Failed,
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
    async fn recovery_from_provider_checkpoint(
        &self,
        _session_id: Uuid,
        _provider_session_id: &str,
    ) -> Result<Option<SessionRecovery>> {
        Ok(None)
    }
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
    #[serde(default)]
    pub integrity_checked: bool,
    pub journal_mode: String,
    pub synchronous: i64,
    pub foreign_keys: bool,
    #[serde(default)]
    pub journal_size_limit_bytes: i64,
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
        (!self.integrity_checked || self.integrity == "ok")
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
        Self::open_path(path.as_ref().to_path_buf(), true).await
    }

    async fn open_path(path: PathBuf, reset_incompatible: bool) -> Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .pragma(
                "journal_size_limit",
                SQLITE_JOURNAL_SIZE_LIMIT_BYTES.to_string(),
            )
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(SQLITE_BUSY_TIMEOUT)
            .foreign_keys(true);
        let open_error_context =
            || format!("failed to open SQLite session store {}", path.display());
        let schema_deadline = std::time::Instant::now() + SQLITE_SCHEMA_WAIT_TIMEOUT;
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
        let schema_deadline = std::time::Instant::now() + SQLITE_SCHEMA_WAIT_TIMEOUT;
        let schema_result = loop {
            match store.ensure_schema().await {
                Ok(()) => break Ok(()),
                Err(error)
                    if sqlite_schema_lock(&error)
                        && std::time::Instant::now() < schema_deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(error) => break Err(error),
            }
        };
        if let Err(error) = schema_result {
            if reset_incompatible && is_disposable_schema_error(&error) {
                store.pool.close().await;
                let archived = archive_incompatible_database(&path)?;
                warn!(
                    database = %path.display(),
                    archived = %archived.display(),
                    "archived incompatible session database and started a fresh one"
                );
                return Box::pin(Self::open_path(path, false)).await;
            }
            return Err(error);
        }
        Ok(store)
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub(crate) fn plugin_store(&self) -> crate::SqlitePluginStore {
        crate::SqlitePluginStore::new(self.pool.clone())
    }

    /// Query the lossless session journal through exact/range, FTS5, or
    /// bounded-regex retrieval. Search rows contain only a rebuildable
    /// projection; every hit is resolved back to `session_events` before it is
    /// returned.
    pub async fn query_history(
        &self,
        session_id: Uuid,
        query: SessionHistoryQuery,
    ) -> Result<SessionHistoryPage> {
        let (inherited_event_count, next_sequence) =
            self.history_session_bounds(session_id).await?;
        if let (Some(start), Some(end)) = (query.start_sequence, query.end_sequence) {
            ensure!(start <= end, "history start_sequence exceeds end_sequence");
        }
        ensure!(
            query.event_kinds.len() <= 64
                && query
                    .event_kinds
                    .iter()
                    .all(|kind| !kind.is_empty() && kind.len() <= 128),
            "history event_kinds must contain at most 64 non-empty typed names"
        );
        ensure!(
            query.actors.len() <= 4,
            "history actors contains more than four values"
        );
        ensure!(
            query.prefilter.is_none() || query.mode == SessionHistorySearchMode::Regex,
            "history prefilter is only valid with regex mode"
        );
        let text = query
            .text
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if let Some(text) = text {
            ensure!(
                text.len() <= MAX_HISTORY_QUERY_BYTES,
                "history query exceeds {MAX_HISTORY_QUERY_BYTES} bytes"
            );
        }
        if let Some(prefilter) = query
            .prefilter
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            ensure!(
                prefilter.len() <= MAX_HISTORY_QUERY_BYTES,
                "history prefilter exceeds {MAX_HISTORY_QUERY_BYTES} bytes"
            );
        }
        if inherited_event_count == 0 && text.is_some() {
            self.ensure_history_projection(session_id, next_sequence.saturating_sub(1))
                .await?;
        }
        if inherited_event_count > 0 {
            return self.query_history_lineage(session_id, &query, text).await;
        }
        match (text, query.mode) {
            (None, _) => self.query_history_exact(session_id, &query).await,
            (Some(text), SessionHistorySearchMode::Lexical) => {
                self.query_history_lexical(session_id, &query, text).await
            }
            (Some(text), SessionHistorySearchMode::Regex) => {
                self.query_history_regex(session_id, &query, text).await
            }
        }
    }

    /// Stream full-text records for an optional external search index. This is
    /// deliberately pull-based and sequence-cursored so Vespa/Postgres can be
    /// rebuilt from SQLite after loss without joining the write transaction or
    /// becoming a second journal.
    pub async fn history_index_documents_after(
        &self,
        session_id: Uuid,
        sequence: u64,
        limit: usize,
    ) -> Result<Vec<SessionHistoryIndexDocument>> {
        let limit = limit.clamp(1, 1_000);
        let (inherited_event_count, next_sequence) =
            self.history_session_bounds(session_id).await?;
        if sequence < inherited_event_count {
            let events = self
                .projected_events(session_id, None)
                .await?
                .into_iter()
                .filter(|event| event.sequence > sequence)
                .take(limit)
                .collect::<Vec<_>>();
            let mut documents = Vec::with_capacity(events.len());
            for event in events {
                documents.push(SessionHistoryIndexDocument {
                    schema_version: 1,
                    document_id: history_index_document_id(session_id, event.id),
                    session_id,
                    event_id: event.id,
                    sequence: event.sequence,
                    event_kind: event_kind(&event.kind)?,
                    actor: event_actor(&event.kind).map(str::to_string),
                    created_at: event.created_at,
                    content: self.history_event_body(&event).await?,
                });
            }
            return Ok(documents);
        }
        let local_event_count = next_sequence
            .saturating_sub(inherited_event_count)
            .saturating_sub(1);
        self.ensure_history_projection(session_id, local_event_count)
            .await?;

        let rows = sqlx::query(
            "select e.event_json, s.event_kind, s.actor, s.body \
             from session_event_search s \
             join session_events e on e.session_id=s.session_id and e.event_id=s.event_id \
             where s.session_id=? and s.sequence>? order by s.sequence limit ?",
        )
        .bind(session_id.to_string())
        .bind(i64::try_from(sequence).unwrap_or(i64::MAX))
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let event: SessionEvent = serde_json::from_str(row.try_get("event_json")?)?;
                Ok(SessionHistoryIndexDocument {
                    schema_version: 1,
                    document_id: history_index_document_id(session_id, event.id),
                    session_id,
                    event_id: event.id,
                    sequence: event.sequence,
                    event_kind: row.try_get("event_kind")?,
                    actor: row.try_get("actor")?,
                    created_at: event.created_at,
                    content: row.try_get("body")?,
                })
            })
            .collect()
    }

    async fn ensure_history_projection(
        &self,
        session_id: Uuid,
        expected_event_count: u64,
    ) -> Result<()> {
        let projected_count: i64 =
            sqlx::query_scalar("select count(*) from session_event_search where session_id = ?")
                .bind(session_id.to_string())
                .fetch_one(&self.pool)
                .await?;
        if u64::try_from(projected_count).context("negative history projection count")?
            == expected_event_count
        {
            return Ok(());
        }
        let mut transaction = self.begin_write().await?;
        sqlx::query(
            "delete from session_event_search where session_id = ? and not exists ( \
                 select 1 from session_events e \
                 where e.session_id=session_event_search.session_id \
                   and e.event_id=session_event_search.event_id \
             )",
        )
        .bind(session_id.to_string())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "insert into session_event_search \
             (session_id, sequence, event_id, event_kind, actor, body) \
             select e.session_id, e.sequence, e.event_id, e.event_kind, \
                    json_extract(e.event_json, '$.kind.actor'), \
                    e.event_json || coalesce(( \
                        select char(10) || group_concat(cast(p.payload as text), char(10)) \
                        from session_payloads p \
                        where p.session_id=e.session_id and p.event_id=e.event_id \
                    ), '') \
             from session_events e \
             where e.session_id=? and not exists ( \
                 select 1 from session_event_search s \
                 where s.session_id=e.session_id and s.event_id=e.event_id \
             )",
        )
        .bind(session_id.to_string())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn query_history_exact(
        &self,
        session_id: Uuid,
        query: &SessionHistoryQuery,
    ) -> Result<SessionHistoryPage> {
        let limit = history_limit(query);
        let mut sql = QueryBuilder::<Sqlite>::new(
            "select e.event_json from session_events e where e.session_id = ",
        );
        sql.push_bind(session_id.to_string());
        push_history_sql_filters(&mut sql, query, "e");
        sql.push(" order by e.sequence ");
        sql.push(if query.newest_first { "desc" } else { "asc" });
        sql.push(" limit ")
            .push_bind(i64::try_from(limit + 1).unwrap_or(i64::MAX));
        let rows = sql.build().fetch_all(&self.pool).await?;
        let truncated = rows.len() > limit;
        let scanned_events = rows.len().min(limit);
        let mut hits = Vec::with_capacity(scanned_events);
        let mut payload_budget = history_payload_budget(query);
        for row in rows.into_iter().take(limit) {
            let event: SessionEvent = serde_json::from_str(row.try_get("event_json")?)?;
            hits.push(
                self.hydrate_history_hit(event, None, None, query, &mut payload_budget)
                    .await?,
            );
        }
        Ok(SessionHistoryPage {
            hits,
            backend: "sqlite_exact".to_string(),
            scanned_events,
            truncated,
        })
    }

    async fn query_history_lexical(
        &self,
        session_id: Uuid,
        query: &SessionHistoryQuery,
        text: &str,
    ) -> Result<SessionHistoryPage> {
        let limit = history_limit(query);
        let mut sql = QueryBuilder::<Sqlite>::new(
            "select e.event_json, \
             snippet(session_event_fts, 0, '[', ']', ' … ', 32) as search_snippet, \
             bm25(session_event_fts) as search_rank \
             from session_event_fts \
             join session_event_search s on s.rowid=session_event_fts.rowid \
             join session_events e on e.session_id=s.session_id and e.event_id=s.event_id \
             where session_event_fts match ",
        );
        sql.push_bind(history_fts_query(text)?);
        sql.push(" and s.session_id = ")
            .push_bind(session_id.to_string());
        push_history_sql_filters(&mut sql, query, "s");
        if query.newest_first {
            sql.push(" order by s.sequence desc");
        } else {
            sql.push(" order by search_rank asc, s.sequence asc");
        }
        sql.push(" limit ")
            .push_bind(i64::try_from(limit + 1).unwrap_or(i64::MAX));
        let rows = sql.build().fetch_all(&self.pool).await?;
        let truncated = rows.len() > limit;
        let scanned_events = rows.len().min(limit);
        let mut hits = Vec::with_capacity(scanned_events);
        let mut payload_budget = history_payload_budget(query);
        for row in rows.into_iter().take(limit) {
            let event: SessionEvent = serde_json::from_str(row.try_get("event_json")?)?;
            let snippet = row.try_get::<Option<String>, _>("search_snippet")?;
            let rank = row.try_get::<f64, _>("search_rank")?;
            hits.push(
                self.hydrate_history_hit(event, snippet, Some(-rank), query, &mut payload_budget)
                    .await?,
            );
        }
        Ok(SessionHistoryPage {
            hits,
            backend: "sqlite_fts5".to_string(),
            scanned_events,
            truncated,
        })
    }

    async fn query_history_regex(
        &self,
        session_id: Uuid,
        query: &SessionHistoryQuery,
        text: &str,
    ) -> Result<SessionHistoryPage> {
        let expression = history_regex(text, query.case_sensitive)?;
        let limit = history_limit(query);
        let scan_limit = history_scan_limit(query);
        let prefilter = query
            .prefilter
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let mut sql = if let Some(prefilter) = prefilter {
            let mut sql = QueryBuilder::<Sqlite>::new(
                "with fts_candidates(rowid) as materialized ( \
                 select session_event_fts.rowid from session_event_fts \
                 join session_event_search s0 on s0.rowid=session_event_fts.rowid \
                 where session_event_fts match ",
            );
            sql.push_bind(history_fts_query(prefilter)?);
            sql.push(" and s0.session_id = ")
                .push_bind(session_id.to_string());
            sql.push(" order by bm25(session_event_fts) asc");
            sql.push(" limit ")
                .push_bind(i64::try_from(scan_limit + 1).unwrap_or(i64::MAX));
            // Keep the selective FTS result as the outer loop. A normal join lets SQLite
            // walk the session sequence index first to satisfy the final ordering.
            sql.push(
                ") select e.event_json, s.body from fts_candidates c \
                 cross join session_event_search s on s.rowid=c.rowid \
                 join session_events e on e.session_id=s.session_id and e.event_id=s.event_id \
                 where s.session_id = ",
            );
            sql.push_bind(session_id.to_string());
            sql
        } else {
            let mut sql = QueryBuilder::<Sqlite>::new(
                "select e.event_json, s.body from session_event_search s \
                 join session_events e on e.session_id=s.session_id and e.event_id=s.event_id \
                 where s.session_id = ",
            );
            sql.push_bind(session_id.to_string());
            sql
        };
        push_history_sql_filters(&mut sql, query, "s");
        sql.push(" order by s.sequence ");
        sql.push(if query.newest_first { "desc" } else { "asc" });
        sql.push(" limit ")
            .push_bind(i64::try_from(scan_limit + 1).unwrap_or(i64::MAX));
        let rows = sql.build().fetch_all(&self.pool).await?;
        let candidate_overflow = rows.len() > scan_limit;
        let mut hits = Vec::new();
        let mut scanned_events = 0;
        let mut payload_budget = history_payload_budget(query);
        for row in rows.into_iter().take(scan_limit) {
            scanned_events += 1;
            let body: &str = row.try_get("body")?;
            let Some(found) = expression.find(body) else {
                continue;
            };
            let event: SessionEvent = serde_json::from_str(row.try_get("event_json")?)?;
            hits.push(
                self.hydrate_history_hit(
                    event,
                    Some(history_match_snippet(body, found.start(), found.end())),
                    None,
                    query,
                    &mut payload_budget,
                )
                .await?,
            );
            if hits.len() > limit {
                break;
            }
        }
        let truncated = candidate_overflow || hits.len() > limit;
        hits.truncate(limit);
        Ok(SessionHistoryPage {
            hits,
            backend: if prefilter.is_some() {
                "sqlite_regex_fts_prefilter"
            } else {
                "sqlite_regex"
            }
            .to_string(),
            scanned_events,
            truncated,
        })
    }

    async fn query_history_lineage(
        &self,
        session_id: Uuid,
        query: &SessionHistoryQuery,
        text: Option<&str>,
    ) -> Result<SessionHistoryPage> {
        let limit = history_limit(query);
        let scan_limit = history_scan_limit(query);
        let regex = match (text, query.mode) {
            (Some(text), SessionHistorySearchMode::Regex) => {
                Some(history_regex(text, query.case_sensitive)?)
            }
            _ => None,
        };
        let lexical_terms = match (text, query.mode) {
            (Some(text), SessionHistorySearchMode::Lexical) => {
                history_literal_terms(text, query.case_sensitive)?
            }
            _ => Vec::new(),
        };
        let regex_prefilter = match (query.prefilter.as_deref(), query.mode) {
            (Some(prefilter), SessionHistorySearchMode::Regex) if !prefilter.trim().is_empty() => {
                history_literal_terms(prefilter, false)?
            }
            _ => Vec::new(),
        };
        let mut events = self.projected_events(session_id, None).await?;
        if query.newest_first {
            events.reverse();
        }
        let mut hits = Vec::new();
        let mut scanned_events = 0;
        let mut payload_budget = history_payload_budget(query);
        let mut candidate_overflow = false;
        for event in events {
            if !history_event_matches_filters(&event, query)? {
                continue;
            }
            if scanned_events == scan_limit {
                candidate_overflow = true;
                break;
            }
            scanned_events += 1;
            let body = if text.is_some() {
                self.history_event_body(&event).await?
            } else {
                String::new()
            };
            let matched = if let Some(regex) = &regex {
                if !regex_prefilter.is_empty()
                    && history_literal_match(&body, &regex_prefilter, false).is_none()
                {
                    None
                } else {
                    regex.find(&body).map(|found| (found.start(), found.end()))
                }
            } else if !lexical_terms.is_empty() {
                history_literal_match(&body, &lexical_terms, query.case_sensitive)
            } else {
                Some((0, 0))
            };
            let Some((start, end)) = matched else {
                continue;
            };
            let snippet = text.map(|_| history_match_snippet(&body, start, end));
            hits.push(
                self.hydrate_history_hit(event, snippet, None, query, &mut payload_budget)
                    .await?,
            );
            if hits.len() > limit {
                break;
            }
        }
        let truncated = candidate_overflow || hits.len() > limit;
        hits.truncate(limit);
        Ok(SessionHistoryPage {
            hits,
            backend: "lineage_scan".to_string(),
            scanned_events,
            truncated,
        })
    }

    async fn history_event_body(&self, event: &SessionEvent) -> Result<String> {
        let mut body = serde_json::to_string(event)?;
        let mut references = Vec::new();
        history_payload_refs(&event.kind, &mut references);
        for reference in references {
            let payload = self.load_payload(reference).await?;
            body.push('\n');
            body.push_str(&String::from_utf8_lossy(&payload));
        }
        Ok(body)
    }

    async fn hydrate_history_hit(
        &self,
        event: SessionEvent,
        snippet: Option<String>,
        score: Option<f64>,
        query: &SessionHistoryQuery,
        payload_budget: &mut usize,
    ) -> Result<SessionHistoryHit> {
        let mut payloads = Vec::new();
        if query.expand_payloads && *payload_budget > 0 {
            let mut references = Vec::new();
            history_payload_refs(&event.kind, &mut references);
            for reference in references {
                if *payload_budget == 0 {
                    break;
                }
                let take = (*payload_budget)
                    .min(usize::try_from(reference.byte_len).unwrap_or(usize::MAX));
                let bytes = self.history_payload_prefix(reference, take).await?;
                *payload_budget = payload_budget.saturating_sub(bytes.len());
                payloads.push(SessionHistoryPayload {
                    reference: reference.clone(),
                    truncated: bytes.len() as u64 != reference.byte_len,
                    text: String::from_utf8_lossy(&bytes).into_owned(),
                });
            }
        }
        Ok(SessionHistoryHit {
            event,
            snippet,
            score,
            payloads,
        })
    }

    async fn history_payload_prefix(
        &self,
        reference: &SessionPayloadRef,
        limit: usize,
    ) -> Result<Vec<u8>> {
        let row = sqlx::query(
            "select payload_kind, byte_len, substr(payload, 1, ?) as payload \
             from session_payloads where id = ?",
        )
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .bind(reference.id.to_string())
        .fetch_optional(&self.pool)
        .await?
        .with_context(|| format!("session payload {} does not exist", reference.id))?;
        ensure!(
            row.try_get::<&str, _>("payload_kind")? == payload_kind_name(reference.kind),
            "session payload {} has a different typed kind",
            reference.id
        );
        let stored_len =
            u64::try_from(row.try_get::<i64, _>("byte_len")?).context("negative payload length")?;
        ensure!(
            stored_len == reference.byte_len,
            "session payload {} length does not match its reference",
            reference.id
        );
        Ok(row.try_get("payload")?)
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

    /// Claim one trusted runtime namespace for a worker process. A running
    /// manifest owned by a different worker is evidence that Borg restarted
    /// or recovered; callers can then offer an explicit checkpoint restore
    /// without replaying arbitrary Python or JavaScript code.
    pub(crate) async fn activate_runtime_manifest(
        &self,
        session_id: Uuid,
        runtime: &str,
        root: &str,
        command: &str,
        worker_id: Uuid,
    ) -> Result<RuntimeManifestActivation> {
        ensure!(
            self.contains_session(session_id).await?,
            "session {session_id} does not exist"
        );
        ensure!(
            !runtime.trim().is_empty() && runtime.len() <= 128,
            "invalid runtime name"
        );
        ensure!(
            !root.trim().is_empty() && root.len() <= 16 * 1024,
            "invalid runtime root"
        );
        ensure!(
            !command.trim().is_empty() && command.len() <= 4 * 1024,
            "invalid runtime command"
        );

        let mut transaction = self.begin_write().await?;
        let existing = sqlx::query(
            "select manifest_version, session_id, runtime, root, command, worker_id, status, \
                    execution_count, last_code_hash, last_error, created_at, updated_at \
             from runtime_manifests where session_id=?",
        )
        .bind(session_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        let now = Utc::now();
        let (manifest, recovered_from_previous_worker) = if let Some(row) = existing {
            let existing = decode_runtime_manifest(&row)?;
            ensure!(
                existing.runtime == runtime,
                "runtime manifest for session {session_id} is for `{}`, not `{runtime}`",
                existing.runtime
            );
            ensure!(
                existing.root == root,
                "runtime manifest for session {session_id} is bound to a different root"
            );
            let recovered = existing.worker_id != worker_id
                && !matches!(existing.status, RuntimeManifestStatus::Stopped);
            sqlx::query(
                "update runtime_manifests set command=?, worker_id=?, status='running', \
                 updated_at=? where session_id=?",
            )
            .bind(command)
            .bind(worker_id.to_string())
            .bind(now.to_rfc3339())
            .bind(session_id.to_string())
            .execute(&mut *transaction)
            .await?;
            let mut manifest = existing;
            manifest.command = command.to_string();
            manifest.worker_id = worker_id;
            manifest.status = RuntimeManifestStatus::Running;
            manifest.updated_at = now;
            (manifest, recovered)
        } else {
            sqlx::query(
                "insert into runtime_manifests \
                 (session_id, manifest_version, runtime, root, command, worker_id, status, \
                  execution_count, last_code_hash, last_error, created_at, updated_at) \
                 values (?, ?, ?, ?, ?, ?, 'running', 0, null, null, ?, ?)",
            )
            .bind(session_id.to_string())
            .bind(i64::from(RUNTIME_MANIFEST_VERSION))
            .bind(runtime)
            .bind(root)
            .bind(command)
            .bind(worker_id.to_string())
            .bind(now.to_rfc3339())
            .bind(now.to_rfc3339())
            .execute(&mut *transaction)
            .await?;
            (
                RuntimeManifest {
                    manifest_version: RUNTIME_MANIFEST_VERSION,
                    session_id,
                    runtime: runtime.to_string(),
                    root: root.to_string(),
                    command: command.to_string(),
                    worker_id,
                    status: RuntimeManifestStatus::Running,
                    execution_count: 0,
                    last_code_hash: None,
                    last_error: None,
                    created_at: now,
                    updated_at: now,
                },
                false,
            )
        };
        transaction.commit().await?;
        Ok(RuntimeManifestActivation {
            manifest,
            recovered_from_previous_worker,
        })
    }

    pub(crate) async fn record_runtime_execution(
        &self,
        session_id: Uuid,
        worker_id: Uuid,
        code_hash: &str,
        worker_failed: bool,
        error: Option<&str>,
    ) -> Result<()> {
        ensure!(
            !code_hash.trim().is_empty(),
            "runtime execution code hash is empty"
        );
        let error = error.map(|value| value.chars().take(8 * 1024).collect::<String>());
        let status = if worker_failed { "failed" } else { "running" };
        let result = sqlx::query(
            "update runtime_manifests set status=?, execution_count=execution_count+1, \
             last_code_hash=?, last_error=?, updated_at=? \
             where session_id=? and worker_id=?",
        )
        .bind(status)
        .bind(code_hash)
        .bind(error)
        .bind(Utc::now().to_rfc3339())
        .bind(session_id.to_string())
        .bind(worker_id.to_string())
        .execute(&self.pool)
        .await?;
        ensure!(
            result.rows_affected() == 1,
            "runtime manifest for session {session_id} is not owned by worker {worker_id}"
        );
        Ok(())
    }

    pub(crate) async fn stop_runtime_manifest(
        &self,
        session_id: Uuid,
        worker_id: Uuid,
    ) -> Result<()> {
        let result = sqlx::query(
            "update runtime_manifests set status='stopped', updated_at=? \
             where session_id=? and worker_id=?",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(session_id.to_string())
        .bind(worker_id.to_string())
        .execute(&self.pool)
        .await?;
        ensure!(
            result.rows_affected() == 1,
            "runtime manifest for session {session_id} is not owned by worker {worker_id}"
        );
        Ok(())
    }

    pub(crate) async fn runtime_manifest(
        &self,
        session_id: Uuid,
    ) -> Result<Option<RuntimeManifest>> {
        let row = sqlx::query(
            "select manifest_version, session_id, runtime, root, command, worker_id, status, \
                    execution_count, last_code_hash, last_error, created_at, updated_at \
             from runtime_manifests where session_id=?",
        )
        .bind(session_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(decode_runtime_manifest).transpose()
    }

    pub(crate) async fn save_runtime_checkpoint(
        &self,
        session_id: Uuid,
        worker_id: Uuid,
        key: &str,
        state: &serde_json::Value,
    ) -> Result<RuntimeCheckpoint> {
        ensure!(
            !key.trim().is_empty() && key.len() <= 256,
            "invalid runtime checkpoint key"
        );
        ensure!(
            !key.starts_with(HARNESS_CHECKPOINT_PREFIX),
            "runtime checkpoint key is reserved for harness state"
        );
        ensure!(
            state.is_object(),
            "runtime checkpoint state must be a JSON object"
        );
        let state_json = serde_json::to_vec(state)?;
        ensure!(
            state_json.len() <= MAX_RUNTIME_CHECKPOINT_BYTES,
            "runtime checkpoint exceeds {MAX_RUNTIME_CHECKPOINT_BYTES} bytes"
        );
        let content_hash = format!("sha256:{}", hex::encode(Sha256::digest(&state_json)));
        let mut transaction = self.begin_write().await?;
        let manifest_exists: i64 = sqlx::query_scalar(
            "select exists(select 1 from runtime_manifests where session_id=? and worker_id=?)",
        )
        .bind(session_id.to_string())
        .bind(worker_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        ensure!(
            manifest_exists != 0,
            "runtime manifest for session {session_id} is not owned by worker {worker_id}"
        );
        let existing = sqlx::query(
            "select session_id, checkpoint_key, state_json, content_hash, revision, created_at \
             from runtime_checkpoints where session_id=? and checkpoint_key=?",
        )
        .bind(session_id.to_string())
        .bind(key)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(row) = existing {
            let existing = decode_runtime_checkpoint(&row)?;
            ensure!(
                existing.content_hash == content_hash,
                "runtime checkpoint `{key}` already exists with different content"
            );
            transaction.commit().await?;
            return Ok(existing);
        }
        let revision: i64 = sqlx::query_scalar(
            "select coalesce(max(revision), 0) + 1 from runtime_checkpoints where session_id=?",
        )
        .bind(session_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        let now = Utc::now();
        sqlx::query(
            "insert into runtime_checkpoints \
             (session_id, checkpoint_key, state_json, content_hash, revision, created_at) \
             values (?, ?, ?, ?, ?, ?)",
        )
        .bind(session_id.to_string())
        .bind(key)
        .bind(String::from_utf8(state_json).context("runtime checkpoint JSON was not UTF-8")?)
        .bind(&content_hash)
        .bind(revision)
        .bind(now.to_rfc3339())
        .execute(&mut *transaction)
        .await?;
        sqlx::query("update runtime_manifests set updated_at=? where session_id=?")
            .bind(now.to_rfc3339())
            .bind(session_id.to_string())
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(RuntimeCheckpoint {
            session_id,
            key: key.to_string(),
            state: state.clone(),
            content_hash,
            revision: u64::try_from(revision).context("negative runtime checkpoint revision")?,
            created_at: now,
        })
    }

    pub(crate) async fn runtime_checkpoint(
        &self,
        session_id: Uuid,
        key: Option<&str>,
    ) -> Result<Option<RuntimeCheckpoint>> {
        let (sql, bind_key) = if key.is_some() {
            (
                "select session_id, checkpoint_key, state_json, content_hash, revision, created_at \
                 from runtime_checkpoints where session_id=? and checkpoint_key=?",
                true,
            )
        } else {
            (
                "select session_id, checkpoint_key, state_json, content_hash, revision, created_at \
                 from runtime_checkpoints where session_id=? and checkpoint_key not like ? \
                 order by revision desc limit 1",
                false,
            )
        };
        let mut query = sqlx::query(sql).bind(session_id.to_string());
        if bind_key {
            query = query.bind(key.unwrap_or_default());
        } else {
            query = query.bind(format!("{HARNESS_CHECKPOINT_PREFIX}%"));
        }
        let row = query.fetch_optional(&self.pool).await?;
        row.as_ref().map(decode_runtime_checkpoint).transpose()
    }

    pub(crate) async fn list_runtime_checkpoints(
        &self,
        session_id: Uuid,
        limit: usize,
    ) -> Result<Vec<RuntimeCheckpoint>> {
        let rows = sqlx::query(
            "select session_id, checkpoint_key, state_json, content_hash, revision, created_at \
             from runtime_checkpoints where session_id=? and checkpoint_key not like ? \
             order by revision desc limit ?",
        )
        .bind(session_id.to_string())
        .bind(format!("{HARNESS_CHECKPOINT_PREFIX}%"))
        .bind(i64::try_from(limit.clamp(1, 100)).unwrap_or(100))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(decode_runtime_checkpoint).collect()
    }

    pub(crate) async fn load_harness_state(
        &self,
        session_id: Uuid,
    ) -> Result<Option<serde_json::Value>> {
        let row = sqlx::query(
            "select state_json from runtime_checkpoints \
             where session_id=? and checkpoint_key like ? order by revision desc limit 1",
        )
        .bind(session_id.to_string())
        .bind(format!("{HARNESS_CHECKPOINT_PREFIX}%"))
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            let state_json: &str = row.try_get("state_json")?;
            let state: serde_json::Value = serde_json::from_str(state_json)?;
            ensure!(
                state.is_object(),
                "stored harness state is not a JSON object"
            );
            Ok(state)
        })
        .transpose()
    }

    pub(crate) async fn save_harness_state(
        &self,
        session_id: Uuid,
        state: &serde_json::Value,
    ) -> Result<()> {
        ensure!(state.is_object(), "harness state must be a JSON object");
        let state_json = serde_json::to_vec(state)?;
        ensure!(
            state_json.len() <= MAX_RUNTIME_CHECKPOINT_BYTES,
            "harness state exceeds {MAX_RUNTIME_CHECKPOINT_BYTES} bytes"
        );
        let content_hash = format!("sha256:{}", hex::encode(Sha256::digest(&state_json)));
        let mut transaction = self.begin_write().await?;
        let revision: i64 = sqlx::query_scalar(
            "select coalesce(max(revision), 0) + 1 from runtime_checkpoints where session_id=?",
        )
        .bind(session_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        let key = format!("{HARNESS_CHECKPOINT_PREFIX}{revision}");
        let now = Utc::now();
        sqlx::query(
            "insert into runtime_checkpoints \
             (session_id, checkpoint_key, state_json, content_hash, revision, created_at) \
             values (?, ?, ?, ?, ?, ?)",
        )
        .bind(session_id.to_string())
        .bind(key)
        .bind(String::from_utf8(state_json).context("harness state JSON was not UTF-8")?)
        .bind(content_hash)
        .bind(revision)
        .bind(now.to_rfc3339())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "delete from runtime_checkpoints \
             where session_id=? and checkpoint_key like ? and revision < ?",
        )
        .bind(session_id.to_string())
        .bind(format!("{HARNESS_CHECKPOINT_PREFIX}%"))
        .bind(revision.saturating_sub(12))
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(crate) async fn rollback_harness_state(
        &self,
        session_id: Uuid,
        steps: usize,
    ) -> Result<serde_json::Value> {
        ensure!(
            (1..=12).contains(&steps),
            "harness rollback steps must be 1..=12"
        );
        let row = sqlx::query(
            "select state_json, revision from runtime_checkpoints \
             where session_id=? and checkpoint_key like ? order by revision desc limit 1 offset ?",
        )
        .bind(session_id.to_string())
        .bind(format!("{HARNESS_CHECKPOINT_PREFIX}%"))
        .bind(i64::try_from(steps).context("harness rollback offset exceeds SQLite integer")?)
        .fetch_optional(&self.pool)
        .await?
        .context("harness rollback target does not exist")?;
        let state_json: &str = row.try_get("state_json")?;
        let target_revision: i64 = row.try_get("revision")?;
        let state: serde_json::Value = serde_json::from_str(state_json)?;
        ensure!(
            state.is_object(),
            "stored harness state is not a JSON object"
        );
        sqlx::query(
            "delete from runtime_checkpoints \
             where session_id=? and checkpoint_key like ? and revision >= ?",
        )
        .bind(session_id.to_string())
        .bind(format!("{HARNESS_CHECKPOINT_PREFIX}%"))
        .bind(target_revision)
        .execute(&self.pool)
        .await?;
        self.save_harness_state(session_id, &state).await?;
        Ok(state)
    }

    /// Check readiness without scanning the full database contents.
    pub async fn readiness(&self) -> Result<SessionStoreHealth> {
        self.health_snapshot(false).await
    }

    /// Check the durable authority, including SQLite's exhaustive quick check.
    pub async fn health(&self) -> Result<SessionStoreHealth> {
        self.health_snapshot(true).await
    }

    async fn health_snapshot(&self, check_integrity: bool) -> Result<SessionStoreHealth> {
        let integrity = if check_integrity {
            sqlx::query_scalar("pragma quick_check")
                .fetch_one(&self.pool)
                .await?
        } else {
            "not_checked".to_string()
        };
        let journal_mode: String = sqlx::query_scalar("pragma journal_mode")
            .fetch_one(&self.pool)
            .await?;
        let synchronous: i64 = sqlx::query_scalar("pragma synchronous")
            .fetch_one(&self.pool)
            .await?;
        let foreign_keys: i64 = sqlx::query_scalar("pragma foreign_keys")
            .fetch_one(&self.pool)
            .await?;
        let journal_size_limit_bytes: i64 = sqlx::query_scalar("pragma journal_size_limit")
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
            integrity_checked: check_integrity,
            journal_mode,
            synchronous,
            foreign_keys: foreign_keys != 0,
            journal_size_limit_bytes,
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

    pub(crate) async fn begin_sqlite_write(
        pool: &SqlitePool,
    ) -> Result<Transaction<'static, Sqlite>, sqlx::Error> {
        Self::begin_sqlite_write_resilient(pool, SQLITE_WRITE_WAIT_TIMEOUT).await
    }

    async fn begin_sqlite_write_resilient(
        pool: &SqlitePool,
        attempt_timeout: Duration,
    ) -> Result<Transaction<'static, Sqlite>, sqlx::Error> {
        loop {
            match Self::begin_sqlite_write_with_timeout(pool, attempt_timeout).await {
                Err(sqlx::Error::PoolTimedOut) => {
                    // The journal is the durable source of truth, so restarting
                    // a healthy session cannot make progress while the same
                    // external writer or I/O stall remains. Keep the caller
                    // alive and retry; cancellation still drops this future
                    // immediately when the session is explicitly stopped.
                    warn!(
                        retry_seconds = attempt_timeout.as_secs(),
                        "SQLite session journal remains busy; continuing to wait"
                    );
                }
                result => return result,
            }
        }
    }

    async fn begin_sqlite_write_with_timeout(
        pool: &SqlitePool,
        timeout: Duration,
    ) -> Result<Transaction<'static, Sqlite>, sqlx::Error> {
        let deadline = tokio::time::Instant::now() + timeout;
        // SQLite has one writer at a time, but this pool also serves reads and
        // projections. Keep only one connection per database in this process
        // waiting for that writer lock; otherwise concurrent retries can
        // occupy every pooled connection and make unrelated store reads time
        // out.
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let write_gate = sqlite_write_begin_gate(pool);
        let _write_gate = match tokio::time::timeout(remaining, write_gate.lock()).await {
            Ok(guard) => guard,
            Err(_) => {
                warn!(
                    timeout_seconds = timeout.as_secs(),
                    "SQLite session journal writer wait timed out"
                );
                return Err(sqlx::Error::PoolTimedOut);
            }
        };
        let mut reported_contention = false;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                warn!(
                    timeout_seconds = timeout.as_secs(),
                    "SQLite session journal writer wait timed out"
                );
                return Err(sqlx::Error::PoolTimedOut);
            }
            let result =
                match tokio::time::timeout(remaining, pool.begin_with(SQLITE_WRITE_TRANSACTION))
                    .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        warn!(
                            timeout_seconds = timeout.as_secs(),
                            "SQLite session journal writer wait timed out"
                        );
                        return Err(sqlx::Error::PoolTimedOut);
                    }
                };
            match result {
                Ok(transaction) => return Ok(transaction),
                Err(error) if sqlite_lock_text(&error.to_string()) => {
                    if !reported_contention {
                        warn!(
                            error = %error,
                            "SQLite session journal is busy; waiting for the current writer"
                        );
                        reported_contention = true;
                    }
                    // A session event is not safely skippable: SQLite is the
                    // canonical journal, and returning SQLITE_BUSY here tears
                    // down an otherwise healthy actor. The connection-level
                    // busy timeout handles ordinary contention; keep waiting
                    // in bounded slices when another Borg process holds the
                    // writer lock longer (for example during a large commit),
                    // but do not let one stalled process block every actor
                    // indefinitely.
                    if tokio::time::Instant::now() >= deadline {
                        warn!(
                            error = %error,
                            timeout_seconds = timeout.as_secs(),
                            "SQLite session journal writer wait timed out"
                        );
                        return Err(sqlx::Error::PoolTimedOut);
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn begin_write(&self) -> Result<Transaction<'static, Sqlite>, sqlx::Error> {
        Self::begin_sqlite_write(&self.pool).await
    }

    pub async fn contains_session(&self, session_id: Uuid) -> Result<bool> {
        let found: i64 = sqlx::query_scalar("select exists(select 1 from sessions where id = ?)")
            .bind(session_id.to_string())
            .fetch_one(&self.pool)
            .await?;
        Ok(found != 0)
    }

    pub async fn discard_empty_session(&self, session_id: Uuid) -> Result<bool> {
        let mut transaction = self.begin_write().await?;
        let Some(state_json) =
            sqlx::query_scalar::<_, String>("select state_json from sessions where id = ?")
                .bind(session_id.to_string())
                .fetch_optional(&mut *transaction)
                .await?
        else {
            transaction.commit().await?;
            return Ok(false);
        };
        let state: SessionState = serde_json::from_str(&state_json)?;
        if state.has_resumable_activity() {
            transaction.commit().await?;
            return Ok(false);
        }
        let has_children: i64 = sqlx::query_scalar(
            "select exists(select 1 from sessions where owner_session_id = ? or parent_session_id = ?)",
        )
        .bind(session_id.to_string())
        .bind(session_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        if has_children != 0 {
            transaction.commit().await?;
            return Ok(false);
        }
        sqlx::query("delete from host_launches where session_id = ?")
            .bind(session_id.to_string())
            .execute(&mut *transaction)
            .await?;
        let deleted = sqlx::query("delete from sessions where id = ?")
            .bind(session_id.to_string())
            .execute(&mut *transaction)
            .await?
            .rows_affected()
            != 0;
        transaction.commit().await?;
        Ok(deleted)
    }

    /// Create a new execution session directly inside an existing or
    /// caller-selected workspace. This is only valid before the session has
    /// events, so a resumed transcript can never silently move workspaces.
    pub async fn create_session_in_workspace(
        &self,
        session_id: Uuid,
        workspace_id: Uuid,
    ) -> Result<SessionWorkspaceBinding> {
        self.create_session_in_workspace_as(session_id, workspace_id, session_id)
            .await
    }

    /// Create a fresh execution session attached to an existing durable agent
    /// participant. Cloud workspaces may keep that participant stable across
    /// many disposable execution sessions.
    pub async fn create_session_in_workspace_as(
        &self,
        session_id: Uuid,
        workspace_id: Uuid,
        participant_id: Uuid,
    ) -> Result<SessionWorkspaceBinding> {
        self.create_session(session_id).await?;
        let attached_at = Utc::now();
        let mut transaction = self.begin_write().await?;
        sqlx::query(
            "update session_workspace_bindings \
             set workspace_id=?, participant_id=?, host_id=null, attached_at=? \
             where session_id=? and workspace_id=? and participant_id=?",
        )
        .bind(workspace_id.to_string())
        .bind(participant_id.to_string())
        .bind(attached_at.to_rfc3339())
        .bind(session_id.to_string())
        .bind(session_id.to_string())
        .bind(session_id.to_string())
        .execute(&mut *transaction)
        .await?
        .rows_affected()
        .eq(&1)
        .then_some(())
        .context("new session workspace binding was not in its default state")?;
        transaction.commit().await?;
        Ok(SessionWorkspaceBinding {
            session_id,
            workspace_id,
            participant_id,
            host_id: None,
            attached_at,
        })
    }

    async fn ensure_schema(&self) -> Result<()> {
        let had_existing_schema = sqlx::query_scalar::<_, i64>(
            "select exists(select 1 from sqlite_master where type='table' and name in \
             ('sessions','session_events','session_actions','borg_session_schema'))",
        )
        .fetch_one(&self.pool)
        .await?
            != 0;
        let has_history_projection_table = sqlx::query_scalar::<_, i64>(
            "select exists(select 1 from sqlite_master where type='table' and name='session_event_search')",
        )
        .fetch_one(&self.pool)
        .await?
            != 0;
        let has_workspace_bindings_table = sqlx::query_scalar::<_, i64>(
            "select exists(select 1 from sqlite_master where type='table' and name='session_workspace_bindings')",
        )
        .fetch_one(&self.pool)
        .await?
            != 0;
        let has_schema_marker = sqlx::query_scalar::<_, i64>(
            "select exists(select 1 from sqlite_master where type='table' and name='borg_session_schema')",
        )
        .fetch_one(&self.pool)
        .await?
            != 0;
        let existing_schema_version: Option<i64> = if has_schema_marker {
            sqlx::query_scalar::<_, i64>("select version from borg_session_schema where id=1")
                .fetch_optional(&self.pool)
                .await?
        } else {
            None
        };
        if had_existing_schema {
            match existing_schema_version {
                Some(version) if version == SESSION_SCHEMA_VERSION => {}
                Some(version) if version > SESSION_SCHEMA_VERSION => {
                    bail!(
                        "unsupported future Borg session schema version {version}; current is {SESSION_SCHEMA_VERSION}"
                    );
                }
                Some(version) => {
                    bail!(
                        "{DISPOSABLE_SCHEMA_ERROR}: version {version} is older than the current version {SESSION_SCHEMA_VERSION}"
                    );
                }
                None => {
                    bail!(
                        "{DISPOSABLE_SCHEMA_ERROR}: legacy database has no current schema marker"
                    );
                }
            }
        }
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

            -- Search is a disposable projection. The event row and payload
            -- blobs above remain the only source of truth, while this table
            -- gives exact tenant/range filtering and FTS a compact join key.
            create table if not exists session_event_search (
                rowid integer primary key,
                session_id text not null references sessions(id) on delete cascade,
                sequence integer not null,
                event_id text not null,
                event_kind text not null,
                actor text,
                body text not null,
                unique (session_id, event_id)
            );

            create index if not exists idx_session_event_search_sequence
                on session_event_search (session_id, sequence);

            create virtual table if not exists session_event_fts using fts5(
                body,
                content='session_event_search',
                content_rowid='rowid',
                tokenize='unicode61 remove_diacritics 2'
            );

            create trigger if not exists session_event_search_insert
            after insert on session_event_search begin
                insert into session_event_fts(rowid, body) values (new.rowid, new.body);
            end;

            create trigger if not exists session_event_search_delete
            after delete on session_event_search begin
                insert into session_event_fts(session_event_fts, rowid, body)
                    values ('delete', old.rowid, old.body);
            end;

            create trigger if not exists session_event_search_update
            after update on session_event_search begin
                insert into session_event_fts(session_event_fts, rowid, body)
                    values ('delete', old.rowid, old.body);
                insert into session_event_fts(rowid, body) values (new.rowid, new.body);
            end;

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

            create index if not exists idx_session_events_context_clear
                on session_events (session_id, sequence desc)
                where event_kind = 'context_cleared';

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

            -- A runtime manifest records how a trusted namespace was opened
            -- and whether its worker survived. Checkpoints are explicit JSON
            -- data; executable code is never replayed automatically.
            create table if not exists runtime_manifests (
                session_id text primary key references sessions(id) on delete cascade,
                manifest_version integer not null,
                runtime text not null,
                root text not null,
                command text not null,
                worker_id text not null,
                status text not null,
                execution_count integer not null default 0,
                last_code_hash text,
                last_error text,
                created_at text not null,
                updated_at text not null
            );

            create table if not exists runtime_checkpoints (
                session_id text not null references sessions(id) on delete cascade,
                checkpoint_key text not null,
                state_json text not null,
                content_hash text not null,
                revision integer not null,
                created_at text not null,
                primary key (session_id, checkpoint_key),
                unique (session_id, revision)
            );

            create index if not exists idx_runtime_checkpoints_revision
                on runtime_checkpoints (session_id, revision desc);
            "#,
        )
        .execute(&self.pool)
        .await?;
        // A current database already has both disposable projections. Do not
        // rescan the canonical journal on every open; missing history rows
        // are repaired for the requested session by ensure_history_projection.
        // A newly-created store, or one missing the projection table, still
        // receives the complete rebuild while its canonical journal is small
        // or empty.
        if !had_existing_schema || !has_history_projection_table {
            sqlx::query(
                "insert into session_event_search \
                 (session_id, sequence, event_id, event_kind, actor, body) \
                 select e.session_id, e.sequence, e.event_id, e.event_kind, \
                        json_extract(e.event_json, '$.kind.actor'), \
                        e.event_json || coalesce(( \
                            select char(10) || group_concat(cast(p.payload as text), char(10)) \
                            from session_payloads p \
                            where p.session_id=e.session_id and p.event_id=e.event_id \
                        ), '') \
                 from session_events e \
                 where not exists ( \
                     select 1 from session_event_search s \
                     where s.session_id=e.session_id and s.event_id=e.event_id \
                 )",
            )
            .execute(&self.pool)
            .await?;
        }
        sqlx::query(
            "create table if not exists borg_session_schema (\
                id integer primary key check(id=1),\
                version integer not null\
            )",
        )
        .execute(&self.pool)
        .await?;
        crate::plugin_store::ensure_schema(&self.pool).await?;
        let mut schema_transaction = self.begin_write().await?;
        let schema_version: Option<i64> =
            sqlx::query_scalar("select version from borg_session_schema where id=1")
                .fetch_optional(&mut *schema_transaction)
                .await?;
        match schema_version {
            Some(version) if version == SESSION_SCHEMA_VERSION => {}
            Some(version) if version > SESSION_SCHEMA_VERSION => {
                bail!(
                    "unsupported future Borg session schema version {version}; current is {SESSION_SCHEMA_VERSION}"
                );
            }
            Some(version) => {
                bail!(
                    "{DISPOSABLE_SCHEMA_ERROR}: version {version} is older than the current version {SESSION_SCHEMA_VERSION}"
                );
            }
            None if had_existing_schema => {
                bail!("{DISPOSABLE_SCHEMA_ERROR}: legacy database has no current schema marker");
            }
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
        if !had_existing_schema || !has_workspace_bindings_table {
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
        }
        self.clear_terminal_live_state().await?;
        Ok(())
    }

    /// The store accepts only the current schema. Incompatible local session
    /// databases are archived and recreated by `open`; future schemas are
    /// rejected so a newer Borg cannot be silently destroyed by an older one.
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
            (
                "runtime_manifests",
                &[
                    "session_id",
                    "manifest_version",
                    "runtime",
                    "root",
                    "command",
                    "worker_id",
                    "status",
                    "execution_count",
                    "last_code_hash",
                    "last_error",
                    "created_at",
                    "updated_at",
                ][..],
            ),
            (
                "runtime_checkpoints",
                &[
                    "session_id",
                    "checkpoint_key",
                    "state_json",
                    "content_hash",
                    "revision",
                    "created_at",
                ][..],
            ),
        ] {
            // `table` comes exclusively from the fixed schema inventory above;
            // SQLx 0.9 requires this dynamic identifier to be explicitly
            // audited because SQLite cannot bind table names.
            let columns = sqlx::query(sqlx::AssertSqlSafe(format!("pragma table_info({table})")))
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
                    "{DISPOSABLE_SCHEMA_ERROR}: stale {table} table; missing columns: {}",
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
        let mut transaction = self.begin_write().await?;
        sqlx::query(
            "delete from session_live_state \
             where live_key <> 'context_window' \
               and session_id in ( \
                 select id from sessions \
                 where json_extract(state_json, '$.status') in \
                       ('ready', 'completed', 'failed', 'stopped') \
               )",
        )
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
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
        let mut transaction = self.begin_write().await?;
        let existing_owner: Option<String> =
            sqlx::query_scalar("select owner_session_id from sessions where id = ?")
                .bind(session_id.to_string())
                .fetch_one(&mut *transaction)
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
                .execute(&mut *transaction)
                .await?;
        }
        let owner_workspace: Option<String> = sqlx::query_scalar(
            "select workspace_id from session_workspace_bindings where session_id=?",
        )
        .bind(owner_session_id.to_string())
        .fetch_optional(&mut *transaction)
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
        .execute(&mut *transaction)
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
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
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

    async fn history_session_bounds(&self, session_id: Uuid) -> Result<(u64, u64)> {
        let row =
            sqlx::query("select inherited_event_count, next_sequence from sessions where id = ?")
                .bind(session_id.to_string())
                .fetch_optional(&self.pool)
                .await?
                .with_context(|| format!("session {session_id} does not exist"))?;
        Ok((
            u64::try_from(row.try_get::<i64, _>("inherited_event_count")?)
                .context("negative inherited event count")?,
            u64::try_from(row.try_get::<i64, _>("next_sequence")?)
                .context("negative next sequence")?,
        ))
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

    fn latest_completed_context_compaction_before<'a>(
        &'a self,
        session_id: Uuid,
        before_or_at: Option<u64>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<SessionEvent>>> + Send + 'a>> {
        Box::pin(async move {
            let session = self.session_row(session_id).await?;
            let logical_limit = before_or_at
                .unwrap_or(session.next_sequence.saturating_sub(1))
                .min(session.next_sequence.saturating_sub(1));
            if logical_limit > session.inherited_event_count {
                let event = sqlx::query_scalar::<_, String>(
                    "select event_json from session_events \
                     where session_id = ? and sequence > ? and sequence <= ? \
                     and event_kind = 'provider_event' \
                     and json_extract(event_json, '$.kind.kind') = 'context_compaction' \
                     and (json_extract(event_json, '$.kind.payload.status') = 'completed' \
                          or json_extract(event_json, '$.kind.payload.status') is null) \
                     and coalesce(json_extract(event_json, '$.kind.payload.provider_context_preserved'), 0) != 1 \
                     order by sequence desc limit 1",
                )
                .bind(session_id.to_string())
                .bind(i64::try_from(session.inherited_event_count).unwrap_or(i64::MAX))
                .bind(i64::try_from(logical_limit).unwrap_or(i64::MAX))
                .fetch_optional(&self.pool)
                .await?;
                if let Some(event) = event {
                    return Ok(Some(serde_json::from_str(&event)?));
                }
            }

            let Some(parent_session_id) = session.parent_session_id else {
                return Ok(None);
            };
            let Some(parent_cut_sequence) = session.parent_cut_sequence else {
                return Ok(None);
            };
            let Some(mut event) = self
                .latest_completed_context_compaction_before(
                    parent_session_id,
                    Some(parent_cut_sequence),
                )
                .await?
            else {
                return Ok(None);
            };
            let parent = self.session_row(parent_session_id).await?;
            let (sequence, _) = self
                .fork_projection(parent_session_id, event.sequence.saturating_add(1), &parent)
                .await?;
            if sequence == 0 || sequence > logical_limit.min(session.inherited_event_count) {
                return Ok(None);
            }
            event.id = inherited_event_id(session_id, event.id);
            event.session_id = session_id;
            event.sequence = sequence;
            Ok(Some(event))
        })
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

    async fn recovery_projection_from_sequence(
        &self,
        session_id: Uuid,
        recovery_start_sequence: i64,
        resolved_message_id: Option<Uuid>,
    ) -> Result<SessionRecovery> {
        // A replay boundary does not discard unresolved prompts or the latest
        // durable state of existing subagents.
        let session_key = session_id.to_string();
        let resolved_message_id = resolved_message_id.map(|id| id.to_string());
        let suffix = sqlx::query(
            "select e.event_json from session_events e \
                 where e.session_id = ? and e.sequence >= ? and e.recovery_relevant = 1 \
                 and (e.event_kind != 'subagent_activity' or e.sequence in ( \
                   select max(sequence) from session_events \
                   where session_id = ? and event_kind = 'subagent_activity' \
                   group by json_extract(event_json, '$.kind.agent.session_id') \
                 )) order by e.sequence",
        )
        .bind(&session_key)
        .bind(recovery_start_sequence)
        .bind(&session_key)
        .fetch_all(&self.pool);
        let legacy_messages = sqlx::query(
            "select e.event_json from session_event_search search \
                 join session_events e \
                   on e.session_id = search.session_id and e.event_id = search.event_id \
                 left join session_actions a \
                   on a.session_id = e.session_id and a.action_id = e.message_id \
                 where search.session_id = ? and search.sequence < ? \
                 and search.event_kind = 'message' \
                 and search.actor in ('user', 'system') \
                 and (? is null or e.message_id != ?) \
                 and (a.action_id is null \
                   or a.state not in ('completed', 'failed', 'cancelled')) \
                 order by search.sequence",
        )
        .bind(&session_key)
        .bind(recovery_start_sequence)
        .bind(&resolved_message_id)
        .bind(&resolved_message_id)
        .fetch_all(&self.pool);
        let legacy_recalls = sqlx::query(
            "select e.event_json from session_event_search search \
                 join session_events e \
                   on e.session_id = search.session_id and e.event_id = search.event_id \
                 where search.session_id = ? and search.sequence < ? \
                 and search.event_kind = 'prompt_recalled' order by search.sequence",
        )
        .bind(&session_key)
        .bind(recovery_start_sequence)
        .fetch_all(&self.pool);
        let prior_subagents = sqlx::query(
            "select e.event_json from session_events e \
                 where e.session_id = ? and e.sequence < ? \
                 and e.event_kind = 'subagent_activity' and e.sequence in ( \
                   select max(sequence) from session_events \
                   where session_id = ? and event_kind = 'subagent_activity' \
                   group by json_extract(event_json, '$.kind.agent.session_id') \
                 ) order by e.sequence",
        )
        .bind(&session_key)
        .bind(recovery_start_sequence)
        .bind(&session_key)
        .fetch_all(&self.pool);
        let (suffix, legacy_messages, legacy_recalls, prior_subagents) =
            tokio::try_join!(suffix, legacy_messages, legacy_recalls, prior_subagents)?;
        let suffix = suffix
            .into_iter()
            .map(|row| serde_json::from_str(row.try_get("event_json")?).map_err(Into::into))
            .collect::<Result<Vec<SessionEvent>>>()?;
        let mut recovery = SessionRecovery::from_events(suffix);
        let mut queue_events = legacy_messages
            .into_iter()
            .chain(legacy_recalls)
            .map(|row| serde_json::from_str(row.try_get("event_json")?).map_err(Into::into))
            .collect::<Result<Vec<SessionEvent>>>()?;
        queue_events.sort_unstable_by_key(|event| event.sequence);
        queue_events.append(&mut recovery.queue_events);
        recovery.queue_events = queue_events;
        let mut subagent_events = prior_subagents
            .into_iter()
            .map(|row| serde_json::from_str(row.try_get("event_json")?).map_err(Into::into))
            .collect::<Result<Vec<SessionEvent>>>()?;
        subagent_events.append(&mut recovery.subagent_events);
        recovery.subagent_events = subagent_events;
        Ok(recovery)
    }

    async fn provider_checkpoint_recovery_projection(
        &self,
        session_id: Uuid,
        provider_session_id: &str,
    ) -> Result<Option<SessionRecovery>> {
        if self.session_row(session_id).await?.inherited_event_count != 0 {
            return Ok(None);
        }
        let Some(row) = sqlx::query(
            "select sequence, event_json from session_events \
             where session_id = ? and event_kind = 'turn_completed' \
               and json_extract(event_json, '$.kind.provider_session_id') = ? \
               and (json_extract(event_json, '$.kind.error') is null \
                    or json_extract(event_json, '$.kind.error') = 'turn interrupted') \
             order by sequence desc limit 1",
        )
        .bind(session_id.to_string())
        .bind(provider_session_id)
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(None);
        };
        let sequence = row.try_get::<i64, _>("sequence")?;
        let checkpoint: SessionEvent = serde_json::from_str(row.try_get("event_json")?)?;
        let SessionEventKind::TurnCompleted { message_id, .. } = checkpoint.kind else {
            unreachable!("provider checkpoint query returned a non-terminal event");
        };
        Ok(Some(
            self.recovery_projection_from_sequence(session_id, sequence, Some(message_id))
                .await?,
        ))
    }

    async fn recovery_projection(&self, session_id: Uuid) -> Result<SessionRecovery> {
        let boundary = sqlx::query(
            "select s.inherited_event_count, ( \
               select e.sequence from session_events e \
               where e.session_id = s.id and e.event_kind = 'context_cleared' \
               order by e.sequence desc limit 1 \
             ) as context_clear_sequence, ( \
               select e.sequence from session_events e \
               where e.session_id = s.id and e.event_kind = 'provider_event' \
                 and json_extract(e.event_json, '$.kind.kind') = 'context_compaction' \
                 and (json_extract(e.event_json, '$.kind.payload.status') = 'completed' \
                   or json_extract(e.event_json, '$.kind.payload.status') is null) \
                 and (coalesce(json_extract(e.event_json, \
                       '$.kind.payload.provider_context_preserved'), 0) != 1 \
                   or coalesce(json_extract(e.event_json, \
                       '$.kind.payload.provider_recovery_checkpoint'), 0) = 1) \
               order by e.sequence desc limit 1 \
             ) as context_compaction_sequence \
             from sessions s where s.id = ?",
        )
        .bind(session_id.to_string())
        .fetch_optional(&self.pool)
        .await?
        .with_context(|| format!("session {session_id} does not exist"))?;
        let inherited_event_count =
            u64::try_from(boundary.try_get::<i64, _>("inherited_event_count")?)
                .context("negative inherited event count")?;
        let context_clear_sequence =
            boundary.try_get::<Option<i64>, _>("context_clear_sequence")?;
        let context_compaction_sequence =
            boundary.try_get::<Option<i64>, _>("context_compaction_sequence")?;
        let mut recovery_start_sequence = context_clear_sequence;
        if inherited_event_count == 0
            && let Some(compaction_sequence) = context_compaction_sequence
            && context_clear_sequence
                .map(|clear_sequence| compaction_sequence > clear_sequence)
                .unwrap_or(true)
        {
            let successful_turn_sequence = sqlx::query_scalar::<_, i64>(
                "select sequence from session_events \
                 where session_id = ? and sequence < ? and event_kind = 'turn_completed' \
                   and json_extract(event_json, '$.kind.error') is null \
                 order by sequence desc limit 1",
            )
            .bind(session_id.to_string())
            .bind(compaction_sequence)
            .fetch_optional(&self.pool)
            .await?;
            if let Some(successful_turn_sequence) = successful_turn_sequence {
                let compaction_recovery_start = successful_turn_sequence.saturating_add(1);
                recovery_start_sequence = Some(
                    recovery_start_sequence
                        .map(|sequence| sequence.max(compaction_recovery_start))
                        .unwrap_or(compaction_recovery_start),
                );
            }
        }
        if inherited_event_count == 0
            && let Some(recovery_start_sequence) = recovery_start_sequence
        {
            return self
                .recovery_projection_from_sequence(session_id, recovery_start_sequence, None)
                .await;
        }
        let events = self
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
            .collect();
        Ok(SessionRecovery::from_events(events))
    }

    async fn fork_projection(
        &self,
        parent_session_id: Uuid,
        sequence: u64,
        parent: &StoredSession,
    ) -> Result<(u64, SessionState)> {
        if parent.parent_session_id.is_none() {
            let cut = i64::try_from(sequence.saturating_sub(1)).unwrap_or(i64::MAX);
            let row = sqlx::query(
                "select count(*) as inherited_event_count, max(sequence) as target_sequence \
                 from session_events where session_id = ? and sequence <= ? \
                 and fork_inheritable = 1",
            )
            .bind(parent_session_id.to_string())
            .bind(cut)
            .fetch_one(&self.pool)
            .await?;
            let inherited_event_count =
                u64::try_from(row.try_get::<i64, _>("inherited_event_count")?)
                    .context("negative inherited event count")?;
            let target_sequence = row
                .try_get::<Option<i64>, _>("target_sequence")?
                .map(u64::try_from)
                .transpose()
                .context("negative fork projection sequence")?;
            let mut projection = match target_sequence {
                Some(target_sequence) => match self
                    .local_projection_at(parent_session_id, 0, target_sequence)
                    .await?
                {
                    Some(projection) => projection,
                    None => SessionState::reduce(
                        &self
                            .projected_events(parent_session_id, Some(target_sequence))
                            .await?,
                    )?,
                },
                None => SessionState::default(),
            };
            projection.latest_sequence = inherited_event_count;
            return Ok((projection.latest_sequence, projection));
        }
        // Local checkpoints already include inherited state, so a descendant
        // fork only needs the bounded local tail. Cuts inside inherited
        // ancestry retain the recursive lineage path.
        let cut = sequence.saturating_sub(1);
        if cut > parent.inherited_event_count
            && let Some(mut state) = self
                .local_projection_at(parent_session_id, parent.inherited_event_count, cut)
                .await?
        {
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
            state.latest_sequence = inherited_event_count;
            return Ok((inherited_event_count, state));
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

    async fn local_projection_at(
        &self,
        session_id: Uuid,
        local_start_sequence: u64,
        target_sequence: u64,
    ) -> Result<Option<SessionState>> {
        let checkpoint = sqlx::query(
            "select sequence, projection_json, created_at from session_events \
             where session_id = ? and sequence > ? and sequence <= ? \
             and projection_json <> '' order by sequence desc limit 1",
        )
        .bind(session_id.to_string())
        .bind(i64::try_from(local_start_sequence).unwrap_or(i64::MAX))
        .bind(i64::try_from(target_sequence).unwrap_or(i64::MAX))
        .fetch_optional(&self.pool)
        .await?;
        let Some(checkpoint) = checkpoint else {
            return Ok(None);
        };
        let checkpoint_sequence = u64::try_from(checkpoint.try_get::<i64, _>("sequence")?)
            .context("negative fork projection sequence")?;
        let mut state: SessionState = serde_json::from_str(checkpoint.try_get("projection_json")?)?;
        state.activity_at = Some(
            DateTime::parse_from_rfc3339(checkpoint.try_get("created_at")?)?.with_timezone(&Utc),
        );
        let rows = sqlx::query(
            "select event_json from session_events where session_id = ? \
             and sequence > ? and sequence <= ? order by sequence",
        )
        .bind(session_id.to_string())
        .bind(i64::try_from(checkpoint_sequence).unwrap_or(i64::MAX))
        .bind(i64::try_from(target_sequence).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await?;
        for row in rows {
            let event: SessionEvent = serde_json::from_str(row.try_get("event_json")?)?;
            state.apply(&event)?;
        }
        Ok(Some(state))
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

    /// Atomically admit a workflow start if this workflow identity has not
    /// already been journaled. The write transaction serializes the read and
    /// append, so two callers cannot both publish a Started event for one
    /// workflow id during concurrent admission.
    pub(crate) async fn ensure_workflow_started(
        &self,
        event: SessionEvent,
        workflow_id: Uuid,
    ) -> Result<SessionEvent> {
        ensure!(
            matches!(
                &event.kind,
                SessionEventKind::BluWorkflowStarted { .. }
                    | SessionEventKind::RuntimeWorkflowStarted { .. }
            ),
            "workflow admission requires a Started event"
        );
        let kind = event_kind(&event.kind)?;
        let mut transaction = self.begin_write().await?;
        let rows = sqlx::query(
            "select event_json from session_events \
             where session_id=? and event_kind=? order by sequence",
        )
        .bind(event.session_id.to_string())
        .bind(kind)
        .fetch_all(&mut *transaction)
        .await?;
        for row in rows {
            let existing: SessionEvent = serde_json::from_str(row.try_get("event_json")?)?;
            if workflow_event_id(&existing.kind) == Some(workflow_id) {
                transaction.commit().await?;
                return Ok(existing);
            }
        }
        let admitted = self
            .append_durable_in_transaction(&mut transaction, event)
            .await?;
        transaction.commit().await?;
        Ok(admitted)
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
        let row = sqlx::query(
            "select inherited_event_count, next_sequence, state_json from sessions where id = ?",
        )
        .bind(event.session_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .with_context(|| format!("session {} does not exist", event.session_id))?;
        let inherited_event_count = u64::try_from(row.try_get::<i64, _>("inherited_event_count")?)
            .context("negative inherited event count")?;
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
        let search_body = serde_json::to_string(&event)?;
        let stored_event_kind = event_kind(&event.kind)?;
        let actor = event_actor(&event.kind);
        let compact_event = self.compact_payloads(transaction, &event).await?;
        let event_json = serde_json::to_string(&compact_event)?;
        let projection_json = serde_json::to_string(&state)?;
        let historical_projection =
            historical_projection_json(event.sequence, inherited_event_count, &projection_json);
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
        .bind(&stored_event_kind)
        .bind(event_json)
        .bind(historical_projection)
        .bind(i64::from(event.kind.is_fork_inheritable()))
        .bind(i64::from(event.kind.is_recovery_relevant()))
        .bind(message_id)
        .bind(event.created_at.to_rfc3339())
        .execute(&mut **transaction)
        .await?;
        sqlx::query(
            "insert into session_event_search \
             (session_id, sequence, event_id, event_kind, actor, body) \
             values (?, ?, ?, ?, ?, ?)",
        )
        .bind(event.session_id.to_string())
        .bind(i64::try_from(event.sequence).context("session sequence exceeds SQLite integer")?)
        .bind(event.id.to_string())
        .bind(stored_event_kind)
        .bind(actor)
        .bind(search_body)
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
        Ok(compact_event)
    }
}

fn sqlite_write_begin_gate(pool: &SqlitePool) -> Arc<tokio::sync::Mutex<()>> {
    static GATES: OnceLock<Mutex<HashMap<PathBuf, Weak<tokio::sync::Mutex<()>>>>> = OnceLock::new();
    let path = pool.connect_options().get_filename().to_owned();
    let mut gates = GATES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("SQLite writer gate registry lock poisoned");
    gates.retain(|_, gate| gate.strong_count() > 0);
    if let Some(gate) = gates.get(&path).and_then(Weak::upgrade) {
        return gate;
    }
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    gates.insert(path, Arc::downgrade(&gate));
    gate
}

fn is_disposable_schema_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.to_string().starts_with(DISPOSABLE_SCHEMA_ERROR))
}

fn archive_incompatible_database(path: &Path) -> Result<PathBuf> {
    ensure!(
        path.exists(),
        "incompatible session database {} disappeared before it could be archived",
        path.display()
    );
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("sessions.sqlite3");
    let stamp = Utc::now().timestamp_nanos_opt().unwrap_or_default();
    let archived = (0..100)
        .map(|attempt| {
            parent.join(format!(
                "{file_name}.incompatible-{stamp}-{}-{attempt}",
                std::process::id()
            ))
        })
        .find(|candidate| !candidate.exists())
        .context("could not choose an archive path for the incompatible session database")?;
    fs::rename(path, &archived).with_context(|| {
        format!(
            "failed to archive incompatible session database {}",
            path.display()
        )
    })?;
    for suffix in ["-wal", "-shm"] {
        let sidecar = path_with_suffix(path, suffix);
        if sidecar.exists() {
            let archived_sidecar = path_with_suffix(&archived, suffix);
            if let Err(error) = fs::rename(&sidecar, &archived_sidecar) {
                warn!(
                    sidecar = %sidecar.display(),
                    archived_sidecar = %archived_sidecar.display(),
                    %error,
                    "could not archive an old SQLite sidecar"
                );
            }
        }
    }
    #[cfg(unix)]
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("failed to sync database directory {}", parent.display()))?;
    Ok(archived)
}

fn path_with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn sqlite_schema_lock(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    sqlite_lock_text(&message)
        // SQLite can expose a partially constructed external-content FTS
        // virtual table to the second opener while the first connection is
        // still finishing the schema batch. Treat that projection-specific
        // constructor error like the lock it represents and retry the whole
        // idempotent schema batch.
        || (message.contains("vtable constructor failed")
            && message.contains("session_event_fts"))
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

fn decode_runtime_manifest(row: &SqliteRow) -> Result<RuntimeManifest> {
    let manifest_version = u32::try_from(row.try_get::<i64, _>("manifest_version")?)
        .context("negative runtime manifest version")?;
    ensure!(
        manifest_version == RUNTIME_MANIFEST_VERSION,
        "unsupported runtime manifest version {manifest_version}"
    );
    Ok(RuntimeManifest {
        manifest_version,
        session_id: parse_uuid(row.try_get("session_id")?)?,
        runtime: row.try_get("runtime")?,
        root: row.try_get("root")?,
        command: row.try_get("command")?,
        worker_id: parse_uuid(row.try_get("worker_id")?)?,
        status: parse_enum(row.try_get("status")?)?,
        execution_count: u64::try_from(row.try_get::<i64, _>("execution_count")?)
            .context("negative runtime execution count")?,
        last_code_hash: row.try_get("last_code_hash")?,
        last_error: row.try_get("last_error")?,
        created_at: parse_timestamp(Some(row.try_get("created_at")?))?
            .context("missing runtime manifest created_at")?,
        updated_at: parse_timestamp(Some(row.try_get("updated_at")?))?
            .context("missing runtime manifest updated_at")?,
    })
}

fn decode_runtime_checkpoint(row: &SqliteRow) -> Result<RuntimeCheckpoint> {
    let state_json: &str = row.try_get("state_json")?;
    ensure!(
        state_json.len() <= MAX_RUNTIME_CHECKPOINT_BYTES,
        "runtime checkpoint exceeds {MAX_RUNTIME_CHECKPOINT_BYTES} bytes"
    );
    let state: serde_json::Value = serde_json::from_str(state_json)?;
    ensure!(
        state.is_object(),
        "runtime checkpoint state is not a JSON object"
    );
    let content_hash: String = row.try_get("content_hash")?;
    ensure!(
        content_hash
            == format!(
                "sha256:{}",
                hex::encode(Sha256::digest(state_json.as_bytes()))
            ),
        "runtime checkpoint content hash does not match its state"
    );
    Ok(RuntimeCheckpoint {
        session_id: parse_uuid(row.try_get("session_id")?)?,
        key: row.try_get("checkpoint_key")?,
        state,
        content_hash,
        revision: u64::try_from(row.try_get::<i64, _>("revision")?)
            .context("negative runtime checkpoint revision")?,
        created_at: parse_timestamp(Some(row.try_get("created_at")?))?
            .context("missing runtime checkpoint created_at")?,
    })
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
            status: status @ (MessageStatus::Queued | MessageStatus::InProgress),
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
            insert_action_row(transaction, &action, *status == MessageStatus::InProgress).await?;
        }
        SessionEventKind::Message {
            message_id,
            actor: crate::EventActor::User | crate::EventActor::System,
            status: status @ (MessageStatus::Complete | MessageStatus::Failed),
            ..
        } => {
            advance_action(
                transaction,
                event.session_id,
                *message_id,
                if *status == MessageStatus::Complete {
                    SessionActionState::Completed
                } else {
                    SessionActionState::Failed
                },
                None,
            )
            .await?;
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
        SessionEventKind::RuntimeWorkflowStarted {
            workflow_id,
            runtime,
            artifact_hash,
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
                    "runtime": runtime,
                    "artifact_hash": artifact_hash,
                    "name": name,
                }),
            );
            action.created_at = event.created_at;
            action.updated_at = event.created_at;
            create_action_and_advance(transaction, action, SessionActionState::Running, None)
                .await?;
        }
        SessionEventKind::RuntimeWorkflowCompleted {
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
    insert_action_row(transaction, &action, false).await?;
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
    allow_in_progress_payload_rewrite: bool,
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
        if allow_in_progress_payload_rewrite && existing.state.is_terminal() {
            // Recovery replays durable in-progress snapshots. A snapshot can
            // legitimately arrive after the action's terminal event (for
            // example when a coalesced queue was interrupted), but it must not
            // resurrect or rewrite a completed action. A later queued event
            // is the explicit retry boundary and still follows the immutable
            // payload checks below.
            tracing::debug!(
                action_id = %action.action_id,
                state = ?existing.state,
                "ignoring stale in-progress snapshot for terminal action"
            );
            return Ok(());
        }
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
            requeue_terminal_action(transaction, existing).await?;
            return Ok(());
        }
        if allow_in_progress_payload_rewrite
            && existing.kind == crate::SessionActionKind::Prompt
            && action.kind == crate::SessionActionKind::Steering
            && same_prompt_payload_ignoring_delivery(&existing.payload, &action.payload)
        {
            // Escape can promote a queued prompt into the active provider
            // turn. The provider's admission event is the durable routing
            // boundary; preserve the action identity and current state while
            // changing only its delivery class.
            anyhow::ensure!(
                !existing.state.is_terminal(),
                "action {} completed before its queued input was flushed",
                action.action_id
            );
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
        if allow_in_progress_payload_rewrite && existing.payload != action.payload {
            // Queue coalescing combines several durable queue entries under
            // the last message id immediately before execution. The
            // in-progress event is the authoritative executed payload for
            // that action; update its projection without allowing arbitrary
            // queued-message identity reuse to bypass the immutable check.
            anyhow::ensure!(
                existing.kind == action.kind,
                "action {} changed kind before its in-progress payload changed",
                action.action_id
            );
            anyhow::ensure!(
                !existing.state.is_terminal(),
                "action {} was completed before its in-progress payload changed",
                action.action_id
            );
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
            let mut rewritten = existing;
            rewritten.kind = action.kind;
            rewritten.delivery = action.delivery;
            rewritten.wake = action.wake;
            rewritten.payload = action.payload.clone();
            requeue_failed_action(transaction, rewritten).await?;
            return Ok(());
        }
        anyhow::ensure!(
            existing.kind == action.kind && existing.payload == action.payload,
            "action {} was reused with a different immutable payload",
            action.action_id
        );
        requeue_failed_action(transaction, existing).await?;
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

/// A retry is represented by another durable queued message with the same
/// action id.  Re-admission must therefore move the existing failed projection
/// back into the executable state machine instead of treating the duplicate
/// insert as a no-op.
async fn requeue_failed_action(
    transaction: &mut Transaction<'_, Sqlite>,
    mut action: SessionAction,
) -> Result<()> {
    if action.state != SessionActionState::Failed {
        return Ok(());
    }
    let from_state = action.state;
    action.transition(Some(from_state), SessionActionState::Queued, None)?;
    append_action_transition(transaction, &action, from_state, None).await?;
    update_action_row(transaction, &action).await
}

/// A legacy accepted steer can have a terminal action projection before its
/// interruption requeues the same message into the FIFO. The durable queue
/// event is an explicit retry boundary, so reopen both terminal states here;
/// ordinary prompt duplicates still use `requeue_failed_action`.
async fn requeue_terminal_action(
    transaction: &mut Transaction<'_, Sqlite>,
    mut action: SessionAction,
) -> Result<()> {
    if !matches!(
        action.state,
        SessionActionState::Completed | SessionActionState::Failed
    ) {
        return Ok(());
    }
    let from_state = action.state;
    action.transition(Some(from_state), SessionActionState::Queued, None)?;
    append_action_transition(transaction, &action, from_state, None).await?;
    update_action_row(transaction, &action).await
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
        let mut transaction = self.begin_write().await?;
        sqlx::query(
            "insert into sessions \
             (id, state_json, projection_version, created_at, updated_at) values (?, ?, 3, ?, ?) \
             on conflict(id) do nothing",
        )
        .bind(session_id.to_string())
        .bind(serde_json::to_string(&SessionState::default())?)
        .bind(&now)
        .bind(&now)
        .execute(&mut *transaction)
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
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
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
        transition: ClaimedActionTransition,
    ) -> Result<SessionAction> {
        let ClaimedActionTransition {
            session_id,
            action_id,
            lease_owner,
            lease_token,
            expected,
            next,
            error,
        } = transition;
        let mut transaction = self.begin_write().await?;
        let mut action = load_action_for_update(&mut transaction, session_id, action_id).await?;
        validate_live_lease(&action, &lease_owner, lease_token, Utc::now())?;
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
        session_id: Uuid,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<SessionAction>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut transaction = self.begin_write().await?;
        let rows = sqlx::query(
            "select * from session_actions \
             where session_id = ? \
               and state in ('running', 'committing') \
               and (lease_expires_at is null or lease_expires_at <= ?) \
             order by lease_expires_at, created_at, action_id limit ?",
        )
        .bind(session_id.to_string())
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

    async fn latest_completed_context_compaction(
        &self,
        session_id: Uuid,
    ) -> Result<Option<SessionEvent>> {
        self.latest_completed_context_compaction_before(session_id, None)
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
        let rows = sqlx::query(
            "select event_json from session_events \
             where session_id = ? and event_kind = 'message' \
             and json_extract(event_json, '$.kind.actor') = 'user' \
             and json_extract(event_json, '$.kind.status') in ('complete', 'failed') \
             order by sequence desc limit ?",
        )
        .bind(session_id.to_string())
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await?;
        let mut messages = rows
            .into_iter()
            .map(|row| serde_json::from_str(row.try_get("event_json")?).map_err(Into::into))
            .collect::<Result<Vec<SessionEvent>>>()?;
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
        self.recovery_projection(session_id).await
    }

    async fn recovery_from_provider_checkpoint(
        &self,
        session_id: Uuid,
        provider_session_id: &str,
    ) -> Result<Option<SessionRecovery>> {
        self.provider_checkpoint_recovery_projection(session_id, provider_session_id)
            .await
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
        let mut transaction = self.begin_write().await?;
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
        .execute(&mut *transaction)
        .await?;
        let parent_workspace: Option<String> = sqlx::query_scalar(
            "select workspace_id from session_workspace_bindings where session_id=?",
        )
        .bind(parent_session_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        sqlx::query(
            "insert into session_workspace_bindings \
             (session_id, workspace_id, participant_id, attached_at) values (?, ?, ?, ?)",
        )
        .bind(session_id.to_string())
        .bind(parent_workspace.unwrap_or_else(|| parent_session_id.to_string()))
        .bind(session_id.to_string())
        .bind(Utc::now().to_rfc3339())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
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

fn event_actor(kind: &SessionEventKind) -> Option<&'static str> {
    let SessionEventKind::Message { actor, .. } = kind else {
        return None;
    };
    Some(match actor {
        crate::EventActor::User => "user",
        crate::EventActor::Assistant => "assistant",
        crate::EventActor::Tool => "tool",
        crate::EventActor::System => "system",
    })
}

fn history_index_document_id(session_id: Uuid, event_id: Uuid) -> String {
    format!("borg-session-event:v1:{session_id}:{event_id}")
}

fn history_limit(query: &SessionHistoryQuery) -> usize {
    query
        .limit
        .unwrap_or(DEFAULT_HISTORY_LIMIT)
        .clamp(1, MAX_HISTORY_LIMIT)
}

fn history_scan_limit(query: &SessionHistoryQuery) -> usize {
    query
        .scan_limit
        .unwrap_or(DEFAULT_HISTORY_SCAN_LIMIT)
        .clamp(history_limit(query), MAX_HISTORY_SCAN_LIMIT)
}

fn history_payload_budget(query: &SessionHistoryQuery) -> usize {
    if !query.expand_payloads {
        return 0;
    }
    query
        .max_payload_bytes
        .unwrap_or(DEFAULT_HISTORY_PAYLOAD_BYTES)
        .clamp(1, MAX_HISTORY_PAYLOAD_BYTES)
}

fn push_history_sql_filters(
    sql: &mut QueryBuilder<Sqlite>,
    query: &SessionHistoryQuery,
    alias: &str,
) {
    if let Some(event_id) = query.event_id {
        sql.push(format!(" and {alias}.event_id = "))
            .push_bind(event_id.to_string());
    }
    if let Some(start) = query.start_sequence {
        sql.push(format!(" and {alias}.sequence >= "))
            .push_bind(i64::try_from(start).unwrap_or(i64::MAX));
    }
    if let Some(end) = query.end_sequence {
        sql.push(format!(" and {alias}.sequence <= "))
            .push_bind(i64::try_from(end).unwrap_or(i64::MAX));
    }
    if !query.event_kinds.is_empty() {
        sql.push(format!(" and {alias}.event_kind in ("));
        let mut separated = sql.separated(", ");
        for kind in &query.event_kinds {
            separated.push_bind(kind.clone());
        }
        separated.push_unseparated(")");
    }
    if !query.actors.is_empty() {
        if alias == "e" {
            sql.push(" and json_extract(e.event_json, '$.kind.actor') in (");
        } else {
            sql.push(format!(" and {alias}.actor in ("));
        }
        let mut separated = sql.separated(", ");
        for actor in &query.actors {
            separated.push_bind(history_actor_name(*actor));
        }
        separated.push_unseparated(")");
    }
}

fn history_event_matches_filters(
    event: &SessionEvent,
    query: &SessionHistoryQuery,
) -> Result<bool> {
    if query.event_id.is_some_and(|event_id| event.id != event_id)
        || query
            .start_sequence
            .is_some_and(|start| event.sequence < start)
        || query.end_sequence.is_some_and(|end| event.sequence > end)
    {
        return Ok(false);
    }
    if !query.event_kinds.is_empty() {
        let stored_kind = event_kind(&event.kind)?;
        if !query.event_kinds.contains(&stored_kind) {
            return Ok(false);
        }
    }
    if !query.actors.is_empty() {
        let SessionEventKind::Message { actor, .. } = event.kind else {
            return Ok(false);
        };
        if !query.actors.contains(&actor) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn history_actor_name(actor: crate::EventActor) -> &'static str {
    match actor {
        crate::EventActor::User => "user",
        crate::EventActor::Assistant => "assistant",
        crate::EventActor::Tool => "tool",
        crate::EventActor::System => "system",
    }
}

fn history_fts_query(text: &str) -> Result<String> {
    let terms = history_literal_terms(text, true)?;
    ensure!(
        !terms.is_empty(),
        "history lexical query has no searchable terms"
    );
    Ok(terms
        .into_iter()
        .map(|term| format!("\"{}\"", term.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" AND "))
}

fn history_literal_terms(text: &str, case_sensitive: bool) -> Result<Vec<String>> {
    let terms = text
        .split_whitespace()
        .filter(|term| !term.is_empty())
        .take(64)
        .map(|term| {
            if case_sensitive {
                term.to_string()
            } else {
                term.to_lowercase()
            }
        })
        .collect::<Vec<_>>();
    ensure!(!terms.is_empty(), "history query has no searchable terms");
    Ok(terms)
}

fn history_regex(text: &str, case_sensitive: bool) -> Result<Regex> {
    RegexBuilder::new(text)
        .case_insensitive(!case_sensitive)
        .size_limit(8 * 1024 * 1024)
        .dfa_size_limit(8 * 1024 * 1024)
        .build()
        .context("invalid bounded history regular expression")
}

fn history_literal_match(
    body: &str,
    terms: &[String],
    case_sensitive: bool,
) -> Option<(usize, usize)> {
    let mut first = None;
    for term in terms {
        let expression = RegexBuilder::new(&regex::escape(term))
            .case_insensitive(!case_sensitive)
            .build()
            .ok()?;
        let found = expression.find(body)?;
        first.get_or_insert((found.start(), found.end()));
    }
    first
}

fn history_match_snippet(body: &str, start: usize, end: usize) -> String {
    let mut left = start.saturating_sub(160).min(body.len());
    let mut right = end.saturating_add(240).min(body.len());
    while left > 0 && !body.is_char_boundary(left) {
        left -= 1;
    }
    while right > left && !body.is_char_boundary(right) {
        right -= 1;
    }
    let prefix = if left > 0 { "… " } else { "" };
    let suffix = if right < body.len() { " …" } else { "" };
    format!("{prefix}{}{suffix}", &body[left..right])
}

fn history_payload_refs<'a>(
    kind: &'a SessionEventKind,
    references: &mut Vec<&'a SessionPayloadRef>,
) {
    match kind {
        SessionEventKind::ToolStarted {
            input_ref: Some(reference),
            ..
        } => references.push(reference),
        SessionEventKind::ToolCompleted {
            output_ref,
            input_ref,
            ..
        } => {
            if let Some(reference) = output_ref {
                references.push(reference);
            }
            if let Some(reference) = input_ref {
                references.push(reference);
            }
        }
        SessionEventKind::SubagentActivity {
            event: Some(event), ..
        } => history_payload_refs(&event.kind, references),
        _ => {}
    }
}

fn workflow_event_id(kind: &SessionEventKind) -> Option<Uuid> {
    match kind {
        SessionEventKind::BluWorkflowStarted { workflow_id, .. }
        | SessionEventKind::RuntimeWorkflowStarted { workflow_id, .. } => Some(*workflow_id),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
