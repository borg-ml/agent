use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use regex::{Regex, RegexBuilder};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::session_action::{SessionAction, SessionActionState, SessionActionTransition};
use crate::{
    CodingProvider, MessageStatus, PermissionMode, PlanItem, ResponseLanguage, SessionEvent,
    SessionEventKind, SessionGoal, SessionPayloadKind, SessionPayloadRef, SessionStatus,
};

pub const INLINE_SESSION_PAYLOAD_BYTES: usize = 64 * 1024;
pub(crate) const SESSION_PAYLOAD_PREVIEW_BYTES: usize = 4 * 1024;
pub(crate) const PROMPT_ADMISSION_SESSION_READY_TIMEOUT: Duration = Duration::from_secs(5);

pub const MAX_HOST_LAUNCH_METADATA_BYTES: usize = 512 * 1024;
// Cap fork replay at 255 local events without duplicating SessionState in every row.
const FORK_PROJECTION_CHECKPOINT_INTERVAL: u64 = 256;
pub const SESSION_PROJECTION_VERSION: i32 = 3;
const SESSION_SCHEMA_VERSION: i64 = 5;
pub(crate) const DEFAULT_HISTORY_LIMIT: usize = 50;
pub(crate) const MAX_HISTORY_LIMIT: usize = 200;
pub(crate) const DEFAULT_HISTORY_SCAN_LIMIT: usize = 10_000;
pub(crate) const MAX_HISTORY_SCAN_LIMIT: usize = 100_000;
pub(crate) const DEFAULT_HISTORY_PAYLOAD_BYTES: usize = 256 * 1024;
pub(crate) const MAX_HISTORY_PAYLOAD_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_HISTORY_QUERY_BYTES: usize = 8 * 1024;
pub const MAX_RUNTIME_CHECKPOINT_BYTES: usize = 512 * 1024;
pub const HARNESS_CHECKPOINT_PREFIX: &str = "__borg_harness__";

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
pub const RUNTIME_MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeManifestStatus {
    Running,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeManifest {
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
pub struct RuntimeCheckpoint {
    pub session_id: Uuid,
    pub key: String,
    pub state: serde_json::Value,
    pub content_hash: String,
    pub revision: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeManifestActivation {
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
    pub fn is_recallable_user_message(&self) -> bool {
        matches!(
            self,
            Self::Message {
                actor: crate::EventActor::User,
                text,
                status: MessageStatus::Complete | MessageStatus::Failed,
                ..
            } if !text.starts_with("Team message from /")
        )
    }

    pub fn is_completed_context_compaction(&self) -> bool {
        matches!(
            self,
            Self::ProviderEvent { kind, payload, .. }
                if kind == "context_compaction"
                    && payload.get("degraded").and_then(serde_json::Value::as_bool)
                        != Some(true)
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
            Self::ProviderEvent { kind, .. }
                if matches!(
                    kind.as_str(),
                    "native_model_message"
                        | "native_prompt_context"
                        | "native_tool_round_completed"
                        | "network_retry"
                        | "usage_limit_retry"
                        | "usage_limit_retry_cancelled"
                        | "usage_limit_retry_released"
                ) =>
            {
                EventPersistence::Durable
            }
            Self::ProviderEvent { kind, .. }
                if matches!(
                    kind.as_str(),
                    "context_compaction"
                        | "context_compaction_failed"
                        | "context_replay_projected"
                        | "context_replay_fallback"
                        | "item/started:contextCompaction"
                        | "item/completed:contextCompaction"
                        | "item/started:context_compaction"
                        | "item/completed:context_compaction"
                ) =>
            {
                EventPersistence::Durable
            }
            // The exact provider input is audit evidence: a reader must be able
            // to see what a turn sent after the fact, not only while it is live.
            Self::ProviderEvent { kind, .. } if kind == crate::PROVIDER_PROMPT_EVENT_KIND => {
                EventPersistence::Durable
            }
            // A native steer is completed only on this marker, so the marker
            // is the evidence for that completion and has to outlive the turn
            // that wrote it. Ephemeral -- the default for a provider event --
            // meant it was never journaled at all: the ordering guarantee
            // rested on a row that did not exist, and a resumed session could
            // not tell a folded steer from one the model never saw.
            Self::ProviderEvent { kind, .. } if kind == crate::session::NATIVE_STEER_APPLIED => {
                EventPersistence::Durable
            }
            Self::ProviderEvent { kind, .. }
                if kind == "action/preparing"
                    || kind == "action/preparing_cancelled"
                    || kind == "action/generation_status" =>
            {
                EventPersistence::Coalesced
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
            // A mirrored child event is only as durable as the child itself
            // records it. Provider heartbeats, reasoning deltas, and streaming
            // assistant text are live state in the child's journal, so the
            // parent must not turn them into durable rows: one busy
            // orchestration session otherwise grows the store by gigabytes.
            // The parent has no live-state key for a child's coalesced event,
            // so it is delivered live and never journaled.
            Self::SubagentActivity {
                event: Some(child_event),
                ..
            } => match child_event.kind.persistence() {
                EventPersistence::Coalesced | EventPersistence::Ephemeral => {
                    EventPersistence::Ephemeral
                }
                // A child's provider audit trail - full model messages, native
                // tool rounds, compaction and retry markers - is durable in the
                // child's own journal and is never rendered or replayed from
                // the parent's rows. Mirroring it durably into the parent is a
                // second copy of the largest payloads in the store: measured on
                // one orchestration session, child `native_model_message`
                // alone was 424 MB of that session's 1,132 MB of subagent rows.
                // The parent still receives them live, and the child's
                // transcript events (tools, messages, approvals, status) stay
                // durable so ordered subagent replay is unchanged.
                EventPersistence::Durable
                    if matches!(child_event.kind, Self::ProviderEvent { .. })
                        || child_event.kind.is_own_session_metadata() =>
                {
                    EventPersistence::Ephemeral
                }
                EventPersistence::Durable => EventPersistence::Durable,
            },
            _ => EventPersistence::Durable,
        }
    }

    /// Metadata a session records about itself rather than about its
    /// transcript: provider admission, the usage counters, the stop gate,
    /// watches, provider linkage and configuration.
    ///
    /// A parent mirrors its children's streams, and these describe the child's
    /// session, not anything the parent renders or reconstructs on replay - the
    /// parent already carries the child's usage and status in the activity
    /// snapshot itself, and the child's own journal is the record for the rest.
    /// Measured across the 25 most recent sessions, mirrored child
    /// `provider_capabilities_updated` alone was 7 MB of 29 MB of subagent
    /// rows. These are still delivered live; only the durable copy is dropped.
    fn is_own_session_metadata(&self) -> bool {
        matches!(
            self,
            Self::SessionStarted
                | Self::SessionTitled { .. }
                | Self::SessionConfigured { .. }
                | Self::ProviderCapabilitiesUpdated { .. }
                | Self::EffectiveCapabilitiesUpdated { .. }
                | Self::ProviderSessionLinked { .. }
                | Self::UsageUpdated { .. }
                | Self::UserStopChanged { .. }
                | Self::WatchesChanged { .. }
        )
    }

    pub fn is_fork_inheritable(&self) -> bool {
        if matches!(self, Self::ProviderEvent { kind, .. }
            if matches!(kind.as_str(), "network_retry" | "usage_limit_retry" | "usage_limit_retry_cancelled" | "usage_limit_retry_released"))
        {
            return false;
        }
        !matches!(
            self,
            Self::ProviderSessionLinked { .. }
                | Self::ProviderCapabilitiesUpdated { .. }
                | Self::EffectiveCapabilitiesUpdated { .. }
                | Self::SessionTitled { .. }
                | Self::TurnStarted { .. }
                | Self::StatusChanged { .. }
                // A fork is a fresh human-initiated branch and never inherits
                // the parent's user-stop gate.
                | Self::UserStopChanged { .. }
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
            Self::ProviderEvent { kind, payload, .. }
                if kind == "context_compaction"
                    && payload.get("degraded").and_then(serde_json::Value::as_bool)
                        == Some(true)
                    && payload.get("status").and_then(serde_json::Value::as_str)
                        == Some("completed") =>
            {
                true
            }
            // Prompt context is replayed where it was sent: dropping it would
            // rewrite the previous turn's request and cost its cached prefix.
            Self::ProviderEvent { kind, .. } => {
                matches!(
                    kind.as_str(),
                    "native_model_message"
                        | "native_prompt_context"
                        | "native_tool_round_completed"
                        | "native_declaration_base"
                        | "native_declaration_delta"
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

    /// Whether this event should advance the session's user-visible activity
    /// clock -- the "last activity" a resume list sorts and displays.
    ///
    /// The journal appends bookkeeping that belongs to the host rather than to
    /// the conversation: a periodic provider-admission probe writes
    /// `ProviderCapabilitiesUpdated` into every live session, and a shutdown or
    /// crash sweep stamps `StatusChanged { stopped }` across every session it
    /// reaps at once. Treating those as activity made a dozen untouched
    /// sessions all claim the same recent timestamp and outrank the one the
    /// user was actually working in, which is exactly the session they came
    /// back to find. Durability is unaffected: every event is still journaled,
    /// and only this display/ordering clock is held back.
    pub fn advances_activity_clock(&self) -> bool {
        match self {
            Self::ProviderCapabilitiesUpdated { .. }
            | Self::EffectiveCapabilitiesUpdated { .. }
            | Self::ContextWindowUpdated { .. }
            | Self::UsageUpdated { .. }
            | Self::SessionTitled { .. } => false,
            // Entering an active state is the user starting work; a terminal
            // mark is the host tidying up, and any real work that preceded it
            // already moved the clock moments earlier.
            Self::StatusChanged { status, .. } => matches!(
                status,
                SessionStatus::Starting
                    | SessionStatus::Running
                    | SessionStatus::WaitingForApproval
            ),
            _ => true,
        }
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
            Self::ProviderEvent { kind, payload, .. }
                if kind == "action/preparing"
                    || kind == "action/preparing_cancelled"
                    || kind == "action/generation_status" =>
            {
                let prefix = if kind == "action/generation_status" {
                    "action_generation_status"
                } else {
                    "action_preparing"
                };
                Some(format!(
                    "{prefix}:{}",
                    payload
                        .get("tool_call_id")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("")
                ))
            }
            Self::ContextWindowUpdated { .. } => Some("context_window".to_string()),
            _ => None,
        }
    }

    pub fn cleared_live_state_keys(&self) -> Vec<String> {
        match self {
            Self::ProviderEvent { kind, payload, .. } if kind == "action/preparing" => {
                let id = payload
                    .get("tool_call_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                let mut keys = vec![
                    format!("action_generation_status:{id}"),
                    "action_preparing".into(),
                ];
                if !id.is_empty() {
                    keys.extend([
                        "action_preparing:".into(),
                        "action_generation_status:".into(),
                    ]);
                }
                keys
            }
            Self::ToolStarted {
                tool_call_id,
                name,
                input,
                ..
            }
            | Self::ToolUpdated {
                tool_call_id,
                name,
                input,
            } if !crate::edit_is_awaiting_diff(name, input) => {
                vec![
                    "reasoning".into(),
                    "action_preparing".into(),
                    "action_preparing:".into(),
                    "action_generation_status:".into(),
                    format!("action_preparing:{tool_call_id}"),
                    format!("action_generation_status:{tool_call_id}"),
                ]
            }
            Self::ToolCompleted { tool_call_id, .. } => {
                vec![
                    "reasoning".into(),
                    "action_preparing".into(),
                    "action_preparing:".into(),
                    "action_generation_status:".into(),
                    format!("action_preparing:{tool_call_id}"),
                    format!("action_generation_status:{tool_call_id}"),
                ]
            }
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
            } => vec!["reasoning".to_string(), "action_preparing".to_string()],
            Self::ReasoningCompleted
            | Self::ApprovalRequested { .. }
            | Self::ProviderInteractionRequested { .. }
            | Self::StatusChanged {
                status: SessionStatus::WaitingForApproval,
                ..
            } => vec!["reasoning".to_string(), "action_preparing".to_string()],
            Self::ContextCleared => vec![
                "reasoning".to_string(),
                "action_preparing".to_string(),
                "context_window".to_string(),
            ],
            _ => Vec::new(),
        }
    }

    /// Terminal boundaries end the current streamed turn. The context-window
    /// snapshot is session metadata and intentionally survives, but streamed
    /// assistant messages and reasoning must not outlive their turn.
    pub fn clears_live_turn_state(&self) -> bool {
        matches!(
            self,
            Self::ContextCleared
                | Self::TurnCompleted { .. }
                | Self::StatusChanged {
                    status: SessionStatus::Ready
                        | SessionStatus::Completed
                        | SessionStatus::Failed
                        | SessionStatus::Stopped,
                    ..
                }
        )
    }

    /// Every deferred payload reference this event owns, in the order a
    /// transfer must move them. Owned because a native provider payload keeps
    /// its reference nested inside the `payload` value rather than in a field
    /// of its own.
    pub fn payload_refs(&self) -> Vec<SessionPayloadRef> {
        let mut refs = match self {
            Self::ToolStarted { input_ref, .. } => input_ref.iter().cloned().collect(),
            Self::ToolCompleted {
                output_ref,
                input_ref,
                ..
            } => output_ref.iter().chain(input_ref.iter()).cloned().collect(),
            _ => Vec::new(),
        };
        if let Some(reference) = self.deferred_provider_payload_ref() {
            refs.push(reference);
        }
        refs
    }

    /// The reference a deferred native provider payload carries inside its own
    /// `payload`.
    pub fn deferred_provider_payload_ref(&self) -> Option<SessionPayloadRef> {
        match self {
            Self::ProviderEvent { payload, .. } => deferred_provider_payload_ref(payload),
            _ => None,
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
    /// None means an older snapshot cannot establish whether every call was priced.
    #[serde(default)]
    pub cost_complete: Option<bool>,
    pub cost_usd: Option<f64>,
    pub context_tokens: Option<u64>,
    pub context_window_tokens: Option<u64>,
}

pub(crate) fn cumulative_cost_basis(
    current_cost: Option<u64>,
    current_basis: &str,
    additional_cost: Option<u64>,
    additional_basis: &str,
) -> String {
    match additional_cost {
        None => current_basis.to_string(),
        Some(_) if current_cost.is_none() => additional_basis.to_string(),
        Some(_) if current_basis == additional_basis => current_basis.to_string(),
        Some(_) => "mixed".to_string(),
    }
}

/// One atomic checkpoint preserves the exact replacement prompt and deadline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingUsageLimitRetry {
    pub(crate) retry_at: Option<DateTime<Utc>>,
    pub(crate) prompt: crate::session::QueuedPrompt,
    pub(crate) replaced_message_ids: Vec<Uuid>,
    pub(crate) continuation: bool,
    #[serde(default)]
    pub(crate) in_progress: bool,
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
    pub imported_from: Option<String>,
    pub imported_title: Option<String>,
    pub title: Option<String>,
    pub title_generated: bool,
    pub title_usage_tokens: Option<u64>,
    pub active_processes: BTreeSet<Uuid>,
    pub provider_session_id: Option<String>,
    /// Instruction contract of the last acknowledged native provider thread.
    /// Missing means the checkpoint predates explicit compatibility tracking.
    #[serde(default)]
    pub provider_context_contract_version: Option<u32>,
    /// Last provider turn committed by a durable `TurnCompleted` boundary.
    pub provider_turn_id: Option<String>,
    /// Codex reports its native checkpoint just before the session actor
    /// commits `TurnCompleted`; keep it pending until that boundary lands.
    pub pending_provider_turn_id: Option<String>,
    pub pending_provider_turn_session_id: Option<String>,
    #[serde(default)]
    pub pending_provider_context_contract_version: Option<u32>,
    pub pending_approval_id: Option<String>,
    pub pending_provider_interaction_id: Option<String>,
    pub pending_provider_interaction_kind: Option<String>,
    pub pending_provider_interaction_payload: Option<serde_json::Value>,
    pub goal: Option<SessionGoal>,
    pub todos: Vec<PlanItem>,
    /// Watches armed by the agent; the last `WatchesChanged` snapshot.
    #[serde(default)]
    pub watches: Vec<crate::WatchSummary>,
    pub usage: SessionUsage,
    pub first_prompt: Option<String>,
    pub latest_prompt: Option<String>,
    pub latest_response: Option<String>,
    /// Explicit user-stop gate. Set true by a human Escape and cleared only by
    /// an explicit human prompt or goal resume (`UserStopChanged`). Persisted
    /// so the session actor re-engages the gate after a reload and never lets
    /// background input override a stop until the human re-engages.
    #[serde(default)]
    pub user_stopped: bool,
    pub usage_limit_retry: Option<PendingUsageLimitRetry>,
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
        if event.kind.advances_activity_clock() {
            self.activity_at = Some(event.created_at);
        }
        match &event.kind {
            SessionEventKind::SessionStarted => self.started_at = Some(event.created_at),
            SessionEventKind::SessionTitled {
                title,
                generated,
                usage_tokens,
            } => {
                if !self.title_generated {
                    self.title = Some(title.clone());
                    self.title_generated = *generated;
                    self.title_usage_tokens = *usage_tokens;
                }
            }
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if kind == "conversation_imported" =>
            {
                self.imported_from = payload
                    .get("source")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string);
                self.imported_title = payload
                    .get("title")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string);
            }
            SessionEventKind::RuntimeProcessStarted { process_id, .. } => {
                self.active_processes.insert(*process_id);
            }
            SessionEventKind::RuntimeProcessCompleted { process_id, .. } => {
                self.active_processes.remove(process_id);
            }
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
                    if let Some(retry) = &mut self.usage_limit_retry {
                        retry.retry_at = None;
                    }
                    self.context_generation = self.context_generation.saturating_add(1);
                    // A model change starts a new Borg context generation, but
                    // an acknowledged Codex thread remains resumable under
                    // the new provider lifecycle key. Other providers do not
                    // have this durable resume path, and provider changes
                    // necessarily invalidate the old session id.
                    if provider_changed || *provider != CodingProvider::Codex {
                        self.provider_session_id = None;
                        self.provider_context_contract_version = None;
                        self.provider_turn_id = None;
                        self.pending_provider_turn_id = None;
                        self.pending_provider_turn_session_id = None;
                        self.pending_provider_context_contract_version = None;
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
                context_contract_version,
            } => {
                if let Some(provider_turn_id) = provider_turn_id {
                    self.pending_provider_turn_id = Some(provider_turn_id.clone());
                    self.pending_provider_turn_session_id = Some(provider_session_id.clone());
                    self.pending_provider_context_contract_version = *context_contract_version;
                } else {
                    self.provider_session_id = Some(provider_session_id.clone());
                    self.provider_context_contract_version = *context_contract_version;
                    self.provider_turn_id = None;
                    self.pending_provider_turn_id = None;
                    self.pending_provider_turn_session_id = None;
                    self.pending_provider_context_contract_version = None;
                }
            }
            SessionEventKind::TurnStarted { message_id, .. } => {
                if let Some(retry) = &mut self.usage_limit_retry {
                    if retry.prompt.message_id == *message_id {
                        retry.in_progress = true;
                        retry.retry_at = None;
                    } else {
                        self.usage_limit_retry = None;
                    }
                }
            }
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if kind == "usage_limit_retry" =>
            {
                // Legacy events recorded only a deadline, not a recoverable prompt.
                self.usage_limit_retry = if payload.get("prompt").is_some() {
                    Some(serde_json::from_value(payload.clone())?)
                } else {
                    None
                };
            }
            SessionEventKind::ProviderEvent { kind, .. }
                if kind == "usage_limit_retry_cancelled" =>
            {
                self.usage_limit_retry = None;
            }
            SessionEventKind::ProviderEvent { kind, .. }
                if kind == "usage_limit_retry_released" =>
            {
                if let Some(retry) = &mut self.usage_limit_retry {
                    retry.retry_at = None;
                }
            }
            SessionEventKind::PromptRecalled { message_id, .. } => {
                if self.usage_limit_retry.as_ref().is_some_and(|retry| {
                    retry.prompt.message_id == *message_id
                        || retry.replaced_message_ids.contains(message_id)
                }) {
                    self.usage_limit_retry = None;
                }
            }
            SessionEventKind::TurnCompleted {
                provider_session_id,
                error,
                ..
            } => {
                if self
                    .usage_limit_retry
                    .as_ref()
                    .is_some_and(|retry| retry.in_progress)
                {
                    self.usage_limit_retry = None;
                }
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
                    self.provider_context_contract_version =
                        self.pending_provider_context_contract_version;
                } else {
                    self.provider_session_id = None;
                    self.provider_context_contract_version = None;
                    self.provider_turn_id = None;
                }
                self.pending_provider_turn_id = None;
                self.pending_provider_turn_session_id = None;
                self.pending_provider_context_contract_version = None;
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
            SessionEventKind::WatchesChanged { watches } => self.watches = watches.clone(),
            SessionEventKind::GoalUpdated { goal } => {
                if !goal.status.is_active() {
                    self.usage_limit_retry = None;
                }
                self.goal = Some(goal.clone());
            }
            SessionEventKind::GoalCleared { .. } => {
                self.usage_limit_retry = None;
                self.goal = None;
            }
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
                let had_usage = self.usage.cost_complete.is_some()
                    || self.usage.total_tokens > 0
                    || self.usage.input_tokens > 0
                    || self.usage.output_tokens > 0
                    || self.usage.cached_input_tokens > 0
                    || self.usage.cache_creation_input_tokens > 0
                    || self.usage.cost_microusd.is_some();
                let usage_bearing = *total_tokens > 0
                    || *input_tokens > 0
                    || *output_tokens > 0
                    || *cached_input_tokens > 0
                    || *cache_creation_input_tokens > 0
                    || cost_microusd.is_some();
                if usage_bearing {
                    self.usage.cost_complete = if cost_microusd.is_none() {
                        Some(false)
                    } else if !had_usage {
                        Some(true)
                    } else {
                        self.usage.cost_complete
                    };
                }
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
                self.usage.cost_basis = cumulative_cost_basis(
                    self.usage.cost_microusd,
                    &self.usage.cost_basis,
                    *cost_microusd,
                    cost_basis,
                );
                self.usage.cost_microusd = match (self.usage.cost_microusd, cost_microusd) {
                    (Some(current), Some(additional)) => Some(current.saturating_add(*additional)),
                    (None, Some(value)) => Some(*value),
                    (current, None) => current,
                };
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
                self.provider_context_contract_version = None;
                self.provider_turn_id = None;
                self.pending_provider_turn_id = None;
                self.pending_provider_turn_session_id = None;
                self.pending_provider_context_contract_version = None;
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
            SessionEventKind::UserStopChanged { engaged } => {
                if *engaged {
                    self.usage_limit_retry = None;
                }
                self.user_stopped = *engaged;
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
        // A fork is a fresh human-initiated branch; it never inherits a
        // parent's user-stop gate.
        state.user_stopped = false;
        state.usage_limit_retry = None;
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

/// One event row exactly as stored, for verbatim copying between backends.
///
/// Distinct from `SessionEvent`, which is the DECODED event. A migration that
/// re-encodes an event necessarily re-applies today's rules to history written
/// under older ones; this carries the stored row instead, including the
/// projection checkpoint and the flags that were computed when it was written.
#[derive(Debug, Clone, PartialEq)]
pub struct RawSessionEvent {
    pub sequence: u64,
    pub event_id: Uuid,
    pub event_kind: String,
    /// The stored body, decoded from whichever tier holds it.
    pub body: serde_json::Value,
    /// Empty for non-checkpoint rows, exactly as stored.
    pub projection_json: String,
    pub fork_inheritable: bool,
    pub recovery_relevant: bool,
    pub message_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
}

/// One session's identity and lineage, for a full resumable scan.
///
/// Distinct from `SessionSummary`, which omits `owner_session_id` and lists
/// only top-level sessions. Migration must see every row -- in the journal this
/// was built against, 548 of 1,144 sessions are subagent-owned, so listing only
/// top-level ones would silently drop nearly half the history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionLineage {
    pub session_id: Uuid,
    pub parent_session_id: Option<Uuid>,
    pub parent_cut_sequence: Option<u64>,
    pub inherited_event_count: u64,
    pub owner_session_id: Option<Uuid>,
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

/// Which slices of a recovery projection a caller actually needs.
///
/// Recovery is the widest read in the store: on a long session it matches
/// every context, queue, and subagent row, and the context slice carries the
/// tool payloads. Resume only needs the queue slice to restore pending prompts
/// and the subagent slice to seed the team roster, so let those callers narrow
/// the scan instead of materialising hundreds of megabytes they drop anyway.
/// Narrowing never changes which events a slice contains, only which slices
/// are populated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryParts {
    pub context: bool,
    pub queue: bool,
    pub subagents: bool,
}

impl RecoveryParts {
    /// The full projection an agent session needs to rebuild provider context.
    pub const ALL: Self = Self {
        context: true,
        queue: true,
        subagents: true,
    };
    /// Pending-prompt recovery only.
    pub const QUEUE: Self = Self {
        context: false,
        queue: true,
        subagents: false,
    };
    /// Team-roster recovery only.
    pub const SUBAGENTS: Self = Self {
        context: false,
        queue: false,
        subagents: true,
    };
}

impl Default for RecoveryParts {
    fn default() -> Self {
        Self::ALL
    }
}

impl SessionRecovery {
    fn from_events(events: Vec<SessionEvent>, parts: RecoveryParts) -> Self {
        let mut recovery = Self::default();
        for event in events {
            if parts.context {
                if matches!(event.kind, SessionEventKind::ContextCleared) {
                    recovery.context_events.clear();
                }
                if event.kind.is_context_relevant() {
                    recovery.context_events.push(event.clone());
                }
            }
            if parts.queue && event.kind.is_queue_relevant() {
                recovery.queue_events.push(event.clone());
            }
            if parts.subagents && event.kind.is_subagent_relevant() {
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
    /// Durable per-workspace relay upload cursors for one host session.
    async fn host_workspace_cursors(
        &self,
        host_id: Uuid,
        session_id: Uuid,
    ) -> Result<HashMap<Uuid, u64>>;
    async fn acknowledge_host_workspaces(
        &self,
        host_id: Uuid,
        session_id: Uuid,
        cursors: &HashMap<Uuid, u64>,
    ) -> Result<()>;
    async fn register_child_session(&self, owner_session_id: Uuid, session_id: Uuid) -> Result<()>;
    async fn append(&self, event: SessionEvent) -> Result<SessionEvent>;
    /// Durably accept a user prompt exactly once before any in-memory routing
    /// or caller acknowledgement. Repeating the same admission is a no-op;
    /// reusing its message id with different content is rejected.
    async fn admit_prompt(&self, event: SessionEvent) -> Result<SessionEvent>;
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
    ) -> Result<Vec<SessionEvent>>;
    /// Return the newest conversation turns **this session authored itself**,
    /// ordered from oldest to newest, in the session's logical sequence space.
    ///
    /// Resume needs the end of the conversation, which is not the end of the
    /// event stream: a long autonomous turn can append hundreds of thousands
    /// of subagent and tool events after the last reply, so any bounded tail
    /// scan keyed on the latest sequence lands in that noise and reaches no
    /// message at all. Implementations answer this from the partial message
    /// index, so the cost tracks `limit` instead of the trailing event volume.
    ///
    /// A fork's inherited prefix is deliberately excluded, because including
    /// it cannot be bounded: an inherited event's logical sequence is its rank
    /// in the composed parent prefix, so placing one would mean counting the
    /// inheritable parent events below it, which is a full index range scan
    /// per row and recursive across grandparents. Excluding it costs nothing
    /// in practice: the inherited prefix occupies logical `1..=inherited`, so
    /// whenever a caller's bounded tail scan reaches down into that range it
    /// is already served by the fork projection, which renumbers and rewrites
    /// inherited events correctly. The only thinner case is a fork that
    /// authored a long local tail but fewer than `limit` local messages, which
    /// returns fewer rows rather than wrong ones, and the remainder still
    /// pages in normally.
    async fn recent_messages(&self, session_id: Uuid, limit: usize) -> Result<Vec<SessionEvent>>;
    async fn state(&self, session_id: Uuid) -> Result<SessionState>;
    /// Cache-routing identity only; a fork must still start its own provider continuation.
    async fn prompt_cache_session_id(&self, session_id: Uuid) -> Result<Uuid>;

    /// Number of leading events this session inherited from a fork parent.
    ///
    /// Reads renumber inherited events into the child's own sequence space, so
    /// this is the only way to tell what the session actually authored.
    async fn inherited_event_count(&self, session_id: Uuid) -> Result<u64>;
    async fn recovery(&self, session_id: Uuid) -> Result<SessionRecovery>;
    /// Recover only the requested slices.
    ///
    /// Resume needs the queue and subagent slices long before (and without)
    /// the context slice that carries every tool payload in the session, so
    /// this lets a caller pay for exactly the rows it will use. The default
    /// falls back to the full projection, which is a superset of every slice.
    async fn recovery_parts(
        &self,
        session_id: Uuid,
        parts: RecoveryParts,
    ) -> Result<SessionRecovery>;
    async fn recovery_from_provider_checkpoint(
        &self,
        session_id: Uuid,
        provider_session_id: &str,
    ) -> Result<Option<SessionRecovery>>;
    /// The team a fork takes over from the session it was cut from.
    ///
    /// A fork does not inherit `SubagentActivity` rows, so without this a
    /// revert (which continues the conversation on a fork) came up with an
    /// empty roster: every worker the parent started was unreachable. Returns
    /// the ancestors' latest activity for children created before the cut,
    /// re-parented onto `session_id`, oldest ancestor first.
    async fn fork_team_events(&self, _session_id: Uuid) -> Result<Vec<SessionEvent>> {
        Ok(Vec::new())
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
    ) -> Result<SessionWorkspaceBinding>;
    async fn workspace_binding(&self, session_id: Uuid) -> Result<Option<SessionWorkspaceBinding>>;
    /// Return the durable autonomous runtime journal on the same authority as
    /// this store, when it has one. Optional so the trait keeps a small
    /// in-memory test seam; the store factory refuses a production backend that
    /// answers `None`, because a missing tier is retried forever rather than
    /// reported.
    async fn autonomy_store(
        &self,
    ) -> Result<Option<std::sync::Arc<dyn crate::autonomy::AutonomyStore>>>;
    /// Return the workspace projection on the same durable authority when the
    /// store supports it. Keeping this optional preserves the trait's small
    /// in-memory test seam without allowing production sessions to silently
    /// create a second workspace database.
    async fn workspace_store(&self) -> Result<Option<std::sync::Arc<dyn crate::WorkspaceStore>>>;

    // The persistent-runtime tier: manifests, checkpoints and harness state.
    //
    // These are REQUIRED rather than defaulted on purpose. A default that
    // returned "unavailable" would let a half-ported backend compile and start,
    // and the failure would surface much later as a session that silently
    // forgets its harness state between turns -- the hardest class of bug this
    // migration can produce. Making them required means the compiler, not
    // production, tells you a backend is incomplete.
    /// Claim this session's runtime manifest for `worker_id`, reporting whether
    /// it was taken over from a worker that died without stopping cleanly.
    async fn activate_runtime_manifest(
        &self,
        session_id: Uuid,
        runtime: &str,
        root: &str,
        command: &str,
        worker_id: Uuid,
    ) -> Result<RuntimeManifestActivation>;
    /// Record one execution against a manifest this worker still owns. Fails
    /// when the manifest has since been claimed by another worker.
    async fn record_runtime_execution(
        &self,
        session_id: Uuid,
        worker_id: Uuid,
        code_hash: &str,
        worker_failed: bool,
        error: Option<&str>,
    ) -> Result<()>;
    /// Mark this worker's manifest stopped; fenced on `worker_id`.
    async fn stop_runtime_manifest(&self, session_id: Uuid, worker_id: Uuid) -> Result<()>;
    /// This session's runtime manifest, if it was ever activated.
    async fn runtime_manifest(&self, session_id: Uuid) -> Result<Option<RuntimeManifest>>;
    /// Save a named checkpoint. Idempotent for identical content, an error for
    /// a differing body under a key that already exists.
    async fn save_runtime_checkpoint(
        &self,
        session_id: Uuid,
        worker_id: Uuid,
        key: &str,
        state: &serde_json::Value,
    ) -> Result<RuntimeCheckpoint>;
    /// One checkpoint by key, or the newest non-harness one when `key` is None.
    async fn runtime_checkpoint(
        &self,
        session_id: Uuid,
        key: Option<&str>,
    ) -> Result<Option<RuntimeCheckpoint>>;
    /// The most recent checkpoints for a session, newest first.
    async fn list_runtime_checkpoints(
        &self,
        session_id: Uuid,
        limit: usize,
    ) -> Result<Vec<RuntimeCheckpoint>>;
    /// The newest harness state for this session.
    async fn load_harness_state(&self, session_id: Uuid) -> Result<Option<serde_json::Value>>;
    /// Append a new harness state revision, pruning all but the last twelve.
    async fn save_harness_state(&self, session_id: Uuid, state: &serde_json::Value) -> Result<()>;
    /// Rewind harness state by `steps` revisions and re-save it as the newest.
    async fn rollback_harness_state(
        &self,
        session_id: Uuid,
        steps: usize,
    ) -> Result<serde_json::Value>;

    // Harness routing. A transcript has exactly one owner -- Borg's harness or
    // the provider's CLI -- and only the owner can replay it, so these pin a
    // route durably rather than recomputing it per turn.
    /// Index documents after `sequence`, for the agent-facing history index.
    async fn history_index_documents_after(
        &self,
        session_id: Uuid,
        sequence: u64,
        limit: usize,
    ) -> Result<Vec<SessionHistoryIndexDocument>>;
    // The workflow tier. A workflow id admits exactly once, its durable action
    // is created already running, and terminal events are fenced on the lease.
    /// Return this workflow's durable action, creating it if it is new.
    async fn ensure_workflow_action(
        &self,
        session_id: Uuid,
        workflow_id: Uuid,
        payload: &serde_json::Value,
    ) -> Result<SessionAction>;
    /// Admit a workflow start exactly once, returning the existing Started
    /// event when this workflow id was already journaled.
    async fn ensure_workflow_started(
        &self,
        event: SessionEvent,
        workflow_id: Uuid,
    ) -> Result<SessionEvent>;
    /// Append a durable event only while the caller still owns `action_id`'s
    /// lease; the check and the append share one transaction.
    async fn append_with_action_lease(
        &self,
        event: SessionEvent,
        action_id: Uuid,
        lease_owner: &str,
        lease_token: Uuid,
    ) -> Result<SessionEvent>;
    /// Does this session exist?
    async fn contains_session(&self, session_id: Uuid) -> Result<bool>;
    /// Create a session already bound to a workspace.
    async fn create_session_in_workspace(
        &self,
        session_id: Uuid,
        workspace_id: Uuid,
    ) -> Result<SessionWorkspaceBinding>;
    /// Drop a session that was created but never used, so an abandoned launch
    /// does not leave a permanent empty row in the session list.
    async fn discard_empty_session(&self, session_id: Uuid) -> Result<bool>;
    /// The durable launch metadata a relay host recorded for this session.
    async fn load_host_launch_metadata(
        &self,
        session_id: Uuid,
    ) -> Result<Option<serde_json::Value>>;
    // The relay host tier: which host owns a launch, how much of a session
    // it has seen, and what it still owes forward. These decide whether a
    // session can be driven at all, so a backend missing them is not a
    // degraded relay -- it is one that silently relays nothing.
    async fn acknowledge_host_journal(
        &self,
        session_id: Uuid,
        event_cursor: u64,
        live_revision: u64,
    ) -> Result<()>;
    async fn begin_host_bootstrap(&self, session_id: Uuid) -> Result<()>;
    async fn claim_legacy_host_launch_owner(
        &self,
        session_id: Uuid,
        host_id: Uuid,
        relay_origin: &str,
    ) -> Result<()>;
    async fn create_session_in_workspace_as(
        &self,
        session_id: Uuid,
        workspace_id: Uuid,
        participant_id: Uuid,
    ) -> Result<SessionWorkspaceBinding>;
    async fn finish_host_bootstrap(&self, session_id: Uuid) -> Result<()>;
    async fn host_launch_owner(&self, session_id: Uuid) -> Result<Option<(Uuid, String)>>;
    async fn pending_host_journals(&self, after: Option<Uuid>, limit: usize) -> Result<Vec<Uuid>>;
    async fn pending_host_launch_metadata(
        &self,
        limit: usize,
    ) -> Result<Vec<(Uuid, serde_json::Value)>>;
    async fn pending_host_launch_metadata_for_host(
        &self,
        offset: usize,
        owner: Option<(Uuid, &str)>,
        limit: usize,
    ) -> Result<Vec<(Uuid, serde_json::Value)>>;
    async fn persist_host_launch_metadata(
        &self,
        session_id: Uuid,
        metadata: &serde_json::Value,
    ) -> Result<()>;
    async fn persist_owned_host_launch_metadata(
        &self,
        session_id: Uuid,
        metadata: &serde_json::Value,
        host_id: Uuid,
        relay_origin: &str,
    ) -> Result<()>;
    async fn settle_terminal_host_session(&self, session_id: Uuid) -> Result<()>;
    async fn pending_host_workspace_messages(
        &self,
        host_id: Uuid,
        after: Option<Uuid>,
        limit: usize,
    ) -> Result<Vec<Uuid>>;
    /// The durable receipt tier on the same authority as the journal.
    ///
    /// Required, like `plugin_backend`: receipts are what make a relayed
    /// mutation replay-safe, so a backend that could not provide them would
    /// let the relay repeat work it had already done.
    async fn receipt_store(&self) -> Result<std::sync::Arc<dyn crate::receipt::ReceiptBackend>>;
    /// Reclaim space and prune derived rows.
    ///
    /// `vacuum` asks for the engine's heavyweight rewrite -- Postgres's
    /// `VACUUM` -- rather than the routine pruning of derived rows.
    async fn compact(&self, vacuum: bool) -> Result<SessionStoreCompaction>;
    /// A cheap readiness probe for startup and `borg doctor`.
    async fn readiness(&self) -> Result<SessionStoreHealth>;
    /// A deeper health check, including an integrity pass.
    async fn health(&self) -> Result<SessionStoreHealth>;
    /// Import a session's events wholesale, for migration and import tooling.
    /// An empty import is a legitimate empty session rather than a failure: a
    /// journal with no events is what an interrupted first run leaves behind,
    /// and the session row is what a later start or resume checks for. Events
    /// that are present but invalid are still refused, so empty and malformed
    /// never collapse into one another.
    async fn import_session_events(
        &self,
        session_id: Uuid,
        events: Vec<SessionEvent>,
    ) -> Result<bool>;
    // Migration primitives. A journal is copied between backends by replaying
    // its events through `append`, which recomputes projections for the
    // destination engine; these two carry the one thing replay cannot, namely
    // the oversized bodies that were moved out of the event into a side table.
    /// Every stored payload for a session, as (event id, reference) pairs.
    async fn session_payload_refs(
        &self,
        session_id: Uuid,
    ) -> Result<Vec<(Uuid, SessionPayloadRef)>>;
    /// Store one payload verbatim, keeping its existing id.
    ///
    /// Idempotent by id, so a resumed migration re-copying a session it had
    /// already reached writes nothing. The id is not regenerated because the
    /// event body already references it: minting a new one would leave the
    /// event pointing at a payload that does not exist.
    async fn import_payload(
        &self,
        session_id: Uuid,
        event_id: Uuid,
        payload: &SessionPayloadRef,
        bytes: &[u8],
    ) -> Result<()>;
    /// A page of sessions ordered by id, for a full resumable scan.
    ///
    /// Ordered by id rather than by activity so the sequence is stable while
    /// the source is still being written: a migration paging by `updated_at`
    /// would revisit sessions touched mid-run and could skip others entirely.
    async fn session_lineage_page(
        &self,
        after: Option<Uuid>,
        limit: usize,
    ) -> Result<Vec<SessionLineage>>;
    /// Append many events to one session in a SINGLE transaction.
    ///
    /// For bulk import only. Ordinary appends are one transaction each because
    /// each one must be durable before the agent acts on it; a migration has no
    /// such reader, and paying a commit per event made copying a 2.5M-event
    /// journal take about fifteen hours at a measured 46 events/second. The
    /// batch holds the session's row lock once instead of re-taking it per
    /// event, and fsyncs once instead of per event.
    ///
    /// All-or-nothing: a failure rolls the whole batch back, so a resumed
    /// migration re-copies the batch rather than finding it half applied.
    async fn append_batch(&self, events: Vec<SessionEvent>) -> Result<Vec<SessionEvent>>;
    // Verbatim copy, for migrating a journal between backends. See
    // `RawSessionEvent` for why replay is not sufficient.
    /// A page of stored event rows after `after_sequence`, in sequence order.
    async fn raw_event_page(
        &self,
        session_id: Uuid,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<RawSessionEvent>>;
    /// Insert stored rows verbatim and advance the session's allocator.
    ///
    /// Bypasses classification entirely: whatever the source journaled is
    /// journaled here, at the same sequence, with the same flags. Idempotent by
    /// (session, sequence), so a resumed migration re-running a batch writes
    /// nothing.
    async fn import_raw_events(
        &self,
        session_id: Uuid,
        events: Vec<RawSessionEvent>,
    ) -> Result<u64>;
    /// Set a migrated session's projected state to the source's.
    ///
    /// The row-level copy carries projection CHECKPOINTS but not the session's
    /// current state, which is a running fold the source already computed.
    /// Recomputing it here would mean replaying every event through today's
    /// rules -- the exact thing verbatim copying exists to avoid.
    ///
    /// `inherited_event_count` is written from the SOURCE rather than left as
    /// the destination computed it. `fork_before` derives it by counting the
    /// parent's inheritable events, and a real journal can carry a value that
    /// no longer matches its own data -- one fork measured here records 15,245
    /// where recounting gives 15,169. The fork's own events start immediately
    /// after the RECORDED value, so recomputing it opens a 76-sequence hole in
    /// the composed history.
    async fn finish_imported_session(
        &self,
        session_id: Uuid,
        state: &SessionState,
        inherited_event_count: u64,
    ) -> Result<()>;
    /// Search EVERY session's history at once.
    ///
    /// The cross-session half of the search tier: an agent asking "have I seen
    /// this error before?" is asking about its whole history, not one thread.
    /// Only the lexical mode is served globally -- a regex would have to scan
    /// every body in the journal, which is a different and much more expensive
    /// promise than this makes.
    async fn search_all_sessions(&self, query: SessionHistoryQuery) -> Result<SessionHistoryPage>;
    /// The extension state tier on the same durable authority as the journal.
    ///
    /// Required, not optional: a store that answered `None` would send plugin
    /// state to a second database, and the whole point of this tier living in
    /// the journal's schema is that it does not.
    fn plugin_backend(&self) -> std::sync::Arc<dyn crate::plugin_store::PluginBackend>;
    /// Search this session's history, composing a fork's inherited range with
    /// its own so callers see one renumbered timeline.
    async fn query_history(
        &self,
        session_id: Uuid,
        query: SessionHistoryQuery,
    ) -> Result<SessionHistoryPage>;
    /// Ensure this session uses Borg's Codex harness.
    async fn uses_native_codex_harness(&self, session_id: Uuid) -> Result<bool>;
    /// Resolve, and durably pin, this session's OpenCode route. The route is
    /// model-aware, so the launch model is passed for a fresh session.
    async fn uses_native_opencode_harness(
        &self,
        session_id: Uuid,
        model: Option<&str>,
    ) -> Result<bool>;
    /// Record which account last drove this session for `provider`.
    #[cfg(any(feature = "subscription-adapters", test))]
    async fn record_model_access(
        &self,
        session_id: Uuid,
        provider: CodingProvider,
        account_identity: &str,
    ) -> Result<()>;
}

/// Outcome of a store's `compact`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionStoreCompaction {
    /// Mirrored subagent rows removed because the live journal would no
    /// longer persist them.
    pub deleted_events: u64,
    /// Search projection rows that no longer had a journal event.
    pub deleted_search_rows: u64,
    pub vacuumed: bool,
    /// Database size in bytes before and after, from the page count. Without
    /// a vacuum the file keeps its size and the freed pages are reused.
    pub bytes_before: i64,
    pub bytes_after: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionStoreHealth {
    /// The result of the integrity check, or why it was not run.
    pub integrity: String,
    #[serde(default)]
    pub integrity_checked: bool,
    /// Whether a commit is durable before the caller is told it succeeded.
    ///
    /// Reported separately from readiness: `synchronous_commit = off` is
    /// Borg's managed-cluster default. It preserves atomic recovery, but a
    /// machine crash can lose recently acknowledged, unflushed commits.
    #[serde(default)]
    pub durable_commits: bool,
    /// The server setting `durable_commits` was derived from, verbatim.
    ///
    /// Reported rather than reduced to a boolean so an operator can inspect
    /// the configured trade-off without calling a healthy async store degraded.
    #[serde(default)]
    pub commit_durability: String,
    pub sessions: i64,
    pub events: i64,
    pub actions: i64,
    pub payloads: i64,
    pub projection_version: i32,
}

impl SessionStoreHealth {
    /// Ready means the journal answered and nothing it checked came back wrong.
    /// The commit policy is reported separately; async commit is intentional.
    pub fn is_ready(&self) -> bool {
        !self.integrity_checked || self.integrity == "ok"
    }
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

fn same_prompt_payload_ignoring_delivery(
    left: &serde_json::Value,
    right: &serde_json::Value,
) -> bool {
    left.get("message_id") == right.get("message_id")
        && left.get("text") == right.get("text")
        && left.get("attachments") == right.get("attachments")
}

pub(crate) fn ensure_prompt_admission(event: &SessionEvent) -> Result<()> {
    ensure!(
        matches!(
            event.kind,
            SessionEventKind::Message {
                actor: crate::EventActor::User,
                status: MessageStatus::Queued,
                ..
            }
        ),
        "prompt admission must be a queued user message"
    );
    ensure!(
        event.sequence == 0,
        "prompt admission sequence must be assigned by the store"
    );
    Ok(())
}

pub(crate) fn prompt_message_id(event: &SessionEvent) -> Result<Uuid> {
    match event.kind {
        SessionEventKind::Message { message_id, .. } => Ok(message_id),
        _ => bail!("prompt admission is not a message"),
    }
}

pub(crate) fn ensure_same_prompt_admission(
    existing: &SessionEvent,
    requested: &SessionEvent,
) -> Result<()> {
    let (
        SessionEventKind::Message {
            message_id: existing_id,
            actor: crate::EventActor::User,
            text: existing_text,
            attachments: existing_attachments,
            delivery: existing_delivery,
            ..
        },
        SessionEventKind::Message {
            message_id: requested_id,
            actor: crate::EventActor::User,
            text: requested_text,
            attachments: requested_attachments,
            delivery: requested_delivery,
            ..
        },
    ) = (&existing.kind, &requested.kind)
    else {
        bail!("prompt message id was reused by a non-user event")
    };
    ensure!(
        existing_id == requested_id
            && existing_text == requested_text
            && existing_attachments == requested_attachments
            && existing_delivery == requested_delivery,
        "prompt message id {requested_id} was reused with different immutable content"
    );
    Ok(())
}

pub fn deferred_json_payload(payload: &SessionPayloadRef) -> serde_json::Value {
    serde_json::json!({
        "borg_payload_deferred": true,
        "payload_id": payload.id,
        "byte_len": payload.byte_len,
    })
}

pub fn deferred_text_payload(value: &str, payload: &SessionPayloadRef) -> String {
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

/// Keep a command's change list visible in a deferred tool result. The full
/// output, including each diff, remains in the payload and loads on inspection.
pub fn deferred_tool_output_payload(value: &str, payload: &SessionPayloadRef) -> String {
    if !value.contains("\"changes\"") {
        return deferred_text_payload(value, payload);
    }
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(value) else {
        return deferred_text_payload(value, payload);
    };
    let nested = parsed
        .get("content")
        .and_then(serde_json::Value::as_array)
        .and_then(|content| {
            content
                .iter()
                .filter_map(|item| item.get("text").and_then(serde_json::Value::as_str))
                .find_map(|text| serde_json::from_str::<serde_json::Value>(text).ok())
        });
    let Some(changes) = parsed
        .get("changes")
        .or_else(|| parsed.pointer("/structuredContent/changes"))
        .or_else(|| nested.as_ref().and_then(|inner| inner.get("changes")))
        .and_then(serde_json::Value::as_array)
        .filter(|changes| !changes.is_empty())
    else {
        return deferred_text_payload(value, payload);
    };

    let mut visible = Vec::new();
    let mut added = 0_u64;
    let mut removed = 0_u64;
    for change in changes {
        added = added.saturating_add(
            change
                .get("added")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0),
        );
        removed = removed.saturating_add(
            change
                .get("removed")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0),
        );
        if visible.len() < 64
            && let Some(path) = change.get("path").and_then(serde_json::Value::as_str)
        {
            let mut chars = path.chars();
            let mut path = chars.by_ref().take(128).collect::<String>();
            if chars.next().is_some() {
                path.push('…');
            }
            visible.push(serde_json::json!({
                "path": path,
                "added": change.get("added").and_then(serde_json::Value::as_u64).unwrap_or(0),
                "removed": change.get("removed").and_then(serde_json::Value::as_u64).unwrap_or(0),
            }));
        }
    }
    if visible.is_empty() {
        return deferred_text_payload(value, payload);
    }
    let mut preview = deferred_json_payload(payload);
    let fields = preview
        .as_object_mut()
        .expect("deferred payload marker is an object");
    fields.insert("changes".to_string(), serde_json::Value::Array(visible));
    fields.insert(
        "changes_deferred".to_string(),
        serde_json::Value::Bool(true),
    );
    fields.insert(
        "changes_count".to_string(),
        serde_json::json!(changes.len()),
    );
    fields.insert("changes_added".to_string(), serde_json::json!(added));
    fields.insert("changes_removed".to_string(), serde_json::json!(removed));
    preview.to_string()
}

/// The inline marker left where a native provider payload used to be. The
/// reference rides inside the marker because a `ProviderEvent` has no
/// reference field of its own, so this is the only place replay can find it.
pub fn deferred_provider_payload(reference: &SessionPayloadRef) -> serde_json::Value {
    let mut marker = deferred_json_payload(reference);
    if let serde_json::Value::Object(fields) = &mut marker {
        fields.insert(
            crate::PROVIDER_PAYLOAD_REF_FIELD.to_string(),
            serde_json::to_value(reference).expect("a payload reference serializes"),
        );
    }
    marker
}

pub fn deferred_provider_payload_ref(payload: &serde_json::Value) -> Option<SessionPayloadRef> {
    payload
        .get(crate::PROVIDER_PAYLOAD_REF_FIELD)
        .and_then(|reference| serde_json::from_value(reference.clone()).ok())
}

/// The bytes to defer for a native provider payload, or `None` when it is
/// already deferred or still fits inline.
pub fn oversized_provider_payload_bytes(payload: &serde_json::Value) -> Result<Option<Vec<u8>>> {
    if deferred_provider_payload_ref(payload).is_some() {
        return Ok(None);
    }
    let bytes = serde_json::to_vec(payload)?;
    Ok((bytes.len() > INLINE_SESSION_PAYLOAD_BYTES).then_some(bytes))
}

/// Restore a deferred native provider payload from its stored bytes. A payload
/// that was never deferred is left exactly as it was.
pub fn resolve_provider_payload(payload: &mut serde_json::Value, bytes: &[u8]) -> Result<()> {
    if deferred_provider_payload_ref(payload).is_none() {
        return Ok(());
    }
    *payload = serde_json::from_slice(bytes)?;
    Ok(())
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

fn history_regex(text: &str, case_sensitive: bool) -> Result<Regex> {
    RegexBuilder::new(text)
        .case_insensitive(!case_sensitive)
        .size_limit(8 * 1024 * 1024)
        .dfa_size_limit(8 * 1024 * 1024)
        .build()
        .context("invalid bounded history regular expression")
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

fn history_payload_refs(kind: &SessionEventKind, references: &mut Vec<SessionPayloadRef>) {
    match kind {
        SessionEventKind::ToolStarted {
            input_ref: Some(reference),
            ..
        } => references.push(reference.clone()),
        SessionEventKind::ToolCompleted {
            output_ref,
            input_ref,
            ..
        } => {
            if let Some(reference) = output_ref {
                references.push(reference.clone());
            }
            if let Some(reference) = input_ref {
                references.push(reference.clone());
            }
        }
        SessionEventKind::ProviderEvent { payload, .. } => {
            // A deferred provider prompt carries its reference beside the
            // preview inside the event payload; a deferred native model message
            // carries one the same way.
            if let Some(reference) = payload.get(crate::PROVIDER_PROMPT_REF_FIELD)
                && let Ok(reference) =
                    serde_json::from_value::<SessionPayloadRef>(reference.clone())
            {
                references.push(reference);
            }
            if let Some(reference) = deferred_provider_payload_ref(payload) {
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

pub mod cluster;
pub mod factory;
pub mod postgres;

#[cfg(test)]
mod conformance;

#[cfg(test)]
mod usage_tests {
    use super::*;

    #[test]
    fn async_commit_is_ready_but_a_failed_integrity_check_is_not() {
        // Borg's managed-cluster default must not make `borg doctor` fail.
        let mut health = SessionStoreHealth {
            integrity: "not_checked".into(),
            integrity_checked: false,
            durable_commits: false,
            commit_durability: "off".into(),
            sessions: 0,
            events: 0,
            actions: 0,
            payloads: 0,
            projection_version: SESSION_PROJECTION_VERSION,
        };
        assert!(health.is_ready());
        health.integrity_checked = true;
        health.integrity = "failed".into();
        assert!(!health.is_ready());
    }

    #[test]
    fn session_cost_basis_tracks_the_contributions_to_its_total() {
        let session_id = Uuid::new_v4();
        let mut state = SessionState::default();
        for (sequence, cost, basis) in [
            (1, Some(100), "subscription_equivalent"),
            (2, None, "unavailable"),
            (3, Some(50), "provider_reported"),
        ] {
            state
                .apply(&SessionEvent::new(
                    session_id,
                    sequence,
                    SessionEventKind::UsageUpdated {
                        provider_duration_ms: 0,
                        turn_id: None,
                        provider_context_reused: None,
                        input_tokens: 1,
                        output_tokens: 1,
                        cached_input_tokens: 0,
                        cache_creation_input_tokens: 0,
                        total_tokens: 2,
                        cost_microusd: cost,
                        cost_basis: basis.to_string(),
                        cost_usd: None,
                        context_tokens: None,
                        context_window_tokens: None,
                    },
                ))
                .unwrap();
            assert_eq!(
                state.usage.cost_microusd,
                Some(if sequence < 3 { 100 } else { 150 })
            );
            assert_eq!(
                state.usage.cost_basis,
                if sequence < 3 {
                    "subscription_equivalent"
                } else {
                    "mixed"
                }
            );
            assert_eq!(state.usage.cost_complete, Some(sequence == 1));
        }

        let mut legacy = serde_json::to_value(&state.usage).unwrap();
        legacy.as_object_mut().unwrap().remove("cost_complete");
        let restored: SessionUsage = serde_json::from_value(legacy).unwrap();
        assert_eq!(restored.cost_complete, None);

        let mut cache_only = SessionState::default();
        for (sequence, cached_input_tokens, cost_microusd) in [(1, 100, None), (2, 0, Some(50))] {
            cache_only
                .apply(&SessionEvent::new(
                    session_id,
                    sequence,
                    SessionEventKind::UsageUpdated {
                        provider_duration_ms: 0,
                        turn_id: None,
                        provider_context_reused: None,
                        input_tokens: 0,
                        output_tokens: 0,
                        cached_input_tokens,
                        cache_creation_input_tokens: 0,
                        total_tokens: 0,
                        cost_microusd,
                        cost_basis: "provider_reported".to_string(),
                        cost_usd: None,
                        context_tokens: None,
                        context_window_tokens: None,
                    },
                ))
                .unwrap();
        }
        assert_eq!(cache_only.usage.cost_complete, Some(false));
    }
}
