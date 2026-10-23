//! Actual terminal transcript reducer, independent of rendering.
use crate::cache_diagnostics::*;
use crate::entry::*;
use crate::policy::*;
use crate::projection_helpers::*;
use crate::semantics::*;
use borg_agent_runtime::*;
use chrono::{DateTime, Utc};
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use uuid::Uuid;
const PARAGRAPH_DELIVERY_INTERVAL: Duration = Duration::from_millis(400);
fn local_event_time(event: &SessionEvent) -> String {
    event.created_at.to_rfc3339()
}
#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum RenderInvalidation {
    Changed(usize),
    Inserted(usize),
    Removed(usize),
    Cleared,
}

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct TranscriptEntries {
    pub entries: Vec<TranscriptEntry>,
    pub identities: Vec<(Uuid, usize)>,
    pub event_identity: Option<(Uuid, usize)>,
    pub changed_from: Cell<usize>,
    #[serde(skip)]
    pub dirty: std::cell::RefCell<HashSet<usize>>,
    #[serde(skip)]
    pub removed: Vec<(Uuid, usize)>,
    #[serde(skip)]
    pub invalidations: std::cell::RefCell<Vec<RenderInvalidation>>,
}

impl TranscriptEntries {
    pub fn mark_changed(&self, index: usize) {
        self.changed_from.set(self.changed_from.get().min(index));
        self.dirty.borrow_mut().insert(index);
    }

    /// The lowest index changed since the previous call, or `usize::MAX`.
    pub fn take_changed_from(&self) -> usize {
        self.changed_from.replace(usize::MAX)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut TranscriptEntry> {
        self.mark_changed(index);
        self.invalidations
            .get_mut()
            .push(RenderInvalidation::Changed(index));
        self.entries.get_mut(index)
    }

    /// The tool-style call at `index`, if that row is one.
    pub fn call(&self, index: usize) -> Option<&Operation> {
        self.entries.get(index)?.call()
    }

    pub fn call_mut(&mut self, index: usize) -> Option<&mut Operation> {
        self.entries.get(index)?.call()?;
        self.get_mut(index)?.call_mut()
    }

    /// The lifecycle row at `index`, if that row is one.
    pub fn lifecycle(&self, index: usize) -> Option<&Operation> {
        self.entries.get(index)?.lifecycle()
    }

    pub fn lifecycle_mut(&mut self, index: usize) -> Option<&mut Operation> {
        self.entries.get(index)?.lifecycle()?;
        self.get_mut(index)?.lifecycle_mut()
    }

    pub fn last_mut(&mut self) -> Option<&mut TranscriptEntry> {
        self.mark_changed(self.entries.len().saturating_sub(1));
        self.invalidations
            .get_mut()
            .push(RenderInvalidation::Changed(
                self.entries.len().saturating_sub(1),
            ));
        self.entries.last_mut()
    }

    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, TranscriptEntry> {
        self.mark_changed(0);
        self.dirty.borrow_mut().extend(0..self.entries.len());
        self.entries.iter_mut()
    }

    pub fn next_identity(&mut self) -> (Uuid, usize) {
        if let Some((event_id, ordinal)) = self.event_identity.as_mut() {
            let identity = (*event_id, *ordinal);
            *ordinal += 1;
            identity
        } else {
            (Uuid::new_v4(), 0)
        }
    }

    pub fn push(&mut self, entry: TranscriptEntry) {
        self.mark_changed(self.entries.len());
        let identity = self.next_identity();
        self.entries.push(entry);
        self.identities.push(identity);
    }

    pub fn pop(&mut self) -> Option<TranscriptEntry> {
        self.mark_changed(self.entries.len().saturating_sub(1));
        if let Some(identity) = self.identities.pop() {
            self.removed.push(identity);
        }
        self.entries.pop()
    }

    pub fn insert(&mut self, index: usize, entry: TranscriptEntry) {
        self.invalidations
            .get_mut()
            .push(RenderInvalidation::Inserted(index));
        self.mark_changed(index);
        let identity = self.next_identity();
        self.entries.insert(index, entry);
        self.identities.insert(index, identity);
    }

    pub fn remove(&mut self, index: usize) -> TranscriptEntry {
        self.invalidations
            .get_mut()
            .push(RenderInvalidation::Removed(index));
        self.mark_changed(index);
        self.removed.push(self.identities.remove(index));
        self.entries.remove(index)
    }

    pub fn clear(&mut self) {
        self.invalidations
            .get_mut()
            .push(RenderInvalidation::Cleared);
        self.mark_changed(0);
        self.entries.clear();
        self.removed.append(&mut self.identities);
    }

    pub fn reserve(&mut self, additional: usize) {
        self.entries.reserve(additional);
        self.identities.reserve(additional);
    }
}

impl std::ops::Deref for TranscriptEntries {
    type Target = Vec<TranscriptEntry>;

    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

impl<I: std::slice::SliceIndex<[TranscriptEntry]>> std::ops::Index<I> for TranscriptEntries {
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output {
        &self.entries[index]
    }
}

impl std::ops::IndexMut<usize> for TranscriptEntries {
    fn index_mut(&mut self, index: usize) -> &mut TranscriptEntry {
        self.mark_changed(index);
        self.invalidations
            .get_mut()
            .push(RenderInvalidation::Changed(index));
        &mut self.entries[index]
    }
}

impl Extend<TranscriptEntry> for TranscriptEntries {
    fn extend<T: IntoIterator<Item = TranscriptEntry>>(&mut self, entries: T) {
        for entry in entries {
            self.push(entry);
        }
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ModelRequestDisplay {
    pub id: String,
    pub started_at: DateTime<Utc>,
    pub summary_requested: bool,
    pub last_received_at: Option<DateTime<Utc>>,
    pub output_tokens: Option<u64>,
}

/// Finished processes stay only so a later poll or result can still name the
/// command it belongs to. The checkpoint is a fixed-size budget, so keep the
/// most recent few and a bounded command preview, not every process ever run.
const FINISHED_PROCESS_LIMIT: usize = 64;
const FINISHED_PROCESS_COMMAND_CHARS: usize = 1_000;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RuntimeProcessProjection {
    pub command: String,
    pub pid: u32,
    pub tool_index: Option<usize>,
    pub running: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ProviderBackgroundProjection {
    pub command: String,
    pub tool_index: usize,
}

#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionDisplayConfig {
    pub cwd: PathBuf,
    pub provider: CodingProvider,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub response_language: ResponseLanguage,
    pub fast: bool,
    pub ultrafast: bool,
    pub permission_mode: PermissionMode,
    pub speed_support: borg_agent_runtime::SpeedSupport,
}

impl SessionDisplayConfig {
    pub fn cache_signature(&self) -> CacheSignature {
        CacheSignature::for_session(self.provider, self.model.as_deref(), self.effort.as_deref())
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ActiveTurnDisplayConfig {
    pub message_id: Uuid,
    pub provider: CodingProvider,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub fast: bool,
    pub ultrafast: bool,
}

impl ActiveTurnDisplayConfig {
    pub fn cache_signature(&self) -> CacheSignature {
        CacheSignature::for_session(self.provider, self.model.as_deref(), self.effort.as_deref())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DirectorPrompt {
    /// A root transcript, a truncated child window, or a child whose
    /// assignment has been cleared away: nothing visible here is the
    /// assignment.
    Unknown,
    /// A child window that starts at the session's first event, so the next
    /// user row it materializes is the assignment.
    Expected,
    /// The message that carries the assignment.
    Row(Uuid),
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ChildMetaReference {
    pub cursor: crate::wire::Cursor,
    pub sources: Vec<String>,
    #[serde(default)]
    pub source_sequence: Option<u64>,
    #[serde(default)]
    pub snapshot: Option<SubagentSnapshot>,
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct TranscriptCore {
    pub meta: crate::wire::Meta,
    pub child_meta_refs: HashMap<Uuid, ChildMetaReference>,
    pub usage_by_turn: HashMap<Uuid, crate::wire::Usage>,
    pub usage_turn_order: std::collections::VecDeque<Uuid>,
    pub team_report_sources: HashMap<Uuid, (Uuid, Uuid, String)>,
    #[serde(default)]
    pub team_report_roles: HashMap<Uuid, AgentMessageSenderRole>,
    #[serde(default)]
    pub message_root_session_id: Option<Uuid>,
    #[serde(default)]
    pub local_agent_ids: HashSet<Uuid>,
    pub order: TranscriptEntries,
    pub messages: HashMap<Uuid, usize>,
    pub child_transcript: bool,
    pub director_prompt: DirectorPrompt,
    pub tools: HashMap<String, usize>,
    #[serde(default)]
    pub reasoning_blocks: HashMap<Uuid, usize>,
    #[serde(skip)]
    pub retain_tool_inspection: bool,
    #[serde(skip)]
    #[doc(hidden)]
    pub tool_inspection_outputs: HashMap<String, String>,
    pub tool_parents: HashMap<String, String>,
    pub command_edit_rows: HashMap<String, usize>,
    pub foreground_tool: Option<String>,
    pub preparing_tools: HashMap<String, String>,
    pub unkeyed_preparing_tools: Vec<String>,
    pub goal: Option<SessionGoal>,
    pub todos: Vec<PlanItem>,
    pub plan_workspace_revision: Option<u64>,
    pub plan_participant_id: Option<Uuid>,
    pub agent_plans: std::collections::BTreeMap<Uuid, borg_agent_runtime::AgentPlanProjection>,
    pub config: Option<SessionDisplayConfig>,
    pub config_updated_at: Option<DateTime<Utc>>,
    pub config_event_updated_at: Option<DateTime<Utc>>,
    pub provider_capabilities: Vec<borg_agent_runtime::ProviderCapability>,
    pub active_turn: Option<ActiveTurnDisplayConfig>,
    pub model_request: Option<ModelRequestDisplay>,
    pub live_turn_closed: bool,
    pub waiting_on_watchers: bool,
    pub subagents: HashMap<Uuid, SubagentStatus>,
    pub subagent_snapshots: HashMap<Uuid, SubagentSnapshot>,
    /// Settled children dropped from `subagent_snapshots` to bound memory.
    /// Remembered by id so the team's inactive count stays the real total.
    #[serde(default)]
    pub pruned_settled_children: HashSet<Uuid>,
    pub agent_names: HashMap<Uuid, String>,
    pub watches: Vec<WatchSummary>,
    pub watch_messages: HashSet<Uuid>,
    pub watch_message_order: std::collections::VecDeque<Uuid>,
    pub subagent_entries: HashMap<Uuid, usize>,
    pub agent_messages: HashSet<Uuid>,
    pub agent_message_order: std::collections::VecDeque<Uuid>,
    pub team_broadcast_entries: HashMap<Uuid, usize>,
    #[serde(default)]
    pub interaction_entries: HashMap<String, usize>,
    #[serde(default)]
    pub compaction_entries: HashMap<Uuid, usize>,
    pub agent_message_senders: HashSet<Uuid>,
    pub runtime_processes: HashMap<Uuid, RuntimeProcessProjection>,
    #[serde(default)]
    pub finished_process_order: std::collections::VecDeque<Uuid>,
    pub poll_processes: HashMap<Uuid, Vec<String>>,
    pub provider_backgrounds: HashMap<String, ProviderBackgroundProjection>,
    pub provider_followups: HashMap<String, String>,
    pub queued_messages: HashSet<Uuid>,
    pub queued_message_sequences: HashMap<Uuid, u64>,
    pub follow_tail: bool,
    pub selected: Option<usize>,
    pub diff_expansion: DiffExpansionPolicy,
    pub auto_expand_tools: bool,
    pub auto_expand_thinking: bool,
    pub action_descriptors: bool,
    pub response_streaming: crate::policy::ResponseStreaming,
    pub streaming_tails: HashMap<Uuid, String>,
    #[serde(skip)]
    pub streaming_last_release: HashMap<Uuid, Instant>,
    pub show_subagent_messages: bool,
    pub context_remaining_percent: u8,
    pub context_known: bool,
    pub context_tokens: Option<u64>,
    pub context_window_tokens: Option<u64>,
    pub session_usage: borg_agent_runtime::SubagentUsage,
    pub cache_diagnostics: CacheDiagnostics,
    pub tool_run_offsets: HashMap<usize, usize>,
    pub expanded_tool_runs: HashSet<usize>,
    pub action_run_expansions: HashMap<usize, bool>,
    pub active_reasoning: Option<usize>,
    pub reasoning_has_preview: bool,
    pub last_edit: Option<usize>,
    pub pending_entry_insertions: Vec<usize>,
    pub next_image_number: usize,
}

impl Default for TranscriptCore {
    fn default() -> Self {
        Self {
            meta: crate::wire::Meta::default(),
            child_meta_refs: HashMap::new(),
            usage_by_turn: HashMap::new(),
            usage_turn_order: Default::default(),
            team_report_sources: HashMap::new(),
            team_report_roles: HashMap::new(),
            message_root_session_id: None,
            local_agent_ids: HashSet::new(),
            order: TranscriptEntries::default(),
            messages: HashMap::new(),
            child_transcript: false,
            director_prompt: DirectorPrompt::Unknown,
            tools: HashMap::new(),
            reasoning_blocks: HashMap::new(),
            retain_tool_inspection: false,
            tool_inspection_outputs: HashMap::new(),
            tool_parents: HashMap::new(),
            command_edit_rows: HashMap::new(),
            foreground_tool: None,
            preparing_tools: HashMap::new(),
            unkeyed_preparing_tools: Vec::new(),
            goal: None,
            todos: Vec::new(),
            plan_workspace_revision: None,
            plan_participant_id: None,
            agent_plans: Default::default(),
            config: None,
            config_updated_at: None,
            config_event_updated_at: None,
            active_turn: None,
            model_request: None,
            live_turn_closed: false,
            waiting_on_watchers: false,
            subagents: HashMap::new(),
            subagent_snapshots: HashMap::new(),
            pruned_settled_children: HashSet::new(),
            agent_names: HashMap::new(),
            watches: Vec::new(),
            watch_messages: HashSet::new(),
            watch_message_order: Default::default(),
            subagent_entries: HashMap::new(),
            agent_messages: HashSet::new(),
            agent_message_order: Default::default(),
            team_broadcast_entries: HashMap::new(),
            interaction_entries: HashMap::new(),
            compaction_entries: HashMap::new(),
            agent_message_senders: HashSet::new(),
            runtime_processes: HashMap::new(),
            finished_process_order: Default::default(),
            poll_processes: HashMap::new(),
            provider_backgrounds: HashMap::new(),
            provider_followups: HashMap::new(),
            queued_messages: HashSet::new(),
            queued_message_sequences: HashMap::new(),
            provider_capabilities: Vec::new(),
            follow_tail: true,
            selected: None,
            diff_expansion: DiffExpansionPolicy::Expanded,
            auto_expand_tools: false,
            auto_expand_thinking: false,
            action_descriptors: true,
            response_streaming: crate::policy::ResponseStreaming::Token,
            streaming_tails: HashMap::new(),
            streaming_last_release: HashMap::new(),
            show_subagent_messages: false,
            context_remaining_percent: 100,
            context_known: false,
            context_tokens: None,
            context_window_tokens: None,
            session_usage: borg_agent_runtime::SubagentUsage::default(),
            cache_diagnostics: CacheDiagnostics::default(),
            tool_run_offsets: HashMap::new(),
            expanded_tool_runs: HashSet::new(),
            action_run_expansions: HashMap::new(),
            active_reasoning: None,
            reasoning_has_preview: false,
            last_edit: None,
            pending_entry_insertions: Vec::new(),
            next_image_number: 1,
        }
    }
}

pub fn ready_detail_is_waiting_on_watchers(detail: &str) -> bool {
    detail.starts_with("Waiting on ")
}

pub fn display_message_effort(
    effort: Option<String>,
    fast: bool,
    ultrafast: bool,
) -> Option<String> {
    if ultrafast || fast {
        let tier = if ultrafast { "ultrafast" } else { "fast" };
        Some(effort.map_or_else(|| tier.to_string(), |effort| format!("{effort} {tier}")))
    } else {
        effort
    }
}

pub fn reasoning_preview(source: &str) -> String {
    const MAX_CHARS: usize = 160;
    const WINDOW_BYTES: usize = 2_000;
    // Words a sentence still arriving needs before it replaces the one before.
    const MIN_LIVE_WORDS: usize = 6;
    let mut window_start = source.len().saturating_sub(WINDOW_BYTES);
    while !source.is_char_boundary(window_start) {
        window_start += 1;
    }
    let window = &source[window_start..];
    let paragraph = window
        .trim_end()
        .rsplit("\n\n")
        .map(str::trim)
        .find(|paragraph| !paragraph.is_empty())
        .unwrap_or_default();
    let title = |line: &str| {
        line.trim()
            .strip_prefix("**")
            .and_then(|line| line.strip_suffix("**"))
            .map(str::trim)
            .filter(|title| !title.is_empty() && !title.contains("**"))
            .map(str::to_string)
    };
    // A title names the whole section under it, not just its own paragraph.
    if let Some(title) = window.lines().rev().find_map(title) {
        return title.chars().take(MAX_CHARS).collect();
    }
    let text = paragraph.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.is_empty() {
        return String::new();
    }
    let mut sentences = Vec::new();
    let mut start = 0;
    for (index, character) in text.char_indices() {
        let end = index + character.len_utf8();
        if matches!(character, '.' | '?' | '!') && text[end..].starts_with(' ') {
            sentences.push(&text[start..end]);
            start = end + 1;
        }
    }
    if start < text.len() {
        sentences.push(&text[start..]);
    }
    // The window may begin inside a sentence; that one has no readable start.
    let clipped = window_start > 0 && paragraph.len() == window.trim().len();
    if clipped && !sentences.is_empty() {
        sentences.remove(0);
    }
    let Some(last) = sentences.last() else {
        // No sentence starts in view: the recent end is all there is to show.
        let tail_start = text
            .char_indices()
            .rev()
            .nth(MAX_CHARS - 2)
            .map_or(0, |(index, _)| index);
        return format!("…{}", &text[tail_start..]);
    };
    let unfinished = !last.ends_with(['.', '?', '!']) && last.split(' ').count() < MIN_LIVE_WORDS;
    let chosen = if unfinished && sentences.len() > 1 {
        sentences[sentences.len() - 2]
    } else {
        last
    };
    chosen.chars().take(MAX_CHARS).collect()
}

pub fn decoded_session_id(output: &str) -> Option<Uuid> {
    let value = serde_json::from_str::<serde_json::Value>(output.trim()).ok()?;
    Uuid::parse_str(value.get("session_id")?.as_str()?).ok()
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum ApplyMode {
    Live,
    History,
}

impl TranscriptCore {
    pub fn append_assistant_message_delta(
        &mut self,
        message_id: Uuid,
        delta: &str,
        event: &SessionEvent,
    ) {
        if self.live_turn_closed || delta.is_empty() {
            return;
        }
        if let Some(index) = self.messages.get(&message_id).copied() {
            if !matches!(
                self.order.get(index),
                Some(TranscriptEntry::Message {
                    actor: EventActor::Assistant,
                    execution,
                    ..
                }) if !execution.is_terminal()
            ) {
                return;
            }
            self.finish_reasoning(event.created_at);
            self.stream_into_message(message_id, delta);
            return;
        }
        self.finish_reasoning(event.created_at);
        self.collapse_previous_edit();
        let (model, effort, fast, ultrafast) = self
            .active_turn
            .as_ref()
            .map(|turn| {
                (
                    turn.model.clone(),
                    turn.effort.clone(),
                    turn.fast,
                    turn.ultrafast,
                )
            })
            .or_else(|| {
                self.config.as_ref().map(|config| {
                    (
                        config.model.clone(),
                        config.effort.clone(),
                        config.fast,
                        config.ultrafast,
                    )
                })
            })
            .unwrap_or_default();
        let time = self.message_event_time(event);
        self.messages.insert(message_id, self.order.len());
        self.order.push(TranscriptEntry::Message {
            actor: EventActor::Assistant,
            text: String::new(),
            attachments: Vec::new(),
            model,
            effort: display_message_effort(effort, fast, ultrafast),
            time,
            status: MessageStatus::InProgress,
            execution: Execution::new(event.created_at, ExecutionState::Running),
            redirected: false,
        });
        self.stream_into_message(message_id, delta);
    }

    fn start_reasoning_block(&mut self, reasoning_id: Uuid, event: &SessionEvent) {
        if self.live_turn_closed || self.reasoning_blocks.contains_key(&reasoning_id) {
            return;
        }
        self.finish_reasoning(event.created_at);
        self.start_reasoning(event.created_at, local_event_time(event));
        if let Some(index) = self.active_reasoning {
            self.reasoning_blocks.insert(reasoning_id, index);
            self.order.identities[index] = (reasoning_id, 0);
        }
    }

    fn update_reasoning_block(&mut self, reasoning_id: Uuid, text: &str, snapshot: bool) {
        let Some(index) = self.reasoning_blocks.get(&reasoning_id).copied() else {
            return;
        };
        if self
            .order
            .get(index)
            .and_then(TranscriptEntry::execution)
            .is_none_or(Execution::is_terminal)
        {
            return;
        }
        if let Some(Operation {
            kind: OperationKind::Reasoning,
            input: Some(OperationView { text: source, .. }),
            detail,
            ..
        }) = self.order.call_mut(index)
        {
            if snapshot {
                Self::merge_reasoning_snapshot(source, text);
            } else {
                source.push_str(text);
            }
            *detail = reasoning_preview(source);
            if self.active_reasoning == Some(index) {
                self.reasoning_has_preview = true;
            }
        }
    }

    fn complete_reasoning_block(&mut self, reasoning_id: Uuid, at: DateTime<Utc>) {
        let Some(index) = self.reasoning_blocks.get(&reasoning_id).copied() else {
            return;
        };
        if self
            .order
            .get(index)
            .and_then(TranscriptEntry::execution)
            .is_none_or(Execution::is_terminal)
        {
            return;
        }
        if let Some(Operation {
            execution,
            label: name,
            expanded,
            ..
        }) = self.order.call_mut(index)
        {
            execution.transition(ExecutionState::Complete, at);
            *name = "Reasoned".to_string();
            *expanded = false;
        }
        if self.active_reasoning == Some(index) {
            self.active_reasoning = None;
            self.reasoning_has_preview = false;
        }
    }

    pub fn append_reasoning(&mut self, text: &str, started_at: DateTime<Utc>, time: String) {
        if self.live_turn_closed || self.has_active_reasoning_block() {
            return;
        }
        if let Some(index) = self.active_reasoning
            && let Some(Operation {
                execution,
                kind: OperationKind::Reasoning,
                input: Some(OperationView { text: source, .. }),
                detail,
                ..
            }) = self.order.call_mut(index)
            && !execution.is_terminal()
        {
            if self.reasoning_has_preview {
                // The owner returns cumulative snapshots; replace any preview with a missing prefix.
                if !source.starts_with(text) {
                    source.clear();
                    source.push_str(text);
                }
            } else {
                Self::merge_reasoning_snapshot(source, text);
            }
            *detail = reasoning_preview(source);
            return;
        }
        if text.trim().is_empty() {
            return;
        }
        self.start_reasoning(started_at, time);
        if let Some(index) = self.active_reasoning
            && let Some(Operation {
                kind: OperationKind::Reasoning,
                input: Some(OperationView { text: source, .. }),
                detail,
                ..
            }) = self.order.call_mut(index)
        {
            Self::merge_reasoning_snapshot(source, text);
            *detail = reasoning_preview(source);
        }
    }

    pub fn append_reasoning_text_delta(
        &mut self,
        delta: &str,
        started_at: DateTime<Utc>,
        time: String,
    ) {
        if self.live_turn_closed || self.has_active_reasoning_block() || delta.is_empty() {
            return;
        }
        if self.active_reasoning.is_none() {
            self.start_reasoning(started_at, time);
        }
        if let Some(index) = self.active_reasoning
            && let Some(Operation {
                execution,
                kind: OperationKind::Reasoning,
                input: Some(OperationView { text: source, .. }),
                detail,
                ..
            }) = self.order.call_mut(index)
            && !execution.is_terminal()
        {
            source.push_str(delta);
            *detail = reasoning_preview(source);
            self.reasoning_has_preview = true;
        }
    }

    pub fn apply(&mut self, event: &SessionEvent) -> Option<usize> {
        self.order.event_identity = Some((event.id, 0));
        let removed = self.apply_event(event, ApplyMode::Live);
        self.order.event_identity = None;
        self.prune_child_metadata();
        removed
    }

    fn apply_event(&mut self, event: &SessionEvent, mode: ApplyMode) -> Option<usize> {
        // Scope recorded insertions to this event so a transcript nobody
        // drains (an unfocused child) cannot accumulate stale indices.
        self.pending_entry_insertions.clear();
        let sequence = self
            .meta
            .source_sequence
            .as_deref()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        // Live input is canonical journal order. History reconstruction may
        // deliberately move user rows for display, so only Live admits by cursor.
        if mode == ApplyMode::Live && event.sequence > 0 && event.sequence <= sequence {
            return None;
        }
        self.meta.source_sequence = Some(sequence.max(event.sequence).to_string());
        if let SessionEventKind::SubagentActivity {
            activity,
            agent,
            event: child_event,
        } = &event.kind
        {
            let snapshot = subagent_snapshot_for_activity(*activity, agent, child_event.as_deref());
            if self
                .subagent_snapshots
                .get(&agent.session_id)
                .is_some_and(|current| !snapshot.supersedes(current))
            {
                return None;
            }
        }
        let target = match &event.kind {
            SessionEventKind::ApprovalRequested { approval_id, .. } => self
                .interaction_entries
                .get(&format!("approval:{approval_id}")),
            SessionEventKind::ProviderInteractionRequested { interaction_id, .. } => self
                .interaction_entries
                .get(&format!("provider:{interaction_id}")),
            SessionEventKind::Message { message_id, .. } => self.messages.get(message_id),
            _ => None,
        };
        if target
            .and_then(|index| self.order.get(*index))
            .and_then(TranscriptEntry::execution)
            .is_some_and(Execution::is_terminal)
        {
            return None;
        }
        match &event.kind {
            SessionEventKind::ReasoningBlockStarted { reasoning_id } => {
                if self.live_turn_closed || self.reasoning_blocks.contains_key(reasoning_id) {
                    return None;
                }
            }
            SessionEventKind::ReasoningBlockDelta { reasoning_id, .. }
            | SessionEventKind::ReasoningBlockSnapshot { reasoning_id, .. }
            | SessionEventKind::ReasoningBlockCompleted { reasoning_id }
                if self
                    .reasoning_blocks
                    .get(reasoning_id)
                    .and_then(|index| self.order.get(*index))
                    .and_then(TranscriptEntry::execution)
                    .is_none_or(Execution::is_terminal) =>
            {
                return None;
            }
            _ => {}
        }
        if let SessionEventKind::ProviderEvent { kind, payload, .. } = &event.kind
            && (is_context_compaction(kind) || kind == "context_compaction_failed")
            && let Some(operation_id) = payload.get("operation_id")
        {
            if let Some(id) = operation_id
                .as_str()
                .and_then(|id| Uuid::parse_str(id).ok())
            {
                self.apply_compaction_operation(id, kind, payload, event, mode);
            }
            return None;
        }
        if self.has_active_reasoning_block()
            && (matches!(
                event.kind,
                SessionEventKind::ReasoningDelta { .. }
                    | SessionEventKind::ReasoningTextDelta { .. }
                    | SessionEventKind::ReasoningCompleted
            ) || matches!(&event.kind, SessionEventKind::ProviderEvent { kind, payload, .. } if Self::provider_reasoning_lifecycle(kind, payload).is_some()))
        {
            return None;
        }
        if mode == ApplyMode::Live
            && let SessionEventKind::ProviderEvent { kind, payload, .. } = &event.kind
            && (is_context_compaction(kind) || kind == "context_compaction_failed")
            && payload.get("operation_id").is_none()
            && (self.has_active_reasoning_block()
                || self.compaction_entries.values().any(|index| {
                    self.order
                        .get(*index)
                        .and_then(TranscriptEntry::execution)
                        .is_some_and(|execution| !execution.is_terminal())
                }))
        {
            return None;
        }
        self.observe_meta(event);
        self.observe_model_request(event, mode == ApplyMode::Live);
        match &event.kind {
            SessionEventKind::ProviderEvent { kind, .. } if kind == "goal_yielded" => {
                self.waiting_on_watchers = true;
            }
            SessionEventKind::ProviderEvent { kind, .. } if kind == "goal_resumed" => {
                self.waiting_on_watchers = false;
            }
            SessionEventKind::TurnStarted { .. } | SessionEventKind::SessionStarted => {
                self.waiting_on_watchers = false;
            }
            // The durable Ready boundary carries the wait in its detail. Unlike
            // the ephemeral `goal_yielded`, it survives both a dropped live
            // event and a reconnect replay, so the label stays truthful there.
            SessionEventKind::StatusChanged {
                status,
                detail,
                lifecycle,
            } => {
                self.waiting_on_watchers = lifecycle.map_or_else(
                    || {
                        *status == SessionStatus::Ready
                            && detail
                                .as_deref()
                                .is_some_and(ready_detail_is_waiting_on_watchers)
                    },
                    |lifecycle| lifecycle == SubagentLifecycle::WaitingOnWatchers,
                );
            }
            _ => {}
        }

        if let SessionEventKind::ToolStarted {
            tool_call_id,
            parent_tool_call_id: Some(parent),
            ..
        } = &event.kind
        {
            self.tool_parents
                .insert(tool_call_id.clone(), parent.clone());
        }
        if let SessionEventKind::ToolCompleted {
            tool_call_id,
            is_error,
            output,
            ..
        } = &event.kind
            && (*is_error || tool_output_background_handle(output).is_none())
        {
            let mut parents = vec![tool_call_id.clone()];
            while let Some(parent) = parents.pop() {
                let children = self
                    .tool_parents
                    .iter()
                    .filter(|(_, recorded_parent)| **recorded_parent == parent)
                    .map(|(child, _)| child.clone())
                    .collect::<Vec<_>>();
                for child in children {
                    self.tool_parents.remove(&child);
                    parents.push(child.clone());
                    if let Some(index) = self.tools.get(&child).copied()
                        && !self.tool_has_live_process(index)
                        && self
                            .order
                            .get(index)
                            .and_then(TranscriptEntry::execution)
                            .is_some_and(|execution| !execution.is_terminal())
                        && let Some(Operation { execution, .. }) = self.order.call_mut(index)
                        && (!execution.is_terminal() || execution.is_backgrounded())
                    {
                        execution.transition(
                            if *is_error {
                                ExecutionState::Failed
                            } else {
                                ExecutionState::Complete
                            },
                            event.created_at,
                        );
                    }
                }
            }
        }

        let provider_advanced = matches!(
            &event.kind,
            SessionEventKind::Message {
                actor: EventActor::Assistant,
                ..
            } | SessionEventKind::MessageDelta { .. }
                | SessionEventKind::ReasoningDelta { .. }
                | SessionEventKind::ReasoningTextDelta { .. }
                | SessionEventKind::ReasoningCompleted
                | SessionEventKind::ReasoningBlockStarted { .. }
                | SessionEventKind::ReasoningBlockDelta { .. }
                | SessionEventKind::ReasoningBlockSnapshot { .. }
                | SessionEventKind::ReasoningBlockCompleted { .. }
                | SessionEventKind::ToolStarted { .. }
        ) || matches!(
            &event.kind,
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if Self::provider_reasoning_lifecycle(kind, payload).is_some()
        );
        let starts_prepared_action = match &event.kind {
            SessionEventKind::ToolStarted { tool_call_id, .. } => {
                !self.tools.contains_key(tool_call_id) && self.has_preparing_tool(tool_call_id)
            }
            _ => false,
        };
        // Text the model wrote before a tool call can be journaled after the
        // process has started. Until the call's own ToolStarted arrives the
        // response has not moved past it, so that text must not background it.
        let preparing_foreground = !matches!(&event.kind, SessionEventKind::ToolStarted { .. })
            && self.foreground_tool.as_deref().is_some_and(|id| {
                self.preparing_tools
                    .values()
                    .any(|preparing| preparing == id)
                    || self
                        .unkeyed_preparing_tools
                        .iter()
                        .any(|preparing| preparing == id)
            });
        if provider_advanced && !starts_prepared_action && !preparing_foreground {
            self.background_foreground_process();
        }
        let completed_turn = match &event.kind {
            SessionEventKind::TurnCompleted { message_id, .. }
            | SessionEventKind::PromptRecalled { message_id, .. } => Some(*message_id),
            _ => None,
        };
        if let Some(message_id) = completed_turn
            && self
                .active_turn
                .as_ref()
                .is_some_and(|turn| turn.message_id == message_id)
        {
            self.active_turn = None;
        }
        match &event.kind {
            // A new turn id retires every action preparation the previous turn
            // left behind. Those spinners describe generation this stream will
            // never report on again, so nothing downstream can ever settle them.
            SessionEventKind::TurnStarted { message_id, .. } => {
                if self
                    .active_turn
                    .as_ref()
                    .is_none_or(|turn| turn.message_id != *message_id)
                {
                    self.settle_stale_preparations(event.created_at);
                }
                self.live_turn_closed = false;
            }
            SessionEventKind::StatusChanged {
                status:
                    SessionStatus::Starting | SessionStatus::Running | SessionStatus::WaitingForApproval,
                ..
            } => {
                // Only a genuine closed→live transition retires preparations;
                // a mid-turn Running refresh (an approval returning) must not
                // cancel generation that is still in flight.
                if self.live_turn_closed {
                    self.settle_stale_preparations(event.created_at);
                }
                self.live_turn_closed = false;
            }
            SessionEventKind::StatusChanged {
                status:
                    SessionStatus::Ready
                    | SessionStatus::Completed
                    | SessionStatus::Failed
                    | SessionStatus::Stopped,
                detail,
                ..
            } => {
                self.live_turn_closed = true;
                self.finish_live_assistant_messages(
                    event.created_at,
                    detail
                        .as_deref()
                        .is_some_and(|detail| detail.eq_ignore_ascii_case("interrupted")),
                    false,
                );
            }
            SessionEventKind::ProviderEvent { kind, .. } if kind == "native_steer_applied" => {
                self.finish_live_assistant_messages(event.created_at, false, true);
                self.settle_stale_preparations(event.created_at);
            }
            SessionEventKind::TurnCompleted { error, .. } => {
                self.live_turn_closed = true;
                self.finish_live_assistant_messages(
                    event.created_at,
                    error
                        .as_deref()
                        .is_some_and(|error| error.to_ascii_lowercase().contains("interrupted")),
                    false,
                );
            }
            _ => {}
        }
        let mut removed_entry = None;
        match &event.kind {
            SessionEventKind::SessionConfigured {
                cwd,
                provider,
                model,
                effort,
                response_language,
                fast,
                ultrafast,
                permission_mode,
                speed_support,
            } if self
                .config_event_updated_at
                .is_none_or(|updated| event.created_at >= updated)
                && (self
                    .config_updated_at
                    .is_none_or(|updated| event.created_at >= updated)
                    || self.config.as_ref().is_some_and(|current| {
                        current.provider == *provider
                            && current.model == *model
                            && current.effort == *effort
                            && current.fast == *fast
                            && current.ultrafast == *ultrafast
                            && current.cwd == *cwd
                    })) =>
            {
                self.config_event_updated_at = Some(event.created_at);
                self.config_updated_at = Some(
                    self.config_updated_at
                        .map_or(event.created_at, |updated| updated.max(event.created_at)),
                );
                let context_identity_changed = self.config.as_ref().is_some_and(|old| {
                    old.provider != *provider || old.model.as_ref() != model.as_ref()
                });
                self.config = Some(SessionDisplayConfig {
                    cwd: cwd.clone(),
                    provider: *provider,
                    model: model.clone(),
                    effort: effort.clone(),
                    response_language: *response_language,
                    fast: *fast,
                    ultrafast: *ultrafast,
                    permission_mode: *permission_mode,
                    speed_support: *speed_support,
                });
                if context_identity_changed {
                    // Context usage belongs to the provider/model identity that
                    // produced it. Do not carry the old model's percentage over
                    // while the new provider is preparing its first report.
                    self.context_known = false;
                    self.context_remaining_percent = 100;
                    self.context_tokens = None;
                    self.context_window_tokens = None;
                }
            }
            SessionEventKind::ProviderCapabilitiesUpdated { providers } => {
                self.provider_capabilities = providers.clone();
            }
            SessionEventKind::TurnStarted {
                message_id,
                provider,
                model,
                effort,
                fast,
                ultrafast,
            } => {
                self.active_turn = Some(ActiveTurnDisplayConfig {
                    message_id: *message_id,
                    provider: *provider,
                    model: model.clone(),
                    effort: effort.clone(),
                    fast: *fast,
                    ultrafast: *ultrafast,
                });
            }
            SessionEventKind::UsageUpdated {
                turn_id,
                provider_context_reused,
                input_tokens,
                output_tokens,
                cached_input_tokens,
                cache_creation_input_tokens,
                total_tokens,
                cost_microusd,
                cost_basis,
                context_tokens,
                context_window_tokens,
                ..
            } => {
                let had_usage = self.session_usage.cost_complete.is_some()
                    || self.session_usage.total_tokens > 0
                    || self.session_usage.input_tokens > 0
                    || self.session_usage.output_tokens > 0
                    || self.session_usage.cost_microusd.is_some();
                let usage_bearing = *total_tokens > 0
                    || *input_tokens > 0
                    || *output_tokens > 0
                    || *cached_input_tokens > 0
                    || *cache_creation_input_tokens > 0
                    || cost_microusd.is_some();
                if usage_bearing {
                    self.session_usage.cost_complete = if cost_microusd.is_none() {
                        Some(false)
                    } else if !had_usage {
                        Some(true)
                    } else {
                        self.session_usage.cost_complete
                    };
                }
                self.session_usage.input_tokens = self
                    .session_usage
                    .input_tokens
                    .saturating_add(*input_tokens);
                self.session_usage.output_tokens = self
                    .session_usage
                    .output_tokens
                    .saturating_add(*output_tokens);
                self.session_usage.cached_input_tokens = self
                    .session_usage
                    .cached_input_tokens
                    .saturating_add(*cached_input_tokens);
                self.session_usage.cache_creation_input_tokens = self
                    .session_usage
                    .cache_creation_input_tokens
                    .saturating_add(*cache_creation_input_tokens);
                self.session_usage.total_tokens = self
                    .session_usage
                    .total_tokens
                    .saturating_add(*total_tokens);
                if context_tokens.is_some() {
                    self.session_usage.context_tokens = *context_tokens;
                }
                if cost_microusd.is_some() {
                    self.session_usage.cost_basis = if self.session_usage.cost_microusd.is_some()
                        && self.session_usage.cost_basis != *cost_basis
                    {
                        "mixed".to_string()
                    } else {
                        cost_basis.clone()
                    };
                }
                self.session_usage.cost_microusd =
                    match (self.session_usage.cost_microusd, cost_microusd) {
                        (Some(current), Some(additional)) => {
                            Some(current.saturating_add(*additional))
                        }
                        (None, Some(value)) => Some(*value),
                        (current, None) => current,
                    };
                if let (Some(context_tokens), Some(context_window_tokens)) =
                    (context_tokens, context_window_tokens)
                {
                    self.context_known = true;
                    self.context_tokens = Some(*context_tokens);
                    self.context_window_tokens = Some(*context_window_tokens);
                    self.context_remaining_percent =
                        context_remaining_percent(*context_tokens, *context_window_tokens);
                }
                let usage_belongs_to_active_turn = turn_id.is_none_or(|turn_id| {
                    self.active_turn
                        .as_ref()
                        .is_some_and(|active| active.message_id == turn_id)
                });
                if usage_belongs_to_active_turn
                    && let Some(signature) = self
                        .active_turn
                        .as_ref()
                        .map(ActiveTurnDisplayConfig::cache_signature)
                        .or_else(|| {
                            self.config
                                .as_ref()
                                .map(SessionDisplayConfig::cache_signature)
                        })
                    && let Some(notice) = self.cache_diagnostics.observe(
                        event.created_at,
                        signature,
                        CacheUsage {
                            input_tokens: *input_tokens,
                            cached_input_tokens: *cached_input_tokens,
                            cache_creation_input_tokens: *cache_creation_input_tokens,
                            context_tokens: *context_tokens,
                            cost_microusd: *cost_microusd,
                            cost_basis,
                            provider_context_reused: *provider_context_reused,
                        },
                    )
                {
                    self.order.push(TranscriptEntry::Info {
                        title: "Prompt cache miss".to_string(),
                        text: notice.text(),
                        time: local_event_time(event),
                    });
                }
            }
            SessionEventKind::ContextWindowUpdated {
                context_tokens,
                context_window_tokens,
            } => {
                self.cache_diagnostics
                    .update_context_tokens(*context_tokens);
                self.context_known = true;
                self.context_tokens = Some(*context_tokens);
                self.context_window_tokens = Some(*context_window_tokens);
                self.session_usage.context_tokens = Some(*context_tokens);
                self.context_remaining_percent =
                    context_remaining_percent(*context_tokens, *context_window_tokens);
            }
            SessionEventKind::SessionStarted if self.child_transcript => {
                // Sequence 1 of a fresh session. Reaching it means the window
                // being replayed really does start at the child's beginning,
                // so the first user row it materializes is the director's
                // assignment. A bounded tail never carries this event, and
                // stays unarmed.
                self.director_prompt = DirectorPrompt::Expected;
            }
            SessionEventKind::ContextCleared => {
                self.clear_visible_entries();
                self.context_remaining_percent = 100;
                self.context_known = true;
                self.context_tokens = self.context_window_tokens.map(|_| 0);
                self.cache_diagnostics.reset();
            }
            SessionEventKind::PromptRecalled { message_id, .. } => {
                self.queued_messages.remove(message_id);
                self.queued_message_sequences.remove(message_id);
                removed_entry = self.remove_message(*message_id);
            }
            SessionEventKind::Message {
                message_id,
                actor,
                text,
                status,
                attachments,
                delivery: _,
            } => {
                if *actor == EventActor::Assistant
                    && *status == MessageStatus::InProgress
                    && let Some(index) = self.messages.get(message_id)
                    && let Some(TranscriptEntry::Message {
                        status: stored_status,
                        ..
                    }) = self.order.get(*index)
                    && (text.trim().is_empty() || *stored_status != MessageStatus::InProgress)
                {
                    return removed_entry;
                }
                if self.messages.get(message_id).and_then(|index| self.order.get(*index))
                    .is_some_and(|row| matches!(row, TranscriptEntry::Message { execution, .. } if execution.is_terminal()))
                { return removed_entry; }
                // A snapshot carries the whole reply so far: it is compared
                // with what is shown plus what paragraph streaming holds back.
                let held_tail = self.streaming_tails.remove(message_id).unwrap_or_default();
                let mut replaced_text = false;
                let event_time = self.message_event_time(event);
                // A coalesced live snapshot can arrive after its durable
                // terminal boundary during reconnect. Never resurrect a
                // responding row once the active turn has been closed.
                if *actor == EventActor::Assistant
                    && *status == MessageStatus::InProgress
                    && self.live_turn_closed
                {
                    return removed_entry;
                }
                // Watch output is provider input too, but the user armed the
                // watch and wants to see what it said: a compact, collapsed
                // row per delivered event.
                if *actor == EventActor::System
                    && let Some((label, body)) = parse_watch_event(text)
                {
                    if *status != MessageStatus::Queued && self.remember_watch_message(*message_id)
                    {
                        self.order
                            .push(TranscriptEntry::Operation(Operation::lifecycle(
                                OperationKind::Watch,
                                "Watch",
                                label,
                                Some(body),
                                local_event_time(event),
                                Execution::new(event.created_at, ExecutionState::Complete),
                            )));
                    }
                    return removed_entry;
                }
                // Provider input is hidden; AgentMessageReceived owns visible team reports.
                if *actor == EventActor::System || self.agent_messages.contains(message_id) {
                    removed_entry = self.remove_message(*message_id);
                    return removed_entry;
                }
                // Queued prompts belong to the pending-input projection only.
                // Materializing an invisible transcript row here would pin the
                // eventual admitted message to its enqueue position instead of
                // the real provider-boundary chronology.
                if *status == MessageStatus::Queued {
                    self.queued_messages.insert(*message_id);
                    if event.sequence > 0 {
                        self.queued_message_sequences
                            .insert(*message_id, event.sequence);
                    }
                    // An accepted steer may have briefly materialized as an
                    // in-progress transcript row. If the turn is interrupted,
                    // its later queue transition must withdraw that row again.
                    removed_entry = self.remove_message(*message_id);
                    return removed_entry;
                }
                if *actor == EventActor::Assistant
                    && ((text.trim().is_empty()
                        && attachments.is_empty()
                        && *status != MessageStatus::InProgress)
                        || assistant_message_is_retired_action_leak(text))
                {
                    removed_entry = self.remove_message(*message_id);
                    return removed_entry;
                }
                if *actor == EventActor::Assistant {
                    self.finish_reasoning(event.created_at);
                }
                if let Some(index) = self.messages.get(message_id).copied() {
                    let mut finished = false;
                    let numbered_attachments = matches!(
                        &self.order[index],
                        TranscriptEntry::Message {
                            attachments: stored,
                            ..
                        } if stored.is_empty() && !attachments.is_empty()
                    )
                    .then(|| {
                        number_message_attachments(text, attachments, &mut self.next_image_number)
                    });
                    if let TranscriptEntry::Message {
                        actor: stored_actor,
                        text: stored_text,
                        attachments: stored_attachments,
                        status: stored_status,
                        execution,
                        ..
                    } = &mut self.order[index]
                    {
                        *stored_actor = *actor;
                        if *actor == EventActor::Assistant
                            && *status == MessageStatus::InProgress
                            && *stored_status == MessageStatus::InProgress
                            && format!("{stored_text}{held_tail}").starts_with(text.as_str())
                        {
                            if !held_tail.is_empty() {
                                self.streaming_tails.insert(*message_id, held_tail);
                            }
                        } else {
                            *stored_text = text.clone();
                        }
                        *stored_status = *status;
                        replaced_text = true;
                        let next =
                            Execution::for_message(event.created_at, *actor, *status).state();
                        execution.transition(next, event.created_at);
                        if let Some(attachments) = numbered_attachments {
                            *stored_attachments = attachments;
                        }
                        finished = matches!(
                            stored_status,
                            MessageStatus::Complete | MessageStatus::Failed
                        );
                    }
                    if finished {
                        // The reply is over, so whatever the paced floor was
                        // still holding lands now as one block rather than
                        // trailing in behind a finished row.
                        self.release_streaming_tail(*message_id);
                    } else if replaced_text && !self.streaming_tails.contains_key(message_id) {
                        self.hold_back_unfinished(*message_id);
                    }
                } else {
                    let attachments =
                        number_message_attachments(text, attachments, &mut self.next_image_number);
                    let (model, effort, fast, ultrafast) = if *actor == EventActor::Assistant {
                        self.active_turn
                            .as_ref()
                            .map(|turn| {
                                (
                                    turn.model.clone(),
                                    turn.effort.clone(),
                                    turn.fast,
                                    turn.ultrafast,
                                )
                            })
                            .or_else(|| {
                                self.config.as_ref().map(|config| {
                                    (
                                        config.model.clone(),
                                        config.effort.clone(),
                                        config.fast,
                                        config.ultrafast,
                                    )
                                })
                            })
                            .unwrap_or_default()
                    } else {
                        (None, None, false, false)
                    };
                    if *status != MessageStatus::Queued
                        && matches!(actor, EventActor::User | EventActor::Assistant)
                    {
                        self.collapse_previous_edit();
                    }
                    let queued = self.queued_messages.remove(message_id);
                    let queued_sequence = self.queued_message_sequences.get(message_id).copied();
                    let insertion_index = if *actor == EventActor::User
                        && matches!(status, MessageStatus::Complete | MessageStatus::Failed)
                        && event.sequence > 0
                        && !self.live_turn_closed
                        && mode == ApplyMode::Live
                        && (!queued || queued_sequence.is_some())
                    {
                        queued_sequence.map_or_else(
                            || self.late_user_message_insertion_index(),
                            |sequence| self.queued_user_message_insertion_index(sequence),
                        )
                    } else {
                        self.order.len()
                    };
                    if insertion_index < self.order.len() {
                        self.reindex_after_insertion(insertion_index);
                    }
                    // Claim the badge for the first user row this window
                    // materializes. Queued, withdrawn and provider-input
                    // messages never reach here, so only a row the reader can
                    // actually see can be the assignment.
                    if self.director_prompt == DirectorPrompt::Expected
                        && *actor == EventActor::User
                    {
                        self.director_prompt = DirectorPrompt::Row(*message_id);
                    }
                    self.messages.insert(*message_id, insertion_index);
                    self.order.insert(
                        insertion_index,
                        TranscriptEntry::Message {
                            actor: *actor,
                            text: text.clone(),
                            attachments,
                            model,
                            effort: display_message_effort(effort, fast, ultrafast),
                            time: event_time,
                            status: *status,
                            execution: Execution::for_message(event.created_at, *actor, *status),
                            redirected: false,
                        },
                    );
                    self.hold_back_unfinished(*message_id);
                }
            }
            SessionEventKind::MessageDelta { message_id, delta } => {
                self.append_assistant_message_delta(*message_id, delta, event);
            }
            SessionEventKind::ReasoningBlockStarted { reasoning_id } => {
                self.start_reasoning_block(*reasoning_id, event);
            }
            SessionEventKind::ReasoningBlockDelta {
                reasoning_id,
                delta,
            } => {
                self.update_reasoning_block(*reasoning_id, delta, false);
            }
            SessionEventKind::ReasoningBlockSnapshot { reasoning_id, text } => {
                self.update_reasoning_block(*reasoning_id, text, true);
            }
            SessionEventKind::ReasoningBlockCompleted { reasoning_id } => {
                self.complete_reasoning_block(*reasoning_id, event.created_at);
            }
            SessionEventKind::ReasoningDelta { text } => {
                self.append_reasoning(text, event.created_at, local_event_time(event));
                if let Some(index) = self.active_reasoning {
                    self.tools
                        .entry(format!("reasoning:{}", event.id))
                        .or_insert(index);
                }
            }
            SessionEventKind::ReasoningTextDelta { delta } => {
                let key = format!("reasoning:{}", event.id);
                if !self.tools.contains_key(&key) {
                    self.append_reasoning_text_delta(
                        delta,
                        event.created_at,
                        local_event_time(event),
                    );
                    if let Some(index) = self.active_reasoning
                        && !self
                            .tools
                            .iter()
                            .any(|(key, stored)| key.starts_with("reasoning:") && *stored == index)
                    {
                        self.tools.insert(key, index);
                    }
                }
            }
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if kind == "reasoning_snapshot" =>
            {
                self.apply_reasoning_snapshot(payload, event);
            }
            SessionEventKind::ReasoningCompleted => {
                self.finish_reasoning(event.created_at);
            }
            SessionEventKind::ProviderEvent { kind, payload, .. } if kind == "action/preparing" => {
                let label = if self.action_descriptors {
                    payload
                        .get("label")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                } else {
                    ""
                };
                self.upsert_action_preparation(
                    event,
                    label,
                    payload
                        .get("tool_call_id")
                        .and_then(serde_json::Value::as_str),
                );
            }
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if kind == "action/generation_status" =>
            {
                let waiting = payload
                    .get("waiting")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false);
                let label = if self.action_descriptors {
                    payload
                        .get("label")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                } else {
                    ""
                };
                self.update_action_generation_status(
                    payload
                        .get("tool_call_id")
                        .and_then(serde_json::Value::as_str),
                    waiting,
                    label,
                );
            }
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if kind == "action/preparing_cancelled" =>
            {
                // A cancel that names its tool call has to reach that exact
                // preparation. Reading only the unkeyed list left every
                // labeled preparation uncancellable, and popping a single
                // entry could never retire concurrent ones.
                let preparing_id = match payload
                    .get("tool_call_id")
                    .and_then(serde_json::Value::as_str)
                {
                    Some(tool_call_id) => self.preparing_tools.remove(tool_call_id).or_else(|| {
                        (!self.unkeyed_preparing_tools.is_empty())
                            .then(|| self.unkeyed_preparing_tools.remove(0))
                    }),
                    None => self.unkeyed_preparing_tools.pop(),
                };
                if let Some(preparing_id) = preparing_id
                    && let Some(index) = self.tools.get(&preparing_id).copied()
                {
                    if self.foreground_tool.as_deref() == Some(preparing_id.as_str()) {
                        self.foreground_tool = None;
                    }
                    if index + 1 == self.order.len() {
                        self.tools.remove(&preparing_id);
                        self.order.pop();
                    } else {
                        self.cancel_preparing_tool(&preparing_id, event.created_at);
                    }
                }
            }
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if is_live_tool_call_event(kind) =>
            {
                let Some(tool_call_id) = payload
                    .get("tool_call_id")
                    .and_then(serde_json::Value::as_str)
                else {
                    return removed_entry;
                };
                let Some(raw_name) = payload.get("name").and_then(serde_json::Value::as_str) else {
                    return removed_entry;
                };
                let input = payload.get("input").unwrap_or(&serde_json::Value::Null);
                self.promote_preparing_tool_if_untracked(tool_call_id);
                self.upsert_running_tool(event, tool_call_id, raw_name, input, None);
            }
            SessionEventKind::ToolStarted {
                tool_call_id,
                name,
                input,
                input_ref,
                ..
            } => {
                if !self.tools.contains_key(tool_call_id)
                    && self.has_preparing_tool(tool_call_id)
                    && edit_is_awaiting_diff(name, input)
                {
                    return removed_entry;
                }
                self.promote_preparing_tool_if_untracked(tool_call_id);
                self.upsert_running_tool(event, tool_call_id, name, input, input_ref.as_ref());
            }
            SessionEventKind::ToolUpdated {
                tool_call_id,
                name,
                input,
            } => {
                self.promote_preparing_tool_if_untracked(tool_call_id);
                if mode == ApplyMode::Live && !self.tools.contains_key(tool_call_id) {
                    return None;
                }
                self.upsert_running_tool(event, tool_call_id, name, input, None);
            }
            SessionEventKind::ToolCompleted {
                tool_call_id,
                output,
                output_ref,
                is_error,
                input,
                input_ref,
                ..
            } => {
                if self
                    .tools
                    .get(tool_call_id)
                    .and_then(|index| self.order.call(*index))
                    .is_some_and(|row| row.execution.is_terminal())
                {
                    return None;
                }
                self.complete_preparing_tool(tool_call_id);
                if self.foreground_tool.as_deref() == Some(tool_call_id) {
                    self.foreground_tool = None;
                }
                let expand_edits = self.diff_expansion != DiffExpansionPolicy::Collapsed;
                let tool_index = self.tools.get(tool_call_id).copied().or_else(|| {
                    if self.provider_followups.contains_key(tool_call_id) {
                        return None;
                    }
                    let index = self.order.len();
                    self.tools.insert(tool_call_id.clone(), index);
                    self.order.push(TranscriptEntry::Operation(Operation::tool(
                        "tool",
                        "Completed tool",
                        "",
                        local_event_time(event),
                        Execution::new(event.created_at, ExecutionState::Running),
                    )));
                    Some(index)
                });
                if self.retain_tool_inspection
                    && let Some(index) = tool_index
                    && let Some((id, ordinal)) = self.order.identities.get(index)
                {
                    self.tool_inspection_outputs
                        .insert(format!("{id}:{ordinal}"), output.clone());
                }
                let (source_name_for_process, command_for_process) = tool_index
                    .and_then(|index| self.order.call(index))
                    .map(|call| (call.source.clone(), call.detail.clone()))
                    .unwrap_or_default();
                let command_edit = command_edit_presentation(&source_name_for_process, output);
                let deferred_command_edit = command_edit
                    .as_ref()
                    .and_then(|edit| edit.input.as_ref())
                    .is_some_and(|body| !is_diff_language(&body.language));
                let run_output = command_edit
                    .as_ref()
                    .map(|_| command_output_without_changes(output))
                    .unwrap_or_else(|| output.clone());
                let stored_followup_handle = self.provider_followups.remove(tool_call_id);
                let followup_handle =
                    tool_process_followup_handle(&source_name_for_process, input.as_ref())
                        .or(stored_followup_handle);
                let native_followup_process = followup_handle
                    .as_deref()
                    .and_then(|handle| Uuid::parse_str(handle).ok());
                let edit_origin = followup_handle
                    .as_deref()
                    .and_then(|handle| Uuid::parse_str(handle).ok())
                    .and_then(|process_id| self.originating_tool_call_id(process_id))
                    .unwrap_or_else(|| tool_call_id.clone());
                let reported_background_handle = (!*is_error)
                    .then(|| tool_output_background_handle(output))
                    .flatten();
                let output_handle = reported_background_handle.clone();
                if reported_background_handle.is_none()
                    && let Some(index) = tool_index
                {
                    self.provider_backgrounds
                        .retain(|_, process| process.tool_index != index);
                }
                let has_live_process =
                    tool_index.is_some_and(|index| self.tool_has_live_process(index));
                if let Some(index) = tool_index
                    && let Some(Operation {
                        kind,
                        execution,
                        source: source_name,
                        label: name,
                        detail,
                        input: code_view,
                        output: output_view,
                        expanded,
                        payload_refs,
                        outcome,
                        cwd,
                        ..
                    }) = self.order.call_mut(index)
                {
                    payload_refs.extend(output_ref.iter().cloned());
                    payload_refs.extend(input_ref.iter().cloned());
                    if name == "Search web"
                        && let Some(query) = input.as_ref().and_then(web_search_query)
                    {
                        *detail = format!("“{}”", compact_text(&query, 120));
                    }
                    // The command, when the completion does not repeat it.
                    let started_input = code_view
                        .as_ref()
                        .filter(|OperationView { language, .. }| language == "command")
                        .map(|OperationView { text: command, .. }| serde_json::json!({ "cmd": command }));
                    let completion_presentation = project_tool_presentation(
                        source_name,
                        input
                            .as_ref()
                            .or(started_input.as_ref())
                            .unwrap_or(&serde_json::Value::Null),
                        Some(&run_output),
                        *is_error,
                    );
                    *outcome = completion_presentation.outcome.clone();
                    if completion_presentation.cwd.is_some() {
                        *cwd = completion_presentation.cwd.clone();
                    }
                    if *is_error && !output.trim().is_empty() {
                        let message = completion_presentation
                            .result
                            .as_deref()
                            .unwrap_or(&run_output);
                        let message = compact_text(message, 120);
                        if detail.trim().is_empty() {
                            *detail = message;
                        } else {
                            *detail = format!("{detail} · {message}");
                        }
                    }
                    execution.transition(
                        if has_live_process {
                            ExecutionState::Backgrounded
                        } else if *is_error
                            || completion_presentation
                                .outcome
                                .as_deref()
                                .is_some_and(crate::semantics::outcome_is_failure)
                        {
                            ExecutionState::Failed
                        } else if completion_presentation.backgrounded
                            || reported_background_handle.is_some()
                        {
                            ExecutionState::Backgrounded
                        } else {
                            ExecutionState::Complete
                        },
                        event.created_at,
                    );
                    if completion_presentation.category == ToolPresentationCategory::Read
                        && !completion_presentation.detail.is_empty()
                    {
                        *detail = completion_presentation.detail.clone();
                    }
                    if completion_presentation.category != ToolPresentationCategory::Edit
                        && let Some(body) = completion_presentation.input.as_ref()
                        && (body.language == "command"
                            || input.as_ref().is_some_and(|value| {
                                value.as_object().is_none_or(|object| !object.is_empty())
                            }))
                    {
                        *code_view =
                            Some(OperationView::new(body.language.clone(), body.text.clone()));
                    }
                    // Keep the result summary on the tool header when the
                    // input had no useful detail (for example `git status`),
                    // while leaving the full output in the expandable tool
                    // body. This keeps command output out of assistant
                    // message bubbles without making completed tools
                    // anonymous.
                    if detail.trim().is_empty()
                        && let Some(result) = completion_presentation.result.as_deref()
                    {
                        *detail = compact_text(result, 120);
                    }
                    let input_is_diff = code_view
                        .as_ref()
                        .is_some_and(|OperationView { language, .. }| is_diff_language(language));
                    if completion_presentation.category == ToolPresentationCategory::Edit {
                        if let Some(body) = completion_presentation
                            .output
                            .filter(|body| is_diff_language(&body.language))
                        {
                            *name = completion_presentation.label;
                            *detail = completion_presentation.detail;
                            *code_view = Some(body.into());
                            *output_view = None;
                            *expanded = expand_edits;
                        } else if let Some(body) = completion_presentation
                            .input
                            .filter(|body| is_diff_language(&body.language))
                        {
                            // Some providers start a tool before its input is
                            // complete, then attach the authoritative edit
                            // envelope to ToolCompleted. Replace the stale
                            // JSON preview with the parsed diff.
                            *name = completion_presentation.label;
                            *detail = completion_presentation.detail;
                            *code_view = Some(body.into());
                            *output_view = None;
                            *expanded = expand_edits;
                        } else if input_is_diff && !*is_error {
                            // Native edit tools return a mutation receipt rather
                            // than a second diff. Keep the useful proposed diff
                            // as the sole expanded body instead of appending an
                            // opaque JSON receipt below it.
                            *output_view = None;
                            *expanded = expand_edits;
                        } else {
                            *output_view = if is_mcp_resource_probe(source_name) {
                                None
                            } else if *is_error && !run_output.trim().is_empty() {
                                Some(OperationView::new(
                                    "text".to_string(),
                                    run_output.trim_end().to_string(),
                                ))
                            } else {
                                tool_output_code_view(source_name, &run_output)
                                    .map(OperationView::from)
                            };
                        }
                    } else {
                        if !(deferred_command_edit && output_ref.is_some() && output_view.is_some())
                        {
                            *output_view = if is_mcp_resource_probe(source_name) {
                                None
                            } else if *is_error
                                && !run_output.trim().is_empty()
                                && completion_presentation
                                    .output
                                    .as_ref()
                                    .is_none_or(|body| body.language != "text")
                            {
                                Some(OperationView::new(
                                    "text".to_string(),
                                    run_output.trim_end().to_string(),
                                ))
                            } else {
                                borg_control_tool_output_view(
                                    source_name,
                                    input.as_ref(),
                                    &run_output,
                                )
                                .or_else(|| {
                                    borg_lsp_diagnostics_view(
                                        source_name,
                                        input.as_ref(),
                                        &run_output,
                                    )
                                })
                                .map(|text| {
                                    OperationView::new(
                                        if *kind == OperationKind::Delegation {
                                            "subagent"
                                        } else {
                                            "command"
                                        },
                                        text,
                                    )
                                })
                                .or_else(|| {
                                    tool_output_code_view(source_name, &run_output)
                                        .map(OperationView::from)
                                })
                            };
                        }
                    }
                    // A result that arrives without the call that produced it
                    // still names the process it came from.
                    if code_view.is_none()
                        && let Some(process) = decoded_session_id(output)
                            .and_then(|id| self.runtime_processes.get(&id))
                    {
                        *code_view = Some(OperationView::new(
                            "command".to_string(),
                            process.command.clone(),
                        ));
                    }
                    let _ = name;
                }
                // The process-start event can precede ToolStarted, and a parallel
                // call prepared later may hold the foreground then. The returned
                // process ID, not the foreground tool at launch, identifies its owner.
                let owned_process = tool_can_start_background_process(&source_name_for_process)
                    .then(|| decoded_session_id(output))
                    .flatten()
                    .or_else(|| {
                        native_followup_process
                            .is_none()
                            .then(|| {
                                output_handle
                                    .as_deref()
                                    .and_then(|h| Uuid::parse_str(h).ok())
                            })
                            .flatten()
                    });
                if let (Some(index), Some(process_id)) = (tool_index, owned_process)
                    && let Some(process) = self.runtime_processes.get_mut(&process_id)
                    && let Some(previous) = process.tool_index.replace(index)
                    && previous != index
                    && !self.tool_has_live_process(previous)
                    && let Some(Operation { execution, .. }) = self.order.call_mut(previous)
                    && execution.is_backgrounded()
                {
                    // The previous row only looked backgrounded because it was
                    // credited with this process.
                    execution.transition(ExecutionState::Complete, event.created_at);
                }
                if let (Some(index), Some(handle)) = (tool_index, output_handle.as_ref()) {
                    let is_native_process = Uuid::parse_str(handle)
                        .ok()
                        .is_some_and(|process_id| self.runtime_processes.contains_key(&process_id));
                    let finished = self
                        .watches
                        .iter()
                        .find(|watch| watch.watch_id.to_string() == *handle && !watch.running);
                    if let Some(watch) = finished {
                        if let Some(Operation { execution, .. }) = self.order.call_mut(index) {
                            execution.transition(
                                ExecutionState::Complete,
                                watch.last_event_at.unwrap_or(event.created_at),
                            );
                        }
                    } else if !is_native_process {
                        self.provider_backgrounds
                            .entry(handle.clone())
                            .or_insert_with(|| ProviderBackgroundProjection {
                                command: command_for_process.clone(),
                                tool_index: index,
                            });
                    }
                }
                if reported_background_handle.is_none()
                    && let Some(handle) = followup_handle
                    && let Some(process) = self.provider_backgrounds.remove(&handle)
                    && let Some(Operation {
                        execution,
                        output: output_view,
                        ..
                    }) = self.order.call_mut(process.tool_index)
                    && !execution.is_terminal()
                {
                    let output = tool_process_output_text(output);
                    if !output.trim().is_empty() {
                        *output_view = Some(OperationView::new("text".to_string(), output));
                    }
                    execution.transition(ExecutionState::Complete, event.created_at);
                }
                if let (Some(process_id), Some(index)) = (native_followup_process, tool_index) {
                    if self
                        .runtime_processes
                        .get(&process_id)
                        .is_some_and(|process| process.running)
                    {
                        self.poll_processes
                            .entry(process_id)
                            .or_default()
                            .push(tool_call_id.clone());
                    } else if let Some(Operation { execution, .. }) = self.order.call_mut(index) {
                        // A late poll event must not revive an exited process.
                        execution.transition(ExecutionState::Complete, event.created_at);
                    }
                }
                if let Some(edit) = command_edit {
                    self.upsert_command_edit(
                        &edit_origin,
                        edit,
                        local_event_time(event),
                        event.created_at,
                        output_ref.as_ref(),
                    );
                }
            }
            SessionEventKind::StatusChanged {
                status: SessionStatus::Ready,
                detail: Some(detail),
                ..
            } if detail.eq_ignore_ascii_case("interrupted") => {
                self.flush_streaming_tails();
                self.mark_running_tools_user_interrupted(event.created_at);
            }
            SessionEventKind::TurnCompleted {
                error: Some(error), ..
            } if error.to_ascii_lowercase().contains("interrupted") => {
                self.flush_streaming_tails();
                self.mark_running_tools_user_interrupted(event.created_at);
            }
            SessionEventKind::TurnCompleted {
                error: Some(error), ..
            } => {
                self.flush_streaming_tails();
                self.finish_running_tools(event.created_at, true, error);
            }
            SessionEventKind::TurnCompleted { error: None, .. } => {
                self.flush_streaming_tails();
                self.finish_running_tools(event.created_at, false, "");
                if let Some(goal) = self.goal.clone().filter(|goal| goal.status.is_active()) {
                    removed_entry = self.upsert_goal(goal, local_event_time(event), true);
                }
            }
            SessionEventKind::ApprovalRequested {
                approval_id,
                title,
                detail,
                ..
            } => {
                let key = format!("approval:{approval_id}");
                if self.interaction_entries.contains_key(&key) {
                    return None;
                }
                self.interaction_entries.insert(key, self.order.len());
                self.finish_reasoning(event.created_at);
                self.order
                    .push(TranscriptEntry::Operation(Operation::lifecycle(
                        OperationKind::Approval,
                        "Approval",
                        format_action_detail(title, detail),
                        (!detail.trim().is_empty()).then(|| detail.clone()),
                        local_event_time(event),
                        Execution::new(event.created_at, ExecutionState::Waiting),
                    )))
            }
            SessionEventKind::ProviderInteractionRequested {
                interaction_id,
                title,
                detail,
                payload,
                ..
            } => {
                self.finish_reasoning(event.created_at);
                let key = format!("provider:{interaction_id}");
                if self.interaction_entries.contains_key(&key) {
                    return None;
                }
                self.interaction_entries.insert(key, self.order.len());
                let options = provider_interaction_options(payload);
                self.order
                    .push(TranscriptEntry::Operation(Operation::lifecycle(
                        OperationKind::ProviderInteraction,
                        "Input needed",
                        if options.is_empty() {
                            format_action_detail(title, detail)
                        } else {
                            format!("{} · {options}", format_action_detail(title, detail))
                        },
                        (!detail.trim().is_empty()).then(|| detail.clone()),
                        local_event_time(event),
                        Execution::new(event.created_at, ExecutionState::Waiting),
                    )))
            }
            SessionEventKind::ApprovalResolved {
                approval_id,
                decision,
            } => {
                let state = if matches!(decision, ApprovalDecision::Deny) {
                    ExecutionState::Stopped
                } else {
                    ExecutionState::Complete
                };
                self.resolve_interaction(
                    &format!("approval:{approval_id}"),
                    state,
                    event.created_at,
                );
            }
            SessionEventKind::ProviderInteractionResolved { interaction_id, .. } => {
                self.resolve_interaction(
                    &format!("provider:{interaction_id}"),
                    ExecutionState::Complete,
                    event.created_at,
                );
            }
            SessionEventKind::WatchesChanged { watches } => {
                self.watches = watches.clone();
                for watch in watches.iter().filter(|watch| !watch.running) {
                    if let Some(process) = self
                        .provider_backgrounds
                        .remove(&watch.watch_id.to_string())
                        && let Some(Operation { execution, .. }) =
                            self.order.call_mut(process.tool_index)
                    {
                        execution.transition(
                            ExecutionState::Complete,
                            watch.last_event_at.unwrap_or(event.created_at),
                        );
                        let mut parents = self
                            .tools
                            .iter()
                            .filter(|(_, index)| **index == process.tool_index)
                            .map(|(id, _)| id.clone())
                            .collect::<Vec<_>>();
                        while let Some(parent) = parents.pop() {
                            let children = self
                                .tool_parents
                                .iter()
                                .filter(|(_, recorded)| **recorded == parent)
                                .map(|(child, _)| child.clone())
                                .collect::<Vec<_>>();
                            for child in children {
                                self.tool_parents.remove(&child);
                                parents.push(child.clone());
                                if let Some(index) = self.tools.get(&child)
                                    && let Some(Operation { execution, .. }) =
                                        self.order.call_mut(*index)
                                {
                                    execution.transition(
                                        ExecutionState::Complete,
                                        watch.last_event_at.unwrap_or(event.created_at),
                                    );
                                }
                            }
                        }
                    }
                }
            }
            SessionEventKind::GoalUpdated { goal } => {
                self.goal = Some(goal.clone());
                return self.upsert_goal(goal.clone(), local_event_time(event), false);
            }
            SessionEventKind::GoalCleared { .. } => {
                self.goal = None;
                self.order.push(TranscriptEntry::Activity {
                    text: "goal cleared".to_string(),
                    time: local_event_time(event),
                });
            }
            SessionEventKind::PlanUpdated { items } => {
                if self.plan_workspace_revision.is_some() {
                    return None;
                }
                self.todos = items.clone();
                if items.is_empty()
                    && !self
                        .order
                        .iter()
                        .any(|entry| matches!(entry, TranscriptEntry::Plan { .. }))
                {
                    return None;
                }
                return self.upsert_plan(items.clone(), local_event_time(event));
            }
            SessionEventKind::PlanProjected {
                participant_id,
                items,
                workspace_revision,
                ..
            } => {
                let stale = self.plan_participant_id == Some(*participant_id)
                    && self
                        .plan_workspace_revision
                        .is_some_and(|current| *workspace_revision < current);
                if stale {
                    return None;
                }
                self.seed_plan(items, Some(*participant_id), Some(*workspace_revision));
                if items.is_empty()
                    && !self
                        .order
                        .iter()
                        .any(|entry| matches!(entry, TranscriptEntry::Plan { .. }))
                {
                    return None;
                }
                // The card moves to where the plan changed, as a legacy update
                // did; left in place it updates hours of scrollback above.
                return self.upsert_plan(items.clone(), local_event_time(event));
            }
            SessionEventKind::AgentPlanProjected {
                session_id,
                participant_id,
                items,
                workspace_revision,
            } => {
                if self.agent_plans.get(session_id).is_none_or(|old| {
                    *participant_id != old.participant_id
                        || *workspace_revision >= old.workspace_revision
                }) {
                    self.agent_plans.insert(
                        *session_id,
                        borg_agent_runtime::AgentPlanProjection {
                            participant_id: *participant_id,
                            items: items.clone(),
                            workspace_revision: *workspace_revision,
                        },
                    );
                }
                return None;
            }
            SessionEventKind::RuntimeProcessStarted {
                process_id,
                pid,
                command,
                ..
            } => {
                let tool_index = self
                    .foreground_tool
                    .as_ref()
                    .and_then(|tool_call_id| self.tools.get(tool_call_id))
                    .copied();
                self.runtime_processes.insert(
                    *process_id,
                    RuntimeProcessProjection {
                        command: command.clone(),
                        pid: *pid,
                        tool_index,
                        running: true,
                    },
                );
            }
            SessionEventKind::RuntimeProcessCompleted {
                process_id,
                stdout,
                stderr,
                changes,
                exit_code,
                status,
                timed_out,
                error,
                ..
            } => {
                let process_state = if *status == RuntimeProcessStatus::Terminated {
                    ExecutionState::Stopped
                } else if *timed_out
                    || error.is_some()
                    || exit_code.is_some_and(|code| code != 0)
                    || matches!(
                        status,
                        RuntimeProcessStatus::TimedOut
                            | RuntimeProcessStatus::Failed
                            | RuntimeProcessStatus::Orphaned
                    )
                {
                    ExecutionState::Failed
                } else {
                    ExecutionState::Complete
                };
                let edit_origin = self.originating_tool_call_id(*process_id);
                let tool_index = self
                    .runtime_processes
                    .get(process_id)
                    .and_then(|process| process.tool_index);
                if let Some(process) = self.runtime_processes.get_mut(process_id) {
                    process.running = false;
                    if !self.finished_process_order.contains(process_id) {
                        self.finished_process_order.push_back(*process_id);
                    }
                }
                self.prune_finished_processes();
                if let Some(tool_index) = tool_index
                    && !self
                        .runtime_processes
                        .values()
                        .any(|process| process.running && process.tool_index == Some(tool_index))
                    && self
                        .order
                        .get(tool_index)
                        .and_then(TranscriptEntry::execution)
                        .is_some_and(|execution| !execution.is_terminal())
                    && let Some(Operation {
                        execution,
                        output: output_view,
                        ..
                    }) = self.order.call_mut(tool_index)
                    && !execution.is_terminal()
                {
                    let output = [stdout.as_str(), stderr.as_str()]
                        .into_iter()
                        .filter(|text| !text.is_empty())
                        .collect::<Vec<_>>()
                        .join("\n");
                    if !output.trim().is_empty() {
                        *output_view = Some(OperationView::new("text".to_string(), output));
                    }
                    execution.transition(process_state, event.created_at);
                }
                if let Some(polls) = self.poll_processes.remove(process_id) {
                    for tool_call_id in polls {
                        if let Some(index) = self.tools.get(&tool_call_id).copied()
                            && self
                                .order
                                .get(index)
                                .and_then(TranscriptEntry::execution)
                                .is_some_and(|execution| !execution.is_terminal())
                            && let Some(Operation { execution, .. }) = self.order.call_mut(index)
                        {
                            execution.transition(process_state, event.created_at);
                        }
                    }
                }
                if !changes.is_empty()
                    && let Some(tool_call_id) = edit_origin
                    && let Some(edit) = command_edit_presentation(
                        "exec",
                        &serde_json::json!({"changes": changes}).to_string(),
                    )
                {
                    self.upsert_command_edit(
                        &tool_call_id,
                        edit,
                        local_event_time(event),
                        event.created_at,
                        None,
                    );
                }
            }
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if Self::provider_reasoning_lifecycle(kind, payload) == Some(true) =>
            {
                self.start_reasoning(event.created_at, local_event_time(event));
                if let (Some(id), Some(index)) = (
                    payload
                        .pointer("/item/id")
                        .and_then(serde_json::Value::as_str),
                    self.active_reasoning,
                ) {
                    self.tools.insert(format!("reasoning:{id}"), index);
                }
            }
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if Self::provider_reasoning_lifecycle(kind, payload) == Some(false) =>
            {
                self.finish_reasoning(event.created_at);
            }
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if kind == "mcp_server_unavailable" =>
            {
                self.order.push(TranscriptEntry::Info {
                    title: "MCP unavailable".to_string(),
                    text: payload["message"]
                        .as_str()
                        .unwrap_or("External MCP tools unavailable for this turn")
                        .to_string(),
                    time: local_event_time(event),
                });
            }
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if kind == "context_replay_projected" =>
            {
                // Truncation is silent bulk reduction; dropping whole messages
                // changes what the model can see, so say so. The journal keeps
                // the record either way.
                let omitted = payload
                    .get("messages_omitted")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                if omitted > 0 {
                    let before = payload
                        .get("context_chars_before")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0);
                    let after = payload
                        .get("context_chars_after")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0);
                    self.order.push(TranscriptEntry::Info {
                        title: "Older history omitted".to_string(),
                        text: format!(
                            "{omitted} older messages were left out of this provider request to \
                             fit its budget ({before} → {after} characters); the durable journal \
                             retains them."
                        ),
                        time: local_event_time(event),
                    });
                }
            }
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if kind == "context_compaction_failed" =>
            {
                if !self.has_active_reasoning_block() {
                    self.finish_reasoning(event.created_at);
                }
                self.cache_diagnostics.reset();
                if let Some(index) = self.legacy_compaction_index()
                    && let Some(TranscriptEntry::Compaction { execution, .. }) =
                        self.order.get_mut(index)
                    && !execution.is_terminal()
                {
                    execution.transition(ExecutionState::Failed, event.created_at);
                }
                // Compaction failing used to leave nothing on screen, so the
                // history it was protecting could be dropped by the backstop
                // with no visible cause. Say what happened and what it cost.
                let detail = payload
                    .get("error")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("the provider did not return a summary");
                let text = if payload
                    .get("history_preserved")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false)
                {
                    format!(
                        "Context compaction could not run, so this turn kept its full \
                         history and failed on the underlying cause instead of \
                         discarding messages: {detail}"
                    )
                } else {
                    format!("Context compaction could not run: {detail}")
                };
                self.order.push(TranscriptEntry::Info {
                    title: "Compaction failed".to_string(),
                    text,
                    time: local_event_time(event),
                });
            }
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if is_context_compaction(kind) =>
            {
                if !self.has_active_reasoning_block() {
                    self.finish_reasoning(event.created_at);
                }
                self.cache_diagnostics.reset();
                let started = context_compaction_started(kind, payload);
                let progress = payload
                    .get("status")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|status| status.eq_ignore_ascii_case("progress"));
                if started || progress {
                    let progress_summary = context_compaction_progress_summary(kind, payload);
                    if let Some(index) = self.legacy_compaction_index()
                        && let Some(TranscriptEntry::Compaction {
                            summary,
                            time,
                            sequence,
                            expanded,
                            execution,
                        }) = self.order.get_mut(index)
                        && !execution.is_terminal()
                    {
                        *summary = progress_summary.to_string();
                        *time = local_event_time(event);
                        *sequence = event.sequence;
                        *expanded = false;
                    } else {
                        self.order.push(TranscriptEntry::Compaction {
                            summary: progress_summary.to_string(),
                            time: local_event_time(event),
                            sequence: event.sequence,
                            expanded: false,
                            execution: Execution::new(event.created_at, ExecutionState::Running),
                        });
                    }
                } else {
                    let summary = context_compaction_card_summary(payload);
                    if let Some(index) = self.legacy_compaction_index()
                        && let Some(TranscriptEntry::Compaction {
                            summary: previous,
                            time,
                            sequence,
                            execution,
                            expanded,
                        }) = self.order.get_mut(index)
                        && !execution.is_terminal()
                    {
                        *previous = summary;
                        *time = local_event_time(event);
                        *sequence = event.sequence;
                        execution.transition(ExecutionState::Complete, event.created_at);
                        *expanded = false;
                    } else if !matches!(
                        self.order.last(),
                        Some(TranscriptEntry::Compaction {
                            summary: previous,
                            execution,
                            ..
                        }) if execution.is_terminal() && previous == &summary
                    ) {
                        self.order.push(TranscriptEntry::Compaction {
                            summary,
                            time: local_event_time(event),
                            sequence: event.sequence,
                            expanded: false,
                            execution: Execution::new(event.created_at, ExecutionState::Complete),
                        });
                    }
                }
            }
            SessionEventKind::TeamBroadcastUpdated {
                message_id,
                text,
                recipient_ids,
                acknowledged,
                steered,
            } => {
                let total = recipient_ids.len();
                let detail = if *steered {
                    format!("steer sent to {total} active subagents")
                } else {
                    format!("sent · {acknowledged}/{total} acknowledged")
                };
                let state =
                    if *steered || usize::try_from(*acknowledged).unwrap_or(usize::MAX) >= total {
                        ExecutionState::Complete
                    } else {
                        ExecutionState::Waiting
                    };
                if let Some(index) = self.team_broadcast_entries.get(message_id).copied() {
                    if self
                        .order
                        .get(index)
                        .and_then(TranscriptEntry::execution)
                        .is_some_and(Execution::is_terminal)
                    {
                        return None;
                    }
                    if let Some(Operation {
                        detail: row_detail,
                        execution,
                        time: row_time,
                        ..
                    }) = self.order.lifecycle_mut(index)
                    {
                        if !execution.transition(state, event.created_at) {
                            return None;
                        }
                        *row_detail = detail;
                        *row_time = local_event_time(event);
                    }
                } else {
                    let index = self.order.len();
                    self.order.push(TranscriptEntry::Operation(Operation {
                        expanded: true,
                        ..Operation::lifecycle(
                            OperationKind::Agent,
                            "Team",
                            detail,
                            Some(text.clone()),
                            local_event_time(event),
                            Execution::new(event.created_at, state),
                        )
                    }));
                    self.team_broadcast_entries.insert(*message_id, index);
                }
            }
            SessionEventKind::AgentMessageReceived {
                message_id,
                sender_id,
                sender_name,
                text,
                from_child,
                sender_role,
            } => {
                removed_entry = self.remove_message(*message_id);
                self.queued_messages.remove(message_id);
                self.queued_message_sequences.remove(message_id);
                let mut row_created = false;
                if self.remember_agent_message(*message_id) {
                    self.agent_message_senders.insert(*sender_id);

                    let label = self.agent_message_label(*sender_id, *sender_role, *from_child);
                    self.order
                        .push(TranscriptEntry::Operation(Operation::lifecycle(
                            OperationKind::Agent,
                            label,
                            if sender_name.trim().is_empty() {
                                self.agent_names
                                    .get(sender_id)
                                    .cloned()
                                    .unwrap_or_else(|| "agent".to_string())
                            } else {
                                sender_name.clone()
                            },
                            Some(text.clone()),
                            local_event_time(event),
                            Execution::new(event.created_at, ExecutionState::Received),
                        )));
                    row_created = true;
                }
                // Duplicate receipts have no new row to own their source metadata.
                if !row_created {
                    self.team_report_sources.remove(&event.id);
                    self.team_report_roles.remove(&event.id);
                }
            }
            SessionEventKind::SubagentActivity {
                activity,
                agent,
                event: child_event,
            } => {
                let snapshot =
                    subagent_snapshot_for_activity(*activity, agent, child_event.as_deref());
                self.reconcile_subagent_snapshot(&snapshot, true);
                if let Some(child) = child_event {
                    self.observe_child_approval(child);
                }
            }
            SessionEventKind::Error { message } => {
                self.finish_reasoning(event.created_at);
                self.order
                    .push(TranscriptEntry::Operation(Operation::lifecycle(
                        OperationKind::Error,
                        "Error",
                        compact_text(message, 180),
                        Some(message.clone()),
                        local_event_time(event),
                        Execution::new(event.created_at, ExecutionState::Failed),
                    )))
            }
            _ => {}
        }
        removed_entry
    }

    pub fn apply_history(&mut self, event: &SessionEvent) -> Option<usize> {
        self.order.event_identity = Some((event.id, 0));
        let removed = self.apply_event(event, ApplyMode::History);
        self.order.event_identity = None;
        self.prune_child_metadata();
        removed
    }

    fn apply_compaction_operation(
        &mut self,
        operation_id: Uuid,
        kind: &str,
        payload: &serde_json::Value,
        event: &SessionEvent,
        mode: ApplyMode,
    ) {
        let started = context_compaction_started(kind, payload);
        let progress = payload
            .get("status")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|status| status.eq_ignore_ascii_case("progress"));
        let failed = kind == "context_compaction_failed";
        let state = if failed {
            ExecutionState::Failed
        } else if started || progress {
            ExecutionState::Running
        } else {
            ExecutionState::Complete
        };
        let existing = self.compaction_entries.get(&operation_id).copied();
        if let Some(index) = existing {
            if self
                .order
                .get(index)
                .and_then(TranscriptEntry::execution)
                .is_none_or(Execution::is_terminal)
            {
                return;
            }
        } else if (progress && mode == ApplyMode::Live) || (started && self.live_turn_closed) {
            return;
        }
        let summary = if failed {
            let detail = payload
                .get("error")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("the provider did not return a summary");
            if payload
                .get("history_preserved")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
            {
                format!("Context compaction failed; history preserved: {detail}")
            } else {
                format!("Context compaction failed: {detail}")
            }
        } else if state == ExecutionState::Running {
            context_compaction_progress_summary(kind, payload)
        } else {
            context_compaction_card_summary(payload)
        };
        if existing.is_none() && started {
            self.finish_reasoning(event.created_at);
            self.cache_diagnostics.reset();
        }
        if let Some(index) = existing {
            if let Some(TranscriptEntry::Compaction {
                execution,
                summary: stored,
                time,
                sequence,
                expanded,
            }) = self.order.get_mut(index)
            {
                execution.transition(state, event.created_at);
                *stored = summary;
                *time = local_event_time(event);
                *sequence = event.sequence;
                *expanded = false;
            }
            return;
        }
        if started {
            // A new operation supersedes an abandoned live attempt; retries
            // within one operation keep its original execution and clock.
            let previous = self
                .compaction_entries
                .values()
                .copied()
                .collect::<Vec<_>>();
            for index in previous {
                if self
                    .order
                    .get(index)
                    .and_then(TranscriptEntry::execution)
                    .is_some_and(|execution| !execution.is_terminal())
                    && let Some(TranscriptEntry::Compaction { execution, .. }) =
                        self.order.get_mut(index)
                {
                    execution.transition(ExecutionState::Stopped, event.created_at);
                }
            }
        }
        let index = self.order.len();
        self.order.push(TranscriptEntry::Compaction {
            summary,
            time: local_event_time(event),
            sequence: event.sequence,
            expanded: false,
            execution: Execution::new(event.created_at, state),
        });
        self.order.identities[index] = (operation_id, 0);
        self.compaction_entries.insert(operation_id, index);
    }

    pub fn apply_reasoning_snapshot(&mut self, payload: &serde_json::Value, event: &SessionEvent) {
        let (Some(id), Some(text)) = (
            payload
                .get("reasoning_id")
                .and_then(serde_json::Value::as_str),
            payload.get("text").and_then(serde_json::Value::as_str),
        ) else {
            return;
        };
        let correlated = payload
            .get("reasoning_block")
            .and_then(serde_json::Value::as_bool)
            == Some(true);
        let finalized = payload.get("complete").and_then(serde_json::Value::as_bool) == Some(true);
        let block_id = if correlated {
            let Ok(id) = Uuid::parse_str(id) else {
                return;
            };
            Some(id)
        } else {
            None
        };
        let key = format!("reasoning:{id}");
        let existing = if let Some(id) = block_id {
            self.reasoning_blocks.get(&id).copied()
        } else {
            self.tools.get(&key).copied()
        };
        let started_at = || {
            payload
                .get("started_at")
                .and_then(serde_json::Value::as_str)
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .map(|value| value.with_timezone(&Utc))
                .unwrap_or(event.created_at)
        };
        let index = if let Some(index) = existing {
            index
        } else if let Some(id) = block_id {
            if !finalized {
                return;
            }
            let mut execution = Execution::new(started_at(), ExecutionState::Running);
            execution.transition(ExecutionState::Complete, event.created_at);
            let index = self.order.len();
            self.order
                .push(TranscriptEntry::Operation(Operation::reasoning(
                    "Reasoned",
                    reasoning_preview(text),
                    text.into(),
                    started_at().to_rfc3339(),
                    execution,
                )));
            self.order.identities[index] = (id, 0);
            self.reasoning_blocks.insert(id, index);
            return;
        } else {
            if self.live_turn_closed || self.has_active_reasoning_block() {
                return;
            }
            self.start_reasoning(started_at(), local_event_time(event));
            let Some(index) = self.active_reasoning else {
                return;
            };
            self.tools.insert(key, index);
            index
        };
        if self
            .order
            .get(index)
            .and_then(TranscriptEntry::execution)
            .is_none_or(Execution::is_terminal)
        {
            if finalized {
                self.hydrate_reasoning_payload(index, text);
            }
            return;
        }
        if let Some(Operation {
            execution,
            kind: OperationKind::Reasoning,
            input: Some(OperationView { text: source, .. }),
            detail,
            label: name,
            expanded,
            ..
        }) = self.order.call_mut(index)
        {
            if finalized {
                source.clear();
                source.push_str(text);
            } else {
                Self::merge_reasoning_snapshot(source, text);
            }
            *detail = reasoning_preview(source);
            if finalized {
                *name = "Reasoned".into();
                execution.transition(ExecutionState::Complete, event.created_at);
                *expanded = false;
            }
        }
        if finalized && self.active_reasoning == Some(index) {
            self.active_reasoning = None;
            self.reasoning_has_preview = false;
        }
    }

    fn has_active_reasoning_block(&self) -> bool {
        self.active_reasoning
            .is_some_and(|active| self.reasoning_blocks.values().any(|index| *index == active))
    }

    fn legacy_compaction_index(&self) -> Option<usize> {
        let index = self.order.len().checked_sub(1)?;
        (!self.compaction_entries.values().any(|stored| *stored == index)
            && matches!(&self.order[index], TranscriptEntry::Compaction { execution, .. } if !execution.is_terminal())).then_some(index)
    }

    // Canonical archived content can arrive after the execution boundary. This
    // loads the stored payload only; it cannot alter the receipt or its clock.
    fn hydrate_reasoning_payload(&mut self, index: usize, text: &str) {
        if let Some(Operation {
            kind: OperationKind::Reasoning,
            input: Some(OperationView { text: source, .. }),
            ..
        }) = self.order.call_mut(index)
        {
            source.clear();
            source.push_str(text);
        }
    }

    pub fn background_foreground_process(&mut self) {
        let Some(tool_call_id) = self.foreground_tool.as_deref() else {
            return;
        };
        let Some(index) = self.tools.get(tool_call_id).copied() else {
            return;
        };
        let has_running_process = self
            .runtime_processes
            .values()
            .any(|process| process.running && process.tool_index == Some(index))
            || self
                .provider_backgrounds
                .values()
                .any(|process| process.tool_index == index);
        if !has_running_process {
            return;
        }
        self.foreground_tool = None;
        if let Some(Operation { execution, .. }) = self.order.call_mut(index)
            && !execution.is_terminal()
            && !execution.is_failed()
            && !execution.is_interrupted()
        {
            execution.transition(ExecutionState::Backgrounded, execution.started_at());
        }
    }

    pub fn cancel_preparing_tool(&mut self, preparing_id: &str, completed_at: DateTime<Utc>) {
        if self.foreground_tool.as_deref() == Some(preparing_id) {
            self.foreground_tool = None;
        }
        if let Some(index) = self.tools.get(preparing_id).copied()
            && self
                .order
                .get(index)
                .and_then(TranscriptEntry::execution)
                .is_some_and(|execution| !execution.is_terminal())
            && let Some(Operation { execution, .. }) = self.order.call_mut(index)
        {
            execution.transition(ExecutionState::Stopped, completed_at);
        }
    }

    fn resolve_interaction(&mut self, key: &str, state: ExecutionState, at: DateTime<Utc>) {
        if let Some(index) = self.interaction_entries.get(key)
            && self
                .order
                .get(*index)
                .and_then(TranscriptEntry::execution)
                .is_some_and(|execution| !execution.is_terminal())
            && let Some(Operation { execution, .. }) = self.order.lifecycle_mut(*index)
        {
            execution.transition(state, at);
        }
    }

    pub fn clear_visible_entries(&mut self) {
        self.order.clear();
        self.messages.clear();
        self.tools.clear();
        self.reasoning_blocks.clear();
        self.tool_inspection_outputs.clear();
        self.command_edit_rows.clear();
        self.subagent_entries.clear();
        self.agent_messages.clear();
        self.agent_message_order.clear();
        self.team_broadcast_entries.clear();
        self.interaction_entries.clear();
        self.compaction_entries.clear();
        self.poll_processes.clear();
        self.agent_message_senders.clear();
        self.watch_messages.clear();
        self.watch_message_order.clear();
        self.queued_messages.clear();
        self.queued_message_sequences.clear();
        self.tool_run_offsets.clear();
        self.expanded_tool_runs.clear();
        self.action_run_expansions.clear();
        self.active_reasoning = None;
        self.reasoning_has_preview = false;
        self.last_edit = None;

        self.selected = None;
        self.follow_tail = true;
        // The assignment row is gone, so nothing visible can carry the badge.
        // Staying armed would hand it to the next prompt the operator types.
        self.director_prompt = DirectorPrompt::Unknown;
    }

    pub fn collapse_previous_edit(&mut self) {
        if let Some(Operation { expanded, .. }) =
            self.last_edit.and_then(|index| self.order.call_mut(index))
        {
            *expanded = false;
        }
    }

    pub fn complete_preparing_tool(&mut self, tool_call_id: &str) {
        self.promote_preparing_tool_if_untracked(tool_call_id);
        let Some(tool_index) = self.tools.get(tool_call_id).copied() else {
            return;
        };
        if self
            .order
            .get(tool_index)
            .and_then(TranscriptEntry::execution)
            .is_none_or(Execution::is_terminal)
        {
            return;
        }
        if let Some(operation) = self.order.call_mut(tool_index)
            && operation.kind == OperationKind::Preparation
            && !operation.execution.is_terminal()
        {
            operation.source = "action".to_string();
            operation.reclassify();
            // The preparation's `{"label": ...}` only repeats the title; the
            // real input, if it ever arrives, replaces it on completion.
            operation.input = None;
            if !operation.detail.is_empty() {
                operation.label = format!("Run {}", operation.detail);
                operation.detail.clear();
            } else {
                let label = operation
                    .label
                    .strip_prefix("Generate ")
                    .or_else(|| operation.label.strip_prefix("Prepare "))
                    .unwrap_or("command")
                    .to_string();
                operation.label = format!("Run {label}");
            }
        }
    }

    pub fn finish_live_assistant_messages(
        &mut self,
        completed_at: DateTime<Utc>,
        user_interrupted_turn: bool,
        redirected_turn: bool,
    ) {
        for index in 0..self.order.len() {
            if !matches!(&self.order[index], TranscriptEntry::Message { actor: EventActor::Assistant, execution, .. } if !execution.is_terminal())
            {
                continue;
            }
            let entry = self.order.get_mut(index).expect("filtered entry");
            let TranscriptEntry::Message {
                actor: EventActor::Assistant,
                status,
                execution,
                redirected,
                ..
            } = entry
            else {
                continue;
            };
            if execution.is_terminal() {
                continue;
            }
            execution.transition(
                if user_interrupted_turn {
                    ExecutionState::Stopped
                } else if *status == MessageStatus::Failed {
                    ExecutionState::Failed
                } else {
                    ExecutionState::Complete
                },
                completed_at,
            );
            *status = MessageStatus::Complete;
            // The stream stopped mid-sentence. Settling the row silently would
            // present the fragment as the assistant's finished answer.
            *redirected = redirected_turn;
        }
        self.finish_reasoning(completed_at);
    }

    pub fn finish_preparing_tool(&mut self, preparing_id: &str, completed_at: DateTime<Utc>) {
        if self.foreground_tool.as_deref() == Some(preparing_id) {
            self.foreground_tool = None;
        }
        let Some(index) = self.tools.get(preparing_id).copied() else {
            return;
        };
        if self
            .order
            .get(index)
            .and_then(TranscriptEntry::execution)
            .is_none_or(Execution::is_terminal)
        {
            return;
        }
        if let Some(Operation { execution, .. }) = self.order.call_mut(index) {
            execution.transition(ExecutionState::Complete, completed_at);
        }
    }

    pub fn finish_reasoning(&mut self, completed_at: DateTime<Utc>) {
        let Some(index) = self.active_reasoning.take() else {
            return;
        };
        self.reasoning_has_preview = false;
        if self
            .order
            .get(index)
            .and_then(TranscriptEntry::execution)
            .is_none_or(Execution::is_terminal)
        {
            return;
        }
        if let Some(Operation {
            execution,
            label: name,
            expanded,
            ..
        }) = self.order.call_mut(index)
            && !execution.is_terminal()
        {
            // The row reads Reasoning while it runs and Reasoned once it ends,
            // so the label matches the tick beside it.
            *name = "Reasoned".to_string();
            execution.transition(ExecutionState::Complete, completed_at);
            *expanded = false;
        }
    }

    fn tool_has_live_process(&self, index: usize) -> bool {
        self.runtime_processes
            .values()
            .any(|process| process.running && process.tool_index == Some(index))
            || self
                .provider_backgrounds
                .values()
                .any(|process| process.tool_index == index)
    }

    fn live_process_tool_indices(&self) -> HashSet<usize> {
        self.runtime_processes
            .values()
            .filter(|process| process.running)
            .filter_map(|process| process.tool_index)
            .chain(
                self.provider_backgrounds
                    .values()
                    .map(|process| process.tool_index),
            )
            .collect()
    }

    pub fn finish_running_tools(
        &mut self,
        completed_at: DateTime<Utc>,
        failed: bool,
        error_detail: &str,
    ) {
        self.finish_reasoning(completed_at);
        self.foreground_tool = None;
        self.preparing_tools.clear();
        self.unkeyed_preparing_tools.clear();
        let active_background_tools = self.live_process_tool_indices();
        for index in 0..self.order.len() {
            if !self
                .order
                .call(index)
                .is_some_and(|call| !call.execution.is_terminal())
            {
                continue;
            }
            if let Some(Operation {
                execution, detail, ..
            }) = self.order.call_mut(index)
                && !execution.is_terminal()
            {
                execution.transition(
                    if active_background_tools.contains(&index) {
                        ExecutionState::Backgrounded
                    } else if failed {
                        ExecutionState::Failed
                    } else {
                        ExecutionState::Complete
                    },
                    completed_at,
                );
                if failed && !error_detail.trim().is_empty() {
                    *detail = format!(
                        "{detail} · {}",
                        compact_text(error_detail.lines().next().unwrap_or_default(), 120)
                    );
                }
            }
        }
    }

    pub fn flush_streaming_tails(&mut self) {
        self.streaming_last_release.clear();
        for (message_id, tail) in std::mem::take(&mut self.streaming_tails) {
            if let Some(index) = self.messages.get(&message_id).copied()
                && let Some(TranscriptEntry::Message { text, .. }) = self.order.get_mut(index)
            {
                text.push_str(&tail);
            }
        }
    }

    pub fn has_preparing_tool(&self, tool_call_id: &str) -> bool {
        self.preparing_tools.contains_key(tool_call_id) || !self.unkeyed_preparing_tools.is_empty()
    }

    pub fn hold_back_unfinished(&mut self, message_id: Uuid) {
        if self.response_streaming != crate::policy::ResponseStreaming::Paragraph {
            return;
        }
        let Some(index) = self.messages.get(&message_id).copied() else {
            return;
        };
        if let Some(TranscriptEntry::Message {
            actor: EventActor::Assistant,
            text,
            status: MessageStatus::InProgress,
            ..
        }) = self.order.get_mut(index)
        {
            let tail = text.split_off(crate::streaming::paragraph_release(text));
            if !tail.is_empty() {
                self.streaming_tails.insert(message_id, tail);
            }
        }
    }

    pub fn is_redundant_process_poll(&self, name: &str, input: &serde_json::Value) -> bool {
        // Sending input to or stopping a process are actions in their own
        // right, and a terminal call is the command itself.
        if input
            .get("chars")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|c| !c.is_empty())
        {
            return false;
        }
        if input.get("terminate").and_then(serde_json::Value::as_bool) == Some(true) {
            return false;
        }
        tool_process_followup_handle(name, Some(input))
            .and_then(|handle| Uuid::parse_str(&handle).ok())
            .is_some_and(|process_id| self.runtime_processes.contains_key(&process_id))
    }

    pub fn late_user_message_insertion_index(&self) -> usize {
        let output_start = self
            .order
            .iter()
            .rposition(|entry| {
                matches!(
                    entry,
                    TranscriptEntry::Message {
                        actor: EventActor::User,
                        ..
                    }
                )
            })
            .and_then(|last_user| {
                self.order
                    .iter()
                    .skip(last_user + 1)
                    .position(transcript_entry_is_turn_output)
                    .map(|offset| last_user + 1 + offset)
            });
        output_start.unwrap_or_else(|| {
            self.order
                .iter()
                .position(transcript_entry_is_turn_output)
                .unwrap_or(self.order.len())
        })
    }

    pub fn mark_running_tools_user_interrupted(&mut self, completed_at: DateTime<Utc>) {
        self.finish_reasoning(completed_at);
        self.foreground_tool = None;
        self.preparing_tools.clear();
        self.unkeyed_preparing_tools.clear();
        let active_background_tools = self.live_process_tool_indices();
        for index in 0..self.order.len() {
            if !self
                .order
                .call(index)
                .is_some_and(|call| !call.execution.is_terminal())
            {
                continue;
            }
            if let Some(Operation { execution, .. }) = self.order.call_mut(index)
                && !execution.is_terminal()
            {
                execution.transition(
                    if active_background_tools.contains(&index) {
                        ExecutionState::Backgrounded
                    } else {
                        ExecutionState::Stopped
                    },
                    completed_at,
                );
            }
        }
    }

    pub fn merge_reasoning_snapshot(source: &mut String, incoming: &str) {
        if incoming.is_empty() || incoming == source {
            return;
        }
        // Most providers emit deltas, but subscription adapters may replay a
        // cumulative reasoning snapshot at a lifecycle boundary. Reconcile
        // that snapshot instead of appending the already-rendered prefix.
        if incoming.starts_with(source.as_str()) {
            source.clear();
            source.push_str(incoming);
        } else if source.starts_with(incoming) {
            // An older snapshot arrived after a newer one during reconnect.
        } else {
            let overlap = longest_suffix_prefix_overlap(source, incoming);
            source.push_str(&incoming[overlap..]);
        }
    }

    pub fn message_event_time(&mut self, event: &SessionEvent) -> String {
        event.created_at.to_rfc3339()
    }

    pub fn observe_model_request(&mut self, event: &SessionEvent, live: bool) {
        match &event.kind {
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if kind == "native_model_request" =>
            {
                if let Some(id) = payload
                    .get("request_id")
                    .and_then(serde_json::Value::as_str)
                    && self
                        .model_request
                        .as_ref()
                        .is_none_or(|request| request.id != id)
                {
                    self.model_request = Some(ModelRequestDisplay {
                        id: id.to_string(),
                        started_at: event.created_at,
                        summary_requested: payload
                            .pointer("/reasoning/summary")
                            .and_then(serde_json::Value::as_str)
                            .is_some_and(|summary| summary != "none"),
                        last_received_at: None,
                        output_tokens: None,
                    });
                }
            }
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if kind == "native_model_usage" =>
            {
                if let Some(request) = self.model_request.as_mut()
                    && payload
                        .get("request_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(request.id.as_str())
                {
                    if live {
                        request.last_received_at = Some(Utc::now());
                    }
                    if let Some(tokens) = payload
                        .pointer("/usage/output_tokens")
                        .and_then(serde_json::Value::as_u64)
                    {
                        request.output_tokens = Some(tokens);
                    }
                    if payload.get("complete").and_then(serde_json::Value::as_bool) == Some(true) {
                        self.model_request = None;
                    }
                }
            }
            SessionEventKind::ToolStarted { .. }
            | SessionEventKind::TurnCompleted { .. }
            | SessionEventKind::TurnStarted { .. }
            | SessionEventKind::ContextCleared
            | SessionEventKind::StatusChanged {
                status:
                    SessionStatus::Ready
                    | SessionStatus::Stopped
                    | SessionStatus::Failed
                    | SessionStatus::Completed,
                ..
            } => {
                self.model_request = None;
            }
            _ => {
                let progress = matches!(
                    &event.kind,
                    SessionEventKind::MessageDelta { .. }
                        | SessionEventKind::ReasoningDelta { .. }
                        | SessionEventKind::ReasoningTextDelta { .. }
                        | SessionEventKind::ReasoningCompleted
                ) || matches!(&event.kind, SessionEventKind::ProviderEvent { kind, payload, .. }
                        if kind == "reasoning_snapshot" || Self::provider_reasoning_lifecycle(kind, payload).is_some() || is_live_tool_call_event(kind));
                if live
                    && progress
                    && let Some(request) = self.model_request.as_mut()
                {
                    request.last_received_at = Some(Utc::now());
                }
            }
        }
    }

    /// Keep the finished-process record within its fixed budget. Also run on
    /// checkpoint restore, so a session whose saved state already outgrew the
    /// budget recovers instead of failing every later commit.
    pub fn prune_dead_report_sources(&mut self) {
        let rows = self
            .order
            .identities
            .iter()
            .map(|(event_id, _)| *event_id)
            .collect::<HashSet<_>>();
        self.team_report_sources
            .retain(|event_id, _| rows.contains(event_id));
        self.team_report_roles
            .retain(|event_id, _| rows.contains(event_id));
    }

    pub fn prune_finished_processes(&mut self) {
        while self.finished_process_order.len() > FINISHED_PROCESS_LIMIT {
            self.finished_process_order.pop_front();
        }
        let finished = &self.finished_process_order;
        self.runtime_processes
            .retain(|id, process| process.running || finished.contains(id));
        for process in self.runtime_processes.values_mut().filter(|p| !p.running) {
            if let Some((end, _)) = process
                .command
                .char_indices()
                .nth(FINISHED_PROCESS_COMMAND_CHARS)
            {
                process.command.truncate(end);
            }
        }
    }

    pub fn originating_tool_call_id(&self, process_id: Uuid) -> Option<String> {
        let origin = self.runtime_processes.get(&process_id)?.tool_index?;
        self.tools
            .iter()
            .find_map(|(tool_call_id, index)| (*index == origin).then(|| tool_call_id.clone()))
    }

    pub fn promote_preparing_tool(&mut self, tool_call_id: &str) {
        let preparing_id = self.preparing_tools.remove(tool_call_id).or_else(|| {
            (!self.unkeyed_preparing_tools.is_empty())
                .then(|| self.unkeyed_preparing_tools.remove(0))
        });
        let Some(preparing_id) = preparing_id else {
            return;
        };
        let Some(tool_index) = self.tools.remove(&preparing_id) else {
            return;
        };
        self.tools.insert(tool_call_id.to_string(), tool_index);
        if self.foreground_tool.as_deref() == Some(preparing_id.as_str()) {
            self.foreground_tool = Some(tool_call_id.to_string());
        }
    }

    pub fn promote_preparing_tool_if_untracked(&mut self, tool_call_id: &str) {
        if !self.tools.contains_key(tool_call_id) {
            self.promote_preparing_tool(tool_call_id);
        }
    }

    pub fn provider_reasoning_lifecycle(kind: &str, payload: &serde_json::Value) -> Option<bool> {
        let (method, suffix) = kind
            .rsplit_once(':')
            .map_or((kind, None), |(method, suffix)| (method, Some(suffix)));
        let item_type = suffix
            .or_else(|| {
                payload
                    .pointer("/item/type")
                    .and_then(serde_json::Value::as_str)
            })
            .or_else(|| {
                payload
                    .pointer("/params/item/type")
                    .and_then(serde_json::Value::as_str)
            });
        let method = method.to_ascii_lowercase().replace(['.', '_', '-'], "/");
        let is_reasoning = item_type
            .is_some_and(|item_type| item_type.to_ascii_lowercase().contains("reasoning"))
            || method.contains("reasoning");
        if !is_reasoning {
            return None;
        }
        if method.ends_with("/started") || method.ends_with("/added") {
            Some(true)
        } else if method.ends_with("/completed") || method.ends_with("/done") {
            Some(false)
        } else {
            None
        }
    }

    pub fn queued_user_message_insertion_index(&self, sequence: u64) -> usize {
        self.order
            .iter()
            .enumerate()
            .find_map(|(index, entry)| {
                if !matches!(entry, TranscriptEntry::Message { .. }) {
                    return None;
                }
                let message_id = self
                    .messages
                    .iter()
                    .find_map(|(message_id, stored_index)| {
                        (*stored_index == index).then_some(message_id)
                    })?;
                (self
                    .queued_message_sequences
                    .get(message_id)
                    .is_some_and(|existing| *existing > sequence))
                .then_some(index)
            })
            .unwrap_or(self.order.len())
    }

    pub fn reindex_after_insertion(&mut self, index: usize) {
        self.pending_entry_insertions.push(index);

        for stored_index in self
            .messages
            .values_mut()
            .chain(self.tools.values_mut())
            .chain(self.reasoning_blocks.values_mut())
            .chain(self.command_edit_rows.values_mut())
            .chain(self.subagent_entries.values_mut())
            .chain(self.team_broadcast_entries.values_mut())
            .chain(self.interaction_entries.values_mut())
            .chain(self.compaction_entries.values_mut())
        {
            if *stored_index >= index {
                *stored_index += 1;
            }
        }
        for process in self.runtime_processes.values_mut() {
            if let Some(tool_index) = process.tool_index.as_mut()
                && *tool_index >= index
            {
                *tool_index += 1;
            }
        }
        for process in self.provider_backgrounds.values_mut() {
            if process.tool_index >= index {
                process.tool_index += 1;
            }
        }
        self.selected = self
            .selected
            .map(|selected| selected + usize::from(selected >= index));
        self.tool_run_offsets = self
            .tool_run_offsets
            .drain()
            .map(|(start, offset)| (start + usize::from(start >= index), offset))
            .collect();
        self.expanded_tool_runs = self
            .expanded_tool_runs
            .drain()
            .map(|start| start + usize::from(start >= index))
            .collect();
        self.action_run_expansions = self
            .action_run_expansions
            .drain()
            .map(|(start, expanded)| (start + usize::from(start >= index), expanded))
            .collect();
        self.active_reasoning = self
            .active_reasoning
            .map(|reasoning| reasoning + usize::from(reasoning >= index));
        self.last_edit = self.last_edit.map(|edit| edit + usize::from(edit >= index));
    }

    pub fn reindex_after_removal(&mut self, index: usize) {
        self.reasoning_blocks
            .retain(|_, stored_index| *stored_index != index);
        self.compaction_entries
            .retain(|_, stored_index| *stored_index != index);
        self.interaction_entries
            .retain(|_, stored_index| *stored_index != index);
        for stored_index in self
            .messages
            .values_mut()
            .chain(self.tools.values_mut())
            .chain(self.reasoning_blocks.values_mut())
            .chain(self.subagent_entries.values_mut())
            .chain(self.team_broadcast_entries.values_mut())
            .chain(self.interaction_entries.values_mut())
            .chain(self.compaction_entries.values_mut())
        {
            if *stored_index > index {
                *stored_index -= 1;
            }
        }
        self.command_edit_rows.retain(|_, stored_index| {
            if *stored_index == index {
                false
            } else {
                *stored_index -= usize::from(*stored_index > index);
                true
            }
        });
        for process in self.runtime_processes.values_mut() {
            process.tool_index = process.tool_index.and_then(|tool_index| {
                (tool_index != index).then_some(tool_index - usize::from(tool_index > index))
            });
        }
        self.provider_backgrounds.retain(|_, process| {
            if process.tool_index == index {
                false
            } else {
                process.tool_index -= usize::from(process.tool_index > index);
                true
            }
        });
        self.selected = self.selected.and_then(|selected| {
            if selected == index {
                None
            } else {
                Some(selected - usize::from(selected > index))
            }
        });
        self.tool_run_offsets = self
            .tool_run_offsets
            .drain()
            .filter_map(|(start, offset)| {
                (start != index).then_some((start - usize::from(start > index), offset))
            })
            .collect();
        self.expanded_tool_runs = self
            .expanded_tool_runs
            .drain()
            .filter_map(|start| (start != index).then_some(start - usize::from(start > index)))
            .collect();
        self.action_run_expansions = self
            .action_run_expansions
            .drain()
            .filter_map(|(start, expanded)| {
                (start != index).then_some((start - usize::from(start > index), expanded))
            })
            .collect();
        self.active_reasoning = self.active_reasoning.and_then(|reasoning| {
            if reasoning == index {
                None
            } else {
                Some(reasoning - usize::from(reasoning > index))
            }
        });
        if self.active_reasoning.is_none() {
            self.reasoning_has_preview = false;
        }
        self.last_edit = self.last_edit.and_then(|edit| {
            if edit == index {
                None
            } else {
                Some(edit - usize::from(edit > index))
            }
        });
    }

    pub fn release_streaming_tail(&mut self, message_id: Uuid) {
        self.streaming_last_release.remove(&message_id);
        let Some(tail) = self.streaming_tails.remove(&message_id) else {
            return;
        };
        let Some(index) = self.messages.get(&message_id).copied() else {
            return;
        };
        if let Some(TranscriptEntry::Message { text, .. }) = self.order.get_mut(index) {
            text.push_str(&tail);
        }
    }

    pub fn remove_message(&mut self, message_id: Uuid) -> Option<usize> {
        let index = self.messages.remove(&message_id)?;
        if self.director_prompt == DirectorPrompt::Row(message_id) {
            // The assignment is gone from view. Disarm rather than re-arm:
            // the next user row to arrive is the operator, not the director.
            self.director_prompt = DirectorPrompt::Unknown;
        }
        self.order.remove(index);
        self.reindex_after_removal(index);
        Some(index)
    }

    pub fn seed_plan(
        &mut self,
        items: &[PlanItem],
        participant_id: Option<Uuid>,
        revision: Option<u64>,
    ) -> bool {
        if self.plan_workspace_revision.is_some()
            && (revision.is_none()
                || (self.plan_participant_id == participant_id
                    && self
                        .plan_workspace_revision
                        .is_some_and(|current| revision.is_some_and(|new| new < current))))
        {
            return false;
        }
        let changed = self.todos != items
            || self.plan_participant_id != participant_id
            || (self.plan_workspace_revision.is_none() && revision.is_some());
        self.todos = items.to_vec();
        self.plan_workspace_revision = revision;
        self.plan_participant_id = participant_id;
        changed
    }

    pub fn settle_stale_preparations(&mut self, completed_at: DateTime<Utc>) {
        self.preparing_tools.clear();
        self.unkeyed_preparing_tools.clear();
        let foreground_index = self
            .foreground_tool
            .as_deref()
            .and_then(|tool_call_id| self.tools.get(tool_call_id).copied());
        for index in 0..self.order.len() {
            let Some(Operation {
                kind: OperationKind::Preparation,
                execution,
                ..
            }) = self.order.call_mut(index)
            else {
                continue;
            };
            if execution.is_terminal() {
                continue;
            }
            execution.transition(ExecutionState::Stopped, completed_at);
            // Terminal, but not a success. This sweep only reaches a
            // preparation whose tool call never ran, and leaving it to read
            // as a finished action is how a lost call would go unnoticed.
            if foreground_index == Some(index) {
                self.foreground_tool = None;
            }
        }
    }

    pub fn start_reasoning(&mut self, started_at: DateTime<Utc>, time: String) {
        if self.live_turn_closed {
            return;
        }
        if let Some(index) = self.active_reasoning {
            if matches!(
                self.order.call(index),
                Some(Operation {
                    kind: OperationKind::Reasoning,
                    execution,
                    ..
                }) if !execution.is_terminal()
            ) {
                return;
            }
            self.active_reasoning = None;
        }
        let index = self.order.len();
        self.order.push(TranscriptEntry::Operation(Operation {
            expanded: self.auto_expand_thinking,
            ..Operation::reasoning(
                "Reasoning",
                "",
                String::new(),
                time,
                Execution::new(started_at, ExecutionState::Running),
            )
        }));
        self.active_reasoning = Some(index);
        self.reasoning_has_preview = false;
    }

    pub fn stream_into_message(&mut self, message_id: Uuid, delta: &str) {
        let Some(index) = self.messages.get(&message_id).copied() else {
            return;
        };
        let Some(TranscriptEntry::Message {
            actor: EventActor::Assistant,
            text,
            status: MessageStatus::InProgress,
            ..
        }) = self.order.get_mut(index)
        else {
            return;
        };
        if self.response_streaming == crate::policy::ResponseStreaming::Paragraph {
            let now = Instant::now();
            let paced = self
                .streaming_last_release
                .get(&message_id)
                .is_some_and(|last| now.duration_since(*last) < PARAGRAPH_DELIVERY_INTERVAL);
            let tail = self.streaming_tails.entry(message_id).or_default();
            tail.push_str(delta);
            let release = crate::streaming::paragraph_release(&format!("{text}{tail}"));
            if release <= text.len() || paced {
                return;
            }
            let rest = tail.split_off(release - text.len());
            text.push_str(tail);
            *tail = rest;
            self.streaming_last_release.insert(message_id, now);
        } else {
            text.push_str(delta);
        }
    }

    pub fn update_action_generation_status(
        &mut self,
        provider_tool_id: Option<&str>,
        waiting: bool,
        label: &str,
    ) {
        if provider_tool_id.is_some_and(|tool_call_id| {
            self.tools.contains_key(tool_call_id)
                && !self.preparing_tools.contains_key(tool_call_id)
        }) {
            return;
        }
        let mapped = provider_tool_id
            .and_then(|provider_tool_id| self.preparing_tools.get(provider_tool_id).cloned());
        let preparing_id = mapped
            .clone()
            .or_else(|| self.unkeyed_preparing_tools.last().cloned());
        let Some(preparing_id) = preparing_id else {
            return;
        };
        let Some(index) = self.tools.get(&preparing_id).copied() else {
            return;
        };
        let Some(Operation {
            kind: OperationKind::Preparation,
            label: name,
            execution,
            ..
        }) = self.order.call_mut(index)
        else {
            return;
        };
        if execution.is_terminal() {
            return;
        }
        *name = if waiting {
            "Wait for provider".to_string()
        } else if label.is_empty() {
            "Generate".to_string()
        } else {
            format!("Generate {label}")
        };
        if mapped.is_none()
            && let Some(provider_tool_id) = provider_tool_id
        {
            self.unkeyed_preparing_tools
                .retain(|candidate| candidate != &preparing_id);
            self.preparing_tools
                .insert(provider_tool_id.to_string(), preparing_id);
        }
    }

    pub fn upsert_action_preparation(
        &mut self,
        event: &SessionEvent,
        label: &str,
        provider_tool_id: Option<&str>,
    ) {
        let existing = provider_tool_id
            .and_then(|provider_tool_id| {
                self.preparing_tools
                    .get(provider_tool_id)
                    .cloned()
                    .or_else(|| {
                        if self.unkeyed_preparing_tools.is_empty() {
                            return None;
                        }
                        let preparing_id = self.unkeyed_preparing_tools.remove(0);
                        self.preparing_tools
                            .insert(provider_tool_id.to_string(), preparing_id.clone());
                        Some(preparing_id)
                    })
            })
            .or_else(|| {
                provider_tool_id
                    .is_none()
                    .then(|| {
                        self.unkeyed_preparing_tools
                            .last()
                            .cloned()
                            .filter(|preparing_id| {
                                self.tools
                            .get(preparing_id)
                            .and_then(|index| self.order.get(*index))
                            .is_some_and(|entry| {
                                matches!(
                                    entry.call(),
                                    Some(Operation {
                                        kind: OperationKind::Preparation,
                                        label: name,
                                        execution,
                                        ..
                                    }) if !execution.is_terminal()
                                        && (name == "Generate" || name == "Wait for provider")
                                )
                            })
                            })
                    })
                    .flatten()
            });
        // A closed turn can still flush a trailing preparation frame after the
        // interrupt or completion boundary has been projected. Materializing a
        // fresh row for it strands a spinner that no later event of this turn
        // can resolve, so only an already-tracked row may still be refreshed.
        if self.live_turn_closed && existing.is_none() {
            return;
        }
        if provider_tool_id.is_none()
            && !label.is_empty()
            && existing.is_none()
            && let Some(preparing_id) = self.unkeyed_preparing_tools.pop()
        {
            self.finish_preparing_tool(&preparing_id, event.created_at);
        }
        let tool_call_id = existing
            .clone()
            .unwrap_or_else(|| format!("action-preparing:{}", event.id));
        if existing.is_none() {
            if let Some(provider_tool_id) = provider_tool_id {
                self.preparing_tools
                    .insert(provider_tool_id.to_string(), tool_call_id.clone());
            } else {
                self.unkeyed_preparing_tools.push(tool_call_id.clone());
            }
        }
        if self.foreground_tool.as_deref() != Some(tool_call_id.as_str()) {
            self.background_foreground_process();
        }
        self.upsert_running_tool(
            event,
            &tool_call_id,
            OperationKind::PREPARATION_SOURCE,
            &serde_json::json!({"label": label}),
            None,
        );
        self.foreground_tool = Some(tool_call_id);
    }

    /// Load an explicitly requested stored result, without executing or updating
    /// an action. Identity, outcome, label and all execution clocks stay sealed.
    pub fn hydrate_command_edit(
        &mut self,
        tool_call_id: &str,
        edit: &borg_agent_runtime::ToolPresentation,
    ) -> bool {
        let Some(index) = self.command_edit_rows.get(tool_call_id).copied() else {
            return false;
        };
        let Some(body) = &edit.input else {
            return false;
        };
        if let Some(Operation {
            input: code_view,
            payload_refs,
            expanded,
            ..
        }) = self.order.call_mut(index)
        {
            if code_view
                .as_ref()
                .is_some_and(|OperationView { language, .. }| is_diff_language(language))
                && !is_diff_language(&body.language)
            {
                return true;
            }
            *code_view = Some(OperationView::new(body.language.clone(), body.text.clone()));
            payload_refs.clear();
            *expanded = is_diff_language(&body.language)
                && self.diff_expansion != DiffExpansionPolicy::Collapsed;
            return true;
        }
        false
    }

    pub fn upsert_command_edit(
        &mut self,
        tool_call_id: &str,
        edit: borg_agent_runtime::ToolPresentation,
        time: String,
        completed_at: DateTime<Utc>,
        output_ref: Option<&SessionPayloadRef>,
    ) {
        let Some(body) = edit.input else {
            return;
        };
        let expanded = is_diff_language(&body.language)
            && self.diff_expansion != DiffExpansionPolicy::Collapsed;
        let payload_refs = if expanded || is_diff_language(&body.language) {
            Vec::new()
        } else {
            output_ref.cloned().into_iter().collect()
        };
        if self.command_edit_rows.contains_key(tool_call_id) {
            return;
        }
        if !self.tools.contains_key(tool_call_id) {
            return;
        }
        if self.diff_expansion == DiffExpansionPolicy::UntilNextAction {
            self.collapse_previous_edit();
        }
        // A backgrounded command reports its changes when it exits, often
        // after later rows; keep the diff under the command that made it.
        let index = self.tools[tool_call_id] + 1;
        if index < self.order.len() {
            self.reindex_after_insertion(index);
        }
        self.order.insert(
            index,
            TranscriptEntry::Operation(Operation {
                input: Some(body.into()),
                payload_refs,
                expanded,
                ..Operation::tool(
                    "command_edit",
                    edit.label,
                    edit.detail,
                    time,
                    Execution::new(completed_at, ExecutionState::Complete),
                )
            }),
        );
        self.command_edit_rows
            .insert(tool_call_id.to_string(), index);
        self.last_edit = Some(index);
    }

    pub fn upsert_goal(&mut self, goal: SessionGoal, time: String, refresh: bool) -> Option<usize> {
        let removed = self
            .order
            .iter()
            .rposition(|entry| matches!(entry, TranscriptEntry::Goal { .. }));
        // Retry/accounting updates keep the card in place; completed turns
        // explicitly refresh it at the tail for the next goal iteration.
        if !refresh
            && let Some(index) = removed
            && let Some(TranscriptEntry::Goal { goal: shown, .. }) = self.order.get(index)
            && goal.objective == shown.objective
            && goal.status == shown.status
            && goal.token_budget == shown.token_budget
        {
            if let Some(TranscriptEntry::Goal { goal: shown, .. }) = self.order.get_mut(index) {
                *shown = goal;
            }
            return Some(index);
        }
        if let Some(index) = removed {
            self.order.remove(index);
            self.reindex_after_removal(index);
        }
        self.order.push(TranscriptEntry::Goal { goal, time });
        removed
    }

    pub fn upsert_plan(&mut self, items: Vec<PlanItem>, time: String) -> Option<usize> {
        let mut expanded = false;
        let mut previous = Vec::new();
        let removed = self
            .order
            .iter()
            .rposition(|entry| matches!(entry, TranscriptEntry::Plan { .. }));
        let identity = removed.map(|index| self.order.identities[index]);
        if let Some(index) = removed {
            if let Some(TranscriptEntry::Plan {
                items: superseded,
                expanded: was_expanded,
                ..
            }) = self.order.get(index)
            {
                expanded = *was_expanded;
                previous.clone_from(superseded);
            }
            self.order.remove(index);
            self.reindex_after_removal(index);
        }
        self.order.push(TranscriptEntry::Plan {
            items,
            previous,
            time,
            expanded,
        });
        if let Some(identity) = identity {
            *self.order.identities.last_mut().expect("plan identity") = identity;
        }
        removed
    }

    pub fn tool_inspection_text(&self, index: usize) -> Option<&str> {
        let (id, ordinal) = self.order.identities.get(index)?;
        self.tool_inspection_outputs
            .get(&format!("{id}:{ordinal}"))
            .map(String::as_str)
            .or_else(|| match self.order.call(index)? {
                Operation {
                    output: Some(OperationView { text, .. }),
                    ..
                } => Some(text.as_str()),
                _ => None,
            })
    }

    pub fn prune_tool_inspection(&mut self) {
        for (id, ordinal) in &self.order.removed {
            self.tool_inspection_outputs
                .remove(&format!("{id}:{ordinal}"));
        }
    }

    pub fn upsert_running_tool(
        &mut self,
        event: &SessionEvent,
        tool_call_id: &str,
        name: &str,
        input: &serde_json::Value,
        input_ref: Option<&SessionPayloadRef>,
    ) {
        if matches!(event.kind, SessionEventKind::ToolStarted { .. })
            && self.order.identities.iter().any(|(id, _)| *id == event.id)
        {
            return;
        }
        if let Some(index) = self.tools.get(tool_call_id).copied()
            && let Some(Operation { execution, .. }) = self.order.call(index)
            && execution.is_terminal()
        {
            // Tool call IDs name executions, not reusable tool entities.
            // A real new invocation must supply a fresh call ID.
            return;
        }
        self.finish_reasoning(event.created_at);
        if let Some(handle) = tool_process_followup_handle(name, Some(input)) {
            self.provider_followups
                .insert(tool_call_id.to_string(), handle);
        }
        if self.is_redundant_process_poll(name, input) {
            return;
        }
        let presentation = project_tool_presentation(name, input, None, false);
        let cwd = presentation.cwd.clone();
        let display_name = presentation.label;
        // A follow-up names only a process handle; show the command it started.
        let detail = tool_process_followup_handle(name, Some(input))
            .and_then(|handle| {
                Uuid::parse_str(&handle)
                    .ok()
                    .and_then(|id| self.runtime_processes.get(&id))
                    .map(|process| process.command.clone())
                    .or_else(|| {
                        self.provider_backgrounds
                            .get(&handle)
                            .map(|process| process.command.clone())
                    })
            })
            .unwrap_or(presentation.detail);
        let code_view = presentation.input.map(OperationView::from);
        let is_edit_diff = matches!(
            code_view.as_ref(),
            Some(OperationView { language, .. }) if is_diff_language(language)
        );
        let rich_ui = tool_has_rich_ui(
            &display_name,
            code_view
                .as_ref()
                .map(|OperationView { language, .. }| language.as_str()),
        );
        let existing_tool = self.tools.get(tool_call_id).copied();
        if self.diff_expansion == DiffExpansionPolicy::UntilNextAction
            && self.last_edit != existing_tool
        {
            self.collapse_previous_edit();
        }
        let expanded = input_ref.is_none()
            && ((is_edit_diff && self.diff_expansion != DiffExpansionPolicy::Collapsed)
                || (!is_edit_diff && code_view.is_some() && (rich_ui || self.auto_expand_tools)));
        if let Some(tool_index) = self.tools.get(tool_call_id).copied()
            && let Some(operation) = self.order.call_mut(tool_index)
            && !operation.execution.is_terminal()
        {
            operation.cwd = cwd.clone();
            if !operation.execution.is_backgrounded() {
                self.foreground_tool = Some(tool_call_id.to_string());
            }
            operation.source = name.to_string();
            operation.label = display_name;
            operation.detail = detail;
            operation.input = code_view;
            operation.output = None;
            operation.payload_refs = input_ref.cloned().into_iter().collect();
            operation.expanded = expanded;
            operation.reclassify();
            if is_edit_diff {
                self.last_edit = Some(tool_index);
            }
            return;
        }
        self.foreground_tool = Some(tool_call_id.to_string());
        let tool_index = self.order.len();
        self.tools.insert(tool_call_id.to_string(), tool_index);
        self.order.push(TranscriptEntry::Operation(Operation {
            input: code_view,
            payload_refs: input_ref.cloned().into_iter().collect(),
            expanded,
            cwd,
            ..Operation::tool(
                name,
                display_name,
                detail,
                local_event_time(event),
                Execution::new(event.created_at, ExecutionState::Running),
            )
        }));
        if is_edit_diff {
            self.last_edit = Some(tool_index);
        }
    }

    pub fn upsert_subagent_snapshot_with_status(
        &mut self,
        agent: &SubagentSnapshot,
        status: SubagentStatus,
    ) -> bool {
        if self
            .subagent_snapshots
            .get(&agent.session_id)
            .is_some_and(|current| !agent.supersedes(current))
        {
            return false;
        }
        let name = display_agent_name(&agent.task_name);
        self.agent_names.insert(agent.session_id, name);
        self.pruned_settled_children.remove(&agent.session_id);
        let mut snapshot = agent.clone();
        snapshot.status = status;
        snapshot.final_text = None;
        let keep_meta =
            self.child_meta_refs
                .get(&agent.session_id)
                .is_some_and(|reference| {
                    match (reference.source_sequence, agent.source.as_ref()) {
                        (Some(sequence), Some(source)) => sequence > source.child_sequence,
                        _ => false,
                    }
                });
        if !keep_meta {
            if let Some(reference) = self.child_meta_refs.get_mut(&agent.session_id) {
                reference.snapshot = None;
            }
            if snapshot.lifecycle() != SubagentLifecycle::WaitingForApproval {
                self.meta
                    .approvals
                    .retain(|a| a.source_session_id != agent.session_id.to_string());
            }
        }
        let status = self
            .child_meta_refs
            .get(&agent.session_id)
            .and_then(|r| r.snapshot.as_ref())
            .map_or(snapshot.status, |s| s.status);
        self.subagents.insert(agent.session_id, status);
        self.subagent_snapshots.insert(agent.session_id, snapshot);
        true
    }

    pub fn reconcile_subagent_snapshot(
        &mut self,
        agent: &SubagentSnapshot,
        create_missing: bool,
    ) -> bool {
        if !self.upsert_subagent_snapshot_with_status(agent, agent.status) {
            return false;
        }
        self.refresh_subagent_summary(agent.session_id, create_missing);
        true
    }

    fn refresh_subagent_summary(&mut self, child_id: Uuid, create_missing: bool) {
        let Some(agent) = self.effective_subagent_snapshot(child_id) else {
            return;
        };
        // Ordinary lifecycle updates belong in the roster, not the conversation.
        let create_missing = create_missing
            && (self.show_subagent_messages
                || matches!(
                    agent.lifecycle(),
                    SubagentLifecycle::Failed | SubagentLifecycle::WaitingForApproval
                ));
        if !create_missing && !self.subagent_entries.contains_key(&child_id) {
            return;
        }
        let Some((label, detail, body, state)) =
            subagent_action_projection(SubagentActivityKind::Updated, &agent, None)
        else {
            return;
        };
        let time = agent.updated_at.to_rfc3339();
        if let Some(index) = self.subagent_entries.get(&child_id).copied() {
            let Some(Operation { execution, .. }) = self.order.lifecycle(index) else {
                return;
            };
            if execution.is_terminal() {
                if state.is_terminal() {
                    return;
                }
            } else {
                if let Some(Operation {
                    execution,
                    label: old_label,
                    detail: old_detail,
                    output: old_body,
                    time: old_time,
                    expanded,
                    ..
                }) = self.order.lifecycle_mut(index)
                {
                    execution.transition(state, agent.updated_at);
                    *old_label = label;
                    *old_detail = detail;
                    *expanded &= body.is_some();
                    *old_body = body.map(|text| OperationView::new("text", text));
                    *old_time = time;
                }
                return;
            }
        }
        if !create_missing {
            return;
        }
        self.subagent_entries.insert(child_id, self.order.len());
        self.order.push(TranscriptEntry::Operation(
            Operation::lifecycle(
                OperationKind::Agent,
                label,
                detail,
                body,
                time,
                Execution::new(agent.updated_at, state),
            )
            .with_origin(Some(child_id)),
        ));
    }

    /// Older receipts lack a durable role. Give focused overlays the host's
    /// ownership scope rather than treating every non-descendant as external.
    pub fn seed_agent_message_scope(
        &mut self,
        root: Uuid,
        director: Option<&TeamDirector>,
        local_agents: impl IntoIterator<Item = Uuid>,
    ) {
        let mut changed = self.message_root_session_id != Some(root);
        self.message_root_session_id = Some(root);
        let previous_len = self.local_agent_ids.len();
        self.local_agent_ids.extend(local_agents);
        changed |= self.local_agent_ids.len() != previous_len;
        if let Some(director) = director
            && self
                .meta
                .director
                .as_ref()
                .is_none_or(|current| current.epoch.parse::<u64>().unwrap_or(0) < director.epoch)
        {
            self.meta.director = Some(director.into());
            changed = true;
        }
        if changed {
            self.refresh_agent_message_labels();
        }
    }

    fn agent_message_label(
        &self,
        sender: Uuid,
        role: Option<AgentMessageSenderRole>,
        from_child: Option<bool>,
    ) -> &'static str {
        if role == Some(AgentMessageSenderRole::Peer) {
            return "Peer";
        }
        let director = self
            .meta
            .director
            .as_ref()
            .and_then(|director| Uuid::parse_str(&director.session_id).ok())
            .or(self.message_root_session_id);
        if director == Some(sender)
            || (director.is_none() && role == Some(AgentMessageSenderRole::Director))
        {
            return "Director";
        }
        if role.is_some()
            || from_child == Some(true)
            || self.message_root_session_id == Some(sender)
            || self.local_agent_ids.contains(&sender)
            || self.agent_names.contains_key(&sender)
            || self.subagent_snapshots.contains_key(&sender)
            || self.pruned_settled_children.contains(&sender)
        {
            "Agent"
        } else {
            "Peer"
        }
    }

    fn refresh_agent_message_labels(&mut self) {
        let updates =
            self.order
                .identities
                .iter()
                .enumerate()
                .filter_map(|(index, (event, _))| {
                    let (_, sender, _) = self.team_report_sources.get(event)?;
                    let operation = self.order.lifecycle(index)?;
                    let role = self.team_report_roles.get(event).copied().or(
                        match operation.label.as_str() {
                            "Director" => Some(AgentMessageSenderRole::Director),
                            "Agent" => Some(AgentMessageSenderRole::Agent),
                            _ => None,
                        },
                    );
                    let label = self.agent_message_label(*sender, role, None);
                    (operation.label != label).then_some((index, label))
                })
                .collect::<Vec<_>>();
        for (index, label) in updates {
            if let Some(operation) = self.order.lifecycle_mut(index) {
                operation.label = label.into();
            }
        }
    }

    pub fn effective_subagent_snapshot(&self, child_id: Uuid) -> Option<SubagentSnapshot> {
        let child = self.subagent_snapshots.get(&child_id)?.clone();
        self.child_meta_refs
            .get(&child_id)
            .and_then(|reference| reference.snapshot.clone())
            .or(Some(child))
    }
    pub fn subagent_summary(&self, child_id: Uuid) -> Option<crate::wire::ChildSummary> {
        let child = self.effective_subagent_snapshot(child_id)?;
        let mut summary = crate::reducer::child_summary(&child, &self.meta.approvals);
        if let Some(reference) = self.child_meta_refs.get(&child_id) {
            summary.approvals = self
                .meta
                .approvals
                .iter()
                .filter(|a| reference.sources.contains(&a.source_session_id))
                .cloned()
                .collect();
        }
        Some(summary)
    }
}

pub fn transcript_entry_is_turn_output(entry: &TranscriptEntry) -> bool {
    matches!(
        entry,
        TranscriptEntry::Message {
            actor: EventActor::Assistant,
            ..
        }
    ) || entry.call().is_some()
}

impl TranscriptCore {
    fn observe_meta(&mut self, event: &SessionEvent) {
        use crate::wire::{Approval, Delivery, QueueItem};
        match &event.kind {
            SessionEventKind::UsageUpdated {
                turn_id: Some(id),
                provider_duration_ms,
                input_tokens,
                output_tokens,
                cached_input_tokens,
                cache_creation_input_tokens,
                total_tokens,
                cost_microusd,
                cost_basis,
                context_tokens,
                context_window_tokens,
                ..
            } => {
                if !self.usage_by_turn.contains_key(id) {
                    self.usage_turn_order.push_back(*id);
                }
                self.usage_by_turn.insert(
                    *id,
                    crate::wire::Usage {
                        input_tokens: input_tokens.to_string(),
                        output_tokens: output_tokens.to_string(),
                        cached_tokens: cached_input_tokens.to_string(),
                        cost_microusd: cost_microusd.map(|n| n.to_string()),
                        duration_ms: Some(provider_duration_ms.to_string()),
                        cache_creation_tokens: Some(cache_creation_input_tokens.to_string()),
                        total_tokens: Some(total_tokens.to_string()),
                        context_tokens: context_tokens.map(|n| n.to_string()),
                        context_window_tokens: context_window_tokens.map(|n| n.to_string()),
                        cost_basis: (!cost_basis.is_empty()).then(|| cost_basis.clone()),
                    },
                );
                if self.usage_turn_order.len() > 128
                    && let Some(old) = self.usage_turn_order.pop_front()
                {
                    self.usage_by_turn.remove(&old);
                }
            }
            SessionEventKind::AgentMessageReceived {
                message_id,
                sender_id,
                sender_name,
                sender_role,
                ..
            } => {
                self.team_report_sources
                    .insert(event.id, (*message_id, *sender_id, sender_name.clone()));
                if let Some(role) = sender_role {
                    self.team_report_roles.insert(event.id, *role);
                }
            }
            SessionEventKind::TeamDirectorChanged { director } => {
                if self.meta.director.as_ref().is_none_or(|current| {
                    current.epoch.parse::<u64>().unwrap_or(0) < director.epoch
                }) {
                    self.meta.director = Some(director.into());
                    self.refresh_agent_message_labels();
                }
            }
            SessionEventKind::StatusChanged {
                status,
                detail,
                lifecycle,
            } => {
                self.meta.status_lifecycle = *lifecycle;
                self.meta.status = Some(*status);
                self.meta.status_detail = detail.clone();
            }
            SessionEventKind::Message {
                message_id,
                actor,
                text,
                status,
                delivery,
                ..
            } if *actor == EventActor::User => {
                self.meta
                    .queue
                    .retain(|item| item.message_id != message_id.to_string());
                if *status == MessageStatus::Queued {
                    self.meta.queue.push(QueueItem {
                        message_id: message_id.to_string(),
                        text: text.clone(),
                        delivery: if *delivery == Some(PromptDelivery::Queue) {
                            Delivery::Queue
                        } else {
                            Delivery::Steer
                        },
                        attachments: Vec::new(),
                    });
                }
            }
            SessionEventKind::PromptRecalled { message_id, .. } => self
                .meta
                .queue
                .retain(|item| item.message_id != message_id.to_string()),
            SessionEventKind::ApprovalRequested {
                approval_id,
                title,
                detail,
                ..
            } => {
                self.meta.approvals.retain(|item| item.id != *approval_id);
                self.meta.approvals.push(Approval {
                    id: approval_id.clone(),
                    target_revision: event.id.to_string(),
                    source_session_id: event.session_id.to_string(),
                    label: title.clone(),
                    detail: detail.chars().take(4096).collect(),
                    question: None,
                });
            }
            SessionEventKind::ApprovalResolved { approval_id, .. } => {
                self.meta.approvals.retain(|item| item.id != *approval_id)
            }
            SessionEventKind::ProviderInteractionRequested {
                interaction_id,
                title,
                detail,
                payload,
                ..
            } => {
                self.meta
                    .approvals
                    .retain(|item| item.id != *interaction_id);
                let secret = payload
                    .get("questions")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|questions| {
                        questions.iter().any(|q| {
                            q.get("isSecret").and_then(serde_json::Value::as_bool) == Some(true)
                        })
                    });
                self.meta.approvals.push(Approval {
                    id: interaction_id.clone(),
                    target_revision: event.id.to_string(),
                    source_session_id: event.session_id.to_string(),
                    label: title.clone(),
                    detail: if secret {
                        String::new()
                    } else {
                        detail.clone()
                    },
                    question: Some(title.clone()),
                });
            }
            SessionEventKind::ProviderInteractionResolved { interaction_id, .. } => self
                .meta
                .approvals
                .retain(|item| item.id != *interaction_id),

            _ => {}
        }
    }
}

impl TranscriptCore {
    pub fn remember_agent_message(&mut self, id: Uuid) -> bool {
        if !self.agent_messages.insert(id) {
            return false;
        }
        self.agent_message_order.push_back(id);
        if self.agent_message_order.len() > 256
            && let Some(old) = self.agent_message_order.pop_front()
        {
            self.agent_messages.remove(&old);
        }
        true
    }
    pub fn remember_watch_message(&mut self, id: Uuid) -> bool {
        if !self.watch_messages.insert(id) {
            return false;
        }
        self.watch_message_order.push_back(id);
        if self.watch_message_order.len() > 256
            && let Some(old) = self.watch_message_order.pop_front()
        {
            self.watch_messages.remove(&old);
        }
        true
    }
}

impl TranscriptCore {
    fn observe_child_approval(&mut self, child: &SessionEvent) {
        use crate::wire::Approval;
        if let SessionEventKind::ApprovalRequested {
            approval_id,
            title,
            detail,
            ..
        } = &child.kind
        {
            self.meta.approvals.retain(|a| {
                a.id != *approval_id || a.source_session_id != child.session_id.to_string()
            });
            self.meta.approvals.push(Approval {
                id: approval_id.clone(),
                target_revision: child.id.to_string(),
                source_session_id: child.session_id.to_string(),
                label: title.clone(),
                detail: detail.chars().take(4096).collect(),
                question: None,
            });
        }
        if let SessionEventKind::ApprovalResolved { approval_id, .. } = &child.kind {
            self.meta.approvals.retain(|a| {
                a.id != *approval_id || a.source_session_id != child.session_id.to_string()
            });
        }
    }

    pub fn apply_child_meta(
        &mut self,
        child_id: Uuid,
        cursor: crate::wire::Cursor,
        meta: &crate::wire::Meta,
    ) -> anyhow::Result<bool> {
        let revision = cursor.revision.parse::<u64>()?;
        anyhow::ensure!(!cursor.epoch.is_empty(), "empty child epoch");
        if let Some(previous) = self.child_meta_refs.get(&child_id)
            && previous.cursor.epoch == cursor.epoch
            && previous.cursor.revision.parse::<u64>()? >= revision
        {
            return Ok(false);
        }
        let child = self
            .subagent_snapshots
            .get(&child_id)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "unknown child ownership; hydrate authoritative subagent snapshot first"
                )
            })?
            .clone();
        let source_sequence = meta
            .source_sequence
            .as_deref()
            .map(str::parse::<u64>)
            .transpose()?;
        if let Some(previous) = self.child_meta_refs.get(&child_id) {
            match (source_sequence, previous.source_sequence) {
                (Some(incoming), Some(current)) if incoming < current => return Ok(false),
                (None, Some(_)) => return Ok(false),
                _ => {}
            }
        }
        let snapshot_sequence = child.source.as_ref().map(|source| source.child_sequence);
        let newer = match (source_sequence, snapshot_sequence) {
            (Some(metadata), Some(snapshot)) => metadata > snapshot,
            (Some(_), None) | (None, None) => true,
            (None, Some(_)) => false,
        };
        if source_sequence.is_none() && snapshot_sequence.is_some() {
            return Ok(false);
        }
        let old_sources = self
            .child_meta_refs
            .get(&child_id)
            .map(|r| r.sources.clone())
            .unwrap_or_else(|| vec![child_id.to_string()]);
        let own_is_current = match (source_sequence, snapshot_sequence) {
            (Some(metadata), Some(snapshot)) => metadata >= snapshot,
            (None, Some(_)) => false,
            _ => true,
        };
        let own_source = child_id.to_string();
        self.meta.approvals.retain(|approval| {
            !old_sources.contains(&approval.source_session_id)
                || (!own_is_current && approval.source_session_id == own_source)
        });
        let approvals = meta
            .approvals
            .iter()
            .filter(|approval| {
                approval.source_session_id != own_source
                    || (own_is_current
                        && (newer || child.lifecycle() == SubagentLifecycle::WaitingForApproval))
            })
            .cloned()
            .collect::<Vec<_>>();
        self.meta.approvals.extend(approvals.iter().cloned());
        let mut sources = meta
            .approvals
            .iter()
            .map(|a| a.source_session_id.clone())
            .collect::<Vec<_>>();
        sources.push(child_id.to_string());
        sources.sort();
        sources.dedup();
        let mut snapshot = child.clone();
        if newer && let Some(status) = meta.status {
            snapshot.status = match status {
                SessionStatus::Starting => SubagentStatus::Starting,
                SessionStatus::Running => SubagentStatus::Running,
                SessionStatus::WaitingForApproval => SubagentStatus::WaitingForApproval,
                SessionStatus::Ready | SessionStatus::Completed => SubagentStatus::Ready,
                SessionStatus::Stopped => SubagentStatus::Stopped,
                SessionStatus::Failed => SubagentStatus::Failed,
            };
            snapshot.detail = meta.status_detail.clone();
            snapshot.lifecycle = Some(meta.status_lifecycle.unwrap_or_else(|| {
                if !meta.approvals.is_empty() {
                    SubagentLifecycle::WaitingForApproval
                } else if meta.waiting_on_watchers {
                    SubagentLifecycle::WaitingOnWatchers
                } else {
                    snapshot.status.into()
                }
            }));
        }
        self.subagents.insert(child_id, snapshot.status);
        if newer {
            let usage = &meta.usage;
            let read = |value: &str, current| value.parse::<u64>().unwrap_or(current);
            snapshot.usage.input_tokens = read(&usage.input_tokens, snapshot.usage.input_tokens);
            snapshot.usage.output_tokens = read(&usage.output_tokens, snapshot.usage.output_tokens);
            snapshot.usage.cached_input_tokens =
                read(&usage.cached_tokens, snapshot.usage.cached_input_tokens);
            if let Some(value) = &usage.total_tokens {
                snapshot.usage.total_tokens = read(value, snapshot.usage.total_tokens);
            }
            if let Some(value) = &usage.context_tokens {
                snapshot.usage.context_tokens =
                    Some(read(value, snapshot.usage.context_tokens.unwrap_or(0)));
            }
            if let Some(value) = &usage.cache_creation_tokens {
                snapshot.usage.cache_creation_input_tokens =
                    read(value, snapshot.usage.cache_creation_input_tokens);
            }
            if let Some(value) = &usage.cost_microusd {
                snapshot.usage.cost_microusd = value.parse().ok().or(snapshot.usage.cost_microusd);
            }
            if let Some(value) = &usage.cost_basis {
                snapshot.usage.cost_basis = value.clone();
            }
            if let Some(config) = &meta.configuration {
                snapshot.provider = config.provider;
                snapshot.model = config.model.clone();
                snapshot.effort = config.effort.clone();
                snapshot.fast = config.fast;
                snapshot.ultrafast = config.ultrafast;
            }
        }
        self.child_meta_refs.insert(
            child_id,
            ChildMetaReference {
                cursor,
                sources,
                source_sequence,
                snapshot: newer.then_some(snapshot),
            },
        );
        self.refresh_subagent_summary(child_id, false);
        self.prune_child_metadata();
        Ok(true)
    }

    /// Children that are not running, including settled ones pruned from the
    /// bounded snapshot map.
    pub fn inactive_subagent_count(&self) -> usize {
        self.subagent_snapshots
            .keys()
            .filter_map(|id| self.effective_subagent_snapshot(*id))
            .filter(|agent| {
                !matches!(
                    agent.lifecycle(),
                    SubagentLifecycle::Starting
                        | SubagentLifecycle::Running
                        | SubagentLifecycle::WaitingOnWatchers
                )
            })
            .count()
            + self.pruned_settled_children.len()
    }

    /// Idle children parked on live watchers they started: active, not inactive.
    pub fn waiting_subagent_count(&self) -> usize {
        self.subagent_snapshots
            .keys()
            .filter_map(|id| self.effective_subagent_snapshot(*id))
            .filter(|agent| agent.lifecycle() == SubagentLifecycle::WaitingOnWatchers)
            .count()
    }

    pub fn prune_child_metadata(&mut self) {
        let mut settled =
            self.subagent_snapshots
                .values()
                .filter(|child| {
                    matches!(
                        child.status,
                        SubagentStatus::Ready | SubagentStatus::Stopped | SubagentStatus::Failed
                    ) && child.waiting_on_watchers().is_none()
                        && !self.meta.approvals.iter().any(|approval| {
                            approval.source_session_id == child.session_id.to_string()
                        })
                        && !self
                            .child_meta_refs
                            .get(&child.session_id)
                            .is_some_and(|reference| {
                                reference.snapshot.as_ref().is_some_and(|snapshot| {
                                    matches!(
                                        snapshot.lifecycle(),
                                        SubagentLifecycle::Starting
                                            | SubagentLifecycle::Running
                                            | SubagentLifecycle::WaitingOnWatchers
                                            | SubagentLifecycle::WaitingForApproval
                                    )
                                }) || self.meta.approvals.iter().any(|approval| {
                                    reference.sources.contains(&approval.source_session_id)
                                })
                            })
                })
                .map(|child| (child.updated_at, child.session_id))
                .collect::<Vec<_>>();
        settled.sort_unstable();
        let remove = settled.len().saturating_sub(64);
        for (_, id) in settled.into_iter().take(remove) {
            self.subagent_snapshots.remove(&id);
            self.pruned_settled_children.insert(id);
            self.subagents.remove(&id);
            self.agent_names.remove(&id);
            self.child_meta_refs.remove(&id);
            self.agent_message_senders.remove(&id);
        }
    }
}
