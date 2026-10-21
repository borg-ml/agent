//! Semantic transcript rows extracted from the TUI. Renderer methods stay in the frontend.
use borg_agent_runtime::{EventActor, MessageStatus, PlanItem, SessionGoal, SessionPayloadRef};
use chrono::{DateTime, Utc};
use std::path::PathBuf;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub enum TranscriptEntry {
    Message {
        actor: EventActor,
        text: String,
        attachments: Vec<(usize, PathBuf)>,
        model: Option<String>,
        effort: Option<String>,
        time: String,
        status: MessageStatus,
        execution: Execution,
        redirected: bool,
    },
    Activity {
        text: String,
        time: String,
    },
    /// Work that runs and finishes: a tool call, reasoning, or an agent,
    /// watch, approval, provider-interaction or error lifecycle row.
    Operation(Operation),
    Plan {
        items: Vec<PlanItem>,
        /// The plan this update replaced, kept so the collapsed card can show
        /// what the update actually changed instead of the first few rows.
        previous: Vec<PlanItem>,
        time: String,
        expanded: bool,
    },
    Goal {
        goal: SessionGoal,
        time: String,
    },
    Info {
        title: String,
        text: String,
        time: String,
    },
    Compaction {
        summary: String,
        time: String,
        sequence: u64,
        expanded: bool,
        execution: Execution,
    },
}

/// What an operation is. Every frontend rule about colour, glyph, grouping
/// and body handling keys on this, never on a tool's name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OperationKind {
    /// An executed tool call.
    Tool,
    /// A tool call that manages subagents (spawn, message, wait).
    Delegation,
    /// A tool call still being generated, or a wait on the provider.
    Preparation,
    /// A model reasoning block.
    Reasoning,
    /// A subagent lifecycle or report.
    Agent,
    Watch,
    Approval,
    ProviderInteraction,
    Error,
}

impl OperationKind {
    /// Source names that are protocol markers rather than real tools.
    pub const PREPARATION_SOURCE: &'static str = "action_preparing";
    pub const REASONING_SOURCE: &'static str = "reasoning";

    /// Classify a tool-style row from what produced it.
    pub fn for_call(source: &str, label: &str) -> Self {
        match source {
            Self::PREPARATION_SOURCE => Self::Preparation,
            Self::REASONING_SOURCE => Self::Reasoning,
            _ if borg_agent_runtime::is_subagent_tool(source)
                || borg_agent_runtime::is_subagent_tool(label) =>
            {
                Self::Delegation
            }
            _ => Self::Tool,
        }
    }

    /// Rows that stand for one tool-style call, named by their source. The
    /// rest are lifecycle rows that stand for an event.
    pub fn is_call(self) -> bool {
        matches!(
            self,
            Self::Tool | Self::Delegation | Self::Preparation | Self::Reasoning
        )
    }
}

/// A block of text with the language it should be drawn as: `command`, a
/// diff such as `diff:rs`, `json`, `reasoning` or plain `text`.
///
/// Saved as a `[language, text]` pair, the format checkpoints already use.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(from = "(String, String)", into = "(String, String)")]
pub struct OperationView {
    pub language: String,
    pub text: String,
}

impl From<(String, String)> for OperationView {
    fn from((language, text): (String, String)) -> Self {
        Self { language, text }
    }
}

impl From<borg_agent_runtime::ToolPresentationBody> for OperationView {
    fn from(body: borg_agent_runtime::ToolPresentationBody) -> Self {
        Self::new(body.language, body.text)
    }
}

impl From<OperationView> for (String, String) {
    fn from(view: OperationView) -> Self {
        (view.language, view.text)
    }
}

impl OperationView {
    pub fn new(language: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            language: language.into(),
            text: text.into(),
        }
    }

    pub fn is_blank(&self) -> bool {
        self.text.trim().is_empty()
    }
}

/// One operation row. Tool calls and lifecycle rows share every field; the
/// ones a kind does not use stay empty.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Operation {
    pub kind: OperationKind,
    /// The tool or synthetic source that produced the row; empty for lifecycle kinds.
    pub source: String,
    /// The display verb or title, such as "Run" or "Watch".
    pub label: String,
    pub detail: String,
    /// What was asked: a command, a diff, a reasoning block.
    pub input: Option<OperationView>,
    /// What came back, or a lifecycle row's message.
    pub output: Option<OperationView>,
    pub payload_refs: Vec<SessionPayloadRef>,
    /// A command's result in a few words: matches, exit status, tests.
    pub outcome: Option<String>,
    /// Where a command ran; shown once on its group's header.
    pub cwd: Option<String>,
    /// The session this row is about, for agent rows.
    #[serde(default)]
    pub origin: Option<uuid::Uuid>,
    pub time: String,
    pub execution: Execution,
    pub expanded: bool,
}

impl Operation {
    fn new(
        kind: OperationKind,
        source: String,
        label: String,
        detail: String,
        time: String,
        execution: Execution,
    ) -> Self {
        Self {
            kind,
            source,
            label,
            detail,
            input: None,
            output: None,
            payload_refs: Vec::new(),
            outcome: None,
            cwd: None,
            origin: None,
            time,
            execution,
            expanded: false,
        }
    }

    /// A tool-style call; its kind follows from its source.
    pub fn tool(
        source: impl Into<String>,
        label: impl Into<String>,
        detail: impl Into<String>,
        time: impl Into<String>,
        execution: Execution,
    ) -> Self {
        let (source, label) = (source.into(), label.into());
        let kind = OperationKind::for_call(&source, &label);
        Self::new(kind, source, label, detail.into(), time.into(), execution)
    }

    /// Re-derive the kind after the source or label changed, as when a
    /// generated call turns into a running tool.
    pub fn reclassify(&mut self) {
        if self.kind.is_call() {
            self.kind = OperationKind::for_call(&self.source, &self.label);
        }
    }

    pub fn reasoning(
        label: impl Into<String>,
        detail: impl Into<String>,
        text: String,
        time: impl Into<String>,
        execution: Execution,
    ) -> Self {
        let mut row = Self::new(
            OperationKind::Reasoning,
            OperationKind::REASONING_SOURCE.into(),
            label.into(),
            detail.into(),
            time.into(),
            execution,
        );
        row.input = Some(OperationView::new("reasoning", text));
        row
    }

    /// A lifecycle row: an agent, watch, approval, provider interaction or error.
    pub fn lifecycle(
        kind: OperationKind,
        label: impl Into<String>,
        detail: impl Into<String>,
        body: Option<String>,
        time: impl Into<String>,
        execution: Execution,
    ) -> Self {
        debug_assert!(!kind.is_call());
        let mut row = Self::new(
            kind,
            String::new(),
            label.into(),
            detail.into(),
            time.into(),
            execution,
        );
        row.output = body.map(|text| OperationView::new("text", text));
        row
    }

    pub fn with_origin(mut self, origin: Option<uuid::Uuid>) -> Self {
        self.origin = origin;
        self
    }

    /// A lifecycle row's message.
    pub fn body(&self) -> Option<&str> {
        self.output.as_ref().map(|view| view.text.as_str())
    }
}

#[derive(Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolRunWindow {
    pub start: usize,
    pub end: usize,
    pub total: usize,
}

/// One execution, independent of the long-lived entity that initiated it.
/// Terminal state and its clock are immutable; a new run needs a new value and row.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Execution {
    state: ExecutionState,
    started_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ExecutionState {
    Running,
    Waiting,
    Backgrounded,
    Complete,
    Received,
    Stopped,
    Failed,
}

impl ExecutionState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Complete | Self::Received | Self::Stopped | Self::Failed
        )
    }
}

impl Execution {
    pub fn new(started_at: DateTime<Utc>, state: ExecutionState) -> Self {
        Self {
            state,
            started_at,
            completed_at: state.is_terminal().then_some(started_at),
        }
    }
    pub fn state(&self) -> ExecutionState {
        self.state
    }
    pub fn started_at(&self) -> DateTime<Utc> {
        self.started_at
    }
    pub fn completed_at(&self) -> Option<DateTime<Utc>> {
        self.completed_at
    }
    pub fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }
    pub fn is_backgrounded(&self) -> bool {
        self.state == ExecutionState::Backgrounded
    }
    pub fn is_failed(&self) -> bool {
        self.state == ExecutionState::Failed
    }
    pub fn is_interrupted(&self) -> bool {
        self.state == ExecutionState::Stopped
    }
    pub fn transition(&mut self, state: ExecutionState, at: DateTime<Utc>) -> bool {
        if self.is_terminal() {
            return false;
        }
        self.state = state;
        self.completed_at = state.is_terminal().then_some(at.max(self.started_at));
        true
    }
}

impl Execution {
    pub fn lifecycle(&self) -> crate::wire::Lifecycle {
        use crate::wire::Lifecycle;
        match self.state {
            ExecutionState::Running => Lifecycle::Running,
            ExecutionState::Waiting => Lifecycle::Waiting,
            ExecutionState::Backgrounded => Lifecycle::Backgrounded,
            ExecutionState::Complete => Lifecycle::Complete,
            ExecutionState::Received => Lifecycle::Received,
            ExecutionState::Stopped => Lifecycle::Stopped,
            ExecutionState::Failed => Lifecycle::Failed,
        }
    }
}
impl<'de> serde::Deserialize<'de> for Execution {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        struct Saved {
            state: ExecutionState,
            started_at: DateTime<Utc>,
            completed_at: Option<DateTime<Utc>>,
        }
        let saved = Saved::deserialize(deserializer)?;
        if saved.state.is_terminal() != saved.completed_at.is_some()
            || saved.completed_at.is_some_and(|at| at < saved.started_at)
        {
            return Err(serde::de::Error::custom("invalid execution clock"));
        }
        Ok(Self {
            state: saved.state,
            started_at: saved.started_at,
            completed_at: saved.completed_at,
        })
    }
}

impl Execution {
    pub fn for_message(at: DateTime<Utc>, actor: EventActor, status: MessageStatus) -> Self {
        Self::new(
            at,
            match status {
                MessageStatus::Queued => ExecutionState::Waiting,
                MessageStatus::InProgress => ExecutionState::Running,
                MessageStatus::Complete => ExecutionState::Complete,
                // Providers archive a failed partial snapshot before the turn boundary.
                MessageStatus::Failed if actor == EventActor::Assistant => ExecutionState::Running,
                MessageStatus::Failed => ExecutionState::Failed,
            },
        )
    }
}

/// What a row is, independent of how a frontend draws it. Every frontend rule
/// about grouping, spacing and folding keys on this class rather than on the
/// storage variant, so a new kind of row cannot grow its own rules.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryClass {
    /// A chat message.
    Turn,
    /// A standalone titled block: plan, goal, info, compaction, system note.
    Card,
    /// Work that runs and finishes: tool calls and agent, watch, approval,
    /// provider-interaction and error rows. Consecutive operations form a run.
    Operation,
}

impl TranscriptEntry {
    pub fn class(&self) -> EntryClass {
        match self {
            Self::Message { .. } => EntryClass::Turn,
            Self::Operation(_) => EntryClass::Operation,
            Self::Activity { .. }
            | Self::Plan { .. }
            | Self::Goal { .. }
            | Self::Info { .. }
            | Self::Compaction { .. } => EntryClass::Card,
        }
    }

    pub fn operation(&self) -> Option<&Operation> {
        match self {
            Self::Operation(operation) => Some(operation),
            _ => None,
        }
    }

    pub fn operation_mut(&mut self) -> Option<&mut Operation> {
        match self {
            Self::Operation(operation) => Some(operation),
            _ => None,
        }
    }

    /// A tool-style call row, as opposed to a lifecycle row.
    pub fn call(&self) -> Option<&Operation> {
        self.operation()
            .filter(|operation| operation.kind.is_call())
    }

    pub fn call_mut(&mut self) -> Option<&mut Operation> {
        self.operation_mut()
            .filter(|operation| operation.kind.is_call())
    }

    /// An agent, watch, approval, provider-interaction or error row.
    pub fn lifecycle(&self) -> Option<&Operation> {
        self.operation()
            .filter(|operation| !operation.kind.is_call())
    }

    pub fn lifecycle_mut(&mut self) -> Option<&mut Operation> {
        self.operation_mut()
            .filter(|operation| !operation.kind.is_call())
    }

    pub fn is_operation(&self) -> bool {
        self.class() == EntryClass::Operation
    }

    /// An operation that has not reached a terminal state.
    pub fn is_live_operation(&self) -> bool {
        self.is_operation()
            && self
                .execution()
                .is_some_and(|execution| !execution.is_terminal())
    }

    pub fn execution(&self) -> Option<&Execution> {
        match self {
            Self::Message { execution, .. } | Self::Compaction { execution, .. } => Some(execution),
            Self::Operation(operation) => Some(&operation.execution),
            _ => None,
        }
    }
}
