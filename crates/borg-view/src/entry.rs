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
    /// A durable, typed lifecycle row.  Agent/approval/provider-interaction
    /// rows used to be flattened into strings and later reparsed to decide
    /// colour, grouping, and state.  Keep the semantic fields together so a
    /// updates can advance a live execution without manufacturing duplicate
    /// transcript text; terminal executions remain immutable.
    Action {
        #[serde(default)]
        source_session_id: Option<uuid::Uuid>,
        kind: TranscriptActionKind,
        label: String,
        detail: String,
        body: Option<String>,
        time: String,
        execution: Execution,
        expanded: bool,
    },
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
    Tool {
        source_name: String,
        name: String,
        detail: String,
        code_view: Option<(String, String)>,
        output_view: Option<(String, String)>,
        payload_refs: Vec<SessionPayloadRef>,
        time: String,
        execution: Execution,
        expanded: bool,
        /// A command's result in a few words: matches, exit status, tests.
        outcome: Option<String>,
        /// Where a command ran; shown once on its group's header.
        cwd: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TranscriptActionKind {
    Agent,
    Watch,
    Approval,
    ProviderInteraction,
    Error,
}

pub type TranscriptActionState = ExecutionState;

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

impl TranscriptEntry {
    pub fn execution(&self) -> Option<&Execution> {
        match self {
            Self::Message { execution, .. }
            | Self::Action { execution, .. }
            | Self::Compaction { execution, .. }
            | Self::Tool { execution, .. } => Some(execution),
            _ => None,
        }
    }
}
