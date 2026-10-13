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
        complete: bool,
        /// A live assistant message the user cut off mid-stream. The contract
        /// status can only say the row is settled, so the partial text would
        /// otherwise read as a finished answer.
        user_interrupted: bool,
        redirected: bool,
    },
    Activity {
        text: String,
        time: String,
    },
    /// A durable, typed lifecycle row.  Agent/approval/provider-interaction
    /// rows used to be flattened into strings and later reparsed to decide
    /// colour, grouping, and state.  Keep the semantic fields together so a
    /// reconnect/update can replace one row without manufacturing duplicate
    /// transcript text.
    Action {
        kind: TranscriptActionKind,
        label: String,
        detail: String,
        body: Option<String>,
        time: String,
        state: TranscriptActionState,
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
        complete: bool,
    },
    Tool {
        source_name: String,
        name: String,
        detail: String,
        code_view: Option<(String, String)>,
        output_view: Option<(String, String)>,
        payload_refs: Vec<SessionPayloadRef>,
        time: String,
        started_at: DateTime<Utc>,
        completed_at: Option<DateTime<Utc>>,
        complete: bool,
        error: bool,
        user_interrupted: bool,
        backgrounded: bool,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TranscriptActionState {
    Running,
    Waiting,
    Complete,
    Received,
    Stopped,
    Failed,
}

#[derive(Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolRunWindow {
    pub start: usize,
    pub end: usize,
    pub total: usize,
}

/// Shared terminal/host outcome classification, including successful tool transport with a failed command result.
pub fn tool_lifecycle_state(
    complete: bool,
    error: bool,
    interrupted: bool,
    backgrounded: bool,
    outcome: Option<&str>,
) -> crate::wire::Lifecycle {
    use crate::wire::Lifecycle;
    if error || outcome.is_some_and(crate::semantics::outcome_is_failure) {
        Lifecycle::Failed
    } else if interrupted {
        Lifecycle::Stopped
    } else if backgrounded {
        Lifecycle::Backgrounded
    } else if complete {
        Lifecycle::Complete
    } else {
        Lifecycle::Running
    }
}
