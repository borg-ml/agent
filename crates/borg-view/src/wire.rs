use serde::{Deserialize, Serialize};
use ts_rs::TS;

pub const PROTOCOL_VERSION: u32 = 2;
pub const SNAPSHOT_ROWS: usize = 75;
pub const SNAPSHOT_BYTES: usize = 1_048_576;

// Decimal strings preserve exact revisions in JavaScript.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct Cursor {
    pub epoch: String,
    pub revision: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Scope {
    Host,
    Session { session_id: String },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum RowKind {
    Message,
    Reasoning,
    Tool,
    ActionGroup,
    Diff,
    TeamReport,
    ChildSummary,
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct AssetRef {
    pub id: String,
    pub media_type: String,
    pub bytes: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct Row {
    pub id: String,
    pub revision: String,
    pub kind: RowKind,
    pub timestamp: String,
    pub source_session_id: String,
    pub history_before: String,
    pub after_id: Option<String>,
    pub text_state: Option<RowTextState>,
    pub body: RowBody,
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct RowTextState {
    pub field: TextField,
    pub bytes: String,
    pub asset: Option<AssetRef>,
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RowBody {
    Message {
        actor: Actor,
        text: String,
        status: Lifecycle,
        attachments: Vec<AssetRef>,
        model: Option<String>,
        effort: Option<String>,
        interrupted: bool,
        redirected: bool,
    },
    Reasoning {
        text: String,
        status: Lifecycle,
    },
    Tool {
        source_name: String,
        name: String,
        detail: String,
        input: Option<String>,
        output_preview: Option<String>,
        assets: Vec<AssetRef>,
        status: Lifecycle,
        started_at: String,
        completed_at: Option<String>,
        outcome: Option<String>,
        cwd: Option<String>,
        parent_id: Option<String>,
    },
    ActionGroup {
        label: String,
        row_ids: Vec<String>,
        actions: Vec<ActionRow>,
        status: Lifecycle,
    },
    Diff {
        patch: String,
        status: Lifecycle,
        path: String,
        language: String,
        before: Option<AssetRef>,
        after: Option<AssetRef>,
        added: usize,
        removed: usize,
    },
    TeamReport {
        sender_id: String,
        sender_name: String,
        message_id: String,
        text: String,
    },
    ChildSummary {
        child: ChildSummary,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActionRow {
    Lifecycle {
        kind: ActionKind,
        label: String,
        detail: String,
        body: Option<String>,
        status: Lifecycle,
    },
    Plan {
        items: Vec<borg_agent_runtime::PlanItem>,
        previous: Vec<borg_agent_runtime::PlanItem>,
    },
    Goal {
        goal: ViewGoal,
    },
    Information {
        title: String,
        text: String,
    },
    Compaction {
        summary: String,
        sequence: String,
        complete: bool,
    },
    Activity {
        text: String,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ActionKind {
    Agent,
    Watch,
    Approval,
    ProviderInteraction,
    Error,
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum Actor {
    User,
    Assistant,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum Lifecycle {
    Queued,
    Running,
    Waiting,
    Complete,
    Received,
    Stopped,
    Failed,
    Backgrounded,
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct ChildSummary {
    pub source_session_id: String,
    pub parent_session_id: String,
    pub name: String,
    pub status: Lifecycle,
    pub last_activity: Option<String>,
    pub updated_at: String,
    pub approvals: Vec<Approval>,
    pub usage: Usage,
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct Approval {
    pub id: String,
    pub target_revision: String,
    pub source_session_id: String,
    pub label: String,
    pub detail: String,
    pub question: Option<String>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, TS)]
pub struct Usage {
    pub input_tokens: String,
    pub output_tokens: String,
    pub cached_tokens: String,
    pub cost_microusd: Option<String>,
    pub duration_ms: Option<String>,
    pub cache_creation_tokens: Option<String>,
    pub total_tokens: Option<String>,
    pub context_tokens: Option<String>,
    pub context_window_tokens: Option<String>,
    pub cost_basis: Option<String>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, TS)]
pub struct Context {
    pub tokens: Option<String>,
    pub window_tokens: Option<String>,
    pub remaining_percent: Option<u8>,
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct QueueItem {
    pub message_id: String,
    pub text: String,
    pub delivery: Delivery,
    pub attachments: Vec<AssetRef>,
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct Watch {
    pub id: String,
    pub started_at: String,
    pub last_event_at: Option<String>,
    pub event_count: String,
    pub label: String,
    pub status: Lifecycle,
    pub source_session_id: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct ViewGoal {
    pub id: String,
    pub objective: String,
    pub status: borg_agent_runtime::GoalStatus,
    pub token_budget: Option<String>,
    pub tokens_used: String,
    pub time_used_seconds: String,
    pub created_at: String,
    pub updated_at: String,
}
impl From<&borg_agent_runtime::SessionGoal> for ViewGoal {
    fn from(goal: &borg_agent_runtime::SessionGoal) -> Self {
        Self {
            id: goal.id.to_string(),
            objective: goal.objective.clone(),
            status: goal.status,
            token_budget: goal.token_budget.map(|n| n.to_string()),
            tokens_used: goal.tokens_used.to_string(),
            time_used_seconds: goal.time_used_seconds.to_string(),
            created_at: goal.created_at.to_rfc3339(),
            updated_at: goal.updated_at.to_rfc3339(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct ViewConfiguration {
    pub cwd: String,
    pub provider: borg_agent_runtime::CodingProvider,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub fast: bool,
    pub ultrafast: bool,
    pub permission_mode: borg_agent_runtime::PermissionMode,
    pub response_language: borg_agent_runtime::ResponseLanguage,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ViewDirector {
    pub session_id: String,
    pub path: String,
    pub epoch: String,
}
impl From<&borg_agent_runtime::TeamDirector> for ViewDirector {
    fn from(director: &borg_agent_runtime::TeamDirector) -> Self {
        Self {
            session_id: director.session_id.to_string(),
            path: director.path.clone(),
            epoch: director.epoch.to_string(),
        }
    }
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, TS)]
pub struct Meta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_sequence: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_lifecycle: Option<borg_agent_runtime::SubagentLifecycle>,
    #[serde(default)]
    pub director: Option<ViewDirector>,
    pub configuration: Option<ViewConfiguration>,
    pub status_detail: Option<String>,
    pub waiting_on_watchers: bool,
    pub status: Option<borg_agent_runtime::SessionStatus>,
    pub goal: Option<ViewGoal>,
    pub plan: Vec<borg_agent_runtime::PlanItem>,
    pub usage: Usage,
    pub context: Context,
    pub queue: Vec<QueueItem>,
    pub approvals: Vec<Approval>,
    pub watches: Vec<Watch>,
    pub children: Vec<ChildSummary>,
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ViewOp {
    RowUpsert {
        row: Row,
    },
    RowAppend {
        id: String,
        expected_revision: String,
        revision: String,
        offset: usize,
        field: TextField,
        text: String,
    },
    RowRemove {
        id: String,
    },
    MetaPatch {
        meta: Meta,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum TextField {
    Message,
    Reasoning,
    ToolInput,
    ToolOutput,
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct Snapshot {
    pub cursor: Cursor,
    pub rows: Vec<Row>,
    pub meta: Meta,
    pub history_anchor: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GoalIntent {
    Set {
        objective: String,
        token_budget: Option<String>,
    },
    Pause,
    Resume,
    Clear,
}
impl From<borg_agent_runtime::GoalAction> for GoalIntent {
    fn from(action: borg_agent_runtime::GoalAction) -> Self {
        match action {
            borg_agent_runtime::GoalAction::Set {
                objective,
                token_budget,
            } => Self::Set {
                objective,
                token_budget: token_budget.map(|n| n.to_string()),
            },
            borg_agent_runtime::GoalAction::Pause => Self::Pause,
            borg_agent_runtime::GoalAction::Resume => Self::Resume,
            borg_agent_runtime::GoalAction::Clear => Self::Clear,
        }
    }
}
impl TryFrom<GoalIntent> for borg_agent_runtime::GoalAction {
    type Error = anyhow::Error;
    fn try_from(intent: GoalIntent) -> Result<Self, Self::Error> {
        Ok(match intent {
            GoalIntent::Set {
                objective,
                token_budget,
            } => {
                let token_budget = token_budget.map(|value| value.parse::<u64>()).transpose()?;
                anyhow::ensure!(
                    token_budget != Some(0),
                    "goal token budget must be positive"
                );
                Self::Set {
                    objective,
                    token_budget,
                }
            }
            GoalIntent::Pause => Self::Pause,
            GoalIntent::Resume => Self::Resume,
            GoalIntent::Clear => Self::Clear,
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    Prompt {
        text: String,
        delivery: Delivery,
        attachments: Vec<AssetRef>,
    },
    Configure {
        provider: Option<borg_agent_runtime::CodingProvider>,
        model: Option<String>,
        effort: Option<String>,
        fast: Option<bool>,
        ultrafast: Option<bool>,
        permission_mode: Option<borg_agent_runtime::PermissionMode>,
        response_language: Option<borg_agent_runtime::ResponseLanguage>,
    },
    Goal {
        action: GoalIntent,
    },
    Todo {
        action: borg_agent_runtime::TodoAction,
    },
    StopWatch {
        watch_id: String,
    },
    NewSession {
        cwd: String,
    },
    Interrupt,
    Compact,
    ClearContext,
    FlushPendingInput,
    RecallQueuedPrompt {
        message_id: Option<String>,
        replacement: Option<String>,
    },
    RespondToQuestion {
        approval_id: String,
        target_revision: String,
        source_session_id: String,
        text: String,
    },

    Approval {
        approval_id: String,
        target_revision: String,
        source_session_id: String,
        decision: Decision,
        edited_input: Option<String>,
    },
    Slash {
        text: String,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum Delivery {
    Steer,
    Queue,
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    Once,
    Session,
    Deny,
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct Subscription {
    pub scope: Scope,
    pub after: Option<Cursor>,
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientFrame {
    AssetPage {
        scope: Scope,
        request_id: String,
        asset_id: String,
        offset: String,
        max_bytes: usize,
    },
    SubscribeInbox {},
    TurnResult {
        scope: Scope,
        command_id: String,
        message_id: String,
    },
    Hello {
        protocol: u32,
        device: String,
        subscriptions: Vec<Subscription>,
    },
    Subscribe {
        subscription: Subscription,
    },
    Unsubscribe {
        scope: Scope,
    },
    Resync {
        scope: Scope,
        reason: String,
    },
    Ack {
        scope: Scope,
        cursor: Cursor,
    },
    History {
        scope: Scope,
        before: String,
        limit: usize,
    },
    Command {
        command_id: String,
        scope: Scope,
        command: Command,
    },
    Presence {
        scope: Scope,
        foreground: bool,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum AttentionKind {
    Approval,
    Question,
    Done,
    Error,
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct InboxItem {
    pub id: String,
    pub host_id: String,
    pub source_session_id: String,
    pub kind: AttentionKind,
    pub label: String,
    pub created_at: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
pub struct SessionHeadline {
    pub session_id: String,
    pub parent_session_id: Option<String>,
    pub name: String,
    pub status: Option<borg_agent_runtime::SessionStatus>,
    pub updated_at: Option<String>,
    pub needs_attention: Vec<AttentionKind>,
}
impl SessionHeadline {
    /// Owner parent comes from subagent ownership, never the summary's fork-provenance parent.
    pub fn from_summary(
        summary: &borg_agent_runtime::SessionSummary,
        owner_parent: Option<uuid::Uuid>,
        needs_attention: Vec<AttentionKind>,
    ) -> Self {
        let state = &summary.state;
        Self {
            session_id: summary.session_id.to_string(),
            parent_session_id: owner_parent.map(|id| id.to_string()),
            name: state
                .title
                .as_ref()
                .or(state.imported_title.as_ref())
                .map(|name| name.chars().take(160).collect())
                .unwrap_or_else(|| summary.session_id.to_string()),
            status: state.status,
            updated_at: state
                .activity_at
                .or(state.started_at)
                .map(|at| at.to_rfc3339()),
            needs_attention,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HostOp {
    SessionUpsert { session: SessionHeadline },
    SessionRemove { session_id: String },
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerFrame {
    HostSnapshot {
        scope: Scope,
        cursor: Cursor,
        sessions: Vec<SessionHeadline>,
        history_anchor: Option<String>,
    },
    HostPatch {
        scope: Scope,
        base: Cursor,
        cursor: Cursor,
        ops: Vec<HostOp>,
    },
    HostHistoryPage {
        scope: Scope,
        sessions: Vec<SessionHeadline>,
        before: Option<String>,
    },
    AssetPage {
        scope: Scope,
        request_id: String,
        asset_id: String,
        offset: String,
        data: String,
        total_bytes: String,
        next_offset: Option<String>,
        media_type: String,
    },
    AttentionCleared {
        scope: Scope,
        source_session_id: String,
        kind: AttentionKind,
    },
    TurnStarted {
        scope: Scope,
        command_id: String,
        message_id: String,
    },
    TurnCompleted {
        scope: Scope,
        command_id: String,
        message_id: String,
        provider_session_id: Option<String>,
        final_text: String,
        final_text_asset: Option<AssetRef>,
        error: Option<String>,
        usage: Option<Box<Usage>>,
    },
    Inbox {
        items: Vec<InboxItem>,
        synchronized_at: String,
    },
    Welcome {
        protocol: u32,
        host_id: String,
        commands: Vec<String>,
    },
    Snapshot {
        scope: Scope,
        view: Box<Snapshot>,
    },
    HistoryPage {
        scope: Scope,
        rows: Vec<Row>,
        before: Option<String>,
    },
    Patch {
        scope: Scope,
        base: Cursor,
        cursor: Cursor,
        ops: Vec<ViewOp>,
    },
    Synchronized {
        scope: Scope,
        cursor: Cursor,
    },
    Result {
        command_id: String,
        scope: Scope,
        cursor: Cursor,
        outcome: CommandOutcome,
    },
    Attention {
        scope: Scope,
        source_session_id: String,
        kind: AttentionKind,
        label: String,
    },
    Gap {
        scope: Scope,
        reason: String,
    },
    AuthRevoked {
        reason: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CommandOutcome {
    Applied,
    Recalled {
        message_id: String,
        replacement: String,
        attachments: Vec<AssetRef>,
    },

    Rejected {
        code: String,
        message: String,
    },
    ClientLocal {
        effect: ClientEffect,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ClientEffect {
    CloseView,
}
