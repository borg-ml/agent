//! Shared semantic kernel for Borg Remote.
//!
//! Terminal, web, mobile, relay, and agent integrations all consume this
//! contract. Agent sessions are the first host workload, but enrollment and
//! transport are intentionally workload-neutral. Provider-specific protocols
//! are normalized at the host boundary; clients never scrape terminal output
//! or interpret provider JSON directly.

mod agent;
mod command;
mod contract;
mod filesystem;
mod host;
mod journal;
mod local_control;
mod lsp;
mod native_context;
mod native_harness;
mod native_io;
mod native_mcp;
mod native_process;
mod orchestration;
mod receipt;
mod session;
mod session_store;
mod subagents;
mod tool_presentation;
mod workspace;

pub use agent::{
    AgentCompaction, AgentTurn, AgentTurnControl, AgentTurnExecutor, AgentTurnResult,
    LocalAgentSettings, LocalAgentTurnExecutor, run_agent_turn, run_agent_turn_controlled,
};
pub use command::execute_workspace_command;
pub use contract::*;
pub use filesystem::execute_workspace_filesystem;
pub use host::{
    HostConfig, default_host_config_path, enroll_host, login_provider, mirror_local_session,
    probe_capabilities, run_host,
};
pub use journal::{SessionJournal, SessionWriterLease};
pub use local_control::{
    LocalSessionControlServer, run_attached_session, send_local_session_command,
    session_control_socket_path,
};
pub use lsp::LspService;
pub use orchestration::*;
pub use session::{
    SessionGoalTools, SessionTodoTools, run_agent_session, run_agent_session_with_executor,
    run_agent_session_with_executor_and_writer, run_agent_session_with_store_and_writer,
    run_agent_session_with_writer,
};
pub use session_store::{
    EventPersistence, JsonlSessionStore, SESSION_PROJECTION_VERSION, SessionConfiguration,
    SessionImport, SessionLiveEvent, SessionRecovery, SessionState, SessionStore, SessionStoreFork,
    SessionStoreHealth, SessionSummary, SessionUsage, SessionWorkspaceBinding, SqliteSessionStore,
};
pub use subagents::{
    AgentToolDispatcher, AgentToolServer, DEFAULT_MAX_SUBAGENTS, SpawnSubagent, SubagentActivity,
    SubagentActivityKind, SubagentCoordinator, SubagentSnapshot, SubagentStatus, SubagentUsage,
    agent_tool_specs, agent_tool_specs_with_capabilities, agent_tool_specs_with_subagents,
    agent_tool_specs_with_team_policy, subagent_tool_specs,
};
pub use tool_presentation::{
    ToolPresentation, ToolPresentationBody, ToolPresentationCategory, compact_text,
    is_diff_language, is_subagent_tool, project_tool_presentation, tool_call_summary,
    tool_code_view, tool_has_rich_ui, tool_output_code_view, tool_output_is_backgrounded,
    web_search_query,
};
pub use workspace::{
    AtomicWorkClaim, Audience, DeliveryAttempt, DeliveryCursor, DeliveryMode, DeliveryState,
    HostAttachment, HostIdentity, Participant, ParticipantKind, PresenceLease, Provenance,
    RecipientDelivery, SharedWork, SqliteWorkspaceStore, StructuredMention, Thread, WorkDependency,
    WorkReview, Workspace, WorkspaceArtifact, WorkspaceDecision, WorkspaceEvent,
    WorkspaceEventKind, WorkspaceHost, WorkspaceHostCapabilities, WorkspaceMembership,
    WorkspaceMessage, WorkspaceMessageBody, WorkspaceReference, WorkspaceReviewRequest,
    WorkspaceRole, WorkspaceRosterEntry, WorkspaceStore, local_human_participant_id,
};
