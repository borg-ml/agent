//! Shared semantic kernel for Borg Remote.
//!
//! Terminal, web, mobile, relay, and agent integrations all consume this
//! contract. Agent sessions are the first host workload, but enrollment and
//! transport are intentionally workload-neutral. Provider-specific protocols
//! are normalized at the host boundary; clients never scrape terminal output
//! or interpret provider JSON directly.

mod agent;
mod autonomy;
mod blu_workflow;
mod command;
mod contract;
mod execution;
mod extension_api;
mod filesystem;
mod harness;
mod host;
mod local_control;
mod lsp;
mod native_context;
mod native_harness;
mod native_io;
mod native_mcp;
mod native_process;
mod orchestration;
mod persistent_runtime;
mod plugin_store;
mod process_environment;
mod profiling;
mod receipt;
mod runtime_protocol;
mod self_service;
mod session;
mod session_action;
mod session_lock;
mod session_store;
mod subagents;
mod tool_presentation;
mod workspace;
mod workspace_snapshot;

pub use agent::{
    AgentCompaction, AgentTurn, AgentTurnControl, AgentTurnExecutor, AgentTurnResult,
    BluWorkflowDefinition, ConsultationRequest, ConsultationResult, LocalAgentSettings,
    LocalAgentTurnExecutor, run_agent_turn, run_agent_turn_controlled,
};
pub use autonomy::{
    AutonomyCheckpoint, AutonomyJob, AutonomyJobHandler, AutonomyJobState, AutonomyJobTransition,
    AutonomyLease, EnqueueAutonomyJob, SaveAutonomyCheckpoint, SqliteAutonomyStore,
    SqliteAutonomySupervisor,
};
pub use blu_workflow::{BluWorkflowRequest, BluWorkflowResult};
pub use command::{
    execute_host_shell_command_with_limits, execute_workspace_command,
    execute_workspace_command_with_limits,
};
pub use contract::*;
pub use execution::{
    ExecutionCommandRequest, ExecutionProvider, ExecutionReadRequest, ExecutionSearchRequest,
    ExecutionStdinRequest, LocalExecutionProvider,
};
pub use extension_api::{
    EXTENSION_API_VERSION, EXTENSION_HOOK_EVENTS, ExtensionApiCommand, ExtensionApiHook,
    ExtensionApiRegistry, ExtensionApiScope, ExtensionApiSnapshot, ExtensionApiTool,
    ExtensionApiTransform, ExtensionEffectClass, MAX_HOOK_ARGUMENT_BYTES, bounded_hook_arguments,
    effect_class_from_name,
};
pub use filesystem::{execute_workspace_filesystem, execute_workspace_filesystem_with_limits};
pub use host::{
    HostConfig, HostExecutorFactory, default_host_config_path, enroll_host, login_provider,
    login_provider_with_output, mirror_local_session, probe_capabilities,
    probe_provider_admission_capabilities, probe_provider_capabilities,
    provider_credentials_present, run_host, run_host_with_executor_factory,
};
pub use local_control::{
    LocalSessionControlServer, force_terminate_local_session_owner, local_session_owner_is_active,
    local_session_owner_uses_current_binary, obsolete_local_session_owner_pid,
    run_attached_session, send_local_session_command, session_control_presence_socket_path,
    session_control_socket_path,
};
pub(crate) use lsp::LspPathPolicy;
pub use lsp::LspService;
pub use native_process::ProcessSnapshot;
pub use orchestration::*;
pub(crate) use plugin_store::SqlitePluginStore;
#[cfg(feature = "profiling")]
pub use profiling::RuntimeProfiler;
pub use profiling::{
    RuntimeProfileActiveTurn, RuntimeProfilePhase, RuntimeProfileSnapshot, RuntimeProfileTurn,
    read_runtime_profile, runtime_profile_path,
};
pub use runtime_protocol::{
    AGENT_RUNTIME_PROTOCOL, AGENT_RUNTIME_PROTOCOL_VERSION, AgentRuntimeCommandEnvelope,
    AgentRuntimeEventEnvelope, AgentRuntimeSnapshot,
};
pub(crate) use session::run_agent_session_with_store_and_writer_and_lsp_policy;
pub use session::{
    SessionConsultationTools, SessionGoalTools, SessionTodoTools, run_agent_session,
    run_agent_session_with_executor, run_agent_session_with_executor_and_writer,
    run_agent_session_with_store_and_writer, run_agent_session_with_store_writer_and_peers,
    run_agent_session_with_writer,
};
pub use session_action::{
    ActionDeliveryPolicy, ActionWakePolicy, SessionAction, SessionActionKind, SessionActionState,
    SessionActionTransition,
};
pub use session_lock::SessionWriterLease;
pub use session_store::{
    ClaimedActionTransition, EventPersistence, SESSION_PROJECTION_VERSION, SessionConfiguration,
    SessionHistoryHit, SessionHistoryIndexDocument, SessionHistoryPage, SessionHistoryPayload,
    SessionHistoryQuery, SessionHistorySearchMode, SessionLiveEvent, SessionRecovery, SessionState,
    SessionStore, SessionStoreFork, SessionStoreHealth, SessionSummary, SessionUsage,
    SessionWorkspaceBinding, SqliteSessionStore,
};
pub use subagents::{
    AgentToolDispatcher, AgentToolServer, DEFAULT_MAX_SUBAGENTS, SpawnSubagent, SubagentActivity,
    SubagentActivityKind, SubagentCoordinator, SubagentSnapshot, SubagentStatus, SubagentUsage,
    agent_tool_specs, agent_tool_specs_with_capabilities,
    agent_tool_specs_with_capabilities_and_consultation, agent_tool_specs_with_subagents,
    agent_tool_specs_with_team_policy, subagent_tool_specs,
};
pub use tool_presentation::{
    ToolPresentation, ToolPresentationBody, ToolPresentationCategory, canonical_action_descriptor,
    compact_text, edit_is_awaiting_diff, is_diff_language, is_edit_tool, is_mcp_resource_probe,
    is_subagent_tool, project_tool_presentation, tool_action_is_instant, tool_call_summary,
    tool_can_start_background_process, tool_code_view, tool_has_rich_ui,
    tool_output_background_handle, tool_output_code_view, tool_output_is_backgrounded,
    tool_process_followup_handle, tool_process_output_text, web_search_query,
};
pub use workspace::{
    AtomicWorkClaim, Audience, DeliveryAttempt, DeliveryCursor, DeliveryMode, DeliveryState,
    HostAttachment, HostIdentity, NewWorkspaceMessage, Participant, ParticipantKind, PresenceLease,
    Provenance, RecipientDelivery, SharedWork, SqliteWorkspaceStore, StructuredMention, Thread,
    WorkDependency, WorkReview, Workspace, WorkspaceArtifact, WorkspaceDecision, WorkspaceEvent,
    WorkspaceEventKind, WorkspaceHost, WorkspaceHostCapabilities, WorkspaceMembership,
    WorkspaceMessage, WorkspaceMessageBody, WorkspaceMessageReceipt, WorkspaceReference,
    WorkspaceReviewRequest, WorkspaceRole, WorkspaceRosterEntry, WorkspaceStore,
    local_human_participant_id,
};
pub use workspace_snapshot::{
    DEFAULT_MAX_SNAPSHOT_BYTES, DEFAULT_MAX_SNAPSHOT_FILES, MAX_SNAPSHOT_FILE_BYTES,
    WORKSPACE_SNAPSHOT_VERSION, WorkspaceRestoreReport, WorkspaceSnapshot, WorkspaceSnapshotFile,
};
