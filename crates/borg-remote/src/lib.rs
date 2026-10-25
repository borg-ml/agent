//! Shared semantic kernel for Borg Remote.
//!
//! Terminal, web, mobile, relay, and agent integrations all consume this
//! contract. Agent sessions are the first host workload, but enrollment and
//! transport are intentionally workload-neutral. Provider-specific protocols
//! are normalized at the host boundary; clients never scrape terminal output
//! or interpret provider JSON directly.

pub mod admission;
pub mod auth;
mod command;
pub mod command_ledger;
mod host;
pub mod projection;
pub mod row_index;
pub mod sync;
pub mod trust;
pub mod tunnel;

pub use borg_agent_runtime::*;

pub use command::{
    execute_host_shell_command_with_limits, execute_workspace_command,
    execute_workspace_command_with_limits,
};
pub use host::{
    DirectListenerConfig, HostConfig, HostExecutorFactory, device_authorization_request,
    enroll_host, host_launch_is_local_mirror, login_provider, login_provider_with_output,
    mirror_local_session, probe_capabilities, probe_provider_admission_capabilities,
    probe_provider_capabilities, provider_credentials_present,
    provider_subscription_credentials_present, read_config as read_host_config,
    register_existing_session, run_host, run_host_with_executor_factory, set_mirror_local_sessions,
    sync_remote_session, validate_server_url,
};
