//! Shared semantic kernel for Borg Remote.
//!
//! Terminal, web, mobile, relay, and agent integrations all consume this
//! contract. Agent sessions are the first host workload, but enrollment and
//! transport are intentionally workload-neutral. Provider-specific protocols
//! are normalized at the host boundary; clients never scrape terminal output
//! or interpret provider JSON directly.

mod command;
mod host;

pub use borg_agent_runtime::*;

pub use command::{
    execute_host_shell_command_with_limits, execute_workspace_command,
    execute_workspace_command_with_limits,
};
pub use host::{
    HostConfig, HostExecutorFactory, enroll_host, login_provider, login_provider_with_output,
    mirror_local_session, probe_capabilities, probe_provider_admission_capabilities,
    probe_provider_capabilities, provider_credentials_present,
    provider_subscription_credentials_present, run_host, run_host_with_executor_factory,
    sync_remote_session,
};
