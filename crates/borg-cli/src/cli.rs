use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "borg")]
#[command(about = "A durable coding agent for local and remote work")]
#[command(version)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Start a local coding-agent session.
    Agent(LocalAgentCliArgs),
    /// Resume the latest local session, or a specific session by id.
    Resume { session: Option<Uuid> },
    /// Enrol and operate this machine through Borg Remote.
    Remote {
        #[command(subcommand)]
        command: RemoteCommand,
    },
}

#[derive(Debug, Args)]
pub(crate) struct LocalAgentCliArgs {
    /// Initial prompt. Omit it to enter interactive mode.
    pub(crate) prompt: Vec<String>,
    /// Project directory. On resume, omit this to reuse the recorded directory.
    #[arg(long)]
    pub(crate) cwd: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = RemoteProviderArg::Codex)]
    pub(crate) provider: RemoteProviderArg,
    #[arg(long)]
    pub(crate) model: Option<String>,
    #[arg(long)]
    pub(crate) effort: Option<String>,
    #[arg(long)]
    pub(crate) fast: bool,
    #[arg(long)]
    pub(crate) config: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = RemotePermissionArg::FullAccess)]
    pub(crate) permission: RemotePermissionArg,
    #[arg(long)]
    pub(crate) json: bool,
    #[arg(long, conflicts_with = "continue_latest")]
    pub(crate) resume: Option<Uuid>,
    #[arg(long = "continue", conflicts_with = "resume")]
    pub(crate) continue_latest: bool,
    #[arg(long)]
    pub(crate) local_only: bool,
}

impl LocalAgentCliArgs {
    pub(crate) fn resume(session: Option<Uuid>) -> Self {
        Self {
            prompt: Vec::new(),
            cwd: None,
            provider: RemoteProviderArg::Codex,
            model: None,
            effort: None,
            fast: false,
            config: None,
            permission: RemotePermissionArg::FullAccess,
            json: false,
            resume: session,
            continue_latest: session.is_none(),
            local_only: false,
        }
    }
}

#[derive(Debug, Subcommand)]
pub(crate) enum RemoteCommand {
    /// Connect this machine to your Borg account in a browser.
    Connect {
        #[arg(long, default_value = "https://borg.ml")]
        server: String,
        #[arg(long = "root")]
        roots: Vec<PathBuf>,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Enrol using a one-time token from Borg's Remote page.
    Enroll {
        #[arg(long)]
        server: String,
        #[arg(long)]
        token: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long = "root", required = true)]
        roots: Vec<PathBuf>,
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Run the outbound host connection and accept remote sessions.
    Host {
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Install and start the outbound host as a systemd user service.
    Install {
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Log in through a provider's native subscription flow.
    Login {
        #[arg(value_enum)]
        provider: RemoteProviderArg,
    },
    /// Inspect installed providers, auth, and enrolled roots.
    Status {
        #[arg(long = "root")]
        roots: Vec<PathBuf>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum RemoteProviderArg {
    Codex,
    Claude,
    OpenCode,
    Kimi,
}

impl From<RemoteProviderArg> for borg_remote::CodingProvider {
    fn from(value: RemoteProviderArg) -> Self {
        match value {
            RemoteProviderArg::Codex => Self::Codex,
            RemoteProviderArg::Claude => Self::Claude,
            RemoteProviderArg::OpenCode => Self::OpenCode,
            RemoteProviderArg::Kimi => Self::Kimi,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum RemotePermissionArg {
    ReadOnly,
    WorkspaceWrite,
    FullAccess,
}

impl From<RemotePermissionArg> for borg_remote::PermissionMode {
    fn from(value: RemotePermissionArg) -> Self {
        match value {
            RemotePermissionArg::ReadOnly => Self::ReadOnly,
            RemotePermissionArg::WorkspaceWrite => Self::WorkspaceWrite,
            RemotePermissionArg::FullAccess => Self::FullAccess,
        }
    }
}
