use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "borg")]
#[command(about = "A high-performance, open-source agent harness and orchestrator")]
#[command(version)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Option<Command>,
}

impl Cli {
    pub(crate) fn command_or_agent(self) -> Command {
        self.command
            .unwrap_or_else(|| Command::Agent(LocalAgentCliArgs::interactive()))
    }
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Start a local agent session.
    Agent(LocalAgentCliArgs),
    /// Resume the latest local session, or a specific session by id.
    Resume { session: Option<Uuid> },
    /// Enrol and operate this machine through Borg Remote.
    Remote {
        #[command(subcommand)]
        command: RemoteCommand,
    },
    /// Check for or install the latest Borg CLI release.
    #[command(visible_alias = "install")]
    Update(UpdateArgs),
    /// Show configured and effective optional runtime capabilities.
    Capabilities(CapabilitiesArgs),
    /// Discover effective project and user extension manifests.
    Extensions(ExtensionsArgs),
    /// List local multiplayer workspaces available to this OS user.
    Workspaces(WorkspacesArgs),
    #[command(name = "__agent-mcp", hide = true)]
    AgentMcp,
}

#[derive(Debug, Args)]
pub(crate) struct ExtensionsArgs {
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct WorkspacesArgs {
    /// Emit the local workspace catalog as JSON.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct CapabilitiesArgs {
    /// Read capabilities from this agent configuration file.
    #[arg(long)]
    pub(crate) config: Option<PathBuf>,
    /// Emit the provider-neutral capability descriptor as JSON.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct UpdateArgs {
    /// Report whether an update is available without installing it.
    #[arg(long)]
    pub(crate) check: bool,
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
    /// Start a new session in this local multiplayer workspace.
    #[arg(long, conflicts_with_all = ["resume", "continue_latest"])]
    pub(crate) workspace: Option<Uuid>,
    #[arg(long)]
    pub(crate) local_only: bool,
    /// Use a temporary local session store and discard it when this process exits.
    #[arg(long, conflicts_with_all = ["resume", "continue_latest"])]
    pub(crate) ephemeral: bool,
}

impl LocalAgentCliArgs {
    fn interactive() -> Self {
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
            resume: None,
            continue_latest: false,
            workspace: None,
            local_only: false,
            ephemeral: false,
        }
    }

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
            workspace: None,
            local_only: false,
            ephemeral: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_subcommand_launches_the_interactive_agent() {
        let command = Cli::try_parse_from(["borg"])
            .expect("plain borg command parses")
            .command_or_agent();

        let Command::Agent(args) = command else {
            panic!("plain borg must launch the agent");
        };
        assert!(args.prompt.is_empty());
        assert!(!args.continue_latest);
        assert!(args.resume.is_none());
    }

    #[test]
    fn capabilities_command_accepts_machine_readable_output() {
        let command = Cli::try_parse_from(["borg", "capabilities", "--json"])
            .expect("capabilities command parses")
            .command_or_agent();
        let Command::Capabilities(args) = command else {
            panic!("capabilities command must not launch an agent");
        };
        assert!(args.json);
        assert!(args.config.is_none());
    }

    #[test]
    fn workspaces_command_accepts_machine_readable_output() {
        let command = Cli::try_parse_from(["borg", "workspaces", "--json"])
            .expect("workspaces command parses")
            .command_or_agent();
        let Command::Workspaces(args) = command else {
            panic!("workspaces command must not launch an agent");
        };
        assert!(args.json);
    }

    #[test]
    fn ephemeral_agents_cannot_claim_a_persistent_resume_target() {
        assert!(Cli::try_parse_from(["borg", "agent", "--ephemeral"]).is_ok());
        assert!(
            Cli::try_parse_from([
                "borg",
                "agent",
                "--ephemeral",
                "--resume",
                "00000000-0000-0000-0000-000000000000"
            ])
            .is_err()
        );
    }

    #[test]
    fn a_new_agent_can_join_a_selected_workspace_but_a_resume_cannot_move() {
        let workspace = "11111111-1111-1111-1111-111111111111";
        let session = "22222222-2222-2222-2222-222222222222";
        let command = Cli::try_parse_from(["borg", "agent", "--workspace", workspace])
            .expect("selected workspace parses")
            .command_or_agent();
        let Command::Agent(args) = command else {
            panic!("agent command expected");
        };
        assert_eq!(args.workspace, Some(Uuid::parse_str(workspace).unwrap()));
        assert!(
            Cli::try_parse_from([
                "borg",
                "agent",
                "--workspace",
                workspace,
                "--resume",
                session
            ])
            .is_err()
        );
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
    OpenRouter,
    OpenAiCompatible,
}

impl From<RemoteProviderArg> for borg_remote::CodingProvider {
    fn from(value: RemoteProviderArg) -> Self {
        match value {
            RemoteProviderArg::Codex => Self::Codex,
            RemoteProviderArg::Claude => Self::Claude,
            RemoteProviderArg::OpenCode => Self::OpenCode,
            RemoteProviderArg::Kimi => Self::Kimi,
            RemoteProviderArg::OpenRouter => Self::OpenRouter,
            RemoteProviderArg::OpenAiCompatible => Self::OpenAiCompatible,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum RemotePermissionArg {
    FullAccess,
    Auto,
    #[value(alias = "read-only", alias = "workspace-write")]
    Manual,
}

impl From<RemotePermissionArg> for borg_remote::PermissionMode {
    fn from(value: RemotePermissionArg) -> Self {
        match value {
            RemotePermissionArg::FullAccess => Self::FullAccess,
            RemotePermissionArg::Auto => Self::Auto,
            RemotePermissionArg::Manual => Self::Manual,
        }
    }
}
