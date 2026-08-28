use std::path::PathBuf;
use std::{env, ffi::OsString};

use clap::{Args, Parser, Subcommand, ValueEnum};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "borg")]
#[command(about = "A high-performance, open-source agent harness and orchestrator")]
#[command(version)]
pub(crate) struct Cli {
    /// Start this invocation without configured local resource limits.
    #[arg(long, global = true)]
    pub(crate) no_limits: bool,
    #[command(subcommand)]
    pub(crate) command: Option<Command>,
}

impl Cli {
    pub(crate) fn parse_borg() -> Self {
        Self::parse_from(Self::agent_default_args(env::args_os()))
    }

    fn agent_default_args(args: impl IntoIterator<Item = OsString>) -> Vec<OsString> {
        let mut args = args.into_iter().collect::<Vec<_>>();
        if args.len() <= 1 {
            return args;
        }
        let command_index = 1 + usize::from(args.get(1).is_some_and(|arg| arg == "--no-limits"));
        let Some(command) = args.get(command_index).and_then(|arg| arg.to_str()) else {
            return args;
        };
        let is_command = matches!(
            command,
            "__agent"
                | "resume"
                | "gui"
                | "remote"
                | "update"
                | "install"
                | "capabilities"
                | "extensions"
                | "customize"
                | "inspect"
                | "workspaces"
                | "session"
                | "acp"
                | "collab"
                | "doctor"
                | "limits"
                | "help"
                | "__agent-mcp"
                | "-h"
                | "--help"
                | "-V"
                | "--version"
        );
        if !is_command {
            args.insert(command_index, OsString::from("__agent"));
        }
        args
    }

    pub(crate) fn command_or_agent(self) -> Command {
        self.command
            .unwrap_or_else(|| Command::Agent(LocalAgentCliArgs::interactive()))
    }
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Start a local agent session.
    #[command(name = "__agent", hide = true, bin_name = "borg")]
    Agent(LocalAgentCliArgs),
    /// Resume the latest local session, or a specific session by id.
    Resume { session: Option<Uuid> },
    /// Open the native GPUI frontend.
    Gui {
        /// Open a specific durable session by id.
        #[arg(long)]
        session: Option<Uuid>,
    },
    /// Enrol and operate this machine through Borg Remote.
    Remote {
        #[command(subcommand)]
        command: RemoteCommand,
    },
    /// Check for or install the latest Borg Agent release.
    #[command(visible_alias = "install")]
    Update(UpdateArgs),
    /// Show configured and effective optional runtime capabilities.
    Capabilities(CapabilitiesArgs),
    /// Manage Blu live extensions.
    Extensions(ExtensionsArgs),
    /// Inspect, export, and import the complete customization profile.
    Customize(CustomizeArgs),
    /// Inspect live local session owners and opt-in runtime profiling data.
    Inspect(InspectArgs),
    /// List local multiplayer workspaces available to this OS user.
    Workspaces(WorkspacesArgs),
    /// Inspect, branch, export, and restore local durable sessions.
    Session {
        #[command(subcommand)]
        command: SessionCommand,
    },
    /// Serve Borg as an Agent Client Protocol agent over stdio.
    Acp(AcpArgs),
    /// Share or join an end-to-end encrypted live session.
    Collab {
        #[command(subcommand)]
        command: CollabCommand,
    },
    /// Check durable storage and runtime readiness.
    Doctor {
        #[arg(long)]
        json: bool,
        /// Include SQLite's exhaustive full-database integrity scan.
        #[arg(long)]
        deep: bool,
    },
    /// Keep local agent workloads within a generous machine-wide budget.
    Limits(LimitsArgs),
    #[command(name = "__agent-mcp", hide = true)]
    AgentMcp,
}

#[derive(Debug, Args)]
pub(crate) struct CustomizeArgs {
    #[command(subcommand)]
    pub(crate) command: CustomizeCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum CustomizeCommand {
    /// Show effective editor settings, extension authority, and contributing files.
    Inspect {
        #[arg(long)]
        json: bool,
    },
    /// Export user and project customization settings as one portable JSON file.
    Export {
        output: PathBuf,
        #[arg(long)]
        force: bool,
    },
    /// Validate and import a customization profile.
    Import {
        input: PathBuf,
        /// Replace customization files that already exist.
        #[arg(long)]
        force: bool,
    },
}

#[derive(Debug, Args)]
pub(crate) struct LimitsArgs {
    #[command(subcommand)]
    pub(crate) command: Option<LimitsCommand>,
    /// Emit machine-readable JSON output.
    #[arg(long, global = true)]
    pub(crate) json: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum LimitsCommand {
    /// Enable automatic limits for future local Borg sessions.
    Enable,
    /// Show configured limits and whether this machine can enforce them.
    Status,
    /// Disable limits for future sessions without stopping active ones.
    Disable,
    /// Keep important user services restartable and favored under pressure.
    Protect(LimitsProtectArgs),
}

#[derive(Debug, Args)]
pub(crate) struct LimitsProtectArgs {
    #[command(subcommand)]
    pub(crate) command: Option<LimitsProtectCommand>,
}

#[derive(Debug, Subcommand)]
pub(crate) enum LimitsProtectCommand {
    /// Protect a user service without interrupting it.
    Add {
        /// A systemd user service such as dms.service.
        service: String,
    },
    /// Show configured services and whether protection is effective.
    List,
    /// Remove Borg's protection without interrupting the service.
    Remove {
        /// The systemd user service to stop protecting.
        service: String,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum SessionCommand {
    /// Show the local session tree or one session's ancestry.
    Tree {
        session: Option<Uuid>,
        #[arg(long)]
        json: bool,
    },
    /// Create a child branch before a durable event sequence.
    Fork {
        session: Uuid,
        #[arg(long)]
        before: u64,
        #[arg(long)]
        json: bool,
    },
    /// Undo the latest completed user prompt by creating a child branch.
    Undo {
        session: Uuid,
        #[arg(long)]
        before: Option<u64>,
        #[arg(long)]
        json: bool,
    },
    /// Re-branch from the full parent of an undo/fork child.
    Redo {
        session: Uuid,
        #[arg(long)]
        json: bool,
    },
    /// Export a portable, conversation-only JSON archive.
    Export {
        session: Option<Uuid>,
        #[arg(short, long)]
        output: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Import a conversation archive as a new local session.
    Import {
        input: PathBuf,
        #[arg(long)]
        cwd: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Capture a bounded workspace snapshot for later restore.
    Snapshot {
        #[arg(long)]
        cwd: Option<PathBuf>,
        #[arg(short, long)]
        output: PathBuf,
    },
    /// Restore a bounded workspace snapshot; keep extra files unless pruned.
    Restore {
        input: PathBuf,
        #[arg(long)]
        cwd: Option<PathBuf>,
        #[arg(long)]
        prune: bool,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum CollabCommand {
    /// Share an active local session through an untrusted relay.
    Host {
        session: Uuid,
        #[arg(long, default_value = "ws://127.0.0.1:8787")]
        relay: String,
    },
    /// Join a collaboration link from this terminal.
    Join { link: String },
    /// Run a stateless, opaque WebSocket relay.
    Relay {
        #[arg(long, default_value = "127.0.0.1:8787")]
        listen: String,
    },
}

#[derive(Debug, Args, Clone)]
pub(crate) struct AcpArgs {
    /// Default provider for newly created ACP sessions.
    #[arg(long, value_enum, default_value_t = RemoteProviderArg::Codex)]
    pub(crate) provider: RemoteProviderArg,
    /// Default model for newly created ACP sessions.
    #[arg(long)]
    pub(crate) model: Option<String>,
    /// Default reasoning effort for newly created ACP sessions.
    #[arg(long)]
    pub(crate) effort: Option<String>,
    /// Permission policy used before the ACP client answers tool requests.
    #[arg(long, value_enum, default_value_t = RemotePermissionArg::Manual)]
    pub(crate) permission: RemotePermissionArg,
    /// Read Borg runtime capabilities from this configuration file.
    #[arg(long)]
    pub(crate) config: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub(crate) struct ExtensionsArgs {
    #[command(subcommand)]
    pub(crate) command: Option<ExtensionCommand>,
    /// Emit machine-readable JSON output.
    // Parent-level and global preserves `borg extensions --json` while also
    // accepting `borg extensions info <id> --json`.
    #[arg(long, global = true)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct InspectArgs {
    #[command(subcommand)]
    pub(crate) command: Option<InspectCommand>,
    /// Emit machine-readable JSON output.
    #[arg(long, global = true)]
    pub(crate) json: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum InspectCommand {
    /// Show live local session owners and their profiling snapshot.
    Live {
        /// Inspect only this session; omit to list every local owner.
        #[arg(long)]
        session: Option<Uuid>,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum ExtensionCommand {
    /// List the effective project and user Blu catalog.
    List,
    /// Show one extension, its activation decision, dependencies, and settings.
    Info { id: String },
    /// Validate every manifest, dependency, path, and configured server.
    Doctor,
    /// Enable an installed extension without editing its manifest.
    Enable(ExtensionTargetArgs),
    /// Disable an installed extension without editing its manifest.
    Disable(ExtensionTargetArgs),
    /// Set or inspect one extension setting.
    Config(ExtensionConfigArgs),
    /// Install an extension package from a local path or Git URL.
    Install(ExtensionInstallArgs),
    /// Update one Git-backed extension, or every Git-backed extension.
    Update(ExtensionUpdateArgs),
    /// Remove an installed extension package and its local state.
    Remove(ExtensionTargetArgs),
    /// Scaffold a new extension package.
    New(ExtensionNewArgs),
    /// Revalidate the catalog. Running sessions notice the filesystem change
    /// and apply the last-known-good catalog at the next turn boundary.
    Reload,
}

#[derive(Debug, Args)]
pub(crate) struct ExtensionTargetArgs {
    pub(crate) id: String,
    /// Target project-local state under .borg instead of the user catalog.
    #[arg(long)]
    pub(crate) project: bool,
}

#[derive(Debug, Args)]
pub(crate) struct ExtensionConfigArgs {
    pub(crate) id: String,
    /// Setting name. Omit with no value to list configured settings.
    pub(crate) key: Option<String>,
    /// TOML value (for example `true`, `42`, or `"text"`). Plain text is
    /// accepted as a string for shell-friendly use.
    pub(crate) value: Option<String>,
    #[arg(long)]
    pub(crate) unset: bool,
    #[arg(long)]
    pub(crate) project: bool,
}

#[derive(Debug, Args)]
pub(crate) struct ExtensionInstallArgs {
    /// Local package directory, manifest path, or Git URL.
    pub(crate) source: String,
    #[arg(long)]
    pub(crate) project: bool,
    /// Replace an existing package with the same id after validation.
    #[arg(long)]
    pub(crate) force: bool,
}

#[derive(Debug, Args)]
pub(crate) struct ExtensionUpdateArgs {
    pub(crate) id: Option<String>,
    #[arg(long)]
    pub(crate) project: bool,
}

#[derive(Debug, Args)]
pub(crate) struct ExtensionNewArgs {
    pub(crate) id: String,
    #[arg(long)]
    pub(crate) project: bool,
    #[arg(long, default_value = "0.1.0")]
    pub(crate) version: String,
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
    /// Start one provider peer in the same durable team thread.
    #[arg(long, value_enum, requires = "prompt", conflicts_with_all = ["resume", "continue_latest"])]
    pub(crate) peer_provider: Option<RemoteProviderArg>,
    /// Model override for --peer-provider.
    #[arg(long, requires = "peer_provider")]
    pub(crate) peer_model: Option<String>,
    /// Reasoning effort override for --peer-provider.
    #[arg(long, requires = "peer_provider")]
    pub(crate) peer_effort: Option<String>,
    #[arg(long)]
    pub(crate) fast: bool,
    #[arg(long)]
    pub(crate) config: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = RemotePermissionArg::FullAccess)]
    pub(crate) permission: RemotePermissionArg,
    #[arg(long)]
    pub(crate) json: bool,
    /// Print only the final response to stdout.
    #[arg(short = 'p', long, requires = "prompt", conflicts_with = "json")]
    pub(crate) print: bool,
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
    /// Run without a terminal frontend while a native GUI owns this session.
    #[arg(long, hide = true)]
    pub(crate) gui_owner: bool,
}

impl LocalAgentCliArgs {
    fn interactive() -> Self {
        Self {
            prompt: Vec::new(),
            cwd: None,
            provider: RemoteProviderArg::Codex,
            model: None,
            effort: None,
            peer_provider: None,
            peer_model: None,
            peer_effort: None,
            fast: false,
            config: None,
            permission: RemotePermissionArg::FullAccess,
            json: false,
            print: false,
            resume: None,
            continue_latest: false,
            workspace: None,
            local_only: false,
            ephemeral: false,
            gui_owner: false,
        }
    }

    pub(crate) fn resume(session: Option<Uuid>) -> Self {
        Self {
            prompt: Vec::new(),
            cwd: None,
            provider: RemoteProviderArg::Codex,
            model: None,
            effort: None,
            peer_provider: None,
            peer_model: None,
            peer_effort: None,
            fast: false,
            config: None,
            permission: RemotePermissionArg::FullAccess,
            json: false,
            print: false,
            resume: session,
            continue_latest: session.is_none(),
            workspace: None,
            local_only: false,
            ephemeral: false,
            gui_owner: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn try_parse_direct<const N: usize>(args: [&str; N]) -> clap::error::Result<Cli> {
        Cli::try_parse_from(Cli::agent_default_args(
            args.into_iter().map(OsString::from),
        ))
    }

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
    fn agent_options_and_prompts_do_not_require_the_agent_subcommand() {
        let args = Cli::agent_default_args(
            [
                "borg",
                "--cwd",
                "/tmp/project",
                "--ephemeral",
                "--model",
                "gpt-5.6-luna",
                "write the message",
            ]
            .into_iter()
            .map(OsString::from),
        );
        let command = Cli::try_parse_from(args)
            .expect("direct agent invocation parses")
            .command_or_agent();
        let Command::Agent(args) = command else {
            panic!("agent command expected");
        };
        assert_eq!(
            args.cwd.as_deref(),
            Some(std::path::Path::new("/tmp/project"))
        );
        assert!(args.ephemeral);
        assert_eq!(args.model.as_deref(), Some("gpt-5.6-luna"));
        assert_eq!(args.prompt, ["write the message"]);
    }

    #[test]
    fn print_mode_requires_a_prompt_and_conflicts_with_json_events() {
        let command = try_parse_direct(["borg", "-p", "write the message"])
            .expect("print mode with a prompt parses")
            .command_or_agent();
        let Command::Agent(args) = command else {
            panic!("agent command expected");
        };
        assert!(args.print);
        assert_eq!(args.prompt, ["write the message"]);
        assert!(try_parse_direct(["borg", "-p"]).is_err());
        assert!(try_parse_direct(["borg", "-p", "--json", "write the message"]).is_err());
    }

    #[test]
    fn explicit_commands_stay_explicit_without_the_agent_subcommand() {
        let args = Cli::agent_default_args(
            ["borg", "--no-limits", "capabilities", "--json"]
                .into_iter()
                .map(OsString::from),
        );
        let cli = Cli::try_parse_from(args).expect("explicit command parses");
        assert!(cli.no_limits);
        assert!(matches!(cli.command, Some(Command::Capabilities(_))));
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
    fn inspect_live_accepts_an_optional_session_target() {
        let session = "22222222-2222-2222-2222-222222222222";
        let command =
            Cli::try_parse_from(["borg", "inspect", "live", "--session", session, "--json"])
                .expect("inspect live command parses")
                .command_or_agent();
        assert!(matches!(
            command,
            Command::Inspect(InspectArgs {
                command: Some(InspectCommand::Live { session: Some(_) }),
                json: true,
            })
        ));
    }

    #[test]
    fn limits_are_one_command_to_enable_and_have_a_global_escape_hatch() {
        let cli = Cli::try_parse_from(["borg", "limits", "enable", "--json"])
            .expect("limits enable parses");
        assert!(!cli.no_limits);
        assert!(matches!(
            cli.command,
            Some(Command::Limits(LimitsArgs {
                command: Some(LimitsCommand::Enable),
                json: true,
            }))
        ));

        let cli = Cli::try_parse_from(["borg", "resume", "--no-limits"])
            .expect("one-run limits bypass parses");
        assert!(cli.no_limits);
        assert!(matches!(
            cli.command,
            Some(Command::Resume { session: None })
        ));

        let command = Cli::try_parse_from(["borg", "limits", "protect", "add", "dms"])
            .expect("protected service parses")
            .command_or_agent();
        assert!(matches!(
            command,
            Command::Limits(LimitsArgs {
                command: Some(LimitsCommand::Protect(LimitsProtectArgs {
                    command: Some(LimitsProtectCommand::Add { service }),
                })),
                json: false,
            }) if service == "dms"
        ));
    }

    #[test]
    fn session_branch_and_snapshot_commands_have_explicit_targets() {
        let session = "22222222-2222-2222-2222-222222222222";
        let command = Cli::try_parse_from(["borg", "session", "undo", session, "--json"])
            .expect("session undo parses")
            .command_or_agent();
        assert!(matches!(
            command,
            Command::Session {
                command: SessionCommand::Undo {
                    json: true,
                    before: None,
                    ..
                }
            }
        ));
        assert!(
            Cli::try_parse_from(["borg", "session", "snapshot", "--output", "workspace.json",])
                .is_ok()
        );
    }

    #[test]
    fn blu_extension_commands_keep_json_global_and_scope_explicit() {
        let command = Cli::try_parse_from([
            "borg",
            "extensions",
            "install",
            "./my-blu",
            "--project",
            "--json",
        ])
        .expect("Blu install command parses")
        .command_or_agent();
        let Command::Extensions(args) = command else {
            panic!("extensions command must not launch an agent");
        };
        assert!(args.json);
        assert!(matches!(
            args.command,
            Some(ExtensionCommand::Install(ExtensionInstallArgs {
                project: true,
                force: false,
                ..
            }))
        ));

        assert!(Cli::try_parse_from(["borg", "extensions", "--json"]).is_ok());
        assert!(
            Cli::try_parse_from([
                "borg",
                "extensions",
                "config",
                "docs",
                "token",
                "--unset",
                "--project",
            ])
            .is_ok()
        );
    }

    #[test]
    fn collaboration_and_acp_commands_have_stable_cli_shapes() {
        let session = "22222222-2222-2222-2222-222222222222";
        assert!(matches!(
            Cli::try_parse_from(["borg", "collab", "host", session])
                .unwrap()
                .command_or_agent(),
            Command::Collab {
                command: CollabCommand::Host { .. }
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["borg", "collab", "relay", "--listen", "127.0.0.1:9999"])
                .unwrap()
                .command_or_agent(),
            Command::Collab {
                command: CollabCommand::Relay { .. }
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["borg", "acp", "--permission", "manual"])
                .unwrap()
                .command_or_agent(),
            Command::Acp(_)
        ));
        assert!(matches!(
            Cli::try_parse_from(["borg", "doctor", "--json"])
                .unwrap()
                .command_or_agent(),
            Command::Doctor {
                json: true,
                deep: false
            }
        ));
        assert!(matches!(
            Cli::try_parse_from(["borg", "doctor", "--deep"])
                .unwrap()
                .command_or_agent(),
            Command::Doctor {
                json: false,
                deep: true
            }
        ));
    }

    #[test]
    fn ephemeral_agents_cannot_claim_a_persistent_resume_target() {
        assert!(try_parse_direct(["borg", "--ephemeral"]).is_ok());
        assert!(
            try_parse_direct([
                "borg",
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
        let command = try_parse_direct(["borg", "--workspace", workspace])
            .expect("selected workspace parses")
            .command_or_agent();
        let Command::Agent(args) = command else {
            panic!("agent command expected");
        };
        assert_eq!(args.workspace, Some(Uuid::parse_str(workspace).unwrap()));
        assert!(try_parse_direct(["borg", "--workspace", workspace, "--resume", session]).is_err());
    }

    #[test]
    fn mixed_provider_peer_requires_a_new_thread_prompt() {
        let command = try_parse_direct([
            "borg",
            "--provider",
            "codex",
            "--peer-provider",
            "claude",
            "compare",
            "approaches",
        ])
        .expect("mixed-provider launch parses")
        .command_or_agent();
        let Command::Agent(args) = command else {
            panic!("agent command expected");
        };
        assert!(matches!(args.provider, RemoteProviderArg::Codex));
        assert!(matches!(
            args.peer_provider,
            Some(RemoteProviderArg::Claude)
        ));
        assert_eq!(args.prompt, ["compare", "approaches"]);

        assert!(
            try_parse_direct([
                "borg",
                "--peer-provider",
                "claude",
                "--resume",
                "00000000-0000-0000-0000-000000000000"
            ])
            .is_err()
        );
    }

    #[test]
    fn openrouter_accepts_arbitrary_root_and_peer_model_slugs() {
        let command = try_parse_direct([
            "borg",
            "--provider",
            "open-router",
            "--model",
            "vendor/future-model",
            "--peer-provider",
            "open-router",
            "--peer-model",
            "another/vendor-model",
            "compare",
        ])
        .expect("arbitrary OpenRouter slugs parse")
        .command_or_agent();
        let Command::Agent(args) = command else {
            panic!("agent command expected");
        };
        assert_eq!(args.model.as_deref(), Some("vendor/future-model"));
        assert_eq!(args.peer_model.as_deref(), Some("another/vendor-model"));
    }

    #[test]
    fn remote_enroll_can_read_the_token_from_stdin() {
        let command = Cli::try_parse_from([
            "borg",
            "remote",
            "enroll",
            "--server",
            "https://borg.ml",
            "--token-stdin",
            "--root",
            "/srv/borg-worker",
        ])
        .expect("stdin enrollment parses")
        .command_or_agent();
        let Command::Remote {
            command: RemoteCommand::Enroll {
                token, token_stdin, ..
            },
        } = command
        else {
            panic!("remote enroll command expected");
        };
        assert!(token.is_none());
        assert!(token_stdin);

        assert!(
            Cli::try_parse_from([
                "borg",
                "remote",
                "enroll",
                "--server",
                "https://borg.ml",
                "--token",
                "argv-token",
                "--token-stdin",
                "--root",
                "/srv/borg-worker",
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
        #[arg(
            long,
            conflicts_with = "token_stdin",
            required_unless_present = "token_stdin"
        )]
        token: Option<String>,
        /// Read the one-time enrollment token from stdin instead of argv.
        #[arg(long, conflicts_with = "token")]
        token_stdin: bool,
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
