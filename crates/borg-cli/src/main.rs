mod agent_config;
mod cli;
mod editor_preferences;
mod remote_commands;
mod sleep_inhibitor;
mod terminal_ui;

use anyhow::Result;
use clap::Parser;

use crate::cli::{Cli, Command, LocalAgentCliArgs};
use crate::remote_commands::{run_local_agent, run_remote_command};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "borg=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    match Cli::parse().command {
        Command::Agent(args) => run_local_agent(args).await,
        Command::Resume { session } => run_local_agent(LocalAgentCliArgs::resume(session)).await,
        Command::Remote { command } => run_remote_command(command).await,
    }
}
