mod agent_config;
mod agent_mcp;
mod cli;
mod editor_preferences;
mod remote_commands;
mod sleep_inhibitor;
mod terminal_ui;
mod updater;

use anyhow::Result;
use clap::Parser;
use std::fs::{self, OpenOptions};
use std::sync::Mutex;
use tracing_subscriber::fmt::writer::BoxMakeWriter;

use crate::cli::{Cli, Command, LocalAgentCliArgs};
use crate::remote_commands::{run_local_agent, run_remote_command};

#[tokio::main]
async fn main() -> Result<()> {
    let writer = log_writer();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "borg=info".into()),
        )
        .with_ansi(false)
        .with_writer(writer)
        .init();
    match Cli::parse().command_or_agent() {
        Command::Agent(args) => run_local_agent(args).await,
        Command::Resume { session } => run_local_agent(LocalAgentCliArgs::resume(session)).await,
        Command::Remote { command } => run_remote_command(command).await,
        Command::Update(args) => updater::run(args).await,
        Command::AgentMcp => agent_mcp::run().await,
    }
}

fn log_writer() -> BoxMakeWriter {
    let borg_home = std::env::var_os("BORG_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|home| std::path::PathBuf::from(home).join(".borg"))
        })
        .unwrap_or_else(|| std::path::PathBuf::from(".borg"));
    let log_dir = borg_home.join("logs");
    let file = fs::create_dir_all(&log_dir).and_then(|()| {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_dir.join("borg.log"))
    });
    match file {
        Ok(file) => BoxMakeWriter::new(Mutex::new(file)),
        Err(_) => BoxMakeWriter::new(std::io::sink),
    }
}
