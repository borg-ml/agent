mod agent_config;
mod agent_mcp;
mod cli;
mod editor_preferences;
mod extensions;
mod remote_commands;
mod sleep_inhibitor;
mod terminal_ui;
mod updater;

use anyhow::Result;
use clap::Parser;
use std::fs::{self, OpenOptions};
use std::sync::Mutex;
use tracing_subscriber::fmt::writer::BoxMakeWriter;

use crate::cli::{CapabilitiesArgs, Cli, Command, ExtensionsArgs, LocalAgentCliArgs};
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
        Command::Capabilities(args) => print_capabilities(args),
        Command::Extensions(args) => print_extensions(args),
        Command::AgentMcp => agent_mcp::run().await,
    }
}

fn print_extensions(args: ExtensionsArgs) -> Result<()> {
    let config = agent_config::AgentConfig::load(None)?;
    let (catalog, _) = extensions::discover(
        &std::env::current_dir()?,
        &config.capabilities,
        config.extensions.allow_project_mcp,
    )?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&catalog)?);
    } else {
        for extension in catalog.extensions {
            println!(
                "{} {} — {}",
                extension.id,
                extension.version,
                extension.reason.unwrap_or_else(|| "active".into())
            );
        }
    }
    Ok(())
}

fn print_capabilities(args: CapabilitiesArgs) -> Result<()> {
    let configured = agent_config::AgentConfig::load(args.config.as_deref())?;
    let effective = borg_remote::SessionCapabilities::from(&configured.capabilities).effective();
    if args.json {
        println!("{}", serde_json::to_string_pretty(&effective)?);
        return Ok(());
    }
    println!("Active capabilities:");
    for capability in effective.active {
        println!(
            "  {}",
            serde_json::to_string(&capability)?.trim_matches('"')
        );
    }
    println!("Inactive capabilities:");
    for capability in effective.inactive {
        println!(
            "  {} — {}",
            serde_json::to_string(&capability.capability)?.trim_matches('"'),
            capability.reason
        );
    }
    Ok(())
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
