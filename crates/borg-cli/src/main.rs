mod acp;
mod agent_config;
mod agent_mcp;
mod cli;
mod collab;
mod customization;
mod dictation {
    pub(crate) use borg_dictation::*;
}
mod editor_preferences {
    pub(crate) use borg_ui::preferences::*;
}
mod extensions;
mod inspect;
mod limits;
mod protection;
mod remote_commands;
mod session_commands;
mod sleep_inhibitor;
mod terminal_ui {
    pub(crate) use borg_tui::*;
}
mod updater;

use anyhow::{Context, Result};
use std::fs::{self, OpenOptions};
use std::sync::Mutex;
use tracing_subscriber::fmt::writer::BoxMakeWriter;

use crate::cli::{
    CapabilitiesArgs, Cli, Command, ExtensionCommand, ExtensionsArgs, LocalAgentCliArgs,
};
use crate::remote_commands::{print_local_workspaces, run_local_agent, run_remote_command};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse_borg();
    let no_limits = cli.no_limits;
    let command = cli.command_or_agent();
    limits::reexec_local_agent_if_enabled(&command, no_limits)?;
    let writer = log_writer();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "borg=info".into()),
        )
        .with_ansi(false)
        .with_writer(writer)
        .init();
    match command {
        Command::Agent(args) => run_local_agent(args).await,
        Command::Resume { session } => run_local_agent(LocalAgentCliArgs::resume(session)).await,
        Command::Gui { session } => run_gui(session).await,
        Command::Remote { command } => run_remote_command(command).await,
        Command::Update(args) => updater::run(args).await,
        Command::Capabilities(args) => print_capabilities(args),
        Command::Extensions(args) => print_extensions(args),
        Command::Customize(args) => customization::run(args),
        Command::Inspect(args) => inspect::run(args).await,
        Command::Workspaces(args) => print_local_workspaces(args.json).await,
        Command::Session { command } => session_commands::run(command).await,
        Command::Acp(args) => acp::run(args).await,
        Command::Collab { command } => collab::run(command).await,
        Command::Doctor { json, deep } => doctor(json, deep).await,
        Command::Limits(args) => limits::run(args).await,
        Command::AgentMcp => agent_mcp::run().await,
    }
}

async fn run_gui(session: Option<uuid::Uuid>) -> Result<()> {
    let executable_name = if cfg!(windows) {
        "borg-gui.exe"
    } else {
        "borg-gui"
    };
    let executable = std::env::current_exe()
        .ok()
        .and_then(|path| path.parent().map(|parent| parent.join(executable_name)))
        .filter(|path| path.is_file())
        .unwrap_or_else(|| executable_name.into());
    let mut command = tokio::process::Command::new(&executable);
    if let Some(session) = session {
        command.args(["--session", &session.to_string()]);
    }
    let status = command
        .status()
        .await
        .with_context(|| format!("failed to start {}", executable.display()))?;
    anyhow::ensure!(status.success(), "native GUI exited with {status}");
    Ok(())
}

async fn doctor(json: bool, deep: bool) -> Result<()> {
    let sessions_dir = borg_remote::default_host_config_path()
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("sessions");
    let store =
        borg_remote::SqliteSessionStore::open(sessions_dir.join("sessions.sqlite3")).await?;
    let health = if deep {
        store.health().await?
    } else {
        store.readiness().await?
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&health)?);
    } else {
        println!(
            "Durable session store: {}",
            if health.is_ready() {
                "ready"
            } else {
                "degraded"
            }
        );
        println!(
            "  integrity: {}",
            if health.integrity_checked {
                health.integrity.as_str()
            } else {
                "not checked (run `borg doctor --deep`)"
            }
        );
        println!(
            "  sqlite: {} · synchronous={} · foreign_keys={}",
            health.journal_mode, health.synchronous, health.foreign_keys
        );
        println!(
            "  WAL: busy={} · log={} · checkpointed={} · retained limit={} MiB",
            health.wal_busy,
            health.wal_log_frames,
            health.wal_checkpointed_frames,
            health.journal_size_limit_bytes / (1024 * 1024)
        );
        println!(
            "  durable rows: {} sessions · {} events · {} payloads",
            health.sessions, health.events, health.payloads
        );
        println!("  projection version: {}", health.projection_version);
    }
    anyhow::ensure!(health.is_ready(), "durable session store is degraded");
    Ok(())
}

fn print_extensions(args: ExtensionsArgs) -> Result<()> {
    let config = agent_config::AgentConfig::load(None)?;
    let cwd = std::env::current_dir()?;
    let discover = || {
        extensions::discover(&cwd, &config.capabilities, &config.extensions)
            .map(|(catalog, _, _)| catalog)
    };
    match args.command.unwrap_or(ExtensionCommand::List) {
        ExtensionCommand::List => print_extension_catalog(&discover()?, args.json)?,
        ExtensionCommand::Info { id } => {
            let catalog = discover()?;
            let extension = catalog
                .extension(&id)
                .with_context(|| format!("Blu extension `{id}` is not installed"))?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(extension)?);
            } else {
                println!("{} {}", extension.name, extension.version);
                println!("  id: {}", extension.id);
                println!("  scope: {}", extension.scope.label());
                println!("  runtime access: {}", extension.requested_access.label());
                println!("  manifest: {}", extension.manifest_path.display());
                println!(
                    "  state: {}",
                    extension.reason.as_deref().unwrap_or(if extension.active {
                        "active"
                    } else {
                        "inactive"
                    })
                );
                if !extension.workflow_names.is_empty() {
                    println!("  workflows: {}", extension.workflow_names.join(", "));
                    for (name, runtime) in &extension.workflow_runtimes {
                        println!("    {name}: {runtime}");
                    }
                }
                if let Some(description) = &extension.description {
                    println!("  description: {description}");
                }
                if !extension.dependencies.is_empty() {
                    println!("  dependencies:");
                    for (id, requirement) in &extension.dependencies {
                        println!("    {id} {requirement}");
                    }
                }
                if !extension.settings.is_empty() {
                    println!("  settings:");
                    for (key, value) in &extension.settings {
                        println!("    {key} = {value}");
                    }
                }
                for root in &extension.skill_roots {
                    println!("  skill root: {}", root.display());
                }
                for server in &extension.servers {
                    println!("  MCP server: {server}");
                }
            }
        }
        ExtensionCommand::Doctor => {
            let catalog = discover()?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&catalog)?);
            } else if catalog.diagnostics.is_empty() {
                println!(
                    "Blu is ready · {} installed · {} active · revision {}",
                    catalog.extensions.len(),
                    catalog
                        .extensions
                        .iter()
                        .filter(|extension| extension.active)
                        .count(),
                    &catalog.revision[..catalog.revision.len().min(12)]
                );
            } else {
                for diagnostic in &catalog.diagnostics {
                    eprintln!(
                        "{}: {} — {}",
                        diagnostic.level.label(),
                        diagnostic.path.display(),
                        diagnostic.message
                    );
                }
            }
            anyhow::ensure!(!catalog.has_errors(), "Blu catalog has errors");
        }
        ExtensionCommand::Enable(target) => {
            let path = extensions::set_enabled(&cwd, &target.id, target.project, true)?;
            let catalog = discover()?;
            let extension = catalog
                .extension(&target.id)
                .with_context(|| format!("Blu extension `{}` is not installed", target.id))?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(extension)?);
            } else {
                println!(
                    "Enabled {} · live state written to {}",
                    target.id,
                    path.display()
                );
            }
        }
        ExtensionCommand::Disable(target) => {
            let path = extensions::set_enabled(&cwd, &target.id, target.project, false)?;
            let catalog = discover()?;
            let extension = catalog
                .extension(&target.id)
                .with_context(|| format!("Blu extension `{}` is not installed", target.id))?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(extension)?);
            } else {
                println!(
                    "Disabled {} · live state written to {}",
                    target.id,
                    path.display()
                );
            }
        }
        ExtensionCommand::Config(config_args) => {
            if config_args.unset {
                anyhow::ensure!(config_args.value.is_none(), "--unset conflicts with VALUE");
            }
            if let Some(key) = config_args.key.as_deref() {
                let value = if config_args.unset {
                    None
                } else {
                    Some(extensions::parse_config_value(
                        config_args
                            .value
                            .as_deref()
                            .context("VALUE is required unless --unset is used")?,
                    ))
                };
                let path =
                    extensions::configure(&cwd, &config_args.id, key, value, config_args.project)?;
                let catalog = discover()?;
                let extension = catalog.extension(&config_args.id).with_context(|| {
                    format!(
                        "setting made extension `{}` invalid; run `borg extensions doctor` and correct or unset it",
                        config_args.id
                    )
                })?;
                if args.json {
                    println!("{}", serde_json::to_string_pretty(extension)?);
                } else {
                    println!("Configured {}.{key} · {}", config_args.id, path.display());
                }
            } else {
                anyhow::ensure!(
                    config_args.value.is_none() && !config_args.unset,
                    "KEY is required when setting or unsetting a value"
                );
                let catalog = discover()?;
                let extension = catalog.extension(&config_args.id).with_context(|| {
                    format!("Blu extension `{}` is not installed", config_args.id)
                })?;
                if args.json {
                    println!("{}", serde_json::to_string_pretty(&extension.settings)?);
                } else if extension.settings.is_empty() {
                    println!("{} has no configured settings.", config_args.id);
                } else {
                    for (key, value) in &extension.settings {
                        println!("{key} = {value}");
                    }
                }
            }
        }
        ExtensionCommand::Install(install) => {
            let id = extensions::install(&cwd, &install.source, install.project, install.force)?;
            let catalog = discover()?;
            let extension = catalog
                .extension(&id)
                .context("installed extension disappeared")?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(extension)?);
            } else {
                println!(
                    "Installed {} {} · {}",
                    extension.id,
                    extension.version,
                    extension
                        .reason
                        .as_deref()
                        .unwrap_or("active at the next turn boundary")
                );
            }
        }
        ExtensionCommand::Update(update) => {
            let updated = extensions::update(&cwd, update.id.as_deref(), update.project)?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&updated)?);
            } else if updated.is_empty() {
                println!("No Git-backed Blu extensions to update.");
            } else {
                println!("Updated {}", updated.join(", "));
            }
        }
        ExtensionCommand::Remove(target) => {
            let removed = extensions::remove(&cwd, &target.id, target.project)?;
            if args.json {
                println!(
                    "{}",
                    serde_json::json!({"removed": target.id, "path": removed})
                );
            } else {
                println!("Removed {} · {}", target.id, removed.display());
            }
        }
        ExtensionCommand::New(new) => {
            let path = extensions::scaffold(&cwd, &new.id, &new.version, new.project)?;
            if args.json {
                println!("{}", serde_json::json!({"id": new.id, "path": path}));
            } else {
                println!("Created {} · {}", new.id, path.display());
                println!(
                    "Edit blu.toml and skills/{}/SKILL.md; changes load at the next turn boundary.",
                    new.id
                );
            }
        }
        ExtensionCommand::Reload => {
            let catalog = discover()?;
            anyhow::ensure!(!catalog.has_errors(), "Blu catalog has errors");
            let signal = extensions::touch_reload(&cwd)?;
            if args.json {
                println!(
                    "{}",
                    serde_json::json!({"revision": catalog.revision, "signal": signal})
                );
            } else {
                println!(
                    "Blu catalog {} validated · running sessions apply it at the next turn boundary",
                    &catalog.revision[..catalog.revision.len().min(12)]
                );
            }
        }
    }
    Ok(())
}

fn print_extension_catalog(catalog: &extensions::ExtensionCatalog, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(catalog)?);
        return Ok(());
    }
    println!("Borg extensions");
    if catalog.extensions.is_empty() {
        println!("  No extensions installed. Try `borg extensions new <id> --project`.");
    }
    for extension in &catalog.extensions {
        println!(
            "  {:<20} {:<10} {:<8} {}",
            extension.id,
            extension.version,
            if extension.active {
                "active"
            } else {
                "inactive"
            },
            extension
                .reason
                .as_deref()
                .unwrap_or(extension.scope.label())
        );
        for (name, runtime) in &extension.workflow_runtimes {
            println!("    workflow {name}: {runtime}");
        }
    }
    for diagnostic in &catalog.diagnostics {
        println!(
            "  {}: {} — {}",
            diagnostic.level.label(),
            diagnostic.path.display(),
            diagnostic.message
        );
    }
    println!(
        "  {} installed · {} active · revision {}",
        catalog.extensions.len(),
        catalog
            .extensions
            .iter()
            .filter(|extension| extension.active)
            .count(),
        &catalog.revision[..catalog.revision.len().min(12)]
    );
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
