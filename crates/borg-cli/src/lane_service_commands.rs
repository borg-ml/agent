//! CLI for host-local shared services; the internal supervisor command runs outside an agent session.
use anyhow::{Context, Result, ensure};
use borg_lanes::{
    lanes::{Holder, LaneStore},
    services::{self, ServiceManager, ServiceRequest, ServiceSpec, ServiceState, ServiceStatus},
};
use clap::{Args, Subcommand};
use std::{fs, path::PathBuf, time::Duration};
use uuid::Uuid;

#[derive(Debug, Args)]
pub(crate) struct ServiceArgs {
    #[command(subcommand)]
    pub(crate) command: ServiceCommand,
    #[arg(skip)]
    pub(crate) json: bool,
    /// Filled by the enclosing `borg lane --state-dir` parser, not a second CLI option.
    #[arg(skip)]
    pub(crate) root: Option<PathBuf>,
}
#[derive(Debug, Subcommand)]
pub(crate) enum ServiceCommand {
    /// Start exactly one service per key from a validated JSON definition.
    Start {
        id: String,
        #[arg(long)]
        definition: PathBuf,
        #[arg(long, default_value_t = 120)]
        wait_ready: u64,
    },
    Stop {
        id: String,
    },
    Restart {
        id: String,
        #[arg(long, default_value = "requested")]
        reason: String,
        #[arg(long)]
        force: bool,
    },
    Status {
        id: String,
    },
    Logs {
        id: String,
        #[arg(long, default_value_t = 60)]
        lines: usize,
    },
    /// Stop the backend before a lane exclusive job is granted.
    Yield {
        id: String,
        #[arg(long)]
        by: String,
        #[arg(long, default_value = "exclusive job")]
        reason: String,
        #[arg(long, default_value_t = 7200)]
        for_seconds: u64,
    },
    /// Clear only this job's yield window.
    Resume {
        id: String,
        #[arg(long)]
        by: String,
    },
    Lease {
        id: String,
        #[arg(long)]
        owner: String,
        #[arg(long, default_value = "work")]
        purpose: String,
        #[arg(long, default_value_t = 600)]
        ttl_seconds: u64,
    },
    Release {
        id: String,
        #[arg(long)]
        owner: String,
        #[arg(long)]
        lease_id: Option<Uuid>,
    },
    /// Mark the service active, undoing adapter-defined idle throttle.
    Touch {
        id: String,
    },
    #[command(hide = true)]
    Supervise {
        id: String,
    },
}
fn holder(owner: &str, purpose: &str) -> Result<Holder> {
    ensure!(
        !owner.is_empty() && owner.len() <= 200 && !owner.chars().any(char::is_control),
        "invalid service owner"
    );
    // Stable across CLI calls without pretending that an owner string proves OS identity.
    let id = Uuid::parse_str(owner)
        .unwrap_or_else(|_| Uuid::new_v5(&Uuid::NAMESPACE_OID, owner.as_bytes()));
    Ok(Holder {
        participant_id: id,
        session_id: id,
        host_pid: None,
        purpose: purpose.into(),
    })
}
fn display(status: &ServiceStatus, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(status)?);
    } else {
        println!(
            "{}: {:?} · {} · backend {:?} · {} client(s)",
            status.id,
            status.state,
            status.reason,
            status.backend_pid,
            status.clients.len()
        );
    }
    Ok(())
}
pub(crate) async fn run(args: ServiceArgs) -> Result<()> {
    let root = args
        .root
        .unwrap_or_else(LaneStore::default_root)
        .join("services");
    let manager = ServiceManager::new(root, std::env::current_exe()?);
    let timeout = Duration::from_secs(120);
    match args.command {
        ServiceCommand::Start {
            id,
            definition,
            wait_ready,
        } => {
            let spec: ServiceSpec = serde_json::from_slice(
                &fs::read(&definition).with_context(|| format!("read {}", definition.display()))?,
            )?;
            ensure!(spec.id == id, "service id must match definition id");
            manager.launch(spec).await?;
            let until = tokio::time::Instant::now() + Duration::from_secs(wait_ready);
            loop {
                let status = manager.read_status(&id)?;
                if matches!(
                    status.state,
                    ServiceState::Healthy { .. }
                        | ServiceState::Yielded
                        | ServiceState::Failed { .. }
                ) || tokio::time::Instant::now() >= until
                {
                    display(&status, args.json)?;
                    ensure!(
                        matches!(
                            status.state,
                            ServiceState::Healthy { .. } | ServiceState::Yielded
                        ),
                        "service not ready: {}",
                        status.reason
                    );
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        ServiceCommand::Status { id } => display(&manager.read_status(&id)?, args.json)?,
        ServiceCommand::Stop { id } => display(
            &manager.send(&id, ServiceRequest::Stop, timeout).await?,
            args.json,
        )?,
        ServiceCommand::Restart { id, reason, force } => display(
            &manager
                .send(&id, ServiceRequest::Restart { reason, force }, timeout)
                .await?,
            args.json,
        )?,
        ServiceCommand::Yield {
            id,
            by,
            reason,
            for_seconds,
        } => display(
            &manager
                .send(
                    &id,
                    ServiceRequest::Yield {
                        by,
                        reason,
                        ttl_ms: for_seconds
                            .checked_mul(1000)
                            .context("yield duration overflow")?,
                    },
                    timeout,
                )
                .await?,
            args.json,
        )?,
        ServiceCommand::Resume { id, by } => display(
            &manager
                .send(&id, ServiceRequest::Resume { by }, timeout)
                .await?,
            args.json,
        )?,
        ServiceCommand::Lease {
            id,
            owner,
            purpose,
            ttl_seconds,
        } => {
            let holder = holder(&owner, &purpose)?;
            let status = manager
                .send(
                    &id,
                    ServiceRequest::Lease {
                        owner: holder,
                        purpose,
                        ttl_ms: ttl_seconds
                            .checked_mul(1000)
                            .context("lease duration overflow")?,
                    },
                    timeout,
                )
                .await?;
            display(&status, args.json)?;
        }
        ServiceCommand::Release {
            id,
            owner,
            lease_id,
        } => {
            let holder = holder(&owner, "")?;
            let status = manager.read_status(&id)?;
            let lease_id = lease_id
                .or_else(|| {
                    status
                        .clients
                        .iter()
                        .find(|lease| lease.owner.participant_id == holder.participant_id)
                        .map(|lease| lease.id)
                })
                .context("no client lease held by owner")?;
            display(
                &manager
                    .send(
                        &id,
                        ServiceRequest::Release {
                            lease_id,
                            owner: holder,
                        },
                        timeout,
                    )
                    .await?,
                args.json,
            )?;
        }
        ServiceCommand::Touch { id } => display(
            &manager.send(&id, ServiceRequest::Touch, timeout).await?,
            args.json,
        )?,
        ServiceCommand::Logs { id, lines } => {
            ensure!(lines <= 1000, "--lines must be <= 1000");
            let path = manager.root.join(&id).join("output.log");
            // Validate ID before constructing a log path; status also checks supervisor liveness.
            manager.read_status(&id)?;
            let text =
                fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
            let tail = text
                .lines()
                .rev()
                .take(lines)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("\n");
            if args.json {
                println!(
                    "{}",
                    serde_json::json!({"id":id,"log_path":path,"output":tail})
                );
            } else {
                println!("{tail}");
            }
        }
        ServiceCommand::Supervise { id } => services::supervise(&manager.root, &id).await?,
    }
    Ok(())
}
