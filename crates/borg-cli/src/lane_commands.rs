//! Engine-neutral lane CLI. An adapter supplies a validated JobSpec; no shell interpolation.
use anyhow::{Context, Result, ensure};
use borg_lanes::lanes::{
    Capacity, JobHandle, JobSpec, JobState, LaneStore, ResourceKey, ResourceScope,
};
use clap::{Args, Subcommand};
use std::io::Read;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug, Args)]
pub(crate) struct LaneArgs {
    #[arg(long, global = true)]
    pub(crate) state_dir: Option<PathBuf>,
    #[arg(long, global = true)]
    pub(crate) json: bool,
    #[command(subcommand)]
    pub(crate) command: LaneCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum LaneCommand {
    Job {
        #[command(subcommand)]
        command: JobCommand,
    },
    Resource {
        #[command(subcommand)]
        command: ResourceCommand,
    },
    #[command(name = "__supervise", hide = true)]
    Supervise { id: Uuid },
}

#[derive(Debug, Subcommand)]
pub(crate) enum JobCommand {
    /// Submit an engine-neutral JSON JobSpec; `-` reads stdin. Returns immediately.
    Submit {
        #[arg(long)]
        spec: String,
    },
    /// Block on the supervisor's kernel completion lock, without polling.
    Wait {
        id: Uuid,
    },
    Status {
        id: Option<Uuid>,
    },
    Logs {
        id: Uuid,
    },
    Cancel {
        id: Uuid,
    },
    Recover {
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum ResourceCommand {
    List,
    Status,
    SetCapacity {
        #[arg(long)]
        name: String,
        #[arg(long)]
        slots: u32,
        #[arg(long)]
        scope: Option<PathBuf>,
    },
}

fn print_job(job: &JobHandle, json: bool) -> Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "job_id": job.id, "ticket": job.ticket, "state": job.state, "log_path": job.log_path,
            }))?
        );
    } else {
        println!("{} {:?} {}", job.id, job.state, job.log_path.display());
    }
    Ok(())
}

pub(crate) async fn run(args: LaneArgs) -> Result<()> {
    let store = LaneStore::new(args.state_dir.unwrap_or_else(LaneStore::default_root))?;
    let json = args.json;
    match args.command {
        LaneCommand::Supervise { id } => {
            let code = store.supervise(id)?;
            if code != 0 {
                std::process::exit(code.clamp(1, 255));
            }
        }
        LaneCommand::Job { command } => match command {
            JobCommand::Submit { spec } => {
                let text = if spec == "-" {
                    let mut buf = String::new();
                    std::io::stdin().read_to_string(&mut buf)?;
                    buf
                } else {
                    std::fs::read_to_string(&spec).with_context(|| format!("reading {spec}"))?
                };
                let spec: JobSpec = serde_json::from_str(&text).context("invalid JobSpec JSON")?;
                print_job(&store.enqueue_job(spec)?, json)?;
            }
            JobCommand::Wait { id } => {
                let job = tokio::task::spawn_blocking(move || store.wait_job(id)).await??;
                print_job(&job, json)?;
                match job.state {
                    JobState::Finished { exit_code } if exit_code != 0 => {
                        std::process::exit(exit_code.clamp(1, 255))
                    }
                    JobState::Cancelled { .. } => std::process::exit(125),
                    _ => {}
                }
            }
            JobCommand::Status { id } => {
                if let Some(id) = id {
                    if json {
                        println!(
                            "{}",
                            serde_json::to_string(
                                &store
                                    .snapshot()?
                                    .into_iter()
                                    .find(|r| r.ticket.id == id)
                                    .context("unknown job")?
                            )?
                        );
                    } else {
                        print_job(&store.job_status(id)?, false)?;
                    }
                } else if json {
                    println!("{}", serde_json::to_string(&store.snapshot()?)?);
                } else {
                    for r in store.snapshot()? {
                        println!(
                            "{} {:?} {}",
                            r.ticket.id,
                            r.state,
                            r.wait_reason.unwrap_or_default()
                        );
                    }
                }
            }
            JobCommand::Logs { id } => {
                let job = store.job_status(id)?;
                if json {
                    println!(
                        "{}",
                        serde_json::json!({"job_id": id, "log_path": job.log_path})
                    );
                } else {
                    println!("{}", job.log_path.display());
                }
            }
            JobCommand::Cancel { id } => {
                store.cancel_ticket(id, "cancelled by requester")?;
                if json {
                    println!("{}", serde_json::json!({"job_id":id,"state":"cancelled"}));
                }
            }
            JobCommand::Recover { dry_run } => {
                let actions = store.recover(dry_run)?;
                if json {
                    println!("{}", serde_json::to_string(&actions)?);
                } else {
                    for action in actions {
                        println!("{action}");
                    }
                }
            }
        },
        LaneCommand::Resource { command } => match command {
            ResourceCommand::List | ResourceCommand::Status => {
                let records = store.snapshot()?;
                if json {
                    println!("{}", serde_json::to_string(&records)?);
                } else {
                    for r in records {
                        println!(
                            "{} {:?} {}",
                            r.ticket.id,
                            r.state,
                            r.wait_reason.unwrap_or_default()
                        );
                    }
                }
            }
            ResourceCommand::SetCapacity { name, slots, scope } => {
                ensure!(slots > 0, "capacity must be positive");
                let scope = match scope {
                    Some(path) => ResourceScope::Worktree(std::fs::canonicalize(path)?),
                    None => ResourceScope::Host,
                };
                store.set_capacity(Capacity {
                    key: ResourceKey { scope, name },
                    slots,
                })?;
                if json {
                    println!("{}", serde_json::json!({"slots":slots}));
                }
            }
        },
    }
    Ok(())
}
