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
    /// Supervised shared services bound to lane resources.
    Service(crate::lane_service_commands::ServiceArgs),
    /// Same as `job recover`.
    Recover(RecoverArgs),
    #[command(name = "__supervise", hide = true)]
    Supervise { id: Uuid },
    #[command(name = "__resume_services", hide = true)]
    ResumeServices { id: Uuid },
}

#[derive(Debug, Subcommand)]
pub(crate) enum JobCommand {
    /// Submit an engine-neutral JSON JobSpec; `-` reads stdin. Returns immediately.
    Submit {
        #[arg(long)]
        spec: String,
        /// Override the five-minute foreign-client wait; zero waits indefinitely.
        #[arg(long)]
        foreign_lease_grace_seconds: Option<u64>,
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
    Recover(RecoverArgs),
}

/// Recover lost jobs and restart pending service resumes.
#[derive(Debug, clap::Args)]
pub(crate) struct RecoverArgs {
    /// Report what recovery would do without doing it.
    #[arg(long, conflicts_with = "wait")]
    dry_run: bool,
    /// Block up to SECONDS until each started service resume clears or
    /// fails; exits nonzero unless every one resumed.
    #[arg(long, value_name = "SECONDS")]
    wait: Option<u64>,
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

fn recover(store: &LaneStore, args: RecoverArgs, json: bool) -> Result<()> {
    let Some(seconds) = args.wait else {
        let actions = store.recover(args.dry_run)?;
        if json {
            println!("{}", serde_json::to_string(&actions)?);
        } else {
            for action in actions {
                println!("{action}");
            }
        }
        return Ok(());
    };
    let (actions, resumes) = store.recover_wait(std::time::Duration::from_secs(seconds))?;
    if json {
        println!(
            "{}",
            serde_json::json!({"actions": actions, "resumes": resumes})
        );
    } else {
        for action in &actions {
            println!("{action}");
        }
        for resume in &resumes {
            match resume.outcome {
                "resumed" => println!("job {}: resumed", resume.job_id),
                outcome => println!(
                    "job {}: {outcome} ({}): {}",
                    resume.job_id,
                    resume.pending.join(", "),
                    resume.error.as_deref().unwrap_or("no attempt finished yet")
                ),
            }
        }
    }
    let unresolved = resumes.iter().filter(|r| r.outcome != "resumed").count();
    ensure!(
        unresolved == 0,
        "{unresolved} service resume(s) failed or still pending"
    );
    Ok(())
}

pub(crate) async fn run(args: LaneArgs) -> Result<()> {
    let store = LaneStore::new(args.state_dir.unwrap_or_else(LaneStore::default_root))?;
    let json = args.json;
    match args.command {
        LaneCommand::Service(mut service) => {
            service.root = Some(store.root().to_path_buf());
            service.json |= json;
            crate::lane_service_commands::run(service).await?;
        }
        LaneCommand::ResumeServices { id } => store.resume_services(id)?,
        LaneCommand::Supervise { id } => {
            let code = store.supervise(id)?;
            if code != 0 {
                std::process::exit(code.clamp(1, 255));
            }
        }
        LaneCommand::Job { command } => match command {
            JobCommand::Submit {
                spec,
                foreign_lease_grace_seconds,
            } => {
                let text = if spec == "-" {
                    let mut buf = String::new();
                    std::io::stdin().read_to_string(&mut buf)?;
                    buf
                } else {
                    std::fs::read_to_string(&spec).with_context(|| format!("reading {spec}"))?
                };
                let mut spec: JobSpec =
                    serde_json::from_str(&text).context("invalid JobSpec JSON")?;
                if let Some(seconds) = foreign_lease_grace_seconds {
                    ensure!(
                        seconds <= 86_400,
                        "foreign lease grace must be <= 86400 seconds"
                    );
                    spec.foreign_client_grace_ms = seconds * 1000;
                }
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
            JobCommand::Recover(args) => recover(&store, args, json)?,
        },
        LaneCommand::Recover(args) => recover(&store, args, json)?,
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
