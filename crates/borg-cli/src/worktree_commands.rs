use std::path::PathBuf;

use anyhow::{Context, Result, ensure};
use borg_lanes::workspace::hygiene::{self, WorkspaceBudgets};
use serde_json::Value;
use uuid::Uuid;

use crate::cli::{WorktreeArgs, WorktreeCommand};

async fn local_instances() -> (Vec<(Uuid, PathBuf)>, Vec<Uuid>) {
    let Ok(value) = crate::agent_mcp::workspace_instances().await else {
        return (Vec::new(), Vec::new());
    };
    let mut live = Vec::new();
    let mut exited = Vec::new();
    for row in value
        .get("instances")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(id) = row
            .get("id")
            .and_then(Value::as_str)
            .and_then(|s| s.parse().ok())
        else {
            continue;
        };
        if row.get("local").and_then(Value::as_bool) != Some(true) {
            continue;
        }
        if row.get("live").and_then(Value::as_bool) == Some(true)
            || row.get("owner_running").and_then(Value::as_bool) == Some(true)
        {
            if let Some(path) = row.get("cwd").and_then(Value::as_str) {
                live.push((id, PathBuf::from(path)));
            }
        } else if row.get("exited_at").is_some_and(|v| !v.is_null()) {
            exited.push(id);
        }
    }
    (live, exited)
}

pub(crate) async fn run(args: WorktreeArgs) -> Result<()> {
    let repo = args.project.canonicalize().context("project directory")?;
    if let WorktreeCommand::Monitor { interval_secs } = &args.command {
        ensure!(
            (10..=3600).contains(interval_secs),
            "monitor interval must be 10..3600 seconds"
        );
        let budget = WorkspaceBudgets::from_env()?;
        loop {
            let available = hygiene::disk_available(&repo)?;
            let ram = hygiene::ram_available()?;
            let admission = hygiene::assess_admission(&budget, available, ram, 0, 0, 0, 0);
            if let Some(reason) = admission.reason {
                use std::io::Write;
                println!("workspace pressure: {reason}");
                std::io::stdout().flush()?;
            }
            std::thread::park_timeout(std::time::Duration::from_secs(*interval_secs));
        }
    }
    let (active, exited) = local_instances().await;
    let budgets = WorkspaceBudgets::from_env()?;
    let value = match args.command {
        WorktreeCommand::New {
            task,
            owner,
            shared_cargo,
        } => {
            let owner = owner.unwrap_or_else(|| {
                active
                    .iter()
                    .find(|(_, cwd)| cwd == &repo)
                    .map(|(id, _)| *id)
                    .unwrap_or_else(Uuid::new_v4)
            });
            let root = args
                .root
                .or_else(|| std::env::var_os("BORG_WORKTREE_ROOT").map(PathBuf::from))
                .unwrap_or_else(|| repo.parent().unwrap_or(&repo).join("borg-wt"));
            serde_json::to_value(hygiene::create_worktree(
                &repo,
                &root,
                &task,
                owner,
                shared_cargo,
                &budgets,
            )?)?
        }
        WorktreeCommand::List => {
            let mut trees = hygiene::inventory(&repo, &active)?;
            for tree in &mut trees {
                tree.owner_gone = tree.owner.is_some_and(|id| exited.contains(&id));
            }
            serde_json::to_value(trees)?
        }
        WorktreeCommand::Gc { apply, force } => {
            if apply {
                use std::io::IsTerminal;
                ensure!(
                    std::io::stdin().is_terminal() && std::io::stdout().is_terminal(),
                    "GC deletion requires a human at a terminal to confirm each exact path; model tools are dry-run only"
                );
                ensure!(!exited.is_empty(), "no journal-confirmed exited owners");
            }
            let trees = hygiene::inventory(&repo, &active)?;
            let mut candidates = Vec::new();
            for mut tree in trees {
                let exit_confirmed = tree.owner.is_some_and(|id| exited.contains(&id));
                tree.owner_gone = exit_confirmed;
                let eligible = tree.gc_reason.is_some() && exit_confirmed
                    || (force
                        && (tree.merged || tree.abandoned)
                        && !tree.owner_live
                        && tree.owner.is_some()
                        && exit_confirmed);
                let protection = if tree.owner.is_none() {
                    "unmanaged or unknown owner"
                } else if tree.owner_live {
                    "live owner"
                } else if !tree.merged && !tree.abandoned {
                    "unmerged branch"
                } else if tree.dirty && !force {
                    "dirty; explicit force required"
                } else if !exit_confirmed {
                    "owner exit unconfirmed"
                } else {
                    "none"
                };
                let removed = if apply && eligible {
                    use std::io::Write;
                    eprintln!(
                        "Delete worktree {} ({} bytes, dirty={})? Type its exact path:",
                        tree.path.display(),
                        tree.size_bytes,
                        tree.dirty
                    );
                    std::io::stderr().flush()?;
                    let mut response = String::new();
                    std::io::stdin().read_line(&mut response)?;
                    ensure!(
                        response.trim_end_matches(['\r', '\n']) == tree.path.to_string_lossy(),
                        "GC cancelled: typed path does not match"
                    );
                    let (fresh_active, fresh_exited) = local_instances().await;
                    ensure!(
                        tree.owner.is_some_and(|id| fresh_exited.contains(&id)),
                        "owner exit is no longer confirmed"
                    );
                    let fresh = hygiene::inventory(&repo, &fresh_active)?
                        .into_iter()
                        .find(|record| record.path == tree.path)
                        .context("worktree disappeared")?;
                    ensure!(
                        !fresh.owner_live,
                        "a session became active in this worktree"
                    );
                    hygiene::gc(&repo, &fresh, &fresh_exited, true, force)?
                } else {
                    false
                };
                candidates.push(serde_json::json!({"tree":tree,"eligible":eligible,
                    "protection":protection,"owner_exit_confirmed":exit_confirmed,"removed":removed}));
            }
            serde_json::json!({"dry_run": !apply, "candidates":candidates})
        }
        WorktreeCommand::Monitor { .. } => unreachable!("handled before discovery"),
        WorktreeCommand::TargetStatus { cap_gib } => serde_json::to_value(hygiene::target_usage(
            &hygiene::inventory(&repo, &active)?,
            cap_gib
                .checked_mul(1024 * 1024 * 1024)
                .context("cap overflows u64")?,
        )?)?,
        WorktreeCommand::Budget => {
            let available = hygiene::disk_available(&repo)?;
            let ram = hygiene::ram_available()?;
            serde_json::to_value(hygiene::assess_admission(
                &budgets, available, ram, 0, 0, 0, 0,
            ))?
        }
        WorktreeCommand::FreezePreview { globs } => {
            serde_json::to_value(hygiene::freeze_preview(&repo, &globs, &active)?)?
        }
        WorktreeCommand::Freeze {
            work_id,
            owner,
            reason,
            deadline_secs,
            globs,
        } => serde_json::to_value(hygiene::request_freeze(
            &repo,
            work_id,
            owner,
            globs,
            reason,
            deadline_secs,
            &active,
        )?)?,
        WorktreeCommand::FreezeStatus => serde_json::to_value(hygiene::freeze_status(&repo)?)?,
        WorktreeCommand::FreezeAck { id, participant } => {
            serde_json::to_value(hygiene::acknowledge_freeze(&repo, id, participant)?)?
        }
        WorktreeCommand::FreezeLand { id, owner, note } => {
            serde_json::to_value(hygiene::land_freeze(&repo, id, owner, note, &active)?)?
        }
        WorktreeCommand::FreezeRelease { id, owner, abort } => {
            serde_json::to_value(hygiene::finish_freeze(&repo, id, owner, abort)?)?
        }
    };
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}
