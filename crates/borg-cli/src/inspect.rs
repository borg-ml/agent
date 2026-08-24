use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use borg_remote::{
    RuntimeProfileSnapshot, default_host_config_path, read_runtime_profile, runtime_profile_path,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::cli::{InspectArgs, InspectCommand};

const OWNER_FILE_SUFFIX: &str = ".control.owner.json";
const MAX_HOTSPOTS: usize = 3;

#[derive(Debug, Clone, Deserialize, Serialize)]
struct OwnerMetadata {
    schema_version: u8,
    pid: u32,
    executable_identity: String,
    #[serde(default)]
    process_start_time: Option<u64>,
}

#[derive(Debug, Serialize)]
struct LiveSessionInspection {
    session_id: Uuid,
    owner: Option<OwnerInspection>,
    running: bool,
    profile_path: PathBuf,
    profile: Option<RuntimeProfileSnapshot>,
    hotspots: Vec<ProfileHotspot>,
    profile_error: Option<String>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct ProfileHotspot {
    phase: String,
    total_ms: u64,
    count: u64,
    average_ms: u64,
    p95_ms: u64,
    max_ms: u64,
    share_percent: u8,
}

#[derive(Debug, Serialize)]
struct OwnerInspection {
    pid: u32,
    running: bool,
    executable_identity: String,
    process_start_time: Option<u64>,
}

pub(crate) async fn run(args: InspectArgs) -> Result<()> {
    let command = args
        .command
        .unwrap_or(InspectCommand::Live { session: None });
    match command {
        InspectCommand::Live { session } => inspect_live(session, args.json),
    }
}

fn inspect_live(requested: Option<Uuid>, json: bool) -> Result<()> {
    let sessions_dir = sessions_dir();
    let session_ids = match requested {
        Some(session_id) => vec![session_id],
        None => discover_owner_sessions(&sessions_dir)?,
    };
    let mut inspections = Vec::with_capacity(session_ids.len());
    for session_id in session_ids {
        inspections.push(inspect_session(&sessions_dir, session_id)?);
    }
    if requested.is_none() {
        inspections.retain(|inspection| inspection.running);
    }

    if json {
        println!("{}", serde_json::to_string_pretty(&inspections)?);
    } else if inspections.is_empty() {
        println!("No live Borg session owners found.");
        println!("Start a profiling build with BORG_PROFILE=1 to collect runtime data.");
    } else {
        for inspection in inspections {
            print_human_inspection(&inspection);
        }
    }
    Ok(())
}

fn sessions_dir() -> PathBuf {
    default_host_config_path()
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("sessions")
}

fn discover_owner_sessions(sessions_dir: &Path) -> Result<Vec<Uuid>> {
    let entries = match fs::read_dir(sessions_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to read {}", sessions_dir.display()));
        }
    };
    let mut session_ids = entries
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let name = entry.file_name();
            let name = name.to_str()?;
            let session = name.strip_suffix(OWNER_FILE_SUFFIX)?;
            Uuid::parse_str(session).ok()
        })
        .collect::<Vec<_>>();
    session_ids.sort_unstable();
    Ok(session_ids)
}

fn inspect_session(sessions_dir: &Path, session_id: Uuid) -> Result<LiveSessionInspection> {
    let owner_path = sessions_dir.join(format!("{session_id}{OWNER_FILE_SUFFIX}"));
    let owner = match fs::read(&owner_path) {
        Ok(bytes) => {
            let metadata: OwnerMetadata = serde_json::from_slice(&bytes)
                .with_context(|| format!("invalid owner metadata {}", owner_path.display()))?;
            (metadata.schema_version == 1).then(|| OwnerInspection {
                pid: metadata.pid,
                running: process_is_alive(&metadata),
                executable_identity: metadata.executable_identity,
                process_start_time: metadata.process_start_time,
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {}", owner_path.display()));
        }
    };
    let profile_path = runtime_profile_path(sessions_dir, session_id);
    let (profile, profile_error) = match read_runtime_profile(&profile_path) {
        Ok(profile) => (Some(profile), None),
        Err(_) if !profile_path.exists() => (None, None),
        Err(error) => (None, Some(error.to_string())),
    };
    let hotspots = profile.as_ref().map(profile_hotspots).unwrap_or_default();
    Ok(LiveSessionInspection {
        session_id,
        running: owner.as_ref().is_some_and(|owner| owner.running),
        owner,
        profile_path,
        profile,
        hotspots,
        profile_error,
    })
}

fn profile_hotspots(profile: &RuntimeProfileSnapshot) -> Vec<ProfileHotspot> {
    let measured_ms = profile
        .phases
        .values()
        .map(|stats| u128::from(stats.total_ms))
        .sum::<u128>();
    if measured_ms == 0 {
        return Vec::new();
    }
    let mut hotspots = profile
        .phases
        .iter()
        .filter(|(_, stats)| stats.total_ms > 0)
        .map(|(phase, stats)| ProfileHotspot {
            phase: phase.clone(),
            total_ms: stats.total_ms,
            count: stats.count,
            average_ms: stats.average_ms,
            p95_ms: stats.p95_ms,
            max_ms: stats.max_ms,
            share_percent: u8::try_from(
                ((u128::from(stats.total_ms) * 100) + measured_ms / 2) / measured_ms,
            )
            .unwrap_or(100),
        })
        .collect::<Vec<_>>();
    hotspots.sort_by(|left, right| {
        right
            .total_ms
            .cmp(&left.total_ms)
            .then_with(|| left.phase.cmp(&right.phase))
    });
    hotspots.truncate(MAX_HOTSPOTS);
    hotspots
}

fn print_human_inspection(inspection: &LiveSessionInspection) {
    println!(
        "{} · {}",
        inspection.session_id,
        if inspection.running {
            "running"
        } else {
            "stale or stopped"
        }
    );
    match &inspection.owner {
        Some(owner) => println!(
            "  owner: pid={} · {} · executable={}",
            owner.pid,
            if owner.running { "alive" } else { "not alive" },
            owner.executable_identity
        ),
        None => println!("  owner: none"),
    }
    match &inspection.profile {
        Some(profile) => {
            println!(
                "  profile: enabled · {} completed turn(s) · updated {}",
                profile.turns_completed, profile.updated_at
            );
            if let Some(active) = &profile.active_turn {
                println!(
                    "  active: {} · phase={}",
                    active.provider,
                    profile.current_phase.as_deref().unwrap_or("unknown")
                );
            }
            if let Some(last_turn) = &profile.last_turn {
                println!(
                    "  last turn: {} · {} ms · {}",
                    last_turn.provider,
                    last_turn.duration_ms,
                    if last_turn.success { "ok" } else { "failed" }
                );
            }
            if !inspection.hotspots.is_empty() {
                println!("  hotspots by measured wall time:");
                for hotspot in &inspection.hotspots {
                    println!(
                        "    {}: {}% · total={} ms · count={} · avg={} ms p95={} ms max={} ms",
                        hotspot.phase,
                        hotspot.share_percent,
                        hotspot.total_ms,
                        hotspot.count,
                        hotspot.average_ms,
                        hotspot.p95_ms,
                        hotspot.max_ms
                    );
                }
            }
        }
        None if inspection.profile_error.is_some() => {
            println!(
                "  profile: unreadable — {}",
                inspection
                    .profile_error
                    .as_deref()
                    .unwrap_or("unknown error")
            );
        }
        None => println!(
            "  profile: off · use a profiling build with BORG_PROFILE=1; path={}",
            inspection.profile_path.display()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use borg_remote::RuntimeProfilePhase;
    use chrono::Utc;

    #[test]
    fn hotspot_ranking_uses_measured_time_and_is_bounded() {
        let mut phases = std::collections::BTreeMap::new();
        for (name, total_ms, count) in [
            ("provider_wait", 800_u64, 4_u64),
            ("model_output", 150, 3),
            ("tool_execution", 50, 2),
            ("prepare", 1, 1),
        ] {
            phases.insert(
                name.to_string(),
                RuntimeProfilePhase {
                    count,
                    total_ms,
                    max_ms: total_ms,
                    average_ms: total_ms / count,
                    p95_ms: total_ms,
                    buckets: [0; 13],
                },
            );
        }
        let profile = RuntimeProfileSnapshot {
            schema_version: 1,
            session_id: Uuid::nil(),
            pid: 1,
            started_at: Utc::now(),
            updated_at: Utc::now(),
            active_turn: None,
            current_phase: None,
            current_phase_started_at: None,
            turns_completed: 1,
            last_turn: None,
            phases,
        };

        let hotspots = profile_hotspots(&profile);

        assert_eq!(hotspots.len(), 3);
        assert_eq!(
            hotspots[0],
            ProfileHotspot {
                phase: "provider_wait".to_string(),
                total_ms: 800,
                count: 4,
                average_ms: 200,
                p95_ms: 800,
                max_ms: 800,
                share_percent: 80,
            }
        );
        assert_eq!(hotspots[1].phase, "model_output");
        assert_eq!(hotspots[2].phase, "tool_execution");
    }
}

#[cfg(target_os = "linux")]
fn process_is_alive(metadata: &OwnerMetadata) -> bool {
    if !process_signal_is_alive(metadata.pid) {
        return false;
    }
    let executable = PathBuf::from("/proc")
        .join(metadata.pid.to_string())
        .join("exe");
    let Ok(file_metadata) = fs::metadata(executable) else {
        return false;
    };
    if executable_identity(&file_metadata) != metadata.executable_identity {
        return false;
    }
    metadata
        .process_start_time
        .is_none_or(|expected| process_start_time(metadata.pid) == Some(expected))
}

#[cfg(unix)]
fn process_signal_is_alive(pid: u32) -> bool {
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    let result = unsafe { libc::kill(pid, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(target_os = "linux")]
fn executable_identity(metadata: &fs::Metadata) -> String {
    use std::os::unix::fs::MetadataExt;

    format!(
        "{}:{}:{}:{}:{}",
        metadata.dev(),
        metadata.ino(),
        metadata.len(),
        metadata.mtime(),
        metadata.mtime_nsec()
    )
}

#[cfg(target_os = "linux")]
fn process_start_time(pid: u32) -> Option<u64> {
    let contents =
        fs::read_to_string(PathBuf::from("/proc").join(pid.to_string()).join("stat")).ok()?;
    let (_, fields) = contents.rsplit_once(") ")?;
    fields.split_whitespace().nth(19)?.parse().ok()
}

#[cfg(all(unix, not(target_os = "linux")))]
fn process_is_alive(metadata: &OwnerMetadata) -> bool {
    process_signal_is_alive(metadata.pid)
}

#[cfg(not(unix))]
fn process_is_alive(_metadata: &OwnerMetadata) -> bool {
    true
}
