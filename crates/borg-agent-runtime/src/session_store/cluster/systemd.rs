//! Start the managed postmaster under its own systemd user unit.
//!
//! `pg_ctl start` daemonises, and systemd tracks processes by cgroup rather
//! than by parent. A postmaster started that way therefore stays in the cgroup
//! of whichever Borg process happened to start it, and `KillMode=control-group`
//! -- systemd's default -- sends SIGTERM to every process in that cgroup when
//! the unit stops. PostgreSQL reads SIGTERM as a smart shutdown, so restarting
//! the Borg process that started the cluster takes the database down with it.
//!
//! That is not hypothetical and it is not specific to one unit. On 2026-09-20
//! a restart of `borg-remote.service` at 04:21:51 produced "received smart
//! shutdown request" at 04:21:51.695 and rejected 183 connections in 855 ms.
//! An ordinary Borg CLI is no safer: it runs inside a transient
//! `borg-session-<uuid>.scope`, so a cluster it started dies when that session
//! ends.
//!
//! Giving the postmaster its own unit puts it in a cgroup no Borg process's
//! teardown reaches. The unit is deliberately tied to nothing: no `PartOf=`,
//! no `BindsTo=`, no `WantedBy=`. Being unreachable from Borg's own lifecycle
//! is the whole point, and it is also why nothing here couples the database to
//! host enrollment.
//!
//! This module never installs a unit file and never stops a cluster. It starts
//! one, and it reports what it can prove about where the result landed.

use std::ffi::OsString;
use std::path::Path;

/// Everything the postmaster needs, so argument construction stays a pure
/// function of its inputs and can be tested without a systemd on the machine.
pub(super) struct Launch<'a> {
    pub postgres: &'a Path,
    pub data_dir: &'a Path,
    pub socket_dir: &'a Path,
    pub log_path: &'a Path,
    pub port: u16,
}

/// What a running postmaster's cgroup proves about who can stop it.
///
/// Measured from `/proc`, not asserted from what we intended to do: the same
/// measurement that identified the original defect. A caller may warn on
/// anything short of [`Supervision::OwnUnit`], but must not act on it --
/// restarting a live cluster to improve its supervision would cause exactly
/// the outage this module exists to prevent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Supervision {
    /// In its own unit's cgroup. Only systemd stops it.
    OwnUnit,
    /// Somewhere else. If that cgroup belongs to a Borg service or session
    /// scope, this cluster dies when that unit does.
    ForeignCgroup {
        cgroup: String,
        /// True when it shares this process's own cgroup, which is the
        /// sharpest form of the problem: it cannot outlive us.
        shared_with_caller: bool,
    },
    /// No cgroup information available. Reported as its own state rather than
    /// guessed either way.
    Unknown,
}

impl Supervision {
    /// Whether this cluster is safe from Borg's own lifecycle.
    pub(super) fn is_isolated(&self) -> bool {
        matches!(self, Self::OwnUnit)
    }

    /// What to tell an operator, or `None` when there is nothing to say.
    pub(super) fn warning(&self) -> Option<String> {
        match self {
            Self::OwnUnit | Self::Unknown => None,
            Self::ForeignCgroup {
                cgroup,
                shared_with_caller,
            } => Some(format!(
                "the session cluster is running in {cgroup}, not its own unit, so stopping \
                 that unit will shut the database down{}. It will move to its own unit the \
                 next time it is started; Borg will not restart it now, because that would \
                 cause the outage it avoids.",
                if *shared_with_caller {
                    " -- and it shares this process's cgroup, so it cannot outlive this process"
                } else {
                    ""
                }
            )),
        }
    }
}

/// The unit that owns one data directory's postmaster.
///
/// Keyed on the data directory rather than the home or the port, because the
/// data directory is what the postmaster actually locks: two Borg homes get
/// two units, and two processes sharing a home get the same one. That
/// collision is wanted. A second concurrent start fails because the unit
/// already exists, which is re-checked against live state and treated as
/// success -- the same rule the rest of this cluster module already follows,
/// where losing a race is indistinguishable from having had nothing to do.
pub(super) fn unit_name(data_dir: &Path) -> String {
    use sha2::{Digest, Sha256};

    // Canonicalise when the path resolves, so two spellings of one directory
    // do not produce two units for one cluster. An absent directory (the very
    // first start) has no canonical form, and its literal path is stable
    // enough to name the unit that is about to create it.
    let identity = data_dir
        .canonicalize()
        .unwrap_or_else(|_| data_dir.to_path_buf());
    let mut hasher = Sha256::new();
    hasher.update(identity.as_os_str().as_encoded_bytes());
    let digest = format!("{:x}", hasher.finalize());
    format!("borg-postgres-{}.service", &digest[..16])
}

/// The `systemd-run` invocation that starts this cluster.
///
/// `postgres` directly rather than `pg_ctl start`: under a unit the postmaster
/// has to be the main process in the foreground, and daemonising would hand
/// systemd a process that immediately exits.
pub(super) fn run_args(launch: &Launch<'_>, unit: &str) -> Vec<OsString> {
    let mut args: Vec<OsString> = vec![
        "--user".into(),
        format!("--unit={unit}").into(),
        // Without this a unit left in the failed state blocks the next start
        // under the same name, and the name is deliberately stable.
        "--collect".into(),
        {
            let mut description = OsString::from("--description=Borg session cluster ");
            description.push(launch.data_dir);
            description
        },
    ];
    // The cluster's log stays the file the rest of this module already points
    // operators at; a unit would otherwise send it to the journal and quietly
    // make that existing message wrong.
    for stream in ["StandardOutput", "StandardError"] {
        let mut property = OsString::from(format!("--property={stream}=append:"));
        property.push(launch.log_path);
        args.push(property);
    }
    args.push("--".into());
    args.push(launch.postgres.into());
    args.push("-D".into());
    args.push(launch.data_dir.into());
    args.push("-p".into());
    args.push(launch.port.to_string().into());
    args.push("-k".into());
    args.push(launch.socket_dir.into());
    args.push("-h".into());
    args.push("127.0.0.1".into());
    args
}

/// Classify a postmaster's cgroup against the unit it should be in.
///
/// Comparing against the caller's cgroup alone is not enough, and the original
/// incident is why: the postmaster sat in `borg-remote.service` while the
/// process observing it sat in a session scope. Those differ, yet the cluster
/// was still killed by a Borg unit stopping. Only membership of its own unit
/// establishes that nothing in Borg's lifecycle reaches it.
pub(super) fn classify(postmaster: Option<&str>, caller: Option<&str>, unit: &str) -> Supervision {
    let Some(postmaster) = postmaster else {
        return Supervision::Unknown;
    };
    if postmaster
        .rsplit('/')
        .next()
        .is_some_and(|leaf| leaf == unit)
    {
        return Supervision::OwnUnit;
    }
    Supervision::ForeignCgroup {
        cgroup: postmaster.to_string(),
        shared_with_caller: caller == Some(postmaster),
    }
}

/// The cgroup v2 path of a process, or `None` when it cannot be read.
///
/// Only the unified (`0::`) entry: Borg's supported hosts are cgroup v2, and a
/// v1 controller list does not answer the question this module asks.
#[cfg(target_os = "linux")]
pub(super) fn cgroup_of(pid: i32) -> Option<String> {
    let raw = std::fs::read_to_string(format!("/proc/{pid}/cgroup")).ok()?;
    parse_cgroup(&raw)
}

#[cfg(not(target_os = "linux"))]
pub(super) fn cgroup_of(_pid: i32) -> Option<String> {
    None
}

fn parse_cgroup(raw: &str) -> Option<String> {
    raw.lines()
        .find_map(|line| line.strip_prefix("0::"))
        .map(|path| path.trim().to_string())
}

/// Whether a postmaster can be started under a unit on this machine.
///
/// All three conditions are required and none is inferable from the others: a
/// container may ship `systemd-run` with no user manager to talk to, and a
/// headless session may have a runtime directory but no bus.
#[cfg(target_os = "linux")]
pub(super) fn available() -> bool {
    which_systemd_run().is_some()
        && std::env::var_os("XDG_RUNTIME_DIR").is_some()
        && std::env::var_os("DBUS_SESSION_BUS_ADDRESS").is_some()
}

#[cfg(not(target_os = "linux"))]
pub(super) fn available() -> bool {
    false
}

#[cfg(target_os = "linux")]
fn which_systemd_run() -> Option<std::path::PathBuf> {
    ["/usr/bin/systemd-run", "/bin/systemd-run"]
        .into_iter()
        .map(std::path::PathBuf::from)
        .find(|candidate| candidate.is_file())
}

/// Start the postmaster under its own unit.
///
/// Returns the same shape as the `pg_ctl` path it stands beside: `Ok(Err(..))`
/// is a cluster that did not start and is the caller's to interpret against
/// live state, while `Err(..)` means the attempt itself could not be made.
#[cfg(target_os = "linux")]
pub(super) async fn spawn(launch: &Launch<'_>) -> anyhow::Result<std::result::Result<(), String>> {
    use anyhow::Context as _;

    let Some(systemd_run) = which_systemd_run() else {
        anyhow::bail!("systemd-run is not available on this machine");
    };
    let unit = unit_name(launch.data_dir);
    tracing::info!(port = launch.port, %unit, "starting the Borg session cluster under its own unit");
    let output = tokio::process::Command::new(&systemd_run)
        .args(run_args(launch, &unit))
        .stdin(std::process::Stdio::null())
        .output()
        .await
        .with_context(|| format!("could not run {}", systemd_run.display()))?;
    if output.status.success() {
        return Ok(Ok(()));
    }
    Ok(Err(super::last_error_line(&output.stderr)))
}

#[cfg(not(target_os = "linux"))]
pub(super) async fn spawn(_launch: &Launch<'_>) -> anyhow::Result<std::result::Result<(), String>> {
    anyhow::bail!("systemd units are only available on Linux")
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    fn launch<'a>(data: &'a Path, log: &'a Path, socket: &'a Path, pg: &'a Path) -> Launch<'a> {
        Launch {
            postgres: pg,
            data_dir: data,
            socket_dir: socket,
            log_path: log,
            port: 5433,
        }
    }

    /// One data directory must always resolve to one unit, or a second Borg
    /// process starts a second postmaster on the same files. Two directories
    /// must never collide, or two Borg homes fight over one unit.
    #[test]
    fn a_unit_name_identifies_exactly_one_data_directory() {
        let one = PathBuf::from("/home/someone/.borg/pgdata");
        let other = PathBuf::from("/home/someone-else/.borg/pgdata");

        assert_eq!(unit_name(&one), unit_name(&one));
        assert_ne!(unit_name(&one), unit_name(&other));
        assert!(unit_name(&one).starts_with("borg-postgres-"));
        assert!(unit_name(&one).ends_with(".service"));
    }

    /// The postmaster has to be the unit's foreground main process, and its
    /// log has to stay the file the rest of the module points operators at.
    #[test]
    fn the_unit_runs_postgres_in_the_foreground_and_keeps_the_existing_log() {
        let args = run_args(
            &launch(
                Path::new("/borg/pgdata"),
                Path::new("/borg/logs/postgres.log"),
                Path::new("/borg"),
                Path::new("/usr/bin/postgres"),
            ),
            "borg-postgres-test.service",
        );
        let rendered: Vec<String> = args
            .iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();

        assert!(rendered.contains(&"/usr/bin/postgres".to_string()));
        assert!(
            !rendered.iter().any(|arg| arg.contains("pg_ctl")),
            "daemonising under a unit hands systemd a process that exits immediately"
        );
        assert!(
            rendered
                .iter()
                .any(|arg| arg == "--property=StandardOutput=append:/borg/logs/postgres.log"),
            "the cluster log must stay the file, not move to the journal: {rendered:?}"
        );
        assert!(rendered.contains(&"--collect".to_string()));
        assert!(rendered.contains(&"-D".to_string()));
        assert!(rendered.contains(&"/borg/pgdata".to_string()));
        assert!(rendered.contains(&"5433".to_string()));
    }

    /// The regression this module exists for. In the incident the postmaster
    /// sat in `borg-remote.service` while the observing process sat in a
    /// session scope: two different cgroups, and the database still died when
    /// that unit stopped. Anything but its own unit must therefore classify as
    /// foreign, including a cgroup that merely differs from the caller's.
    #[test]
    fn only_its_own_unit_counts_as_isolated() {
        let unit = "borg-postgres-abc.service";
        let caller = "/user.slice/user-1000.slice/user@1000.service/app.slice/app-borg.slice/borg-session-1.scope";
        let incident = "/user.slice/user-1000.slice/user@1000.service/app.slice/borg-remote.service";

        assert_eq!(
            classify(Some(incident), Some(caller), unit),
            Supervision::ForeignCgroup {
                cgroup: incident.to_string(),
                shared_with_caller: false,
            },
            "a cgroup that is not ours is still fatal when it belongs to a Borg unit"
        );
        assert!(!classify(Some(incident), Some(caller), unit).is_isolated());
        assert!(classify(Some(incident), Some(caller), unit).warning().is_some());

        assert_eq!(
            classify(Some(caller), Some(caller), unit),
            Supervision::ForeignCgroup {
                cgroup: caller.to_string(),
                shared_with_caller: true,
            }
        );

        let own = format!("/user.slice/user-1000.slice/user@1000.service/app.slice/{unit}");
        assert_eq!(classify(Some(&own), Some(caller), unit), Supervision::OwnUnit);
        assert!(classify(Some(&own), Some(caller), unit).warning().is_none());

        // Absent cgroup information is its own answer, never a safe one.
        assert_eq!(classify(None, Some(caller), unit), Supervision::Unknown);
        assert!(!classify(None, Some(caller), unit).is_isolated());
    }

    #[test]
    fn a_unified_cgroup_line_is_read_and_others_ignored() {
        assert_eq!(
            parse_cgroup("0::/user.slice/app.slice/borg-postgres-x.service\n").as_deref(),
            Some("/user.slice/app.slice/borg-postgres-x.service")
        );
        // A v1-only process answers nothing rather than the wrong thing.
        assert_eq!(parse_cgroup("3:cpu:/some/v1/path\n"), None);
        assert_eq!(parse_cgroup(""), None);
    }
}
