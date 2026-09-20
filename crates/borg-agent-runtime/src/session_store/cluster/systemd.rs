//! Naming and verifying the unit the managed postmaster runs under.
//!
//! `pg_ctl start` daemonises and systemd tracks by cgroup, not by parent, so a
//! postmaster started that way stays in the cgroup of whichever Borg process
//! started it. With `KillMode=control-group` (the default) stopping that unit
//! SIGTERMs the postmaster, which PostgreSQL reads as a smart shutdown. A
//! restart of `borg-remote.service` at 04:21:51 did exactly that: "received
//! smart shutdown request" at 04:21:51.695, 183 connections rejected in 855 ms.
//! An ordinary CLI is no safer -- it runs in a transient
//! `borg-session-<uuid>.scope`.
//!
//! The unit is tied to nothing: no `PartOf`, `BindsTo` or `WantedBy`. Being
//! unreachable from Borg's own lifecycle is the point, and is why the database
//! is not coupled to host enrollment.
//!
//! KNOWN LIMITATION, deliberately not worked around: a user manager stops when
//! the user's last session ends unless lingering is enabled, so on such a host
//! the cluster stops at logout. Borg does not enable lingering. That is a
//! policy decision belonging to the machine's owner, not to a session store,
//! and the failure it leaves is smaller and far more predictable than the one
//! this module fixes -- the database stops when the user logs out, rather than
//! whenever any Borg unit happens to restart.

use std::ffi::OsString;
use std::path::Path;
#[cfg(target_os = "linux")]
use std::path::PathBuf;

/// Everything the postmaster needs, so [`run_args`] stays a pure function of
/// its inputs and is testable without systemd on the machine.
pub(super) struct Launch<'a> {
    pub postgres: &'a Path,
    pub data_dir: &'a Path,
    pub socket_dir: &'a Path,
    pub log_path: &'a Path,
    pub port: u16,
}

/// What a running postmaster's cgroup proves about who can stop it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Supervision {
    /// In its own unit's cgroup. Only systemd stops it.
    OwnUnit,
    /// Somewhere else. If that cgroup belongs to a Borg service or session
    /// scope, this cluster dies when that unit does.
    ForeignCgroup {
        cgroup: String,
        shared_with_caller: bool,
    },
    /// No cgroup information. Its own state, never reported as safe.
    Unknown,
}

impl Supervision {
    /// What to tell an operator, or `None` when there is nothing to say.
    pub(super) fn warning(&self) -> Option<String> {
        let Self::ForeignCgroup {
            cgroup,
            shared_with_caller,
        } = self
        else {
            return None;
        };
        Some(format!(
            "the session cluster is running in {cgroup}, not its own unit, so stopping that \
             unit will shut the database down{}. It moves to its own unit the next time it is \
             started; Borg will not restart it now, because that would cause the outage it \
             avoids.",
            if *shared_with_caller {
                " -- and it shares this process's cgroup, so it cannot outlive this process"
            } else {
                ""
            }
        ))
    }
}

/// The unit that owns one data directory's postmaster.
///
/// Keyed on the data directory because that is what the postmaster locks: two
/// Borg homes get two units, and two processes sharing a home get the same
/// one. That collision is wanted -- the second start finds the unit already
/// present, which the caller re-checks against live state and treats as
/// success, the rule this cluster module already follows everywhere else.
pub(super) fn unit_name(data_dir: &Path) -> String {
    use sha2::{Digest, Sha256};

    // Canonicalise when the path resolves, so two spellings of one directory
    // do not name two units. A first run has no canonical form yet.
    let identity = data_dir
        .canonicalize()
        .unwrap_or_else(|_| data_dir.to_path_buf());
    let mut hasher = Sha256::new();
    hasher.update(identity.as_os_str().as_encoded_bytes());
    let digest = format!("{:x}", hasher.finalize());
    format!("borg-postgres-{}.service", &digest[..16])
}

/// The `systemd-run` arguments that start this cluster.
pub(super) fn run_args(launch: &Launch<'_>, unit: &str) -> Vec<OsString> {
    let mut args: Vec<OsString> = vec![
        "--user".into(),
        format!("--unit={unit}").into(),
        // A unit left failed would otherwise block the next start under what
        // is deliberately a stable name.
        "--collect".into(),
        {
            let mut description = OsString::from("--description=Borg session cluster ");
            description.push(launch.data_dir);
            description
        },
    ];
    // Stop semantics copied from the distribution's own postgresql.service,
    // which pairs `KillMode=mixed` with `KillSignal=SIGINT` over a foreground
    // `postgres`. Both halves matter. SIGINT is a FAST shutdown, where the
    // postmaster disconnects clients and shuts its children down in order;
    // systemd's default SIGTERM is a SMART shutdown, which waits for every
    // client to leave on its own and would hold the stop open. And `mixed`
    // sends that signal to the postmaster alone, so it coordinates its own
    // backends -- `control-group` would signal every backend directly, which
    // is the postmaster's job and not systemd's.
    args.push("--property=KillMode=mixed".into());
    args.push("--property=KillSignal=SIGINT".into());
    // Keep the log the file the rest of this module points operators at; a
    // unit would otherwise send it to the journal and falsify that message.
    for stream in ["StandardOutput", "StandardError"] {
        let mut property = OsString::from(format!("--property={stream}=append:"));
        property.push(launch.log_path);
        args.push(property);
    }
    args.push("--".into());
    // `postgres`, not `pg_ctl start`: under a unit the postmaster has to be
    // the foreground main process, and daemonising would hand systemd a main
    // process that exits immediately.
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
/// Comparing against the caller's cgroup alone is not enough: in the incident
/// the postmaster sat in `borg-remote.service` while the process observing it
/// sat in a session scope. Those differ, and the database still died. Only
/// membership of its own unit establishes that nothing in Borg's lifecycle
/// reaches it.
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
#[cfg(target_os = "linux")]
pub(super) fn cgroup_of(pid: i32) -> Option<String> {
    parse_cgroup(&std::fs::read_to_string(format!("/proc/{pid}/cgroup")).ok()?)
}

#[cfg(not(target_os = "linux"))]
pub(super) fn cgroup_of(_pid: i32) -> Option<String> {
    None
}

/// Only the unified (`0::`) entry: a v1 controller list does not answer the
/// question this module asks, so it answers nothing rather than wrongly.
#[cfg(any(target_os = "linux", test))]
fn parse_cgroup(raw: &str) -> Option<String> {
    raw.lines()
        .find_map(|line| line.strip_prefix("0::"))
        .map(|path| path.trim().to_string())
}

/// How long the manager probe may take before the host counts as unsupervised.
///
/// Answering "is there a user manager" must never be able to hang a Borg
/// start-up; a manager that cannot reply in this long is not one worth waiting
/// for.
#[cfg(target_os = "linux")]
const PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Whether the user manager will accept a unit here.
///
/// An actual bounded probe, not an inference from the environment. Reading
/// `DBUS_SESSION_BUS_ADDRESS` was wrong twice over: a process started *by* a
/// systemd user service normally has no such variable, so requiring it sent
/// exactly the processes this module protects down the unsupervised path; and
/// its presence would still not prove a manager is listening. Asking
/// `systemctl --user` removes both guesses at once, because it locates the bus
/// itself. `show` reads one property and starts nothing, so probing never has
/// the side effect the probe is asking about.
///
/// THREE OUTCOMES, and the middle one is the whole reason this returns a
/// `Result`. `Ok(true)` is a manager that answered. `Ok(false)` is a host with
/// no systemd to ask, which falls back to `pg_ctl` exactly as before. But a
/// probe that times out or is refused on a host that HAS systemd is neither:
/// falling back there would put the postmaster in this process's own cgroup,
/// which is the defect this module exists to remove, and it would do it
/// precisely on the machines it was written for. When that happens inside a
/// Borg-owned unit or scope it is an error with a way out, not a quiet
/// downgrade.
#[cfg(target_os = "linux")]
pub(super) async fn available() -> anyhow::Result<bool> {
    if which("systemd-run").is_none() {
        return Ok(false);
    }
    let Some(systemctl) = which("systemctl") else {
        return Ok(false);
    };
    let mut probe = tokio::process::Command::new(systemctl);
    probe
        .arg("--user")
        .arg("show")
        .arg("--property=Version")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    if matches!(
        tokio::time::timeout(PROBE_TIMEOUT, probe.status()).await,
        Ok(Ok(status)) if status.success()
    ) {
        return Ok(true);
    }
    let caller = cgroup_of(std::process::id() as i32);
    if caller
        .as_deref()
        .is_some_and(|cgroup| in_borg_scope(cgroup))
    {
        anyhow::bail!(
            "this machine runs systemd but its user manager did not answer within \
             {PROBE_TIMEOUT:?}, and Borg is running inside {}. Starting the cluster \
             with pg_ctl here would put the database in that same cgroup, so stopping \
             this unit would shut it down -- the failure the unit exists to prevent. \
             Restore the user bus (`systemctl --user status`), or point Borg at a \
             server you already run: BORG_SESSIONS_URL=postgres://user@host/db",
            caller.unwrap_or_default()
        );
    }
    // Systemd is installed but unreachable, and nothing Borg owns is holding
    // the cluster's fate. `pg_ctl` is then no worse than it ever was.
    Ok(false)
}

#[cfg(not(target_os = "linux"))]
pub(super) async fn available() -> anyhow::Result<bool> {
    Ok(false)
}

/// Whether a cgroup path belongs to a unit or scope Borg's own lifecycle tears
/// down.
///
/// Matched on any `borg-*` leaf rather than the two names observed in the
/// incident, so a unit added later is covered by default. A Borg process
/// somewhere unrecognised falls through to `Ok(false)` and the pre-existing
/// `pg_ctl` behaviour, which is the old risk rather than a new one.
#[cfg(any(target_os = "linux", test))]
fn in_borg_scope(cgroup: &str) -> bool {
    cgroup.split('/').any(|leaf| {
        leaf.starts_with("borg-") && (leaf.ends_with(".service") || leaf.ends_with(".scope"))
    })
}

/// Distributions put these in `/usr/bin`; a split-usr host keeps `/bin`.
#[cfg(target_os = "linux")]
fn which(name: &str) -> Option<PathBuf> {
    ["/usr/bin", "/bin"]
        .into_iter()
        .map(|dir| PathBuf::from(dir).join(name))
        .find(|candidate| candidate.is_file())
}

/// Start the postmaster under its own unit.
///
/// `Ok(Err(..))` is a unit that did not start and is the caller's to weigh
/// against live state -- losing the race for the stable unit name looks like
/// this. `Err(..)` means the attempt could not be made at all.
#[cfg(target_os = "linux")]
pub(super) async fn spawn(launch: &Launch<'_>) -> anyhow::Result<std::result::Result<(), String>> {
    use anyhow::Context as _;

    let Some(systemd_run) = which("systemd-run") else {
        anyhow::bail!("systemd-run is not available on this machine");
    };
    let unit = unit_name(launch.data_dir);
    tracing::info!(port = launch.port, %unit, "starting the Borg session cluster under its own unit");
    // The caller's open has the deadline; this only guarantees that if that
    // deadline drops us mid-launch, no systemd-run is left running behind it.
    let output = tokio::process::Command::new(&systemd_run)
        .args(run_args(launch, &unit))
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true)
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

    /// One data directory must resolve to one unit, or a second Borg process
    /// starts a second postmaster on the same files; two directories must not
    /// collide, or two Borg homes fight over one unit.
    #[test]
    fn a_unit_name_identifies_exactly_one_data_directory() {
        let one = Path::new("/home/someone/.borg/pgdata");
        let other = Path::new("/home/someone-else/.borg/pgdata");

        assert_eq!(unit_name(one), unit_name(one));
        assert_ne!(unit_name(one), unit_name(other));
        assert!(unit_name(one).starts_with("borg-postgres-"));
        assert!(unit_name(one).ends_with(".service"));
    }

    /// The postmaster has to be the unit's foreground main process, and the
    /// log has to stay the file the cluster's error messages point at.
    #[test]
    fn the_unit_runs_postgres_in_the_foreground_and_keeps_the_existing_log() {
        let rendered: Vec<String> = run_args(
            &Launch {
                postgres: Path::new("/usr/bin/postgres"),
                data_dir: Path::new("/borg/pgdata"),
                socket_dir: Path::new("/borg"),
                log_path: Path::new("/borg/logs/postgres.log"),
                port: 5433,
            },
            "borg-postgres-test.service",
        )
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();

        assert!(rendered.contains(&"/usr/bin/postgres".to_string()));
        assert!(
            !rendered.iter().any(|arg| arg.contains("pg_ctl")),
            "daemonising hands systemd a main process that exits immediately"
        );
        assert!(
            rendered
                .iter()
                .any(|arg| arg == "--property=StandardOutput=append:/borg/logs/postgres.log"),
            "the cluster log must stay the file, not move to the journal: {rendered:?}"
        );
        assert!(rendered.contains(&"/borg/pgdata".to_string()));
        assert!(rendered.contains(&"5433".to_string()));
        // SIGINT is a fast shutdown; systemd's default SIGTERM is a smart one
        // that waits for every client to leave and would hold the stop open.
        // `mixed` leaves the postmaster to shut its own backends down.
        assert!(rendered.contains(&"--property=KillSignal=SIGINT".to_string()));
        assert!(rendered.contains(&"--property=KillMode=mixed".to_string()));
        assert!(
            !rendered.iter().any(|arg| arg.contains("Restart=")),
            "ensure_running is the only supervisor; a second one would race it"
        );
    }

    /// The regression this module exists for. In the incident the postmaster
    /// sat in `borg-remote.service` and the observing process in a session
    /// scope: different cgroups, and the database still died. So a cgroup that
    /// merely differs from the caller's must still classify as foreign.
    #[test]
    fn only_its_own_unit_counts_as_isolated() {
        let unit = "borg-postgres-abc.service";
        let caller = "/user.slice/user@1000.service/app.slice/app-borg.slice/borg-session-1.scope";
        let incident = "/user.slice/user@1000.service/app.slice/borg-remote.service";

        assert_eq!(
            classify(Some(incident), Some(caller), unit),
            Supervision::ForeignCgroup {
                cgroup: incident.to_string(),
                shared_with_caller: false,
            },
            "a cgroup that is not ours is still fatal when it belongs to a Borg unit"
        );
        assert!(
            classify(Some(incident), Some(caller), unit)
                .warning()
                .is_some()
        );
        assert_eq!(
            classify(Some(caller), Some(caller), unit),
            Supervision::ForeignCgroup {
                cgroup: caller.to_string(),
                shared_with_caller: true,
            }
        );

        let own = format!("/user.slice/user@1000.service/app.slice/{unit}");
        assert_eq!(
            classify(Some(&own), Some(caller), unit),
            Supervision::OwnUnit
        );
        assert!(classify(Some(&own), Some(caller), unit).warning().is_none());
        assert_eq!(classify(None, Some(caller), unit), Supervision::Unknown);
    }

    /// The guard that stops a manager timeout from silently reinstating the
    /// bug: inside anything Borg owns, an unreachable manager must be an error
    /// rather than a fallback, because falling back puts the postmaster in
    /// this very cgroup.
    #[test]
    fn a_borg_owned_cgroup_is_recognised_whatever_the_unit_is_called() {
        assert!(in_borg_scope(
            "/user.slice/user@1000.service/app.slice/borg-remote.service"
        ));
        assert!(in_borg_scope(
            "/user.slice/user@1000.service/app.slice/app-borg.slice/borg-session-1a2b.scope"
        ));
        // A unit Borg grows later is covered without being listed.
        assert!(in_borg_scope(
            "/user.slice/app.slice/borg-workflow-7.service"
        ));

        // Somewhere Borg does not own, where pg_ctl is no worse than before.
        assert!(!in_borg_scope(
            "/user.slice/user@1000.service/app.slice/ghostty.scope"
        ));
        assert!(!in_borg_scope("/system.slice/postgresql.service"));
        assert!(!in_borg_scope(""));
    }

    #[test]
    fn a_unified_cgroup_line_is_read_and_others_ignored() {
        assert_eq!(
            parse_cgroup("0::/user.slice/borg-postgres-x.service\n").as_deref(),
            Some("/user.slice/borg-postgres-x.service")
        );
        assert_eq!(parse_cgroup("3:cpu:/some/v1/path\n"), None);
        assert_eq!(parse_cgroup(""), None);
    }
}
