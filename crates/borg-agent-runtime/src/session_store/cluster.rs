//! Borg's own PostgreSQL cluster, provisioned and supervised on this machine.
//!
//! WHY THIS EXISTS: Borg's default posture is several agents running at once on
//! one machine, and Postgres serialises writers per session row rather than
//! machine-wide. Serving that as the default is only useful if it needs no
//! setup, so this module does the setup.
//!
//! WHAT IT DOES NOT DO: it never installs PostgreSQL. It locates the `initdb`
//! and `pg_ctl` that a PostgreSQL installation already provides, and creates a
//! cluster under Borg's own home directory. When those binaries are absent it
//! fails with the install command for this platform rather than silently
//! journalling somewhere else -- a silent fallback would split one machine's
//! history across two databases, which is the failure the store factory exists
//! to prevent.
//!
//! CONCURRENCY: several Borg processes routinely start at the same moment, and
//! all of them run this. Provisioning and starting are serialised by an
//! advisory lock on the Borg home, because those two steps have a window that
//! idempotence alone cannot close: `initdb` creates `PG_VERSION` partway
//! through its work, so a second process checking whether a cluster exists can
//! see one that is not finished yet. The lock is held only for the duration of
//! [`ManagedCluster::ensure_running`], never across a caller's use of the
//! journal.
//!
//! Past the lock, each step is still written so that losing a race is
//! indistinguishable from having had nothing to do: `initdb` refuses a
//! populated directory, a start refuses a cluster that is already running, and
//! both outcomes are re-checked against live state before being treated as
//! failures. The lock removes a corruption window; it does not make this the
//! only process that can be doing any of this.
//!
//! A cluster is also a thing that starts and stops underneath us. Whoever
//! stops it -- a crash, a session-scope cleanup, an operator -- leaves a window
//! in which the postmaster is alive and holds a valid `postmaster.pid` while
//! refusing every connection. Every step here is a single pass that can lose to
//! that window; recovering from it means re-running the whole sequence, up to
//! and including opening the journal, so the retry loop belongs to the one
//! caller that spans all of it. This module supplies the two things that loop
//! needs to make its decision -- [`is_between_states`] and [`TRANSITION_BUDGET`]
//! -- and `session_store::factory::open` owns the loop itself.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::process::Command;

mod systemd;

/// The port the managed cluster listens on.
///
/// Deliberately not 5432: a developer machine frequently already runs a system
/// PostgreSQL there, and adopting someone else's cluster is not Borg's call to
/// make.
const DEFAULT_PORT: u16 = 5433;

/// Override for the managed cluster's port.
pub const PORT_ENV: &str = "BORG_SESSIONS_PORT";

/// The superuser the managed cluster is initialised with.
const ROLE: &str = "borg";

/// The database holding the journal.
const DATABASE: &str = "borg_sessions";

/// How long a caller should keep working a cluster that is between states
/// before calling it broken.
///
/// This bounds a shutdown plus the start that follows it, not a single
/// connection: the point of the budget is to outlast a transition, and a
/// cluster carrying a large journal can take seconds to shut down cleanly and
/// seconds more to recover on the way back up. It is a single budget spanning
/// the whole open, so that a process cannot spend it twice over and turn a
/// bounded wait into a multiple of itself.
pub(crate) const TRANSITION_BUDGET: Duration = Duration::from_secs(30);

/// Where `initdb` and `pg_ctl` live when they are not on `PATH`.
///
/// Distributions keep server binaries off the default `PATH` on purpose, since
/// they are administrative rather than client tools. A user who installed
/// PostgreSQL through their package manager should not have to discover that.
const BINARY_SEARCH_PREFIXES: &[&str] = &[
    "/usr/lib/postgresql",
    "/usr/pgsql",
    "/usr/local/pgsql/bin",
    "/usr/local/opt/postgresql/bin",
    "/opt/homebrew/opt/postgresql/bin",
    "/opt/homebrew/bin",
    "/usr/local/bin",
    "/usr/bin",
];

/// A PostgreSQL cluster owned by this machine's Borg installation.
#[derive(Debug, Clone)]
pub struct ManagedCluster {
    data_dir: PathBuf,
    socket_dir: PathBuf,
    log_path: PathBuf,
    start_lock: PathBuf,
    port: u16,
}

impl ManagedCluster {
    /// The cluster belonging to a Borg home directory.
    pub fn in_home(home: impl AsRef<Path>) -> Self {
        let home = home.as_ref();
        Self {
            data_dir: home.join("pgdata"),
            socket_dir: home.to_path_buf(),
            log_path: home.join("logs").join("postgres.log"),
            start_lock: home.join("cluster-start.lock"),
            port: port_from_env().unwrap_or(DEFAULT_PORT),
        }
    }

    /// The connection string for this cluster's journal database.
    pub fn url(&self) -> String {
        format!("postgres://{ROLE}@127.0.0.1:{}/{DATABASE}", self.port)
    }

    /// Where this cluster lives, for diagnostics.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Whether a cluster has already been initialised here.
    ///
    /// `PG_VERSION` rather than the directory itself: `initdb` requires an
    /// empty or absent directory, and a directory that exists but holds no
    /// cluster is the state left behind by an interrupted first run.
    pub fn is_initialized(&self) -> bool {
        self.data_dir.join("PG_VERSION").is_file()
    }

    /// Provision if necessary, start if necessary, and return the journal URL.
    ///
    /// One pass, and a pass can legitimately lose. `pg_ctl status` answers from
    /// `postmaster.pid`, so a postmaster that is shutting down is still alive,
    /// still holds the file, and still reports as running -- this skips
    /// starting it and hands back a URL that will refuse the next connection.
    /// That is not a failure worth handling here, because handling it means
    /// running this whole sequence again: once a shutdown completes, nobody has
    /// restarted the cluster, so waiting and reconnecting finds nothing
    /// listening. Only re-entry starts it again.
    ///
    /// `session_store::factory::open` is where that re-entry happens, because
    /// its loop also covers the connection and the ownership claim that follow.
    pub async fn ensure_running(&self) -> Result<String> {
        let pg_ctl = locate_binary("pg_ctl")?;
        {
            // Held across the check-and-provision and the check-and-start,
            // and dropped before the journal is touched. `initdb` writes
            // `PG_VERSION` before it has finished, so without this a second
            // process can read a half-built cluster as a built one and start
            // a postmaster on it.
            let _guard = self.lock_startup().await?;
            if !self.is_initialized() {
                let initdb = locate_binary("initdb")?;
                self.initialize(&initdb).await?;
            }
            if !self.is_running(&pg_ctl).await? {
                self.start(&pg_ctl).await?;
            }
        }
        self.ensure_database().await?;
        // Here rather than in `start`, so it covers the cluster that was
        // already running when we arrived -- the case we did not create and
        // the one most likely to be supervised wrongly -- and so it reports
        // only once the server has actually answered.
        self.report_supervision();
        Ok(self.url())
    }

    /// Take the advisory lock that serialises provisioning and starting.
    ///
    /// Polled rather than blocking, so a caller's timeout can still cut this
    /// short; the holder only ever keeps it for one provision-and-start.
    async fn lock_startup(&self) -> Result<StartupGuard> {
        use std::fs::{OpenOptions, TryLockError};

        if let Some(parent) = self.start_lock.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("could not create {} for the cluster lock", parent.display())
            })?;
        }
        let mut options = OpenOptions::new();
        options.create(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(&self.start_lock).with_context(|| {
            format!("could not open {}", self.start_lock.display())
        })?;
        let mut attempt: u32 = 0;
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(StartupGuard { _file: file }),
                Err(TryLockError::WouldBlock) => {}
                Err(TryLockError::Error(error)) => {
                    return Err(error).with_context(|| {
                        format!("could not lock {}", self.start_lock.display())
                    });
                }
            }
            tokio::time::sleep(Duration::from_millis(50 << attempt.min(3))).await;
            attempt += 1;
        }
    }

    /// Create the cluster, in a sibling directory that is renamed into place.
    ///
    /// Never `initdb -D` straight at the data directory. `initdb` writes
    /// `PG_VERSION` partway through its work, so an interrupted run -- and it
    /// can be interrupted, since the child is killed when a cancelled caller
    /// drops this future -- leaves a directory that every later run reads as a
    /// finished cluster. Building in a sibling and renaming makes the cluster
    /// appear in one step or not at all.
    async fn initialize(&self, initdb: &Path) -> Result<()> {
        let Some(parent) = self.data_dir.parent() else {
            bail!("{} has no parent directory", self.data_dir.display());
        };
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "could not create {} for the session cluster",
                parent.display()
            )
        })?;
        // Named for this process, so it is ours to clear and never another
        // run's half-built cluster.
        let staging = parent.join(format!(".pgdata-initdb-{}", std::process::id()));
        if staging.exists() {
            std::fs::remove_dir_all(&staging).with_context(|| {
                format!("could not clear {} before initdb", staging.display())
            })?;
        }
        tracing::info!(data_dir = %self.data_dir.display(), "initialising the Borg session cluster");
        // Trust auth is safe here and only here: the cluster listens on
        // loopback and a unix socket inside Borg's own home directory, and it
        // is owned by the user running Borg. A password would be stored beside
        // the socket it protects, which protects nothing.
        let output = Command::new(initdb)
            // A cancelled caller must not leave initdb running against a
            // directory nobody is waiting for. Safe because the worst it can
            // leave behind is an abandoned staging directory.
            .kill_on_drop(true)
            .arg("-D")
            .arg(&staging)
            .arg("-U")
            .arg(ROLE)
            .arg("--auth-local=trust")
            .arg("--auth-host=trust")
            .arg("--encoding=UTF8")
            .stdin(Stdio::null())
            .output()
            .await
            .with_context(|| format!("could not run {}", initdb.display()))?;
        if !output.status.success() {
            std::fs::remove_dir_all(&staging).ok();
            bail!(
                "initdb failed for {}: {}",
                self.data_dir.display(),
                last_error_line(&output.stderr)
            );
        }
        // Fails rather than overwrites when another process got there first,
        // which is the outcome we wanted and is re-checked as such. An
        // existing data directory is never removed: it may be a cluster this
        // Borg does not know about, and losing it is unrecoverable.
        if std::fs::rename(&staging, &self.data_dir).is_err() {
            std::fs::remove_dir_all(&staging).ok();
            if self.is_initialized() {
                return Ok(());
            }
            bail!(
                "could not move the new cluster into {}",
                self.data_dir.display()
            );
        }
        Ok(())
    }

    /// Whether this cluster is currently accepting connections.
    async fn is_running(&self, pg_ctl: &Path) -> Result<bool> {
        let output = Command::new(pg_ctl)
            .kill_on_drop(true)
            .arg("-D")
            .arg(&self.data_dir)
            .arg("status")
            .stdin(Stdio::null())
            .output()
            .await
            .with_context(|| format!("could not run {}", pg_ctl.display()))?;
        // pg_ctl reports 0 for running, 3 for stopped and 4 for an
        // unusable data directory. Only 0 means we can skip starting.
        Ok(output.status.success())
    }

    async fn start(&self, pg_ctl: &Path) -> Result<()> {
        if let Some(parent) = self.log_path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("could not create {} for the cluster log", parent.display())
            })?;
        }
        std::fs::create_dir_all(&self.socket_dir).with_context(|| {
            format!(
                "could not create {} for the cluster socket",
                self.socket_dir.display()
            )
        })?;
        // A host with a user manager gets a unit, and gets nothing else. The
        // pg_ctl path leaves the postmaster in the cgroup of whichever Borg
        // process started it, which is the defect the unit exists to fix, so
        // falling back to it here would quietly reintroduce that defect on
        // exactly the machines the fix was written for.
        if systemd::available().await {
            return self.start_under_unit(pg_ctl).await;
        }
        match self.spawn_postmaster(pg_ctl).await? {
            Ok(()) => Ok(()),
            Err(failure) => {
                // Losing the start race to another Borg process is success.
                if self.is_running(pg_ctl).await? {
                    return Ok(());
                }
                bail!(
                    "could not start the Borg session cluster at {}: {failure}\nits log is {}",
                    self.data_dir.display(),
                    self.log_path.display()
                )
            }
        }
    }

    /// Start the postmaster under its own unit, or say why it could not be.
    async fn start_under_unit(&self, pg_ctl: &Path) -> Result<()> {
        let postgres = locate_binary("postgres")?;
        let failure = match systemd::spawn(&systemd::Launch {
            postgres: &postgres,
            data_dir: &self.data_dir,
            socket_dir: &self.socket_dir,
            log_path: &self.log_path,
            port: self.port,
        })
        .await?
        {
            Ok(()) => return Ok(()),
            Err(failure) => failure,
        };
        // The unit name is derived from the data directory, so two processes
        // starting one cluster contend for one name and the loser is told the
        // unit already exists. That is the outcome it wanted, but the winner's
        // postmaster has not necessarily written its pid file yet, so a single
        // check can miss a cluster that is seconds from being up. Give it a
        // short grace before concluding the failure was ours.
        if self.became_running(pg_ctl).await? {
            return Ok(());
        }
        let unit = systemd::unit_name(&self.data_dir);
        bail!(
            "could not start the Borg session cluster under its own systemd unit: {failure}\n\n\
             Borg will not fall back to starting it unsupervised, because a postmaster \
             outside its own unit is stopped by whichever Borg service or session scope \
             started it.\n\n\
             Inspect the unit:\n  systemctl --user status {unit}\n\n\
             The cluster's log is {}\n\n\
             Or point Borg at a server you already run:\n  \
             BORG_SESSIONS_URL=postgres://user@host/db",
            self.log_path.display()
        )
    }

    /// Whether the cluster comes up within a short grace period.
    ///
    /// This only has to outlast another process's postmaster writing its pid
    /// file, so the grace is deliberately small. Waiting for the server to be
    /// *ready* is the recovery budget's job, not this one's.
    async fn became_running(&self, pg_ctl: &Path) -> Result<bool> {
        for attempt in 0..3 {
            if self.is_running(pg_ctl).await? {
                return Ok(true);
            }
            tokio::time::sleep(Duration::from_millis(100 << attempt)).await;
        }
        self.is_running(pg_ctl).await
    }

    /// Say so when the cluster is running somewhere Borg's own teardown
    /// reaches, and do nothing else about it.
    ///
    /// Reported from the postmaster's cgroup rather than from what we just
    /// tried to do, because a cluster that was already running when this
    /// process arrived is exactly the case that matters and we did not start
    /// it. Restarting it to improve its supervision would cause the outage
    /// this avoids, so a warning is the whole of the response; it moves to its
    /// own unit the next time something starts it.
    fn report_supervision(&self) {
        let Some(pid) = self.postmaster_pid() else {
            return;
        };
        let supervision = systemd::classify(
            systemd::cgroup_of(pid).as_deref(),
            systemd::cgroup_of(std::process::id() as i32).as_deref(),
            &systemd::unit_name(&self.data_dir),
        );
        if let Some(warning) = supervision.warning() {
            tracing::warn!("{warning}");
        }
    }

    /// The PID the running postmaster recorded, if there is one.
    fn postmaster_pid(&self) -> Option<i32> {
        std::fs::read_to_string(self.data_dir.join("postmaster.pid"))
            .ok()?
            .lines()
            .next()?
            .trim()
            .parse()
            .ok()
    }

    // Borg used to unlink a stale postmaster.pid here. Removed: the check it
    // made was weaker than the one the postmaster already makes on startup,
    // and unlinking on a false negative destroys the interlock that stops a
    // second postmaster attaching to one data directory.

    /// Create the journal database if this is a fresh cluster.
    async fn ensure_database(&self) -> Result<()> {
        use sqlx::{Connection, Executor, postgres::PgConnection};

        let maintenance = format!("postgres://{ROLE}@127.0.0.1:{}/postgres", self.port);
        let mut connection = PgConnection::connect(&maintenance).await.with_context(|| {
            format!("could not reach the Borg session cluster on {maintenance}")
        })?;
        let exists: Option<(i32,)> = sqlx::query_as("select 1 from pg_database where datname = $1")
            .bind(DATABASE)
            .fetch_optional(&mut connection)
            .await
            .context("could not inspect the session cluster's databases")?;
        if exists.is_none() {
            // The name is this module's own constant, never user input.
            match connection
                .execute(sqlx::AssertSqlSafe(format!("create database {DATABASE}")))
                .await
            {
                Ok(_) => {}
                // Another Borg process created it between the check and here.
                Err(error) if is_duplicate_database(&error) => {}
                Err(error) => {
                    return Err(error).context("could not create the session journal database");
                }
            }
        }
        connection.close().await.ok();
        Ok(())
    }
}

/// Whether a failure means the cluster is changing state rather than broken.
///
/// Matched on SQLSTATE and error kind rather than message text, so it holds
/// under a non-English server locale. Three conditions make up the window:
///
/// - `57P03` (`cannot_connect_now`) is the live postmaster refusing work, and
///   covers both "the database system is shutting down" and "the database
///   system is starting up".
/// - `57P01` (`admin_shutdown`) and `57P02` (`crash_shutdown`) are a shutdown
///   or a backend crash reaching a connection this call had already opened.
///   The cluster comes back from both, and from `57P02` by recovering, which
///   is the case the caller most needs to wait through rather than report.
/// - A refused connection is the gap after a shutdown completes and before
///   anything has started the cluster again.
/// - Startup closes the listening socket on clients mid-handshake, which
///   surfaces as an unexpected end of file rather than a refusal.
///
/// All three resolve on their own or on the caller's next start attempt. A
/// rejected role, a missing database or a corrupt data directory do not, and
/// must stay loud.
pub(crate) fn is_between_states(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let Some(error) = cause.downcast_ref::<sqlx::Error>() else {
            return false;
        };
        if error
            .as_database_error()
            .and_then(|error| error.code())
            .is_some_and(|code| matches!(code.as_ref(), "57P01" | "57P02" | "57P03"))
        {
            return true;
        }
        matches!(
            error,
            sqlx::Error::Io(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionRefused
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::UnexpectedEof
                )
        )
    })
}

fn is_duplicate_database(error: &sqlx::Error) -> bool {
    // 42P04 is duplicate_database. Matching the code rather than the message
    // keeps this working under a non-English server locale.
    matches!(
        error.as_database_error().and_then(|error| error.code()),
        Some(code) if code == "42P04"
    )
}

/// Open the cluster, waiting out one that is merely changing state.
///
/// One deadline governs the whole operation. Every await is bounded by that
/// same absolute instant -- the attempt itself as well as the pause between
/// attempts -- so a subprocess that never returns or a connect that hangs
/// cannot outlive the budget the way a deadline consulted only between
/// attempts would allow.
///
/// On expiry the last transitional failure is reported rather than a bare
/// timeout, because "the database system is shutting down" is what an
/// operator needs to see; the timeout only says we stopped waiting for it.
pub(crate) async fn recover_transitional<T, F, Fut>(mut open: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let deadline = tokio::time::Instant::now() + TRANSITION_BUDGET;
    let mut last: Option<anyhow::Error> = None;
    let mut attempt: u32 = 0;
    loop {
        match tokio::time::timeout_at(deadline, open()).await {
            Ok(Ok(value)) => return Ok(value),
            // Anything that is not the cluster changing state is a settled
            // fact, and waiting only delays reporting it.
            Ok(Err(error)) if !is_between_states(&error) => return Err(error),
            Ok(Err(error)) => last = Some(error),
            Err(_) => return Err(gave_up(last)),
        }
        attempt += 1;
        tracing::warn!(
            attempt,
            "the Borg session cluster is between states; re-opening it"
        );
        // Capped early: a transition resolves in seconds, and the budget buys
        // more by being spent on attempts than on longer sleeps.
        let pause = tokio::time::Instant::now() + Duration::from_millis(100 << (attempt - 1).min(3));
        tokio::time::sleep_until(pause.min(deadline)).await;
        if tokio::time::Instant::now() >= deadline {
            return Err(gave_up(last));
        }
    }
}

fn gave_up(last: Option<anyhow::Error>) -> anyhow::Error {
    let waited = TRANSITION_BUDGET.as_secs();
    match last {
        Some(error) => error.context(format!(
            "the Borg session cluster did not settle within {waited}s"
        )),
        None => anyhow::anyhow!("opening the Borg session cluster timed out after {waited}s"),
    }
}

/// Holds the startup lock. Releasing it is the file closing.
struct StartupGuard {
    _file: std::fs::File,
}

fn port_from_env() -> Option<u16> {
    std::env::var(PORT_ENV)
        .ok()
        .and_then(|value| value.trim().parse().ok())
}

/// The last meaningful line of a failed command's stderr.
fn last_error_line(stderr: &[u8]) -> String {
    String::from_utf8_lossy(stderr)
        .lines()
        .map(str::trim)
        .rfind(|line| !line.is_empty())
        .unwrap_or("no error output")
        .to_string()
}

/// Find a PostgreSQL server binary, or explain how to install one.
fn locate_binary(name: &str) -> Result<PathBuf> {
    if let Some(found) = search_path(name).or_else(|| search_prefixes(name)) {
        return Ok(found);
    }
    bail!(
        "Borg defaults to PostgreSQL so several agents can write at once, but `{name}` \
         was not found on this machine.\n\n\
         Install PostgreSQL:\n{}\n\n\
         Or point Borg at a server you already run:\n  \
         BORG_SESSIONS_URL=postgres://user@host/db",
        install_hint()
    )
}

fn search_path(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|directory| directory.join(name))
        .find(|candidate| is_executable(candidate))
}

/// Search the locations distributions use for server binaries.
///
/// Version-suffixed directories (`/usr/lib/postgresql/16/bin`) are expanded one
/// level and taken in descending order, so the newest installed server wins.
fn search_prefixes(name: &str) -> Option<PathBuf> {
    for prefix in BINARY_SEARCH_PREFIXES {
        let prefix = Path::new(prefix);
        let direct = prefix.join(name);
        if is_executable(&direct) {
            return Some(direct);
        }
        let Ok(entries) = std::fs::read_dir(prefix) else {
            continue;
        };
        let mut versioned: Vec<PathBuf> = entries
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .collect();
        versioned.sort();
        for directory in versioned.into_iter().rev() {
            let candidate = directory.join("bin").join(name);
            if is_executable(&candidate) {
                return Some(candidate);
            }
        }
    }
    None
}

fn is_executable(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path)
            .map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        path.is_file()
    }
}

fn install_hint() -> &'static str {
    if cfg!(target_os = "macos") {
        "  brew install postgresql@17"
    } else if cfg!(target_os = "windows") {
        "  winget install PostgreSQL.PostgreSQL"
    } else {
        "  Debian/Ubuntu:  sudo apt install postgresql\n  \
           Fedora/RHEL:    sudo dnf install postgresql-server\n  \
           Arch:           sudo pacman -S postgresql"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cluster_lives_under_the_borg_home_and_not_on_the_system_port() {
        let cluster = ManagedCluster::in_home("/tmp/borg-home");
        assert_eq!(cluster.data_dir(), Path::new("/tmp/borg-home/pgdata"));
        assert!(
            cluster.url().contains("5433"),
            "the managed cluster must not adopt a system server's 5432"
        );
        assert!(cluster.url().contains(DATABASE));
    }

    #[test]
    fn an_absent_cluster_is_not_reported_as_initialized() {
        let cluster = ManagedCluster::in_home("/nonexistent/borg-home");
        assert!(!cluster.is_initialized());
    }

    /// The message a user hits on a machine with no PostgreSQL is the entire
    /// value of failing loudly, so it must name both escape hatches.
    #[test]
    fn a_missing_binary_explains_how_to_install_and_how_to_opt_out() {
        let error = locate_binary("borg-postgres-binary-that-does-not-exist")
            .expect_err("a missing binary must not resolve");
        let message = format!("{error:#}");
        assert!(message.contains("BORG_SESSIONS_URL"));
        assert!(
            message.contains("Install PostgreSQL"),
            "the error must say how to get a server, got: {message}"
        );
    }

    /// The conditions the recovery tests above do not each exercise, kept
    /// because the whole loop turns on this one predicate and it is keyed on
    /// bare SQLSTATE strings that nothing else checks.
    #[test]
    fn every_transitional_condition_is_recognised_and_nothing_else_is() {
        let reported = |code: &'static str| {
            anyhow::Error::new(sqlx::Error::Database(Box::new(Reported(code))))
        };
        let io = |kind: std::io::ErrorKind| {
            anyhow::Error::new(sqlx::Error::Io(std::io::Error::new(kind, "socket")))
        };

        // A live postmaster refusing work, and a shutdown or backend crash
        // reaching a connection that was already open.
        for code in ["57P01", "57P02", "57P03"] {
            assert!(is_between_states(&reported(code)), "{code} is transitional");
        }
        // The gap before anything restarts it, and a socket closed on a
        // client mid-handshake during startup.
        assert!(is_between_states(&io(std::io::ErrorKind::ConnectionRefused)));
        assert!(is_between_states(&io(std::io::ErrorKind::UnexpectedEof)));

        // Settled facts about a running cluster.
        for code in ["28P01", "3D000", "42P04"] {
            assert!(!is_between_states(&reported(code)), "{code} is settled");
        }
        assert!(!is_between_states(&io(std::io::ErrorKind::PermissionDenied)));
        assert!(!is_between_states(&anyhow::anyhow!("pg_ctl is not installed")));
    }

    /// The release-blocking behaviour itself: a cluster that is shutting down
    /// when we arrive must be waited out and the whole open re-entered, not
    /// reported as unreachable. Classifying the error correctly is not enough
    /// -- the loop has to run again, which is what restarts a cluster nobody
    /// else is going to restart.
    #[tokio::test(start_paused = true)]
    async fn a_shutdown_is_waited_out_and_the_open_is_re_entered() {
        use std::sync::atomic::{AtomicU32, Ordering};

        // Two refusals, as a shutdown completing and the restart that follows
        // not yet accepting connections, then a cluster that is up.
        let attempts = AtomicU32::new(0);
        let opened = recover_transitional(|| async {
            match attempts.fetch_add(1, Ordering::SeqCst) {
                0 => Err(shutting_down()),
                1 => Err(anyhow::Error::new(sqlx::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "socket",
                )))),
                _ => Ok("journal"),
            }
        })
        .await
        .expect("a cluster that comes back must be opened, not reported unreachable");
        assert_eq!(opened, "journal");
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            3,
            "the open must be re-entered, not merely retried at the connection"
        );
    }

    /// The other half of the contract: waiting is bounded, and what the
    /// operator is told is why the cluster never settled, not that a timer
    /// elapsed.
    #[tokio::test(start_paused = true)]
    async fn a_cluster_that_never_settles_gives_up_and_still_says_why() {
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let error = recover_transitional(|| async {
            attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err::<(), _>(shutting_down())
        })
        .await
        .expect_err("a cluster that never comes back must not be waited on forever");
        let message = format!("{error:#}");
        assert!(
            message.contains("shutting down"),
            "the last real failure must survive the timeout, got: {message}"
        );
        assert!(attempts.load(std::sync::atomic::Ordering::SeqCst) > 1);
    }

    /// A settled failure must not be retried at all: waiting only delays the
    /// report, and startup is where a wrong password should be loudest.
    #[tokio::test(start_paused = true)]
    async fn a_broken_cluster_fails_on_the_first_attempt() {
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let error = recover_transitional(|| async {
            attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err::<(), _>(anyhow::Error::new(sqlx::Error::Database(Box::new(
                Reported("28P01"),
            ))))
        })
        .await
        .expect_err("a rejected role is not a transition");
        assert!(format!("{error:#}").contains("28P01"));
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    fn shutting_down() -> anyhow::Error {
        anyhow::Error::new(sqlx::Error::Database(Box::new(Reported("57P03"))))
            .context("could not reach the Borg session cluster")
    }

    /// A database error carrying a chosen SQLSTATE. sqlx's own
    /// `PgDatabaseError` is only constructible from the wire protocol.
    #[derive(Debug)]
    struct Reported(&'static str);

    impl std::fmt::Display for Reported {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "the database system is shutting down ({})", self.0)
        }
    }

    impl std::error::Error for Reported {}

    impl sqlx::error::DatabaseError for Reported {
        fn message(&self) -> &str {
            self.0
        }
        fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
            Some(std::borrow::Cow::Borrowed(self.0))
        }
        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }
        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    #[test]
    fn the_last_error_line_survives_a_noisy_stderr() {
        let stderr =
            b"initdb: warning: enabling trust\n\ninitdb: error: directory is not empty\n\n";
        assert_eq!(
            last_error_line(stderr),
            "initdb: error: directory is not empty"
        );
    }
}
