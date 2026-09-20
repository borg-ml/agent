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
//! all of them run this. Rather than take a lock, each step is written so that
//! losing the race is indistinguishable from having had nothing to do: `initdb`
//! refuses a populated directory, `pg_ctl start` refuses a running cluster, and
//! both outcomes are re-checked against live state before being treated as
//! failures.
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
        if !self.is_initialized() {
            let initdb = locate_binary("initdb")?;
            self.initialize(&initdb).await?;
        }
        if !self.is_running(&pg_ctl).await? {
            self.start(&pg_ctl).await?;
        }
        self.ensure_database().await?;
        // Here rather than in `start`, so it covers the cluster that was
        // already running when we arrived -- the case we did not create and
        // the one most likely to be supervised wrongly -- and so it reports
        // only once the server has actually answered.
        self.report_supervision();
        Ok(self.url())
    }

    /// Create the cluster. Only ever called when `PG_VERSION` is absent.
    async fn initialize(&self, initdb: &Path) -> Result<()> {
        if let Some(parent) = self.data_dir.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "could not create {} for the session cluster",
                    parent.display()
                )
            })?;
        }
        tracing::info!(data_dir = %self.data_dir.display(), "initialising the Borg session cluster");
        // Trust auth is safe here and only here: the cluster listens on
        // loopback and a unix socket inside Borg's own home directory, and it
        // is owned by the user running Borg. A password would be stored beside
        // the socket it protects, which protects nothing.
        let output = Command::new(initdb)
            .arg("-D")
            .arg(&self.data_dir)
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
            // Another Borg process initialising concurrently is the expected
            // way to lose this race, and it leaves the cluster we wanted.
            if self.is_initialized() {
                return Ok(());
            }
            bail!(
                "initdb failed for {}: {}",
                self.data_dir.display(),
                last_error_line(&output.stderr)
            );
        }
        Ok(())
    }

    /// Whether this cluster is currently accepting connections.
    async fn is_running(&self, pg_ctl: &Path) -> Result<bool> {
        let output = Command::new(pg_ctl)
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
        if systemd::available() {
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
    /// Bounded tightly and on purpose: this only has to outlast another
    /// process's postmaster writing its pid file. Waiting for the server to
    /// be *ready* is the factory recovery loop's budget to spend, not this
    /// one's, and spending it twice is what makes a bounded wait unbounded.
    async fn became_running(&self, pg_ctl: &Path) -> Result<bool> {
        for attempt in 0..5 {
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

    /// Start the postmaster under its own unit, or say why it could not be.
    async fn start_under_unit(&self) -> Result<()> {
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
        let pg_ctl = locate_binary("pg_ctl")?;
        if self.became_running(&pg_ctl).await? {
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
    /// Bounded tightly and on purpose: this only has to outlast another
    /// process's postmaster writing its pid file. Waiting for the server to
    /// be *ready* is the factory recovery loop's budget to spend, not this
    /// one's, and spending it twice is what makes a bounded wait unbounded.
    async fn became_running(&self, pg_ctl: &Path) -> Result<bool> {
        for attempt in 0..5 {
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

    // A crash leaves postmaster.pid behind, and Borg used to unlink it here
    // when `kill(pid, 0)` said the owner was gone. That is removed rather than
    // repaired, because it could only ever be weaker than the check PostgreSQL
    // already makes and it destroyed the file that check depends on.
    //
    // The postmaster reads that same file on startup and cross-checks the
    // shared memory segment named in it before deciding the cluster is
    // abandoned, so it clears a genuinely stale file by itself. A liveness
    // probe on the PID alone cannot see that segment, and it is wrong in both
    // directions: `kill` reports EPERM, not success, for a live process owned
    // by another user, and a recycled PID belonging to some unrelated program
    // reads as the server still running.
    //
    // Unlinking on a false negative is the damaging half. postmaster.pid is
    // the interlock that stops a second postmaster attaching to one data
    // directory, and between the liveness probe and the unlink another Borg
    // process can legitimately have started a server and written a fresh file.
    // Deleting it there removes a live cluster's interlock. Nothing is lost by
    // leaving this to PostgreSQL: a start that fails because the file is
    // genuinely stale is a transitional failure like any other, and the
    // factory's recovery loop re-enters and starts the cluster.

    /// Run `pg_ctl start`, distinguishing a failed launch from a failed call.
    async fn spawn_postmaster(&self, pg_ctl: &Path) -> Result<std::result::Result<(), String>> {
        tracing::info!(port = self.port, "starting the Borg session cluster");
        let output = Command::new(pg_ctl)
            .arg("-D")
            .arg(&self.data_dir)
            .arg("-l")
            .arg(&self.log_path)
            .arg("-o")
            .arg(format!(
                "-p {} -k {} -h 127.0.0.1",
                self.port,
                self.socket_dir.display()
            ))
            // Wait for the server to accept connections rather than returning
            // the moment the process exists, so the caller's first connection
            // is not a race against startup.
            .arg("-w")
            .arg("start")
            .stdin(Stdio::null())
            .output()
            .await
            .with_context(|| format!("could not run {}", pg_ctl.display()))?;
        if output.status.success() {
            return Ok(Ok(()));
        }
        Ok(Err(last_error_line(&output.stderr)))
    }

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
            .is_some_and(|code| code.as_ref() == "57P03")
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

    /// A cluster that is merely between states must be waited out, and a
    /// cluster that is genuinely broken must not be. This classification is
    /// the whole of the fix for the recurring "could not reach the Borg
    /// session cluster ... the database system is shutting down" startup
    /// failure, and it is keyed on SQLSTATE and error kind rather than on
    /// anything the compiler or the surrounding types can check. If it silently
    /// stops matching, concurrent CLI opens go back to failing outright during
    /// every restart, which is exactly the regression this guards.
    #[test]
    fn a_cluster_between_states_is_waited_out_and_a_broken_one_is_not() {
        use std::borrow::Cow;
        use std::error::Error as StdError;

        #[derive(Debug)]
        struct Reported(&'static str);

        impl std::fmt::Display for Reported {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(self.0)
            }
        }

        impl StdError for Reported {}

        impl sqlx::error::DatabaseError for Reported {
            fn message(&self) -> &str {
                self.0
            }
            fn code(&self) -> Option<Cow<'_, str>> {
                Some(Cow::Borrowed(self.0))
            }
            fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
                self
            }
            fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
                self
            }
            fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
                self
            }
            fn kind(&self) -> sqlx::error::ErrorKind {
                sqlx::error::ErrorKind::Other
            }
        }

        let reported = |code: &'static str| {
            anyhow::Error::new(sqlx::Error::Database(Box::new(Reported(code))))
                .context("could not reach the Borg session cluster")
        };
        let refused = |kind: std::io::ErrorKind| {
            anyhow::Error::new(sqlx::Error::Io(std::io::Error::new(kind, "socket")))
                .context("could not reach the Borg session cluster")
        };

        // 57P03 is how a live postmaster says it is shutting down or still
        // starting up -- the reported failure.
        assert!(is_between_states(&reported("57P03")));
        // The gap between a completed shutdown and the next start.
        assert!(is_between_states(&refused(
            std::io::ErrorKind::ConnectionRefused
        )));
        assert!(is_between_states(&refused(
            std::io::ErrorKind::UnexpectedEof
        )));

        // A rejected role and a missing database are settled facts about a
        // running cluster; retrying only delays the report.
        assert!(!is_between_states(&reported("28P01")));
        assert!(!is_between_states(&reported("3D000")));
        assert!(!is_between_states(&refused(
            std::io::ErrorKind::PermissionDenied
        )));
        assert!(!is_between_states(&anyhow::anyhow!(
            "pg_ctl is not installed"
        )));
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
