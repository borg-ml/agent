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

use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result, bail};
use tokio::process::Command;

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
        match self.spawn_postmaster(pg_ctl).await? {
            Ok(()) => Ok(()),
            Err(failure) => {
                // A crash leaves postmaster.pid behind, and the next start
                // refuses on the assumption the old server is alive. The
                // machine this runs on does crash, so recover rather than
                // requiring the user to know about this file.
                if self.clear_stale_pid_file()?
                    && let Ok(()) = self.spawn_postmaster(pg_ctl).await?
                {
                    return Ok(());
                }
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

    /// Remove a `postmaster.pid` whose process is gone. Returns whether one was
    /// removed, so the caller only retries when something actually changed.
    fn clear_stale_pid_file(&self) -> Result<bool> {
        let pid_file = self.data_dir.join("postmaster.pid");
        let Ok(contents) = std::fs::read_to_string(&pid_file) else {
            return Ok(false);
        };
        let Some(pid) = contents
            .lines()
            .next()
            .and_then(|line| line.trim().parse::<i32>().ok())
        else {
            return Ok(false);
        };
        if process_is_alive(pid) {
            return Ok(false);
        }
        tracing::warn!(
            pid,
            "removing a stale postmaster.pid left by a crashed session cluster"
        );
        std::fs::remove_file(&pid_file)
            .with_context(|| format!("could not remove {}", pid_file.display()))?;
        Ok(true)
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

#[cfg(unix)]
fn process_is_alive(pid: i32) -> bool {
    // Signal 0 performs the permission and existence checks without delivering
    // anything, which is exactly the question being asked.
    unsafe { libc::kill(pid, 0) == 0 }
}

#[cfg(not(unix))]
fn process_is_alive(_pid: i32) -> bool {
    // Without a cheap liveness check, assume the owner is alive rather than
    // delete a pid file belonging to a running server.
    true
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
