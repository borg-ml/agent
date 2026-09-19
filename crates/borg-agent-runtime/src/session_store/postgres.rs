//! PostgreSQL backend for the Borg session journal.
//!
//! WHY THIS EXISTS: SQLite permits exactly one writer per FILE, and every Borg
//! process on a machine shares one journal file. Under real load that produced
//! measured lock starvation -- 171,861 "database is locked" waits in a single
//! log window, rising to 8,537/hour. Postgres serialises writers per SESSION
//! ROW instead (see the `next_sequence` allocator in postgres_schema.sql), so
//! concurrent agents writing to different sessions never block each other.
//!
//! This module owns connection lifecycle, schema bootstrap and health only.
//! The `SessionStore` implementation is layered on top of it.

use std::time::Duration;

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::{ConnectOptions, Executor};

use super::{SESSION_SCHEMA_VERSION, SessionStoreHealth};

pub mod actions;
pub mod body;
pub mod cold;
pub mod fork;
pub mod harness;
pub mod host;
pub mod maintenance;
pub mod recovery;
pub mod runtime;
pub mod search;
pub mod store;
pub mod sync;
pub mod workflow;

#[cfg(any(test, feature = "test-support"))]
pub mod testing;

#[cfg(test)]
mod contracts;

/// The journal schema. Its fingerprint prevents replaying DDL on ordinary opens.
const POSTGRES_SCHEMA_SQL: &str = include_str!("postgres_schema.sql");

/// The satellite tiers -- workspaces, autonomy, plugin state, receipts and the
/// relay queue. They share the journal's database because they shared the
/// SQLite file; the difference is that here sharing a database no longer means
/// sharing one write lock.
const POSTGRES_SATELLITE_SCHEMA_SQL: &str = include_str!("postgres_satellite_schema.sql");

/// Environment override for the journal connection string.
pub const SESSIONS_URL_ENV: &str = "BORG_SESSIONS_URL";

/// Opt-in for a journal deliberately shared between machines or OS users.
pub const SESSIONS_SHARED_ENV: &str = "BORG_SESSIONS_SHARED";

/// Connections held per process. SQLite was capped at 4 because additional
/// connections only deepened contention on the single write lock. Postgres has
/// no such ceiling; this is sized so one busy process can overlap reads with
/// its writer without monopolising a 200-connection server shared by dozens of
/// agent processes.
const POSTGRES_MAX_CONNECTIONS: u32 = 8;
const POSTGRES_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(10);

/// Advisory lock key guarding schema bootstrap. Arbitrary but fixed: every
/// process must choose the same number for the lock to mean anything.
const SCHEMA_BOOTSTRAP_LOCK: i64 = 0x0B01_65E5_5101;

/// The durable session journal, backed by PostgreSQL.
#[derive(Debug, Clone)]
pub struct PostgresSessionStore {
    pool: PgPool,
    /// Dictionaries are append-only and immutable, so caching one is safe for
    /// the life of the process and saves a fetch per cold read.
    dictionaries: cold::DictionaryCache,
}

impl PostgresSessionStore {
    /// Connect with a caller-chosen pool size.
    ///
    /// Tests run one store per core against one server, so they ask for a small
    /// pool; production uses `connect`, which is sized for a busy agent.
    pub async fn connect_with_pool_size(url: &str, max_connections: u32) -> Result<Self> {
        Self::connect_inner(url, max_connections).await
    }

    /// Connect to `url` and bring the schema up to the current version.
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_inner(url, POSTGRES_MAX_CONNECTIONS).await
    }

    async fn connect_inner(url: &str, max_connections: u32) -> Result<Self> {
        let options: PgConnectOptions = url
            .parse::<PgConnectOptions>()
            .with_context(|| format!("invalid Postgres session store URL: {url}"))?
            .application_name("borg-session-journal")
            // Statement logging at INFO would echo event bodies into the
            // process log; the journal is the authority for that content.
            .log_statements(tracing::log::LevelFilter::Debug);
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(POSTGRES_ACQUIRE_TIMEOUT)
            .connect_with(options)
            .await
            .with_context(|| format!("failed to connect to Postgres session store: {url}"))?;
        Self::from_pool(pool).await
    }

    /// Adopt an existing pool. Tests and embedded callers use this to share one
    /// server across stores without reconnecting.
    pub async fn from_pool(pool: PgPool) -> Result<Self> {
        let store = Self {
            pool,
            dictionaries: cold::DictionaryCache::default(),
        };
        store.ensure_schema().await?;
        Ok(store)
    }

    /// The connection string this process should use, from the environment.
    pub fn url_from_env() -> Option<String> {
        std::env::var(SESSIONS_URL_ENV)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Serialize real migrations, but never repeat unchanged DDL against live writers.
    pub async fn ensure_schema(&self) -> Result<()> {
        let mut hash = Sha256::new();
        hash.update(SESSION_SCHEMA_VERSION.to_le_bytes());
        hash.update(POSTGRES_SCHEMA_SQL);
        hash.update(POSTGRES_SATELLITE_SCHEMA_SQL);
        let fingerprint = hex::encode(hash.finalize());
        for attempt in 0..6 {
            match self.ensure_schema_once(&fingerprint).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    let transient = error.chain().any(|cause| {
                        cause
                            .downcast_ref::<sqlx::Error>()
                            .and_then(sqlx::Error::as_database_error)
                            .and_then(|error| error.code())
                            .is_some_and(|code| {
                                matches!(code.as_ref(), "40P01" | "55P03" | "40001")
                            })
                    });
                    if !transient || attempt == 5 {
                        return Err(error);
                    }
                    tracing::warn!(
                        attempt = attempt + 1,
                        "retrying contended Postgres schema bootstrap"
                    );
                    tokio::time::sleep(Duration::from_millis(100 << attempt)).await;
                }
            }
        }
        unreachable!()
    }

    async fn ensure_schema_once(&self, fingerprint: &str) -> Result<()> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .context("failed to begin the Postgres schema bootstrap")?;
        sqlx::query("set local lock_timeout = 5000")
            .execute(&mut *transaction)
            .await?;
        sqlx::query("select pg_advisory_xact_lock($1)")
            .bind(SCHEMA_BOOTSTRAP_LOCK)
            .execute(&mut *transaction)
            .await
            .context("failed to take the Postgres schema bootstrap lock")?;
        let metadata_exists: bool = sqlx::query_scalar("select to_regclass($1) is not null")
            .bind("borg_session_schema")
            .fetch_one(&mut *transaction)
            .await?;
        if metadata_exists {
            let stored: Option<(i64, Option<String>)> = sqlx::query_as(
                "select version, to_jsonb(s) ->> $1 from borg_session_schema s where id = 1",
            )
            .bind("definition_hash")
            .fetch_optional(&mut *transaction)
            .await?;
            if let Some((version, applied)) = stored {
                anyhow::ensure!(
                    version <= SESSION_SCHEMA_VERSION,
                    "unsupported future Borg session schema version {version}; current is {SESSION_SCHEMA_VERSION}"
                );
                if version == SESSION_SCHEMA_VERSION && applied.as_deref() == Some(fingerprint) {
                    transaction.commit().await?;
                    return Ok(());
                }
            }
        }
        // gin_trgm_ops in the schema requires pg_trgm. Attempted here rather
        // than in the .sql file because it needs privileges the journal role
        // may not hold in a managed deployment, where an operator installs it
        // once up front; a failure is only fatal if the dependent index is
        // then missing, which the schema application below surfaces.
        if let Err(error) = transaction
            .execute("create extension if not exists pg_trgm")
            .await
        {
            tracing::debug!(%error, "pg_trgm extension not created; assuming it is already installed");
        }
        sqlx::raw_sql(POSTGRES_SCHEMA_SQL)
            .execute(&mut *transaction)
            .await
            .context("failed to apply the Postgres session schema")?;
        sqlx::raw_sql(POSTGRES_SATELLITE_SCHEMA_SQL)
            .execute(&mut *transaction)
            .await
            .context("failed to apply the Postgres satellite schema")?;
        sqlx::query(
            "alter table borg_session_schema add column if not exists definition_hash text",
        )
        .execute(&mut *transaction)
        .await?;
        // `id` is `generated always as identity` with `check (id = 1)`, so the
        // single row must be written with an explicit id and OVERRIDING SYSTEM
        // VALUE; a plain insert would allocate id=2 and trip the check.
        sqlx::query(
            "insert into borg_session_schema (id, version, definition_hash) overriding system value \
             values (1, $1, $2) \
             on conflict (id) do update set version = excluded.version, definition_hash = excluded.definition_hash",
        )
        .bind(SESSION_SCHEMA_VERSION)
        .bind(fingerprint)
        .execute(&mut *transaction)
        .await
        .context("failed to record the Postgres session schema version")?;
        transaction
            .commit()
            .await
            .context("failed to commit the Postgres schema bootstrap")?;
        Ok(())
    }

    /// This machine and OS user, as one stable string.
    ///
    /// Hostname plus username, because that pair IS the boundary in question:
    /// workspace membership and `/broadcast` are scoped to one OS user on one
    /// machine, and a shared journal widens them to everyone who can reach the
    /// database.
    fn owner_fingerprint() -> (String, String) {
        let host = std::env::var("HOSTNAME")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                std::fs::read_to_string("/etc/hostname")
                    .ok()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
            .unwrap_or_else(|| "unknown-host".to_string());
        let user = std::env::var("USER")
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or_else(|_| "unknown-user".to_string());
        (format!("{user}@{host}"), format!("{user} on {host}"))
    }

    /// Record this process as an owner, and refuse a journal that silently
    /// acquired a second one.
    ///
    /// NOT a policy decision about whether sharing is allowed -- it is allowed,
    /// by setting `BORG_SESSIONS_SHARED`. What is refused is arriving at a
    /// shared journal by ACCIDENT, because the thing being shared is a trust
    /// boundary: every participant in that database can see and address every
    /// other. SQLite could not be shared by accident; a connection string can.
    pub async fn ensure_single_owner(&self) -> Result<()> {
        let (fingerprint, display) = Self::owner_fingerprint();
        sqlx::query(
            "insert into borg_journal_owners (fingerprint, display) values ($1, $2) \
             on conflict (fingerprint) do update set last_seen = now()",
        )
        .bind(&fingerprint)
        .bind(&display)
        .execute(&self.pool)
        .await
        .context("failed to record this journal owner")?;

        if std::env::var(SESSIONS_SHARED_ENV)
            .map(|value| !value.trim().is_empty() && value != "0")
            .unwrap_or(false)
        {
            return Ok(());
        }

        let others: Vec<String> = sqlx::query_scalar(
            "select display from borg_journal_owners where fingerprint <> $1 \
             order by first_seen limit 8",
        )
        .bind(&fingerprint)
        .fetch_all(&self.pool)
        .await
        .context("failed to read this journal's owners")?;
        anyhow::ensure!(
            others.is_empty(),
            "this journal has already been used by {}, and this process is {display}. \
             Sharing one journal means sharing a trust boundary: workspace membership \
             and /broadcast are scoped per machine and OS user, so every owner of this \
             database can see and address every other. If that is what you intend, set \
             {SESSIONS_SHARED_ENV}=1. If it is not, point {} at a database of its own.",
            others.join(", "),
            display
        );
        Ok(())
    }

    /// Whether the connected database already carries the current schema.
    pub async fn has_current_schema(&self) -> Result<bool> {
        let version: Option<i64> =
            sqlx::query_scalar("select version from borg_session_schema where id = 1")
                .fetch_optional(&self.pool)
                .await
                .unwrap_or(None);
        Ok(version == Some(SESSION_SCHEMA_VERSION))
    }

    /// Reject a database written by a newer Borg than this binary.
    pub async fn validate_current_schema(&self) -> Result<()> {
        let version: Option<i64> =
            sqlx::query_scalar("select version from borg_session_schema where id = 1")
                .fetch_optional(&self.pool)
                .await?;
        match version {
            Some(version) if version > SESSION_SCHEMA_VERSION => anyhow::bail!(
                "unsupported future Borg session schema version {version}; current is {SESSION_SCHEMA_VERSION}"
            ),
            _ => Ok(()),
        }
    }

    /// Cheap liveness snapshot, without the expensive integrity check.
    pub async fn readiness(&self) -> Result<SessionStoreHealth> {
        self.health_snapshot(false).await
    }

    /// Full health snapshot.
    pub async fn health(&self) -> Result<SessionStoreHealth> {
        self.health_snapshot(true).await
    }

    async fn health_snapshot(&self, check_integrity: bool) -> Result<SessionStoreHealth> {
        let integrity = if check_integrity {
            // Postgres has no `quick_check`. The equivalent cheap assertion is
            // that the journal is readable and its constraints are intact;
            // deep verification belongs to amcheck, which is not assumed here.
            sqlx::query_scalar::<_, i64>("select count(*) from borg_session_schema")
                .fetch_one(&self.pool)
                .await
                .map(|_| "ok".to_string())?
        } else {
            "not_checked".to_string()
        };
        // The durability question, asked of the server rather than assumed.
        // `off` and `local` both acknowledge a commit that a crash can still
        // lose, and they are set by operators chasing throughput, so this is
        // worth reading every time rather than trusting a default.
        let commit_durability: String = sqlx::query_scalar("show synchronous_commit")
            .fetch_one(&self.pool)
            .await?;
        let sessions: i64 = sqlx::query_scalar("select count(*) from sessions")
            .fetch_one(&self.pool)
            .await?;
        let events: i64 = sqlx::query_scalar("select count(*) from session_events")
            .fetch_one(&self.pool)
            .await?;
        let actions: i64 = sqlx::query_scalar("select count(*) from session_actions")
            .fetch_one(&self.pool)
            .await?;
        let payloads: i64 = sqlx::query_scalar("select count(*) from session_payloads")
            .fetch_one(&self.pool)
            .await?;
        Ok(SessionStoreHealth {
            integrity,
            integrity_checked: check_integrity,
            durable_commits: matches!(
                commit_durability.as_str(),
                "on" | "remote_apply" | "remote_write"
            ),
            commit_durability,
            sessions,
            events,
            actions,
            payloads,
            projection_version: super::SESSION_PROJECTION_VERSION,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// These tests need a real server: Postgres semantics (identity columns,
    /// `overriding system value`, extension availability) are exactly what a
    /// mock would get wrong. Without one they skip rather than fail, so the
    /// suite still runs on a machine with no database.
    use super::testing::{ScratchDatabase, test_url};

    #[tokio::test]
    async fn concurrent_bootstraps_do_not_race_on_schema_objects() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let scratch = ScratchDatabase::create(&url).await;
        let scratch_url = scratch.url.clone();

        // Every borg process bootstraps on open, so a cold start with several
        // agents launching at once hits exactly this path. Before the advisory
        // lock, `create index if not exists` raced and the loser failed with a
        // duplicate key on pg_class.
        let mut bootstraps = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let url = scratch_url.clone();
            bootstraps.spawn(async move {
                PostgresSessionStore::connect_with_pool_size(&url, 2)
                    .await
                    .map(|_| ())
            });
        }
        let mut failures = Vec::new();
        while let Some(joined) = bootstraps.join_next().await {
            if let Err(error) = joined.expect("bootstrap task panicked") {
                failures.push(format!("{error:#}"));
            }
        }
        assert!(
            failures.is_empty(),
            "concurrent bootstrap must serialise, got: {failures:?}"
        );

        let store = PostgresSessionStore::connect(&scratch_url)
            .await
            .expect("connect after concurrent bootstrap");
        let rows: i64 = sqlx::query_scalar("select count(*) from borg_session_schema")
            .fetch_one(store.pool())
            .await
            .expect("count schema rows");
        assert_eq!(rows, 1);
        drop(store);

        scratch.discard().await;
    }

    #[tokio::test]
    async fn current_schema_opens_without_blocking_live_journal_writers() {
        let Some(url) = test_url() else {
            return;
        };
        let scratch = ScratchDatabase::create(&url).await;
        let store = PostgresSessionStore::connect(&scratch.url).await.unwrap();
        let mut writer = store.pool.begin().await.unwrap();
        sqlx::query("lock table sessions in row exclusive mode")
            .execute(&mut *writer)
            .await
            .unwrap();
        let opened = tokio::time::timeout(
            Duration::from_secs(2),
            PostgresSessionStore::connect(&scratch.url),
        )
        .await;
        writer.rollback().await.unwrap();
        let opened = opened
            .expect("opening a current schema must not request DDL locks")
            .expect("open alongside journal writer");
        drop(opened);
        drop(store);
        scratch.discard().await;
    }

    #[tokio::test]
    async fn schema_bootstrap_retries_a_transient_migration_lock_timeout() {
        let Some(url) = test_url() else {
            return;
        };
        let scratch = ScratchDatabase::create(&url).await;
        let store = PostgresSessionStore::connect(&scratch.url).await.unwrap();
        sqlx::query("update borg_session_schema set definition_hash = null")
            .execute(store.pool())
            .await
            .unwrap();
        let mut blocker = store.pool.begin().await.unwrap();
        sqlx::query("select pg_advisory_xact_lock($1)")
            .bind(SCHEMA_BOOTSTRAP_LOCK)
            .execute(&mut *blocker)
            .await
            .unwrap();
        let retrying = store.clone();
        let opening = tokio::spawn(async move { retrying.ensure_schema().await });
        tokio::time::sleep(Duration::from_secs(6)).await;
        blocker.rollback().await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), opening)
            .await
            .expect("bootstrap retry is bounded")
            .unwrap()
            .expect("transient migration contention must recover");
        let applied: Option<String> =
            sqlx::query_scalar("select definition_hash from borg_session_schema where id = 1")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert!(applied.is_some());
        drop(store);
        scratch.discard().await;
    }

    #[tokio::test]
    async fn schema_bootstrap_is_idempotent_and_reports_health() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let store = PostgresSessionStore::connect_with_pool_size(&url, 4)
            .await
            .expect("connect and bootstrap");
        assert!(
            store.has_current_schema().await.expect("schema version"),
            "bootstrap must record the current schema version"
        );

        // Re-running bootstrap is what every process does on open.
        store.ensure_schema().await.expect("second bootstrap");
        store.validate_current_schema().await.expect("validate");
        assert!(store.has_current_schema().await.expect("schema version"));

        // The version row is constrained to exactly one row; a second
        // bootstrap must update it rather than insert beside it.
        let rows: i64 = sqlx::query_scalar("select count(*) from borg_session_schema")
            .fetch_one(store.pool())
            .await
            .expect("count schema rows");
        assert_eq!(rows, 1, "schema marker must stay a single row");

        let health = store.health().await.expect("health");
        assert_eq!(health.integrity, "ok");
        assert!(health.integrity_checked);
        assert_eq!(health.projection_version, crate::SESSION_PROJECTION_VERSION);
        // Readiness turns on durability, so the reported setting has to be the
        // server's real one rather than a constant: a test that accepted any
        // value here would pass against a server configured to lose commits.
        assert!(
            health.durable_commits,
            "a default server acknowledges commits durably, got synchronous_commit={}",
            health.commit_durability
        );
        assert!(
            !health.commit_durability.is_empty(),
            "the setting behind the verdict must be reported for diagnosis"
        );

        let readiness = store.readiness().await.expect("readiness");
        assert_eq!(readiness.integrity, "not_checked");
        assert!(!readiness.integrity_checked);
        assert!(
            readiness.is_ready(),
            "a freshly bootstrapped journal must report ready: {readiness:?}"
        );
    }

    #[tokio::test]
    async fn every_schema_object_the_store_depends_on_exists() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let store = PostgresSessionStore::connect_with_pool_size(&url, 4)
            .await
            .expect("connect");
        for table in [
            "sessions",
            "session_events",
            "session_event_dicts",
            "session_live_state",
            "session_payloads",
            "session_event_search",
            "session_actions",
            "session_action_transitions",
        ] {
            let present: bool = sqlx::query_scalar("select to_regclass($1) is not null")
                .bind(table)
                .fetch_one(store.pool())
                .await
                .expect("table lookup");
            assert!(present, "missing table {table}");
        }

        // Every satellite tier must be present too: leaving one in SQLite
        // would keep its writers on that file's single write lock.
        for table in [
            "workspaces",
            "workspace_participants",
            "workspace_members",
            "workspace_events",
            "workspace_deliveries",
            "workspace_threads",
            "workspace_work_items",
            "workspace_work_claims",
            "workspace_work_dependencies",
            "workspace_presence_leases",
            "agent_instances",
            "autonomy_jobs",
            "autonomy_job_transitions",
            "autonomy_checkpoints",
            "plugin_state",
            "plugin_artifacts",
            "plugin_mutation_receipts",
            "receipt_records",
            "receipt_transitions",
            "host_operation_queue",
        ] {
            let present: bool = sqlx::query_scalar("select to_regclass($1) is not null")
                .bind(table)
                .fetch_one(store.pool())
                .await
                .expect("table lookup");
            assert!(present, "missing satellite table {table}");
        }

        // The trigram index cannot be created without pg_trgm, so its presence
        // proves the extension bootstrap worked rather than silently skipping.
        let trigram_index: bool = sqlx::query_scalar(
            "select exists(select 1 from pg_indexes where indexname = 'idx_session_event_search_trgm')",
        )
        .fetch_one(store.pool())
        .await
        .expect("index lookup");
        assert!(trigram_index, "trigram search index is missing");
    }
}
