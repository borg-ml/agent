//! Shared harness for tests that need a real PostgreSQL server.
//!
//! These tests are not mocked on purpose: Postgres semantics -- identity
//! columns, `overriding system value`, advisory locks, `skip locked`, jsonb
//! round-tripping -- are exactly what a mock would get wrong, and are what the
//! store depends on.
//!
//! Postgres is the only backend, so a missing server is a broken test
//! environment rather than a reason to run a narrower suite. `session_store`
//! fails with instructions instead of skipping: a suite that quietly skips its
//! storage tests reports success while covering nothing, which is how a
//! backend regression reaches a release.

use sqlx::ConnectOptions;
use sqlx::postgres::{PgConnectOptions, PgPool};
use uuid::Uuid;

use super::PostgresSessionStore;

/// Pool size for a store under test.
///
/// Small on purpose: the suite runs many stores at once against one server, and
/// a production-sized pool per test exhausts `max_connections` long before the
/// tests exhaust anything they are actually measuring.
const TEST_POOL_SIZE: u32 = 4;

/// The server to test against, if one is configured.
///
/// Prefer [`session_store`], which fails with instructions rather than handing
/// back a `None` that a caller can turn into a silent skip.
pub fn test_url() -> Option<String> {
    std::env::var("BORG_TEST_SESSIONS_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

/// The server to test against, or a failure that says how to provide one.
pub fn required_test_url() -> String {
    test_url().unwrap_or_else(|| {
        panic!(
            "BORG_TEST_SESSIONS_URL is not set, so there is no session journal to test against.\n\
             Point it at a PostgreSQL server whose role may create databases, for example:\n  \
             BORG_TEST_SESSIONS_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres\n\
             Each test creates and drops its own scratch database, so the server needs \
             CREATEDB and is never written to outside those databases."
        )
    })
}

/// A scratch database and a store opened on it, with the schema installed.
///
/// The store is connected, which brings the schema up to the current version,
/// and nothing further: no session is created. Callers create the sessions
/// their own case needs, because a pre-made session is either unused or subtly
/// wrong for the test that receives it.
///
/// The returned [`ScratchDatabase`] must be passed to
/// [`ScratchDatabase::discard`] when the test succeeds.
pub async fn session_store() -> (ScratchDatabase, PostgresSessionStore) {
    let url = required_test_url();
    let scratch = ScratchDatabase::create(&url).await;
    let store = PostgresSessionStore::connect_with_pool_size(&scratch.url, TEST_POOL_SIZE)
        .await
        .expect("connect to the scratch session store");
    (scratch, store)
}

/// A scratch database owned by one test.
///
/// Per-test isolation is required rather than tidy: tests run in parallel, and
/// a shared database let one test's ageing pass compress another test's
/// fixtures out from under it.
pub struct ScratchDatabase {
    pub url: String,
    name: String,
    admin: PgPool,
}

impl ScratchDatabase {
    pub async fn create(url: &str) -> Self {
        let name = format!("borg_test_{}", Uuid::new_v4().simple());
        let admin = PgPool::connect(url)
            .await
            .expect("connect to admin database");
        // The name is a locally generated UUID, never user input.
        sqlx::query(sqlx::AssertSqlSafe(format!("create database {name}")))
            .execute(&admin)
            .await
            .expect("create scratch database");
        let options: PgConnectOptions = url.parse().expect("parse url");
        let url = options.database(&name).to_url_lossy().to_string();
        Self { url, name, admin }
    }

    /// Drop the scratch database. Called explicitly rather than on `Drop`
    /// because dropping a database is async; a test that fails early simply
    /// leaves it behind, which is harmless on a test server.
    pub async fn discard(self) {
        let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
            "drop database if exists {} with (force)",
            self.name
        )))
        .execute(&self.admin)
        .await;
    }
}
