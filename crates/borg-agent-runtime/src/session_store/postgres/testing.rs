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
///
/// Dropped when this value is: explicitly through [`ScratchDatabase::discard`],
/// or from `Drop` when a test fails or returns early. Leaving them behind
/// accumulated hundreds of databases on shared servers.
pub struct ScratchDatabase {
    pub url: String,
    name: String,
    admin_url: String,
    discarded: bool,
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
        admin.close().await;
        let options: PgConnectOptions = url.parse().expect("parse url");
        let scratch_url = options.database(&name).to_url_lossy().to_string();
        Self {
            url: scratch_url,
            name,
            admin_url: url.to_string(),
            discarded: false,
        }
    }

    /// Drop the scratch database now, disconnecting anything still using it.
    pub async fn discard(mut self) {
        drop_database(&self.admin_url, &self.name).await;
        self.discarded = true;
    }
}

impl Drop for ScratchDatabase {
    fn drop(&mut self) {
        if self.discarded {
            return;
        }
        // `Drop` cannot await, and the test's runtime may be the one
        // unwinding, so the drop runs on its own thread and runtime.
        let (admin_url, name) = (self.admin_url.clone(), self.name.clone());
        let _ = std::thread::spawn(move || {
            if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                runtime.block_on(drop_database(&admin_url, &name));
            }
        })
        .join();
    }
}

async fn drop_database(admin_url: &str, name: &str) {
    let Ok(admin) = PgPool::connect(admin_url).await else {
        return;
    };
    let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
        "drop database if exists {name} with (force)"
    )))
    .execute(&admin)
    .await;
    admin.close().await;
}
