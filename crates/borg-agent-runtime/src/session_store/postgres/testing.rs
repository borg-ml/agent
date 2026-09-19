//! Shared harness for tests that need a real PostgreSQL server.
//!
//! These tests are not mocked on purpose: Postgres semantics -- identity
//! columns, `overriding system value`, advisory locks, `skip locked`, jsonb
//! round-tripping -- are exactly what a mock would get wrong, and are what the
//! store depends on.

use sqlx::postgres::{PgConnectOptions, PgPool};
use uuid::Uuid;

/// The server to test against, or `None` to skip. Skipping rather than failing
/// keeps the suite runnable on a machine with no database.
pub fn test_url() -> Option<String> {
    std::env::var("BORG_TEST_SESSIONS_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
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
        let url = format!(
            "postgres://{}@{}:{}/{}",
            options.get_username(),
            options.get_host(),
            options.get_port(),
            name
        );
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
