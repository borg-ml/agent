//! Schema bootstrap that never holds a session start hostage to DDL locks.
//!
//! WHY THIS EXISTS: every open used to re-apply the whole schema script in one
//! transaction whenever the stored fingerprint differed from this binary's --
//! which, with two releases running side by side, was every open, since each
//! rewrote the fingerprint to its own. The script's `ALTER TABLE` statements
//! need ACCESS EXCLUSIVE locks even when there is nothing to change, so a
//! running `pg_dump` (ACCESS SHARE on every table for the whole dump) queued
//! them, each queued request stalled every reader and writer behind it, and
//! session start failed after 30 s.
//!
//! Now an open first checks the catalog, read-only, for everything the
//! scripts create -- tables, views, indexes, added columns, dropped NOT NULLs,
//! pending backfills -- and does no DDL at all when it is all there, whatever
//! the fingerprint says. Only missing objects are created, one statement at a
//! time under a short lock timeout. A statement that cannot get its lock is
//! skipped: the store opens on the schema it has, code that depends on a
//! newer object falls back without it, and background maintenance retries.
//! The fingerprint is only a fast path, written after a migration completes.

use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::{Connection, Executor, PgConnection};

use super::{
    POSTGRES_SATELLITE_SCHEMA_SQL, POSTGRES_SCHEMA_SQL, PostgresSessionStore,
    SCHEMA_BOOTSTRAP_LOCK, SESSION_SCHEMA_VERSION,
};

/// The longest any schema statement waits for a lock. A waiting ACCESS
/// EXCLUSIVE request blocks everything queued behind it, so this bounds the
/// stall a migration can cause, and a long reader just makes it skip.
const STATEMENT_LOCK_TIMEOUT: &str = "1s";
/// How long an open waits for another process's migration to finish.
const BOOTSTRAP_LOCK_WAIT: Duration = Duration::from_secs(10);

/// What one schema statement brings into being, as the catalog can see it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Creates {
    Relation(String),
    Column {
        table: String,
        column: String,
    },
    Nullable {
        table: String,
        column: String,
    },
    /// A data backfill, still needed while rows match `condition`.
    Backfill {
        from: String,
        condition: String,
    },
    /// Storage settings, `do` blocks and the like: applied whenever a
    /// migration runs, never a reason to run one.
    Other,
}

struct Statement {
    sql: String,
    creates: Creates,
}

/// Split a schema script into statements, respecting comments, quoted
/// strings and `$$` bodies.
fn split(script: &str) -> Vec<String> {
    let bytes = script.as_bytes();
    let mut statements = Vec::new();
    let mut start = 0;
    let mut index = 0;
    let (mut quoted, mut dollar) = (false, false);
    while index < bytes.len() {
        match bytes[index] {
            b'-' if !quoted && !dollar && bytes.get(index + 1) == Some(&b'-') => {
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
                continue;
            }
            b'\'' if !dollar => quoted = !quoted,
            b'$' if !quoted && bytes.get(index + 1) == Some(&b'$') => {
                dollar = !dollar;
                index += 1;
            }
            b';' if !quoted && !dollar => {
                statements.push(script[start..index].to_string());
                start = index + 1;
            }
            _ => {}
        }
        index += 1;
    }
    statements.push(script[start..].to_string());
    statements
        .into_iter()
        .filter(|statement| !normalized(statement).is_empty())
        .collect()
}

/// Lower case, comments removed, whitespace collapsed.
fn normalized(statement: &str) -> String {
    statement
        .lines()
        .map(|line| line.split("--").next().unwrap_or_default())
        .collect::<Vec<_>>()
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn identifier(text: &str) -> String {
    text.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .next()
        .unwrap_or_default()
        .to_string()
}

fn classify(statement: &str) -> Creates {
    let text = normalized(statement);
    for prefix in [
        "create table if not exists ",
        "create index if not exists ",
        "create unique index if not exists ",
        "create or replace view ",
    ] {
        if let Some(rest) = text.strip_prefix(prefix) {
            return Creates::Relation(identifier(rest));
        }
    }
    if let Some(rest) = text.strip_prefix("alter table ") {
        let table = identifier(rest);
        if let Some((_, column)) = rest.split_once(" add column if not exists ") {
            return Creates::Column {
                table,
                column: identifier(column),
            };
        }
        if let Some((_, column)) = rest.split_once(" alter column ")
            && column.ends_with(" drop not null")
        {
            return Creates::Nullable {
                table,
                column: identifier(column),
            };
        }
        return Creates::Other;
    }
    if let Some(rest) = text.strip_prefix("update ")
        && let (Some((from, _)), Some((_, condition))) =
            (rest.split_once(" set "), rest.rsplit_once(" where "))
    {
        return Creates::Backfill {
            from: from.to_string(),
            condition: condition.to_string(),
        };
    }
    Creates::Other
}

fn statements() -> Vec<Statement> {
    [POSTGRES_SCHEMA_SQL, POSTGRES_SATELLITE_SCHEMA_SQL]
        .into_iter()
        .flat_map(split)
        .map(|sql| Statement {
            creates: classify(&sql),
            sql,
        })
        .collect()
}

/// Indexes of the statements still needed, in script order; empty when the
/// schema is complete. A needed migration also re-applies every `Other`.
async fn needed(connection: &mut PgConnection, statements: &[Statement]) -> Result<Vec<usize>> {
    let mut relations = Vec::new();
    let (mut column_tables, mut columns) = (Vec::new(), Vec::new());
    let (mut nullable_tables, mut nullable) = (Vec::new(), Vec::new());
    for statement in statements {
        match &statement.creates {
            Creates::Relation(name) => relations.push(name.clone()),
            Creates::Column { table, column } => {
                column_tables.push(table.clone());
                columns.push(column.clone());
            }
            Creates::Nullable { table, column } => {
                nullable_tables.push(table.clone());
                nullable.push(column.clone());
            }
            Creates::Backfill { .. } | Creates::Other => {}
        }
    }
    let missing_relations: Vec<String> = sqlx::query_scalar(
        "select name from unnest($1::text[]) name where to_regclass(name) is null",
    )
    .bind(&relations)
    .fetch_all(&mut *connection)
    .await?;
    let missing_columns: Vec<(String, String)> = sqlx::query_as(
        "select t, c from unnest($1::text[], $2::text[]) x(t, c) where not exists ( \
             select 1 from pg_attribute a where a.attrelid = to_regclass(x.t) \
               and a.attname = x.c and a.attnum > 0 and not a.attisdropped)",
    )
    .bind(&column_tables)
    .bind(&columns)
    .fetch_all(&mut *connection)
    .await?;
    let still_not_null: Vec<(String, String)> = sqlx::query_as(
        "select t, c from unnest($1::text[], $2::text[]) x(t, c) where not exists ( \
             select 1 from pg_attribute a where a.attrelid = to_regclass(x.t) \
               and a.attname = x.c and a.attnum > 0 and not a.attisdropped and not a.attnotnull)",
    )
    .bind(&nullable_tables)
    .bind(&nullable)
    .fetch_all(&mut *connection)
    .await?;
    let mut needed = Vec::new();
    for (index, statement) in statements.iter().enumerate() {
        let missing = match &statement.creates {
            Creates::Relation(name) => missing_relations.contains(name),
            Creates::Column { table, column } => {
                missing_columns.contains(&(table.clone(), column.clone()))
            }
            Creates::Nullable { table, column } => {
                still_not_null.contains(&(table.clone(), column.clone()))
            }
            Creates::Backfill { from, condition } => {
                // The table may not exist yet, in which case the script
                // creates it first and the backfill has nothing to do anyway.
                let mut savepoint = connection.begin().await?;
                let pending = sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(format!(
                    "select exists (select 1 from {from} where {condition})"
                )))
                .fetch_one(&mut *savepoint)
                .await;
                savepoint.rollback().await?;
                pending.unwrap_or(true)
            }
            Creates::Other => false,
        };
        if missing {
            needed.push(index);
        }
    }
    if !needed.is_empty() {
        needed.extend(
            statements
                .iter()
                .enumerate()
                .filter(|(_, statement)| statement.creates == Creates::Other)
                .map(|(index, _)| index),
        );
        needed.sort_unstable();
        needed.dedup();
    }
    Ok(needed)
}

fn fingerprint() -> String {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update(SESSION_SCHEMA_VERSION.to_le_bytes());
    hash.update(POSTGRES_SCHEMA_SQL);
    hash.update(POSTGRES_SATELLITE_SCHEMA_SQL);
    hex::encode(hash.finalize())
}

fn lock_unavailable(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|error| error.code())
        .is_some_and(|code| matches!(code.as_ref(), "55P03" | "40P01"))
}

impl PostgresSessionStore {
    /// Bring the schema up to this release without ever failing an open on a
    /// lock. Returns whether the schema is complete; when it is not, the store
    /// runs on what exists and `schema_incomplete` tells maintenance to retry.
    pub async fn ensure_schema(&self) -> Result<()> {
        let complete = self.migrate_schema().await?;
        self.schema_incomplete
            .store(!complete, std::sync::atomic::Ordering::Release);
        self.archive_column.forget_absent();
        Ok(())
    }

    /// Whether the last bootstrap left statements it could not apply.
    pub fn schema_incomplete(&self) -> bool {
        self.schema_incomplete
            .load(std::sync::atomic::Ordering::Acquire)
    }

    async fn migrate_schema(&self) -> Result<bool> {
        let fingerprint = fingerprint();
        // A dedicated connection: the bootstrap lock below is session-level
        // and is released when this connection closes.
        let mut connection = self.pool().acquire().await?.detach();
        let stored: Option<(i64, Option<String>)> = if sqlx::query_scalar::<_, bool>(
            "select to_regclass('borg_session_schema') is not null",
        )
        .fetch_one(&mut connection)
        .await?
        {
            sqlx::query_as(
                "select version, to_jsonb(s) ->> 'definition_hash' \
                     from borg_session_schema s where id = 1",
            )
            .fetch_optional(&mut connection)
            .await?
        } else {
            None
        };
        if let Some((version, applied)) = &stored {
            anyhow::ensure!(
                *version <= SESSION_SCHEMA_VERSION,
                "unsupported future Borg session schema version {version}; current is {SESSION_SCHEMA_VERSION}"
            );
            if *version == SESSION_SCHEMA_VERSION
                && applied.as_deref() == Some(fingerprint.as_str())
            {
                return Ok(true);
            }
        }
        let statements = statements();
        // Read-only: a schema another release already completed is left
        // alone, and its fingerprint with it, so releases running side by
        // side stop re-migrating each other.
        if needed(&mut connection, &statements).await?.is_empty() {
            return Ok(true);
        }

        connection
            .execute(sqlx::AssertSqlSafe(format!(
                "set lock_timeout = '{STATEMENT_LOCK_TIMEOUT}'"
            )))
            .await?;
        let deadline = tokio::time::Instant::now() + BOOTSTRAP_LOCK_WAIT;
        loop {
            // Same key as older releases' transaction-level bootstrap lock,
            // so a migration never runs beside theirs.
            let locked: bool = sqlx::query_scalar("select pg_try_advisory_lock($1)")
                .bind(SCHEMA_BOOTSTRAP_LOCK)
                .fetch_one(&mut connection)
                .await?;
            if locked {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(
                    "another process is migrating the session schema; opening on the existing schema"
                );
                return Ok(false);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        // The migration that held the lock may have done everything.
        let needed = needed(&mut connection, &statements).await?;
        if needed.is_empty() {
            return Ok(true);
        }
        // gin_trgm_ops in the schema requires pg_trgm. It needs privileges a
        // managed deployment's role may lack, where an operator installs it
        // once up front; a failure only matters if the index is then missing.
        if let Err(error) = connection
            .execute("create extension if not exists pg_trgm")
            .await
        {
            tracing::debug!(%error, "pg_trgm extension not created; assuming it is already installed");
        }
        let mut skipped = Vec::new();
        for index in needed {
            let statement = &statements[index];
            match sqlx::raw_sql(sqlx::AssertSqlSafe(statement.sql.clone()))
                .execute(&mut connection)
                .await
            {
                Ok(_) => {}
                Err(error) if lock_unavailable(&error) => {
                    skipped.push(
                        normalized(&statement.sql)
                            .chars()
                            .take(80)
                            .collect::<String>(),
                    );
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "failed to apply the Postgres session schema: {}",
                            normalized(&statement.sql)
                                .chars()
                                .take(120)
                                .collect::<String>()
                        )
                    });
                }
            }
        }
        let complete =
            skipped.is_empty() && self::needed(&mut connection, &statements).await?.is_empty();
        if complete {
            // `id` is `generated always as identity` with `check (id = 1)`, so
            // the single row is written with an explicit id and OVERRIDING
            // SYSTEM VALUE; a plain insert would allocate id=2.
            sqlx::query(
                "insert into borg_session_schema (id, version, definition_hash) \
                 overriding system value values (1, $1, $2) \
                 on conflict (id) do update set version = excluded.version, \
                   definition_hash = excluded.definition_hash",
            )
            .bind(SESSION_SCHEMA_VERSION)
            .bind(&fingerprint)
            .execute(&mut connection)
            .await
            .context("failed to record the Postgres session schema version")?;
        } else {
            tracing::warn!(
                ?skipped,
                "session schema statements could not get their locks (a long reader such as \
                 pg_dump holds them); opening on the existing schema, maintenance retries"
            );
        }
        Ok(complete)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_store::SessionStore;
    use crate::{SessionEvent, SessionEventKind};

    /// What pg_dump does for the whole of a dump: ACCESS SHARE on every
    /// table, in one transaction left open.
    async fn hold_access_share_on_every_table(
        pool: &sqlx::PgPool,
    ) -> sqlx::Transaction<'static, sqlx::Postgres> {
        let mut reader = pool.begin().await.unwrap();
        let tables: String = sqlx::query_scalar(
            "select string_agg(quote_ident(tablename), ', ') from pg_tables \
             where schemaname = current_schema()",
        )
        .fetch_one(&mut *reader)
        .await
        .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "lock table {tables} in access share mode"
        )))
        .execute(&mut *reader)
        .await
        .unwrap();
        reader
    }

    /// Opening must take seconds and journal normally, never queue DDL
    /// behind the reader and fail the session start.
    async fn open_and_journal(url: &str) -> PostgresSessionStore {
        let started = std::time::Instant::now();
        let store = tokio::time::timeout(
            Duration::from_secs(15),
            PostgresSessionStore::connect_with_pool_size(url, 4),
        )
        .await
        .expect("opening must not wait on DDL locks a long reader holds")
        .expect("open beside a long reader");
        assert!(started.elapsed() < Duration::from_secs(10));
        let session_id = uuid::Uuid::new_v4();
        store.create_session(session_id).await.unwrap();
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
            .unwrap();
        // Oversized, so it goes through the payload path.
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ToolCompleted {
                    tool_call_id: "call".into(),
                    output: "x".repeat(200_000),
                    output_ref: None,
                    is_error: false,
                    input: None,
                    input_ref: None,
                    parent_tool_call_id: None,
                },
            ))
            .await
            .unwrap();
        let events = store.events_after(session_id, 0, 10).await.unwrap();
        let SessionEventKind::ToolCompleted {
            output_ref: Some(reference),
            ..
        } = &events.last().unwrap().kind
        else {
            panic!("the output was deferred to a payload");
        };
        assert_eq!(store.load_payload(reference).await.unwrap().len(), 200_000);
        store
    }

    #[tokio::test]
    async fn a_session_opens_and_journals_while_a_long_reader_holds_every_table() {
        let (scratch, store) = super::super::testing::session_store().await;
        let pool = store.pool().clone();

        // Another release recorded its own fingerprint over a schema that is
        // already complete: nothing to do, and nothing to rewrite.
        sqlx::query("update borg_session_schema set definition_hash = 'older-release'")
            .execute(&pool)
            .await
            .unwrap();
        let reader = hold_access_share_on_every_table(&pool).await;
        let opened = open_and_journal(&scratch.url).await;
        assert!(!opened.schema_incomplete());
        reader.rollback().await.unwrap();
        let recorded: Option<String> =
            sqlx::query_scalar("select definition_hash from borg_session_schema")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(recorded.as_deref(), Some("older-release"));

        // A schema that predates this release, so the migration really is
        // needed but its ACCESS EXCLUSIVE statements cannot get their locks.
        sqlx::raw_sql(
            "delete from session_payloads; alter table sessions drop column archive_ref; \
             drop table session_payload_parts; drop table session_blobs; \
             alter table session_payloads alter column payload set not null",
        )
        .execute(&pool)
        .await
        .unwrap();
        let reader = hold_access_share_on_every_table(&pool).await;
        let degraded = open_and_journal(&scratch.url).await;
        assert!(degraded.schema_incomplete());
        reader.rollback().await.unwrap();

        // Maintenance finishes the migration once the reader is gone.
        degraded.ensure_schema().await.unwrap();
        assert!(!degraded.schema_incomplete());
        let nullable: bool = sqlx::query_scalar(
            "select is_nullable = 'YES' from information_schema.columns \
             where table_name = 'session_payloads' and column_name = 'payload'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(nullable);
        assert!(degraded.archive_column().await.unwrap());
        drop((opened, degraded, store));
        pool.close().await;
        scratch.discard().await;
    }

    #[test]
    fn every_schema_statement_is_recognised() {
        let statements = statements();
        assert!(statements.len() > 100);
        let script = format!("{POSTGRES_SCHEMA_SQL}\n{POSTGRES_SATELLITE_SCHEMA_SQL}");
        // Statements that change the schema must be understood, or the
        // read-only check would call an incomplete schema complete.
        for statement in &statements {
            let text = normalized(&statement.sql);
            if text.starts_with("create")
                || text.contains(" add column ")
                || text.contains("drop not null")
                || text.starts_with("update")
            {
                assert_ne!(statement.creates, Creates::Other, "unrecognised: {text}");
            }
        }
        assert!(statements.iter().any(|statement| statement.creates
            == Creates::Nullable {
                table: "session_payloads".into(),
                column: "payload".into()
            }));
        assert!(script.contains("do $$"));
    }
}
