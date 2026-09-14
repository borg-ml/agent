//! Additive, in-place SQLite schema migration for Borg's durable stores.
//!
//! Every store keeps one canonical `create table if not exists` batch. Older
//! databases are brought forward by re-running that batch (new tables and
//! indexes) and adding any column the batch declares that the live table
//! lacks. Only columns SQLite can add in place are migrated: no primary key,
//! uniqueness, foreign-key, or generated constraints, and `not null` only
//! with a default. Anything else is reported as unmigratable so the caller
//! can fall back to archiving the database instead of losing it silently.

use std::collections::HashSet;

use anyhow::{Context, Result, bail};
use sqlx::{Row, SqliteConnection};

/// One column declared by a canonical `create table` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnDefinition {
    pub(crate) name: String,
    /// The full column clause exactly as declared, e.g. `attempt integer not
    /// null default 0`.
    pub(crate) definition: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableDefinition {
    pub(crate) name: String,
    pub(crate) columns: Vec<ColumnDefinition>,
}

/// Parse every `create table [if not exists] <name> (...)` in a schema batch.
/// Virtual tables, indexes, and table-level constraints are ignored.
pub(crate) fn parse_tables(schema_sql: &str) -> Vec<TableDefinition> {
    let sql = strip_line_comments(schema_sql);
    let lower = sql.to_ascii_lowercase();
    let mut tables = Vec::new();
    let mut cursor = 0;
    while let Some(offset) = lower[cursor..].find("create table") {
        let start = cursor + offset;
        let Some(open) = sql[start..].find('(') else {
            break;
        };
        let header = &sql[start..start + open];
        let name = header
            .split_whitespace()
            .last()
            .map(|name| name.trim_matches(|c| c == '"' || c == '`' || c == '[' || c == ']'))
            .unwrap_or_default()
            .to_string();
        let body_start = start + open + 1;
        let Some(close) = matching_paren(&sql, body_start - 1) else {
            break;
        };
        let body = &sql[body_start..close];
        let columns = split_top_level(body)
            .into_iter()
            .map(|clause| clause.trim().to_string())
            .filter(|clause| !clause.is_empty() && !is_table_constraint(clause))
            .map(|clause| ColumnDefinition {
                name: clause
                    .split_whitespace()
                    .next()
                    .unwrap_or_default()
                    .trim_matches(|c| c == '"' || c == '`' || c == '[' || c == ']')
                    .to_string(),
                definition: clause.split_whitespace().collect::<Vec<_>>().join(" "),
            })
            .collect();
        if !name.is_empty() {
            tables.push(TableDefinition { name, columns });
        }
        cursor = close + 1;
    }
    tables
}

/// Whether SQLite can add this column to an existing table in place.
pub(crate) fn column_is_addable(definition: &str) -> bool {
    let lower = definition.to_ascii_lowercase();
    let forbidden = [
        "primary key",
        "unique",
        "references",
        "generated",
        "autoincrement",
    ];
    if forbidden.iter().any(|clause| lower.contains(clause)) {
        return false;
    }
    !lower.contains("not null") || lower.contains("default")
}

/// Add every column the canonical batch declares that the live table lacks.
/// Tables absent from the database are skipped (the batch creates them).
/// Returns the added `table.column` names; fails if a needed column cannot be
/// added in place.
pub(crate) async fn add_missing_columns(
    connection: &mut SqliteConnection,
    schema_sql: &str,
) -> Result<Vec<String>> {
    let mut added = Vec::new();
    for table in parse_tables(schema_sql) {
        let exists: i64 = sqlx::query_scalar(
            "select exists(select 1 from sqlite_master where type='table' and name=?)",
        )
        .bind(&table.name)
        .fetch_one(&mut *connection)
        .await?;
        if exists == 0 {
            continue;
        }
        // `table.name` comes from the canonical schema text compiled into the
        // binary, never from user input; SQLite cannot bind identifiers.
        let live = sqlx::query(sqlx::AssertSqlSafe(format!(
            "pragma table_info({})",
            table.name
        )))
        .fetch_all(&mut *connection)
        .await?
        .into_iter()
        .map(|row| row.get::<String, _>("name").to_ascii_lowercase())
        .collect::<HashSet<_>>();
        for column in table.columns {
            if live.contains(&column.name.to_ascii_lowercase()) {
                continue;
            }
            if !column_is_addable(&column.definition) {
                bail!(
                    "column {}.{} cannot be added in place ({})",
                    table.name,
                    column.name,
                    column.definition
                );
            }
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "alter table {} add column {}",
                table.name, column.definition
            )))
            .execute(&mut *connection)
            .await
            .with_context(|| format!("add column {}.{}", table.name, column.name))?;
            added.push(format!("{}.{}", table.name, column.name));
        }
    }
    Ok(added)
}

fn strip_line_comments(sql: &str) -> String {
    sql.lines()
        .map(|line| match line.find("--") {
            Some(index) => &line[..index],
            None => line,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn matching_paren(sql: &str, open: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (index, byte) in sql.bytes().enumerate().skip(open) {
        match byte {
            b'(' => depth += 1,
            b')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

fn split_top_level(body: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (index, byte) in body.bytes().enumerate() {
        match byte {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => {
                parts.push(&body[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    parts.push(&body[start..]);
    parts
}

fn is_table_constraint(clause: &str) -> bool {
    let lower = clause.to_ascii_lowercase();
    [
        "primary key",
        "foreign key",
        "unique",
        "check",
        "constraint",
    ]
    .iter()
    .any(|keyword| lower.starts_with(keyword))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    const SCHEMA: &str = r#"
        -- a comment with (parens) and, commas
        create table if not exists jobs (
            job_id text primary key,
            idempotency_key text not null unique,
            attempt integer not null default 0,
            result_json text,
            check (attempt >= 0),
            unique (job_id, attempt)
        );
        create index if not exists idx_jobs on jobs (attempt);
        create virtual table if not exists jobs_fts using fts5(body, content='jobs');
        create table checkpoints (
            id text primary key,
            job_id text not null references jobs(job_id) on delete cascade,
            kind text not null default 'state'
        );
    "#;

    #[test]
    fn parser_extracts_columns_and_skips_constraints_and_virtual_tables() {
        let tables = parse_tables(SCHEMA);
        let names = tables
            .iter()
            .map(|table| table.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, ["jobs", "checkpoints"]);
        let jobs = &tables[0];
        assert_eq!(
            jobs.columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["job_id", "idempotency_key", "attempt", "result_json"]
        );
        assert_eq!(
            jobs.columns[2].definition,
            "attempt integer not null default 0"
        );
    }

    #[test]
    fn addability_rejects_constraints_and_bare_not_null() {
        assert!(column_is_addable("result_json text"));
        assert!(column_is_addable("attempt integer not null default 0"));
        assert!(!column_is_addable("state text not null"));
        assert!(!column_is_addable("job_id text primary key"));
        assert!(!column_is_addable("key text not null unique"));
        assert!(!column_is_addable("job_id text references jobs(job_id)"));
    }

    #[tokio::test]
    async fn missing_addable_columns_are_added_and_rows_survive() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let mut connection = pool.acquire().await.unwrap();
        sqlx::raw_sql(
            "create table jobs (job_id text primary key, idempotency_key text not null unique);
             insert into jobs values ('j1', 'k1');",
        )
        .execute(&mut *connection)
        .await
        .unwrap();
        let added = add_missing_columns(&mut connection, SCHEMA).await.unwrap();
        assert_eq!(added, ["jobs.attempt", "jobs.result_json"]);
        let (attempt, result): (i64, Option<String>) =
            sqlx::query_as("select attempt, result_json from jobs where job_id='j1'")
                .fetch_one(&mut *connection)
                .await
                .unwrap();
        assert_eq!(attempt, 0);
        assert_eq!(result, None);
        // Re-running is a no-op.
        assert!(
            add_missing_columns(&mut connection, SCHEMA)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn unmigratable_columns_are_reported_not_forced() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let mut connection = pool.acquire().await.unwrap();
        sqlx::raw_sql("create table checkpoints (id text primary key);")
            .execute(&mut *connection)
            .await
            .unwrap();
        let error = add_missing_columns(&mut connection, SCHEMA)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("checkpoints.job_id"), "{error}");
    }
}
