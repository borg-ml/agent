//! Where the journal's disk goes, measured cheaply enough to run on a live
//! store.
//!
//! Table sizes come from the catalog. Breakdowns by event kind, session and
//! age come from a block sample (`tablesample system`), so a report over a
//! journal of tens of millions of rows reads about one percent of its pages
//! and takes seconds rather than a full scan. Sampled figures are scaled to
//! the whole table and labelled as estimates. Payload figures are exact:
//! that table has few rows, and its sizes are read without detoasting.

use anyhow::{Result, ensure};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::Row;

use super::PostgresSessionStore;

/// Sessions untouched this long count as idle for cache retention, and the
/// report splits every breakdown at these ages.
pub const AGE_BUCKETS_DAYS: [i64; 2] = [7, 30];

#[derive(Debug, Clone, Serialize)]
pub struct StorageReport {
    pub database_bytes: i64,
    /// Free bytes on the filesystem holding the data directory, when it is
    /// local to this process.
    pub data_directory: Option<String>,
    pub data_filesystem_free_bytes: Option<u64>,
    pub sample_percent: f64,
    pub tables: Vec<TableUsage>,
    pub indexes: Vec<IndexUsage>,
    /// `session_events` by event kind (provider events by provider kind).
    pub event_kinds: Vec<KindUsage>,
    /// Mirrored child events inside `subagent_activity` rows, by child kind.
    pub subagent_mirror_kinds: Vec<KindUsage>,
    pub payload_kinds: Vec<PayloadUsage>,
    pub workspace_event_kinds: Vec<KindUsage>,
    pub session_ages: Vec<AgeUsage>,
    pub top_sessions: Vec<SessionUsage>,
    /// Derived tables (remote view, search) split by the age of the session
    /// they describe.
    pub caches: Vec<CacheUsage>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableUsage {
    pub name: String,
    pub total_bytes: i64,
    pub heap_bytes: i64,
    pub toast_bytes: i64,
    pub index_bytes: i64,
    /// Planner estimate (`reltuples`).
    pub estimated_rows: i64,
    pub live_rows: i64,
    pub dead_rows: i64,
    pub modified_since_analyze: i64,
    pub last_vacuum: Option<DateTime<Utc>>,
    pub last_analyze: Option<DateTime<Utc>>,
    /// Planner statistics disagree with the row counters by more than 2x, or
    /// a fifth of the table changed since the last analyze.
    pub stale_statistics: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct IndexUsage {
    pub name: String,
    pub table: String,
    pub bytes: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct KindUsage {
    pub kind: String,
    pub estimated_rows: i64,
    pub estimated_bytes: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PayloadUsage {
    pub kind: String,
    pub rows: i64,
    pub raw_bytes: i64,
    pub stored_bytes: i64,
    pub largest_bytes: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgeUsage {
    /// `"<7d"`, `"7-30d"` or `">30d"` since the session was last updated.
    pub age: String,
    pub sessions: i64,
    pub estimated_event_bytes: i64,
    pub payload_bytes: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionUsage {
    pub session_id: uuid::Uuid,
    pub updated_at: DateTime<Utc>,
    pub status: Option<String>,
    pub cwd: Option<String>,
    pub estimated_event_bytes: i64,
    pub payload_bytes: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CacheUsage {
    pub table: String,
    pub age: String,
    pub estimated_bytes: i64,
}

/// The age bucket of a timestamp column, as SQL.
fn age_bucket(column: &str) -> String {
    let [recent, old] = AGE_BUCKETS_DAYS;
    format!(
        "case when {column} > now() - interval '{recent} days' then '<{recent}d' \
              when {column} > now() - interval '{old} days' then '{recent}-{old}d' \
              else '>{old}d' end"
    )
}

/// Session id embedded in a remote-view scope key (`host:<id>/session:<id>`).
const SCOPE_SESSION: &str = "substring(scope from 'session:([0-9a-f-]{36})')::uuid";

impl PostgresSessionStore {
    /// Measure the store. Read-only; `sample_percent` bounds how much of the
    /// large tables is read for the breakdowns.
    pub async fn storage_report(&self, sample_percent: f64) -> Result<StorageReport> {
        ensure!(
            sample_percent > 0.0 && sample_percent <= 100.0,
            "sample percent must be in (0, 100]"
        );
        let scale = 100.0 / sample_percent;
        let pool = self.pool();
        let database_bytes: i64 =
            sqlx::query_scalar("select pg_database_size(current_database())::bigint")
                .fetch_one(pool)
                .await?;
        // Readable only with pg_read_all_settings; a managed server may not
        // grant it, and the path is meaningless when the server is remote.
        let data_directory: Option<String> =
            sqlx::query_scalar("select current_setting('data_directory', true)")
                .fetch_one(pool)
                .await
                .ok()
                .flatten();
        let data_filesystem_free_bytes = data_directory
            .as_deref()
            .and_then(|path| filesystem_free_bytes(std::path::Path::new(path)));

        let tables = sqlx::query(
            "select c.relname::text as name, \
                    pg_total_relation_size(c.oid)::bigint as total, \
                    pg_relation_size(c.oid)::bigint as heap, \
                    coalesce(pg_total_relation_size(nullif(c.reltoastrelid, 0)), 0)::bigint as toast, \
                    pg_indexes_size(c.oid)::bigint as idx, \
                    greatest(c.reltuples, 0)::bigint as estimated, \
                    coalesce(s.n_live_tup, 0) as live, coalesce(s.n_dead_tup, 0) as dead, \
                    coalesce(s.n_mod_since_analyze, 0) as modified, \
                    greatest(s.last_vacuum, s.last_autovacuum) as vacuumed, \
                    greatest(s.last_analyze, s.last_autoanalyze) as analyzed \
             from pg_class c \
             join pg_namespace n on n.oid = c.relnamespace and n.nspname = current_schema() \
             left join pg_stat_user_tables s on s.relid = c.oid \
             where c.relkind = 'r' and pg_total_relation_size(c.oid) > 1048576 \
             order by total desc",
        )
        .fetch_all(pool)
        .await?
        .iter()
        .map(|row| {
            let estimated: i64 = row.try_get("estimated")?;
            let live: i64 = row.try_get("live")?;
            let modified: i64 = row.try_get("modified")?;
            let rows = estimated.max(live).max(1);
            Ok(TableUsage {
                name: row.try_get("name")?,
                total_bytes: row.try_get("total")?,
                heap_bytes: row.try_get("heap")?,
                toast_bytes: row.try_get("toast")?,
                index_bytes: row.try_get("idx")?,
                estimated_rows: estimated,
                live_rows: live,
                dead_rows: row.try_get("dead")?,
                modified_since_analyze: modified,
                last_vacuum: row.try_get("vacuumed")?,
                last_analyze: row.try_get("analyzed")?,
                stale_statistics: estimated.min(live).saturating_mul(2) < rows
                    || modified.saturating_mul(5) > rows,
            })
        })
        .collect::<Result<Vec<_>>>()?;

        let indexes = sqlx::query(
            "select i.relname::text as name, t.relname::text as tbl, \
                    pg_relation_size(i.oid)::bigint as bytes \
             from pg_index x join pg_class i on i.oid = x.indexrelid \
             join pg_class t on t.oid = x.indrelid \
             join pg_namespace n on n.oid = t.relnamespace and n.nspname = current_schema() \
             order by bytes desc limit 15",
        )
        .fetch_all(pool)
        .await?
        .iter()
        .map(|row| {
            Ok(IndexUsage {
                name: row.try_get("name")?,
                table: row.try_get("tbl")?,
                bytes: row.try_get("bytes")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

        let event_bytes = "coalesce(pg_column_size(event_json), 0) \
                           + coalesce(pg_column_size(event_body), 0) \
                           + pg_column_size(projection_json)";
        let event_kinds = kinds(
            pool,
            &format!(
                "select case when event_kind = 'provider_event' \
                        then 'provider_event:' || coalesce(provider_event_kind, '?') \
                        else event_kind end as kind, \
                        count(*)::bigint as n, sum({event_bytes})::bigint as bytes \
                 from session_events tablesample system ($1) group by 1"
            ),
            sample_percent,
            scale,
        )
        .await?;
        let subagent_mirror_kinds = kinds(
            pool,
            &format!(
                "select coalesce(event_json #>> '{{kind,event,kind,type}}', '(no event)') as kind, \
                        count(*)::bigint as n, sum({event_bytes})::bigint as bytes \
                 from session_events tablesample system ($1) \
                 where event_kind = 'subagent_activity' group by 1"
            ),
            sample_percent,
            scale,
        )
        .await?;
        let workspace_event_kinds = kinds(
            pool,
            "select coalesce(event_json::jsonb #>> '{kind,type}', '?') as kind, count(*)::bigint as n, \
                    sum(pg_column_size(event_json) + pg_column_size(canonical_json))::bigint as bytes \
             from workspace_events tablesample system ($1) group by 1",
            sample_percent,
            scale,
        )
        .await?;

        let payload_kinds = sqlx::query(
            "select payload_kind, count(*)::bigint as n, sum(byte_len)::bigint as raw, \
                    sum(pg_column_size(payload))::bigint as stored, max(byte_len)::bigint as largest \
             from session_payloads group by 1 order by raw desc",
        )
        .fetch_all(pool)
        .await?
        .iter()
        .map(|row| {
            Ok(PayloadUsage {
                kind: row.try_get("payload_kind")?,
                rows: row.try_get("n")?,
                raw_bytes: row.try_get("raw")?,
                stored_bytes: row.try_get("stored")?,
                largest_bytes: row.try_get("largest")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

        let bucket = age_bucket("s.updated_at");
        let session_ages = sqlx::query(sqlx::AssertSqlSafe(format!(
            "with ages as (select s.id, {bucket} as age from sessions s), \
                  events as (select e.session_id, sum({event_bytes})::bigint as bytes \
                             from session_events e tablesample system ($1) group by 1), \
                  payloads as (select session_id, sum(byte_len)::bigint as bytes \
                               from session_payloads group by 1) \
             select a.age, count(*)::bigint as sessions, \
                    coalesce(sum(e.bytes), 0)::bigint as event_bytes, \
                    coalesce(sum(p.bytes), 0)::bigint as payload_bytes \
             from ages a left join events e on e.session_id = a.id \
             left join payloads p on p.session_id = a.id group by 1 order by 1"
        )))
        .bind(sample_percent)
        .fetch_all(pool)
        .await?
        .iter()
        .map(|row| {
            Ok(AgeUsage {
                age: row.try_get("age")?,
                sessions: row.try_get("sessions")?,
                estimated_event_bytes: scaled(row.try_get("event_bytes")?, scale),
                payload_bytes: row.try_get("payload_bytes")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

        let top_sessions = sqlx::query(sqlx::AssertSqlSafe(format!(
            "with events as (select session_id, sum({event_bytes})::bigint as bytes \
                             from session_events tablesample system ($1) group by 1), \
                  payloads as (select session_id, sum(byte_len)::bigint as bytes \
                               from session_payloads group by 1) \
             select s.id, s.updated_at, \
                    s.state_json::jsonb ->> 'status' as status, \
                    s.state_json::jsonb #>> '{{configuration,cwd}}' as cwd, \
                    coalesce(e.bytes, 0)::bigint as event_bytes, \
                    coalesce(p.bytes, 0)::bigint as payload_bytes \
             from sessions s left join events e on e.session_id = s.id \
             left join payloads p on p.session_id = s.id \
             order by coalesce(e.bytes, 0) * $2 + coalesce(p.bytes, 0) desc limit 15"
        )))
        .bind(sample_percent)
        .bind(scale)
        .fetch_all(pool)
        .await?
        .iter()
        .map(|row| {
            Ok(SessionUsage {
                session_id: row.try_get("id")?,
                updated_at: row.try_get("updated_at")?,
                status: row.try_get("status")?,
                cwd: row.try_get("cwd")?,
                estimated_event_bytes: scaled(row.try_get("event_bytes")?, scale),
                payload_bytes: row.try_get("payload_bytes")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

        let mut caches = Vec::new();
        let cache_tables: Vec<String> = sqlx::query_scalar(
            "select relname::text from pg_class c \
             join pg_namespace n on n.oid = c.relnamespace and n.nspname = current_schema() \
             where c.relkind = 'r' and (relname like 'remote_view_%' or relname = 'remote_turn_results') \
             order by 1",
        )
        .fetch_all(pool)
        .await?;
        for table in cache_tables {
            // Table names come from the catalog filtered above, never input.
            let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
                "select {bucket} as age, sum(pg_column_size(r.*))::bigint as bytes \
                 from \"{table}\" r tablesample system ($1) \
                 left join sessions s on s.id = {SCOPE_SESSION} group by 1 order by 1"
            )))
            .bind(sample_percent)
            .fetch_all(pool)
            .await?;
            for row in rows {
                caches.push(CacheUsage {
                    table: table.clone(),
                    age: row.try_get("age")?,
                    estimated_bytes: scaled(row.try_get("bytes")?, scale),
                });
            }
        }
        for row in sqlx::query(sqlx::AssertSqlSafe(format!(
            "select {bucket} as age, sum(pg_column_size(r.*))::bigint as bytes \
             from session_event_search r tablesample system ($1) \
             join sessions s on s.id = r.session_id group by 1 order by 1"
        )))
        .bind(sample_percent)
        .fetch_all(pool)
        .await?
        {
            caches.push(CacheUsage {
                table: "session_event_search".to_string(),
                age: row.try_get("age")?,
                estimated_bytes: scaled(row.try_get("bytes")?, scale),
            });
        }

        Ok(StorageReport {
            database_bytes,
            data_directory,
            data_filesystem_free_bytes,
            sample_percent,
            tables,
            indexes,
            event_kinds,
            subagent_mirror_kinds,
            payload_kinds,
            workspace_event_kinds,
            session_ages,
            top_sessions,
            caches,
        })
    }
}

fn scaled(value: i64, scale: f64) -> i64 {
    (value as f64 * scale) as i64
}

/// Run a sampled `kind, n, bytes` breakdown, largest first.
async fn kinds(
    pool: &sqlx::PgPool,
    sql: &str,
    sample_percent: f64,
    scale: f64,
) -> Result<Vec<KindUsage>> {
    let mut kinds = sqlx::query(sqlx::AssertSqlSafe(sql.to_string()))
        .bind(sample_percent)
        .fetch_all(pool)
        .await?
        .iter()
        .map(|row| {
            Ok(KindUsage {
                kind: row.try_get("kind")?,
                estimated_rows: scaled(row.try_get("n")?, scale),
                estimated_bytes: scaled(row.try_get::<Option<i64>, _>("bytes")?.unwrap_or(0), scale),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    kinds.sort_by_key(|kind| std::cmp::Reverse(kind.estimated_bytes));
    Ok(kinds)
}

/// Free bytes available to an unprivileged writer on the filesystem holding
/// `path`, or `None` when the path is not local.
pub fn filesystem_free_bytes(path: &std::path::Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is NUL-terminated and `stat` is a valid out-pointer.
    let status = unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) };
    if status != 0 {
        return None;
    }
    // SAFETY: statvfs succeeded, so it initialised `stat`.
    let stat = unsafe { stat.assume_init() };
    Some(u64::from(stat.f_bavail).saturating_mul(u64::from(stat.f_frsize)))
}
