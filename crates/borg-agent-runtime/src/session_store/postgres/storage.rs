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
use super::payloads::PayloadConversion;

/// Sessions untouched this long lose their derived caches (remote view,
/// search index); both are rebuilt from the journal when next used.
pub const IDLE_CACHE_DAYS: i64 = 7;

/// The report splits every breakdown at these ages.
pub const AGE_BUCKETS_DAYS: [i64; 2] = [IDLE_CACHE_DAYS, 30];

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
    pub blobs: BlobUsage,
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
    /// Bytes still held raw in `session_payloads.payload` (rows written
    /// before content-addressed blobs and not yet rewritten by prune).
    pub legacy_bytes: i64,
    pub largest_bytes: i64,
}

/// The content-addressed payload store.
#[derive(Debug, Clone, Default, Serialize)]
pub struct BlobUsage {
    pub blobs: i64,
    /// Bytes of distinct content.
    pub raw_bytes: i64,
    /// What that content occupies after compression.
    pub stored_bytes: i64,
    /// Bytes payload parts reference, counting every repeat.
    pub referenced_bytes: i64,
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
                    coalesce(sum(pg_column_size(payload)), 0)::bigint as legacy, \
                    max(byte_len)::bigint as largest \
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
                legacy_bytes: row.try_get("legacy")?,
                largest_bytes: row.try_get("largest")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
        let blobs = sqlx::query(
            "select count(*)::bigint as blobs, coalesce(sum(raw_len), 0)::bigint as raw, \
                    coalesce(sum(pg_column_size(body)), 0)::bigint as stored, \
                    (select coalesce(sum(b.raw_len), 0)::bigint from session_payload_parts p \
                     join session_blobs b on b.hash = p.blob_hash) as referenced \
             from session_blobs",
        )
        .fetch_one(pool)
        .await?;
        let blobs = BlobUsage {
            blobs: blobs.try_get("blobs")?,
            raw_bytes: blobs.try_get("raw")?,
            stored_bytes: blobs.try_get("stored")?,
            referenced_bytes: blobs.try_get("referenced")?,
        };

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
            blobs,
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
                estimated_bytes: scaled(
                    row.try_get::<Option<i64>, _>("bytes")?.unwrap_or(0),
                    scale,
                ),
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
    // The field widths differ by platform (u32 on macOS).
    #[allow(clippy::useless_conversion)]
    Some(u64::from(stat.f_bavail).saturating_mul(u64::from(stat.f_frsize)))
}

/// Rows deleted per transaction by the retention passes below.
const PRUNE_BATCH: i64 = 5_000;

impl PostgresSessionStore {
    /// Drop the search index of every session idle since before
    /// `idle_before`. The index is derived: searching such a session by id
    /// rebuilds its rows, and global search reports it as not indexed.
    pub async fn purge_idle_search_index(
        &self,
        idle_before: DateTime<Utc>,
        mut progress: impl FnMut(u64, u64),
    ) -> Result<(u64, u64)> {
        let sessions: Vec<uuid::Uuid> = sqlx::query_scalar(
            "select s.id from sessions s where s.updated_at < $1 \
               and exists (select 1 from session_event_search x where x.session_id = s.id) \
             order by s.updated_at",
        )
        .bind(idle_before)
        .fetch_all(self.pool())
        .await?;
        let total = sessions.len() as u64;
        let mut rows = 0;
        for (index, session_id) in sessions.into_iter().enumerate() {
            loop {
                let deleted = sqlx::query(
                    "delete from session_event_search where session_id = $1 and event_id in ( \
                         select event_id from session_event_search where session_id = $1 limit $2)",
                )
                .bind(session_id)
                .bind(PRUNE_BATCH)
                .execute(self.pool())
                .await?
                .rows_affected();
                rows += deleted;
                if deleted == 0 {
                    break;
                }
            }
            progress(index as u64 + 1, total);
        }
        Ok((total, rows))
    }

    /// Delete `session_event` workspace pointers created before `cutoff`.
    ///
    /// Each pointer only names a session event the journal already holds, and
    /// nothing reads them except the projection's own resume watermark: the
    /// highest projected sequence per session. So the newest pointer of every
    /// session is kept and the watermark is unchanged. Messages, shared work
    /// and every other workspace event are untouched.
    pub async fn prune_session_event_pointers(
        &self,
        cutoff: DateTime<Utc>,
        mut progress: impl FnMut(u64, u64, u64),
    ) -> Result<u64> {
        let workspaces: Vec<String> = sqlx::query_scalar("select id from workspaces order by id")
            .fetch_all(self.pool())
            .await?;
        let total = workspaces.len() as u64;
        let mut deleted_total = 0;
        for (index, workspace_id) in workspaces.into_iter().enumerate() {
            // Pointers appended after this read have higher sequences than
            // `through`, so they are never candidates in this pass.
            let keep: Vec<i64> = sqlx::query_scalar(
                "select max(sequence) from workspace_events \
                 where workspace_id = $1 and not is_message \
                   and (event_json::jsonb #>> '{kind,type}') = 'session_event' \
                 group by event_json::jsonb #>> '{kind,session_id}'",
            )
            .bind(&workspace_id)
            .fetch_all(self.pool())
            .await?;
            let through = keep.iter().copied().max().unwrap_or(0);
            let mut after = 0i64;
            loop {
                let deleted: Vec<i64> = sqlx::query_scalar(
                    "delete from workspace_events where workspace_id = $1 and sequence in ( \
                         select sequence from workspace_events \
                         where workspace_id = $1 and sequence > $2 and sequence < $3 \
                           and not is_message \
                           and (event_json::jsonb #>> '{kind,type}') = 'session_event' \
                           and created_at::timestamptz < $4 and sequence <> all($5) \
                         order by sequence limit $6) \
                     returning sequence",
                )
                .bind(&workspace_id)
                .bind(after)
                .bind(through)
                .bind(cutoff)
                .bind(&keep)
                .bind(PRUNE_BATCH)
                .fetch_all(self.pool())
                .await?;
                let Some(last) = deleted.iter().copied().max() else {
                    break;
                };
                after = last;
                deleted_total += deleted.len() as u64;
                progress(index as u64, total, deleted_total);
            }
        }
        progress(total, total, deleted_total);
        Ok(deleted_total)
    }
}

impl PostgresSessionStore {
    /// Rewrite readable mirrored child events into the compressed tier, as
    /// appends now write them. Trains the event dictionary first when the
    /// store has none. Batched by primary key and resumable: it only ever
    /// selects rows that are still readable. Returns `(rows, bytes before,
    /// bytes after)`.
    pub async fn compress_mirrored_events(
        &self,
        mut progress: impl FnMut(u64, u64, u64),
    ) -> Result<(u64, u64, u64)> {
        let dictionary = self.current_dictionary().await?;
        let (mut rows, mut before, mut after) = (0u64, 0u64, 0u64);
        let mut cursor = (uuid::Uuid::nil(), -1i64);
        loop {
            let batch = sqlx::query(
                "select session_id, sequence, event_json::text as body from session_events \
                 where (session_id, sequence) > ($1, $2) and event_kind = 'subagent_activity' \
                   and event_json is not null \
                   and jsonb_typeof(event_json -> 'kind' -> 'event') = 'object' \
                 order by session_id, sequence limit $3",
            )
            .bind(cursor.0)
            .bind(cursor.1)
            .bind(2_000i64)
            .fetch_all(self.pool())
            .await?;
            let Some(last) = batch.last() else { break };
            cursor = (last.try_get("session_id")?, last.try_get("sequence")?);
            let mut transaction = self.pool().begin().await?;
            for row in &batch {
                let body: String = row.try_get("body")?;
                let super::body::StoredBody::Compressed { bytes, dict_id } =
                    super::body::compress_at(
                        body.as_bytes(),
                        dictionary.as_deref(),
                        super::body::APPEND_COMPRESSION_LEVEL,
                    )?
                else {
                    anyhow::bail!("compression produces a compressed body");
                };
                // Only a still-readable row is rewritten, so a concurrent
                // pass or restore never has its row overwritten twice.
                sqlx::query(
                    "update session_events set event_body = $1, dict_id = $2, event_json = null \
                     where session_id = $3 and sequence = $4 and event_json is not null",
                )
                .bind(&bytes)
                .bind(dict_id)
                .bind(row.try_get::<uuid::Uuid, _>("session_id")?)
                .bind(row.try_get::<i64, _>("sequence")?)
                .execute(&mut *transaction)
                .await?;
                before += body.len() as u64;
                after += bytes.len() as u64;
            }
            transaction.commit().await?;
            rows += batch.len() as u64;
            progress(rows, before, after);
        }
        Ok((rows, before, after))
    }
}

/// Scope-keyed remote-view tables (created by borg-remote's projector),
/// dependents first. Every row in them is derived from the journal.
const SCOPED_TABLES: [&str; 8] = [
    "remote_view_field_chunks",
    "remote_view_text_versions",
    "remote_view_text_heads",
    "remote_view_text_chunks",
    "remote_view_rows",
    "remote_turn_results",
    "remote_view_replay",
    "remote_view_checkpoints",
];

/// Drop the remote view of every session idle since before `idle_before`.
/// Each scope goes in its own short transaction and is skipped while its
/// session row is locked by a writer. Returns the scopes purged.
pub async fn purge_idle_session_views(
    pool: &sqlx::PgPool,
    idle_before: chrono::DateTime<chrono::Utc>,
    mut progress: impl FnMut(u64, u64),
) -> Result<u64> {
    let mut scopes = std::collections::BTreeSet::new();
    for table in SCOPED_TABLES {
        if !sqlx::query_scalar::<_, bool>("select to_regclass($1) is not null")
            .bind(table)
            .fetch_one(pool)
            .await?
        {
            continue;
        }
        // A skip scan over the primary key, which leads with `scope`: one
        // index probe per scope rather than a pass over millions of rows.
        let found: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "with recursive s(scope) as ( \
                 (select scope from {table} order by scope limit 1) \
                 union all \
                 select (select scope from {table} where scope > s.scope order by scope limit 1) \
                 from s where s.scope is not null) \
             select scope from s where scope is not null"
        )))
        .fetch_all(pool)
        .await?;
        scopes.extend(found);
    }
    let candidates: Vec<(String, uuid::Uuid)> = scopes
        .into_iter()
        .filter_map(|scope| {
            let session = scope.split_once("/session:")?.1.parse().ok()?;
            Some((scope, session))
        })
        .collect();
    let total = candidates.len() as u64;
    let mut purged = 0;
    for (index, (scope, session)) in candidates.into_iter().enumerate() {
        let mut tx = pool.begin().await?;
        // A missing session row means the journal itself is gone.
        let idle: Option<bool> = sqlx::query_scalar(
            "select coalesce((select updated_at < $2 from sessions where id = $1 \
                              for share skip locked), \
                             not exists (select 1 from sessions where id = $1))",
        )
        .bind(session)
        .bind(idle_before)
        .fetch_one(&mut *tx)
        .await?;
        if idle != Some(true) {
            tx.rollback().await?;
            continue;
        }
        for table in SCOPED_TABLES {
            if sqlx::query_scalar::<_, bool>("select to_regclass($1) is not null")
                .bind(table)
                .fetch_one(&mut *tx)
                .await?
            {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "delete from {table} where scope = $1"
                )))
                .bind(&scope)
                .execute(&mut *tx)
                .await?;
            }
        }
        tx.commit().await?;
        purged += 1;
        progress(index as u64 + 1, total);
    }
    Ok(purged)
}

/// Advisory lock key that lets one process at a time run maintenance.
const MAINTENANCE_LOCK: i64 = 0x0B01_65E5_5702;
/// How often retention runs, machine-wide.
const MAINTENANCE_INTERVAL_HOURS: i32 = 6;
/// Payloads rewritten per pass; the rest wait for later passes.
const MAINTENANCE_PAYLOADS: usize = 500;
/// Session trees archived per pass.
const MAINTENANCE_ARCHIVES: usize = 100;

/// Run retention in the background for as long as this process lives, so
/// the journal stays bounded without anyone running `borg storage prune`.
///
/// Every long-lived Borg process starts this; an advisory lock and a
/// last-run row make one pass run machine-wide per interval. A pass drops
/// the derived caches of idle sessions and old workspace pointers, which any
/// release reads correctly or rebuilds. Rewriting legacy payloads and
/// archiving idle session trees (when `BORG_ARCHIVE_DIR` names an archive
/// directory; `BORG_ARCHIVE_AFTER_DAYS`, default 30) change data an older
/// release cannot read, so they wait until an operator has run `borg storage
/// prune --apply` once -- after restarting every Borg process on this
/// release. It never rewrites tables; returning space to the filesystem
/// stays `borg storage compact`. `BORG_STORAGE_MAINTENANCE=0` turns it off.
pub fn spawn_storage_maintenance(pool: sqlx::PgPool) {
    if std::env::var("BORG_STORAGE_MAINTENANCE").as_deref() == Ok("0") {
        return;
    }
    tokio::spawn(async move {
        // Let startup and the first turn have the database to themselves.
        tokio::time::sleep(std::time::Duration::from_secs(600)).await;
        let store = match PostgresSessionStore::from_pool(pool).await {
            Ok(store) => store,
            Err(error) => {
                tracing::warn!(%error, "storage maintenance could not open the store");
                return;
            }
        };
        loop {
            if let Err(error) = store.maintenance_pass().await {
                tracing::warn!(%error, "storage maintenance pass failed; retrying later");
            }
            tokio::time::sleep(std::time::Duration::from_secs(1800)).await;
        }
    });
}

impl PostgresSessionStore {
    /// Record a completed retention pass; `operator` marks one run by
    /// `borg storage prune --apply`, which enables the background steps an
    /// older release could not read.
    pub async fn record_maintenance(&self, operator: bool) -> Result<()> {
        sqlx::query(
            "insert into borg_storage_maintenance (id, last_run, operator_prune_at) \
             values (1, now(), case when $1 then now() end) \
             on conflict (id) do update set last_run = excluded.last_run, \
               operator_prune_at = coalesce(excluded.operator_prune_at, \
                                            borg_storage_maintenance.operator_prune_at)",
        )
        .bind(operator)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// Take the maintenance lock for an operator command, waiting while a
    /// background pass holds it. Released when the returned connection drops.
    pub async fn hold_maintenance_lock(&self) -> Result<sqlx::PgConnection> {
        let mut connection = self.pool().acquire().await?.detach();
        let locked: bool = sqlx::query_scalar("select pg_try_advisory_lock($1)")
            .bind(MAINTENANCE_LOCK)
            .fetch_one(&mut connection)
            .await?;
        if !locked {
            eprintln!("Waiting for a background storage maintenance pass to finish ...");
            sqlx::query("select pg_advisory_lock($1)")
                .bind(MAINTENANCE_LOCK)
                .execute(&mut connection)
                .await?;
        }
        Ok(connection)
    }

    /// One maintenance pass, if this process wins the lock and one is due.
    async fn maintenance_pass(&self) -> Result<()> {
        let mut lock = self.pool().acquire().await?;
        let locked: bool = sqlx::query_scalar("select pg_try_advisory_lock($1)")
            .bind(MAINTENANCE_LOCK)
            .fetch_one(&mut *lock)
            .await?;
        if !locked {
            return Ok(());
        }
        let result = self.maintenance_steps().await;
        sqlx::query("select pg_advisory_unlock($1)")
            .bind(MAINTENANCE_LOCK)
            .execute(&mut *lock)
            .await?;
        result
    }

    async fn maintenance_steps(&self) -> Result<()> {
        let (due, operator_pruned): (bool, bool) = sqlx::query_as(
            "select coalesce((select last_run < now() - make_interval(hours => $1) \
                              from borg_storage_maintenance where id = 1), true), \
                    exists (select 1 from borg_storage_maintenance \
                            where id = 1 and operator_prune_at is not null)",
        )
        .bind(MAINTENANCE_INTERVAL_HOURS)
        .fetch_one(self.pool())
        .await?;
        if !due {
            return Ok(());
        }
        let idle_before = Utc::now() - chrono::Duration::days(IDLE_CACHE_DAYS);
        let (_, search_rows) = self.purge_idle_search_index(idle_before, |_, _| {}).await?;
        let views = purge_idle_session_views(self.pool(), idle_before, |_, _| {}).await?;
        let pointers = self
            .prune_session_event_pointers(Utc::now() - chrono::Duration::days(1), |_, _, _| {})
            .await?;
        let mut payloads = PayloadConversion::default();
        let mut archived = 0;
        if operator_pruned {
            payloads = self
                .convert_legacy_payloads(MAINTENANCE_PAYLOADS, |_| {})
                .await?;
        }
        if operator_pruned
            && let Some(dir) = std::env::var_os("BORG_ARCHIVE_DIR").filter(|dir| !dir.is_empty())
        {
            let days = std::env::var("BORG_ARCHIVE_AFTER_DAYS")
                .ok()
                .and_then(|days| days.parse::<i64>().ok())
                .filter(|days| *days >= 1)
                .unwrap_or(super::archive::DEFAULT_ARCHIVE_AFTER_DAYS);
            let cutoff = Utc::now() - chrono::Duration::days(days);
            let dir = std::path::PathBuf::from(dir);
            // An unmounted archive drive must not turn into a directory on
            // whatever filesystem is underneath.
            anyhow::ensure!(
                dir.is_dir(),
                "BORG_ARCHIVE_DIR {} does not exist; not archiving",
                dir.display()
            );
            // Their remote views would otherwise read (and so restore) them.
            purge_idle_session_views(self.pool(), cutoff, |_, _| {}).await?;
            for session_id in
                super::archive::archive_candidates(self, cutoff, MAINTENANCE_ARCHIVES).await?
            {
                if self.archive_session(session_id, &dir).await?.is_some() {
                    archived += 1;
                }
            }
        }
        let blobs = self.collect_unreferenced_blobs().await?;
        self.record_maintenance(false).await?;
        tracing::info!(
            search_rows,
            views,
            pointers,
            payloads = payloads.payloads_converted,
            archived,
            blobs,
            "storage maintenance pass complete"
        );
        Ok(())
    }
}
