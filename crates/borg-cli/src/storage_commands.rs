//! `borg storage`: measure, archive and compact the session database.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use borg_remote::session_store::postgres::storage::StorageReport;

use crate::cli::StorageCommand;
use crate::session_commands::{human_bytes, open_postgres};

pub(crate) async fn run(command: StorageCommand) -> Result<()> {
    match command {
        StorageCommand::Report {
            sample_percent,
            json,
        } => {
            let report = open_postgres()
                .await?
                .storage_report(sample_percent)
                .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print_report(&report);
            }
            Ok(())
        }
        StorageCommand::Prune {
            apply,
            no_backup_check,
        } => {
            if apply && !no_backup_check {
                require_recent_backup(&archive_dir(None))?;
            }
            prune(apply).await
        }
        StorageCommand::Archive {
            older_than_days,
            archive_dir: dir,
            limit,
            apply,
            no_backup_check,
        } => {
            let dir = archive_dir(dir);
            if apply && !no_backup_check {
                require_recent_backup(&dir)?;
            }
            archive(older_than_days, &dir, limit.unwrap_or(usize::MAX), apply).await
        }
        StorageCommand::Restore { session } => {
            let store = open_postgres().await?;
            if store.restore_archived_session(session).await? {
                println!("Restored {session}.");
            } else {
                println!("{session} is not archived.");
            }
            Ok(())
        }
        StorageCommand::Backup {
            archive_dir: dir,
            jobs,
        } => backup(&archive_dir(dir), jobs),
        StorageCommand::Compact {
            tables,
            apply,
            force,
            no_backup_check,
        } => {
            if apply && !no_backup_check {
                require_recent_backup(&archive_dir(None))?;
            }
            compact(tables, apply, force).await
        }
    }
}

fn database_url() -> String {
    borg_remote::session_store::postgres::PostgresSessionStore::url_from_env().unwrap_or_else(
        || {
            borg_remote::session_store::factory::SessionStoreConfig::from_env()
                .cluster()
                .url()
        },
    )
}

/// `--archive-dir`, else `$BORG_ARCHIVE_DIR`, else `archive/` beside the
/// managed cluster in the Borg home.
fn archive_dir(requested: Option<PathBuf>) -> PathBuf {
    requested
        .or_else(|| {
            std::env::var_os("BORG_ARCHIVE_DIR")
                .filter(|value| !value.is_empty())
                .map(PathBuf::from)
        })
        .unwrap_or_else(|| {
            borg_remote::session_store::factory::SessionStoreConfig::from_env()
                .cluster()
                .data_dir()
                .parent()
                .map(|home| home.join("archive"))
                .unwrap_or_else(|| PathBuf::from("archive"))
        })
}

/// A completed `borg storage backup` leaves `<dump>.complete` beside it.
fn require_recent_backup(dir: &Path) -> Result<()> {
    let recent = std::fs::read_dir(dir.join("backups"))
        .into_iter()
        .flatten()
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".complete"))
        .filter_map(|entry| entry.metadata().ok()?.modified().ok())
        .any(|modified| {
            modified
                .elapsed()
                .is_ok_and(|age| age.as_secs() < 24 * 3600)
        });
    ensure!(
        recent,
        "no backup from the last 24 hours in {}; run `borg storage backup --archive-dir {}` first \
         (or pass --no-backup-check)",
        dir.join("backups").display(),
        dir.display()
    );
    Ok(())
}

fn backup(dir: &Path, jobs: u32) -> Result<()> {
    let backups = dir.join("backups");
    std::fs::create_dir_all(&backups)?;
    let target = backups.join(format!(
        "borg_sessions-{}",
        chrono::Utc::now().format("%Y%m%dT%H%M%SZ")
    ));
    if let Some(free) =
        borg_remote::session_store::postgres::storage::filesystem_free_bytes(&backups)
    {
        println!(
            "{} free in {}.",
            human_bytes(free as i64),
            backups.display()
        );
    }
    println!("Dumping to {} ...", target.display());
    let started = std::time::Instant::now();
    // pg_dump reads one MVCC snapshot; it takes only ACCESS SHARE locks, so
    // sessions keep writing while it runs.
    let status = std::process::Command::new("pg_dump")
        .arg(format!("--dbname={}", database_url()))
        .args(["--format=directory", "--compress=zstd:3", "--no-owner"])
        .arg(format!("--jobs={jobs}"))
        .arg(format!("--file={}", target.display()))
        .status()
        .context("failed to run pg_dump; is the PostgreSQL client installed?")?;
    ensure!(status.success(), "pg_dump failed: {status}");
    std::fs::write(target.with_extension("complete"), b"")?;
    let bytes: u64 = std::fs::read_dir(&target)?
        .flatten()
        .filter_map(|entry| entry.metadata().ok())
        .map(|metadata| metadata.len())
        .sum();
    println!(
        "Backup complete: {} in {:.0?}. Restore with: pg_restore --dbname=<url> --jobs={jobs} {}",
        human_bytes(bytes as i64),
        started.elapsed(),
        target.display()
    );
    Ok(())
}

async fn archive(older_than_days: i64, dir: &Path, limit: usize, apply: bool) -> Result<()> {
    use borg_remote::session_store::postgres::archive::archive_candidates;
    ensure!(older_than_days >= 1, "--older-than-days must be at least 1");
    let store = open_postgres().await?;
    let cutoff = chrono::Utc::now() - chrono::Duration::days(older_than_days);
    let candidates = archive_candidates(&store, cutoff, limit).await?;
    if !apply {
        let (events, payload_bytes): (i64, i64) = sqlx::query_as(
            "select (select count(*) from session_events where session_id = any($1)), \
                    (select coalesce(sum(byte_len), 0)::bigint from session_payloads \
                     where session_id = any($1))",
        )
        .bind(&candidates)
        .fetch_one(store.pool())
        .await?;
        println!(
            "Dry run. {} sessions idle over {older_than_days} days ({events} events, {} of payloads) \
             would move to {}. Re-run with --apply.",
            candidates.len(),
            human_bytes(payload_bytes),
            dir.display()
        );
        return Ok(());
    }
    let _lock = store.hold_maintenance_lock().await?;
    if let Some(free) = borg_remote::session_store::postgres::storage::filesystem_free_bytes(dir) {
        println!("{} free in {}.", human_bytes(free as i64), dir.display());
    }
    // Their remote views would otherwise read (and so restore) them.
    let views = borg_remote::session_store::postgres::storage::purge_idle_session_views(
        store.pool(),
        cutoff,
        |_, _| {},
    )
    .await?;
    println!("Dropped the remote view of {views} idle sessions.");
    let total = candidates.len();
    let (mut archived, mut skipped, mut file_bytes) = (0usize, 0usize, 0u64);
    for (index, session_id) in candidates.into_iter().enumerate() {
        match store.archive_session(session_id, dir).await? {
            Some(session) => {
                archived += 1;
                file_bytes += session.file_bytes;
                println!(
                    "[{}/{total}] {session_id}: {} events, {} payloads ({}) -> {} ({})",
                    index + 1,
                    session.events,
                    session.payloads,
                    human_bytes(session.payload_bytes as i64),
                    session.path.display(),
                    human_bytes(session.file_bytes as i64)
                );
            }
            None => {
                skipped += 1;
                println!(
                    "[{}/{total}] {session_id}: changed or in use; kept",
                    index + 1
                );
            }
        }
    }
    let blobs = store.collect_unreferenced_blobs().await?;
    println!(
        "Archived {archived} sessions into {} of session files (blobs are shared and stored once); \
         kept {skipped}. Collected {blobs} database blobs. Run `borg storage compact` to return the space.",
        human_bytes(file_bytes as i64)
    );
    Ok(())
}

/// Roughly how fast VACUUM FULL rewrites a table and rebuilds its indexes on
/// local SSD; only used to size the quiet window in the plan.
const REWRITE_BYTES_PER_SECOND: f64 = 50.0 * 1024.0 * 1024.0;

async fn compact(requested: Vec<String>, apply: bool, force: bool) -> Result<()> {
    let store = open_postgres().await?;
    let pool = store.pool();
    let report = store.storage_report(0.1).await?;
    let tables: Vec<_> = report
        .tables
        .iter()
        .filter(|table| {
            if requested.is_empty() {
                table.total_bytes > 256 * 1024 * 1024
            } else {
                requested.contains(&table.name)
            }
        })
        .collect();
    for name in &requested {
        ensure!(
            tables.iter().any(|table| &table.name == name),
            "no table {name} over 1 MiB in this database"
        );
    }
    let repack = which("pg_repack").is_some()
        && sqlx::query_scalar::<_, bool>(
            "select exists (select 1 from pg_available_extensions where name = 'pg_repack')",
        )
        .fetch_one(pool)
        .await?;
    let free = report.data_filesystem_free_bytes;
    let blockers = quiet_window_blockers(pool).await?;

    println!(
        "Method: {}.",
        if repack {
            "pg_repack (online; brief ACCESS EXCLUSIVE locks at start and swap)"
        } else {
            "VACUUM FULL (pg_repack is not installed; ACCESS EXCLUSIVE lock on each table for its whole rewrite: reads and writes to it wait)"
        }
    );
    match free {
        Some(free) => println!(
            "{} free on the data filesystem; each rewrite needs about its table's size.",
            human_bytes(free as i64)
        ),
        None => println!("Free space on the data filesystem is unknown (remote server)."),
    }
    let mut total_seconds = 0.0;
    for table in &tables {
        let seconds = table.total_bytes as f64 / REWRITE_BYTES_PER_SECOND;
        total_seconds += seconds;
        let command = if repack {
            format!(
                "pg_repack --dbname='{}' --table={} --no-order --wait-timeout=60",
                database_url(),
                table.name
            )
        } else {
            format!("VACUUM (FULL, ANALYZE) {};", table.name)
        };
        println!(
            "  {:<32} {:>10}  ~{:>4.0} min  {command}",
            table.name,
            human_bytes(table.total_bytes),
            (seconds / 60.0).ceil()
        );
    }
    println!(
        "  then ANALYZE;  (all tables, seconds, no blocking lock). Total ~{:.0} min.",
        (total_seconds / 60.0).ceil()
    );
    if blockers.is_empty() {
        println!("Quiet window: nothing is running.");
    } else {
        println!("Not a quiet window:");
        for blocker in &blockers {
            println!("  {blocker}");
        }
    }
    if !apply {
        println!("Dry run. Re-run with --apply to compact.");
        return Ok(());
    }
    ensure!(
        blockers.is_empty() || force,
        "refusing to compact while sessions are active; stop them or pass --force"
    );
    for table in &tables {
        // An exclusive request queued behind a lock holder -- above all a
        // pg_dump, which holds every table for its whole run -- stalls every
        // reader and writer of the table until it is granted. Never queue.
        let holders: Vec<String> = sqlx::query_scalar(
            "select distinct coalesce(nullif(a.application_name, ''), a.backend_type) \
                    || ' (pid ' || a.pid || ')' \
             from pg_locks l join pg_stat_activity a on a.pid = l.pid \
             where l.relation = $1::regclass and l.granted and l.pid <> pg_backend_pid()",
        )
        .bind(&table.name)
        .fetch_all(pool)
        .await?;
        if !holders.is_empty() {
            println!(
                "{}: skipped; held by {} (re-run once they finish)",
                table.name,
                holders.join(", ")
            );
            continue;
        }
        let before = table_bytes(pool, &table.name).await?;
        if let Some(free) = report.data_directory.as_deref().and_then(|dir| {
            borg_remote::session_store::postgres::storage::filesystem_free_bytes(Path::new(dir))
        }) {
            ensure!(
                free as i64 > before + before / 10 || force,
                "{} needs ~{} free to rewrite but only {} is free; free space or pass --force",
                table.name,
                human_bytes(before + before / 10),
                human_bytes(free as i64)
            );
        }
        let started = std::time::Instant::now();
        eprintln!("{}: compacting {} ...", table.name, human_bytes(before));
        if repack {
            sqlx::query("create extension if not exists pg_repack")
                .execute(pool)
                .await?;
            let status = std::process::Command::new("pg_repack")
                .arg(format!("--dbname={}", database_url()))
                .arg(format!("--table={}", table.name))
                .args(["--no-order", "--wait-timeout=60"])
                .status()
                .context("failed to run pg_repack")?;
            ensure!(
                status.success(),
                "pg_repack failed on {}: {status}",
                table.name
            );
        } else {
            let progress = tokio::spawn(report_rewrite_progress(pool.clone(), table.name.clone()));
            let mut connection = pool.acquire().await?;
            // Bound the wait for a holder that appeared since the check.
            sqlx::query("set lock_timeout = '3s'")
                .execute(&mut *connection)
                .await?;
            let result = sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                "vacuum (full, analyze) {}",
                table.name
            )))
            .execute(&mut *connection)
            .await;
            progress.abort();
            result.with_context(|| format!("VACUUM FULL of {} failed", table.name))?;
        }
        let after = table_bytes(pool, &table.name).await?;
        println!(
            "{}: {} -> {} in {:.0?}",
            table.name,
            human_bytes(before),
            human_bytes(after),
            started.elapsed()
        );
    }
    sqlx::raw_sql("analyze").execute(pool).await?;
    let size: i64 = sqlx::query_scalar("select pg_database_size(current_database())::bigint")
        .fetch_one(pool)
        .await?;
    println!("Database is now {}.", human_bytes(size));
    Ok(())
}

async fn table_bytes(pool: &sqlx::PgPool, table: &str) -> Result<i64> {
    Ok(
        sqlx::query_scalar("select pg_total_relation_size($1::regclass)::bigint")
            .bind(table)
            .fetch_one(pool)
            .await?,
    )
}

async fn report_rewrite_progress(pool: sqlx::PgPool, table: String) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(15)).await;
        let row: Option<(String, i64, i64, i64)> = sqlx::query_as(
            "select phase, heap_blks_scanned, heap_blks_total, index_rebuild_count \
             from pg_stat_progress_cluster where relid = $1::regclass",
        )
        .bind(&table)
        .fetch_optional(&pool)
        .await
        .ok()
        .flatten();
        if let Some((phase, scanned, total, indexes)) = row {
            eprintln!("{table}: {phase}, {scanned}/{total} heap blocks, {indexes} indexes rebuilt");
        }
    }
}

/// What makes this a bad moment to lock tables: sessions mid-turn and other
/// backends with work in flight.
async fn quiet_window_blockers(pool: &sqlx::PgPool) -> Result<Vec<String>> {
    let mut blockers: Vec<String> = sqlx::query_scalar(
        "select id::text || ' is ' || (state_json::jsonb ->> 'status') \
                || ' (updated ' || to_char(now() - updated_at, 'HH24:MI:SS') || ' ago)' \
         from sessions \
         where coalesce(state_json::jsonb ->> 'status', '') in ('running', 'waiting_for_approval') \
           and updated_at > now() - interval '1 hour' \
         order by updated_at desc",
    )
    .fetch_all(pool)
    .await?;
    blockers.extend(
        sqlx::query_scalar::<_, String>(
            "select 'backend ' || pid || ' (' || coalesce(nullif(application_name, ''), '?') || ') ' \
                    || state || ' for ' || to_char(now() - coalesce(xact_start, query_start), 'HH24:MI:SS') \
                    || ': ' || left(regexp_replace(query, '\\s+', ' ', 'g'), 80) \
             from pg_stat_activity \
             where datname = current_database() and pid <> pg_backend_pid() \
               and backend_type = 'client backend' and state <> 'idle'",
        )
        .fetch_all(pool)
        .await?,
    );
    Ok(blockers)
}

fn which(program: &str) -> Option<PathBuf> {
    std::env::var_os("PATH").and_then(|path| {
        std::env::split_paths(&path)
            .map(|dir| dir.join(program))
            .find(|candidate| candidate.is_file())
    })
}

async fn prune(apply: bool) -> Result<()> {
    use borg_remote::session_store::postgres::storage::IDLE_CACHE_DAYS;
    let store = open_postgres().await?;
    let idle_before = chrono::Utc::now() - chrono::Duration::days(IDLE_CACHE_DAYS);
    if !apply {
        let report = store.storage_report(1.0).await?;
        let legacy = report
            .payload_kinds
            .iter()
            .map(|kind| kind.legacy_bytes)
            .sum::<i64>();
        let idle_caches: i64 = report
            .caches
            .iter()
            .filter(|cache| cache.age != format!("<{IDLE_CACHE_DAYS}d"))
            .map(|cache| cache.estimated_bytes)
            .sum();
        let pointers: i64 = report
            .workspace_event_kinds
            .iter()
            .filter(|kind| kind.kind == "session_event")
            .map(|kind| kind.estimated_bytes)
            .sum();
        let mirrors: i64 = report
            .subagent_mirror_kinds
            .iter()
            .filter(|kind| kind.kind != "(no event)")
            .map(|kind| kind.estimated_bytes)
            .sum();
        println!("Dry run. With --apply, prune would:");
        println!(
            "  compress ~{} of readable mirrored subagent events (now written compressed);",
            human_bytes(mirrors)
        );
        println!(
            "  rewrite payloads still stored raw (up to {}) as deduplicated blobs;",
            human_bytes(legacy)
        );
        println!(
            "  drop ~{} of remote-view and search rows for sessions idle over {IDLE_CACHE_DAYS} days;",
            human_bytes(idle_caches)
        );
        println!(
            "  delete most of ~{} of workspace session-event pointers older than a day.",
            human_bytes(pointers)
        );
        println!("Row sizes exclude indexes. Nothing takes an exclusive lock.");
        return Ok(());
    }
    let _lock = store.hold_maintenance_lock().await?;
    let (stale_events, orphaned_search) = store.prune_stale_rows().await?;
    println!(
        "Deleted {stale_events} mirrored subagent rows this release no longer keeps and \
         {orphaned_search} orphaned search rows."
    );
    let (mirrors, mirror_before, mirror_after) = store
        .compress_mirrored_events(|rows, before, after| {
            if rows % 200_000 < 2_000 {
                eprintln!(
                    "mirrored subagent events: {rows} compressed ({} -> {})",
                    human_bytes(before as i64),
                    human_bytes(after as i64)
                );
            }
        })
        .await?;
    println!(
        "Compressed {mirrors} mirrored subagent events: {} -> {}.",
        human_bytes(mirror_before as i64),
        human_bytes(mirror_after as i64)
    );
    let converted = store
        .convert_legacy_payloads(usize::MAX, |outcome| {
            if outcome.payloads_converted % 100 == 0 {
                eprintln!(
                    "payloads: {} rewritten ({})",
                    outcome.payloads_converted,
                    human_bytes(outcome.bytes_converted as i64)
                );
            }
        })
        .await?;
    println!(
        "Rewrote {} raw payloads ({}) as blobs.",
        converted.payloads_converted,
        human_bytes(converted.bytes_converted as i64)
    );
    let views = borg_remote::session_store::postgres::storage::purge_idle_session_views(
        store.pool(),
        idle_before,
        |done, total| {
            if done % 100 == 0 {
                eprintln!("remote views: {done}/{total} scopes checked");
            }
        },
    )
    .await?;
    println!("Dropped the remote view of {views} idle sessions.");
    let (sessions, rows) = store
        .purge_idle_search_index(idle_before, |done, total| {
            if done % 100 == 0 {
                eprintln!("search index: {done}/{total} sessions");
            }
        })
        .await?;
    println!("Dropped {rows} search rows of {sessions} idle sessions.");
    // Nothing reads the pointers but the projection's resume watermark, which
    // pruning keeps, so a day is plenty.
    let pointers = store
        .prune_session_event_pointers(
            chrono::Utc::now() - chrono::Duration::days(1),
            |done, total, deleted| {
                if deleted % 100_000 < 5_000 {
                    eprintln!("workspace pointers: workspace {done}/{total}, {deleted} deleted");
                }
            },
        )
        .await?;
    println!("Deleted {pointers} workspace session-event pointers.");
    let blobs = store.collect_unreferenced_blobs().await?;
    println!("Collected {blobs} unreferenced blobs.");
    // Bulk deletes leave planner statistics stale; ANALYZE takes no lock
    // that blocks reads or writes.
    sqlx::raw_sql(
        "analyze session_events, session_payloads, session_blobs, session_payload_parts, \
         session_event_search, workspace_events",
    )
    .execute(store.pool())
    .await?;
    // From now on background maintenance may also rewrite payloads and
    // archive: the operator ran this with every process on this release.
    store.record_maintenance(true).await?;
    println!(
        "Background maintenance now also rewrites payloads and archives (when \
         BORG_ARCHIVE_DIR is set). Run `borg storage compact` to return the freed space \
         to the filesystem."
    );
    Ok(())
}

fn print_report(report: &StorageReport) {
    let free = report
        .data_filesystem_free_bytes
        .map(|free| format!(", {} free on its filesystem", human_bytes(free as i64)))
        .unwrap_or_default();
    println!(
        "Database: {}{free}. Breakdowns sample {}% of pages and are estimates.",
        human_bytes(report.database_bytes),
        report.sample_percent
    );
    println!();
    println!(
        "{:<32} {:>10} {:>10} {:>10} {:>10} {:>12} {:>10}  stats",
        "table", "total", "heap", "toast", "indexes", "rows", "dead"
    );
    for table in &report.tables {
        println!(
            "{:<32} {:>10} {:>10} {:>10} {:>10} {:>12} {:>10}  {}",
            table.name,
            human_bytes(table.total_bytes),
            human_bytes(table.heap_bytes),
            human_bytes(table.toast_bytes),
            human_bytes(table.index_bytes),
            table.estimated_rows.max(table.live_rows),
            table.dead_rows,
            if table.stale_statistics {
                "STALE"
            } else {
                "ok"
            }
        );
    }
    println!();
    println!("Largest indexes:");
    for index in &report.indexes {
        println!(
            "  {:<52} {:>10}  on {}",
            index.name,
            human_bytes(index.bytes),
            index.table
        );
    }
    print_kinds("session_events by kind", &report.event_kinds);
    print_kinds(
        "subagent_activity rows by mirrored child event",
        &report.subagent_mirror_kinds,
    );
    println!();
    println!("session_payloads by kind (exact):");
    for payload in &report.payload_kinds {
        println!(
            "  {:<40} {:>8} rows {:>10} raw {:>10} still raw  largest {}",
            payload.kind,
            payload.rows,
            human_bytes(payload.raw_bytes),
            human_bytes(payload.legacy_bytes),
            human_bytes(payload.largest_bytes)
        );
    }
    let blobs = &report.blobs;
    println!(
        "  blobs: {} distinct, {} of content referenced as {}, stored in {}",
        blobs.blobs,
        human_bytes(blobs.raw_bytes),
        human_bytes(blobs.referenced_bytes),
        human_bytes(blobs.stored_bytes)
    );
    print_kinds("workspace_events by kind", &report.workspace_event_kinds);
    println!();
    println!("Sessions by time since last update:");
    for age in &report.session_ages {
        println!(
            "  {:<8} {:>6} sessions {:>10} events {:>10} payloads",
            age.age,
            age.sessions,
            human_bytes(age.estimated_event_bytes),
            human_bytes(age.payload_bytes)
        );
    }
    println!();
    println!("Largest sessions:");
    for session in &report.top_sessions {
        println!(
            "  {} {:>10} events {:>10} payloads  {} {}  {}",
            session.session_id,
            human_bytes(session.estimated_event_bytes),
            human_bytes(session.payload_bytes),
            session.updated_at.format("%Y-%m-%d"),
            session.status.as_deref().unwrap_or("-"),
            session.cwd.as_deref().unwrap_or("")
        );
    }
    println!();
    println!("Derived caches by age of their session:");
    for cache in &report.caches {
        println!(
            "  {:<32} {:<8} {:>10}",
            cache.table,
            cache.age,
            human_bytes(cache.estimated_bytes)
        );
    }
}

fn print_kinds(title: &str, kinds: &[borg_remote::session_store::postgres::storage::KindUsage]) {
    println!();
    println!("{title}:");
    for kind in kinds.iter().take(20) {
        println!(
            "  {:<48} {:>12} rows {:>10}",
            kind.kind,
            kind.estimated_rows,
            human_bytes(kind.estimated_bytes)
        );
    }
    if kinds.len() > 20 {
        let rest: i64 = kinds[20..].iter().map(|kind| kind.estimated_bytes).sum();
        println!("  ({} more kinds, {})", kinds.len() - 20, human_bytes(rest));
    }
}
