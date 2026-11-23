//! `borg storage`: measure, archive and compact the session database.

use anyhow::Result;
use borg_remote::session_store::postgres::storage::StorageReport;

use crate::cli::StorageCommand;
use crate::session_commands::{human_bytes, open_postgres};

pub(crate) async fn run(command: StorageCommand) -> Result<()> {
    match command {
        StorageCommand::Report {
            sample_percent,
            json,
        } => {
            let report = open_postgres().await?.storage_report(sample_percent).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print_report(&report);
            }
            Ok(())
        }
    }
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
            if table.stale_statistics { "STALE" } else { "ok" }
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
            "  {:<40} {:>8} rows {:>10} raw {:>10} stored  largest {}",
            payload.kind,
            payload.rows,
            human_bytes(payload.raw_bytes),
            human_bytes(payload.stored_bytes),
            human_bytes(payload.largest_bytes)
        );
    }
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
