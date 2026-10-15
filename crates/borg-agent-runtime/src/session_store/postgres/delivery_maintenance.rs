//! Operator commands for legacy workspace delivery rows and test databases.
//!
//! Releases up to 0.16.5 wrote one `workspace_deliveries` row per member for
//! every workspace-wide event (session projections, shared work...). Nothing
//! ever updated or deleted them; on a busy machine they reached hundreds of
//! millions of rows. Replay now derives that audience from
//! `workspace_members.joined_sequence`, which the schema backfills from these
//! same rows, so once every member has one the rows carry no information.
//!
//! Nothing here runs automatically. `prune` deletes the rows online in bounded
//! per-workspace transactions and records progress, so an interrupted run
//! resumes; the space is reused by Postgres but not returned to the
//! filesystem. `reclaim` returns it: under one short `access exclusive` lock it
//! copies the message deliveries aside, truncates the table and restores them.

use anyhow::{Result, ensure};
use serde::Serialize;
use sqlx::Row;

use super::PostgresSessionStore;

/// What the legacy delivery rows cost, from planner statistics plus an exact
/// count of the message deliveries that are kept.
#[derive(Debug, Clone, Serialize)]
pub struct DeliveryStorageReport {
    /// Heap, indexes and TOAST of `workspace_deliveries`.
    pub table_bytes: i64,
    /// Planner estimate of all rows; exact counting would scan the table.
    pub estimated_rows: i64,
    /// Message deliveries, which carry per-recipient state and are kept.
    pub message_deliveries: i64,
    pub estimated_legacy_rows: i64,
    /// What `reclaim` would return to the filesystem.
    pub estimated_reclaimable_bytes: i64,
    /// Members without `joined_sequence`. Must be zero before rows are
    /// removed, or those members would lose sight of older events.
    pub members_missing_joined_sequence: i64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DeliveryPruneOutcome {
    pub workspaces_done: u64,
    pub workspaces_total: u64,
    pub batches: u64,
    pub deleted_rows: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeliveryReclaimOutcome {
    pub kept_rows: u64,
    pub bytes_before: i64,
    pub bytes_after: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TestDatabase {
    pub name: String,
    pub bytes: i64,
    /// Someone is connected; never dropped, because a running test owns it.
    pub in_use: bool,
    pub dropped: bool,
    pub error: Option<String>,
}

impl PostgresSessionStore {
    pub async fn delivery_storage_report(&self) -> Result<DeliveryStorageReport> {
        let row = sqlx::query(
            "select pg_total_relation_size('workspace_deliveries')::bigint as table_bytes, \
                    greatest(c.reltuples, 0)::bigint as estimated_rows \
             from pg_class c where c.oid = 'workspace_deliveries'::regclass",
        )
        .fetch_one(self.pool())
        .await?;
        let table_bytes: i64 = row.try_get("table_bytes")?;
        let estimated_rows: i64 = row.try_get("estimated_rows")?;
        // Seeks through the partial message index, so this stays cheap however
        // many legacy rows exist.
        let message_deliveries: i64 = sqlx::query_scalar(
            "select count(*) from workspace_events e \
             join workspace_deliveries d \
               on d.workspace_id = e.workspace_id and d.sequence = e.sequence \
             where e.is_message",
        )
        .fetch_one(self.pool())
        .await?;
        let estimated_legacy_rows = (estimated_rows - message_deliveries).max(0);
        let kept_bytes = if estimated_rows > 0 {
            (table_bytes as f64 * message_deliveries as f64 / estimated_rows as f64) as i64
        } else {
            table_bytes
        };
        Ok(DeliveryStorageReport {
            table_bytes,
            estimated_rows,
            message_deliveries,
            estimated_legacy_rows,
            estimated_reclaimable_bytes: (table_bytes - kept_bytes).max(0),
            members_missing_joined_sequence: self.members_missing_joined_sequence().await?,
        })
    }

    async fn members_missing_joined_sequence(&self) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "select count(*) from workspace_members where joined_sequence is null",
        )
        .fetch_one(self.pool())
        .await?)
    }

    /// Delete legacy delivery rows in transactions of at most `batch_events`
    /// events of one workspace, resuming where an earlier run stopped.
    ///
    /// Message deliveries are never touched: a row goes only when its event
    /// is not a message, which `workspace_events.is_message` (a generated
    /// column) decides. `on_progress` runs after each workspace.
    pub async fn prune_legacy_deliveries(
        &self,
        batch_events: i64,
        mut on_progress: impl FnMut(&DeliveryPruneOutcome),
    ) -> Result<DeliveryPruneOutcome> {
        ensure!(batch_events > 0, "batch size must be positive");
        ensure!(
            self.members_missing_joined_sequence().await? == 0,
            "some workspace members have no joined_sequence yet; open the store with the \
             current release so the schema backfill runs before pruning"
        );
        let workspaces = sqlx::query(
            "select w.id, w.next_sequence - 1 as last_sequence, \
                    coalesce(p.through_sequence, 0) as through_sequence \
             from workspaces w \
             left join workspace_delivery_prune_progress p on p.workspace_id = w.id \
             order by w.id",
        )
        .fetch_all(self.pool())
        .await?;
        let mut outcome = DeliveryPruneOutcome {
            workspaces_total: workspaces.len() as u64,
            ..Default::default()
        };
        for workspace in &workspaces {
            let workspace_id: String = workspace.try_get("id")?;
            let last: i64 = workspace.try_get("last_sequence")?;
            let mut through: i64 = workspace.try_get("through_sequence")?;
            while through < last {
                let upper = through.saturating_add(batch_events).min(last);
                let mut transaction = self.pool().begin().await?;
                let deleted = sqlx::query(
                    "delete from workspace_deliveries d using workspace_events e \
                     where d.workspace_id = $1 and e.workspace_id = $1 \
                       and d.sequence = e.sequence \
                       and e.sequence > $2 and e.sequence <= $3 and not e.is_message",
                )
                .bind(&workspace_id)
                .bind(through)
                .bind(upper)
                .execute(&mut *transaction)
                .await?
                .rows_affected();
                sqlx::query(
                    "insert into workspace_delivery_prune_progress (workspace_id, through_sequence) \
                     values ($1, $2) on conflict (workspace_id) \
                     do update set through_sequence = excluded.through_sequence",
                )
                .bind(&workspace_id)
                .bind(upper)
                .execute(&mut *transaction)
                .await?;
                transaction.commit().await?;
                outcome.batches += 1;
                outcome.deleted_rows += deleted;
                through = upper;
            }
            outcome.workspaces_done += 1;
            on_progress(&outcome);
        }
        Ok(outcome)
    }

    /// Return the legacy rows' space to the filesystem by truncating the table
    /// and restoring only the message deliveries.
    ///
    /// Takes `access exclusive` on `workspace_deliveries` for the duration,
    /// which blocks every workspace append and replay; it only copies the
    /// message rows, found through the partial message index, so it holds the
    /// lock for seconds rather than for a scan of the old table. If the lock
    /// is not granted within `lock_timeout_secs` (a long transaction holds
    /// the table) it gives up instead of queueing every writer behind it.
    pub async fn reclaim_delivery_storage(
        &self,
        lock_timeout_secs: u64,
    ) -> Result<DeliveryReclaimOutcome> {
        let bytes_before = table_bytes(self).await?;
        let mut transaction = self.pool().begin().await?;
        sqlx::query("select set_config('lock_timeout', $1, true)")
            .bind(format!("{}s", lock_timeout_secs.max(1)))
            .execute(&mut *transaction)
            .await?;
        sqlx::query("lock table workspace_deliveries in access exclusive mode")
            .execute(&mut *transaction)
            .await?;
        let missing: i64 = sqlx::query_scalar(
            "select count(*) from workspace_members where joined_sequence is null",
        )
        .fetch_one(&mut *transaction)
        .await?;
        ensure!(
            missing == 0,
            "some workspace members have no joined_sequence yet; open the store with the \
             current release so the schema backfill runs before reclaiming"
        );
        sqlx::query(
            "create temporary table borg_kept_deliveries on commit drop as \
             select d.* from workspace_events e \
             join workspace_deliveries d \
               on d.workspace_id = e.workspace_id and d.sequence = e.sequence \
             where e.is_message",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query("truncate workspace_deliveries")
            .execute(&mut *transaction)
            .await?;
        let kept_rows =
            sqlx::query("insert into workspace_deliveries select * from borg_kept_deliveries")
                .execute(&mut *transaction)
                .await?
                .rows_affected();
        transaction.commit().await?;
        sqlx::query("analyze workspace_deliveries")
            .execute(self.pool())
            .await?;
        Ok(DeliveryReclaimOutcome {
            kept_rows,
            bytes_before,
            bytes_after: table_bytes(self).await?,
        })
    }

    /// Scratch databases test runs left on this server, dropping them when
    /// `apply` is set.
    ///
    /// A database anyone is connected to is reported and left alone: it
    /// belongs to a test that is running now. The drop is the plain form, not
    /// `with (force)`, so a test connecting in between makes it fail rather
    /// than be disconnected.
    pub async fn stale_test_databases(&self, apply: bool) -> Result<Vec<TestDatabase>> {
        let rows = sqlx::query(
            "select d.datname::text as name, pg_database_size(d.oid)::bigint as bytes, \
                    exists(select 1 from pg_stat_activity a where a.datid = d.oid) as in_use \
             from pg_database d \
             where d.datname ~ '^borg_test_[0-9a-f]{32}$' and d.datname <> current_database() \
             order by d.datname",
        )
        .fetch_all(self.pool())
        .await?;
        let mut databases = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut database = TestDatabase {
                name: row.try_get("name")?,
                bytes: row.try_get("bytes")?,
                in_use: row.try_get("in_use")?,
                dropped: false,
                error: None,
            };
            if apply && !database.in_use {
                // The name matched the pattern above: hex digits only.
                match sqlx::query(sqlx::AssertSqlSafe(format!(
                    "drop database if exists {}",
                    database.name
                )))
                .execute(self.pool())
                .await
                {
                    Ok(_) => database.dropped = true,
                    Err(error) => database.error = Some(error.to_string()),
                }
            }
            databases.push(database);
        }
        Ok(databases)
    }
}

async fn table_bytes(store: &PostgresSessionStore) -> Result<i64> {
    Ok(
        sqlx::query_scalar("select pg_total_relation_size('workspace_deliveries')::bigint")
            .fetch_one(store.pool())
            .await?,
    )
}
