//! Durable reducer checkpoints and bounded wire replay. Callers publish only
//! after `commit` returns; a failed compare-and-swap requires reloading state.
use anyhow::{Result, ensure};
use sqlx::{PgPool, Row};

pub const ACTIVE_CHECKPOINT_BYTES: usize = 65_536;

pub const REPLAY_FRAMES: i64 = 128;
pub const REPLAY_BYTES: usize = 1_048_576;

#[derive(Debug, Clone)]
pub struct ProjectionCheckpoint {
    pub epoch: String,
    pub revision: i64,
    pub source_sequence: i64,
    pub source_live_revision: i64,
    /// Bounded active identity/lifecycle state, never historical rows or text.
    pub reducer: Vec<u8>,
}

#[derive(Clone)]
pub struct ProjectionStore { pool: PgPool }

impl ProjectionStore {
    pub async fn open(pool: PgPool) -> Result<Self> {
        // Dedicated tables: no canonical event or provider-state projection.
        sqlx::raw_sql("create table if not exists remote_view_checkpoints (
            scope text primary key, epoch text not null, revision bigint not null,
            source_sequence bigint not null, source_live_revision bigint not null,
            reducer bytea not null);
            create table if not exists remote_view_replay (
            scope text not null references remote_view_checkpoints(scope) on delete cascade,
            epoch text not null, revision bigint not null, frame bytea not null,
            primary key(scope, revision));")
            .execute(&pool).await?;
        crate::row_index::initialize(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn load(&self, scope: &str) -> Result<Option<ProjectionCheckpoint>> {
        let row = sqlx::query("select epoch,revision,source_sequence,source_live_revision,reducer from remote_view_checkpoints where scope=$1")
            .bind(scope).fetch_optional(&self.pool).await?;
        row.map(|r| Ok(ProjectionCheckpoint {
            epoch: r.try_get("epoch")?, revision: r.try_get("revision")?,
            source_sequence: r.try_get("source_sequence")?,
            source_live_revision: r.try_get("source_live_revision")?,
            reducer: r.try_get("reducer")?,
        })).transpose()
    }

    pub async fn initialize(&self, scope: &str, state: &ProjectionCheckpoint) -> Result<bool> {
        ensure!(state.reducer.len() <= ACTIVE_CHECKPOINT_BYTES, "active checkpoint exceeds budget");
        ensure!(state.revision == 0 && state.source_sequence >= 0 && state.source_live_revision >= 0, "invalid initial checkpoint");
        Ok(sqlx::query("insert into remote_view_checkpoints values($1,$2,$3,$4,$5,$6) on conflict do nothing")
            .bind(scope).bind(&state.epoch).bind(state.revision).bind(state.source_sequence)
            .bind(state.source_live_revision).bind(&state.reducer).execute(&self.pool).await?.rows_affected() == 1)
    }

    /// Atomically persist reducer, source watermarks and already encoded typed
    /// wire frame. Oversized frames require a bounded snapshot, not publication.
    pub async fn commit(&self, scope: &str, expected_revision: i64, state: &ProjectionCheckpoint, frame: &[u8]) -> Result<()> {
        self.commit_rows(scope, expected_revision, state, frame, Vec::new()).await
    }

    pub async fn commit_rows(&self, scope: &str, expected_revision: i64, state: &ProjectionCheckpoint, frame: &[u8], rows: Vec<crate::row_index::RowMutation<'_>>) -> Result<()> {
        ensure!(state.revision == expected_revision.checked_add(1).ok_or_else(|| anyhow::anyhow!("revision exhausted"))?, "noncontiguous projection revision");
        ensure!(state.reducer.len() <= ACTIVE_CHECKPOINT_BYTES, "active checkpoint exceeds budget");
        ensure!(frame.len() <= REPLAY_BYTES, "projection frame exceeds wire budget");
        let mut tx = self.pool.begin().await?;
        let changed = sqlx::query("update remote_view_checkpoints set revision=$3,source_sequence=$4,source_live_revision=$5,reducer=$6 where scope=$1 and epoch=$2 and revision=$7 and source_sequence<=$4 and source_live_revision<=$5")
            .bind(scope).bind(&state.epoch).bind(state.revision).bind(state.source_sequence)
            .bind(state.source_live_revision).bind(&state.reducer).bind(expected_revision)
            .execute(&mut *tx).await?.rows_affected();
        ensure!(changed == 1, "projection checkpoint conflict");
        for row in rows { crate::row_index::apply(&mut tx, scope, row).await?; }
        sqlx::query("insert into remote_view_replay values($1,$2,$3,$4)")
            .bind(scope).bind(&state.epoch).bind(state.revision).bind(frame).execute(&mut *tx).await?;
        // Keep at most 128 frames and 1 MiB total, including the newest frame.
        sqlx::query("delete from remote_view_replay where scope=$1 and revision in (select revision from (select revision,row_number() over(order by revision desc) n,sum(octet_length(frame)) over(order by revision desc) bytes from remote_view_replay where scope=$1) r where n>$2 or bytes>$3)")
            .bind(scope).bind(REPLAY_FRAMES).bind(REPLAY_BYTES as i64).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    /// None means an epoch mismatch or evicted gap: caller must snapshot.
    /// Empty replay means already synchronized. Reads share one MVCC snapshot.
    pub async fn replay(&self, scope: &str, epoch: &str, after: i64) -> Result<Option<Vec<Vec<u8>>>> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("set transaction isolation level repeatable read read only").execute(&mut *tx).await?;
        let head = sqlx::query("select epoch,revision from remote_view_checkpoints where scope=$1").bind(scope).fetch_optional(&mut *tx).await?;
        let Some(head) = head else { return Ok(None) };
        let revision: i64 = head.try_get("revision")?;
        if head.try_get::<String,_>("epoch")? != epoch || after < 0 || after > revision { return Ok(None) }
        let rows = sqlx::query("select revision,frame from remote_view_replay where scope=$1 and epoch=$2 and revision>$3 order by revision limit 128")
            .bind(scope).bind(epoch).bind(after).fetch_all(&mut *tx).await?;
        if rows.len() as i64 != revision - after { return Ok(None) }
        let mut frames = Vec::with_capacity(rows.len());
        for (index,row) in rows.into_iter().enumerate() {
            if row.try_get::<i64,_>("revision")? != after + index as i64 + 1 { return Ok(None) }
            frames.push(row.try_get("frame")?);
        }
        tx.commit().await?;
        Ok(Some(frames))
    }
}

/// Never use a shared literal `host` scope in a multi-host journal.
pub fn scope_key(host_id: uuid::Uuid, session_id: Option<uuid::Uuid>) -> String {
    match session_id {
        Some(session) => format!("host:{host_id}/session:{session}"),
        None => format!("host:{host_id}/host"),
    }
}
