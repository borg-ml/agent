//! The persistent-runtime tier: manifests, checkpoints and harness state.
//!
//! WHY THIS EXISTS: these three groups all hang off a session and were the last
//! part of the journal with tables in `postgres_schema.sql` but no code behind
//! them. That gap was invisible from the outside -- the tables existed, the
//! schema bootstrapped cleanly -- and it is exactly what kept a Postgres-backed
//! process from serving the tool dispatcher, which reaches for harness state on
//! nearly every turn.
//!
//! WHAT IS DIFFERENT FROM SQLITE, AND WHY:
//!
//! * Revision allocation takes the session's row lock. SQLite could compute
//!   `max(revision) + 1` and insert without a lock because the whole file had
//!   one writer, so no second allocator could exist. Here two workers really
//!   can save a checkpoint for the same session at once, and both would read
//!   the same maximum. `select ... for update` on the `sessions` row serialises
//!   them, matching the allocator the journal itself uses. The schema's
//!   `unique (session_id, revision)` is the backstop, not the mechanism: it
//!   would turn the race into a spurious error rather than preventing it.
//!
//! * Checkpoint state is stored and read as text, never jsonb. The content hash
//!   covers the exact stored bytes; see the long comment on the table in
//!   `postgres_schema.sql` for why normalising them would break the integrity
//!   check it exists to provide.
//!
//! * `worker_id` is text here, as in SQLite, rather than `uuid`. Ownership is
//!   compared for equality only, and keeping the column faithful means the two
//!   backends bind identical Rust types.

use anyhow::{Context, Result, ensure};
use chrono::Utc;
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use uuid::Uuid;

use super::PostgresSessionStore;
use crate::session_store::{
    HARNESS_CHECKPOINT_PREFIX, MAX_RUNTIME_CHECKPOINT_BYTES, RUNTIME_MANIFEST_VERSION,
    RuntimeCheckpoint, RuntimeManifest, RuntimeManifestActivation, RuntimeManifestStatus,
};

/// The columns every manifest read selects, in one place so the decoder and its
/// queries cannot drift apart.
///
/// A macro rather than a `const` because sqlx refuses a query built with
/// `format!` -- dynamic SQL has to be audited for injection. `concat!` splices
/// these at compile time, so every query below stays a single string literal
/// and keeps that protection while still having one definition of the columns.
macro_rules! manifest_columns {
    () => {
        "manifest_version, session_id, runtime, root, command, worker_id, status, \
         execution_count, last_code_hash, last_error, created_at, updated_at"
    };
}

macro_rules! checkpoint_columns {
    () => {
        "session_id, checkpoint_key, state_json, content_hash, revision, created_at"
    };
}

fn decode_manifest(row: &PgRow) -> Result<RuntimeManifest> {
    let manifest_version = u32::try_from(row.try_get::<i64, _>("manifest_version")?)
        .context("negative runtime manifest version")?;
    ensure!(
        manifest_version == RUNTIME_MANIFEST_VERSION,
        "unsupported runtime manifest version {manifest_version}"
    );
    Ok(RuntimeManifest {
        manifest_version,
        session_id: row.try_get("session_id")?,
        runtime: row.try_get("runtime")?,
        root: row.try_get("root")?,
        command: row.try_get("command")?,
        worker_id: row
            .try_get::<String, _>("worker_id")?
            .parse()
            .context("runtime manifest worker_id is not a UUID")?,
        status: serde_json::from_value(serde_json::Value::String(row.try_get("status")?))
            .context("unknown runtime manifest status")?,
        execution_count: u64::try_from(row.try_get::<i64, _>("execution_count")?)
            .context("negative runtime execution count")?,
        last_code_hash: row.try_get("last_code_hash")?,
        last_error: row.try_get("last_error")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Decode a checkpoint, refusing any row whose stored hash does not match its
/// stored state. A checkpoint is replayed into a live runtime, so a silently
/// corrupted one is worse than a missing one.
fn decode_checkpoint(row: &PgRow) -> Result<RuntimeCheckpoint> {
    let state_json: String = row.try_get("state_json")?;
    ensure!(
        state_json.len() <= MAX_RUNTIME_CHECKPOINT_BYTES,
        "runtime checkpoint exceeds {MAX_RUNTIME_CHECKPOINT_BYTES} bytes"
    );
    let state: serde_json::Value = serde_json::from_str(&state_json)?;
    ensure!(
        state.is_object(),
        "runtime checkpoint state is not a JSON object"
    );
    let content_hash: String = row.try_get("content_hash")?;
    ensure!(
        content_hash == hash_state(state_json.as_bytes()),
        "runtime checkpoint content hash does not match its state"
    );
    Ok(RuntimeCheckpoint {
        session_id: row.try_get("session_id")?,
        key: row.try_get("checkpoint_key")?,
        state,
        content_hash,
        revision: u64::try_from(row.try_get::<i64, _>("revision")?)
            .context("negative runtime checkpoint revision")?,
        created_at: row.try_get("created_at")?,
    })
}

fn hash_state(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

/// Serialise checkpoint state once, so the bytes that are hashed are exactly
/// the bytes that are stored.
fn encode_state(state: &serde_json::Value, label: &str) -> Result<(String, String)> {
    ensure!(state.is_object(), "{label} must be a JSON object");
    let bytes = serde_json::to_vec(state)?;
    ensure!(
        bytes.len() <= MAX_RUNTIME_CHECKPOINT_BYTES,
        "{label} exceeds {MAX_RUNTIME_CHECKPOINT_BYTES} bytes"
    );
    let content_hash = hash_state(&bytes);
    let text = String::from_utf8(bytes).context("checkpoint state JSON was not UTF-8")?;
    Ok((text, content_hash))
}

/// Take the session's row lock, and confirm the session exists.
///
/// Every revision allocation in this module goes through here; see the module
/// comment for why an unlocked `max(revision) + 1` is not safe on Postgres.
async fn lock_session(transaction: &mut Transaction<'_, Postgres>, session_id: Uuid) -> Result<()> {
    sqlx::query("select 1 from sessions where id = $1 for update")
        .bind(session_id)
        .fetch_optional(&mut **transaction)
        .await?
        .with_context(|| format!("session {session_id} does not exist"))?;
    Ok(())
}

/// Allocate the next checkpoint revision. Callers must already hold the lock.
async fn next_revision(
    transaction: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "select coalesce(max(revision), 0) + 1 from runtime_checkpoints where session_id = $1",
    )
    .bind(session_id)
    .fetch_one(&mut **transaction)
    .await?)
}

impl PostgresSessionStore {
    /// Claim this session's runtime manifest for `worker_id`.
    ///
    /// Reports whether it was taken over from a previous worker, which is how a
    /// caller learns that the old process died without stopping cleanly.
    pub(crate) async fn activate_runtime_manifest(
        &self,
        session_id: Uuid,
        runtime: &str,
        root: &str,
        command: &str,
        worker_id: Uuid,
    ) -> Result<RuntimeManifestActivation> {
        ensure!(
            !runtime.trim().is_empty() && runtime.len() <= 128,
            "invalid runtime name"
        );
        ensure!(
            !root.trim().is_empty() && root.len() <= 16 * 1024,
            "invalid runtime root"
        );
        ensure!(
            !command.trim().is_empty() && command.len() <= 4 * 1024,
            "invalid runtime command"
        );

        let mut transaction = self.pool().begin().await?;
        // The lock also proves the session exists, so this is one round trip
        // rather than a separate existence check that another writer could
        // invalidate before the insert.
        lock_session(&mut transaction, session_id).await?;
        let existing = sqlx::query(concat!(
            "select ",
            manifest_columns!(),
            " from runtime_manifests where session_id = $1"
        ))
        .bind(session_id)
        .fetch_optional(&mut *transaction)
        .await?;
        let now = Utc::now();
        let (manifest, recovered_from_previous_worker) = if let Some(row) = existing {
            let existing = decode_manifest(&row)?;
            // A manifest is bound to its runtime and root for life. Rebinding
            // would point a live session's checkpoints at a different tree.
            ensure!(
                existing.runtime == runtime,
                "runtime manifest for session {session_id} is for `{}`, not `{runtime}`",
                existing.runtime
            );
            ensure!(
                existing.root == root,
                "runtime manifest for session {session_id} is bound to a different root"
            );
            let recovered = existing.worker_id != worker_id;
            sqlx::query(
                "update runtime_manifests set command = $1, worker_id = $2, status = 'running', \
                 updated_at = $3 where session_id = $4",
            )
            .bind(command)
            .bind(worker_id.to_string())
            .bind(now)
            .bind(session_id)
            .execute(&mut *transaction)
            .await?;
            let mut manifest = existing;
            manifest.command = command.to_string();
            manifest.worker_id = worker_id;
            manifest.status = RuntimeManifestStatus::Running;
            manifest.updated_at = now;
            (manifest, recovered)
        } else {
            sqlx::query(
                "insert into runtime_manifests \
                 (session_id, manifest_version, runtime, root, command, worker_id, status, \
                  execution_count, last_code_hash, last_error, created_at, updated_at) \
                 values ($1, $2, $3, $4, $5, $6, 'running', 0, null, null, $7, $7)",
            )
            .bind(session_id)
            .bind(i64::from(RUNTIME_MANIFEST_VERSION))
            .bind(runtime)
            .bind(root)
            .bind(command)
            .bind(worker_id.to_string())
            .bind(now)
            .execute(&mut *transaction)
            .await?;
            (
                RuntimeManifest {
                    manifest_version: RUNTIME_MANIFEST_VERSION,
                    session_id,
                    runtime: runtime.to_string(),
                    root: root.to_string(),
                    command: command.to_string(),
                    worker_id,
                    status: RuntimeManifestStatus::Running,
                    execution_count: 0,
                    last_code_hash: None,
                    last_error: None,
                    created_at: now,
                    updated_at: now,
                },
                false,
            )
        };
        transaction.commit().await?;
        Ok(RuntimeManifestActivation {
            manifest,
            recovered_from_previous_worker,
        })
    }

    /// Record one execution against a manifest this worker still owns.
    ///
    /// The `worker_id` predicate is the fence: a worker that was taken over by
    /// a newer one updates zero rows and is told so, rather than continuing to
    /// write into a session it no longer owns.
    pub(crate) async fn record_runtime_execution(
        &self,
        session_id: Uuid,
        worker_id: Uuid,
        code_hash: &str,
        worker_failed: bool,
        error: Option<&str>,
    ) -> Result<()> {
        ensure!(
            !code_hash.trim().is_empty(),
            "runtime execution code hash is empty"
        );
        // Truncated by characters, not bytes, so a multi-byte boundary is never
        // split -- Postgres text would reject the invalid sequence.
        let error = error.map(|value| value.chars().take(8 * 1024).collect::<String>());
        let status = if worker_failed { "failed" } else { "running" };
        let result = sqlx::query(
            "update runtime_manifests set status = $1, execution_count = execution_count + 1, \
             last_code_hash = $2, last_error = $3, updated_at = $4 \
             where session_id = $5 and worker_id = $6",
        )
        .bind(status)
        .bind(code_hash)
        .bind(error)
        .bind(Utc::now())
        .bind(session_id)
        .bind(worker_id.to_string())
        .execute(self.pool())
        .await?;
        ensure!(
            result.rows_affected() == 1,
            "runtime manifest for session {session_id} is not owned by worker {worker_id}"
        );
        Ok(())
    }

    /// Mark this worker's manifest stopped. Fenced on `worker_id` like
    /// `record_runtime_execution`, so a superseded worker cannot stop the
    /// runtime its successor is now driving.
    pub(crate) async fn stop_runtime_manifest(
        &self,
        session_id: Uuid,
        worker_id: Uuid,
    ) -> Result<()> {
        let result = sqlx::query(
            "update runtime_manifests set status = 'stopped', updated_at = $1 \
             where session_id = $2 and worker_id = $3",
        )
        .bind(Utc::now())
        .bind(session_id)
        .bind(worker_id.to_string())
        .execute(self.pool())
        .await?;
        ensure!(
            result.rows_affected() == 1,
            "runtime manifest for session {session_id} is not owned by worker {worker_id}"
        );
        Ok(())
    }

    /// This session's runtime manifest, if it has ever been activated.
    pub(crate) async fn runtime_manifest(
        &self,
        session_id: Uuid,
    ) -> Result<Option<RuntimeManifest>> {
        let row = sqlx::query(concat!(
            "select ",
            manifest_columns!(),
            " from runtime_manifests where session_id = $1"
        ))
        .bind(session_id)
        .fetch_optional(self.pool())
        .await?;
        row.as_ref().map(decode_manifest).transpose()
    }

    /// Save a named checkpoint for a manifest this worker owns.
    ///
    /// Saving the same key twice is idempotent when the content matches and an
    /// error when it does not: a checkpoint key names one immutable state, so
    /// silently overwriting it would let a replay produce a different runtime
    /// than the one that was recorded.
    pub(crate) async fn save_runtime_checkpoint(
        &self,
        session_id: Uuid,
        worker_id: Uuid,
        key: &str,
        state: &serde_json::Value,
    ) -> Result<RuntimeCheckpoint> {
        ensure!(
            !key.trim().is_empty() && key.len() <= 256,
            "invalid runtime checkpoint key"
        );
        ensure!(
            !key.starts_with(HARNESS_CHECKPOINT_PREFIX),
            "runtime checkpoint key is reserved for harness state"
        );
        let (state_json, content_hash) = encode_state(state, "runtime checkpoint state")?;

        let mut transaction = self.pool().begin().await?;
        lock_session(&mut transaction, session_id).await?;
        let manifest_exists: bool = sqlx::query_scalar(
            "select exists(select 1 from runtime_manifests \
             where session_id = $1 and worker_id = $2)",
        )
        .bind(session_id)
        .bind(worker_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        ensure!(
            manifest_exists,
            "runtime manifest for session {session_id} is not owned by worker {worker_id}"
        );
        let existing = sqlx::query(concat!(
            "select ",
            checkpoint_columns!(),
            " from runtime_checkpoints ",
            "where session_id = $1 and checkpoint_key = $2"
        ))
        .bind(session_id)
        .bind(key)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(row) = existing {
            let existing = decode_checkpoint(&row)?;
            ensure!(
                existing.content_hash == content_hash,
                "runtime checkpoint `{key}` already exists with different content"
            );
            transaction.commit().await?;
            return Ok(existing);
        }
        let revision = next_revision(&mut transaction, session_id).await?;
        let now = Utc::now();
        sqlx::query(
            "insert into runtime_checkpoints \
             (session_id, checkpoint_key, state_json, content_hash, revision, created_at) \
             values ($1, $2, $3, $4, $5, $6)",
        )
        .bind(session_id)
        .bind(key)
        .bind(&state_json)
        .bind(&content_hash)
        .bind(revision)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        sqlx::query("update runtime_manifests set updated_at = $1 where session_id = $2")
            .bind(now)
            .bind(session_id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(RuntimeCheckpoint {
            session_id,
            key: key.to_string(),
            state: state.clone(),
            content_hash,
            revision: u64::try_from(revision).context("negative runtime checkpoint revision")?,
            created_at: now,
        })
    }

    /// One checkpoint by key, or the newest non-harness one when `key` is None.
    pub(crate) async fn runtime_checkpoint(
        &self,
        session_id: Uuid,
        key: Option<&str>,
    ) -> Result<Option<RuntimeCheckpoint>> {
        // Harness state shares this table but is not a user-visible checkpoint,
        // so every unkeyed read excludes the reserved prefix.
        let row = match key {
            Some(key) => {
                sqlx::query(concat!(
                    "select ",
                    checkpoint_columns!(),
                    " from runtime_checkpoints ",
                    "where session_id = $1 and checkpoint_key = $2"
                ))
                .bind(session_id)
                .bind(key)
                .fetch_optional(self.pool())
                .await?
            }
            None => {
                sqlx::query(concat!(
                    "select ",
                    checkpoint_columns!(),
                    " from runtime_checkpoints ",
                    "where session_id = $1 and checkpoint_key not like $2 ",
                    "order by revision desc limit 1"
                ))
                .bind(session_id)
                .bind(format!("{HARNESS_CHECKPOINT_PREFIX}%"))
                .fetch_optional(self.pool())
                .await?
            }
        };
        row.as_ref().map(decode_checkpoint).transpose()
    }

    /// The most recent checkpoints for a session, newest first.
    pub(crate) async fn list_runtime_checkpoints(
        &self,
        session_id: Uuid,
        limit: usize,
    ) -> Result<Vec<RuntimeCheckpoint>> {
        let rows = sqlx::query(concat!(
            "select ",
            checkpoint_columns!(),
            " from runtime_checkpoints ",
            "where session_id = $1 and checkpoint_key not like $2 ",
            "order by revision desc limit $3"
        ))
        .bind(session_id)
        .bind(format!("{HARNESS_CHECKPOINT_PREFIX}%"))
        .bind(i64::try_from(limit.clamp(1, 100)).unwrap_or(100))
        .fetch_all(self.pool())
        .await?;
        rows.iter().map(decode_checkpoint).collect()
    }

    /// The newest harness state for this session.
    pub(crate) async fn load_harness_state(
        &self,
        session_id: Uuid,
    ) -> Result<Option<serde_json::Value>> {
        let state_json: Option<String> = sqlx::query_scalar(
            "select state_json from runtime_checkpoints \
             where session_id = $1 and checkpoint_key like $2 order by revision desc limit 1",
        )
        .bind(session_id)
        .bind(format!("{HARNESS_CHECKPOINT_PREFIX}%"))
        .fetch_optional(self.pool())
        .await?;
        state_json
            .map(|state_json| {
                let state: serde_json::Value = serde_json::from_str(&state_json)?;
                ensure!(
                    state.is_object(),
                    "stored harness state is not a JSON object"
                );
                Ok(state)
            })
            .transpose()
    }

    /// Append a new harness state revision, pruning all but the last twelve.
    ///
    /// Unlike a runtime checkpoint, harness state is a history rather than a
    /// keyed value: each save is a new revision, and `rollback_harness_state`
    /// walks back through them. The bound is what makes that history affordable
    /// to keep for every session forever.
    pub(crate) async fn save_harness_state(
        &self,
        session_id: Uuid,
        state: &serde_json::Value,
    ) -> Result<()> {
        let (state_json, content_hash) = encode_state(state, "harness state")?;
        let mut transaction = self.pool().begin().await?;
        lock_session(&mut transaction, session_id).await?;
        let revision = next_revision(&mut transaction, session_id).await?;
        let now = Utc::now();
        sqlx::query(
            "insert into runtime_checkpoints \
             (session_id, checkpoint_key, state_json, content_hash, revision, created_at) \
             values ($1, $2, $3, $4, $5, $6)",
        )
        .bind(session_id)
        .bind(format!("{HARNESS_CHECKPOINT_PREFIX}{revision}"))
        .bind(&state_json)
        .bind(&content_hash)
        .bind(revision)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "delete from runtime_checkpoints \
             where session_id = $1 and checkpoint_key like $2 and revision < $3",
        )
        .bind(session_id)
        .bind(format!("{HARNESS_CHECKPOINT_PREFIX}%"))
        .bind(revision.saturating_sub(12))
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Rewind harness state by `steps` revisions and re-save it as the newest.
    ///
    /// The rewound state is re-appended rather than merely uncovered, so the
    /// history stays append-only and a rollback is itself rollback-able.
    pub(crate) async fn rollback_harness_state(
        &self,
        session_id: Uuid,
        steps: usize,
    ) -> Result<serde_json::Value> {
        ensure!(
            (1..=12).contains(&steps),
            "harness rollback steps must be 1..=12"
        );
        let mut transaction = self.pool().begin().await?;
        // The target is selected, deleted-past and re-saved as one unit: a
        // concurrent save landing between those steps would otherwise be
        // erased by the delete without ever being rolled back to.
        lock_session(&mut transaction, session_id).await?;
        let row = sqlx::query(
            "select state_json, revision from runtime_checkpoints \
             where session_id = $1 and checkpoint_key like $2 \
             order by revision desc limit 1 offset $3",
        )
        .bind(session_id)
        .bind(format!("{HARNESS_CHECKPOINT_PREFIX}%"))
        .bind(i64::try_from(steps).context("harness rollback offset is out of range")?)
        .fetch_optional(&mut *transaction)
        .await?
        .context("harness rollback target does not exist")?;
        let state_json: String = row.try_get("state_json")?;
        let target_revision: i64 = row.try_get("revision")?;
        let state: serde_json::Value = serde_json::from_str(&state_json)?;
        ensure!(
            state.is_object(),
            "stored harness state is not a JSON object"
        );
        sqlx::query(
            "delete from runtime_checkpoints \
             where session_id = $1 and checkpoint_key like $2 and revision >= $3",
        )
        .bind(session_id)
        .bind(format!("{HARNESS_CHECKPOINT_PREFIX}%"))
        .bind(target_revision)
        .execute(&mut *transaction)
        .await?;
        let revision = next_revision(&mut transaction, session_id).await?;
        let content_hash = hash_state(state_json.as_bytes());
        sqlx::query(
            "insert into runtime_checkpoints \
             (session_id, checkpoint_key, state_json, content_hash, revision, created_at) \
             values ($1, $2, $3, $4, $5, $6)",
        )
        .bind(session_id)
        .bind(format!("{HARNESS_CHECKPOINT_PREFIX}{revision}"))
        .bind(&state_json)
        .bind(&content_hash)
        .bind(revision)
        .bind(Utc::now())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(state)
    }
}
