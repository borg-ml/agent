//! Archiving idle sessions out of the hot tables, and restoring them.
//!
//! An archived session keeps its `sessions` row, so it is still listed with
//! its title, status and lineage. Its events, payloads and actions move to a
//! zstd-compressed file named by the SHA-256 of its bytes, and `archive_ref`
//! names that file. Payload parts are written as content-addressed blob files
//! beside it, so an image shared by many sessions is stored once.
//!
//! Nothing is deleted until the file has been written, synced, read back and
//! counted, and the deletion commits in one transaction that first checks the
//! session has not changed since it was exported. Any read through
//! `session_row` restores an archived session before answering, so
//! resume, replay, inspection and search work unchanged, only slower once.
//! Archive files are never deleted by Borg.

use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::Row;
use uuid::Uuid;

use super::PostgresSessionStore;
use super::payloads::split_parts;

pub const DEFAULT_ARCHIVE_AFTER_DAYS: i64 = 30;
const FORMAT: &str = "borg-session-archive";
const VERSION: u64 = 1;
const ZSTD_LEVEL: i32 = 9;
/// Rows per insert while restoring.
const RESTORE_BATCH: usize = 500;
/// Per-session tables archived and restored as whole rows, in restore order.
const ROW_TABLES: [&str; 3] = [
    "session_events",
    "session_actions",
    "session_action_transitions",
];

/// Whether `sessions.archive_ref` exists. A store whose migration could not
/// get its locks (see schema.rs) runs without it, and reads must not name it
/// until it exists. Present stays present; absent is rechecked each minute.
#[derive(Debug, Default)]
pub struct ColumnCache {
    present: std::sync::atomic::AtomicBool,
    absent_since: std::sync::Mutex<Option<std::time::Instant>>,
}

impl ColumnCache {
    /// A migration just ran: look again on next use.
    pub(super) fn forget_absent(&self) {
        if let Ok(mut absent) = self.absent_since.lock() {
            *absent = None;
        }
    }
}

impl PostgresSessionStore {
    pub(super) async fn archive_column(&self) -> Result<bool> {
        use std::sync::atomic::Ordering;
        let cache = &self.archive_column;
        if cache.present.load(Ordering::Acquire) {
            return Ok(true);
        }
        if let Ok(absent) = cache.absent_since.lock()
            && absent.is_some_and(|since| since.elapsed() < std::time::Duration::from_secs(60))
        {
            return Ok(false);
        }
        let present: bool = sqlx::query_scalar(
            "select exists (select 1 from pg_attribute where attrelid = to_regclass('sessions') \
               and attname = 'archive_ref' and not attisdropped)",
        )
        .fetch_one(self.pool())
        .await?;
        if present {
            cache.present.store(true, Ordering::Release);
        } else if let Ok(mut absent) = cache.absent_since.lock() {
            *absent = Some(std::time::Instant::now());
        }
        Ok(present)
    }

    async fn require_archive_column(&self) -> Result<()> {
        ensure!(
            self.archive_column().await?,
            "the session schema has not finished migrating (sessions.archive_ref is missing); \
             it completes on its own once no long reader such as pg_dump holds its locks"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ArchivedSession {
    pub session_id: Uuid,
    pub path: PathBuf,
    pub file_bytes: u64,
    pub events: u64,
    pub payloads: u64,
    pub payload_bytes: u64,
}

/// Sessions eligible for archiving, oldest first: untouched since `cutoff`,
/// not running, and with no descendant -- owned child or fork, at any depth
/// -- that is running or was touched since `cutoff`. So an active session
/// and its ancestors stay hot. A director does not read an archived child
/// unless its mirror of that child is behind, which restores it then.
pub async fn archive_candidates(
    store: &PostgresSessionStore,
    cutoff: DateTime<Utc>,
    limit: usize,
) -> Result<Vec<Uuid>> {
    store.require_archive_column().await?;
    Ok(sqlx::query_scalar(
        "with recursive family(root, id) as ( \
             select id, id from sessions \
             union \
             select f.root, s.id from sessions s join family f \
               on s.owner_session_id = f.id or s.parent_session_id = f.id), \
         busy as ( \
             select distinct f.root from family f join sessions d on d.id = f.id \
             where d.updated_at >= $1 \
                or coalesce(d.state_json::jsonb ->> 'status', '') \
                   in ('running', 'waiting_for_approval')) \
         select s.id from sessions s \
         where s.archive_ref is null and s.updated_at < $1 \
           and s.id not in (select root from busy) \
           and exists (select 1 from session_events e where e.session_id = s.id) \
         order by s.updated_at limit $2",
    )
    .bind(cutoff)
    .bind(i64::try_from(limit).unwrap_or(i64::MAX))
    .fetch_all(store.pool())
    .await?)
}

/// What must not change between export and deletion.
#[derive(Debug, PartialEq, Eq)]
struct Fingerprint {
    next_sequence: i64,
    updated_at: DateTime<Utc>,
    payloads: i64,
    actions: i64,
    actions_updated: Option<DateTime<Utc>>,
}

async fn fingerprint(
    connection: &mut sqlx::PgConnection,
    session_id: Uuid,
    lock: bool,
) -> Result<Option<Fingerprint>> {
    let session = sqlx::query(if lock {
        "select next_sequence, updated_at from sessions \
         where id = $1 and archive_ref is null for update nowait"
    } else {
        "select next_sequence, updated_at from sessions where id = $1 and archive_ref is null"
    })
    .bind(session_id)
    .fetch_optional(&mut *connection)
    .await?;
    let Some(session) = session else {
        return Ok(None);
    };
    let counts = sqlx::query(
        "select (select count(*) from session_payloads where session_id = $1) as payloads, \
                (select count(*) from session_actions where session_id = $1) as actions, \
                (select max(updated_at) from session_actions where session_id = $1) as actions_updated",
    )
    .bind(session_id)
    .fetch_one(&mut *connection)
    .await?;
    Ok(Some(Fingerprint {
        next_sequence: session.try_get("next_sequence")?,
        updated_at: session.try_get("updated_at")?,
        payloads: counts.try_get("payloads")?,
        actions: counts.try_get("actions")?,
        actions_updated: counts.try_get("actions_updated")?,
    }))
}

fn blob_path(dir: &Path, hash: &[u8]) -> PathBuf {
    let hex = hex::encode(hash);
    dir.join("blobs").join(&hex[..2]).join(hex)
}

/// Write a content-addressed blob file unless it already exists. The first
/// byte says whether the rest is the bytes (0) or their zstd frame (1).
fn write_blob(dir: &Path, hash: &[u8], bytes: &[u8], compress: bool) -> Result<()> {
    let path = blob_path(dir, hash);
    if path.exists() {
        return Ok(());
    }
    std::fs::create_dir_all(path.parent().expect("blob path has a parent"))?;
    let compressed = compress
        .then(|| zstd::bulk::compress(bytes, ZSTD_LEVEL).ok())
        .flatten()
        .filter(|compressed| compressed.len() < bytes.len());
    let temporary = path.with_extension(format!("tmp-{}", Uuid::new_v4()));
    let mut file = std::fs::File::create(&temporary)?;
    match &compressed {
        Some(compressed) => {
            file.write_all(&[1])?;
            file.write_all(compressed)?;
        }
        None => {
            file.write_all(&[0])?;
            file.write_all(bytes)?;
        }
    }
    file.sync_all()?;
    std::fs::rename(&temporary, &path)?;
    Ok(())
}

fn read_blob(dir: &Path, hash: &[u8]) -> Result<Vec<u8>> {
    let path = blob_path(dir, hash);
    let stored = std::fs::read(&path)
        .with_context(|| format!("archive blob {} is missing", path.display()))?;
    let bytes = match stored.split_first() {
        Some((0, bytes)) => bytes.to_vec(),
        Some((1, frame)) => zstd::stream::decode_all(frame)?,
        _ => bail!("archive blob {} is corrupt", path.display()),
    };
    ensure!(
        Sha256::digest(&bytes).as_slice() == hash,
        "archive blob {} does not match its name",
        path.display()
    );
    Ok(bytes)
}

/// Counts written to the end record, checked on read-back and restore.
#[derive(Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Counts {
    session_events: u64,
    session_actions: u64,
    session_action_transitions: u64,
    payloads: u64,
}

impl Counts {
    fn add(&mut self, table: &str) -> Result<()> {
        match table {
            "session_events" => self.session_events += 1,
            "session_actions" => self.session_actions += 1,
            "session_action_transitions" => self.session_action_transitions += 1,
            "session_payloads" => self.payloads += 1,
            other => bail!("unknown archive table {other}"),
        }
        Ok(())
    }
}

/// Reads through to `inner` while hashing everything read.
struct HashingReader<R> {
    inner: R,
    hash: Sha256,
}

impl<R: std::io::Read> std::io::Read for HashingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.hash.update(&buffer[..read]);
        Ok(read)
    }
}

fn file_sha256(path: &Path) -> Result<String> {
    let mut reader = HashingReader {
        inner: std::fs::File::open(path)?,
        hash: Sha256::new(),
    };
    std::io::copy(&mut reader, &mut std::io::sink())?;
    Ok(hex::encode(reader.hash.finalize()))
}

/// One record handed to restore: whole rows of a table, or one payload with
/// its reassembled bytes.
enum Restored {
    Rows(String, Vec<Value>),
    Payload(Value, Vec<u8>),
}

/// Decompress and parse an archive, checking its name, header, counts and
/// hash. Calls `record` for every row record in file order; the hash is only
/// known at the end, so callers must not commit before this returns `Ok`.
fn read_archive(
    path: &Path,
    session_id: Uuid,
    mut record: impl FnMut(&str, Value) -> Result<()>,
) -> Result<()> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_suffix(".jsonl.zst"))
        .context("archive file is not named by its hash")?
        .to_string();
    let file = std::fs::File::open(path)
        .with_context(|| format!("archive {} is missing", path.display()))?;
    let mut hashing = HashingReader {
        inner: std::io::BufReader::new(file),
        hash: Sha256::new(),
    };
    {
        let decoder = zstd::stream::Decoder::with_buffer(std::io::BufReader::new(&mut hashing))?;
        let mut lines = std::io::BufReader::new(decoder).lines();
        let header: Value = serde_json::from_str(&lines.next().context("archive is empty")??)?;
        ensure!(
            header["format"] == FORMAT && header["version"] == VERSION,
            "unsupported archive format in {}",
            path.display()
        );
        ensure!(
            header["session_id"] == session_id.to_string(),
            "archive {} belongs to another session",
            path.display()
        );
        let mut counts = Counts::default();
        let mut ended = false;
        for line in lines {
            let mut value: Value = serde_json::from_str(&line?)?;
            let table = value["table"].as_str().unwrap_or_default().to_string();
            if table == "end" {
                let expected: Counts = serde_json::from_value(value["counts"].take())?;
                ensure!(
                    expected == counts,
                    "archive {} is truncated",
                    path.display()
                );
                ended = true;
                continue;
            }
            ensure!(
                !ended,
                "archive {} has records after its end",
                path.display()
            );
            counts.add(&table)?;
            record(&table, value)?;
        }
        ensure!(ended, "archive {} has no end record", path.display());
    }
    std::io::copy(&mut hashing, &mut std::io::sink())?;
    ensure!(
        hex::encode(hashing.hash.finalize()) == name,
        "archive {} does not match its hash",
        path.display()
    );
    Ok(())
}

impl PostgresSessionStore {
    /// Export one session to `dir` and remove it from the hot tables.
    /// Returns `None` when it stopped being eligible or is in use.
    pub async fn archive_session(
        &self,
        session_id: Uuid,
        dir: &Path,
    ) -> Result<Option<ArchivedSession>> {
        self.require_archive_column().await?;
        let sessions_dir = dir.join("sessions");
        std::fs::create_dir_all(&sessions_dir)?;
        let mut snapshot = self.pool().begin().await?;
        sqlx::query("set transaction isolation level repeatable read read only")
            .execute(&mut *snapshot)
            .await?;
        let Some(before) = fingerprint(&mut snapshot, session_id, false).await? else {
            return Ok(None);
        };
        let temporary = sessions_dir.join(format!(".{session_id}-{}.tmp", Uuid::new_v4()));
        let mut encoder =
            zstd::stream::Encoder::new(std::fs::File::create(&temporary)?, ZSTD_LEVEL)?;
        let mut write = |value: Value| -> Result<()> {
            serde_json::to_writer(&mut encoder, &value)?;
            encoder.write_all(b"\n")?;
            Ok(())
        };
        write(json!({
            "format": FORMAT,
            "version": VERSION,
            "session_id": session_id,
            "archived_at": Utc::now(),
        }))?;
        let mut counts = Counts::default();
        for table in ROW_TABLES {
            let order = if table == "session_events" {
                "sequence"
            } else {
                "created_at"
            };
            // Whole rows, columns as Postgres renders them in jsonb: restore
            // reads them back with jsonb_populate_recordset, bytes for bytes.
            let mut rows = sqlx::query_scalar::<_, Value>(sqlx::AssertSqlSafe(format!(
                "select to_jsonb(t) from {table} t where session_id = $1 order by {order}"
            )))
            .bind(session_id)
            .fetch(&mut *snapshot);
            while let Some(row) = rows.try_next().await? {
                write(json!({"table": table, "row": row}))?;
                counts.add(table)?;
            }
        }
        let payloads = sqlx::query(
            "select id, event_id, payload_kind, byte_len, created_at from session_payloads \
             where session_id = $1 order by created_at, id",
        )
        .bind(session_id)
        .fetch_all(&mut *snapshot)
        .await?;
        let mut payload_bytes = 0u64;
        for payload in &payloads {
            let id: Uuid = payload.try_get("id")?;
            let bytes = super::payloads::read_payload_range(
                &mut snapshot,
                id,
                Some(session_id),
                0,
                usize::MAX,
            )
            .await?
            .with_context(|| format!("session payload {id} vanished during archiving"))?
            .bytes;
            payload_bytes += bytes.len() as u64;
            let mut parts = Vec::new();
            for part in split_parts(&bytes) {
                let hash = Sha256::digest(&part.bytes).to_vec();
                write_blob(dir, &hash, &part.bytes, !part.base64)?;
                parts.push(json!({"hash": hex::encode(&hash), "base64": part.base64}));
            }
            write(json!({
                "table": "session_payloads",
                "row": {
                    "id": id,
                    "event_id": payload.try_get::<Uuid, _>("event_id")?,
                    "payload_kind": payload.try_get::<String, _>("payload_kind")?,
                    "byte_len": payload.try_get::<i64, _>("byte_len")?,
                    "created_at": payload.try_get::<DateTime<Utc>, _>("created_at")?,
                },
                "parts": parts,
            }))?;
            counts.add("session_payloads")?;
        }
        write(json!({"table": "end", "counts": counts}))?;
        snapshot.commit().await?;
        encoder.finish()?.sync_all()?;
        let file_bytes = std::fs::metadata(&temporary)?.len();
        let path = sessions_dir.join(format!("{}.jsonl.zst", file_sha256(&temporary)?));
        std::fs::rename(&temporary, &path)?;
        std::fs::File::open(&sessions_dir)?.sync_all()?;

        // Read back everything that is about to be deleted.
        let mut verified = 0u64;
        read_archive(&path, session_id, |table, value| {
            if table == "session_payloads" {
                for part in value["parts"].as_array().context("payload parts")? {
                    let hash = hex::decode(part["hash"].as_str().context("part hash")?)?;
                    read_blob(dir, &hash)?;
                }
            }
            verified += 1;
            Ok(())
        })?;
        ensure!(
            verified
                == counts.session_events
                    + counts.session_actions
                    + counts.session_action_transitions
                    + counts.payloads,
            "archive read-back count mismatch"
        );

        let mut transaction = self.pool().begin().await?;
        let current = match fingerprint(&mut transaction, session_id, true).await {
            Ok(current) => current,
            // Locked by a writer right now: it is not idle after all.
            Err(error)
                if error.chain().any(|cause| {
                    cause
                        .downcast_ref::<sqlx::Error>()
                        .and_then(sqlx::Error::as_database_error)
                        .is_some_and(|database| database.code().as_deref() == Some("55P03"))
                }) =>
            {
                None
            }
            Err(error) => return Err(error),
        };
        if current.as_ref() != Some(&before) {
            transaction.rollback().await?;
            return Ok(None);
        }
        sqlx::query("update sessions set archive_ref = $2 where id = $1")
            .bind(session_id)
            .bind(path.to_string_lossy().as_ref())
            .execute(&mut *transaction)
            .await?;
        for table in [
            "session_event_search",
            "session_payloads",
            "session_action_transitions",
            "session_actions",
            "session_events",
        ] {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "delete from {table} where session_id = $1"
            )))
            .bind(session_id)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(Some(ArchivedSession {
            session_id,
            file_bytes,
            path,
            events: counts.session_events,
            payloads: counts.payloads,
            payload_bytes,
        }))
    }

    /// Bring an archived session back into the hot tables. Returns whether
    /// anything was restored; a session that is not archived is a no-op.
    pub async fn restore_archived_session(&self, session_id: Uuid) -> Result<bool> {
        if !self.archive_column().await? {
            return Ok(false);
        }
        let mut transaction = self.pool().begin().await?;
        let archive_ref: Option<String> =
            sqlx::query_scalar("select archive_ref from sessions where id = $1 for update")
                .bind(session_id)
                .fetch_optional(&mut *transaction)
                .await?
                .flatten();
        let Some(archive_ref) = archive_ref else {
            transaction.rollback().await?;
            return Ok(false);
        };
        let path = PathBuf::from(&archive_ref);
        let dir = path
            .parent()
            .and_then(Path::parent)
            .context("archive path has no archive directory")?
            .to_path_buf();
        // Parse on a blocking thread and stream batches back, so a large
        // archive never sits whole in memory or on an async worker.
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<Result<Restored>>(4);
        let reader = tokio::task::spawn_blocking(move || {
            let send = |item: Result<Restored>| {
                sender
                    .blocking_send(item)
                    .map_err(|_| anyhow::anyhow!("restore was abandoned"))
            };
            let mut batch: Vec<Value> = Vec::new();
            let mut batch_table = String::new();
            let result = read_archive(&path, session_id, |table, mut value| {
                if (table != batch_table || batch.len() >= RESTORE_BATCH) && !batch.is_empty() {
                    send(Ok(Restored::Rows(
                        batch_table.clone(),
                        std::mem::take(&mut batch),
                    )))?;
                }
                batch_table = table.to_string();
                if table != "session_payloads" {
                    batch.push(value["row"].take());
                    return Ok(());
                }
                let mut bytes = Vec::new();
                for part in value["parts"].as_array().context("payload parts")? {
                    let hash = hex::decode(part["hash"].as_str().context("part hash")?)?;
                    let raw = read_blob(&dir, &hash)?;
                    if part["base64"].as_bool() == Some(true) {
                        use base64::Engine;
                        bytes.extend(
                            base64::engine::general_purpose::STANDARD
                                .encode(raw)
                                .into_bytes(),
                        );
                    } else {
                        bytes.extend(raw);
                    }
                }
                send(Ok(Restored::Payload(value["row"].take(), bytes)))
            });
            let result = result.and_then(|()| {
                if batch.is_empty() {
                    Ok(())
                } else {
                    send(Ok(Restored::Rows(batch_table, batch)))
                }
            });
            if let Err(error) = result {
                let _ = sender.blocking_send(Err(error));
            }
        });
        while let Some(item) = receiver.recv().await {
            match item? {
                Restored::Payload(row, bytes) => {
                    ensure!(
                        row["byte_len"].as_u64() == Some(bytes.len() as u64),
                        "archived payload has the wrong length"
                    );
                    super::payloads::insert_payload(
                        &mut transaction,
                        serde_json::from_value(row["id"].clone())?,
                        session_id,
                        serde_json::from_value(row["event_id"].clone())?,
                        row["payload_kind"].as_str().context("payload kind")?,
                        &bytes,
                        serde_json::from_value(row["created_at"].clone())?,
                    )
                    .await?;
                }
                Restored::Rows(table, rows) => {
                    ensure!(
                        ROW_TABLES.contains(&table.as_str()),
                        "unknown archive table {table}"
                    );
                    sqlx::query(sqlx::AssertSqlSafe(format!(
                        "insert into {table} select * from jsonb_populate_recordset(null::{table}, $1) \
                         on conflict do nothing"
                    )))
                    .bind(Value::Array(rows))
                    .execute(&mut *transaction)
                    .await?;
                }
            }
        }
        reader.await?;
        sqlx::query("update sessions set archive_ref = null where id = $1")
            .bind(session_id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        tracing::info!(%session_id, archive = %archive_ref, "restored an archived session");
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_store::SessionStore;
    use crate::{EventActor, MessageStatus, SessionEvent, SessionEventKind};

    /// Archiving deletes the hot rows, so what comes back must be exactly
    /// what went out: every event, and deferred payloads byte for byte.
    #[tokio::test]
    async fn an_archived_session_stays_listed_and_restores_exactly_on_read() {
        let (scratch, store) = super::super::testing::session_store().await;
        let archive = tempfile::tempdir().unwrap();
        let session_id = Uuid::new_v4();
        store.create_session(session_id).await.unwrap();
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
            .unwrap();
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: EventActor::User,
                    text: "archive me".into(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ))
            .await
            .unwrap();
        let output = "tool output line\n".repeat(10_000);
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ToolCompleted {
                    tool_call_id: "call".into(),
                    output: output.clone(),
                    output_ref: None,
                    is_error: false,
                    input: None,
                    input_ref: None,
                    parent_tool_call_id: None,
                },
            ))
            .await
            .unwrap();
        let before = store.events_after(session_id, 0, 100).await.unwrap();

        // Not idle yet: a session touched after the cutoff stays hot.
        let past = Utc::now() - chrono::Duration::days(1);
        assert!(
            archive_candidates(&store, past, 10)
                .await
                .unwrap()
                .is_empty()
        );
        let future = Utc::now() + chrono::Duration::days(1);
        assert_eq!(
            archive_candidates(&store, future, 10).await.unwrap(),
            vec![session_id]
        );
        let archived = store
            .archive_session(session_id, archive.path())
            .await
            .unwrap()
            .expect("eligible session is archived");
        assert_eq!(archived.events, before.len() as u64);
        assert_eq!(archived.payloads, 1);
        let hot: i64 =
            sqlx::query_scalar("select count(*) from session_events where session_id = $1")
                .bind(session_id)
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(hot, 0);
        assert!(
            store
                .list_sessions(10)
                .await
                .unwrap()
                .iter()
                .any(|summary| summary.session_id == session_id),
            "an archived session is still listed"
        );

        let after = store.events_after(session_id, 0, 100).await.unwrap();
        assert_eq!(
            serde_json::to_value(&after).unwrap(),
            serde_json::to_value(&before).unwrap()
        );
        let SessionEventKind::ToolCompleted {
            output_ref: Some(reference),
            ..
        } = &after.last().unwrap().kind
        else {
            panic!("the tool output was deferred to a payload");
        };
        assert_eq!(
            store.load_payload(reference).await.unwrap(),
            output.as_bytes()
        );
        assert!(!store.restore_archived_session(session_id).await.unwrap());

        // An active session's ancestors stay hot, however old they are.
        let child = Uuid::new_v4();
        store.create_session(child).await.unwrap();
        store
            .append(SessionEvent::new(
                child,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
            .unwrap();
        let set_owner_and_age = |id: Uuid, owner: Option<Uuid>, days: i64| {
            sqlx::query(
                "update sessions set owner_session_id = coalesce($2, owner_session_id), \
                 updated_at = now() - make_interval(days => $3) where id = $1",
            )
            .bind(id)
            .bind(owner)
            .bind(i32::try_from(days).unwrap())
            .execute(store.pool())
        };
        set_owner_and_age(session_id, None, 40).await.unwrap();
        set_owner_and_age(child, Some(session_id), 0).await.unwrap();
        let cutoff = Utc::now() - chrono::Duration::days(30);
        assert!(
            archive_candidates(&store, cutoff, 10)
                .await
                .unwrap()
                .is_empty()
        );
        set_owner_and_age(child, None, 40).await.unwrap();
        let mut candidates = archive_candidates(&store, cutoff, 10).await.unwrap();
        candidates.sort();
        let mut expected = vec![session_id, child];
        expected.sort();
        assert_eq!(candidates, expected);
        scratch.discard().await;
    }
}
