//! Payload bodies as content-addressed, compressed blobs.
//!
//! WHY THIS EXISTS: `session_payloads.payload` held raw bytea in TOAST. On
//! one 168 GB store, 16 GB of it was `provider_model_message` payloads: user
//! messages whose attachments are base64 images. Base64 is a third larger
//! than the image, pglz cannot compress it, and the same images recur across
//! messages, children and forks.
//!
//! A payload is now an ordered list of parts in `session_payload_parts`, each
//! naming a blob in `session_blobs` by the SHA-256 of its bytes, so a part
//! that recurs anywhere is stored once. Long canonical base64 runs become a
//! part holding the decoded bytes; everything else is zstd-compressed when
//! that saves space. Reassembly is byte-exact by construction: a run is split
//! out only when re-encoding its decoded bytes reproduces it exactly.
//!
//! Rows written by older releases keep their bytes in `payload` and no parts;
//! readers accept both, and `convert_legacy_payloads` rewrites them online.

use anyhow::{Context, Result, ensure};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::PostgresSessionStore;

/// Shorter base64 runs are not worth a part of their own.
const MIN_BASE64_RUN: usize = 4096;
const CODEC_STORED: i16 = 0;
const CODEC_ZSTD: i16 = 1;
/// Payloads are written inside the journal append, so the level favours speed.
const ZSTD_LEVEL: i32 = 3;

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Part {
    /// The blob's content: decoded bytes for a base64 part.
    pub bytes: Vec<u8>,
    pub base64: bool,
}

/// Split payload bytes into literal parts and canonical base64 runs.
pub(super) fn split_parts(bytes: &[u8]) -> Vec<Part> {
    fn is_base64(byte: u8) -> bool {
        byte.is_ascii_alphanumeric() || byte == b'+' || byte == b'/'
    }
    let mut parts = Vec::new();
    let mut literal_start = 0;
    let mut index = 0;
    while index < bytes.len() {
        if !is_base64(bytes[index]) {
            index += 1;
            continue;
        }
        let start = index;
        while index < bytes.len() && is_base64(bytes[index]) {
            index += 1;
        }
        let mut end = index;
        while end < bytes.len() && end - index < 2 && bytes[end] == b'=' {
            end += 1;
        }
        let run = &bytes[start..end];
        if run.len() >= MIN_BASE64_RUN
            && run.len().is_multiple_of(4)
            && let Ok(decoded) = STANDARD.decode(run)
            && STANDARD.encode(&decoded).as_bytes() == run
        {
            if literal_start < start {
                parts.push(Part {
                    bytes: bytes[literal_start..start].to_vec(),
                    base64: false,
                });
            }
            parts.push(Part {
                bytes: decoded,
                base64: true,
            });
            literal_start = end;
        }
        index = end;
    }
    if literal_start < bytes.len() || parts.is_empty() {
        parts.push(Part {
            bytes: bytes[literal_start..].to_vec(),
            base64: false,
        });
    }
    parts
}

/// The bytes a part contributes to the reassembled payload.
fn part_len(raw_len: usize, base64: bool) -> usize {
    if base64 {
        raw_len.div_ceil(3) * 4
    } else {
        raw_len
    }
}

/// Store `bytes` as the parts of payload `payload_id`, which must exist.
async fn write_parts(connection: &mut PgConnection, payload_id: Uuid, bytes: &[u8]) -> Result<()> {
    let parts = split_parts(bytes);
    let hashes: Vec<Vec<u8>> = parts
        .iter()
        .map(|part| Sha256::digest(&part.bytes).to_vec())
        .collect();
    // `for key share` keeps a blob that already exists from being collected
    // between this check and the part rows that reference it.
    let existing: Vec<Vec<u8>> = sqlx::query_scalar(
        "select hash from session_blobs where hash = any($1) order by hash for key share",
    )
    .bind(&hashes)
    .fetch_all(&mut *connection)
    .await?;
    let mut missing: Vec<(&Vec<u8>, &Part)> = hashes
        .iter()
        .zip(&parts)
        .filter(|(hash, _)| !existing.contains(hash))
        .collect();
    // A fixed insert order keeps two writers of the same new blobs from
    // deadlocking on each other's unique-index entries.
    missing.sort_by(|left, right| left.0.cmp(right.0));
    missing.dedup_by(|left, right| left.0 == right.0);
    for (hash, part) in missing {
        let compressed = if part.base64 {
            None
        } else {
            zstd::bulk::compress(&part.bytes, ZSTD_LEVEL)
                .ok()
                .filter(|compressed| compressed.len() + compressed.len() / 8 < part.bytes.len())
        };
        let (codec, body) = match &compressed {
            Some(compressed) => (CODEC_ZSTD, compressed.as_slice()),
            None => (CODEC_STORED, part.bytes.as_slice()),
        };
        sqlx::query(
            "insert into session_blobs (hash, codec, raw_len, body) values ($1, $2, $3, $4) \
             on conflict (hash) do nothing",
        )
        .bind(hash)
        .bind(codec)
        .bind(i64::try_from(part.bytes.len())?)
        .bind(body)
        .execute(&mut *connection)
        .await?;
    }
    let ordinals: Vec<i32> = (0..i32::try_from(parts.len())?).collect();
    let base64: Vec<bool> = parts.iter().map(|part| part.base64).collect();
    sqlx::query(
        "insert into session_payload_parts (payload_id, ordinal, blob_hash, base64) \
         select $1, * from unnest($2::integer[], $3::bytea[], $4::boolean[])",
    )
    .bind(payload_id)
    .bind(&ordinals)
    .bind(&hashes)
    .bind(&base64)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

/// Insert one payload. Idempotent on `id`: a retried write stores nothing new.
#[allow(clippy::too_many_arguments)]
pub async fn insert_payload(
    connection: &mut PgConnection,
    id: Uuid,
    session_id: Uuid,
    event_id: Uuid,
    kind: &str,
    bytes: &[u8],
    created_at: DateTime<Utc>,
) -> Result<()> {
    let inserted = sqlx::query(
        "insert into session_payloads \
         (id, session_id, event_id, payload_kind, payload, byte_len, created_at) \
         values ($1, $2, $3, $4, null, $5, $6) on conflict (id) do nothing",
    )
    .bind(id)
    .bind(session_id)
    .bind(event_id)
    .bind(kind)
    .bind(i64::try_from(bytes.len()).context("session payload exceeds a bigint")?)
    .bind(created_at)
    .execute(&mut *connection)
    .await?
    .rows_affected();
    if inserted == 1 {
        write_parts(connection, id, bytes).await?;
    }
    Ok(())
}

/// A byte range of one payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadSlice {
    pub kind: String,
    /// Length of the whole payload.
    pub byte_len: u64,
    pub bytes: Vec<u8>,
}

/// Read `len` bytes of payload `id` from `start`, or `None` if it does not
/// exist (or, with `session_id`, belongs to another session).
pub async fn read_payload_range(
    connection: &mut PgConnection,
    id: Uuid,
    session_id: Option<Uuid>,
    start: usize,
    len: usize,
) -> Result<Option<PayloadSlice>> {
    // `substring(bytea from int for int)` is the only bytea overload; the
    // clamps never bind in practice because payloads are far below 2 GiB.
    let row = sqlx::query(
        "select payload_kind, byte_len, payload is null as split, \
                substring(payload from $3 for $4) as body \
         from session_payloads where id = $1 and ($2::uuid is null or session_id = $2)",
    )
    .bind(id)
    .bind(session_id)
    .bind(i32::try_from(start.saturating_add(1)).unwrap_or(i32::MAX))
    .bind(i32::try_from(len).unwrap_or(i32::MAX))
    .fetch_optional(&mut *connection)
    .await?;
    let Some(row) = row else { return Ok(None) };
    let kind: String = row.try_get("payload_kind")?;
    let byte_len =
        u64::try_from(row.try_get::<i64, _>("byte_len")?).context("negative payload length")?;
    if !row.try_get::<bool, _>("split")? {
        return Ok(Some(PayloadSlice {
            kind,
            byte_len,
            bytes: row.try_get("body")?,
        }));
    }
    let extents = sqlx::query(
        "select p.ordinal, p.base64, b.raw_len from session_payload_parts p \
         join session_blobs b on b.hash = p.blob_hash where p.payload_id = $1 order by p.ordinal",
    )
    .bind(id)
    .fetch_all(&mut *connection)
    .await?;
    let end = start.saturating_add(len);
    let mut offset = 0usize;
    let mut wanted = Vec::new();
    for extent in &extents {
        let raw_len = usize::try_from(extent.try_get::<i64, _>("raw_len")?)?;
        let size = part_len(raw_len, extent.try_get("base64")?);
        if offset < end && offset + size > start {
            wanted.push((extent.try_get::<i32, _>("ordinal")?, offset));
        }
        offset += size;
    }
    ensure!(
        offset as u64 == byte_len,
        "session payload {id} parts do not add up to its length"
    );
    let ordinals: Vec<i32> = wanted.iter().map(|(ordinal, _)| *ordinal).collect();
    let rows = sqlx::query(
        "select p.ordinal, p.base64, b.codec, b.raw_len, b.body from session_payload_parts p \
         join session_blobs b on b.hash = p.blob_hash \
         where p.payload_id = $1 and p.ordinal = any($2) order by p.ordinal",
    )
    .bind(id)
    .bind(&ordinals)
    .fetch_all(&mut *connection)
    .await?;
    ensure!(
        rows.len() == wanted.len(),
        "session payload {id} lost a part"
    );
    let mut bytes = Vec::with_capacity(len.min(byte_len as usize));
    for (row, (_, part_start)) in rows.iter().zip(&wanted) {
        let raw_len = usize::try_from(row.try_get::<i64, _>("raw_len")?)?;
        let body: Vec<u8> = row.try_get("body")?;
        let raw = match row.try_get::<i16, _>("codec")? {
            CODEC_STORED => body,
            CODEC_ZSTD => zstd::bulk::decompress(&body, raw_len)
                .context("failed to decompress a session payload part")?,
            codec => anyhow::bail!("session payload {id} has a part with unknown codec {codec}"),
        };
        ensure!(
            raw.len() == raw_len,
            "session payload {id} part has the wrong length"
        );
        let content = if row.try_get("base64")? {
            STANDARD.encode(&raw).into_bytes()
        } else {
            raw
        };
        let from = start.saturating_sub(*part_start).min(content.len());
        let to = end.saturating_sub(*part_start).min(content.len());
        bytes.extend_from_slice(&content[from..to]);
    }
    Ok(Some(PayloadSlice {
        kind,
        byte_len,
        bytes,
    }))
}

/// What one conversion or collection pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct PayloadConversion {
    pub payloads_converted: u64,
    pub bytes_converted: u64,
    pub blobs_collected: u64,
}

impl PostgresSessionStore {
    /// Read a whole payload.
    pub async fn read_payload(&self, id: Uuid) -> Result<Option<Vec<u8>>> {
        let mut connection = self.pool().acquire().await?;
        Ok(read_payload_range(&mut connection, id, None, 0, usize::MAX)
            .await?
            .map(|slice| slice.bytes))
    }

    /// Rewrite up to `max_payloads` legacy raw payloads into blobs, one per
    /// short transaction, so a live store never waits on more than one row.
    pub async fn convert_legacy_payloads(
        &self,
        max_payloads: usize,
        mut progress: impl FnMut(&PayloadConversion),
    ) -> Result<PayloadConversion> {
        let mut outcome = PayloadConversion::default();
        let mut after = Uuid::nil();
        while (outcome.payloads_converted as usize) < max_payloads {
            let Some(id) = sqlx::query_scalar::<_, Uuid>(
                "select id from session_payloads where payload is not null and id > $1 \
                 order by id limit 1",
            )
            .bind(after)
            .fetch_optional(self.pool())
            .await?
            else {
                break;
            };
            after = id;
            let mut transaction = self.pool().begin().await?;
            let bytes: Option<Vec<u8>> = sqlx::query_scalar(
                "select payload from session_payloads where id = $1 and payload is not null \
                 for update skip locked",
            )
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?;
            let Some(bytes) = bytes else {
                transaction.rollback().await?;
                continue;
            };
            write_parts(&mut transaction, id, &bytes).await?;
            sqlx::query("update session_payloads set payload = null where id = $1")
                .bind(id)
                .execute(&mut *transaction)
                .await?;
            transaction.commit().await?;
            outcome.payloads_converted += 1;
            outcome.bytes_converted += bytes.len() as u64;
            progress(&outcome);
        }
        Ok(outcome)
    }

    /// Delete blobs no payload references any more.
    pub async fn collect_unreferenced_blobs(&self) -> Result<u64> {
        let mut collected = 0;
        loop {
            // A writer that references a blob after the subquery saw none
            // makes this delete fail its foreign key rather than dangle; the
            // next pass collects whatever that round left.
            let deleted = match sqlx::query(
                "delete from session_blobs where hash in ( \
                     select b.hash from session_blobs b where not exists ( \
                         select 1 from session_payload_parts p where p.blob_hash = b.hash) \
                     limit 500)",
            )
            .execute(self.pool())
            .await
            {
                Ok(result) => result.rows_affected(),
                Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("23503") => {
                    return Ok(collected);
                }
                Err(error) => return Err(error.into()),
            };
            collected += deleted;
            if deleted == 0 {
                return Ok(collected);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reassemble(parts: &[Part]) -> Vec<u8> {
        parts
            .iter()
            .flat_map(|part| {
                if part.base64 {
                    STANDARD.encode(&part.bytes).into_bytes()
                } else {
                    part.bytes.clone()
                }
            })
            .collect()
    }

    /// Reassembly must be byte-exact: native model messages are replayed
    /// verbatim to the provider, and a payload that comes back different
    /// breaks the request prefix it was cached under.
    #[test]
    fn split_payloads_reassemble_byte_exactly() {
        let image: Vec<u8> = (0..9000u32).map(|n| (n * 7 % 251) as u8).collect();
        let encoded = STANDARD.encode(&image);
        let message = format!(
            "{{\"role\":\"user\",\"attachments\":[{{\"data_base64\":\"{encoded}\"}},{{\"data_base64\":\"{encoded}\"}}]}}"
        );
        let parts = split_parts(message.as_bytes());
        assert_eq!(reassemble(&parts), message.as_bytes());
        let images: Vec<&Part> = parts.iter().filter(|part| part.base64).collect();
        assert_eq!(images.len(), 2);
        assert_eq!(images[0].bytes, image);

        // Unpadded or non-canonical runs stay literal rather than change.
        let unpadded = format!("x{}y", encoded.trim_end_matches('='));
        let parts = split_parts(unpadded.as_bytes());
        assert_eq!(reassemble(&parts), unpadded.as_bytes());
        for text in ["", "short", &"A".repeat(5000)] {
            assert_eq!(reassemble(&split_parts(text.as_bytes())), text.as_bytes());
        }
    }

    #[tokio::test]
    async fn payloads_dedupe_round_trip_and_convert_from_legacy_rows() {
        let (scratch, store) = super::super::testing::session_store().await;
        let session_id = Uuid::new_v4();
        sqlx::query(
            "insert into sessions (id, state_json, created_at, updated_at) values ($1, '{}', now(), now())",
        )
        .bind(session_id)
        .execute(store.pool())
        .await
        .unwrap();
        let image = STANDARD.encode((0..60_000u32).map(|n| (n % 253) as u8).collect::<Vec<_>>());
        let body = |text: &str| {
            format!("{{\"content\":\"{text}\",\"attachments\":[{{\"data_base64\":\"{image}\"}}]}}")
        };
        let first = body(&"first ".repeat(2000));
        let second = body("second");
        let mut connection = store.pool().acquire().await.unwrap();
        for (id, bytes) in [(Uuid::new_v4(), &first), (Uuid::new_v4(), &second)] {
            insert_payload(
                &mut connection,
                id,
                session_id,
                Uuid::new_v4(),
                "provider_model_message",
                bytes.as_bytes(),
                Utc::now(),
            )
            .await
            .unwrap();
            assert_eq!(
                store.read_payload(id).await.unwrap().unwrap(),
                bytes.as_bytes()
            );
            let window = read_payload_range(&mut connection, id, Some(session_id), 30, 20_000)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(window.bytes, bytes.as_bytes()[30..20_030]);
        }
        // One copy of the image, decoded, however many messages carry it.
        let image_blobs: i64 =
            sqlx::query_scalar("select count(*) from session_blobs where raw_len = 60000")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(image_blobs, 1);

        let legacy = Uuid::new_v4();
        sqlx::query(
            "insert into session_payloads (id, session_id, event_id, payload_kind, payload, byte_len, created_at) \
             values ($1, $2, $3, 'tool_output', $4, $5, now())",
        )
        .bind(legacy)
        .bind(session_id)
        .bind(Uuid::new_v4())
        .bind(first.as_bytes())
        .bind(first.len() as i64)
        .execute(store.pool())
        .await
        .unwrap();
        let converted = store.convert_legacy_payloads(10, |_| {}).await.unwrap();
        assert_eq!(converted.payloads_converted, 1);
        assert_eq!(
            store.read_payload(legacy).await.unwrap().unwrap(),
            first.as_bytes()
        );

        sqlx::query("delete from session_payloads")
            .execute(store.pool())
            .await
            .unwrap();
        assert!(store.collect_unreferenced_blobs().await.unwrap() >= 3);
        drop(connection);
        scratch.discard().await;
    }
}
