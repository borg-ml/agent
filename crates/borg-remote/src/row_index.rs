//! Normalized projection rows. Text chunks are immutable; no append rewrites
//! accumulated text. Operations participate in the checkpoint transaction.
use anyhow::{Result, ensure};
use sqlx::{Postgres, Transaction, Row};

pub async fn initialize(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::raw_sql("create table if not exists remote_view_rows(
        scope text not null,row_id text not null,source_sequence bigint not null,
        ordinal bigint not null,revision bigint not null,metadata bytea not null,text_bytes bigint not null default 0,
        primary key(scope,row_id));
        create index if not exists remote_view_rows_history on remote_view_rows(scope,source_sequence desc,ordinal desc,row_id);
        create table if not exists remote_view_text_chunks(
        scope text not null,row_id text not null,byte_offset bigint not null,body bytea not null,
        primary key(scope,row_id,byte_offset),foreign key(scope,row_id) references remote_view_rows(scope,row_id) on delete cascade);")
        .execute(pool).await?;
    Ok(())
}

pub enum RowMutation<'a> {
    /// Metadata excludes growing message/reasoning/tool text; text is separate.
    Upsert { id: &'a str, revision: i64, source_sequence: i64, ordinal: i64, metadata: &'a [u8] },
    Append { id: &'a str, expected_revision: i64, revision: i64, offset: i64, text: &'a str },
    Remove { id: &'a str },
}

pub async fn apply(tx: &mut Transaction<'_, Postgres>, scope: &str, mutation: RowMutation<'_>) -> Result<()> {
    match mutation {
        RowMutation::Upsert { id, revision, source_sequence, ordinal, metadata } => {
            ensure!(metadata.len() <= 65_536 && revision >= 0 && source_sequence >= 0 && ordinal >= 0, "invalid row metadata");
            sqlx::query("insert into remote_view_rows(scope,row_id,source_sequence,ordinal,revision,metadata) values($1,$2,$3,$4,$5,$6) on conflict(scope,row_id) do update set source_sequence=excluded.source_sequence,ordinal=excluded.ordinal,revision=excluded.revision,metadata=excluded.metadata")
                .bind(scope).bind(id).bind(source_sequence).bind(ordinal).bind(revision).bind(metadata).execute(&mut **tx).await?;
        }
        RowMutation::Append { id, expected_revision, revision, offset, text } => {
            ensure!(offset >= 0 && revision > expected_revision && !text.is_empty() && text.len() <= 1_048_576, "invalid row append");
            let length = offset.checked_add(text.len() as i64).ok_or_else(|| anyhow::anyhow!("text offset overflow"))?;
            let changed = sqlx::query("update remote_view_rows set revision=$3,text_bytes=$4 where scope=$1 and row_id=$2 and revision=$5 and text_bytes=$6")
                .bind(scope).bind(id).bind(revision).bind(length).bind(expected_revision).bind(offset).execute(&mut **tx).await?.rows_affected();
            ensure!(changed == 1, "row append revision/offset conflict");
            sqlx::query("insert into remote_view_text_chunks values($1,$2,$3,$4)")
                .bind(scope).bind(id).bind(offset).bind(text.as_bytes()).execute(&mut **tx).await?;
        }
        RowMutation::Remove { id } => {
            sqlx::query("delete from remote_view_rows where scope=$1 and row_id=$2").bind(scope).bind(id).execute(&mut **tx).await?;
        }
    }
    Ok(())
}

/// History navigation inspects only one indexed page, independent of journal size.
/// Text is fetched separately by bounded asset/preview readers.
pub async fn page(pool: &sqlx::PgPool, scope: &str, before: (i64, i64, &str), limit: usize) -> Result<Vec<(String,i64,i64,Vec<u8>)>> {
    ensure!((1..=75).contains(&limit), "row page limit exceeds budget");
    let rows = sqlx::query("select row_id,source_sequence,ordinal,metadata from remote_view_rows where scope=$1 and (source_sequence,ordinal,row_id)<($2,$3,$4) order by source_sequence desc,ordinal desc,row_id desc limit $5")
        .bind(scope).bind(before.0).bind(before.1).bind(before.2).bind(limit as i64).fetch_all(pool).await?;
    let mut bytes = 0;
    let mut result = Vec::new();
    for row in rows {
        let metadata: Vec<u8> = row.try_get("metadata")?;
        bytes += metadata.len();
        if bytes > 1_048_576 { break }
        result.push((row.try_get("row_id")?,row.try_get("source_sequence")?,row.try_get("ordinal")?,metadata));
    }
    Ok(result)
}

/// Read a UTF-8 byte range without assembling the entire row. Offsets must be
/// character boundaries. A 4KiB preview and paged assets use the same reader.
pub async fn text_page(pool: &sqlx::PgPool, scope: &str, id: &str, offset: i64, budget: usize) -> Result<String> {
    ensure!(offset >= 0 && (1..=65_536).contains(&budget), "invalid text page budget");
    let end = offset.checked_add(budget as i64).ok_or_else(|| anyhow::anyhow!("text offset overflow"))?;
    let rows = sqlx::query("select byte_offset,body from remote_view_text_chunks where scope=$1 and row_id=$2 and byte_offset<$4 and byte_offset+octet_length(body)>$3 order by byte_offset limit 1024")
        .bind(scope).bind(id).bind(offset).bind(end).fetch_all(pool).await?;
    let mut bytes = Vec::with_capacity(budget);
    for row in rows {
        let start: i64 = row.try_get("byte_offset")?;
        let body: Vec<u8> = row.try_get("body")?;
        let lo = (offset-start).max(0) as usize;
        let hi = body.len().min((end-start).max(0) as usize);
        bytes.extend_from_slice(&body[lo..hi]);
    }
    match std::str::from_utf8(&bytes) {
        Ok(text) => Ok(text.to_owned()),
        Err(error) if error.error_len().is_none() => Ok(std::str::from_utf8(&bytes[..error.valid_up_to()])?.to_owned()),
        Err(error) => Err(error.into()),
    }
}
