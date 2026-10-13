//! Normalized projection rows. Text chunks are immutable; no append rewrites
//! accumulated text. Operations participate in the checkpoint transaction.
use anyhow::{Result, ensure};
use sqlx::{Postgres, Transaction, Row};

pub async fn initialize(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::raw_sql("create table if not exists remote_view_rows(
        scope text not null,row_id text not null,source_sequence bigint not null,
        ordinal bigint not null,history_anchor text not null,revision bigint not null,metadata bytea not null,text_bytes bigint not null default 0,
        primary key(scope,row_id));
        create index if not exists remote_view_rows_history on remote_view_rows(scope,source_sequence desc,ordinal desc,row_id);
        create index if not exists remote_view_rows_anchor on remote_view_rows(scope,history_anchor);
        create table if not exists remote_view_text_chunks(
        scope text not null,row_id text not null,byte_offset bigint not null,body bytea not null,
        primary key(scope,row_id,byte_offset),foreign key(scope,row_id) references remote_view_rows(scope,row_id) on delete cascade);")
        .execute(pool).await?;
    Ok(())
}

pub enum RowMutation<'a> {
    /// Metadata excludes growing message/reasoning/tool text; text is separate.
    Upsert { id: &'a str, revision: i64, source_sequence: i64, ordinal: i64, anchor: &'a str, metadata: &'a [u8] },
    Append { id: &'a str, expected_revision: i64, revision: i64, offset: i64, text: &'a str },
    Advance { id: &'a str, expected_revision: i64, revision: i64, bytes: usize },
    Remove { id: &'a str },
}

pub async fn apply(tx: &mut Transaction<'_, Postgres>, scope: &str, mutation: RowMutation<'_>) -> Result<()> {
    match mutation {
        RowMutation::Upsert { id, revision, source_sequence, ordinal, anchor, metadata } => {
            ensure!(metadata.len() <= 65_536 && revision >= 0 && source_sequence >= 0 && ordinal >= 0, "invalid row metadata");
            sqlx::query("insert into remote_view_rows(scope,row_id,source_sequence,ordinal,history_anchor,revision,metadata) values($1,$2,$3,$4,$5,$6,$7) on conflict(scope,row_id) do update set revision=excluded.revision,metadata=excluded.metadata")
                .bind(scope).bind(id).bind(source_sequence).bind(ordinal).bind(anchor).bind(revision).bind(metadata).execute(&mut **tx).await?;
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
        RowMutation::Advance {id,expected_revision,revision,bytes}=>{
            let changed=sqlx::query("update remote_view_rows set revision=$3,metadata=convert_to(jsonb_set(jsonb_set(convert_from(metadata,'UTF8')::jsonb,'{revision}',to_jsonb($3::text)),'{text_state,bytes}',to_jsonb($4::text))::text,'UTF8') where scope=$1 and row_id=$2 and revision=$5")
                .bind(scope).bind(id).bind(revision).bind(bytes.to_string()).bind(expected_revision).execute(&mut **tx).await?.rows_affected();
            ensure!(changed==1,"append row revision conflict");
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

/// Field-addressed active text includes queue/held-tail references, which have
/// no visible row. This namespace is also the `view-text:` asset authority.
pub async fn initialize_text_fields(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::raw_sql("create table if not exists remote_view_text_heads(scope text not null,row_id text not null,field text not null,revision text not null,text_bytes bigint not null,primary key(scope,row_id,field));
        create table if not exists remote_view_field_chunks(scope text not null,row_id text not null,field text not null,byte_offset bigint not null,body bytea not null,primary key(scope,row_id,field,byte_offset),foreign key(scope,row_id,field) references remote_view_text_heads(scope,row_id,field) on delete cascade);")
        .execute(pool).await?;
    Ok(())
}

/// Persist only the newly appended suffix, or one explicit replacement at a
/// lifecycle correction. Revision labels refer to the reducer's row version.
pub async fn persist_text(tx: &mut Transaction<'_,Postgres>,scope:&str,id:&str,field:&str,revision:&str,text:&str)->Result<()> {
    let prior: Option<i64> = sqlx::query_scalar("select text_bytes from remote_view_text_heads where scope=$1 and row_id=$2 and field=$3 for update")
        .bind(scope).bind(id).bind(field).fetch_optional(&mut **tx).await?;
    let mut offset=prior.unwrap_or(0) as usize;
    if offset>text.len() {
        sqlx::query("delete from remote_view_field_chunks where scope=$1 and row_id=$2 and field=$3").bind(scope).bind(id).bind(field).execute(&mut **tx).await?;
        offset=0;
    }
    let delta=text.get(offset..).ok_or_else(||anyhow::anyhow!("text persistence offset splits UTF-8"))?;
    sqlx::query("insert into remote_view_text_heads values($1,$2,$3,$4,$5) on conflict(scope,row_id,field) do update set revision=excluded.revision,text_bytes=excluded.text_bytes")
        .bind(scope).bind(id).bind(field).bind(revision).bind(text.len() as i64).execute(&mut **tx).await?;
    if !delta.is_empty() {
        sqlx::query("insert into remote_view_field_chunks values($1,$2,$3,$4,$5)").bind(scope).bind(id).bind(field).bind(offset as i64).bind(delta.as_bytes()).execute(&mut **tx).await?;
    }
    Ok(())
}

pub async fn resolve_text(pool:&sqlx::PgPool,scope:&str,id:&str,field:&str,revision:&str,bytes:usize)->Result<String> {
    let mut tx=pool.begin().await?;
    sqlx::query("set transaction isolation level repeatable read read only").execute(&mut *tx).await?;
    let head=sqlx::query("select revision,text_bytes from remote_view_text_heads where scope=$1 and row_id=$2 and field=$3")
        .bind(scope).bind(id).bind(field).fetch_optional(&mut *tx).await?;
    if head.is_none() && bytes==0 {return Ok(String::new())}
    let head=head.ok_or_else(||anyhow::anyhow!("missing checkpoint text"))?;
    ensure!(head.try_get::<String,_>("revision")?==revision && head.try_get::<i64,_>("text_bytes")?==bytes as i64,"checkpoint text revision/length mismatch");
    let chunks:Vec<Vec<u8>>=sqlx::query_scalar("select body from remote_view_field_chunks where scope=$1 and row_id=$2 and field=$3 order by byte_offset")
        .bind(scope).bind(id).bind(field).fetch_all(&mut *tx).await?;
    let mut body=Vec::with_capacity(bytes);
    for chunk in chunks {body.extend(chunk);}
    ensure!(body.len()==bytes,"checkpoint text gap");
    tx.commit().await?;
    Ok(String::from_utf8(body)?)
}

/// Reads never fetch a whole accumulated chunk from PostgreSQL into the host.
pub async fn field_page(pool:&sqlx::PgPool,scope:&str,id:&str,field:&str,offset:usize,budget:usize)->Result<(Vec<u8>,usize)> {
    ensure!((1..=65_536).contains(&budget),"asset page budget exceeds limit");
    let mut tx=pool.begin().await?;
    sqlx::query("set transaction isolation level repeatable read read only").execute(&mut *tx).await?;
    let total:Option<i64>=sqlx::query_scalar("select text_bytes from remote_view_text_heads where scope=$1 and row_id=$2 and field=$3")
        .bind(scope).bind(id).bind(field).fetch_optional(&mut *tx).await?;
    let total=usize::try_from(total.ok_or_else(||anyhow::anyhow!("unknown view text asset"))?)?;
    ensure!(offset<=total,"asset offset outside body");
    let end=offset.checked_add(budget).ok_or_else(||anyhow::anyhow!("asset range exhausted"))?.min(total);
    let chunks:Vec<Vec<u8>>=sqlx::query_scalar("select substring(body from (greatest($4-byte_offset,0)+1)::integer for (least(byte_offset+octet_length(body),$5)-greatest($4,byte_offset))::integer) from remote_view_field_chunks where scope=$1 and row_id=$2 and field=$3 and byte_offset<$5 and byte_offset+octet_length(body)>$4 order by byte_offset limit 65536")
        .bind(scope).bind(id).bind(field).bind(i64::try_from(offset)?).bind(i64::try_from(end)?).fetch_all(&mut *tx).await?;
    let mut bytes=Vec::with_capacity(end-offset);
    for chunk in chunks {bytes.extend(chunk);}
    ensure!(bytes.len()==end-offset,"view asset chunk gap");
    tx.commit().await?;
    Ok((bytes,total))
}
