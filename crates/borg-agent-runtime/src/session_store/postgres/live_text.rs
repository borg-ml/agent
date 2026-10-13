//! Append-only streamed text. Full text is reconstructed only on explicit
//! recovery/archive reads, never to commit or return a streaming append.
use crate::{SessionEvent, SessionEventKind};
use anyhow::{Result, ensure};
use sqlx::{Postgres, Transaction};

pub async fn append(
    tx: &mut Transaction<'_, Postgres>,
    event: &SessionEvent,
    key: &str,
    text: &str,
    fragment: bool,
    revision: i64,
) -> Result<()> {
    sqlx::query("insert into session_live_text_heads values($1,$2,0) on conflict do nothing")
        .bind(event.session_id)
        .bind(key)
        .execute(&mut **tx)
        .await?;
    let offset: i64 = sqlx::query_scalar("select text_bytes from session_live_text_heads where session_id=$1 and live_key=$2 for update")
        .bind(event.session_id).bind(key).fetch_one(&mut **tx).await?;
    let delta = if fragment {
        text
    } else if text.len() < offset as usize {
        ""
    } else {
        text.get(offset as usize..)
            .ok_or_else(|| anyhow::anyhow!("live text offset splits UTF-8"))?
    };
    if delta.is_empty() {
        return Ok(());
    }
    let next = offset
        .checked_add(delta.len() as i64)
        .ok_or_else(|| anyhow::anyhow!("live text exhausted"))?;
    sqlx::query("insert into session_live_text_chunks values($1,$2,$3,$4,$5)")
        .bind(event.session_id)
        .bind(key)
        .bind(offset)
        .bind(delta.as_bytes())
        .bind(revision)
        .execute(&mut **tx)
        .await?;
    sqlx::query(
        "update session_live_text_heads set text_bytes=$3 where session_id=$1 and live_key=$2",
    )
    .bind(event.session_id)
    .bind(key)
    .bind(next)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub async fn materialize(
    tx: &mut Transaction<'_, Postgres>,
    event: &mut SessionEvent,
) -> Result<()> {
    let Some(key) = event.kind.live_state_key() else {
        return Ok(());
    };
    let bodies: Vec<Vec<u8>> = sqlx::query_scalar("select body from session_live_text_chunks where session_id=$1 and live_key=$2 order by byte_offset")
        .bind(event.session_id).bind(&key).fetch_all(&mut **tx).await?;
    if bodies.is_empty() {
        return Ok(());
    } // Old journals retain inline text.
    let mut bytes = Vec::new();
    for body in bodies {
        bytes.extend(body);
    }
    let text = String::from_utf8(bytes)?;
    match &mut event.kind {
        SessionEventKind::Message { text: value, .. }
        | SessionEventKind::ReasoningDelta { text: value } => *value = text,
        _ => ensure!(false, "text chunks on nontext live key"),
    }
    Ok(())
}

/// Seed an old inline live row once before its first new-format append.
pub async fn seed_inline(
    tx: &mut Transaction<'_, Postgres>,
    event: &SessionEvent,
    key: &str,
) -> Result<Option<SessionEvent>> {
    let prior: Option<serde_json::Value> = sqlx::query_scalar(
        "select event_json from session_live_state where session_id=$1 and live_key=$2",
    )
    .bind(event.session_id)
    .bind(key)
    .fetch_optional(&mut **tx)
    .await?;
    let prior = prior
        .map(serde_json::from_value::<SessionEvent>)
        .transpose()?;
    let exists: bool = sqlx::query_scalar(
        "select exists(select 1 from session_live_text_heads where session_id=$1 and live_key=$2)",
    )
    .bind(event.session_id)
    .bind(key)
    .fetch_one(&mut **tx)
    .await?;
    if !exists && let Some(prior) = &prior {
        match &prior.kind {
            SessionEventKind::Message { text, .. } | SessionEventKind::ReasoningDelta { text } => {
                append(tx, prior, key, text, false, 0).await?
            }
            _ => {}
        }
    }
    Ok(prior)
}
