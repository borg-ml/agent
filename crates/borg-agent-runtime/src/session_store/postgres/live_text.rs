//! Append-only streamed text. Full text is reconstructed only on explicit
//! recovery/archive reads, never to commit or return a streaming append.
use crate::{SessionEvent, SessionEventKind};
use anyhow::{Result, ensure};
use sqlx::{Postgres, Row, Transaction};

/// Append a streamed fragment or snapshot. When `known_offset` is `Some`,
/// `session_live_text_heads` already exists and the caller holds the session
/// row lock, saving both the head upsert and the `for update` read.
pub async fn append_with_offset(
    tx: &mut Transaction<'_, Postgres>,
    event: &SessionEvent,
    key: &str,
    text: &str,
    fragment: bool,
    revision: i64,
    known_offset: Option<i64>,
) -> Result<()> {
    let offset = match known_offset {
        Some(offset) => offset,
        None => {
            sqlx::query(
                "insert into session_live_text_heads values($1,$2,0) on conflict do nothing",
            )
            .bind(event.session_id)
            .bind(key)
            .execute(&mut **tx)
            .await?;
            sqlx::query_scalar("select text_bytes from session_live_text_heads where session_id=$1 and live_key=$2 for update")
                .bind(event.session_id)
                .bind(key)
                .fetch_one(&mut **tx)
                .await?
        }
    };
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
        | SessionEventKind::ReasoningBlockSnapshot { text: value, .. }
        | SessionEventKind::ReasoningDelta { text: value } => *value = text,
        _ => ensure!(false, "text chunks on nontext live key"),
    }
    Ok(())
}

/// Look up existing live state and its text head in one query, seeding an old
/// inline live row once before its first new-format append. Returns the prior
/// event envelope and the current `text_bytes` offset when the head exists.
pub async fn seed_inline(
    tx: &mut Transaction<'_, Postgres>,
    event: &SessionEvent,
    key: &str,
) -> Result<(Option<SessionEvent>, Option<i64>)> {
    let row = sqlx::query(
        "select s.event_json, h.text_bytes          from session_live_state s          left join session_live_text_heads h            on h.session_id = s.session_id and h.live_key = s.live_key          where s.session_id = $1 and s.live_key = $2",
    )
    .bind(event.session_id)
    .bind(key)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(row) = row else {
        return Ok((None, None));
    };
    let prior_json: serde_json::Value = row.try_get("event_json")?;
    let prior: SessionEvent = serde_json::from_value(prior_json)?;
    let existing_bytes: Option<i64> = row.try_get("text_bytes")?;
    if let Some(offset) = existing_bytes {
        return Ok((Some(prior), Some(offset)));
    }
    match &prior.kind {
        SessionEventKind::Message { text, .. }
        | SessionEventKind::ReasoningBlockSnapshot { text, .. }
        | SessionEventKind::ReasoningDelta { text } => {
            let seeded_len = text.len() as i64;
            append_with_offset(tx, &prior, key, text, false, 0, None).await?;
            return Ok((Some(prior), Some(seeded_len)));
        }
        _ => {}
    }
    Ok((Some(prior), None))
}
