//! Commit-coupled wakeups, not event transport. A reconnect always tails durable
//! watermarks: NOTIFY is deliberately only a hint and carries no private data.
use anyhow::Result;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

pub const CHANNEL: &str = "borg_session_committed";

pub async fn notify(tx: &mut Transaction<'_, Postgres>, session_id: Uuid) -> Result<()> {
    sqlx::query("select pg_notify($1,$2)")
        .bind(CHANNEL)
        .bind(session_id.to_string())
        .execute(&mut **tx)
        .await?;
    Ok(())
}

impl super::PostgresSessionStore {
    /// LISTEN completes before the caller reads checkpoints/tails. Notifications
    /// received during tail construction remain queued on this connection.
    pub async fn subscribe_committed(&self) -> Result<sqlx::postgres::PgListener> {
        let mut listener = sqlx::postgres::PgListener::connect_with(self.pool()).await?;
        listener.listen(CHANNEL).await?;
        Ok(listener)
    }
}

/// Narrow capability to the exact journal database selected at startup.
#[derive(Clone)]
pub struct PostgresAuthority {
    pub pool: sqlx::PgPool,
}
impl PostgresAuthority {
    pub async fn subscribe_committed(&self) -> Result<sqlx::postgres::PgListener> {
        let mut listener = sqlx::postgres::PgListener::connect_with(&self.pool).await?;
        listener.listen(CHANNEL).await?;
        Ok(listener)
    }
}

impl PostgresAuthority {
    /// Bounded committed delta page; revision-zero seeding is old inline state,
    /// not a new delta. Terminal rows are recovered from the canonical journal.
    pub async fn text_deltas_after(
        &self,
        session: Uuid,
        after: u64,
    ) -> Result<Vec<(u64, crate::SessionEvent)>> {
        use sqlx::Row;
        let rows=sqlx::query("select revision,body,event_json from (select c.revision,c.body,s.event_json from session_live_text_chunks c join session_live_state s using(session_id,live_key) where c.session_id=$1 and c.revision>$2 union all select s.revision,null::bytea,s.event_json from session_live_state s where s.session_id=$1 and s.revision>$2 and s.live_key not like 'message:%' and s.live_key<>'reasoning') live order by revision limit 128")
            .bind(session).bind(i64::try_from(after)?).fetch_all(&self.pool).await?;
        let mut result = Vec::with_capacity(rows.len());
        let mut bytes = 0;
        for row in rows {
            let body: Option<Vec<u8>> = row.try_get("body")?;
            let mut event: crate::SessionEvent =
                serde_json::from_value(row.try_get("event_json")?)?;
            bytes += body.as_ref().map_or(0, Vec::len) + serde_json::to_vec(&event)?.len();
            if bytes > 1_048_576 {
                break;
            }
            if let Some(body) = body {
                let delta = String::from_utf8(body)?;
                event.kind = match event.kind {
                    crate::SessionEventKind::Message { message_id, .. } => {
                        crate::SessionEventKind::MessageDelta { message_id, delta }
                    }
                    crate::SessionEventKind::ReasoningDelta { .. } => {
                        crate::SessionEventKind::ReasoningTextDelta { delta }
                    }
                    _ => continue,
                };
            }
            result.push((u64::try_from(row.try_get::<i64, _>("revision")?)?, event));
        }
        Ok(result)
    }
}
