//! Commit-coupled wakeups, not event transport. A reconnect always tails durable
//! watermarks: NOTIFY is deliberately only a hint and carries no private data.
use anyhow::Result;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

pub const CHANNEL: &str = "borg_session_committed";

pub async fn notify(tx: &mut Transaction<'_, Postgres>, session_id: Uuid) -> Result<()> {
    sqlx::query("select pg_notify($1,$2)").bind(CHANNEL).bind(session_id.to_string()).execute(&mut **tx).await?;
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
pub struct PostgresAuthority { pub pool: sqlx::PgPool }
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
    pub async fn text_deltas_after(&self, session:Uuid, after:u64)->Result<Vec<(u64,crate::SessionEvent)>> {
        use sqlx::Row;
        let rows=sqlx::query("select c.revision,c.body,s.event_json from session_live_text_chunks c join session_live_state s using(session_id,live_key) where c.session_id=$1 and c.revision>$2 order by c.revision limit 128")
            .bind(session).bind(i64::try_from(after)?).fetch_all(&self.pool).await?;
        let mut result=Vec::with_capacity(rows.len());
        let mut bytes=0;
        for row in rows {
            let body:Vec<u8>=row.try_get("body")?;
            bytes+=body.len();
            if bytes>1_048_576 {break}
            let delta=String::from_utf8(body)?;
            let mut event:crate::SessionEvent=serde_json::from_value(row.try_get("event_json")?)?;
            event.kind=match event.kind {
                crate::SessionEventKind::Message{message_id,..}=>crate::SessionEventKind::MessageDelta{message_id,delta},
                crate::SessionEventKind::ReasoningDelta{..}=>crate::SessionEventKind::ReasoningTextDelta{delta},
                _=>continue,
            };
            result.push((u64::try_from(row.try_get::<i64,_>("revision")?)?,event));
        }
        Ok(result)
    }
}
