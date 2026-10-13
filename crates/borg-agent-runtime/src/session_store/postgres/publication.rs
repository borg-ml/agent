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
