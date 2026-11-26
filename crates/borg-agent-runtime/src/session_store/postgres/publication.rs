//! Commit-coupled wakeups, not event transport. A reconnect always tails durable
//! watermarks: NOTIFY is deliberately only a hint and carries no private data.
use anyhow::Result;
use uuid::Uuid;

pub const CHANNEL: &str = "borg_session_committed";

/// Wakeups sent after the writer commits, never inside its transaction.
///
/// A transaction that NOTIFYs holds Postgres's single cluster-wide notify lock
/// from pre-commit until its WAL flush finishes, so every event append on the
/// host committed one at a time behind every other. With busy subagents that
/// queue reached seconds and stalled prompt admission. A NOTIFY-only statement
/// writes no WAL, and hints raised while one is in flight coalesce into the
/// next, so writers never wait on notification at all.
#[derive(Debug, Default)]
pub struct CommittedHints {
    state: std::sync::Mutex<(std::collections::HashSet<Uuid>, bool)>,
}

impl CommittedHints {
    fn hint(self: &std::sync::Arc<Self>, pool: &sqlx::PgPool, session_id: Uuid) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.0.insert(session_id);
            if std::mem::replace(&mut state.1, true) {
                return;
            }
        }
        let (hints, pool) = (std::sync::Arc::clone(self), pool.clone());
        runtime.spawn(async move {
            loop {
                let batch: Vec<String> = {
                    let mut state = hints
                        .state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if state.0.is_empty() {
                        state.1 = false;
                        return;
                    }
                    state.0.drain().map(|id| id.to_string()).collect()
                };
                if let Err(error) =
                    sqlx::query("select pg_notify($1, session) from unnest($2::text[]) session")
                        .bind(CHANNEL)
                        .bind(&batch)
                        .execute(&pool)
                        .await
                {
                    tracing::warn!(%error, "committed-session wakeup failed");
                }
            }
        });
    }
}

impl super::PostgresSessionStore {
    /// Call after `commit()` of a transaction that appended to `session_id`.
    pub(super) fn committed(&self, session_id: Uuid) {
        self.committed.hint(self.pool(), session_id);
    }

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
        let rows=sqlx::query("select revision,body,event_json from (select c.revision,c.body,s.event_json from session_live_text_chunks c join session_live_state s using(session_id,live_key) where c.session_id=$1 and c.revision>$2 union all select s.revision,null::bytea,s.event_json from session_live_state s where s.session_id=$1 and s.revision>$2 and s.live_key not like 'message:%' and s.live_key<>'reasoning' and s.live_key not like 'reasoning:%') live order by revision limit 128")
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
                    crate::SessionEventKind::ReasoningBlockSnapshot { reasoning_id, .. } => {
                        crate::SessionEventKind::ReasoningBlockDelta {
                            reasoning_id,
                            delta,
                        }
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
