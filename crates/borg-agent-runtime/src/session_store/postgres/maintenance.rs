//! Housekeeping: compaction, retention, import, and session disposal.

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::PostgresSessionStore;
use crate::SessionEvent;
use crate::session_store::{
    EventPersistence, SessionState, SessionStoreCompaction, SessionWorkspaceBinding,
};

/// Rows examined per transaction while compacting.
const COMPACT_BATCH: i64 = 2_000;

/// What a retention sweep removed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RetentionOutcome {
    pub sessions_deleted: u64,
    pub events_deleted: u64,
}

impl PostgresSessionStore {
    /// Drop rows the live journal would no longer persist, and search rows that
    /// no longer have an event.
    ///
    /// `vacuum` runs a plain `vacuum analyze`, which returns space to the table
    /// for reuse and refreshes planner statistics. It deliberately does NOT run
    /// `vacuum full`: that takes an exclusive lock and rewrites the table, which
    /// on a shared journal would stall every agent on the machine -- exactly the
    /// contention this backend exists to remove.
    pub async fn compact(&self, vacuum: bool) -> Result<SessionStoreCompaction> {
        let bytes_before = self.journal_bytes().await?;
        let mut deleted_events = 0u64;
        let mut cursor: Option<(Uuid, i64)> = None;
        loop {
            let (after_session, after_sequence) = cursor
                .map(|(session_id, sequence)| (Some(session_id), sequence))
                .unwrap_or((None, -1));
            let rows = sqlx::query(
                "select session_id, sequence, event_json, event_body, dict_id \
                 from session_events \
                 where event_kind = 'subagent_activity' \
                   and ($1::uuid is null or session_id > $1::uuid \
                        or (session_id = $1::uuid and sequence > $2)) \
                 order by session_id, sequence limit $3",
            )
            .bind(after_session)
            .bind(after_sequence)
            .bind(COMPACT_BATCH)
            .fetch_all(self.pool())
            .await?;
            let Some(last) = rows.last() else { break };
            cursor = Some((last.try_get("session_id")?, last.try_get("sequence")?));

            // Mirrored subagent rows whose live counterpart is no longer
            // durable are the only events safe to remove: everything else is
            // canonical history.
            let mut stale = Vec::new();
            for row in &rows {
                let json: Option<serde_json::Value> = row.try_get("event_json")?;
                let bytes: Option<Vec<u8>> = row.try_get("event_body")?;
                let dict_id: Option<i32> = row.try_get("dict_id")?;
                let value = self.decode_body(json, bytes, dict_id).await?;
                let event: SessionEvent = serde_json::from_value(value)?;
                if event.kind.persistence() != EventPersistence::Durable {
                    stale.push((
                        row.try_get::<Uuid, _>("session_id")?,
                        row.try_get::<i64, _>("sequence")?,
                    ));
                }
            }
            if stale.is_empty() {
                continue;
            }
            let mut transaction = self.pool().begin().await?;
            for (session_id, sequence) in &stale {
                sqlx::query("delete from session_events where session_id = $1 and sequence = $2")
                    .bind(session_id)
                    .bind(sequence)
                    .execute(&mut *transaction)
                    .await?;
            }
            transaction.commit().await?;
            deleted_events += stale.len() as u64;
        }

        let deleted_search_rows = sqlx::query(
            "delete from session_event_search s where not exists ( \
                 select 1 from session_events e \
                 where e.session_id = s.session_id and e.event_id = s.event_id)",
        )
        .execute(self.pool())
        .await?
        .rows_affected();

        if vacuum {
            // Cannot run inside a transaction, hence the raw execution.
            sqlx::raw_sql("vacuum analyze").execute(self.pool()).await?;
        }
        let bytes_after = self.journal_bytes().await?;
        Ok(SessionStoreCompaction {
            deleted_events,
            deleted_search_rows,
            vacuumed: vacuum,
            bytes_before,
            bytes_after,
        })
    }

    async fn journal_bytes(&self) -> Result<i64> {
        Ok(
            sqlx::query_scalar("select pg_database_size(current_database())::bigint")
                .fetch_one(self.pool())
                .await?,
        )
    }

    /// Delete whole sessions last touched before `cutoff`.
    ///
    /// The growth problem this journal has is unbounded history, and no engine
    /// fixes that by itself. Deliberately explicit and caller-driven: nothing
    /// deletes history on a schedule unless an operator asks for it, and a
    /// session with descendants is kept so a fork can never be orphaned.
    pub async fn enforce_retention(
        &self,
        cutoff: DateTime<Utc>,
        max_sessions: usize,
    ) -> Result<RetentionOutcome> {
        let mut outcome = RetentionOutcome::default();
        for _ in 0..max_sessions {
            let candidate: Option<Uuid> = sqlx::query_scalar(
                "select id from sessions s where s.updated_at < $1 \
                   and not exists (select 1 from sessions c \
                     where c.parent_session_id = s.id or c.owner_session_id = s.id) \
                 order by s.updated_at limit 1",
            )
            .bind(cutoff)
            .fetch_optional(self.pool())
            .await?;
            let Some(session_id) = candidate else { break };
            let mut transaction = self.pool().begin().await?;
            let events: i64 =
                sqlx::query_scalar("select count(*) from session_events where session_id = $1")
                    .bind(session_id)
                    .fetch_one(&mut *transaction)
                    .await?;
            sqlx::query("delete from host_launches where session_id = $1")
                .bind(session_id)
                .execute(&mut *transaction)
                .await?;
            // Every satellite table cascades from `sessions`, so this one
            // delete removes the events, payloads, actions and search rows too.
            let deleted = sqlx::query("delete from sessions where id = $1")
                .bind(session_id)
                .execute(&mut *transaction)
                .await?
                .rows_affected();
            transaction.commit().await?;
            if deleted == 0 {
                break;
            }
            outcome.sessions_deleted += 1;
            outcome.events_deleted += u64::try_from(events).unwrap_or(0);
        }
        Ok(outcome)
    }

    /// Remove a session that never did anything.
    ///
    /// Refuses once a session has resumable activity or descendants: an empty
    /// row is disposable, a conversation is not.
    pub async fn discard_empty_session(&self, session_id: Uuid) -> Result<bool> {
        let mut transaction = self.pool().begin().await?;
        let state_json: Option<String> =
            sqlx::query_scalar("select state_json from sessions where id = $1 for update")
                .bind(session_id)
                .fetch_optional(&mut *transaction)
                .await?;
        let Some(state_json) = state_json else {
            transaction.rollback().await?;
            return Ok(false);
        };
        let state: SessionState = serde_json::from_str(&state_json)?;
        if state.has_resumable_activity() {
            transaction.rollback().await?;
            return Ok(false);
        }
        let has_children: bool = sqlx::query_scalar(
            "select exists(select 1 from sessions \
             where owner_session_id = $1 or parent_session_id = $1)",
        )
        .bind(session_id)
        .fetch_one(&mut *transaction)
        .await?;
        if has_children {
            transaction.rollback().await?;
            return Ok(false);
        }
        sqlx::query("delete from host_launches where session_id = $1")
            .bind(session_id)
            .execute(&mut *transaction)
            .await?;
        let deleted = sqlx::query("delete from sessions where id = $1")
            .bind(session_id)
            .execute(&mut *transaction)
            .await?
            .rows_affected()
            != 0;
        transaction.commit().await?;
        Ok(deleted)
    }

    /// Create a session directly inside an existing workspace.
    ///
    /// Only valid before the session has events, so a resumed transcript can
    /// never silently move workspaces.
    pub async fn create_session_in_workspace(
        &self,
        session_id: Uuid,
        workspace_id: Uuid,
    ) -> Result<SessionWorkspaceBinding> {
        self.create_session_in_workspace_as(session_id, workspace_id, session_id)
            .await
    }

    /// Create a session attached to an existing durable participant, which a
    /// cloud workspace may keep stable across many disposable sessions.
    pub async fn create_session_in_workspace_as(
        &self,
        session_id: Uuid,
        workspace_id: Uuid,
        participant_id: Uuid,
    ) -> Result<SessionWorkspaceBinding> {
        crate::SessionStore::create_session(self, session_id).await?;
        let attached_at = Utc::now();
        // The update matches only the default self-binding written by
        // `create_session`, so re-homing a session that is already attached
        // elsewhere fails instead of moving its history.
        let moved = sqlx::query(
            "update session_workspace_bindings \
             set workspace_id = $1, participant_id = $2, host_id = null, attached_at = $3 \
             where session_id = $4 and workspace_id = $4 and participant_id = $4",
        )
        .bind(workspace_id)
        .bind(participant_id)
        .bind(attached_at)
        .bind(session_id)
        .execute(self.pool())
        .await?
        .rows_affected();
        ensure!(
            moved == 1,
            "new session workspace binding was not in its default state"
        );
        Ok(SessionWorkspaceBinding {
            session_id,
            workspace_id,
            participant_id,
            host_id: None,
            attached_at,
        })
    }

    /// Import a transcript as a brand new session.
    ///
    /// All-or-nothing, and only into a session id that does not exist yet: a
    /// partial import would leave a transcript with a hole in it, and importing
    /// over a live session would rewrite history that something else owns.
    pub async fn import_session_events(
        &self,
        session_id: Uuid,
        events: Vec<SessionEvent>,
    ) -> Result<bool> {
        ensure!(!events.is_empty(), "import contains no events");
        ensure!(
            events
                .iter()
                .all(|event| event.session_id == session_id && event.sequence == 0),
            "import events must belong to the destination session with unassigned sequences"
        );
        let mut transaction = self.pool().begin().await?;
        let now = Utc::now();
        let inserted = sqlx::query(
            "insert into sessions (id, state_json, projection_version, created_at, updated_at) \
             values ($1, $2, 3, $3, $3) on conflict (id) do nothing",
        )
        .bind(session_id)
        .bind(serde_json::to_string(&SessionState::default())?)
        .bind(now)
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if inserted == 0 {
            transaction.rollback().await?;
            return Ok(false);
        }
        sqlx::query(
            "insert into session_workspace_bindings \
             (session_id, workspace_id, participant_id, attached_at) values ($1, $1, $1, $2)",
        )
        .bind(session_id)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        for event in events {
            self.append_durable_in_transaction(&mut transaction, event)
                .await
                .context("importing a session event")?;
        }
        transaction.commit().await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::super::testing::{ScratchDatabase, test_url};
    use super::*;
    use crate::session_store::SessionStore;
    use crate::{EventActor, MessageStatus, SessionEventKind};

    async fn store(url: &str) -> (ScratchDatabase, PostgresSessionStore) {
        let scratch = ScratchDatabase::create(url).await;
        let store = PostgresSessionStore::connect_with_pool_size(&scratch.url, 4)
            .await
            .expect("connect");
        (scratch, store)
    }

    fn message(session_id: Uuid, text: &str) -> SessionEvent {
        SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: text.to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        )
    }

    #[tokio::test]
    async fn an_untouched_session_is_disposable_but_a_used_one_is_not() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store) = store(&url).await;
        let empty = Uuid::new_v4();
        store.create_session(empty).await.expect("create");
        assert!(store.discard_empty_session(empty).await.expect("discard"));
        assert!(!store.contains_session(empty).await.unwrap());
        // Discarding something that is already gone is not an error.
        assert!(!store.discard_empty_session(empty).await.expect("discard"));

        let used = Uuid::new_v4();
        store.create_session(used).await.expect("create");
        store
            .append(SessionEvent::new(used, 0, SessionEventKind::SessionStarted))
            .await
            .expect("started");
        store
            .append(message(used, "real work"))
            .await
            .expect("message");
        assert!(
            !store.discard_empty_session(used).await.expect("discard"),
            "a session with resumable activity must not be discarded"
        );
        assert!(store.contains_session(used).await.unwrap());

        // A parent with a fork must survive too, or the fork is orphaned.
        let parent = Uuid::new_v4();
        store.create_session(parent).await.expect("create");
        store
            .append(SessionEvent::new(
                parent,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
            .expect("started");
        let child = Uuid::new_v4();
        store.fork_before(parent, child, 2).await.expect("fork");
        assert!(!store.discard_empty_session(parent).await.expect("discard"));
        scratch.discard().await;
    }

    #[tokio::test]
    async fn a_session_is_created_directly_inside_a_workspace_once() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store) = store(&url).await;
        let session_id = Uuid::new_v4();
        let workspace_id = Uuid::new_v4();
        let binding = store
            .create_session_in_workspace(session_id, workspace_id)
            .await
            .expect("create in workspace");
        assert_eq!(binding.workspace_id, workspace_id);
        assert_eq!(binding.participant_id, session_id);
        assert_eq!(
            store
                .workspace_binding(session_id)
                .await
                .unwrap()
                .unwrap()
                .workspace_id,
            workspace_id
        );

        // Re-homing an attached session would move its history; refused.
        assert!(
            store
                .create_session_in_workspace(session_id, Uuid::new_v4())
                .await
                .is_err()
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn an_import_lands_whole_or_not_at_all() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store) = store(&url).await;
        let session_id = Uuid::new_v4();
        let events = vec![
            SessionEvent::new(session_id, 0, SessionEventKind::SessionStarted),
            message(session_id, "imported one"),
            message(session_id, "imported two"),
        ];
        assert!(
            store
                .import_session_events(session_id, events.clone())
                .await
                .expect("import")
        );
        let stored = store.read(session_id).await.expect("read");
        assert_eq!(stored.len(), 3);
        assert_eq!(
            stored
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3],
            "an import must be renumbered into one contiguous sequence space"
        );

        // Importing over an existing session would rewrite history it owns.
        assert!(
            !store
                .import_session_events(session_id, events)
                .await
                .expect("import")
        );
        assert_eq!(store.read(session_id).await.unwrap().len(), 3);

        // An import carrying another session's events is refused outright.
        let other = Uuid::new_v4();
        assert!(
            store
                .import_session_events(other, vec![message(Uuid::new_v4(), "wrong session")])
                .await
                .is_err()
        );
        assert!(!store.contains_session(other).await.unwrap());
        assert!(
            store
                .import_session_events(other, Vec::new())
                .await
                .is_err()
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn retention_deletes_aged_sessions_and_spares_ones_with_descendants() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store) = store(&url).await;
        let aged = Uuid::new_v4();
        store.create_session(aged).await.expect("create");
        store
            .append(SessionEvent::new(aged, 0, SessionEventKind::SessionStarted))
            .await
            .expect("started");
        store
            .append(message(aged, "old news"))
            .await
            .expect("message");

        let parent = Uuid::new_v4();
        store.create_session(parent).await.expect("create");
        store
            .append(SessionEvent::new(
                parent,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
            .expect("started");
        let child = Uuid::new_v4();
        store.fork_before(parent, child, 2).await.expect("fork");

        let recent = Uuid::new_v4();
        store.create_session(recent).await.expect("create");
        store
            .append(SessionEvent::new(
                recent,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
            .expect("started");

        sqlx::query(
            "update sessions set updated_at = now() - interval '400 days' where id = any($1)",
        )
        .bind(vec![aged, parent])
        .execute(store.pool())
        .await
        .expect("age");

        let outcome = store
            .enforce_retention(Utc::now() - chrono::Duration::days(365), 64)
            .await
            .expect("retention");
        assert_eq!(outcome.sessions_deleted, 1);
        assert_eq!(outcome.events_deleted, 2);
        assert!(!store.contains_session(aged).await.unwrap());
        assert!(
            store.contains_session(parent).await.unwrap(),
            "a session with a fork must not be deleted out from under it"
        );
        assert!(store.contains_session(recent).await.unwrap());

        // Deleting a session takes its events with it rather than orphaning
        // them: every satellite table cascades from `sessions`.
        let orphans: i64 =
            sqlx::query_scalar("select count(*) from session_events where session_id = $1")
                .bind(aged)
                .fetch_one(store.pool())
                .await
                .expect("count");
        assert_eq!(orphans, 0);
        scratch.discard().await;
    }

    #[tokio::test]
    async fn compaction_prunes_stale_projections_and_reports_sizes() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store) = store(&url).await;
        let session_id = Uuid::new_v4();
        store.create_session(session_id).await.expect("create");
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::SessionStarted,
            ))
            .await
            .expect("started");
        store
            .append(message(session_id, "searchable"))
            .await
            .expect("message");
        store
            .query_history(
                session_id,
                crate::SessionHistoryQuery {
                    text: Some("searchable".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("search");

        // Orphan the projection the way a deleted event would.
        sqlx::query("delete from session_events where session_id = $1 and sequence = 2")
            .bind(session_id)
            .execute(store.pool())
            .await
            .expect("delete event");

        let outcome = store.compact(false).await.expect("compact");
        assert_eq!(
            outcome.deleted_search_rows, 1,
            "a search row without an event must be pruned"
        );
        assert!(!outcome.vacuumed);
        assert!(outcome.bytes_before > 0 && outcome.bytes_after > 0);

        // Compaction is idempotent.
        assert_eq!(
            store
                .compact(false)
                .await
                .expect("compact")
                .deleted_search_rows,
            0
        );
        scratch.discard().await;
    }
}
