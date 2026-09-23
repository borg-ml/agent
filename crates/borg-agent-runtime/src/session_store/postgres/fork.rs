//! Forking a session, and reading a fork's composed history.
//!
//! A fork does not copy its parent's events. It records a cut, and every read
//! composes the inherited prefix with the fork's own events, renumbering the
//! prefix into the child's sequence space. That keeps a fork cheap to create no
//! matter how long its parent is, at the cost of this composition on read.
//!
//! An inherited event is rewritten as it is read: its session id becomes the
//! child's and its id is derived from the parent's, so two forks of the same
//! parent never share an event id while the lineage stays reconstructable.

use anyhow::Result;
use chrono::Utc;
use sqlx::Row;
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

use super::PostgresSessionStore;
use super::recovery::StoredSession;
use crate::SessionEvent;
use crate::session_store::{SessionState, SessionStoreFork};

/// A forked session renumbers its inherited prefix, so an inherited event needs
/// a stable id of its own rather than reusing the parent's.
pub(super) fn inherited_event_id(session_id: Uuid, source_event_id: Uuid) -> Uuid {
    Uuid::new_v5(&session_id, source_event_id.as_bytes())
}

impl PostgresSessionStore {
    /// Every event this session authored itself, within bounds.
    async fn local_events(
        &self,
        session_id: Uuid,
        after: u64,
        until: u64,
    ) -> Result<Vec<(SessionEvent, bool)>> {
        let rows = sqlx::query(
            "select event_json, event_body, dict_id, fork_inheritable from session_events \
             where session_id = $1 and sequence > $2 and sequence <= $3 order by sequence",
        )
        .bind(session_id)
        .bind(i64::try_from(after).unwrap_or(i64::MAX))
        .bind(i64::try_from(until).unwrap_or(i64::MAX))
        .fetch_all(self.pool())
        .await?;
        let mut events = Vec::with_capacity(rows.len());
        for row in &rows {
            let json: Option<serde_json::Value> = row.try_get("event_json")?;
            let bytes: Option<Vec<u8>> = row.try_get("event_body")?;
            let dict_id: Option<i32> = row.try_get("dict_id")?;
            let value = self.decode_body(json, bytes, dict_id).await?;
            let event: SessionEvent = serde_json::from_value(value)?;
            events.push((event, row.try_get::<bool, _>("fork_inheritable")?));
        }
        Ok(events)
    }

    /// A session's full history: its inherited prefix followed by its own
    /// events, all in one contiguous sequence space.
    pub(super) fn composed_events<'a>(
        &'a self,
        session_id: Uuid,
        before_or_at: Option<u64>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<SessionEvent>>> + Send + 'a>> {
        Box::pin(async move {
            let session = self.session_row(session_id).await?;
            let logical_limit = before_or_at.unwrap_or(session.next_sequence.saturating_sub(1));
            let mut events = Vec::new();
            if let (Some(parent), Some(cut)) =
                (session.parent_session_id, session.parent_cut_sequence)
            {
                // The prefix is whatever the parent would itself read up to the
                // cut, so a fork of a fork composes correctly through the whole
                // lineage rather than only one generation.
                let inherited = self.composed_events(parent, Some(cut)).await?;
                let inherited_limit = logical_limit.min(session.inherited_event_count);
                for (index, mut event) in inherited
                    .into_iter()
                    .filter(|event| event.kind.is_fork_inheritable())
                    .take(usize::try_from(inherited_limit).unwrap_or(usize::MAX))
                    .enumerate()
                {
                    event.id = inherited_event_id(session_id, event.id);
                    event.session_id = session_id;
                    event.sequence = index as u64 + 1;
                    events.push(event);
                }
            }
            if logical_limit > session.inherited_event_count {
                let local = self
                    .local_events(session_id, session.inherited_event_count, logical_limit)
                    .await?;
                events.extend(local.into_iter().map(|(event, _)| event));
            }
            Ok(events)
        })
    }

    /// The parent's state at the cut, and how many of its events the fork
    /// inherits.
    pub(super) async fn fork_projection(
        &self,
        parent_session_id: Uuid,
        sequence: u64,
    ) -> Result<(u64, SessionState)> {
        let events = self
            .composed_events(parent_session_id, sequence.checked_sub(1))
            .await?;
        let inherited_event_count = events
            .iter()
            .filter(|event| event.kind.is_fork_inheritable())
            .count() as u64;
        Ok((inherited_event_count, SessionState::reduce(&events)?))
    }

    pub(super) async fn fork_session_before(
        &self,
        parent_session_id: Uuid,
        session_id: Uuid,
        sequence: u64,
    ) -> Result<SessionStoreFork> {
        let parent: StoredSession = self.session_row(parent_session_id).await?;
        anyhow::ensure!(
            sequence > 0 && sequence <= parent.next_sequence,
            "fork sequence {sequence} is outside session {parent_session_id}"
        );
        let (inherited_event_count, parent_state) =
            self.fork_projection(parent_session_id, sequence).await?;
        let state = parent_state.for_fork(inherited_event_count);
        let now = Utc::now();

        let mut transaction = self.pool().begin().await?;
        sqlx::query(
            "insert into sessions \
             (id, parent_session_id, parent_cut_sequence, inherited_event_count, next_sequence, \
              state_json, projection_version, created_at, updated_at) \
             values ($1, $2, $3, $4, $5, $6, 3, $7, $7)",
        )
        .bind(session_id)
        .bind(parent_session_id)
        .bind(i64::try_from(sequence.saturating_sub(1)).unwrap_or(i64::MAX))
        .bind(i64::try_from(inherited_event_count).unwrap_or(i64::MAX))
        .bind(i64::try_from(inherited_event_count.saturating_add(1)).unwrap_or(i64::MAX))
        .bind(serde_json::to_string(&state)?)
        .bind(now)
        .execute(&mut *transaction)
        .await?;

        // A fork inherits the parent's transcript, so it inherits whichever
        // harness owns that transcript; an undecided parent leaves the fork
        // undecided too.
        Self::copy_harness_routes(&mut transaction, parent_session_id, session_id).await?;

        let parent_workspace: Option<Uuid> = sqlx::query_scalar(
            "select workspace_id from session_workspace_bindings where session_id = $1",
        )
        .bind(parent_session_id)
        .fetch_optional(&mut *transaction)
        .await?;
        sqlx::query(
            "insert into session_workspace_bindings \
             (session_id, workspace_id, participant_id, attached_at) \
             values ($1, $2, $1, $3) on conflict (session_id) do nothing",
        )
        .bind(session_id)
        .bind(parent_workspace.unwrap_or(parent_session_id))
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;

        Ok(SessionStoreFork {
            session_id,
            parent_session_id,
            parent_cut_sequence: sequence.saturating_sub(1),
            inherited_event_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::testing::{ScratchDatabase, test_url};
    use super::*;
    use crate::session_store::SessionStore;
    use crate::{EventActor, MessageStatus, SessionEventKind};

    async fn conversation(url: &str) -> (ScratchDatabase, PostgresSessionStore, Uuid) {
        let scratch = ScratchDatabase::create(url).await;
        let store = PostgresSessionStore::connect_with_pool_size(&scratch.url, 4)
            .await
            .expect("connect");
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
        for index in 0..4 {
            store
                .append(SessionEvent::new(
                    session_id,
                    0,
                    SessionEventKind::Message {
                        message_id: Uuid::new_v4(),
                        actor: EventActor::Assistant,
                        text: format!("reply {index}"),
                        attachments: Vec::new(),
                        status: MessageStatus::Complete,
                        delivery: None,
                    },
                ))
                .await
                .expect("message");
        }
        (scratch, store, session_id)
    }

    fn texts(events: &[SessionEvent]) -> Vec<String> {
        events
            .iter()
            .filter_map(|event| match &event.kind {
                SessionEventKind::Message { text, .. } => Some(text.clone()),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn a_fork_inherits_its_parents_prefix_and_renumbers_it() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, parent) = conversation(&url).await;
        let parent_events = store.read(parent).await.expect("read parent");
        assert_eq!(texts(&parent_events).len(), 4);

        // Cut before the last message: the fork sees everything above it.
        let cut_at = parent_events.last().expect("events").sequence;
        let child = Uuid::new_v4();
        let fork = store
            .fork_before(parent, child, cut_at)
            .await
            .expect("fork");
        assert_eq!(fork.parent_session_id, parent);
        assert_eq!(fork.parent_cut_sequence, cut_at - 1);
        assert!(fork.inherited_event_count > 0);

        let inherited = store.read(child).await.expect("read fork");
        assert_eq!(
            texts(&inherited),
            vec!["reply 0", "reply 1", "reply 2"],
            "a fork inherits its parent's history up to the cut"
        );
        // The prefix is renumbered into the child's own space, contiguously.
        let sequences: Vec<u64> = inherited.iter().map(|event| event.sequence).collect();
        assert_eq!(
            sequences,
            (1..=inherited.len() as u64).collect::<Vec<_>>(),
            "an inherited prefix must be contiguous in the child's sequence space"
        );
        // Inherited events are rewritten to belong to the child, and must not
        // reuse the parent's event ids.
        assert!(inherited.iter().all(|event| event.session_id == child));
        let parent_ids: Vec<Uuid> = parent_events.iter().map(|event| event.id).collect();
        assert!(
            inherited
                .iter()
                .all(|event| !parent_ids.contains(&event.id))
        );
        assert_eq!(
            store.inherited_event_count(child).await.unwrap(),
            fork.inherited_event_count
        );
        scratch.discard().await;
    }

    /// Failure mode: a revert continues the conversation on a fork, which
    /// inherits no `SubagentActivity`, so every worker the parent started
    /// vanished from the roster and could not be messaged or woken.
    #[tokio::test]
    async fn a_fork_takes_over_the_team_started_before_its_cut() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, parent) = conversation(&url).await;
        let child = |id: Uuid, task: &str, created_at: chrono::DateTime<Utc>| {
            SessionEvent::new(
                parent,
                0,
                SessionEventKind::SubagentActivity {
                    activity: crate::SubagentActivityKind::Started,
                    agent: crate::SubagentSnapshot {
                        session_id: id,
                        parent_session_id: parent,
                        task_name: task.to_string(),
                        status: crate::SubagentStatus::Running,
                        provider: crate::CodingProvider::Claude,
                        model: None,
                        effort: None,
                        cwd: std::path::PathBuf::from("/tmp"),
                        created_at,
                        updated_at: created_at,
                        detail: None,
                        final_text: None,
                        usage: crate::SubagentUsage::default(),
                        interrupted_by: None,
                    },
                    event: None,
                },
            )
        };
        let (kept, later) = (Uuid::new_v4(), Uuid::new_v4());
        let before = Utc::now() - chrono::Duration::minutes(1);
        store
            .append(child(kept, "/root/kept", before))
            .await
            .unwrap();
        let cut_at = store
            .append(SessionEvent::new(
                parent,
                0,
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: crate::EventActor::User,
                    text: "reverted prompt".to_string(),
                    attachments: Vec::new(),
                    status: crate::MessageStatus::Complete,
                    delivery: None,
                },
            ))
            .await
            .unwrap()
            .sequence;
        let after = Utc::now() + chrono::Duration::minutes(1);
        store
            .append(child(later, "/root/later", after))
            .await
            .unwrap();

        let fork = Uuid::new_v4();
        store.fork_before(parent, fork, cut_at).await.expect("fork");
        let team = store.fork_team_events(fork).await.unwrap();
        let agents: Vec<_> = team
            .iter()
            .filter_map(|event| match &event.kind {
                SessionEventKind::SubagentActivity { agent, .. } => Some(agent),
                _ => None,
            })
            .collect();
        assert_eq!(agents.len(), 1, "{agents:?}");
        assert_eq!(agents[0].session_id, kept);
        assert_eq!(agents[0].parent_session_id, fork);
        assert!(store.fork_team_events(parent).await.unwrap().is_empty());
        scratch.discard().await;
    }

    #[tokio::test]
    async fn a_fork_and_its_parent_diverge_without_disturbing_each_other() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, parent) = conversation(&url).await;
        let cut_at = store.read(parent).await.unwrap().last().unwrap().sequence;
        let child = Uuid::new_v4();
        store
            .fork_before(parent, child, cut_at)
            .await
            .expect("fork");

        store
            .append(SessionEvent::new(
                child,
                0,
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: EventActor::Assistant,
                    text: "a different path".to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ))
            .await
            .expect("append to fork");

        assert_eq!(
            texts(&store.read(child).await.unwrap()),
            vec!["reply 0", "reply 1", "reply 2", "a different path"]
        );
        // The parent keeps its own history, including the event the fork cut
        // away: a fork is a branch, not a rewrite.
        assert_eq!(
            texts(&store.read(parent).await.unwrap()),
            vec!["reply 0", "reply 1", "reply 2", "reply 3"]
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn a_fork_of_a_fork_composes_through_the_whole_lineage() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, parent) = conversation(&url).await;
        let cut_at = store.read(parent).await.unwrap().last().unwrap().sequence;
        let child = Uuid::new_v4();
        store
            .fork_before(parent, child, cut_at)
            .await
            .expect("fork");
        store
            .append(SessionEvent::new(
                child,
                0,
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: EventActor::Assistant,
                    text: "child reply".to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ))
            .await
            .expect("append to fork");

        let grandchild = Uuid::new_v4();
        let child_tail = store.read(child).await.unwrap().last().unwrap().sequence;
        store
            .fork_before(child, grandchild, child_tail + 1)
            .await
            .expect("fork the fork");
        for id in [parent, child, grandchild] {
            assert_eq!(store.prompt_cache_session_id(id).await.unwrap(), parent);
        }
        assert!(
            store
                .state(grandchild)
                .await
                .unwrap()
                .provider_session_id
                .is_none()
        );
        assert_eq!(
            texts(&store.read(grandchild).await.unwrap()),
            vec!["reply 0", "reply 1", "reply 2", "child reply"],
            "a nested fork must compose through every generation"
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn a_cut_outside_the_parent_is_refused() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, parent) = conversation(&url).await;
        assert!(
            store.fork_before(parent, Uuid::new_v4(), 0).await.is_err(),
            "sequence 0 is not a position in any session"
        );
        assert!(
            store
                .fork_before(parent, Uuid::new_v4(), 10_000)
                .await
                .is_err(),
            "a cut beyond the parent's history is not a fork point"
        );
        scratch.discard().await;
    }
}
