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
    /// Latest team activity visible at a sequence in this session's lineage.
    /// A fork may cut inside an inherited prefix, so map that cut back into
    /// the parent's sequence space before reading its team.
    pub(super) fn team_events_at<'a>(
        &'a self,
        session_id: Uuid,
        until: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<SessionEvent>>> + Send + 'a>> {
        Box::pin(async move {
            let session = self.session_row(session_id).await?;
            let inherited_limit = until.min(session.inherited_event_count);
            let mut events = if inherited_limit > 0 {
                if let (Some(parent), Some(cut)) =
                    (session.parent_session_id, session.parent_cut_sequence)
                {
                    let parent_until = if inherited_limit == session.inherited_event_count {
                        cut
                    } else {
                        self.inherited_sequence(parent, cut, inherited_limit)
                            .await?
                    };
                    self.team_events_at(parent, parent_until).await?
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };
            let rows = sqlx::query(
                "select latest.event_json, latest.event_body, latest.dict_id \
                 from (select distinct on (subagent_session_id) \
                       sequence, event_json, event_body, dict_id \
                       from session_events \
                       where session_id = $1 and sequence <= $2 \
                         and event_kind = 'subagent_activity' \
                       order by subagent_session_id, sequence desc) latest \
                 order by latest.sequence",
            )
            .bind(session_id)
            .bind(i64::try_from(until).unwrap_or(i64::MAX))
            .fetch_all(self.pool())
            .await?;
            events.extend(self.decode_events(&rows).await?);
            Ok(events)
        })
    }

    /// Count inherited events without fetching their bodies.
    pub(super) async fn fork_event_count(&self, session_id: Uuid, until: u64) -> Result<u64> {
        let session = self.session_row(session_id).await?;
        let local: i64 = sqlx::query_scalar(
            "select count(*) from session_events \
             where session_id = $1 and sequence <= $2 and fork_inheritable",
        )
        .bind(session_id)
        .bind(i64::try_from(until).unwrap_or(i64::MAX))
        .fetch_one(self.pool())
        .await?;
        Ok(until.min(session.inherited_event_count) + u64::try_from(local)?)
    }

    /// Map an inherited ordinal back to the parent's sequence space using the
    /// fork index, not a decode of the parent's entire history.
    pub(super) async fn inherited_sequence(
        &self,
        parent: Uuid,
        cut: u64,
        ordinal: u64,
    ) -> Result<u64> {
        if ordinal == 0 {
            return Ok(0);
        }
        let session = self.session_row(parent).await?;
        if ordinal <= session.inherited_event_count {
            anyhow::ensure!(
                ordinal <= cut,
                "inherited event {ordinal} is beyond cut {cut}"
            );
            return Ok(ordinal);
        }
        let sequence: Option<i64> = sqlx::query_scalar(
            "select sequence from session_events \
             where session_id = $1 and sequence <= $2 and fork_inheritable \
             order by sequence limit 1 offset $3",
        )
        .bind(parent)
        .bind(i64::try_from(cut).unwrap_or(i64::MAX))
        .bind(i64::try_from(ordinal - session.inherited_event_count - 1).unwrap_or(i64::MAX))
        .fetch_optional(self.pool())
        .await?;
        let sequence = sequence.ok_or_else(|| {
            anyhow::anyhow!("session {parent} has no inherited event {ordinal} before cut {cut}")
        })?;
        Ok(u64::try_from(sequence)?)
    }

    /// A bounded slice in this session's contiguous sequence space. Ancestors
    /// supply only inheritable rows in the mapped range, including for a fork
    /// of a fork; excluded bodies are never fetched or decoded.
    pub(super) fn composed_events_range<'a>(
        &'a self,
        session_id: Uuid,
        after: u64,
        until: u64,
        inheritable_only: bool,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<SessionEvent>>> + Send + 'a>> {
        Box::pin(async move {
            if until <= after {
                return Ok(Vec::new());
            }
            let session = self.session_row(session_id).await?;
            let inherited_after = after.min(session.inherited_event_count);
            let inherited_until = until.min(session.inherited_event_count);
            let mut events = Vec::new();
            if inherited_until > inherited_after
                && let (Some(parent), Some(cut)) =
                    (session.parent_session_id, session.parent_cut_sequence)
            {
                let parent_after = self
                    .inherited_sequence(parent, cut, inherited_after)
                    .await?;
                let parent_until = if inherited_until == session.inherited_event_count {
                    cut
                } else {
                    self.inherited_sequence(parent, cut, inherited_until)
                        .await?
                };
                let inherited = self
                    .composed_events_range(parent, parent_after, parent_until, true)
                    .await?;
                for (index, mut event) in inherited.into_iter().enumerate() {
                    event.id = inherited_event_id(session_id, event.id);
                    event.session_id = session_id;
                    event.sequence = inherited_after + index as u64 + 1;
                    events.push(event);
                }
            }
            if until > session.inherited_event_count {
                let rows = sqlx::query(
                    "select event_json, event_body, dict_id from session_events \
                     where session_id = $1 and sequence > $2 and sequence <= $3 \
                       and (not $4 or fork_inheritable) order by sequence",
                )
                .bind(session_id)
                .bind(i64::try_from(after.max(session.inherited_event_count)).unwrap_or(i64::MAX))
                .bind(i64::try_from(until).unwrap_or(i64::MAX))
                .bind(inheritable_only)
                .fetch_all(self.pool())
                .await?;
                events.extend(self.decode_events(&rows).await?);
            }
            Ok(events)
        })
    }

    pub(super) async fn composed_events(
        &self,
        session_id: Uuid,
        before_or_at: Option<u64>,
    ) -> Result<Vec<SessionEvent>> {
        let until = match before_or_at {
            Some(until) => until,
            None => self
                .session_row(session_id)
                .await?
                .next_sequence
                .saturating_sub(1),
        };
        self.composed_events_range(session_id, 0, until, false)
            .await
    }

    /// Restore state from the closest durable checkpoint, then replay at most
    /// 255 local events. Older sessions without checkpoints still replay.
    pub(super) async fn fork_projection(
        &self,
        parent_session_id: Uuid,
        sequence: u64,
    ) -> Result<(u64, SessionState)> {
        let until = sequence.saturating_sub(1);
        let inherited_event_count = self.fork_event_count(parent_session_id, until).await?;
        let checkpoint: Option<String> = sqlx::query_scalar(
            "select projection_json from session_events \
             where session_id = $1 and sequence <= $2 and projection_json <> '' \
             order by sequence desc limit 1",
        )
        .bind(parent_session_id)
        .bind(i64::try_from(until).unwrap_or(i64::MAX))
        .fetch_optional(self.pool())
        .await?;
        let mut state = checkpoint
            .map(|json| serde_json::from_str::<SessionState>(&json))
            .transpose()?
            .unwrap_or_default();
        for event in self
            .composed_events_range(parent_session_id, state.latest_sequence, until, false)
            .await?
        {
            state.apply(&event)?;
        }
        Ok((inherited_event_count, state))
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
                        fast: false,
                        ultrafast: false,
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
        let mut changed = child(kept, "/root/kept", before);
        if let SessionEventKind::SubagentActivity { agent, .. } = &mut changed.kind {
            agent.status = crate::SubagentStatus::Ready;
            agent.updated_at = after;
            agent.detail = Some("finished after the cut".to_string());
        }
        store.append(changed).await.unwrap();

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
        assert_eq!(agents[0].status, crate::SubagentStatus::Running);
        assert_eq!(agents[0].detail, None);
        assert!(store.fork_team_events(parent).await.unwrap().is_empty());
        scratch.discard().await;
    }

    /// Reverting an inherited prompt must restore the team at that prompt,
    /// even when the intermediate fork was cut after a later status change.
    #[tokio::test]
    async fn nested_fork_team_uses_snapshot_before_inherited_cut() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, parent) = conversation(&url).await;
        let child_id = Uuid::new_v4();
        let before = Utc::now() - chrono::Duration::minutes(1);
        let mut agent = crate::SubagentSnapshot {
            session_id: child_id,
            parent_session_id: parent,
            task_name: "/root/worker".to_string(),
            status: crate::SubagentStatus::Running,
            provider: crate::CodingProvider::Claude,
            model: None,
            effort: None,
            fast: false,
            ultrafast: false,
            cwd: std::path::PathBuf::from("/tmp"),
            created_at: before,
            updated_at: before,
            detail: None,
            final_text: None,
            usage: crate::SubagentUsage::default(),
            interrupted_by: None,
        };
        store
            .append(SessionEvent::new(
                parent,
                0,
                SessionEventKind::SubagentActivity {
                    activity: crate::SubagentActivityKind::Started,
                    agent: agent.clone(),
                    event: None,
                },
            ))
            .await
            .unwrap();
        store
            .append(SessionEvent::new(
                parent,
                0,
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: EventActor::Assistant,
                    text: "before update".to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ))
            .await
            .unwrap();
        agent.status = crate::SubagentStatus::Ready;
        agent.updated_at = Utc::now();
        store
            .append(SessionEvent::new(
                parent,
                0,
                SessionEventKind::SubagentActivity {
                    activity: crate::SubagentActivityKind::Completed,
                    agent,
                    event: None,
                },
            ))
            .await
            .unwrap();
        let after_update = store
            .append(SessionEvent::new(
                parent,
                0,
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: EventActor::Assistant,
                    text: "after update".to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ))
            .await
            .unwrap()
            .sequence;

        let first = Uuid::new_v4();
        store
            .fork_before(parent, first, after_update + 1)
            .await
            .unwrap();
        let inherited_after_update = store
            .read(first)
            .await
            .unwrap()
            .into_iter()
            .find(|event| {
                matches!(&event.kind, SessionEventKind::Message { text, .. } if text == "after update")
            })
            .unwrap()
            .sequence;
        let second = Uuid::new_v4();
        store
            .fork_before(first, second, inherited_after_update)
            .await
            .unwrap();

        let team = store.fork_team_events(second).await.unwrap();
        let agents = team
            .iter()
            .filter_map(|event| match &event.kind {
                SessionEventKind::SubagentActivity { agent, .. } => Some(agent),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(agents.len(), 1, "{agents:?}");
        assert_eq!(agents[0].session_id, child_id);
        assert_eq!(agents[0].parent_session_id, second);
        assert_eq!(agents[0].status, crate::SubagentStatus::Running);
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
    async fn revert_and_tail_paging_do_not_decode_history_before_the_checkpoint() {
        let Some(url) = test_url() else {
            return;
        };
        let (scratch, store, parent) = conversation(&url).await;
        for _ in 0..260 {
            store
                .append(SessionEvent::new(
                    parent,
                    0,
                    SessionEventKind::StatusChanged {
                        status: crate::SessionStatus::Ready,
                        detail: None,
                    },
                ))
                .await
                .unwrap();
        }
        let tail = store
            .append(SessionEvent::new(
                parent,
                0,
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: EventActor::Assistant,
                    text: "newest reply".to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ))
            .await
            .unwrap();
        let state = store.state(parent).await.unwrap();
        // An unreadable old body makes a full-history regression deterministic
        // without a timing assertion or a hundreds-of-thousands-row fixture.
        sqlx::query(
            "update session_events set event_json = '{}'::jsonb \
             where session_id = $1 and sequence = 2",
        )
        .bind(parent)
        .execute(store.pool())
        .await
        .unwrap();
        let fork = Uuid::new_v4();
        let result = store
            .fork_before(parent, fork, tail.sequence + 1)
            .await
            .unwrap();
        assert_eq!(result.inherited_event_count, 6);
        assert_eq!(store.state(fork).await.unwrap(), state.for_fork(6));
        let page = store.events_after(fork, 4, 2).await.unwrap();
        assert_eq!(texts(&page), ["reply 3", "newest reply"]);
        assert_eq!(
            page.iter().map(|event| event.sequence).collect::<Vec<_>>(),
            [5, 6]
        );
        store
            .append(SessionEvent::new(fork, 0, SessionEventKind::SessionStarted))
            .await
            .unwrap();
        let nested = Uuid::new_v4();
        store.fork_before(fork, nested, 8).await.unwrap();
        assert_eq!(
            texts(&store.events_after(nested, 4, 2).await.unwrap()),
            texts(&page)
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
