//! Rebuilding a session's provider context after a restart.
//!
//! Recovery is the widest read in the store, so the shape of these queries is
//! what decides whether resuming a long session takes milliseconds or minutes.
//! Two rules drive the port:
//!
//! * Narrow in SQL using indexed or lifted columns only. Predicates that look
//!   inside the body are evaluated in Rust, because a cold row's body is opaque
//!   to SQL and a jsonb filter would silently drop aged history from recovery.
//! * Never read a slice the caller did not ask for. The context slice carries
//!   every tool payload in the session; a queue-only resume must not pay for it.

use anyhow::{Context, Result};
use sqlx::Row;
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

use super::PostgresSessionStore;
use crate::session_store::{RecoveryParts, SessionRecovery};
use crate::{SessionEvent, SessionEventKind};

/// The fork metadata and sequence bounds of one session row.
#[derive(Debug, Clone)]
pub(super) struct StoredSession {
    pub parent_session_id: Option<Uuid>,
    pub parent_cut_sequence: Option<u64>,
    pub inherited_event_count: u64,
    pub next_sequence: u64,
}

/// One stored event plus the flag deciding whether a fork inherits it.
struct StoredEvent {
    event: SessionEvent,
    fork_inheritable: bool,
}

/// A forked session renumbers its inherited prefix into its own space, so an
/// inherited event needs a stable id of its own rather than the parent's.
fn inherited_event_id(session_id: Uuid, source_event_id: Uuid) -> Uuid {
    Uuid::new_v5(&session_id, source_event_id.as_bytes())
}

/// SQL restricting a recovery scan to the requested slices.
///
/// The context slice spans many event kinds with no cheap column expression,
/// so it uses the `recovery_relevant` partial index unchanged. Narrower callers
/// add an `event_kind` filter on top of that index, which is what keeps the
/// multi-hundred-megabyte tool payloads out of the result set.
fn recovery_scan_predicate(parts: RecoveryParts, alias: &str) -> String {
    if parts.context {
        return format!("{alias}recovery_relevant");
    }
    let mut kinds: Vec<&str> = Vec::new();
    if parts.queue {
        kinds.push("'message'");
        kinds.push("'prompt_recalled'");
    }
    if parts.subagents {
        kinds.push("'subagent_activity'");
    }
    match kinds.as_slice() {
        [] => "false".to_string(),
        // Every subagent_activity row is recovery-relevant by construction, so
        // dropping the redundant flag lets the planner use the dedicated roster
        // index instead of walking the whole recovery index.
        ["'subagent_activity'"] => format!("{alias}event_kind = 'subagent_activity'"),
        kinds => format!(
            "{alias}recovery_relevant and {alias}event_kind in ({})",
            kinds.join(", ")
        ),
    }
}

/// Keep only the newest activity row per subagent.
///
/// A roster needs each child's latest state, not its whole history. SQLite
/// expressed this with `json_extract(...)` over the body; here the subagent id
/// is a real column, so the dedup also works for compressed rows.
const LATEST_SUBAGENT_ROWS: &str = "e.sequence in ( \
     select max(sequence) from session_events \
     where session_id = $1 and event_kind = 'subagent_activity' \
     group by subagent_session_id \
   )";

impl PostgresSessionStore {
    pub(super) async fn session_row(&self, session_id: Uuid) -> Result<StoredSession> {
        let row = sqlx::query(
            "select parent_session_id, parent_cut_sequence, inherited_event_count, \
             next_sequence from sessions where id = $1",
        )
        .bind(session_id)
        .fetch_optional(self.pool())
        .await?
        .with_context(|| format!("session {session_id} does not exist"))?;
        let parent_cut_sequence: Option<i64> = row.try_get("parent_cut_sequence")?;
        let inherited: i64 = row.try_get("inherited_event_count")?;
        let next_sequence: i64 = row.try_get("next_sequence")?;
        Ok(StoredSession {
            parent_session_id: row.try_get("parent_session_id")?,
            parent_cut_sequence: parent_cut_sequence.map(|value| u64::try_from(value).unwrap_or(0)),
            inherited_event_count: u64::try_from(inherited)
                .context("negative inherited event count")?,
            next_sequence: u64::try_from(next_sequence).context("negative next sequence")?,
        })
    }

    async fn decode_events(&self, rows: &[sqlx::postgres::PgRow]) -> Result<Vec<SessionEvent>> {
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let json: Option<serde_json::Value> = row.try_get("event_json")?;
            let bytes: Option<Vec<u8>> = row.try_get("event_body")?;
            let dict_id: Option<i32> = row.try_get("dict_id")?;
            let value = self.decode_body(json, bytes, dict_id).await?;
            events.push(serde_json::from_value(value)?);
        }
        Ok(events)
    }

    /// The recovery-relevant events a session authored itself, within bounds.
    async fn local_recovery_events(
        &self,
        session_id: Uuid,
        after: u64,
        until: u64,
        parts: RecoveryParts,
    ) -> Result<Vec<StoredEvent>> {
        let predicate = recovery_scan_predicate(parts, "e.");
        let sql = format!(
            "select e.event_json, e.event_body, e.dict_id, e.fork_inheritable \
             from session_events e \
             where e.session_id = $1 and e.sequence > $2 and e.sequence <= $3 \
               and {predicate} \
               and (e.event_kind <> 'subagent_activity' or {LATEST_SUBAGENT_ROWS}) \
             order by e.sequence"
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(session_id)
            .bind(i64::try_from(after).unwrap_or(i64::MAX))
            .bind(i64::try_from(until).unwrap_or(i64::MAX))
            .fetch_all(self.pool())
            .await?;
        let events = self.decode_events(&rows).await?;
        let mut stored = Vec::with_capacity(events.len());
        for (row, event) in rows.iter().zip(events) {
            stored.push(StoredEvent {
                event,
                fork_inheritable: row.try_get("fork_inheritable")?,
            });
        }
        Ok(stored)
    }

    /// The recovery projection's events, including any inherited fork prefix.
    ///
    /// The inherited prefix recurses through this same recovery-only path
    /// rather than through a full event read: materialising every tool payload
    /// in a parent just to discard the non-recovery rows turns resume on a
    /// large forked session into a multi-gigabyte scan.
    fn composed_recovery_events<'a>(
        &'a self,
        session_id: Uuid,
        before_or_at: Option<u64>,
        parts: RecoveryParts,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<StoredEvent>>> + Send + 'a>> {
        Box::pin(async move {
            let session = self.session_row(session_id).await?;
            let logical_limit = before_or_at.unwrap_or(session.next_sequence.saturating_sub(1));
            let inherited_limit = logical_limit.min(session.inherited_event_count);
            let mut events = match (session.parent_session_id, session.parent_cut_sequence) {
                (Some(parent), Some(cut)) => {
                    let narrowed = if inherited_limit < session.inherited_event_count {
                        // A cut inside the inherited prefix has to be counted in
                        // the parent's own terms, so the parent is asked for
                        // every slice and the narrowing is applied after.
                        RecoveryParts::ALL
                    } else {
                        parts
                    };
                    let mut inherited =
                        self.composed_recovery_events(parent, Some(cut), narrowed).await?;
                    inherited.retain(|stored| stored.fork_inheritable);
                    if inherited_limit < session.inherited_event_count {
                        inherited.truncate(usize::try_from(inherited_limit).unwrap_or(usize::MAX));
                        inherited.retain(|stored| {
                            (parts.context && stored.event.kind.is_context_relevant())
                                || (parts.queue && stored.event.kind.is_queue_relevant())
                                || (parts.subagents && stored.event.kind.is_subagent_relevant())
                        });
                    }
                    inherited
                }
                _ => Vec::new(),
            };
            if logical_limit > session.inherited_event_count {
                let mut local = self
                    .local_recovery_events(
                        session_id,
                        session.inherited_event_count,
                        logical_limit,
                        parts,
                    )
                    .await?;
                events.append(&mut local);
            }
            Ok(events)
        })
    }

    /// Recovery from a replay boundary: everything at or after it, plus the
    /// state that survives a boundary.
    ///
    /// A boundary discards superseded context, but it must not discard
    /// unresolved prompts or the latest durable state of existing subagents --
    /// those would be silently lost work and a vanished team roster.
    async fn recovery_projection_from_sequence(
        &self,
        session_id: Uuid,
        recovery_start_sequence: i64,
        resolved_message_id: Option<Uuid>,
        parts: RecoveryParts,
    ) -> Result<SessionRecovery> {
        let predicate = recovery_scan_predicate(parts, "e.");
        let suffix_sql = format!(
            "select e.event_json, e.event_body, e.dict_id from session_events e \
             where e.session_id = $1 and e.sequence >= $2 and {predicate} \
               and (e.event_kind <> 'subagent_activity' or {LATEST_SUBAGENT_ROWS}) \
             order by e.sequence"
        );
        let suffix_rows = sqlx::query(sqlx::AssertSqlSafe(suffix_sql))
            .bind(session_id)
            .bind(recovery_start_sequence)
            .fetch_all(self.pool())
            .await?;
        let suffix = self.decode_events(&suffix_rows).await?;
        let mut recovery = SessionRecovery::from_events(suffix, parts);

        if parts.queue {
            // Unresolved prompts from before the boundary. The action row is
            // the authority on whether a prompt still needs running, so the
            // join is what stops a completed prompt being replayed.
            let rows = sqlx::query(
                "select e.event_json, e.event_body, e.dict_id from session_events e \
                 left join session_actions a \
                   on a.session_id = e.session_id and a.action_id = e.message_id \
                 where e.session_id = $1 and e.sequence < $2 \
                   and e.event_kind in ('message', 'prompt_recalled') \
                   and ($3::uuid is null or e.message_id is distinct from $3::uuid) \
                   and (a.action_id is null \
                        or a.state not in ('completed', 'failed', 'cancelled')) \
                 order by e.sequence",
            )
            .bind(session_id)
            .bind(recovery_start_sequence)
            .bind(resolved_message_id)
            .fetch_all(self.pool())
            .await?;
            // The actor filter lives here rather than in SQL: it reads the
            // event body, which is opaque for a compressed row.
            let mut queue_events: Vec<SessionEvent> = self
                .decode_events(&rows)
                .await?
                .into_iter()
                .filter(|event| match &event.kind {
                    SessionEventKind::Message { actor, .. } => matches!(
                        actor,
                        crate::EventActor::User | crate::EventActor::System
                    ),
                    SessionEventKind::PromptRecalled { .. } => true,
                    _ => false,
                })
                .collect();
            queue_events.sort_unstable_by_key(|event| event.sequence);
            queue_events.append(&mut recovery.queue_events);
            recovery.queue_events = queue_events;
        }

        if parts.subagents {
            let sql = format!(
                "select e.event_json, e.event_body, e.dict_id from session_events e \
                 where e.session_id = $1 and e.sequence < $2 \
                   and e.event_kind = 'subagent_activity' and {LATEST_SUBAGENT_ROWS} \
                 order by e.sequence"
            );
            let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(session_id)
                .bind(recovery_start_sequence)
                .fetch_all(self.pool())
                .await?;
            let mut subagent_events = self.decode_events(&rows).await?;
            subagent_events.append(&mut recovery.subagent_events);
            recovery.subagent_events = subagent_events;
        }
        Ok(recovery)
    }

    /// Where recovery should start: after the newest context boundary.
    ///
    /// Two kinds of boundary exist. `context_cleared` is explicit. A completed
    /// compaction is implicit, and only counts when the provider's own context
    /// was NOT preserved (or it is an explicit recovery checkpoint) -- a
    /// compaction that preserved provider context is not a replay boundary.
    async fn recovery_boundary(&self, session_id: Uuid) -> Result<Option<i64>> {
        let context_clear_sequence: Option<i64> = sqlx::query_scalar(
            "select sequence from session_events \
             where session_id = $1 and event_kind = 'context_cleared' \
             order by sequence desc limit 1",
        )
        .bind(session_id)
        .fetch_optional(self.pool())
        .await?
        .flatten();

        // Candidates are narrowed by the lifted provider_event_kind column and
        // the qualifying predicate is applied in Rust, so an aged row is
        // examined rather than skipped.
        let mut compaction_sequence = None;
        let mut before = i64::MAX;
        'outer: loop {
            let rows = sqlx::query(
                "select sequence, event_json, event_body, dict_id from session_events \
                 where session_id = $1 and sequence < $2 \
                   and event_kind = 'provider_event' \
                   and provider_event_kind = 'context_compaction' \
                 order by sequence desc limit 32",
            )
            .bind(session_id)
            .bind(before)
            .fetch_all(self.pool())
            .await?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                let sequence: i64 = row.try_get("sequence")?;
                before = before.min(sequence);
                let event = self.decode_events(std::slice::from_ref(row)).await?;
                let event = &event[0];
                if event.kind.is_completed_context_compaction()
                    || event.kind.is_completed_provider_recovery_checkpoint()
                {
                    compaction_sequence = Some(sequence);
                    break 'outer;
                }
            }
        }

        let mut recovery_start_sequence = context_clear_sequence;
        if let Some(compaction_sequence) = compaction_sequence
            && context_clear_sequence
                .map(|clear_sequence| compaction_sequence > clear_sequence)
                .unwrap_or(true)
        {
            // Replay resumes after the last turn that actually succeeded before
            // the compaction, so an interrupted turn is retried rather than
            // assumed done.
            let successful_turn_sequence = self
                .last_successful_turn_before(session_id, compaction_sequence)
                .await?;
            if let Some(successful_turn_sequence) = successful_turn_sequence {
                let compaction_recovery_start = successful_turn_sequence.saturating_add(1);
                recovery_start_sequence = Some(
                    recovery_start_sequence
                        .map(|sequence| sequence.max(compaction_recovery_start))
                        .unwrap_or(compaction_recovery_start),
                );
            }
        }
        Ok(recovery_start_sequence)
    }

    async fn last_successful_turn_before(
        &self,
        session_id: Uuid,
        before: i64,
    ) -> Result<Option<i64>> {
        let mut cursor = before;
        loop {
            let rows = sqlx::query(
                "select sequence, event_json, event_body, dict_id from session_events \
                 where session_id = $1 and sequence < $2 and event_kind = 'turn_completed' \
                 order by sequence desc limit 32",
            )
            .bind(session_id)
            .bind(cursor)
            .fetch_all(self.pool())
            .await?;
            if rows.is_empty() {
                return Ok(None);
            }
            for row in &rows {
                let sequence: i64 = row.try_get("sequence")?;
                cursor = cursor.min(sequence);
                let decoded = self.decode_events(std::slice::from_ref(row)).await?;
                if matches!(
                    &decoded[0].kind,
                    SessionEventKind::TurnCompleted { error: None, .. }
                ) {
                    return Ok(Some(sequence));
                }
            }
        }
    }

    pub(super) async fn recovery_projection(
        &self,
        session_id: Uuid,
        parts: RecoveryParts,
    ) -> Result<SessionRecovery> {
        let session = self.session_row(session_id).await?;
        // A boundary is only a shortcut for a session that owns its whole
        // history; a fork's sequence space spans its parent's, so it takes the
        // composed path.
        if session.inherited_event_count == 0
            && let Some(recovery_start_sequence) = self.recovery_boundary(session_id).await?
        {
            return self
                .recovery_projection_from_sequence(session_id, recovery_start_sequence, None, parts)
                .await;
        }
        let events = self
            .composed_recovery_events(session_id, None, parts)
            .await?
            .into_iter()
            .map(|stored| {
                let mut event = stored.event;
                if event.session_id != session_id {
                    event.id = inherited_event_id(session_id, event.id);
                    event.session_id = session_id;
                }
                event
            })
            .collect();
        Ok(SessionRecovery::from_events(events, parts))
    }

    /// Recovery anchored on a provider's own checkpoint, when one exists.
    ///
    /// An interrupted turn still counts: the provider kept its context, so
    /// replay resumes from that boundary and the interrupted prompt is left
    /// unresolved so it runs again.
    pub(super) async fn provider_checkpoint_recovery_projection(
        &self,
        session_id: Uuid,
        provider_session_id: &str,
    ) -> Result<Option<SessionRecovery>> {
        if self.session_row(session_id).await?.inherited_event_count != 0 {
            return Ok(None);
        }
        let mut cursor = i64::MAX;
        loop {
            let rows = sqlx::query(
                "select sequence, event_json, event_body, dict_id from session_events \
                 where session_id = $1 and sequence < $2 and event_kind = 'turn_completed' \
                 order by sequence desc limit 32",
            )
            .bind(session_id)
            .bind(cursor)
            .fetch_all(self.pool())
            .await?;
            if rows.is_empty() {
                return Ok(None);
            }
            for row in &rows {
                let sequence: i64 = row.try_get("sequence")?;
                cursor = cursor.min(sequence);
                let decoded = self.decode_events(std::slice::from_ref(row)).await?;
                let SessionEventKind::TurnCompleted {
                    message_id,
                    provider_session_id: checkpoint_session,
                    error,
                    ..
                } = &decoded[0].kind
                else {
                    continue;
                };
                if checkpoint_session.as_deref() != Some(provider_session_id) {
                    continue;
                }
                if !matches!(error.as_deref(), None | Some("turn interrupted")) {
                    continue;
                }
                let message_id = *message_id;
                return Ok(Some(
                    self.recovery_projection_from_sequence(
                        session_id,
                        sequence,
                        Some(message_id),
                        RecoveryParts::ALL,
                    )
                    .await?,
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::testing::{ScratchDatabase, test_url};
    use super::*;
    use crate::session_store::SessionStore;
    use crate::{CodingProvider, EventActor, MessageStatus, PromptDelivery};

    async fn started_session(url: &str) -> (ScratchDatabase, PostgresSessionStore, Uuid) {
        let scratch = ScratchDatabase::create(url).await;
        let store = PostgresSessionStore::connect(&scratch.url)
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
        (scratch, store, session_id)
    }

    async fn user_message(store: &PostgresSessionStore, session_id: Uuid, text: &str) -> Uuid {
        let message_id = Uuid::new_v4();
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::Message {
                    message_id,
                    actor: EventActor::User,
                    text: text.to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Queued,
                    delivery: Some(PromptDelivery::Queue),
                },
            ))
            .await
            .expect("message");
        message_id
    }

    /// A completed assistant reply: the kind of event that is context-relevant.
    /// A queued user prompt is queue-relevant but not yet part of context.
    async fn assistant_message(store: &PostgresSessionStore, session_id: Uuid, text: &str) {
        store
            .append(SessionEvent::new(
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
            ))
            .await
            .expect("assistant message");
    }

    async fn complete_turn(store: &PostgresSessionStore, session_id: Uuid, message_id: Uuid) {
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::TurnCompleted {
                    message_id,
                    provider_session_id: Some("provider-session-1".to_string()),
                    final_text: "ok".to_string(),
                    error: None,
                },
            ))
            .await
            .expect("turn completed");
    }

    #[tokio::test]
    async fn recovery_rebuilds_context_queue_and_roster_slices() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, session_id) = started_session(&url).await;
        let answered = user_message(&store, session_id, "first").await;
        complete_turn(&store, session_id, answered).await;
        let pending = user_message(&store, session_id, "still waiting").await;

        let recovery = store.recovery(session_id).await.expect("recovery");
        assert!(
            !recovery.context_events.is_empty(),
            "context slice must carry the conversation"
        );
        let queue_ids: Vec<Uuid> = recovery
            .queue_events
            .iter()
            .filter_map(|event| match &event.kind {
                SessionEventKind::Message { message_id, .. } => Some(*message_id),
                _ => None,
            })
            .collect();
        assert!(
            queue_ids.contains(&pending),
            "an unanswered prompt must survive recovery"
        );

        // Narrowing changes which slices are populated, never which events a
        // slice contains.
        let queue_only = store
            .recovery_parts(session_id, RecoveryParts::QUEUE)
            .await
            .expect("queue recovery");
        assert!(queue_only.context_events.is_empty());
        assert!(queue_only.subagent_events.is_empty());
        assert_eq!(
            queue_only
                .queue_events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            recovery
                .queue_events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            "a narrowed queue slice must match the full projection's queue slice"
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn clearing_context_discards_earlier_context_but_not_pending_prompts() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, session_id) = started_session(&url).await;
        let old = user_message(&store, session_id, "ancient history").await;
        assistant_message(&store, session_id, "ancient history").await;
        complete_turn(&store, session_id, old).await;
        let unresolved = user_message(&store, session_id, "never ran").await;

        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ContextCleared,
            ))
            .await
            .expect("context cleared");
        let after = user_message(&store, session_id, "after the boundary").await;

        let recovery = store.recovery(session_id).await.expect("recovery");
        let context_text: Vec<String> = recovery
            .context_events
            .iter()
            .filter_map(|event| match &event.kind {
                SessionEventKind::Message { text, .. } => Some(text.clone()),
                _ => None,
            })
            .collect();
        assert!(
            !context_text.iter().any(|text| text == "ancient history"),
            "a cleared boundary must drop superseded context: {context_text:?}"
        );

        // A boundary discards context, never outstanding work.
        let queue_ids: Vec<Uuid> = recovery
            .queue_events
            .iter()
            .filter_map(|event| match &event.kind {
                SessionEventKind::Message { message_id, .. } => Some(*message_id),
                _ => None,
            })
            .collect();
        assert!(
            queue_ids.contains(&unresolved),
            "an unresolved prompt from before the boundary must survive"
        );
        assert!(queue_ids.contains(&after));
        scratch.discard().await;
    }

    #[tokio::test]
    async fn a_completed_prompt_is_not_replayed_across_a_boundary() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, session_id) = started_session(&url).await;
        let answered = user_message(&store, session_id, "already handled").await;
        complete_turn(&store, session_id, answered).await;
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ContextCleared,
            ))
            .await
            .expect("context cleared");

        let recovery = store.recovery(session_id).await.expect("recovery");
        let queue_ids: Vec<Uuid> = recovery
            .queue_events
            .iter()
            .filter_map(|event| match &event.kind {
                SessionEventKind::Message { message_id, .. } => Some(*message_id),
                _ => None,
            })
            .collect();
        assert!(
            !queue_ids.contains(&answered),
            "a completed prompt must not be re-queued by recovery"
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn recovery_resumes_from_a_provider_checkpoint_when_one_matches() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, session_id) = started_session(&url).await;
        let first = user_message(&store, session_id, "first").await;
        complete_turn(&store, session_id, first).await;

        assert!(
            store
                .recovery_from_provider_checkpoint(session_id, "no-such-provider-session")
                .await
                .expect("checkpoint recovery")
                .is_none(),
            "an unknown provider session has no checkpoint to resume from"
        );
        let recovery = store
            .recovery_from_provider_checkpoint(session_id, "provider-session-1")
            .await
            .expect("checkpoint recovery")
            .expect("a matching checkpoint must be found");
        // The checkpoint's own prompt is resolved, so it must not be replayed.
        let queue_ids: Vec<Uuid> = recovery
            .queue_events
            .iter()
            .filter_map(|event| match &event.kind {
                SessionEventKind::Message { message_id, .. } => Some(*message_id),
                _ => None,
            })
            .collect();
        assert!(!queue_ids.contains(&first));
        scratch.discard().await;
    }

    #[tokio::test]
    async fn a_compaction_that_preserved_provider_context_is_not_a_replay_boundary() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, session_id) = started_session(&url).await;
        let early = user_message(&store, session_id, "before compaction").await;
        assistant_message(&store, session_id, "before compaction").await;
        complete_turn(&store, session_id, early).await;
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ProviderEvent {
                    provider: CodingProvider::Claude,
                    kind: "context_compaction".to_string(),
                    payload: serde_json::json!({
                        "status": "completed",
                        "provider_context_preserved": true,
                    }),
                },
            ))
            .await
            .expect("compaction");

        // The provider still holds this context, so replay must not restart
        // after the compaction and lose it.
        let recovery = store.recovery(session_id).await.expect("recovery");
        let context_text: Vec<String> = recovery
            .context_events
            .iter()
            .filter_map(|event| match &event.kind {
                SessionEventKind::Message { text, .. } => Some(text.clone()),
                _ => None,
            })
            .collect();
        assert!(
            context_text.iter().any(|text| text == "before compaction"),
            "a preserved-context compaction must not discard history: {context_text:?}"
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn recovery_reads_aged_history_that_sql_predicates_cannot_see() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let (scratch, store, session_id) = started_session(&url).await;
        let answered = user_message(&store, session_id, "compressed history").await;
        assistant_message(&store, session_id, "compressed reply").await;
        complete_turn(&store, session_id, answered).await;
        let pending = user_message(&store, session_id, "compressed and pending").await;

        // Age the whole session into the cold tier, then recover from it. This
        // is why the body predicates live in Rust: a jsonb filter would return
        // nothing here and recovery would silently lose the session.
        sqlx::query("update sessions set updated_at = now() - interval '30 days' where id = $1")
            .bind(session_id)
            .execute(store.pool())
            .await
            .expect("age the session");
        let cutoff = chrono::Utc::now() - chrono::Duration::days(7);
        let outcome = store.age_cold_sessions(cutoff, 8).await.expect("age");
        assert!(outcome.events_compressed > 0, "{outcome:?}");

        let recovery = store.recovery(session_id).await.expect("recovery");
        let context_text: Vec<String> = recovery
            .context_events
            .iter()
            .filter_map(|event| match &event.kind {
                SessionEventKind::Message { text, .. } => Some(text.clone()),
                _ => None,
            })
            .collect();
        assert!(
            context_text.iter().any(|text| text == "compressed reply"),
            "recovery must decode cold bodies: {context_text:?}"
        );
        let queue_ids: Vec<Uuid> = recovery
            .queue_events
            .iter()
            .filter_map(|event| match &event.kind {
                SessionEventKind::Message { message_id, .. } => Some(*message_id),
                _ => None,
            })
            .collect();
        assert!(
            queue_ids.contains(&pending),
            "an unresolved prompt must survive compression"
        );
        scratch.discard().await;
    }
}
