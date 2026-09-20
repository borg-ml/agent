//! `SessionStore` on PostgreSQL.
//!
//! The one structural difference from the SQLite implementation is where
//! writers serialise. SQLite takes a database-wide write lock, so every agent
//! on the machine queues behind every other agent. Here the durable append
//! takes a row lock on its own `sessions` row -- `select ... for update` --
//! so two agents writing to two sessions never contend at all, which is the
//! entire reason for this backend.
//!
//! Reads go through `decode_body`, so whether a row is in the hot jsonb tier
//! or the cold compressed tier is invisible above this layer.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

use super::PostgresSessionStore;
#[cfg(any(feature = "subscription-adapters", test))]
use crate::CodingProvider;
use crate::session_store::{
    ClaimedActionTransition, EventPersistence, INLINE_SESSION_PAYLOAD_BYTES, RawSessionEvent,
    RuntimeCheckpoint, RuntimeManifest, RuntimeManifestActivation, SessionAction,
    SessionActionState, SessionActionTransition, SessionEvent, SessionEventKind,
    SessionHistoryIndexDocument, SessionHistoryPage, SessionHistoryQuery, SessionHistorySearchMode,
    SessionLineage, SessionLiveEvent, SessionPayloadKind, SessionPayloadRef, SessionRecovery,
    SessionState, SessionStatus, SessionStore, SessionStoreCompaction, SessionStoreFork,
    SessionStoreHealth, SessionSummary, SessionWorkspaceBinding, deferred_json_payload,
    deferred_text_payload, event_kind, historical_projection_json,
};

impl PostgresSessionStore {
    /// Move oversized tool inputs, tool outputs and provider prompts out of the
    /// event body and into `session_payloads`, replacing them with references.
    ///
    /// Mirrors the SQLite path exactly, including recursing into a subagent's
    /// nested event, so an event written by either backend carries the same
    /// deferred-payload shape.
    fn compact_payloads<'a>(
        &'a self,
        transaction: &'a mut Transaction<'_, Postgres>,
        event: &'a SessionEvent,
    ) -> Pin<Box<dyn Future<Output = Result<SessionEvent>> + Send + 'a>> {
        Box::pin(async move {
            let mut compact = event.clone();
            match &mut compact.kind {
                SessionEventKind::ToolStarted {
                    input, input_ref, ..
                } if input_ref.is_none() => {
                    let bytes = serde_json::to_vec(input)?;
                    if bytes.len() > INLINE_SESSION_PAYLOAD_BYTES {
                        let payload = store_payload(
                            transaction,
                            event,
                            SessionPayloadKind::ToolInput,
                            &bytes,
                        )
                        .await?;
                        *input = deferred_json_payload(&payload);
                        *input_ref = Some(payload);
                    }
                }
                SessionEventKind::ToolCompleted {
                    output,
                    output_ref,
                    input,
                    input_ref,
                    ..
                } => {
                    if output_ref.is_none() && output.len() > INLINE_SESSION_PAYLOAD_BYTES {
                        let payload = store_payload(
                            transaction,
                            event,
                            SessionPayloadKind::ToolOutput,
                            output.as_bytes(),
                        )
                        .await?;
                        *output = deferred_text_payload(output, &payload);
                        *output_ref = Some(payload);
                    }
                    if input_ref.is_none()
                        && let Some(value) = input
                    {
                        let bytes = serde_json::to_vec(value)?;
                        if bytes.len() > INLINE_SESSION_PAYLOAD_BYTES {
                            let payload = store_payload(
                                transaction,
                                event,
                                SessionPayloadKind::ToolResultInput,
                                &bytes,
                            )
                            .await?;
                            *value = deferred_json_payload(&payload);
                            *input_ref = Some(payload);
                        }
                    }
                }
                SessionEventKind::ProviderEvent { kind, payload, .. }
                    if kind == crate::PROVIDER_PROMPT_EVENT_KIND =>
                {
                    let prompt = payload
                        .get(crate::PROVIDER_PROMPT_FIELD)
                        .and_then(serde_json::Value::as_str)
                        .filter(|prompt| prompt.len() > INLINE_SESSION_PAYLOAD_BYTES)
                        .map(str::to_string);
                    if let Some(prompt) = prompt {
                        let reference = store_payload(
                            transaction,
                            event,
                            SessionPayloadKind::ProviderPrompt,
                            prompt.as_bytes(),
                        )
                        .await?;
                        payload[crate::PROVIDER_PROMPT_FIELD] =
                            serde_json::Value::String(deferred_text_payload(&prompt, &reference));
                        payload[crate::PROVIDER_PROMPT_REF_FIELD] =
                            serde_json::to_value(&reference)?;
                    }
                }
                SessionEventKind::SubagentActivity {
                    event: Some(child_event),
                    ..
                } => {
                    **child_event = self.compact_payloads(transaction, child_event).await?;
                }
                _ => {}
            }
            Ok(compact)
        })
    }

    /// Page backwards over a session's message rows, newest first.
    ///
    /// WHY NOT A SQL PREDICATE: the SQLite store filters on actor and status
    /// with `json_extract` in the query. That is impossible here for a COLD
    /// row, whose body is compressed bytes the planner cannot read -- a jsonb
    /// predicate would silently skip every aged session, and the tail of an
    /// aged session is exactly what these callers want. So the index narrows to
    /// message rows (`idx_session_events_message_sequence`, partial on
    /// `event_kind = 'message'` and already ordered by descending sequence) and
    /// the predicate runs in Rust over decoded bodies.
    ///
    /// Bounded work: it stops as soon as `limit` matches are found, so a chatty
    /// session costs one page rather than a full read.
    async fn recent_messages_matching(
        &self,
        session_id: Uuid,
        after_sequence: u64,
        limit: usize,
        matches: impl Fn(&SessionEvent) -> bool,
    ) -> Result<Vec<SessionEvent>> {
        const PAGE: i64 = 256;
        let mut found: Vec<SessionEvent> = Vec::new();
        let mut before = i64::MAX;
        loop {
            let rows = sqlx::query(
                "select sequence, event_json, event_body, dict_id from session_events \
                 where session_id = $1 and event_kind = 'message' \
                   and sequence > $2 and sequence < $3 \
                 order by sequence desc limit $4",
            )
            .bind(session_id)
            .bind(i64::try_from(after_sequence).unwrap_or(i64::MAX))
            .bind(before)
            .bind(PAGE)
            .fetch_all(self.pool())
            .await?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                before = before.min(row.try_get::<i64, _>("sequence")?);
                let body = match row.try_get::<Option<serde_json::Value>, _>("event_json")? {
                    Some(body) => body,
                    None => {
                        let bytes: Option<Vec<u8>> = row.try_get("event_body")?;
                        let dict_id: Option<i32> = row.try_get("dict_id")?;
                        let dictionary = match dict_id {
                            Some(dict_id) => Some(self.dictionary(dict_id).await?),
                            None => None,
                        };
                        let dictionary = dictionary.map(|dictionary| (*dictionary).clone());
                        serde_json::from_slice(&super::body::decompress(
                            &bytes.unwrap_or_default(),
                            dictionary.as_ref(),
                        )?)?
                    }
                };
                let event: SessionEvent = serde_json::from_value(body)?;
                if matches(&event) {
                    found.push(event);
                    if found.len() >= limit {
                        found.reverse();
                        return Ok(found);
                    }
                }
            }
        }
        found.reverse();
        Ok(found)
    }

    /// Append one durable event under a per-session row lock.
    async fn append_durable(&self, event: SessionEvent) -> Result<SessionEvent> {
        let mut transaction = self.pool().begin().await?;
        let stored = self
            .append_durable_in_transaction(&mut transaction, event)
            .await?;
        transaction.commit().await?;
        Ok(stored)
    }

    /// Append one durable event inside a caller-owned transaction.
    ///
    /// Import needs several events to land atomically, so the transaction
    /// boundary belongs to the caller rather than to each append.
    pub(super) async fn append_durable_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        mut event: SessionEvent,
    ) -> Result<SessionEvent> {
        // `for update` is the whole design: it serialises writers for THIS
        // session and no other. Two agents appending to two sessions proceed
        // in parallel, which is what SQLite's single file writer made
        // impossible.
        let row = sqlx::query(
            "select inherited_event_count, next_sequence, state_json from sessions \
             where id = $1 for update",
        )
        .bind(event.session_id)
        .fetch_optional(&mut **transaction)
        .await?
        .with_context(|| format!("session {} does not exist", event.session_id))?;

        let inherited_event_count = u64::try_from(row.try_get::<i64, _>("inherited_event_count")?)
            .context("negative inherited event count")?;
        let next_sequence = u64::try_from(row.try_get::<i64, _>("next_sequence")?)
            .context("negative session sequence")?;
        if event.sequence == 0 {
            event.sequence = next_sequence;
        }
        anyhow::ensure!(
            event.sequence == next_sequence,
            "session event sequence must be {next_sequence}, received {}",
            event.sequence
        );

        let mut state: SessionState = serde_json::from_str(row.try_get("state_json")?)?;
        state.apply(&event)?;
        let stored_event_kind = event_kind(&event.kind)?;
        let compact_event = self.compact_payloads(transaction, &event).await?;
        // POSTGRES CANNOT PUT \u0000 IN JSONB. `jsonb` parses to a normalised
        // tree whose strings are Postgres `text`, and text cannot contain a NUL
        // -- the server rejects the whole insert with "unsupported Unicode
        // escape sequence". SQLite's JSON is just text, so it accepts these
        // happily; 246 events in the journal this was measured against carry
        // one, almost all of them tool output that captured a binary byte.
        //
        // Dropping or replacing the NUL would corrupt the evidence a journal
        // exists to preserve, so such an event goes to the `event_body` bytea
        // tier instead, uncompressed, with a null `dict_id`. The schema already
        // allows either column per row and every read already handles both, so
        // this costs one branch rather than a new mechanism. The trade is that
        // these rows are opaque to ad-hoc SQL -- exactly the documented trade
        // for any cold row.
        let body_value = serde_json::to_value(&compact_event)?;
        let body_text = serde_json::to_string(&compact_event)?;
        let nul_bearing = body_text.contains("\\u0000");
        let body = if nul_bearing { None } else { Some(body_value) };
        let body_bytes = nul_bearing.then(|| body_text.into_bytes());
        let projection_json = serde_json::to_string(&state)?;
        // Only checkpoint rows carry a projection; the rest replay forward from
        // the newest one. Identical policy to SQLite, and the single largest
        // reason this journal is smaller than its predecessor.
        let historical_projection =
            historical_projection_json(event.sequence, inherited_event_count, &projection_json);
        let message_id = match &event.kind {
            SessionEventKind::Message { message_id, .. } => Some(*message_id),
            _ => None,
        };

        sqlx::query(
            "insert into session_events \
             (session_id, sequence, event_id, event_kind, event_json, event_body, \
              projection_json, fork_inheritable, recovery_relevant, message_id, created_at, \
              subagent_session_id, provider_event_kind) \
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
        )
        .bind(event.session_id)
        .bind(i64::try_from(event.sequence).context("session sequence exceeds a bigint")?)
        .bind(event.id)
        .bind(&stored_event_kind)
        .bind(&body)
        .bind(body_bytes.as_deref())
        .bind(historical_projection)
        .bind(event.kind.is_fork_inheritable())
        .bind(event.kind.is_recovery_relevant())
        .bind(message_id)
        .bind(event.created_at)
        // Lifted out of the body at write time: a compressed body is opaque to
        // the planner, so these predicates cannot be expressions over it.
        .bind(subagent_session_id(&compact_event.kind))
        .bind(provider_event_kind(&compact_event.kind))
        .execute(&mut **transaction)
        .await?;

        // The action queue and the event proving its admission or terminal
        // boundary commit in one transaction, exactly as in SQLite. Splitting
        // them would let a prompt exist with no work to run, or work with no
        // record of why it started.
        super::sync::sync_session_action(transaction, &event).await?;

        if event.kind.clears_live_turn_state() {
            sqlx::query(
                "delete from session_live_state \
                 where session_id = $1 and live_key <> 'context_window'",
            )
            .bind(event.session_id)
            .execute(&mut **transaction)
            .await?;
        } else {
            for live_key in event.kind.cleared_live_state_keys() {
                sqlx::query(
                    "delete from session_live_state where session_id = $1 and live_key = $2",
                )
                .bind(event.session_id)
                .bind(live_key)
                .execute(&mut **transaction)
                .await?;
            }
        }

        sqlx::query(
            "update sessions set next_sequence = $1, state_json = $2, projection_version = 3, \
             updated_at = $3 where id = $4",
        )
        .bind(i64::try_from(event.sequence.saturating_add(1)).unwrap_or(i64::MAX))
        .bind(projection_json)
        .bind(event.created_at)
        .bind(event.session_id)
        .execute(&mut **transaction)
        .await?;
        Ok(compact_event)
    }

    /// Store one coalesced live snapshot, replacing any previous value for its
    /// live key. Live state is a rebuildable view of an in-flight turn, so it
    /// is written outside the durable event sequence and carries no sequence.
    async fn append_live(&self, mut event: SessionEvent) -> Result<SessionEvent> {
        let live_key = event
            .kind
            .live_state_key()
            .context("coalesced session event has no live-state key")?;
        event.sequence = 0;
        let mut transaction = self.pool().begin().await?;
        let row =
            sqlx::query("select live_revision, state_json from sessions where id = $1 for update")
                .bind(event.session_id)
                .fetch_optional(&mut *transaction)
                .await?
                .with_context(|| format!("session {} does not exist", event.session_id))?;

        for key in event.kind.cleared_live_state_keys() {
            sqlx::query("delete from session_live_state where session_id = $1 and live_key = $2")
                .bind(event.session_id)
                .bind(key)
                .execute(&mut *transaction)
                .await?;
        }

        let current_revision: i64 = row.try_get("live_revision")?;
        let state: SessionState = serde_json::from_str(row.try_get("state_json")?)?;
        let turn_live_allowed = matches!(
            state.status,
            Some(SessionStatus::Running | SessionStatus::WaitingForApproval)
        );
        if !turn_live_allowed {
            // A provider task can finish after the actor published its terminal
            // status. Do not let that delayed snapshot resurrect a responding
            // message or reasoning disclosure in an idle session.
            sqlx::query(
                "delete from session_live_state \
                 where session_id = $1 and live_key <> 'context_window'",
            )
            .bind(event.session_id)
            .execute(&mut *transaction)
            .await?;
            if live_key != "context_window" {
                transaction.commit().await?;
                return Ok(event);
            }
        }

        let revision = current_revision.saturating_add(1);
        let stored_event = if let SessionEventKind::ReasoningDelta { text } = &event.kind {
            let prior: Option<serde_json::Value> = sqlx::query_scalar(
                "select event_json from session_live_state \
                 where session_id = $1 and live_key = $2",
            )
            .bind(event.session_id)
            .bind(&live_key)
            .fetch_optional(&mut *transaction)
            .await?;
            let prior = prior
                .map(serde_json::from_value::<SessionEvent>)
                .transpose()?;
            let mut accumulated = prior
                .as_ref()
                .and_then(|event| match &event.kind {
                    SessionEventKind::ReasoningDelta { text } => Some(text.clone()),
                    _ => None,
                })
                .unwrap_or_default();
            if text.starts_with(accumulated.as_str()) {
                accumulated.clear();
                accumulated.push_str(text);
            } else if !accumulated.starts_with(text) {
                accumulated.push_str(text);
            }
            // The live key is one logical action: keep its identity and start
            // time and replace only the accumulated payload, or clients
            // rebuilding from live state see the timer restart on every frame.
            let mut snapshot = prior.unwrap_or_else(|| event.clone());
            snapshot.kind = SessionEventKind::ReasoningDelta { text: accumulated };
            snapshot
        } else {
            event.clone()
        };

        sqlx::query(
            "insert into session_live_state \
             (session_id, live_key, revision, event_json, updated_at) \
             values ($1, $2, $3, $4, $5) \
             on conflict (session_id, live_key) do update set \
             revision = excluded.revision, event_json = excluded.event_json, \
             updated_at = excluded.updated_at",
        )
        .bind(event.session_id)
        .bind(&live_key)
        .bind(revision)
        .bind(serde_json::to_value(&stored_event)?)
        .bind(event.created_at)
        .execute(&mut *transaction)
        .await?;
        sqlx::query("update sessions set live_revision = $1, updated_at = $2 where id = $3")
            .bind(revision)
            .bind(event.created_at)
            .bind(event.session_id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        // The COALESCED snapshot, not the frame that was passed in. A
        // continuation keeps the identity and start time of the key it extends,
        // so returning the caller's fresh event would hand back an id and a
        // timestamp that exist nowhere in the store -- and a client rendering
        // the return value would see the turn's timer restart on every frame,
        // which is the exact effect the coalescing above exists to prevent.
        Ok(stored_event)
    }

    /// Decode one `session_events` row into an event, hot or cold.
    async fn event_from_row(&self, row: &sqlx::postgres::PgRow) -> Result<SessionEvent> {
        let json: Option<serde_json::Value> = row.try_get("event_json")?;
        let bytes: Option<Vec<u8>> = row.try_get("event_body")?;
        let dict_id: Option<i32> = row.try_get("dict_id")?;
        let value = self.decode_body(json, bytes, dict_id).await?;
        serde_json::from_value(value).context("stored session event is not a SessionEvent")
    }

    async fn events_query(
        &self,
        session_id: Uuid,
        after: i64,
        limit: Option<i64>,
    ) -> Result<Vec<SessionEvent>> {
        let rows = sqlx::query(
            "select event_json, event_body, dict_id from session_events \
             where session_id = $1 and sequence > $2 \
             order by sequence limit $3",
        )
        .bind(session_id)
        .bind(after)
        .bind(limit.unwrap_or(i64::MAX))
        .fetch_all(self.pool())
        .await?;
        let mut events = Vec::with_capacity(rows.len());
        for row in &rows {
            events.push(self.event_from_row(row).await?);
        }
        Ok(events)
    }
}

/// Persist one oversized payload beside its event.
///
/// The id is derived from the event id and payload kind, so a retried write
/// stores the same payload once rather than accumulating duplicates.
async fn store_payload(
    transaction: &mut Transaction<'_, Postgres>,
    event: &SessionEvent,
    kind: SessionPayloadKind,
    bytes: &[u8],
) -> Result<SessionPayloadRef> {
    let id = Uuid::new_v5(&event.id, kind.as_str().as_bytes());
    let payload = SessionPayloadRef {
        id,
        kind,
        byte_len: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
    };
    sqlx::query(
        "insert into session_payloads \
         (id, session_id, event_id, payload_kind, payload, byte_len, created_at) \
         values ($1, $2, $3, $4, $5, $6, $7) on conflict (id) do nothing",
    )
    .bind(id)
    .bind(event.session_id)
    .bind(event.id)
    .bind(kind.as_str())
    .bind(bytes)
    .bind(i64::try_from(bytes.len()).context("session payload exceeds a bigint")?)
    .bind(event.created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(payload)
}

/// The subagent a `subagent_activity` event belongs to, lifted into its own
/// column so recovery can find it without opening the body.
fn subagent_session_id(kind: &SessionEventKind) -> Option<Uuid> {
    match kind {
        SessionEventKind::SubagentActivity { agent, .. } => Some(agent.session_id),
        _ => None,
    }
}

/// The provider-specific discriminant of a `provider_event`, lifted out for
/// the same reason.
fn provider_event_kind(kind: &SessionEventKind) -> Option<String> {
    match kind {
        SessionEventKind::ProviderEvent { kind, .. } => Some(kind.clone()),
        _ => None,
    }
}

#[async_trait]
/// Lineage-aware message lookup.
///
/// A session does not only contain the events it appended: a fork contains the
/// prefix it inherited, whose rows belong to the parent. Answering from one
/// session's rows therefore tells a fork that a message it demonstrably carries
/// is absent, and the callers that ask are deciding whether a prompt has already
/// been admitted -- so a false negative admits it twice.
impl PostgresSessionStore {
    /// `inherited_only` restricts the search to what a descendant actually
    /// inherits. A session owns every event it appended, but a fork inherits
    /// only the fork-inheritable ones, so a queue entry left below the cut is
    /// not part of the fork's history.
    fn contains_message_in<'a>(
        &'a self,
        session_id: Uuid,
        message_id: Uuid,
        before_or_at: Option<u64>,
        inherited_only: bool,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>> {
        Box::pin(async move {
            let session = self.session_row(session_id).await?;
            let limit = before_or_at.unwrap_or(session.next_sequence.saturating_sub(1));
            let found: bool = sqlx::query_scalar(
                "select exists(select 1 from session_events \
                 where session_id = $1 and message_id = $2 and sequence <= $3 \
                   and (not $4 or fork_inheritable))",
            )
            .bind(session_id)
            .bind(message_id)
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .bind(inherited_only)
            .fetch_one(self.pool())
            .await?;
            if found {
                return Ok(true);
            }
            let inherited_limit = limit.min(session.inherited_event_count);
            if inherited_limit == 0 {
                return Ok(false);
            }
            let (Some(parent), Some(cut)) =
                (session.parent_session_id, session.parent_cut_sequence)
            else {
                return Ok(false);
            };
            // A cut inside the inherited prefix is uncommon; resolve that
            // bounded prefix through the composed view rather than guessing a
            // sequence bound in the parent's space, which is a different
            // numbering from this session's.
            if inherited_limit < session.inherited_event_count {
                return Ok(self
                    .composed_events(session_id, Some(inherited_limit))
                    .await?
                    .iter()
                    .any(|event| {
                        matches!(
                            event.kind,
                            SessionEventKind::Message {
                                message_id: existing,
                                ..
                            } if existing == message_id
                        )
                    }));
            }
            self.contains_message_in(parent, message_id, Some(cut), true)
                .await
        })
    }
}

impl PostgresSessionStore {
    /// The newest completed compaction visible to this session, including one
    /// it inherited.
    ///
    /// A fork's context boundary is usually its PARENT's: forking does not
    /// re-compact, it carries the prefix across. Reading only this session's own
    /// rows therefore reports no boundary at all for a fresh fork, and a caller
    /// that resumes it replays from the beginning of a conversation that was
    /// already compacted.
    ///
    /// The inherited event is re-projected into this session's identity --
    /// derived id, this session_id, the sequence the fork gave it -- because a
    /// caller comparing the checkpoint against this session's own events must
    /// see one numbering, not the parent's.
    fn latest_completed_context_compaction_before<'a>(
        &'a self,
        session_id: Uuid,
        before_or_at: Option<u64>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<SessionEvent>>> + Send + 'a>> {
        Box::pin(async move {
            let session = self.session_row(session_id).await?;
            let logical_limit = before_or_at.unwrap_or(session.next_sequence.saturating_sub(1));
            if logical_limit > session.inherited_event_count {
                // Candidates are narrowed by the lifted `provider_event_kind`
                // column, served by idx_session_events_context_compaction. The
                // remaining predicate is evaluated in Rust rather than as a
                // jsonb expression, because a cold row's body is opaque to SQL
                // and a jsonb filter would silently skip aged history.
                //
                // Paged rather than limited to a fixed window: a run of
                // superseded or in-progress compactions must not hide the
                // completed one beneath it.
                let floor = i64::try_from(session.inherited_event_count).unwrap_or(i64::MAX);
                let mut before =
                    i64::try_from(logical_limit.saturating_add(1)).unwrap_or(i64::MAX);
                loop {
                    let rows = sqlx::query(
                        "select sequence, event_json, event_body, dict_id from session_events \
                         where session_id = $1 and sequence > $2 and sequence < $3 \
                           and event_kind = 'provider_event' \
                           and provider_event_kind = 'context_compaction' \
                         order by sequence desc limit 32",
                    )
                    .bind(session_id)
                    .bind(floor)
                    .bind(before)
                    .fetch_all(self.pool())
                    .await?;
                    if rows.is_empty() {
                        break;
                    }
                    for row in &rows {
                        let sequence: i64 = row.try_get("sequence")?;
                        before = before.min(sequence);
                        let event = self.event_from_row(row).await?;
                        if event.kind.is_completed_context_compaction() {
                            return Ok(Some(event));
                        }
                    }
                }
            }

            let (Some(parent_session_id), Some(parent_cut_sequence)) =
                (session.parent_session_id, session.parent_cut_sequence)
            else {
                return Ok(None);
            };
            let Some(mut event) = self
                .latest_completed_context_compaction_before(
                    parent_session_id,
                    Some(parent_cut_sequence),
                )
                .await?
            else {
                return Ok(None);
            };
            let (sequence, _) = self
                .fork_projection(parent_session_id, event.sequence.saturating_add(1))
                .await?;
            if sequence == 0 || sequence > logical_limit.min(session.inherited_event_count) {
                return Ok(None);
            }
            event.id = super::fork::inherited_event_id(session_id, event.id);
            event.session_id = session_id;
            event.sequence = sequence;
            Ok(Some(event))
        })
    }
}

impl SessionStore for PostgresSessionStore {
    async fn create_session(&self, session_id: Uuid) -> Result<()> {
        let now = Utc::now();
        let mut transaction = self.pool().begin().await?;
        sqlx::query(
            "insert into sessions (id, state_json, projection_version, created_at, updated_at) \
             values ($1, $2, 3, $3, $3) on conflict (id) do nothing",
        )
        .bind(session_id)
        .bind(serde_json::to_string(&SessionState::default())?)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "insert into session_workspace_bindings \
             (session_id, workspace_id, participant_id, attached_at) \
             values ($1, $1, $1, $2) on conflict (session_id) do nothing",
        )
        .bind(session_id)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn append(&self, mut event: SessionEvent) -> Result<SessionEvent> {
        match event.kind.persistence() {
            EventPersistence::Ephemeral => {
                event.sequence = 0;
                Ok(event)
            }
            EventPersistence::Coalesced => self.append_live(event).await,
            EventPersistence::Durable => self.append_durable(event).await,
        }
    }

    async fn read(&self, session_id: Uuid) -> Result<Vec<SessionEvent>> {
        // A session that owns its whole history reads straight from its own
        // rows; only a fork pays for composing an inherited prefix.
        if self
            .session_row(session_id)
            .await?
            .parent_session_id
            .is_none()
        {
            return self.events_query(session_id, 0, None).await;
        }
        self.composed_events(session_id, None).await
    }

    async fn events_after(
        &self,
        session_id: Uuid,
        sequence: u64,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        if self
            .session_row(session_id)
            .await?
            .parent_session_id
            .is_none()
        {
            return self
                .events_query(
                    session_id,
                    i64::try_from(sequence).unwrap_or(i64::MAX),
                    Some(i64::try_from(limit).unwrap_or(i64::MAX)),
                )
                .await;
        }
        Ok(self
            .composed_events(session_id, None)
            .await?
            .into_iter()
            .filter(|event| event.sequence > sequence)
            .take(limit)
            .collect())
    }

    async fn latest_completed_context_compaction(
        &self,
        session_id: Uuid,
    ) -> Result<Option<SessionEvent>> {
        self.latest_completed_context_compaction_before(session_id, None)
            .await
    }

    async fn state(&self, session_id: Uuid) -> Result<SessionState> {
        let state_json: Option<String> =
            sqlx::query_scalar("select state_json from sessions where id = $1")
                .bind(session_id)
                .fetch_optional(self.pool())
                .await?;
        let Some(state_json) = state_json else {
            return Ok(SessionState::default());
        };
        Ok(serde_json::from_str(&state_json)?)
    }

    async fn inherited_event_count(&self, session_id: Uuid) -> Result<u64> {
        let inherited: Option<i64> =
            sqlx::query_scalar("select inherited_event_count from sessions where id = $1")
                .bind(session_id)
                .fetch_optional(self.pool())
                .await?;
        Ok(u64::try_from(inherited.unwrap_or(0)).unwrap_or(0))
    }

    async fn live_events_after(
        &self,
        session_id: Uuid,
        revision: u64,
    ) -> Result<Vec<SessionLiveEvent>> {
        let rows = sqlx::query(
            "select revision, event_json from session_live_state \
             where session_id = $1 and revision > $2 order by revision",
        )
        .bind(session_id)
        .bind(i64::try_from(revision).unwrap_or(i64::MAX))
        .fetch_all(self.pool())
        .await?;
        let mut live = Vec::with_capacity(rows.len());
        for row in rows {
            let revision: i64 = row.try_get("revision")?;
            let event: serde_json::Value = row.try_get("event_json")?;
            live.push(SessionLiveEvent {
                revision: u64::try_from(revision).unwrap_or(0),
                event: serde_json::from_value(event)?,
            });
        }
        Ok(live)
    }

    async fn load_payload(&self, payload: &SessionPayloadRef) -> Result<Vec<u8>> {
        let bytes: Option<Vec<u8>> =
            sqlx::query_scalar("select payload from session_payloads where id = $1")
                .bind(payload.id)
                .fetch_optional(self.pool())
                .await?;
        bytes.with_context(|| format!("session payload {} is missing", payload.id))
    }

    async fn contains_message(&self, session_id: Uuid, message_id: Uuid) -> Result<bool> {
        self.contains_message_in(session_id, message_id, None, false)
            .await
    }

    async fn list_sessions(&self, limit: usize) -> Result<Vec<SessionSummary>> {
        let rows = sqlx::query(
            "select id, parent_session_id, parent_cut_sequence, inherited_event_count, state_json \
             from sessions where owner_session_id is null order by updated_at desc limit $1",
        )
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(self.pool())
        .await?;
        let mut sessions = Vec::with_capacity(rows.len());
        for row in rows {
            let state_json: String = row.try_get("state_json")?;
            let session_id: Uuid = row.try_get("id")?;
            let state = match serde_json::from_str(&state_json) {
                Ok(state) => state,
                Err(error) => {
                    // A provider removed from the runtime can still be present
                    // in an older session projection. It is not resumable by
                    // this binary, but it must not make `/resume` take down the
                    // entire TUI. Keep the row intact for explicit inspection.
                    //
                    // Skipped rather than listed with an empty state: an empty
                    // state renders as a resumable session, so offering it hands
                    // the user a session that cannot open. Skipping one row
                    // still lists every other, which is the part that matters.
                    tracing::warn!(
                        %session_id,
                        %error,
                        "skipping session with incompatible stored provider state"
                    );
                    continue;
                }
            };
            let parent_cut_sequence: Option<i64> = row.try_get("parent_cut_sequence")?;
            let inherited: i64 = row.try_get("inherited_event_count")?;
            sessions.push(SessionSummary {
                session_id,
                parent_session_id: row.try_get("parent_session_id")?,
                parent_cut_sequence: parent_cut_sequence
                    .map(|sequence| u64::try_from(sequence).unwrap_or(0)),
                inherited_event_count: u64::try_from(inherited).unwrap_or(0),
                state,
            });
        }
        Ok(sessions)
    }

    /// The workspace tier on the same database.
    ///
    /// This is what stops a Postgres-backed session from reporting "no
    /// workspace store": host.rs retries that condition forever rather than
    /// degrading, so a backend that cannot supply every tier must fail at
    /// startup instead of at first use.
    async fn workspace_store(&self) -> Result<Option<std::sync::Arc<dyn crate::WorkspaceStore>>> {
        Ok(Some(std::sync::Arc::new(
            crate::workspace_postgres::PostgresWorkspaceStore::from_pool(self.pool().clone()),
        )))
    }

    // The persistent-runtime tier. These forward to the inherent methods of the
    // same name: the inherent versions predate the trait and are still called
    // directly through concrete handles, so the trait adopts them rather than
    // duplicating their bodies.

    async fn activate_runtime_manifest(
        &self,
        session_id: Uuid,
        runtime: &str,
        root: &str,
        command: &str,
        worker_id: Uuid,
    ) -> Result<RuntimeManifestActivation> {
        Self::activate_runtime_manifest(self, session_id, runtime, root, command, worker_id).await
    }

    async fn record_runtime_execution(
        &self,
        session_id: Uuid,
        worker_id: Uuid,
        code_hash: &str,
        worker_failed: bool,
        error: Option<&str>,
    ) -> Result<()> {
        Self::record_runtime_execution(self, session_id, worker_id, code_hash, worker_failed, error)
            .await
    }

    async fn stop_runtime_manifest(&self, session_id: Uuid, worker_id: Uuid) -> Result<()> {
        Self::stop_runtime_manifest(self, session_id, worker_id).await
    }

    async fn runtime_manifest(&self, session_id: Uuid) -> Result<Option<RuntimeManifest>> {
        Self::runtime_manifest(self, session_id).await
    }

    async fn save_runtime_checkpoint(
        &self,
        session_id: Uuid,
        worker_id: Uuid,
        key: &str,
        state: &serde_json::Value,
    ) -> Result<RuntimeCheckpoint> {
        Self::save_runtime_checkpoint(self, session_id, worker_id, key, state).await
    }

    async fn runtime_checkpoint(
        &self,
        session_id: Uuid,
        key: Option<&str>,
    ) -> Result<Option<RuntimeCheckpoint>> {
        Self::runtime_checkpoint(self, session_id, key).await
    }

    async fn list_runtime_checkpoints(
        &self,
        session_id: Uuid,
        limit: usize,
    ) -> Result<Vec<RuntimeCheckpoint>> {
        Self::list_runtime_checkpoints(self, session_id, limit).await
    }

    async fn load_harness_state(&self, session_id: Uuid) -> Result<Option<serde_json::Value>> {
        Self::load_harness_state(self, session_id).await
    }

    async fn save_harness_state(&self, session_id: Uuid, state: &serde_json::Value) -> Result<()> {
        Self::save_harness_state(self, session_id, state).await
    }

    async fn rollback_harness_state(
        &self,
        session_id: Uuid,
        steps: usize,
    ) -> Result<serde_json::Value> {
        Self::rollback_harness_state(self, session_id, steps).await
    }

    async fn query_history(
        &self,
        session_id: Uuid,
        query: SessionHistoryQuery,
    ) -> Result<SessionHistoryPage> {
        Self::query_history(self, session_id, query).await
    }

    async fn history_index_documents_after(
        &self,
        session_id: Uuid,
        sequence: u64,
        limit: usize,
    ) -> Result<Vec<SessionHistoryIndexDocument>> {
        Self::history_index_documents_after(self, session_id, sequence, limit).await
    }

    async fn ensure_workflow_action(
        &self,
        session_id: Uuid,
        workflow_id: Uuid,
        payload: &serde_json::Value,
    ) -> Result<SessionAction> {
        Self::ensure_workflow_action(self, session_id, workflow_id, payload).await
    }

    async fn ensure_workflow_started(
        &self,
        event: SessionEvent,
        workflow_id: Uuid,
    ) -> Result<SessionEvent> {
        Self::ensure_workflow_started(self, event, workflow_id).await
    }

    async fn append_with_action_lease(
        &self,
        event: SessionEvent,
        action_id: Uuid,
        lease_owner: &str,
        lease_token: Uuid,
    ) -> Result<SessionEvent> {
        Self::append_with_action_lease(self, event, action_id, lease_owner, lease_token).await
    }

    async fn autonomy_store(
        &self,
    ) -> Result<Option<std::sync::Arc<dyn crate::autonomy::AutonomyStore>>> {
        Ok(Some(std::sync::Arc::new(
            crate::autonomy_postgres::PostgresAutonomyStore::from_pool(self.pool().clone()),
        )))
    }

    async fn contains_session(&self, session_id: Uuid) -> Result<bool> {
        Self::contains_session(self, session_id).await
    }

    async fn prompt_cache_session_id(&self, session_id: Uuid) -> Result<Uuid> {
        Ok(sqlx::query_scalar(
            "with recursive lineage(id, parent_session_id) as (select id, parent_session_id from sessions where id = $1 union select s.id, s.parent_session_id from sessions s join lineage l on s.id = l.parent_session_id) select id from lineage where parent_session_id is null",
        )
        .bind(session_id)
        .fetch_one(self.pool())
        .await?)
    }

    async fn create_session_in_workspace(
        &self,
        session_id: Uuid,
        workspace_id: Uuid,
    ) -> Result<SessionWorkspaceBinding> {
        Self::create_session_in_workspace(self, session_id, workspace_id).await
    }

    async fn discard_empty_session(&self, session_id: Uuid) -> Result<bool> {
        Self::discard_empty_session(self, session_id).await
    }

    async fn load_host_launch_metadata(
        &self,
        session_id: Uuid,
    ) -> Result<Option<serde_json::Value>> {
        Self::load_host_launch_metadata(self, session_id).await
    }

    async fn acknowledge_host_journal(
        &self,
        session_id: Uuid,
        event_cursor: u64,
        live_revision: u64,
    ) -> Result<()> {
        Self::acknowledge_host_journal(self, session_id, event_cursor, live_revision).await
    }

    async fn begin_host_bootstrap(&self, session_id: Uuid) -> Result<()> {
        Self::begin_host_bootstrap(self, session_id).await
    }

    async fn claim_legacy_host_launch_owner(
        &self,
        session_id: Uuid,
        host_id: Uuid,
        relay_origin: &str,
    ) -> Result<()> {
        Self::claim_legacy_host_launch_owner(self, session_id, host_id, relay_origin).await
    }

    async fn create_session_in_workspace_as(
        &self,
        session_id: Uuid,
        workspace_id: Uuid,
        participant_id: Uuid,
    ) -> Result<SessionWorkspaceBinding> {
        Self::create_session_in_workspace_as(self, session_id, workspace_id, participant_id).await
    }

    async fn finish_host_bootstrap(&self, session_id: Uuid) -> Result<()> {
        Self::finish_host_bootstrap(self, session_id).await
    }

    async fn host_launch_owner(&self, session_id: Uuid) -> Result<Option<(Uuid, String)>> {
        Self::host_launch_owner(self, session_id).await
    }

    async fn pending_host_journals(&self, after: Option<Uuid>, limit: usize) -> Result<Vec<Uuid>> {
        Self::pending_host_journals(self, after, limit).await
    }

    async fn pending_host_launch_metadata(
        &self,
        limit: usize,
    ) -> Result<Vec<(Uuid, serde_json::Value)>> {
        Self::pending_host_launch_metadata(self, limit).await
    }

    async fn pending_host_launch_metadata_for_host(
        &self,
        offset: usize,
        owner: Option<(Uuid, &str)>,
        limit: usize,
    ) -> Result<Vec<(Uuid, serde_json::Value)>> {
        Self::pending_host_launch_metadata_for_host(self, offset, owner, limit).await
    }

    async fn persist_host_launch_metadata(
        &self,
        session_id: Uuid,
        metadata: &serde_json::Value,
    ) -> Result<()> {
        Self::persist_host_launch_metadata(self, session_id, metadata).await
    }

    async fn persist_owned_host_launch_metadata(
        &self,
        session_id: Uuid,
        metadata: &serde_json::Value,
        host_id: Uuid,
        relay_origin: &str,
    ) -> Result<()> {
        Self::persist_owned_host_launch_metadata(self, session_id, metadata, host_id, relay_origin)
            .await
    }

    async fn settle_terminal_host_session(&self, session_id: Uuid) -> Result<()> {
        Self::settle_terminal_host_session(self, session_id).await
    }

    async fn pending_host_workspace_messages(
        &self,
        host_id: Uuid,
        after: Option<Uuid>,
        limit: usize,
    ) -> Result<Vec<Uuid>> {
        Self::pending_host_workspace_messages(self, host_id, after, limit).await
    }

    async fn receipt_store(&self) -> Result<std::sync::Arc<dyn crate::receipt::ReceiptBackend>> {
        Ok(std::sync::Arc::new(
            crate::receipt_postgres::PostgresReceiptStore::from_pool(self.pool().clone()),
        ))
    }

    async fn compact(&self, vacuum: bool) -> Result<SessionStoreCompaction> {
        Self::compact(self, vacuum).await
    }

    async fn readiness(&self) -> Result<SessionStoreHealth> {
        Self::readiness(self).await
    }

    async fn health(&self) -> Result<SessionStoreHealth> {
        Self::health(self).await
    }

    async fn import_session_events(
        &self,
        session_id: Uuid,
        events: Vec<SessionEvent>,
    ) -> Result<bool> {
        Self::import_session_events(self, session_id, events).await
    }

    async fn session_payload_refs(
        &self,
        session_id: Uuid,
    ) -> Result<Vec<(Uuid, SessionPayloadRef)>> {
        let rows = sqlx::query(
            "select id, event_id, payload_kind, byte_len from session_payloads \
             where session_id = $1 order by created_at, id",
        )
        .bind(session_id)
        .fetch_all(self.pool())
        .await?;
        rows.iter()
            .map(|row| {
                Ok((
                    row.try_get("event_id")?,
                    SessionPayloadRef {
                        id: row.try_get("id")?,
                        kind: serde_json::from_value(serde_json::Value::String(
                            row.try_get("payload_kind")?,
                        ))?,
                        byte_len: u64::try_from(row.try_get::<i64, _>("byte_len")?)
                            .context("negative payload length")?,
                    },
                ))
            })
            .collect()
    }

    async fn import_payload(
        &self,
        session_id: Uuid,
        event_id: Uuid,
        payload: &SessionPayloadRef,
        bytes: &[u8],
    ) -> Result<()> {
        sqlx::query(
            "insert into session_payloads \
             (id, session_id, event_id, payload_kind, payload, byte_len, created_at) \
             values ($1, $2, $3, $4, $5, $6, $7) on conflict (id) do nothing",
        )
        .bind(payload.id)
        .bind(session_id)
        .bind(event_id)
        .bind(
            serde_json::to_value(payload.kind)?
                .as_str()
                .unwrap_or_default(),
        )
        .bind(bytes)
        .bind(i64::try_from(bytes.len()).context("payload exceeds a bigint")?)
        .bind(Utc::now())
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn session_lineage_page(
        &self,
        after: Option<Uuid>,
        limit: usize,
    ) -> Result<Vec<SessionLineage>> {
        let rows = sqlx::query(
            "select id, parent_session_id, parent_cut_sequence, inherited_event_count, \
                    owner_session_id \
             from sessions where id > $1 order by id limit $2",
        )
        .bind(after.unwrap_or(Uuid::nil()))
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(self.pool())
        .await?;
        rows.iter()
            .map(|row| {
                Ok(SessionLineage {
                    session_id: row.try_get("id")?,
                    parent_session_id: row.try_get("parent_session_id")?,
                    parent_cut_sequence: row
                        .try_get::<Option<i64>, _>("parent_cut_sequence")?
                        .map(|value| u64::try_from(value).context("negative cut sequence"))
                        .transpose()?,
                    inherited_event_count: u64::try_from(
                        row.try_get::<i64, _>("inherited_event_count")?,
                    )
                    .context("negative inherited event count")?,
                    owner_session_id: row.try_get("owner_session_id")?,
                })
            })
            .collect()
    }

    async fn append_batch(&self, events: Vec<SessionEvent>) -> Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }
        let mut transaction = self.pool().begin().await?;
        let mut appended = 0u64;
        for event in events {
            self.append_durable_in_transaction(&mut transaction, event)
                .await?;
            appended += 1;
        }
        transaction.commit().await?;
        Ok(appended)
    }

    async fn raw_event_page(
        &self,
        session_id: Uuid,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<RawSessionEvent>> {
        let rows = sqlx::query(
            "select sequence, event_id, event_kind, event_json, event_body, dict_id, \
                    projection_json, fork_inheritable, recovery_relevant, message_id, created_at \
             from session_events where session_id = $1 and sequence > $2 \
             order by sequence limit $3",
        )
        .bind(session_id)
        .bind(i64::try_from(after_sequence).unwrap_or(i64::MAX))
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(self.pool())
        .await?;
        let mut events = Vec::with_capacity(rows.len());
        for row in &rows {
            // A cold row is decoded here so the caller always receives the
            // readable body, whichever tier it happens to live in.
            let body = match row.try_get::<Option<serde_json::Value>, _>("event_json")? {
                Some(body) => body,
                None => {
                    let bytes: Option<Vec<u8>> = row.try_get("event_body")?;
                    let dict_id: Option<i32> = row.try_get("dict_id")?;
                    let dictionary = match dict_id {
                        Some(dict_id) => Some(self.dictionary(dict_id).await?),
                        None => None,
                    };
                    let dictionary = dictionary.map(|dictionary| (*dictionary).clone());
                    serde_json::from_slice(&super::body::decompress(
                        &bytes.unwrap_or_default(),
                        dictionary.as_ref(),
                    )?)?
                }
            };
            events.push(RawSessionEvent {
                sequence: u64::try_from(row.try_get::<i64, _>("sequence")?)
                    .context("negative sequence")?,
                event_id: row.try_get("event_id")?,
                event_kind: row.try_get("event_kind")?,
                body,
                projection_json: row.try_get("projection_json")?,
                fork_inheritable: row.try_get("fork_inheritable")?,
                recovery_relevant: row.try_get("recovery_relevant")?,
                message_id: row.try_get("message_id")?,
                created_at: row.try_get("created_at")?,
            });
        }
        Ok(events)
    }

    async fn import_raw_events(
        &self,
        session_id: Uuid,
        events: Vec<RawSessionEvent>,
    ) -> Result<u64> {
        if events.is_empty() {
            return Ok(0);
        }
        let highest = events.iter().map(|event| event.sequence).max().unwrap_or(0);
        let mut transaction = self.pool().begin().await?;
        let mut imported = 0u64;
        for event in events {
            // Same NUL rule as the live append path: jsonb cannot hold one, so
            // such a body goes to the byte tier verbatim rather than being
            // altered to fit.
            let text = serde_json::to_string(&event.body)?;
            let nul_bearing = text.contains("\\u0000");
            let json = (!nul_bearing).then(|| event.body.clone());
            let bytes = nul_bearing.then(|| text.into_bytes());
            // The predicate columns are lifted out of the body at write time
            // because a cold body is opaque to the planner, so they are derived
            // here too rather than carried across.
            let decoded: Option<SessionEvent> = serde_json::from_value(event.body.clone()).ok();
            let affected = sqlx::query(
                "insert into session_events \
                 (session_id, sequence, event_id, event_kind, event_json, event_body, \
                  projection_json, fork_inheritable, recovery_relevant, message_id, created_at, \
                  subagent_session_id, provider_event_kind) \
                 values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) \
                 on conflict (session_id, sequence) do nothing",
            )
            .bind(session_id)
            .bind(i64::try_from(event.sequence).context("sequence exceeds a bigint")?)
            .bind(event.event_id)
            .bind(&event.event_kind)
            .bind(&json)
            .bind(bytes.as_deref())
            .bind(&event.projection_json)
            .bind(event.fork_inheritable)
            .bind(event.recovery_relevant)
            .bind(event.message_id)
            .bind(event.created_at)
            .bind(
                decoded
                    .as_ref()
                    .and_then(|event| subagent_session_id(&event.kind)),
            )
            .bind(
                decoded
                    .as_ref()
                    .and_then(|event| provider_event_kind(&event.kind)),
            )
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            imported += affected;
        }
        sqlx::query(
            "update sessions set next_sequence = greatest(next_sequence, $1), updated_at = $2 \
             where id = $3",
        )
        .bind(i64::try_from(highest.saturating_add(1)).unwrap_or(i64::MAX))
        .bind(Utc::now())
        .bind(session_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(imported)
    }

    async fn finish_imported_session(
        &self,
        session_id: Uuid,
        state: &SessionState,
        inherited_event_count: u64,
    ) -> Result<()> {
        sqlx::query(
            "update sessions set state_json = $1, inherited_event_count = $2, updated_at = $3 \
             where id = $4",
        )
        .bind(serde_json::to_string(state)?)
        .bind(i64::try_from(inherited_event_count).context("inherited count exceeds a bigint")?)
        .bind(Utc::now())
        .bind(session_id)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// Admit a durable prompt exactly once.
    ///
    /// Idempotent by the prompt's message id: a retried send returns the
    /// already-journaled event rather than adding a second one, and a DIFFERENT
    /// prompt reusing that id is refused. The lookup and the append share one
    /// transaction, so two clients racing the same retry cannot both admit.
    ///
    /// The readiness wait is not ceremony: a prompt admitted before the session
    /// has journaled its configuration would be replayed against a session that
    /// does not yet know what provider it is.
    async fn admit_prompt(&self, mut event: SessionEvent) -> Result<SessionEvent> {
        crate::session_store::ensure_prompt_admission(&event)?;
        let message_id = crate::session_store::prompt_message_id(&event)?;
        let ready_deadline = tokio::time::Instant::now()
            + crate::session_store::PROMPT_ADMISSION_SESSION_READY_TIMEOUT;
        loop {
            let state = SessionStore::state(self, event.session_id).await?;
            if state.started_at.is_some() && state.configuration.is_some() {
                break;
            }
            anyhow::ensure!(
                tokio::time::Instant::now() < ready_deadline,
                "session {} did not establish its durable journal before prompt admission",
                event.session_id
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let mut transaction = self.pool().begin().await?;
        // `message_id` is a lifted column with its own partial index, because a
        // compressed body is opaque to the planner; this is the lookup that
        // index exists for.
        let existing = sqlx::query(
            "select event_json, event_body, dict_id from session_events \
             where session_id = $1 and message_id = $2 order by sequence asc limit 1",
        )
        .bind(event.session_id)
        .bind(message_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(row) = existing {
            // Decoded from whichever tier holds it: an admitted prompt that had
            // aged into the cold tier must still be recognised as admitted, or
            // the retry would journal it a second time.
            let body = match row.try_get::<Option<serde_json::Value>, _>("event_json")? {
                Some(body) => body,
                None => {
                    let bytes: Option<Vec<u8>> = row.try_get("event_body")?;
                    let dict_id: Option<i32> = row.try_get("dict_id")?;
                    let dictionary = match dict_id {
                        Some(dict_id) => Some(self.dictionary(dict_id).await?),
                        None => None,
                    };
                    let dictionary = dictionary.map(|dictionary| (*dictionary).clone());
                    serde_json::from_slice(&super::body::decompress(
                        &bytes.unwrap_or_default(),
                        dictionary.as_ref(),
                    )?)?
                }
            };
            let existing: SessionEvent = serde_json::from_value(body)?;
            crate::session_store::ensure_same_prompt_admission(&existing, &event)?;
            transaction.commit().await?;
            return Ok(existing);
        }
        event = self
            .append_durable_in_transaction(&mut transaction, event)
            .await?;
        transaction.commit().await?;
        Ok(event)
    }

    async fn recent_messages(&self, session_id: Uuid, limit: usize) -> Result<Vec<SessionEvent>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        // A fork stores only its own events, numbered from inherited + 1, so
        // the bound is stated rather than left to where a fork's rows happen to
        // start -- matching the trait's contract by construction.
        let inherited = self.session_row(session_id).await?.inherited_event_count;
        self.recent_messages_matching(session_id, inherited, limit, |event| {
            matches!(
                event.kind,
                SessionEventKind::Message {
                    actor: crate::EventActor::User | crate::EventActor::Assistant,
                    status: crate::MessageStatus::Complete | crate::MessageStatus::InProgress,
                    ..
                }
            )
        })
        .await
    }

    async fn recent_user_messages(
        &self,
        session_id: Uuid,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.recent_messages_matching(session_id, 0, limit, |event| {
            event.kind.is_recallable_user_message()
        })
        .await
    }

    async fn search_all_sessions(&self, query: SessionHistoryQuery) -> Result<SessionHistoryPage> {
        anyhow::ensure!(
            matches!(query.mode, SessionHistorySearchMode::Lexical),
            "cross-session search is lexical only"
        );
        anyhow::ensure!(
            query
                .text
                .as_deref()
                .is_some_and(|text| !text.trim().is_empty()),
            "cross-session search requires search text"
        );
        Self::query_history_across_sessions(self, query).await
    }

    fn plugin_backend(&self) -> std::sync::Arc<dyn crate::plugin_store::PluginBackend> {
        std::sync::Arc::new(crate::plugin_store::postgres::PostgresPluginStore::new(
            self.pool().clone(),
        ))
    }

    // Harness routing; forwards to the inherent methods of the same name.
    async fn uses_native_codex_harness(&self, session_id: Uuid) -> Result<bool> {
        Self::uses_native_codex_harness(self, session_id).await
    }

    async fn uses_native_opencode_harness(
        &self,
        session_id: Uuid,
        model: Option<&str>,
    ) -> Result<bool> {
        Self::uses_native_opencode_harness(self, session_id, model).await
    }

    #[cfg(any(feature = "subscription-adapters", test))]
    async fn record_model_access(
        &self,
        session_id: Uuid,
        provider: CodingProvider,
        account_identity: &str,
    ) -> Result<()> {
        Self::record_model_access(self, session_id, provider, account_identity).await
    }

    async fn attach_workspace(
        &self,
        binding: crate::SessionWorkspaceBinding,
    ) -> Result<crate::SessionWorkspaceBinding> {
        self.attach_session_workspace(binding).await
    }

    async fn workspace_binding(
        &self,
        session_id: Uuid,
    ) -> Result<Option<crate::SessionWorkspaceBinding>> {
        self.session_workspace_binding(session_id).await
    }

    async fn register_child_session(&self, owner_session_id: Uuid, session_id: Uuid) -> Result<()> {
        PostgresSessionStore::register_child_session(self, owner_session_id, session_id).await
    }

    async fn host_workspace_cursors(
        &self,
        host_id: Uuid,
        session_id: Uuid,
    ) -> Result<HashMap<Uuid, u64>> {
        let rows = sqlx::query(
            "select workspace_id, sequence from host_workspace_cursors \
             where host_id = $1 and session_id = $2",
        )
        .bind(host_id)
        .bind(session_id)
        .fetch_all(self.pool())
        .await?;
        let mut cursors = HashMap::with_capacity(rows.len());
        for row in rows {
            let sequence: i64 = row.try_get("sequence")?;
            cursors.insert(
                row.try_get::<Uuid, _>("workspace_id")?,
                u64::try_from(sequence).unwrap_or(0),
            );
        }
        Ok(cursors)
    }

    async fn acknowledge_host_workspaces(
        &self,
        host_id: Uuid,
        session_id: Uuid,
        cursors: &HashMap<Uuid, u64>,
    ) -> Result<()> {
        let mut transaction = self.pool().begin().await?;
        for (workspace_id, sequence) in cursors {
            // A cursor only ever moves forward: an out-of-order acknowledgement
            // must not rewind delivery and replay work that was already sent.
            sqlx::query(
                "insert into host_workspace_cursors (host_id, session_id, workspace_id, sequence) \
                 values ($1, $2, $3, $4) \
                 on conflict (host_id, session_id, workspace_id) do update set \
                 sequence = greatest(host_workspace_cursors.sequence, excluded.sequence)",
            )
            .bind(host_id)
            .bind(session_id)
            .bind(workspace_id)
            .bind(i64::try_from(*sequence).unwrap_or(i64::MAX))
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn enqueue_action(&self, action: SessionAction) -> Result<SessionAction> {
        self.pg_enqueue_action(action).await
    }

    async fn transition_action(
        &self,
        session_id: Uuid,
        action_id: Uuid,
        expected: Option<SessionActionState>,
        next: SessionActionState,
        error: Option<String>,
    ) -> Result<SessionAction> {
        self.pg_transition_action(session_id, action_id, expected, next, error)
            .await
    }

    async fn claim_action(
        &self,
        session_id: Uuid,
        action_id: Uuid,
        lease_owner: &str,
        lease_duration: Duration,
    ) -> Result<Option<SessionAction>> {
        self.pg_claim_action(session_id, action_id, lease_owner, lease_duration)
            .await
    }

    async fn heartbeat_action(
        &self,
        session_id: Uuid,
        action_id: Uuid,
        lease_owner: &str,
        lease_token: Uuid,
        lease_duration: Duration,
    ) -> Result<SessionAction> {
        self.pg_heartbeat_action(
            session_id,
            action_id,
            lease_owner,
            lease_token,
            lease_duration,
        )
        .await
    }

    async fn transition_claimed_action(
        &self,
        transition: ClaimedActionTransition,
    ) -> Result<SessionAction> {
        self.pg_transition_claimed_action(transition).await
    }

    async fn recover_expired_actions(
        &self,
        session_id: Uuid,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<SessionAction>> {
        self.pg_recover_expired_actions(session_id, now, limit)
            .await
    }

    async fn action(&self, session_id: Uuid, action_id: Uuid) -> Result<Option<SessionAction>> {
        self.pg_action(session_id, action_id).await
    }

    async fn action_transitions(
        &self,
        session_id: Uuid,
        action_id: Uuid,
    ) -> Result<Vec<SessionActionTransition>> {
        self.pg_action_transitions(session_id, action_id).await
    }

    async fn pending_actions(&self, session_id: Uuid, limit: usize) -> Result<Vec<SessionAction>> {
        self.pg_pending_actions(session_id, limit).await
    }

    async fn recovery(&self, session_id: Uuid) -> Result<SessionRecovery> {
        self.recovery_projection(session_id, crate::RecoveryParts::ALL)
            .await
    }

    async fn recovery_parts(
        &self,
        session_id: Uuid,
        parts: crate::RecoveryParts,
    ) -> Result<SessionRecovery> {
        self.recovery_projection(session_id, parts).await
    }

    async fn recovery_from_provider_checkpoint(
        &self,
        session_id: Uuid,
        provider_session_id: &str,
    ) -> Result<Option<SessionRecovery>> {
        self.provider_checkpoint_recovery_projection(session_id, provider_session_id)
            .await
    }

    async fn fork_before(
        &self,
        parent_session_id: Uuid,
        session_id: Uuid,
        sequence: u64,
    ) -> Result<SessionStoreFork> {
        self.fork_session_before(parent_session_id, session_id, sequence)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::super::testing::{ScratchDatabase, test_url};
    use super::*;
    use crate::{EventActor, MessageStatus};
    use std::sync::Arc;

    fn message(session_id: Uuid, text: &str) -> SessionEvent {
        SessionEvent::new(
            session_id,
            0,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: text.to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        )
    }

    #[tokio::test]
    async fn events_append_read_back_and_allocate_their_own_sequences() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let scratch = ScratchDatabase::create(&url).await;
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
        let first = store
            .append(message(session_id, "first"))
            .await
            .expect("append");
        let second = store
            .append(message(session_id, "second"))
            .await
            .expect("append");
        assert_eq!((first.sequence, second.sequence), (2, 3));

        let events = store.read(session_id).await.expect("read");
        assert_eq!(events.len(), 3);
        assert_eq!(events[1].id, first.id);
        assert_eq!(events[2].id, second.id);

        let after = store.events_after(session_id, 1, 10).await.expect("after");
        assert_eq!(after.len(), 2);
        assert_eq!(after[0].sequence, 2);

        let SessionEventKind::Message { message_id, .. } = first.kind else {
            panic!("message expected");
        };
        assert!(
            store
                .contains_message(session_id, message_id)
                .await
                .expect("contains")
        );
        assert!(
            !store
                .contains_message(session_id, Uuid::new_v4())
                .await
                .expect("contains")
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn an_out_of_order_sequence_is_rejected_rather_than_written() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let scratch = ScratchDatabase::create(&url).await;
        let store = PostgresSessionStore::connect_with_pool_size(&scratch.url, 4)
            .await
            .expect("connect");
        let session_id = Uuid::new_v4();
        store.create_session(session_id).await.expect("create");
        let mut event = message(session_id, "from the future");
        event.sequence = 7;
        assert!(
            store.append(event).await.is_err(),
            "a caller-supplied sequence must match the allocator"
        );
        assert!(store.read(session_id).await.expect("read").is_empty());
        scratch.discard().await;
    }

    /// The claim this whole backend exists to make: writers to different
    /// sessions do not block each other. SQLite serialised every one of these
    /// behind a single file lock.
    #[tokio::test]
    async fn concurrent_writers_to_different_sessions_do_not_contend() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let scratch = ScratchDatabase::create(&url).await;
        let store = Arc::new(
            PostgresSessionStore::connect_with_pool_size(&scratch.url, 4)
                .await
                .expect("connect"),
        );
        let sessions: Vec<Uuid> = (0..8).map(|_| Uuid::new_v4()).collect();
        for session_id in &sessions {
            store.create_session(*session_id).await.expect("create");
        }

        let mut writers = tokio::task::JoinSet::new();
        for session_id in sessions.clone() {
            let store = Arc::clone(&store);
            writers.spawn(async move {
                for index in 0..25 {
                    store
                        .append(message(session_id, &format!("event {index}")))
                        .await
                        .expect("append");
                }
            });
        }
        while let Some(joined) = writers.join_next().await {
            joined.expect("writer panicked");
        }

        // Every session must have its own gap-free sequence space: a lost or
        // duplicated allocation is the failure mode a row lock has to prevent.
        for session_id in &sessions {
            let events = store.read(*session_id).await.expect("read");
            assert_eq!(events.len(), 25);
            let sequences: Vec<u64> = events.iter().map(|event| event.sequence).collect();
            assert_eq!(sequences, (1..=25).collect::<Vec<_>>());
        }
        scratch.discard().await;
    }

    /// Two writers racing on the SAME session are the case a row lock must
    /// serialise: the sequence space stays gap-free and nothing is lost.
    #[tokio::test]
    async fn concurrent_writers_to_one_session_still_produce_one_sequence_space() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let scratch = ScratchDatabase::create(&url).await;
        let store = Arc::new(
            PostgresSessionStore::connect_with_pool_size(&scratch.url, 4)
                .await
                .expect("connect"),
        );
        let session_id = Uuid::new_v4();
        store.create_session(session_id).await.expect("create");

        let mut writers = tokio::task::JoinSet::new();
        for writer in 0..6 {
            let store = Arc::clone(&store);
            writers.spawn(async move {
                let mut written = 0;
                for index in 0..10 {
                    // A contended append may legitimately fail its sequence
                    // check; what must never happen is a gap or a duplicate.
                    if store
                        .append(message(session_id, &format!("{writer}:{index}")))
                        .await
                        .is_ok()
                    {
                        written += 1;
                    }
                }
                written
            });
        }
        let mut accepted = 0;
        while let Some(joined) = writers.join_next().await {
            accepted += joined.expect("writer panicked");
        }

        let events = store.read(session_id).await.expect("read");
        assert_eq!(events.len(), accepted);
        let sequences: Vec<u64> = events.iter().map(|event| event.sequence).collect();
        assert_eq!(
            sequences,
            (1..=accepted as u64).collect::<Vec<_>>(),
            "the sequence space must stay gap-free under contention"
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn live_state_coalesces_and_never_enters_the_durable_sequence() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let scratch = ScratchDatabase::create(&url).await;
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

        // A live snapshot outside a running turn must be discarded, not
        // stored: a provider task can finish after the actor has published its
        // terminal status, and that late frame must not resurrect a reasoning
        // disclosure in an idle session.
        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::ReasoningDelta {
                    text: "late frame".to_string(),
                },
            ))
            .await
            .expect("late live append");
        assert!(
            store
                .live_events_after(session_id, 0)
                .await
                .expect("live events")
                .is_empty(),
            "an idle session must not retain live turn state"
        );

        store
            .append(SessionEvent::new(
                session_id,
                0,
                SessionEventKind::StatusChanged {
                    status: SessionStatus::Running,
                    detail: None,
                },
            ))
            .await
            .expect("running");

        for text in ["think", "thinking", "thinking hard"] {
            store
                .append(SessionEvent::new(
                    session_id,
                    0,
                    SessionEventKind::ReasoningDelta {
                        text: text.to_string(),
                    },
                ))
                .await
                .expect("live append");
        }

        // Three snapshots, one live row: coalescing is what keeps a streaming
        // turn from writing a durable event per token.
        let live = store
            .live_events_after(session_id, 0)
            .await
            .expect("live events");
        assert_eq!(live.len(), 1);
        let SessionEventKind::ReasoningDelta { text } = &live[0].event.kind else {
            panic!("reasoning delta expected");
        };
        assert_eq!(text, "thinking hard");
        assert!(live[0].revision >= 3);

        // Only the two durable events occupy sequences; no number of live
        // frames advances the durable sequence space.
        assert_eq!(store.read(session_id).await.expect("read").len(), 2);
        assert!(
            store
                .live_events_after(session_id, live[0].revision)
                .await
                .expect("live events")
                .is_empty(),
            "a cursor at the newest revision must see nothing new"
        );
        scratch.discard().await;
    }
}
