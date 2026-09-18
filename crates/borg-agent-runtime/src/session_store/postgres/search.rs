//! Cross-session history search.
//!
//! `session_event_search` is a disposable projection: `session_events` remains
//! the only authority, and this table can be dropped and rebuilt at any time.
//! It exists because a cold event body is compressed bytes, so the searchable
//! text has to live somewhere the planner can index -- which is also what keeps
//! compression and search from being in tension.
//!
//! IT IS BUILT OUTSIDE THE WRITE PATH, DELIBERATELY. The SQLite store rebuilds
//! its FTS index inside `ensure_history_projection` while holding the
//! database-wide write lock, so one agent running a history search stalls every
//! other agent's writes -- a measured cause of the stalls this migration exists
//! to fix. Here the projection is filled in its own short transactions, and
//! `append` never touches it.

use anyhow::{Context, Result, ensure};
use sqlx::Row;
use uuid::Uuid;

use super::PostgresSessionStore;
use crate::session_store::{
    MAX_HISTORY_QUERY_BYTES, SessionHistoryHit, SessionHistoryIndexDocument, SessionHistoryPage,
    SessionHistoryQuery, SessionHistorySearchMode, event_actor, event_kind,
    history_event_matches_filters, history_index_document_id, history_limit, history_match_snippet,
    history_payload_budget, history_payload_refs, history_regex, history_scan_limit,
};
use crate::SessionEvent;

/// Rows projected per transaction. Small enough that a session with a long
/// history never holds one transaction open for the whole backfill.
const PROJECTION_BATCH: i64 = 256;

/// One search row, before it is written.
struct ProjectedRow {
    sequence: i64,
    event_id: Uuid,
    event_kind: String,
    actor: Option<String>,
    body: String,
}

/// Translate a user's words into a tsquery.
///
/// `websearch_to_tsquery` is used rather than `plainto_tsquery` because it
/// accepts quoted phrases and `or`/`-` the way a person expects from a search
/// box, and it never raises on malformed input.
fn history_tsquery(text: &str) -> Result<String> {
    let trimmed = text.trim();
    ensure!(
        !trimmed.is_empty(),
        "history lexical query has no searchable terms"
    );
    Ok(trimmed.to_string())
}

/// SQL for the shared history filters, with `$n` placeholders appended in a
/// fixed order the caller then binds.
fn history_filter_sql(query: &SessionHistoryQuery, alias: &str, next: &mut usize) -> String {
    let mut sql = String::new();
    if query.event_id.is_some() {
        sql.push_str(&format!(" and {alias}.event_id = ${}", next_index(next)));
    }
    if query.start_sequence.is_some() {
        sql.push_str(&format!(" and {alias}.sequence >= ${}", next_index(next)));
    }
    if query.end_sequence.is_some() {
        sql.push_str(&format!(" and {alias}.sequence <= ${}", next_index(next)));
    }
    if !query.event_kinds.is_empty() {
        sql.push_str(&format!(
            " and {alias}.event_kind = any(${})",
            next_index(next)
        ));
    }
    if !query.actors.is_empty() {
        sql.push_str(&format!(" and {alias}.actor = any(${})", next_index(next)));
    }
    sql
}

fn next_index(next: &mut usize) -> usize {
    let index = *next;
    *next += 1;
    index
}

/// Bind the filter values in the same order `history_filter_sql` emitted them.
fn bind_history_filters<'q>(
    mut builder: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    query: &'q SessionHistoryQuery,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    if let Some(event_id) = query.event_id {
        builder = builder.bind(event_id);
    }
    if let Some(start) = query.start_sequence {
        builder = builder.bind(i64::try_from(start).unwrap_or(i64::MAX));
    }
    if let Some(end) = query.end_sequence {
        builder = builder.bind(i64::try_from(end).unwrap_or(i64::MAX));
    }
    if !query.event_kinds.is_empty() {
        builder = builder.bind(query.event_kinds.clone());
    }
    if !query.actors.is_empty() {
        let actors: Vec<String> = query
            .actors
            .iter()
            .map(|actor| crate::session_store::history_actor_name(*actor).to_string())
            .collect();
        builder = builder.bind(actors);
    }
    builder
}

impl PostgresSessionStore {
    /// The searchable text of one event: its body plus any deferred payloads,
    /// so a search matches what a reader would actually see.
    async fn history_event_body(&self, event: &SessionEvent) -> Result<String> {
        let mut body = serde_json::to_string(event)?;
        let mut references = Vec::new();
        history_payload_refs(&event.kind, &mut references);
        for reference in references {
            let payload = crate::SessionStore::load_payload(self, &reference).await?;
            body.push('\n');
            body.push_str(&String::from_utf8_lossy(&payload));
        }
        Ok(body)
    }

    /// Bring one session's search projection up to date.
    ///
    /// Idempotent and resumable: it only ever inserts rows that are missing, so
    /// an interrupted backfill is finished by the next call. Runs in its own
    /// transactions and never inside an append.
    pub async fn ensure_history_projection(&self, session_id: Uuid) -> Result<u64> {
        let mut projected = 0u64;
        loop {
            // Cold rows have to be decoded in Rust, so this cannot be a pure
            // `insert ... select`; the decode is why the projection exists.
            let rows = sqlx::query(
                "select e.sequence, e.event_id, e.event_kind, e.event_json, e.event_body, \
                        e.dict_id \
                 from session_events e \
                 where e.session_id = $1 \
                   and not exists (select 1 from session_event_search s \
                     where s.session_id = e.session_id and s.event_id = e.event_id) \
                 order by e.sequence limit $2",
            )
            .bind(session_id)
            .bind(PROJECTION_BATCH)
            .fetch_all(self.pool())
            .await?;
            if rows.is_empty() {
                return Ok(projected);
            }
            let mut batch = Vec::with_capacity(rows.len());
            for row in &rows {
                let json: Option<serde_json::Value> = row.try_get("event_json")?;
                let bytes: Option<Vec<u8>> = row.try_get("event_body")?;
                let dict_id: Option<i32> = row.try_get("dict_id")?;
                let value = self.decode_body(json, bytes, dict_id).await?;
                let event: SessionEvent = serde_json::from_value(value)?;
                batch.push(ProjectedRow {
                    sequence: row.try_get("sequence")?,
                    event_id: row.try_get("event_id")?,
                    event_kind: row.try_get("event_kind")?,
                    actor: event_actor(&event.kind).map(str::to_string),
                    body: self.history_event_body(&event).await?,
                });
            }
            let mut transaction = self.pool().begin().await?;
            for projected_row in &batch {
                sqlx::query(
                    "insert into session_event_search \
                     (session_id, sequence, event_id, event_kind, actor, body) \
                     values ($1, $2, $3, $4, $5, $6) \
                     on conflict (session_id, event_id) do nothing",
                )
                .bind(session_id)
                .bind(projected_row.sequence)
                .bind(projected_row.event_id)
                .bind(&projected_row.event_kind)
                .bind(projected_row.actor.as_deref())
                .bind(&projected_row.body)
                .execute(&mut *transaction)
                .await?;
            }
            transaction.commit().await?;
            projected += batch.len() as u64;
        }
    }

    fn validate_history_query(&self, query: &SessionHistoryQuery) -> Result<Option<String>> {
        if let (Some(start), Some(end)) = (query.start_sequence, query.end_sequence) {
            ensure!(start <= end, "history start_sequence exceeds end_sequence");
        }
        ensure!(
            query.event_kinds.len() <= 64
                && query
                    .event_kinds
                    .iter()
                    .all(|kind| !kind.is_empty() && kind.len() <= 128),
            "history event_kinds must contain at most 64 non-empty typed names"
        );
        ensure!(
            query.actors.len() <= 4,
            "history actors contains more than four values"
        );
        ensure!(
            query.prefilter.is_none() || query.mode == SessionHistorySearchMode::Regex,
            "history prefilter is only valid with regex mode"
        );
        let text = query
            .text
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        if let Some(text) = text.as_deref() {
            ensure!(
                text.len() <= MAX_HISTORY_QUERY_BYTES,
                "history query exceeds {MAX_HISTORY_QUERY_BYTES} bytes"
            );
        }
        if let Some(prefilter) = query
            .prefilter
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            ensure!(
                prefilter.len() <= MAX_HISTORY_QUERY_BYTES,
                "history prefilter exceeds {MAX_HISTORY_QUERY_BYTES} bytes"
            );
        }
        Ok(text)
    }

    /// Search one session's history.
    pub async fn query_history(
        &self,
        session_id: Uuid,
        query: SessionHistoryQuery,
    ) -> Result<SessionHistoryPage> {
        let text = self.validate_history_query(&query)?;
        let session = self.session_row(session_id).await?;
        // A fork's history spans its parent's rows, which carry the parent's
        // ids and sequences. Composing in Rust is the only way to search the
        // renumbered view a caller actually sees.
        if session.inherited_event_count > 0 {
            return self.query_history_composed(session_id, &query, text.as_deref()).await;
        }
        if text.is_some() {
            self.ensure_history_projection(session_id).await?;
        }
        match (text.as_deref(), query.mode) {
            (None, _) => self.query_history_exact(Some(session_id), &query).await,
            (Some(text), SessionHistorySearchMode::Lexical) => {
                self.query_history_lexical(Some(session_id), &query, text)
                    .await
            }
            (Some(text), SessionHistorySearchMode::Regex) => {
                self.query_history_regex(Some(session_id), &query, text)
                    .await
            }
        }
    }

    /// Search EVERY session's history at once.
    ///
    /// This is the capability the migration buys: SQLite's per-file FTS could
    /// only ever answer "what did this session say?", so cross-session recall
    /// meant opening each session in turn. One GIN index answers it here.
    pub async fn query_history_across_sessions(
        &self,
        query: SessionHistoryQuery,
    ) -> Result<SessionHistoryPage> {
        let text = self.validate_history_query(&query)?;
        // Every session must be projected before a global search can be
        // trusted; an unprojected session would silently contribute nothing.
        if text.is_some() {
            let sessions: Vec<Uuid> = sqlx::query_scalar(
                "select id from sessions where inherited_event_count = 0 \
                 and exists (select 1 from session_events e where e.session_id = sessions.id \
                   and not exists (select 1 from session_event_search s \
                     where s.session_id = e.session_id and s.event_id = e.event_id))",
            )
            .fetch_all(self.pool())
            .await?;
            for session_id in sessions {
                self.ensure_history_projection(session_id).await?;
            }
        }
        match (text.as_deref(), query.mode) {
            (None, _) => self.query_history_exact(None, &query).await,
            (Some(text), SessionHistorySearchMode::Lexical) => {
                self.query_history_lexical(None, &query, text).await
            }
            (Some(text), SessionHistorySearchMode::Regex) => {
                self.query_history_regex(None, &query, text).await
            }
        }
    }

    /// A typed or ranged read with no text, straight off the journal.
    async fn query_history_exact(
        &self,
        session_id: Option<Uuid>,
        query: &SessionHistoryQuery,
    ) -> Result<SessionHistoryPage> {
        let limit = history_limit(query);
        let mut next = 1usize;
        let session_clause = match session_id {
            Some(_) => format!("e.session_id = ${}", next_index(&mut next)),
            None => "true".to_string(),
        };
        let filters = history_filter_sql(query, "e", &mut next);
        let order = if query.newest_first { "desc" } else { "asc" };
        let sql = format!(
            "select e.event_json, e.event_body, e.dict_id from session_events e \
             where {session_clause}{filters} order by e.sequence {order} limit ${}",
            next_index(&mut next)
        );
        let mut builder = sqlx::query(sqlx::AssertSqlSafe(sql));
        if let Some(session_id) = session_id {
            builder = builder.bind(session_id);
        }
        builder = bind_history_filters(builder, query);
        let rows = builder
            .bind(i64::try_from(limit + 1).unwrap_or(i64::MAX))
            .fetch_all(self.pool())
            .await?;
        let truncated = rows.len() > limit;
        let mut hits = Vec::new();
        let mut payload_budget = history_payload_budget(query);
        for row in rows.iter().take(limit) {
            let json: Option<serde_json::Value> = row.try_get("event_json")?;
            let bytes: Option<Vec<u8>> = row.try_get("event_body")?;
            let dict_id: Option<i32> = row.try_get("dict_id")?;
            let event: SessionEvent =
                serde_json::from_value(self.decode_body(json, bytes, dict_id).await?)?;
            hits.push(
                self.hydrate_history_hit(event, None, None, query, &mut payload_budget)
                    .await?,
            );
        }
        let scanned_events = hits.len();
        Ok(SessionHistoryPage {
            hits,
            backend: "postgres_exact".to_string(),
            scanned_events,
            truncated,
        })
    }

    /// Ranked full-text search over the projection.
    async fn query_history_lexical(
        &self,
        session_id: Option<Uuid>,
        query: &SessionHistoryQuery,
        text: &str,
    ) -> Result<SessionHistoryPage> {
        let limit = history_limit(query);
        let tsquery = history_tsquery(text)?;
        let mut next = 1usize;
        let query_index = next_index(&mut next);
        let session_clause = match session_id {
            Some(_) => format!(" and s.session_id = ${}", next_index(&mut next)),
            None => String::new(),
        };
        let filters = history_filter_sql(query, "s", &mut next);
        let order = if query.newest_first {
            "order by s.sequence desc".to_string()
        } else {
            "order by score desc, s.sequence asc".to_string()
        };
        let sql = format!(
            "select e.event_json, e.event_body, e.dict_id, \
                    ts_rank_cd(s.body_tsv, websearch_to_tsquery('simple', ${query_index})) as score, \
                    ts_headline('simple', s.body, \
                      websearch_to_tsquery('simple', ${query_index}), \
                      'MaxFragments=1,MaxWords=32,MinWords=8,StartSel=[,StopSel=]') as snippet \
             from session_event_search s \
             join session_events e \
               on e.session_id = s.session_id and e.event_id = s.event_id \
             where s.body_tsv @@ websearch_to_tsquery('simple', ${query_index})\
             {session_clause}{filters} {order} limit ${}",
            next_index(&mut next)
        );
        let mut builder = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(tsquery);
        if let Some(session_id) = session_id {
            builder = builder.bind(session_id);
        }
        builder = bind_history_filters(builder, query);
        let rows = builder
            .bind(i64::try_from(limit + 1).unwrap_or(i64::MAX))
            .fetch_all(self.pool())
            .await?;
        let truncated = rows.len() > limit;
        let mut hits = Vec::new();
        let mut payload_budget = history_payload_budget(query);
        for row in rows.iter().take(limit) {
            let json: Option<serde_json::Value> = row.try_get("event_json")?;
            let bytes: Option<Vec<u8>> = row.try_get("event_body")?;
            let dict_id: Option<i32> = row.try_get("dict_id")?;
            let event: SessionEvent =
                serde_json::from_value(self.decode_body(json, bytes, dict_id).await?)?;
            let score: f32 = row.try_get("score")?;
            let snippet: Option<String> = row.try_get("snippet")?;
            hits.push(
                self.hydrate_history_hit(
                    event,
                    snippet,
                    Some(f64::from(score)),
                    query,
                    &mut payload_budget,
                )
                .await?,
            );
        }
        let scanned_events = hits.len();
        Ok(SessionHistoryPage {
            hits,
            backend: "postgres_tsvector".to_string(),
            scanned_events,
            truncated,
        })
    }

    /// Bounded regex search.
    ///
    /// Postgres cannot run Rust's regex dialect, so candidates are narrowed in
    /// SQL -- by an optional tsquery prefilter -- and the expression is applied
    /// in Rust, exactly as SQLite did.
    async fn query_history_regex(
        &self,
        session_id: Option<Uuid>,
        query: &SessionHistoryQuery,
        text: &str,
    ) -> Result<SessionHistoryPage> {
        let expression = history_regex(text, query.case_sensitive)?;
        let limit = history_limit(query);
        let scan_limit = history_scan_limit(query);
        let prefilter = query
            .prefilter
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());

        let mut next = 1usize;
        let prefilter_clause = match prefilter {
            Some(_) => format!(
                " and s.body_tsv @@ websearch_to_tsquery('simple', ${})",
                next_index(&mut next)
            ),
            None => String::new(),
        };
        let session_clause = match session_id {
            Some(_) => format!(" and s.session_id = ${}", next_index(&mut next)),
            None => String::new(),
        };
        let filters = history_filter_sql(query, "s", &mut next);
        let order = if query.newest_first { "desc" } else { "asc" };
        let sql = format!(
            "select e.event_json, e.event_body, e.dict_id, s.body from session_event_search s \
             join session_events e \
               on e.session_id = s.session_id and e.event_id = s.event_id \
             where true{prefilter_clause}{session_clause}{filters} \
             order by s.sequence {order} limit ${}",
            next_index(&mut next)
        );
        let mut builder = sqlx::query(sqlx::AssertSqlSafe(sql));
        if let Some(prefilter) = prefilter {
            builder = builder.bind(history_tsquery(prefilter)?);
        }
        if let Some(session_id) = session_id {
            builder = builder.bind(session_id);
        }
        builder = bind_history_filters(builder, query);
        let rows = builder
            .bind(i64::try_from(scan_limit + 1).unwrap_or(i64::MAX))
            .fetch_all(self.pool())
            .await?;

        let candidate_overflow = rows.len() > scan_limit;
        let mut hits = Vec::new();
        let mut scanned_events = 0;
        let mut payload_budget = history_payload_budget(query);
        for row in rows.iter().take(scan_limit) {
            scanned_events += 1;
            let body: String = row.try_get("body")?;
            let Some(found) = expression.find(&body) else {
                continue;
            };
            let json: Option<serde_json::Value> = row.try_get("event_json")?;
            let bytes: Option<Vec<u8>> = row.try_get("event_body")?;
            let dict_id: Option<i32> = row.try_get("dict_id")?;
            let event: SessionEvent =
                serde_json::from_value(self.decode_body(json, bytes, dict_id).await?)?;
            hits.push(
                self.hydrate_history_hit(
                    event,
                    Some(history_match_snippet(&body, found.start(), found.end())),
                    None,
                    query,
                    &mut payload_budget,
                )
                .await?,
            );
            if hits.len() > limit {
                break;
            }
        }
        let truncated = candidate_overflow || hits.len() > limit;
        hits.truncate(limit);
        Ok(SessionHistoryPage {
            hits,
            backend: if prefilter.is_some() {
                "postgres_regex_tsquery_prefilter"
            } else {
                "postgres_regex"
            }
            .to_string(),
            scanned_events,
            truncated,
        })
    }

    /// Search a fork's composed history.
    ///
    /// A fork's inherited rows live in its parent under the parent's ids and
    /// sequences, so the projection cannot answer for the renumbered view the
    /// caller sees. This composes and filters in Rust instead -- bounded by the
    /// scan limit, and correct rather than fast.
    async fn query_history_composed(
        &self,
        session_id: Uuid,
        query: &SessionHistoryQuery,
        text: Option<&str>,
    ) -> Result<SessionHistoryPage> {
        let limit = history_limit(query);
        let scan_limit = history_scan_limit(query);
        let expression = match (text, query.mode) {
            (Some(text), SessionHistorySearchMode::Regex) => {
                Some(history_regex(text, query.case_sensitive)?)
            }
            _ => None,
        };
        let terms: Vec<String> = match (text, query.mode) {
            (Some(text), SessionHistorySearchMode::Lexical) => text
                .split_whitespace()
                .map(|term| {
                    if query.case_sensitive {
                        term.to_string()
                    } else {
                        term.to_lowercase()
                    }
                })
                .collect(),
            _ => Vec::new(),
        };

        let mut events = self.composed_events(session_id, None).await?;
        if query.newest_first {
            events.reverse();
        }
        let mut hits = Vec::new();
        let mut scanned_events = 0;
        let mut payload_budget = history_payload_budget(query);
        let mut candidate_overflow = false;
        for event in events {
            if !history_event_matches_filters(&event, query)? {
                continue;
            }
            if scanned_events >= scan_limit {
                candidate_overflow = true;
                break;
            }
            scanned_events += 1;
            let snippet = if let Some(expression) = &expression {
                let body = self.history_event_body(&event).await?;
                let Some(found) = expression.find(&body) else {
                    continue;
                };
                Some(history_match_snippet(&body, found.start(), found.end()))
            } else if !terms.is_empty() {
                let body = self.history_event_body(&event).await?;
                let haystack = if query.case_sensitive {
                    body.clone()
                } else {
                    body.to_lowercase()
                };
                let Some(position) = terms
                    .iter()
                    .all(|term| haystack.contains(term.as_str()))
                    .then(|| haystack.find(terms[0].as_str()))
                    .flatten()
                else {
                    continue;
                };
                Some(history_match_snippet(
                    &body,
                    position,
                    position + terms[0].len(),
                ))
            } else {
                None
            };
            hits.push(
                self.hydrate_history_hit(event, snippet, None, query, &mut payload_budget)
                    .await?,
            );
            if hits.len() > limit {
                break;
            }
        }
        let truncated = candidate_overflow || hits.len() > limit;
        hits.truncate(limit);
        Ok(SessionHistoryPage {
            hits,
            backend: "postgres_lineage".to_string(),
            scanned_events,
            truncated,
        })
    }

    async fn hydrate_history_hit(
        &self,
        event: SessionEvent,
        snippet: Option<String>,
        score: Option<f64>,
        query: &SessionHistoryQuery,
        payload_budget: &mut usize,
    ) -> Result<SessionHistoryHit> {
        let mut payloads = Vec::new();
        if query.expand_payloads && *payload_budget > 0 {
            let mut references = Vec::new();
            history_payload_refs(&event.kind, &mut references);
            for reference in references {
                if *payload_budget == 0 {
                    break;
                }
                let take =
                    (*payload_budget).min(usize::try_from(reference.byte_len).unwrap_or(usize::MAX));
                let row = sqlx::query(
                    "select payload_kind, byte_len, substring(payload from 1 for $1) as payload \
                     from session_payloads where id = $2",
                )
                .bind(i64::try_from(take).unwrap_or(i64::MAX))
                .bind(reference.id)
                .fetch_optional(self.pool())
                .await?
                .with_context(|| format!("session payload {} does not exist", reference.id))?;
                ensure!(
                    row.try_get::<String, _>("payload_kind")? == reference.kind.as_str(),
                    "session payload {} has a different typed kind",
                    reference.id
                );
                let stored_len = u64::try_from(row.try_get::<i64, _>("byte_len")?)
                    .context("negative payload length")?;
                ensure!(
                    stored_len == reference.byte_len,
                    "session payload {} length does not match its reference",
                    reference.id
                );
                let bytes: Vec<u8> = row.try_get("payload")?;
                *payload_budget = payload_budget.saturating_sub(bytes.len());
                let truncated = bytes.len() as u64 != reference.byte_len;
                payloads.push(crate::SessionHistoryPayload {
                    reference,
                    truncated,
                    text: String::from_utf8_lossy(&bytes).into_owned(),
                });
            }
        }
        Ok(SessionHistoryHit {
            event,
            snippet,
            score,
            payloads,
        })
    }

    /// Stream records for an external index, cursored by sequence.
    ///
    /// Pull-based on purpose: an external index can be rebuilt from the journal
    /// after loss without joining the write transaction or becoming a second
    /// source of truth.
    pub async fn history_index_documents_after(
        &self,
        session_id: Uuid,
        sequence: u64,
        limit: usize,
    ) -> Result<Vec<SessionHistoryIndexDocument>> {
        let limit = limit.clamp(1, 1_000);
        let events = self
            .composed_events(session_id, None)
            .await?
            .into_iter()
            .filter(|event| event.sequence > sequence)
            .take(limit)
            .collect::<Vec<_>>();
        let mut documents = Vec::with_capacity(events.len());
        for event in events {
            documents.push(SessionHistoryIndexDocument {
                schema_version: 1,
                document_id: history_index_document_id(session_id, event.id),
                session_id,
                event_id: event.id,
                sequence: event.sequence,
                event_kind: event_kind(&event.kind)?,
                actor: event_actor(&event.kind).map(str::to_string),
                created_at: event.created_at,
                content: self.history_event_body(&event).await?,
            });
        }
        Ok(documents)
    }
}

#[cfg(test)]
mod tests {
    use super::super::testing::{ScratchDatabase, test_url};
    use super::*;
    use crate::session_store::SessionStore;
    use crate::{EventActor, MessageStatus, SessionEventKind};

    async fn session_with(
        store: &PostgresSessionStore,
        texts: &[(EventActor, &str)],
    ) -> Uuid {
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
        for (actor, text) in texts {
            store
                .append(SessionEvent::new(
                    session_id,
                    0,
                    SessionEventKind::Message {
                        message_id: Uuid::new_v4(),
                        actor: *actor,
                        text: (*text).to_string(),
                        attachments: Vec::new(),
                        status: MessageStatus::Complete,
                        delivery: None,
                    },
                ))
                .await
                .expect("message");
        }
        session_id
    }

    fn lexical(text: &str) -> SessionHistoryQuery {
        SessionHistoryQuery {
            text: Some(text.to_string()),
            ..Default::default()
        }
    }

    fn hit_texts(page: &SessionHistoryPage) -> Vec<String> {
        page.hits
            .iter()
            .filter_map(|hit| match &hit.event.kind {
                SessionEventKind::Message { text, .. } => Some(text.clone()),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn lexical_search_finds_ranks_and_snippets_a_match() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let scratch = ScratchDatabase::create(&url).await;
        let store = PostgresSessionStore::connect(&scratch.url).await.expect("connect");
        let session_id = session_with(
            &store,
            &[
                (EventActor::User, "please migrate the journal to postgres"),
                (EventActor::Assistant, "the migration is under way"),
                (EventActor::Assistant, "unrelated chatter about lunch"),
            ],
        )
        .await;

        let page = store
            .query_history(session_id, lexical("migrate"))
            .await
            .expect("search");
        assert_eq!(page.backend, "postgres_tsvector");
        assert_eq!(hit_texts(&page), vec!["please migrate the journal to postgres"]);
        assert!(page.hits[0].score.is_some(), "a ranked hit must carry a score");
        assert!(
            page.hits[0]
                .snippet
                .as_deref()
                .is_some_and(|snippet| snippet.contains("migrate")),
            "a hit must show why it matched: {:?}",
            page.hits[0].snippet
        );

        // A word nobody said must not match anything.
        assert!(
            store
                .query_history(session_id, lexical("kubernetes"))
                .await
                .expect("search")
                .hits
                .is_empty()
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn search_spans_every_session_at_once() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let scratch = ScratchDatabase::create(&url).await;
        let store = PostgresSessionStore::connect(&scratch.url).await.expect("connect");
        let first = session_with(&store, &[(EventActor::User, "the deadlock was in the ager")]).await;
        let second =
            session_with(&store, &[(EventActor::Assistant, "I fixed the deadlock yesterday")]).await;
        session_with(&store, &[(EventActor::User, "something else entirely")]).await;

        // The capability SQLite could not offer: one query, every session.
        let page = store
            .query_history_across_sessions(lexical("deadlock"))
            .await
            .expect("global search");
        let sessions: std::collections::HashSet<Uuid> =
            page.hits.iter().map(|hit| hit.event.session_id).collect();
        assert_eq!(page.hits.len(), 2, "both sessions must answer");
        assert!(sessions.contains(&first) && sessions.contains(&second));

        // A single-session search still answers only for that session.
        let scoped = store
            .query_history(first, lexical("deadlock"))
            .await
            .expect("scoped search");
        assert_eq!(scoped.hits.len(), 1);
        assert_eq!(scoped.hits[0].event.session_id, first);
        scratch.discard().await;
    }

    #[tokio::test]
    async fn the_search_projection_is_not_built_by_the_write_path() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let scratch = ScratchDatabase::create(&url).await;
        let store = PostgresSessionStore::connect(&scratch.url).await.expect("connect");
        let session_id = session_with(&store, &[(EventActor::User, "indexed lazily")]).await;

        // Appending must not touch the projection. SQLite rebuilt its FTS index
        // while holding the write lock, which is what stalled other agents.
        let projected: i64 = sqlx::query_scalar(
            "select count(*) from session_event_search where session_id = $1",
        )
        .bind(session_id)
        .fetch_one(store.pool())
        .await
        .expect("count");
        assert_eq!(projected, 0, "append must not populate the search projection");

        store
            .query_history(session_id, lexical("indexed"))
            .await
            .expect("search");
        let projected: i64 = sqlx::query_scalar(
            "select count(*) from session_event_search where session_id = $1",
        )
        .bind(session_id)
        .fetch_one(store.pool())
        .await
        .expect("count");
        assert!(projected > 0, "searching must build the projection");

        // Rebuilding is idempotent: a second pass adds nothing.
        assert_eq!(
            store.ensure_history_projection(session_id).await.expect("reproject"),
            0
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn filters_narrow_by_kind_actor_and_sequence() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let scratch = ScratchDatabase::create(&url).await;
        let store = PostgresSessionStore::connect(&scratch.url).await.expect("connect");
        let session_id = session_with(
            &store,
            &[
                (EventActor::User, "shared keyword from the user"),
                (EventActor::Assistant, "shared keyword from the assistant"),
            ],
        )
        .await;

        let by_actor = store
            .query_history(
                session_id,
                SessionHistoryQuery {
                    actors: vec![EventActor::User],
                    ..lexical("shared")
                },
            )
            .await
            .expect("search");
        assert_eq!(hit_texts(&by_actor), vec!["shared keyword from the user"]);

        let by_kind = store
            .query_history(
                session_id,
                SessionHistoryQuery {
                    event_kinds: vec!["session_started".to_string()],
                    ..lexical("shared")
                },
            )
            .await
            .expect("search");
        assert!(by_kind.hits.is_empty(), "a kind filter must exclude messages");

        // An empty query is a typed/range read that never consults the index.
        let ranged = store
            .query_history(
                session_id,
                SessionHistoryQuery {
                    start_sequence: Some(2),
                    end_sequence: Some(2),
                    ..Default::default()
                },
            )
            .await
            .expect("range read");
        assert_eq!(ranged.backend, "postgres_exact");
        assert_eq!(ranged.hits.len(), 1);
        assert_eq!(ranged.hits[0].event.sequence, 2);
        scratch.discard().await;
    }

    #[tokio::test]
    async fn regex_search_matches_patterns_a_word_index_cannot() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let scratch = ScratchDatabase::create(&url).await;
        let store = PostgresSessionStore::connect(&scratch.url).await.expect("connect");
        let session_id = session_with(
            &store,
            &[
                (EventActor::Assistant, "error code E1042 was returned"),
                (EventActor::Assistant, "error code E7 was returned"),
            ],
        )
        .await;

        let page = store
            .query_history(
                session_id,
                SessionHistoryQuery {
                    text: Some(r"E\d{4}".to_string()),
                    mode: SessionHistorySearchMode::Regex,
                    ..Default::default()
                },
            )
            .await
            .expect("regex search");
        assert_eq!(page.backend, "postgres_regex");
        assert_eq!(hit_texts(&page), vec!["error code E1042 was returned"]);

        // A prefilter narrows candidates before the expression runs.
        let prefiltered = store
            .query_history(
                session_id,
                SessionHistoryQuery {
                    text: Some(r"E\d{4}".to_string()),
                    mode: SessionHistorySearchMode::Regex,
                    prefilter: Some("returned".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("prefiltered regex");
        assert_eq!(prefiltered.backend, "postgres_regex_tsquery_prefilter");
        assert_eq!(hit_texts(&prefiltered), vec!["error code E1042 was returned"]);

        // A prefilter is meaningless for lexical mode and is refused.
        assert!(
            store
                .query_history(
                    session_id,
                    SessionHistoryQuery {
                        prefilter: Some("returned".to_string()),
                        ..lexical("error")
                    }
                )
                .await
                .is_err()
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn compressed_history_is_still_searchable() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let scratch = ScratchDatabase::create(&url).await;
        let store = PostgresSessionStore::connect(&scratch.url).await.expect("connect");
        let session_id =
            session_with(&store, &[(EventActor::User, "a memorable phrase worth finding")]).await;

        // Age the session into the cold tier BEFORE it has ever been projected,
        // so the projection has to decode compressed bodies to build itself.
        sqlx::query("update sessions set updated_at = now() - interval '30 days' where id = $1")
            .bind(session_id)
            .execute(store.pool())
            .await
            .expect("age");
        let outcome = store
            .age_cold_sessions(chrono::Utc::now() - chrono::Duration::days(7), 8)
            .await
            .expect("age");
        assert!(outcome.events_compressed > 0);

        let page = store
            .query_history(session_id, lexical("memorable"))
            .await
            .expect("search compressed history");
        assert_eq!(
            hit_texts(&page),
            vec!["a memorable phrase worth finding"],
            "compression must not make history unsearchable"
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn index_documents_stream_forward_from_a_cursor() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let scratch = ScratchDatabase::create(&url).await;
        let store = PostgresSessionStore::connect(&scratch.url).await.expect("connect");
        let session_id = session_with(
            &store,
            &[(EventActor::User, "first"), (EventActor::User, "second")],
        )
        .await;

        let all = store
            .history_index_documents_after(session_id, 0, 100)
            .await
            .expect("documents");
        assert_eq!(all.len(), 3);
        assert_eq!(
            all.iter().map(|doc| doc.sequence).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert!(all.iter().all(|doc| doc.schema_version == 1));

        // A cursor resumes strictly after what it already consumed.
        let resumed = store
            .history_index_documents_after(session_id, 2, 100)
            .await
            .expect("documents");
        assert_eq!(resumed.len(), 1);
        assert_eq!(resumed[0].sequence, 3);
        assert!(resumed[0].content.contains("second"));
        scratch.discard().await;
    }
}
