//! Ageing hot event bodies into the cold, dictionary-compressed tier.
//!
//! POLICY: a session whose thread has not been opened for a week is cold. That
//! is read straight off `sessions.updated_at`, which every append already
//! bumps and `idx_sessions_activity` already indexes, so the policy needs no
//! extra bookkeeping and cannot drift out of sync with real activity.
//!
//! Ageing is per row and reversible in principle: a cold row keeps the exact
//! `dict_id` that decodes it, and dictionaries are append-only, so nothing here
//! can strand history.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::PostgresSessionStore;
use super::body::{self, DICTIONARY_TARGET_BYTES, EventDictionary};

/// How long a thread must go unopened before its bodies are compressed.
pub const COLD_AFTER: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Rows rewritten per transaction. Ageing holds the session row while it runs,
/// so batches stay small enough that a re-opened session waits milliseconds
/// rather than for an entire history to be rewritten.
const AGE_BATCH_ROWS: i64 = 512;

/// Bodies sampled to train a dictionary, and the minimum corpus worth training
/// from. Below this the dictionary would encode the quirks of a handful of
/// events rather than the shape of the corpus, so rows are compressed without
/// one instead -- still ~2.94x, and still readable by exactly the same path.
const DICTIONARY_SAMPLE_ROWS: i64 = 4_096;
const DICTIONARY_MIN_SAMPLES: usize = 256;

/// What one ageing pass did. Byte counts are of the bodies actually rewritten,
/// so a caller can report a real ratio rather than an estimate.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColdAgeOutcome {
    pub sessions_aged: u64,
    pub events_compressed: u64,
    pub bytes_before: u64,
    pub bytes_after: u64,
}

/// Immutable dictionary cache. `session_event_dicts` is append-only, so a
/// dictionary that has been read once can never change underneath us.
#[derive(Debug, Clone, Default)]
pub struct DictionaryCache {
    entries: Arc<RwLock<HashMap<i32, Arc<EventDictionary>>>>,
}

impl DictionaryCache {
    fn get(&self, dict_id: i32) -> Option<Arc<EventDictionary>> {
        self.entries
            .read()
            .ok()
            .and_then(|entries| entries.get(&dict_id).cloned())
    }

    fn insert(&self, dictionary: Arc<EventDictionary>) {
        if let Ok(mut entries) = self.entries.write() {
            entries.insert(dictionary.dict_id, dictionary);
        }
    }
}

impl PostgresSessionStore {
    /// Load a dictionary by id, through the cache.
    pub async fn dictionary(&self, dict_id: i32) -> Result<Arc<EventDictionary>> {
        if let Some(cached) = self.dictionaries.get(dict_id) {
            return Ok(cached);
        }
        let bytes: Vec<u8> =
            sqlx::query_scalar("select dict_bytes from session_event_dicts where dict_id = $1")
                .bind(dict_id)
                .fetch_optional(self.pool())
                .await?
                // A row naming a dictionary that does not exist is a corrupted
                // journal, not a cache miss: its body can never be decoded.
                .with_context(|| format!("event dictionary {dict_id} is missing"))?;
        let dictionary = Arc::new(EventDictionary { dict_id, bytes });
        self.dictionaries.insert(Arc::clone(&dictionary));
        Ok(dictionary)
    }

    /// Decode one stored body back to JSON bytes, hot or cold.
    ///
    /// This is the single read path that makes the cold tier invisible to
    /// callers: hot rows are returned as-is, cold rows are decoded with the
    /// dictionary they name.
    pub async fn decode_body(
        &self,
        event_json: Option<serde_json::Value>,
        event_body: Option<Vec<u8>>,
        dict_id: Option<i32>,
    ) -> Result<serde_json::Value> {
        if let Some(value) = event_json {
            return Ok(value);
        }
        let bytes = event_body.context("session event row has neither a hot nor a cold body")?;
        let dictionary = match dict_id {
            Some(dict_id) => Some(self.dictionary(dict_id).await?),
            None => None,
        };
        let json = body::decompress(&bytes, dictionary.as_deref())?;
        serde_json::from_slice(&json).context("cold event body is not valid JSON")
    }

    /// The newest trained dictionary, training one from real bodies if the
    /// journal has none yet and has enough history to train from.
    ///
    /// Returns `None` when the corpus is too small to be worth training on;
    /// callers then write cold rows with `dict_id` null.
    pub async fn current_dictionary(&self) -> Result<Option<Arc<EventDictionary>>> {
        if let Some(dict_id) =
            sqlx::query_scalar::<_, Option<i32>>("select max(dict_id) from session_event_dicts")
                .fetch_one(self.pool())
                .await?
        {
            return self.dictionary(dict_id).await.map(Some);
        }

        // Train from bodies that are actually about to be compressed, so the
        // dictionary reflects this deployment's events rather than a guess.
        let samples: Vec<Vec<u8>> = sqlx::query_scalar::<_, String>(
            "select event_json::text from session_events \
             where event_json is not null limit $1",
        )
        .bind(DICTIONARY_SAMPLE_ROWS)
        .fetch_all(self.pool())
        .await?
        .into_iter()
        .map(String::into_bytes)
        .collect();
        if samples.len() < DICTIONARY_MIN_SAMPLES {
            return Ok(None);
        }
        let bytes = body::train_dictionary(&samples, DICTIONARY_TARGET_BYTES)?;

        // Two processes may train concurrently. Both dictionaries are valid and
        // append-only, so the insert simply claims the next free id; whichever
        // lands first becomes current and the other is still decodable by any
        // row that referenced it.
        let dict_id: i32 = sqlx::query_scalar(
            "insert into session_event_dicts (dict_id, dict_bytes) \
             select coalesce(max(dict_id), 0) + 1, $1 from session_event_dicts \
             returning dict_id",
        )
        .bind(&bytes)
        .fetch_one(self.pool())
        .await
        .context("failed to store a trained event dictionary")?;
        let dictionary = Arc::new(EventDictionary { dict_id, bytes });
        self.dictionaries.insert(Arc::clone(&dictionary));
        Ok(Some(dictionary))
    }

    /// Compress the bodies of every session untouched since `cutoff`.
    ///
    /// Idempotent and resumable: it only ever selects rows that still have a
    /// hot body, so an interrupted pass is completed by the next one.
    pub async fn age_cold_sessions(
        &self,
        cutoff: DateTime<Utc>,
        max_sessions: usize,
    ) -> Result<ColdAgeOutcome> {
        let dictionary = self.current_dictionary().await?;
        let mut outcome = ColdAgeOutcome::default();
        // Sessions already handled this pass, including ones skipped because a
        // live writer held them. Without this the same locked session would be
        // selected on every iteration and the pass would spin instead of
        // moving on to the next cold thread.
        let mut visited: Vec<Uuid> = Vec::new();
        for _ in 0..max_sessions {
            let Some(session_id) = self.next_cold_session(cutoff, &visited).await? else {
                break;
            };
            visited.push(session_id);
            // Keep draining this session until its hot rows are gone; the row
            // budget is per transaction, not per session.
            let mut aged_any = false;
            loop {
                let progressed = self
                    .age_session_batch(session_id, cutoff, dictionary.as_deref(), &mut outcome)
                    .await?;
                if !progressed {
                    break;
                }
                aged_any = true;
            }
            if aged_any {
                outcome.sessions_aged += 1;
            }
        }
        Ok(outcome)
    }

    /// Decompress every cold body in `session_id` back to the hot tier.
    ///
    /// Ageing is a storage decision, not a one-way door. Re-opening a thread
    /// makes it interesting again -- to its own agent, to other agents reading
    /// across the database, and to anyone composing SQL by hand -- and cold
    /// bodies are invisible to `event_json` predicates and jsonb operators.
    /// Re-heating on open restores full ad-hoc queryability for the session a
    /// human or agent is actually looking at, and the ager will cool it again
    /// after another week of silence.
    ///
    /// Idempotent: it only selects rows that are still cold, so a session with
    /// nothing to re-heat costs one indexed lookup.
    pub async fn reheat_session(&self, session_id: Uuid) -> Result<u64> {
        let mut reheated = 0u64;
        loop {
            let rows = sqlx::query(
                "select sequence, event_body, dict_id from session_events \
                 where session_id = $1 and event_json is null \
                 order by sequence limit $2",
            )
            .bind(session_id)
            .bind(AGE_BATCH_ROWS)
            .fetch_all(self.pool())
            .await?;
            if rows.is_empty() {
                return Ok(reheated);
            }
            let mut transaction = self.pool().begin().await?;
            for row in rows {
                let sequence: i64 = row.try_get("sequence")?;
                let bytes: Option<Vec<u8>> = row.try_get("event_body")?;
                let dict_id: Option<i32> = row.try_get("dict_id")?;
                let value = self.decode_body(None, bytes, dict_id).await?;
                // Clearing event_body keeps the invariant that exactly one tier
                // holds the body, so nothing has to decide which copy is
                // authoritative if the two ever disagreed.
                sqlx::query(
                    "update session_events \
                     set event_json = $1, event_body = null, dict_id = null \
                     where session_id = $2 and sequence = $3",
                )
                .bind(&value)
                .bind(session_id)
                .bind(sequence)
                .execute(&mut *transaction)
                .await?;
                reheated += 1;
            }
            transaction.commit().await?;
        }
    }

    /// The next session eligible for ageing, or `None` when there are none.
    async fn next_cold_session(
        &self,
        cutoff: DateTime<Utc>,
        visited: &[Uuid],
    ) -> Result<Option<Uuid>> {
        let session_id: Option<Uuid> = sqlx::query_scalar(
            "select s.id from sessions s \
             where s.updated_at < $1 \
               and s.id <> all($2) \
               and exists ( \
                   select 1 from session_events e \
                   where e.session_id = s.id and e.event_json is not null \
               ) \
             order by s.updated_at \
             limit 1",
        )
        .bind(cutoff)
        .bind(visited)
        .fetch_optional(self.pool())
        .await?;
        Ok(session_id)
    }

    /// Compress up to one batch of rows for `session_id`.
    ///
    /// Returns whether anything was rewritten, so the caller can drain a
    /// session without guessing at its length.
    async fn age_session_batch(
        &self,
        session_id: Uuid,
        cutoff: DateTime<Utc>,
        dictionary: Option<&EventDictionary>,
        outcome: &mut ColdAgeOutcome,
    ) -> Result<bool> {
        let mut transaction = self.pool().begin().await?;

        // `skip locked` means a session with a live writer is left alone rather
        // than queued behind, and re-reading `updated_at` under the lock closes
        // the race where a thread is re-opened between selection and rewrite:
        // a woken session is no longer cold and must keep its readable bodies.
        let still_cold: Option<DateTime<Utc>> = sqlx::query_scalar(
            "select updated_at from sessions where id = $1 and updated_at < $2 \
             for no key update skip locked",
        )
        .bind(session_id)
        .bind(cutoff)
        .fetch_optional(&mut *transaction)
        .await?;
        if still_cold.is_none() {
            transaction.rollback().await?;
            return Ok(false);
        }

        let rows = sqlx::query(
            "select sequence, event_json::text as body from session_events \
             where session_id = $1 and event_json is not null \
             order by sequence limit $2",
        )
        .bind(session_id)
        .bind(AGE_BATCH_ROWS)
        .fetch_all(&mut *transaction)
        .await?;
        if rows.is_empty() {
            transaction.rollback().await?;
            return Ok(false);
        }

        for row in rows {
            let sequence: i64 = row.try_get("sequence")?;
            let json: String = row.try_get("body")?;
            let body::StoredBody::Compressed { bytes, dict_id } =
                body::compress(json.as_bytes(), dictionary)?
            else {
                anyhow::bail!("compressing an event body must produce a cold body");
            };
            outcome.bytes_before += json.len() as u64;
            outcome.bytes_after += bytes.len() as u64;
            outcome.events_compressed += 1;
            sqlx::query(
                "update session_events \
                 set event_body = $1, dict_id = $2, event_json = null \
                 where session_id = $3 and sequence = $4",
            )
            .bind(&bytes)
            .bind(dict_id)
            .bind(session_id)
            .bind(sequence)
            .execute(&mut *transaction)
            .await?;
        }

        // Ageing is storage maintenance, not session activity: bumping
        // `updated_at` here would reset the very clock that selected this
        // session and make ageing re-trigger forever.
        transaction.commit().await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::testing::{ScratchDatabase, test_url};

    fn event_body(session: Uuid, sequence: i64) -> serde_json::Value {
        serde_json::json!({
            "id": Uuid::new_v4(),
            "session_id": session,
            "sequence": sequence,
            "kind": {
                "type": "tool_completed",
                "tool": "Bash",
                "actor": "assistant",
                "output": format!("checking borg-agent-runtime, step {sequence}"),
            },
            "created_at": "2026-09-01T00:00:00Z",
        })
    }

    /// Insert a session whose thread was last opened `age` ago, with `events`
    /// hot bodies. Written with raw SQL because the trait port is not done yet;
    /// the ager is being tested, not the writer.
    async fn seed_session(
        store: &PostgresSessionStore,
        age: chrono::Duration,
        events: i64,
    ) -> Uuid {
        let session_id = Uuid::new_v4();
        let updated_at = Utc::now() - age;
        sqlx::query(
            "insert into sessions (id, state_json, created_at, updated_at) \
             values ($1, '{}', $2, $2)",
        )
        .bind(session_id)
        .bind(updated_at)
        .execute(store.pool())
        .await
        .expect("insert session");
        for sequence in 1..=events {
            sqlx::query(
                "insert into session_events \
                 (session_id, sequence, event_id, event_kind, event_json, \
                  projection_json, fork_inheritable, recovery_relevant, created_at) \
                 values ($1, $2, $3, 'tool_completed', $4, '', true, true, $5)",
            )
            .bind(session_id)
            .bind(sequence)
            .bind(Uuid::new_v4())
            .bind(event_body(session_id, sequence))
            .bind(updated_at)
            .execute(store.pool())
            .await
            .expect("insert event");
        }
        session_id
    }

    async fn hot_count(store: &PostgresSessionStore, session_id: Uuid) -> i64 {
        sqlx::query_scalar(
            "select count(*) from session_events \
             where session_id = $1 and event_json is not null",
        )
        .bind(session_id)
        .fetch_one(store.pool())
        .await
        .expect("count hot rows")
    }

    #[tokio::test]
    async fn a_thread_unopened_for_a_week_ages_and_a_live_one_does_not() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let scratch = ScratchDatabase::create(&url).await;
        let store = PostgresSessionStore::connect(&scratch.url)
            .await
            .expect("connect");
        let cold = seed_session(&store, chrono::Duration::days(8), 6).await;
        let warm = seed_session(&store, chrono::Duration::hours(1), 6).await;
        let cutoff = Utc::now() - chrono::Duration::from_std(COLD_AFTER).expect("cutoff");

        let outcome = store.age_cold_sessions(cutoff, 64).await.expect("age");
        assert!(outcome.events_compressed >= 6, "{outcome:?}");
        assert!(
            outcome.bytes_after < outcome.bytes_before,
            "ageing must shrink bodies: {outcome:?}"
        );

        assert_eq!(hot_count(&store, cold).await, 0, "cold thread must age");
        assert_eq!(
            hot_count(&store, warm).await,
            6,
            "a thread opened an hour ago must keep readable bodies"
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn an_aged_body_still_reads_back_byte_for_byte() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let scratch = ScratchDatabase::create(&url).await;
        let store = PostgresSessionStore::connect(&scratch.url)
            .await
            .expect("connect");
        let session_id = seed_session(&store, chrono::Duration::days(30), 4).await;
        let before: Vec<(i64, serde_json::Value)> = sqlx::query_as(
            "select sequence, event_json from session_events \
             where session_id = $1 order by sequence",
        )
        .bind(session_id)
        .fetch_all(store.pool())
        .await
        .expect("read hot bodies");
        assert_eq!(before.len(), 4);

        let cutoff = Utc::now() - chrono::Duration::from_std(COLD_AFTER).expect("cutoff");
        store.age_cold_sessions(cutoff, 64).await.expect("age");

        // Compression is only acceptable if it is lossless; a cold body that
        // decodes to anything else is silent history corruption.
        let after: Vec<(i64, Option<serde_json::Value>, Option<Vec<u8>>, Option<i32>)> =
            sqlx::query_as(
                "select sequence, event_json, event_body, dict_id from session_events \
                 where session_id = $1 order by sequence",
            )
            .bind(session_id)
            .fetch_all(store.pool())
            .await
            .expect("read cold bodies");
        for ((sequence, original), (cold_sequence, json, bytes, dict_id)) in
            before.into_iter().zip(after)
        {
            assert_eq!(sequence, cold_sequence);
            assert!(json.is_none(), "an aged row must not keep a hot body");
            let decoded = store
                .decode_body(json, bytes, dict_id)
                .await
                .expect("decode cold body");
            assert_eq!(decoded, original);
        }
        scratch.discard().await;
    }

    #[tokio::test]
    async fn reopening_a_cold_thread_restores_plain_sql_queryability() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let scratch = ScratchDatabase::create(&url).await;
        let store = PostgresSessionStore::connect(&scratch.url)
            .await
            .expect("connect");
        let session_id = seed_session(&store, chrono::Duration::days(9), 7).await;
        let before: Vec<serde_json::Value> = sqlx::query_scalar(
            "select event_json from session_events where session_id = $1 order by sequence",
        )
        .bind(session_id)
        .fetch_all(store.pool())
        .await
        .expect("read hot bodies");

        let cutoff = Utc::now() - chrono::Duration::from_std(COLD_AFTER).expect("cutoff");
        store.age_cold_sessions(cutoff, 64).await.expect("age");
        assert_eq!(hot_count(&store, session_id).await, 0);

        // A jsonb predicate cannot see a cold body at all -- which is exactly
        // why re-opening a thread has to restore it.
        let matched_while_cold: i64 = sqlx::query_scalar(
            "select count(*) from session_events \
             where session_id = $1 and event_json @> '{\"kind\":{\"tool\":\"Bash\"}}'",
        )
        .bind(session_id)
        .fetch_one(store.pool())
        .await
        .expect("query cold");
        assert_eq!(matched_while_cold, 0);

        let reheated = store.reheat_session(session_id).await.expect("reheat");
        assert_eq!(reheated, 7);
        assert_eq!(hot_count(&store, session_id).await, 7);

        let matched_after: i64 = sqlx::query_scalar(
            "select count(*) from session_events \
             where session_id = $1 and event_json @> '{\"kind\":{\"tool\":\"Bash\"}}'",
        )
        .bind(session_id)
        .fetch_one(store.pool())
        .await
        .expect("query hot");
        assert_eq!(matched_after, 7, "re-heating must restore jsonb predicates");

        // Round-tripping through the cold tier must not alter one byte of
        // history, and must leave no stale second copy behind.
        let after: Vec<serde_json::Value> = sqlx::query_scalar(
            "select event_json from session_events where session_id = $1 order by sequence",
        )
        .bind(session_id)
        .fetch_all(store.pool())
        .await
        .expect("read reheated bodies");
        assert_eq!(after, before);
        let stale: i64 = sqlx::query_scalar(
            "select count(*) from session_events \
             where session_id = $1 and event_body is not null",
        )
        .bind(session_id)
        .fetch_one(store.pool())
        .await
        .expect("count stale bodies");
        assert_eq!(stale, 0);

        assert_eq!(
            store.reheat_session(session_id).await.expect("reheat again"),
            0,
            "re-heating an already hot session must be a no-op"
        );
        scratch.discard().await;
    }

    #[tokio::test]
    async fn ageing_is_idempotent_and_leaves_cold_rows_queryable() {
        let Some(url) = test_url() else {
            eprintln!("skipping: BORG_TEST_SESSIONS_URL is not set");
            return;
        };
        let scratch = ScratchDatabase::create(&url).await;
        let store = PostgresSessionStore::connect(&scratch.url)
            .await
            .expect("connect");
        let session_id = seed_session(&store, chrono::Duration::days(14), 5).await;
        let cutoff = Utc::now() - chrono::Duration::from_std(COLD_AFTER).expect("cutoff");

        store.age_cold_sessions(cutoff, 64).await.expect("first");
        let second = store.age_cold_sessions(cutoff, 64).await.expect("second");
        assert_eq!(
            second.events_compressed, 0,
            "a second pass must find nothing left to age: {second:?}"
        );

        // The predicate columns are lifted out of the body precisely so that
        // cold history stays selectable without decompressing anything.
        let by_kind: i64 = sqlx::query_scalar(
            "select count(*) from session_events \
             where session_id = $1 and event_kind = 'tool_completed'",
        )
        .bind(session_id)
        .fetch_one(store.pool())
        .await
        .expect("count by kind");
        assert_eq!(by_kind, 5);

        // The readable view must announce a cold body rather than hide it, so
        // an agent's ad-hoc SQL cannot silently under-report history.
        let (total, compressed): (i64, i64) = sqlx::query_as(
            "select count(*), count(*) filter (where body_compressed) \
             from session_events_readable where session_id = $1",
        )
        .bind(session_id)
        .fetch_one(store.pool())
        .await
        .expect("read view");
        assert_eq!(total, 5);
        assert_eq!(compressed, 5);
        scratch.discard().await;
    }
}
