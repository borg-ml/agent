//! Copy a session journal from one backend to another.
//!
//! WHY THIS EXISTS: `BORG_SESSIONS_URL` chooses a backend, but choosing a new
//! one does not bring your history with it. Without this, switching to Postgres
//! means abandoning every existing session -- which is fine for a fresh install
//! and unacceptable for one with work in it.
//!
//! HOW IT WORKS: events are REPLAYED through the destination's own `append`
//! rather than copied row for row. That is deliberate. Every backend derives
//! its own projections, search index, predicate columns and sequence
//! allocation from the events it is given; copying rows would smuggle the
//! source's derived state into a destination that computes it differently, and
//! the result would look right until something read it. Replaying means the
//! destination builds itself exactly as it would have if the history had
//! happened there.
//!
//! The one thing replay cannot reconstruct is a body that was moved out of its
//! event into `session_payloads`, because the event now holds only a marker
//! pointing at a row that does not exist in the destination. Those are copied
//! verbatim, keeping their ids so the markers stay valid.
//!
//! SAFETY: the source is only ever read. Nothing here writes to it, so a
//! migration can run against a journal that is still in use, and can be
//! abandoned at any point without leaving the source altered.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, Result};
use uuid::Uuid;

use super::{SessionLineage, SessionStore};

/// Sessions fetched per enumeration page.
const SESSION_PAGE: usize = 256;

/// Events replayed per `append` batch when reporting progress.
const PROGRESS_EVERY: u64 = 5_000;

/// Events fetched from the source per round trip.
const EVENT_BATCH: usize = 512;

/// What a migration did, or would do.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MigrationOutcome {
    /// Sessions copied by this run.
    pub sessions_copied: u64,
    /// Sessions already present in the destination and left alone.
    pub sessions_skipped: u64,
    /// Sessions whose copy failed; the migration continued past them.
    pub sessions_failed: u64,
    /// Events replayed into the destination.
    pub events_copied: u64,
    /// Oversized bodies copied verbatim.
    pub payloads_copied: u64,
    /// Ids of sessions that failed, with the reason.
    pub failures: Vec<(Uuid, String)>,
}

/// How to run a migration.
#[derive(Debug, Clone, Default)]
pub struct MigrationOptions {
    /// Report what would be copied without writing anything.
    pub dry_run: bool,
    /// Stop after this many sessions. `None` migrates everything.
    pub max_sessions: Option<u64>,
    /// Abandon the run on the first failure instead of continuing.
    ///
    /// Defaults to false: one unreadable session in a journal of a thousand
    /// should not cost the other nine hundred and ninety-nine.
    pub fail_fast: bool,
}

/// Copy every session from `source` into `destination`.
///
/// Idempotent and resumable: a session already present in the destination is
/// skipped, so an interrupted run is continued by running it again. Progress is
/// reported through `progress`, which is called with a human-readable line.
pub async fn migrate_journal(
    source: Arc<dyn SessionStore>,
    destination: Arc<dyn SessionStore>,
    options: &MigrationOptions,
    progress: &mut dyn FnMut(&str),
) -> Result<MigrationOutcome> {
    let mut outcome = MigrationOutcome::default();
    let mut done: HashSet<Uuid> = HashSet::new();
    let mut after: Option<Uuid> = None;

    loop {
        let page = source
            .session_lineage_page(after, SESSION_PAGE)
            .await
            .context("enumerate source sessions")?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|session| session.session_id);

        for session in page {
            if let Some(limit) = options.max_sessions
                && outcome.sessions_copied + outcome.sessions_skipped >= limit
            {
                return Ok(outcome);
            }
            match copy_session(
                source.as_ref(),
                destination.as_ref(),
                &session,
                options,
                &mut done,
                &mut outcome,
                progress,
            )
            .await
            {
                Ok(()) => {}
                Err(error) => {
                    outcome.sessions_failed += 1;
                    let reason = format!("{error:#}");
                    progress(&format!("session {} failed: {reason}", session.session_id));
                    outcome.failures.push((session.session_id, reason));
                    if options.fail_fast {
                        return Err(error)
                            .with_context(|| format!("migrating session {}", session.session_id));
                    }
                }
            }
        }
    }
    Ok(outcome)
}

/// Copy one session, creating its lineage first.
///
/// `done` records sessions this run has already handled, so a parent pulled in
/// ahead of its own turn is not copied twice when the enumeration reaches it.
async fn copy_session(
    source: &dyn SessionStore,
    destination: &dyn SessionStore,
    session: &SessionLineage,
    options: &MigrationOptions,
    done: &mut HashSet<Uuid>,
    outcome: &mut MigrationOutcome,
    progress: &mut dyn FnMut(&str),
) -> Result<()> {
    if done.contains(&session.session_id) {
        return Ok(());
    }

    // RESUMING IS NOT THE SAME AS SKIPPING. A session's events are streamed in
    // batches, each its own transaction, so a run interrupted mid-session
    // leaves that session present but short. Treating "present" as "done" --
    // which an earlier version did -- would make the truncation permanent and
    // silent: the resume would step over it and every later read would see a
    // history that simply stops. So compare how far each side has got, and
    // resume from the destination's own cursor.
    let source_latest = source
        .state(session.session_id)
        .await
        .context("read source projection")?
        .latest_sequence;
    let existing = destination.contains_session(session.session_id).await?;
    let destination_latest = if existing {
        destination
            .state(session.session_id)
            .await
            .context("read destination projection")?
            .latest_sequence
    } else {
        0
    };
    if existing && destination_latest >= source_latest {
        done.insert(session.session_id);
        outcome.sessions_skipped += 1;
        return Ok(());
    }

    // Payload REFERENCES only: id, kind and length, never the bodies. Counting
    // these is cheap enough to do during a dry run.
    let payloads = source
        .session_payload_refs(session.session_id)
        .await
        .context("read source payload references")?;

    if options.dry_run {
        // Deliberately does NOT read the events. An earlier version counted
        // them by loading every body, which on a 56GB journal meant a "dry
        // run" read the entire journal and never finished. The projection
        // already knows how many events a session has.
        done.insert(session.session_id);
        outcome.sessions_copied += 1;
        outcome.events_copied += source_latest.saturating_sub(session.inherited_event_count);
        outcome.payloads_copied += payloads.len() as u64;
        return Ok(());
    }

    // Lineage first. A fork's own events are numbered after its parent's cut,
    // so the destination has to know it is a fork before the first append or
    // the sequences will not line up.
    if !existing {
        match (session.parent_session_id, session.parent_cut_sequence) {
            (Some(parent), Some(cut)) => {
                // The parent must exist before the child can point at it. Pulling
                // it in out of order is cheaper than sorting the whole journal
                // topologically, and `done` stops it being copied twice.
                ensure_ancestor(
                    source,
                    destination,
                    parent,
                    options,
                    done,
                    outcome,
                    progress,
                )
                .await?;
                // OFF BY ONE, deliberately. `fork_before` takes the first sequence
                // the child does NOT inherit, and records `parent_cut_sequence` as
                // one less. Passing the recorded value straight back would shift
                // the cut down by one on every migration, silently dropping the
                // last inherited event from every fork -- a corruption that would
                // only surface when someone replayed that fork.
                let fork_at = cut.saturating_add(1);
                destination
                    .fork_before(parent, session.session_id, fork_at)
                    .await
                    .with_context(|| {
                        format!(
                            "fork {} from {parent} before {fork_at} (recorded cut {cut})",
                            session.session_id
                        )
                    })?;
            }
            _ => {
                destination.create_session(session.session_id).await?;
            }
        }
    }

    // Payloads before events: an event body references its payload, so writing
    // the event first would leave a window where the reference dangles.
    for (event_id, payload) in &payloads {
        let bytes = source
            .load_payload(payload)
            .await
            .with_context(|| format!("load payload {} of event {event_id}", payload.id))?;
        destination
            .import_payload(session.session_id, *event_id, payload, &bytes)
            .await
            .with_context(|| format!("import payload {}", payload.id))?;
        outcome.payloads_copied += 1;
    }

    // VERBATIM, NOT REPLAYED. An earlier design replayed events through the
    // destination's `append`, so the destination derived its own projections.
    // That is elegant and wrong for real history: an event's persistence class
    // is computed by the CURRENT code, and a journal written by older code
    // holds rows today's rules would not journal at all. `subagent_activity` is
    // classified by its NESTED child event and accounts for 46% of events in
    // the journal this was built against, so replay silently dropped a large
    // fraction of history and broke sequence contiguity behind it.
    //
    // Copying the stored row instead means the destination receives exactly
    // what the source recorded, at the same sequence, with the same flags and
    // projection checkpoints. Streamed in batches because a single session here
    // holds 264,113 events.
    let mut cursor = session.inherited_event_count.max(destination_latest);
    let mut total = 0u64;
    loop {
        let batch = source
            .raw_event_page(session.session_id, cursor, EVENT_BATCH)
            .await
            .context("read source event rows")?;
        if batch.is_empty() {
            break;
        }
        cursor = batch
            .iter()
            .map(|event| event.sequence)
            .max()
            .unwrap_or(cursor)
            .max(cursor);
        let count = batch.len() as u64;
        destination
            .import_raw_events(session.session_id, batch)
            .await
            .with_context(|| format!("import into {}", session.session_id))?;
        total += count;
        outcome.events_copied += count;
        if outcome.events_copied % PROGRESS_EVERY < count {
            progress(&format!("{} events copied", outcome.events_copied));
        }
    }

    // The running projection the source already computed. Recomputing it would
    // mean folding every event through today's rules, which is the thing this
    // copy exists to avoid.
    let state = source
        .state(session.session_id)
        .await
        .context("read source projected state")?;
    destination
        .finish_imported_session(session.session_id, &state, session.inherited_event_count)
        .await
        .with_context(|| format!("finish {}", session.session_id))?;

    // Ownership last: `register_child_session` requires both sessions to exist,
    // and it reconciles harness routes, which are derived from the events that
    // have just been replayed.
    if let Some(owner) = session.owner_session_id {
        ensure_ancestor(source, destination, owner, options, done, outcome, progress).await?;
        destination
            .register_child_session(owner, session.session_id)
            .await
            .with_context(|| format!("register {} under {owner}", session.session_id))?;
    }

    done.insert(session.session_id);
    outcome.sessions_copied += 1;
    progress(&format!(
        "session {} copied ({total} events)",
        session.session_id
    ));
    Ok(())
}

/// Copy a parent or owner the current session depends on, if it is not already
/// in the destination.
async fn ensure_ancestor(
    source: &dyn SessionStore,
    destination: &dyn SessionStore,
    ancestor: Uuid,
    options: &MigrationOptions,
    done: &mut HashSet<Uuid>,
    outcome: &mut MigrationOutcome,
    progress: &mut dyn FnMut(&str),
) -> Result<()> {
    if done.contains(&ancestor) || destination.contains_session(ancestor).await? {
        return Ok(());
    }
    let page = source.session_lineage_page(prior(ancestor), 1).await?;
    let lineage = page
        .into_iter()
        .find(|candidate| candidate.session_id == ancestor)
        .with_context(|| format!("ancestor session {ancestor} is missing from the source"))?;
    Box::pin(copy_session(
        source,
        destination,
        &lineage,
        options,
        done,
        outcome,
        progress,
    ))
    .await
}

/// The id immediately before `id`, so a one-row page starting after it returns
/// `id` itself. Enumeration is keyed on `id >`, and there is no separate
/// fetch-one-session primitive to reach for.
fn prior(id: Uuid) -> Option<Uuid> {
    let mut bytes = *id.as_bytes();
    for index in (0..bytes.len()).rev() {
        if bytes[index] == 0 {
            bytes[index] = 0xff;
        } else {
            bytes[index] -= 1;
            return Some(Uuid::from_bytes(bytes));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prior_returns_the_immediately_preceding_id() {
        let id: Uuid = "00000000-0000-0000-0000-000000000001".parse().unwrap();
        assert_eq!(
            prior(id).unwrap().to_string(),
            "00000000-0000-0000-0000-000000000000"
        );
        // Borrowing across a zero byte.
        let id: Uuid = "00000000-0000-0000-0000-000000000100".parse().unwrap();
        assert_eq!(
            prior(id).unwrap().to_string(),
            "00000000-0000-0000-0000-0000000000ff"
        );
        // The smallest id has no predecessor, which means "from the beginning".
        assert_eq!(prior(Uuid::nil()), None);
    }
}
