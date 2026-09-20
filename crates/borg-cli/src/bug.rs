//! `borg bug`: a diagnostic bundle you can read before you send it anywhere.
//!
//! This command is local only. It opens no socket, calls no model, and uploads
//! nothing; the bundle is written to a file or printed, and what happens to it
//! next is the user's decision. That is the whole design constraint, and the
//! rest of this module follows from it.
//!
//! WHY AN ALLOWLIST: a diagnostic collector that gathers everything and then
//! strips the dangerous parts is one forgotten field away from shipping a
//! token. So nothing reaches the bundle unless it is named here. Configuration
//! files are never read verbatim, environment variables are never enumerated,
//! request headers are never touched, and provider credentials have no code
//! path into this module at all.
//!
//! WHY ERRORS ARE CLASSIFIED: the most dangerous field in a diagnostic report
//! is the error message. A connection failure prints the URL it dialled, and a
//! session store URL carries a password. So failures are reduced to a fixed
//! class -- `connection_refused`, `permission_denied` -- and the underlying
//! text is dropped without ever being stored. A class is nearly as useful for
//! triage and cannot leak.
//!
//! WHY THE TRANSCRIPT IS OPT-IN: message text is the user's conversation, and
//! it is the one thing here that can contain anything at all. It is excluded
//! unless `--include-transcript` is passed, it may only be written to a file
//! (never to a terminal or a pipe, where permissions mean nothing), and the
//! file is created private to the user.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, ensure};
use borg_remote::{
    EventActor, MessageStatus, SessionEvent, SessionEventKind, SessionState, SessionStatus,
    SessionStore, default_host_config_path, local_session_owner_is_active,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use crate::cli::BugArgs;

/// Bumped when a field is added, removed, or changes meaning.
const BUG_BUNDLE_VERSION: u32 = 1;
/// Sessions listed in the bundle, newest first.
const MAX_RECENT_SESSIONS: usize = 20;
/// Events read from the end of the selected session's journal.
///
/// The bundle samples the tail rather than reading the journal, because a long
/// session's full history is both unbounded and far more than a bug report
/// needs. Everything the bundle knows about events comes from this one sample.
const MAX_EVENT_SAMPLE: usize = 200;
/// Messages carried in an opted-in transcript.
const MAX_TRANSCRIPT_MESSAGES: usize = 200;
/// Characters kept from one message before it is truncated.
const MAX_TRANSCRIPT_MESSAGE_CHARS: usize = 2_000;
/// Refuse to write a bundle larger than this.
const MAX_BUNDLE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Serialize)]
struct BugBundle {
    schema_version: u32,
    generated_at: DateTime<Utc>,
    /// The privacy contract, restated inside the artifact.
    ///
    /// A bundle usually outlives the terminal that produced it, so the warning
    /// travels with the file rather than only appearing once on stderr.
    privacy: Privacy,
    build: Build,
    session_store: Probe<StoreReport>,
    runtimes: Vec<RuntimeReport>,
    recent_sessions: Probe<Vec<SessionReport>>,
    session: Option<Probe<SessionDetail>>,
    transcript: Option<Transcript>,
}

#[derive(Debug, Serialize)]
struct Privacy {
    local_only: bool,
    uploaded: bool,
    includes_transcript: bool,
    note: &'static str,
}

#[derive(Debug, Serialize)]
struct Build {
    version: &'static str,
    os: &'static str,
    arch: &'static str,
    debug_assertions: bool,
}

/// Something the bundle tried to collect.
///
/// A bug report is most often produced when something is already broken, so a
/// probe that fails has to degrade into a classified failure rather than
/// aborting the command: refusing to produce a bundle exactly when the store
/// is unreachable would remove the tool from the situation it exists for.
#[derive(Debug, Serialize)]
#[serde(untagged)]
enum Probe<T> {
    Available { available: bool, value: T },
    Unavailable { available: bool, reason: Reason },
}

impl<T> Probe<T> {
    fn ok(value: T) -> Self {
        Self::Available {
            available: true,
            value,
        }
    }

    fn failed(error: &anyhow::Error) -> Self {
        Self::classified(error_class(error))
    }

    /// Could not be collected, with no failure to classify.
    fn unavailable() -> Self {
        Self::classified("unavailable")
    }

    fn classified(error_class: &'static str) -> Self {
        Self::Unavailable {
            available: false,
            reason: Reason { error_class },
        }
    }
}

/// Why a probe failed, with the original text discarded.
#[derive(Debug, Serialize)]
struct Reason {
    error_class: &'static str,
}

#[derive(Debug, Serialize)]
struct StoreReport {
    ready: bool,
    durable_commits: bool,
    /// The server setting, not an error message: a fixed vocabulary of
    /// durability modes, and the one value an operator has to see to act.
    commit_durability: String,
    integrity_checked: bool,
    integrity: String,
    sessions: i64,
    events: i64,
    actions: i64,
    payloads: i64,
    projection_version: i32,
}

/// Whether a provider runtime is usable.
///
/// Deliberately a boolean and not the diagnosis text: `diagnose` reports the
/// path it resolved or the install hint it would print, and both name
/// directories under the user's home.
#[derive(Debug, Serialize)]
struct RuntimeReport {
    runtime: &'static str,
    ready: bool,
}

/// One row of the session list, lineage included.
///
/// Lineage lives here and not in [`SessionDetail`] because a listed session
/// carries its parent with it, and a session looked up on its own does not.
#[derive(Debug, Serialize)]
struct SessionReport {
    session_id: Uuid,
    parent_session_id: Option<Uuid>,
    inherited_event_count: u64,
    #[serde(flatten)]
    common: SessionCommon,
}

/// What both session views read straight from `SessionState`.
#[derive(Debug, Serialize)]
struct SessionCommon {
    latest_sequence: u64,
    status: Option<SessionStatus>,
    started_at: Option<DateTime<Utc>>,
    activity_at: Option<DateTime<Utc>>,
    provider: Option<String>,
    model: Option<String>,
    permission_mode: Option<String>,
}

#[derive(Debug, Serialize)]
struct SessionDetail {
    session_id: Uuid,
    /// Events this session inherited from a fork parent.
    ///
    /// The parent's id is deliberately absent rather than reported as null.
    /// `list_sessions` returns only top-level sessions, and the only bounded
    /// per-session lineage read is this count, so a synthesized `null` would
    /// describe every forked session as a root one -- in precisely the reports
    /// filed about forking. A non-zero count is the honest signal that this
    /// session has a parent; `recent_sessions` names that parent whenever the
    /// journal still lists it.
    inherited_event_count: Probe<u64>,
    #[serde(flatten)]
    common: SessionCommon,
    context_generation: u64,
    active_process_count: usize,
    /// Set when the session ended, reduced to its class.
    ///
    /// `status_detail` is free text written by whatever stopped the session,
    /// which can be a provider error carrying a URL, so the bundle keeps the
    /// classification and drops the sentence.
    status_detail_class: Option<&'static str>,
    event_sample: EventSample,
    process_ownership: Probe<ProcessOwnership>,
}

#[derive(Debug, Serialize)]
struct EventSample {
    /// Events actually examined, and the window they came from.
    sampled: usize,
    from_sequence: u64,
    truncated: bool,
    kind_counts: BTreeMap<String, usize>,
    recent: Vec<EventReference>,
}

/// An event named and located, with its payload left in the journal.
#[derive(Debug, Serialize)]
struct EventReference {
    sequence: u64,
    event_id: Uuid,
    kind: String,
    created_at: DateTime<Utc>,
}

/// What can be said about the process that owns a session.
///
/// Only liveness. The owner record also holds a pid and an executable path,
/// and neither survives into the bundle: the pid is meaningless to a reader on
/// another machine, and the path names the user's filesystem.
#[derive(Debug, Serialize)]
struct ProcessOwnership {
    owner_active: bool,
    note: &'static str,
}

#[derive(Debug, Serialize)]
struct Transcript {
    session_id: Uuid,
    /// Restated here because a transcript is the one part of the bundle whose
    /// sensitivity depends on what the user happened to type.
    warning: &'static str,
    messages: Vec<TranscriptMessage>,
    /// True when the tail sample or the message cap cut the conversation.
    truncated: bool,
}

#[derive(Debug, Serialize)]
struct TranscriptMessage {
    sequence: u64,
    actor: EventActor,
    created_at: DateTime<Utc>,
    text: String,
    text_truncated: bool,
    /// Attachments are counted, never named: a filename is a path.
    attachment_count: usize,
}

const PRIVACY_NOTE: &str = "Collected locally and never uploaded by Borg. Read this file before \
                            sharing it. Credentials, environment variables, request headers, and \
                            configuration file contents are never collected; failures are \
                            recorded as a class rather than as their original message.";

const TRANSCRIPT_WARNING: &str = "Contains conversation text you asked to include with \
                                  --include-transcript. Review it before sharing.";

const OWNERSHIP_NOTE: &str = "Liveness only; the owning pid and executable path are deliberately \
                              excluded.";

pub(crate) async fn run(args: BugArgs) -> Result<()> {
    if args.include_transcript {
        // On stderr so it is visible even when the bundle goes to stdout, and
        // so it never lands inside the bundle itself.
        eprintln!(
            "borg bug: --include-transcript copies your conversation text into the bundle. \
             The file is written private to your user; review it before sharing it."
        );
    }

    let store = open_store().await;
    let bundle = collect(store.as_deref(), &args).await?;
    let serialized = serde_json::to_string_pretty(&bundle)?;
    ensure!(
        serialized.len() <= MAX_BUNDLE_BYTES,
        "diagnostic bundle is larger than {MAX_BUNDLE_BYTES} bytes"
    );

    match args.output.as_deref() {
        Some(output) => {
            write_private(output, &serialized, args.force)?;
            if args.json {
                println!(
                    "{}",
                    serde_json::json!({
                        "output": output,
                        "bytes": serialized.len(),
                        "includes_transcript": bundle.transcript.is_some(),
                    })
                );
            } else {
                println!("Wrote diagnostic bundle to {}.", output.display());
                print_summary(&bundle);
                println!(
                    "Nothing was uploaded. Review the file before sharing it{}.",
                    if bundle.transcript.is_some() {
                        " -- it contains conversation text"
                    } else {
                        ""
                    }
                );
            }
        }
        None => {
            if args.json {
                println!("{serialized}");
            } else {
                print_summary(&bundle);
                println!("Re-run with --output <path> to write the full bundle.");
            }
        }
    }
    Ok(())
}

/// Open the journal, or report that it could not be opened.
///
/// Returns an error-free `Option` on purpose: a bug bundle is most valuable
/// when the store is the thing that is broken.
async fn open_store() -> Option<std::sync::Arc<dyn SessionStore>> {
    borg_remote::session_store::factory::open(
        &borg_remote::session_store::factory::SessionStoreConfig::from_env(),
    )
    .await
    .ok()
    .map(|opened| std::sync::Arc::clone(opened.session()))
}

async fn collect(store: Option<&dyn SessionStore>, args: &BugArgs) -> Result<BugBundle> {
    let mut runtimes = Vec::new();
    for runtime in borg_provider::Runtime::ALL {
        runtimes.push(RuntimeReport {
            runtime: runtime.program(),
            ready: borg_provider::provider_bin::diagnose(runtime).await.is_ok(),
        });
    }

    let (session_store, recent_sessions) = match store {
        Some(store) => (store_report(store).await, recent_sessions(store).await),
        // The journal would not open at all, which is a diagnosis rather than
        // a reason to abandon the bundle.
        None => (Probe::unavailable(), Probe::unavailable()),
    };

    let mut transcript = None;
    let session = match (store, args.session) {
        (Some(store), requested) => {
            match resolve_session(store, requested).await {
                Ok(Some(session_id)) => {
                    let sample = event_tail(store, session_id).await;
                    if args.include_transcript
                        && let Ok(events) = sample.as_ref()
                    {
                        transcript = Some(build_transcript(session_id, events));
                    }
                    Some(session_detail(store, session_id, sample).await)
                }
                // No session at all is a fact about the machine, not a
                // failure to collect one.
                Ok(None) => None,
                Err(error) => Some(Probe::failed(&error)),
            }
        }
        (None, _) => None,
    };

    Ok(BugBundle {
        schema_version: BUG_BUNDLE_VERSION,
        generated_at: Utc::now(),
        privacy: Privacy {
            local_only: true,
            uploaded: false,
            includes_transcript: transcript.is_some(),
            note: PRIVACY_NOTE,
        },
        build: Build {
            version: env!("CARGO_PKG_VERSION"),
            os: std::env::consts::OS,
            arch: std::env::consts::ARCH,
            debug_assertions: cfg!(debug_assertions),
        },
        session_store,
        runtimes,
        recent_sessions,
        session,
        transcript,
    })
}

async fn store_report(store: &dyn SessionStore) -> Probe<StoreReport> {
    // Readiness, never `health`: the deep integrity scan reads the whole
    // database, and a bug report should not cost that.
    match store.readiness().await {
        Ok(health) => Probe::ok(StoreReport {
            ready: health.is_ready(),
            durable_commits: health.durable_commits,
            commit_durability: health.commit_durability.clone(),
            integrity_checked: health.integrity_checked,
            integrity: health.integrity.clone(),
            sessions: health.sessions,
            events: health.events,
            actions: health.actions,
            payloads: health.payloads,
            projection_version: health.projection_version,
        }),
        Err(error) => Probe::failed(&error),
    }
}

async fn recent_sessions(store: &dyn SessionStore) -> Probe<Vec<SessionReport>> {
    match store.list_sessions(MAX_RECENT_SESSIONS).await {
        Ok(summaries) => Probe::ok(summaries.iter().map(session_report).collect()),
        Err(error) => Probe::failed(&error),
    }
}

fn session_report(summary: &borg_remote::SessionSummary) -> SessionReport {
    SessionReport {
        session_id: summary.session_id,
        parent_session_id: summary.parent_session_id,
        inherited_event_count: summary.inherited_event_count,
        common: session_common(&summary.state),
    }
}

fn session_common(state: &SessionState) -> SessionCommon {
    let configuration = state.configuration.as_ref();
    SessionCommon {
        latest_sequence: state.latest_sequence,
        status: state.status,
        started_at: state.started_at,
        activity_at: state.activity_at,
        // `cwd` is intentionally absent: a working directory names the user's
        // filesystem and often their projects.
        provider: configuration.map(|configuration| format!("{:?}", configuration.provider)),
        model: configuration.and_then(|configuration| configuration.model.clone()),
        permission_mode: configuration
            .map(|configuration| format!("{:?}", configuration.permission_mode)),
    }
}

async fn resolve_session(
    store: &dyn SessionStore,
    requested: Option<Uuid>,
) -> Result<Option<Uuid>> {
    if let Some(session_id) = requested {
        store
            .state(session_id)
            .await
            .with_context(|| format!("local session {session_id} does not exist"))?;
        return Ok(Some(session_id));
    }
    Ok(store
        .list_sessions(1)
        .await?
        .first()
        .map(|summary| summary.session_id))
}

async fn event_tail(store: &dyn SessionStore, session_id: Uuid) -> Result<Vec<SessionEvent>> {
    let state = store.state(session_id).await?;
    let from = state
        .latest_sequence
        .saturating_sub(MAX_EVENT_SAMPLE as u64);
    store.events_after(session_id, from, MAX_EVENT_SAMPLE).await
}

async fn session_detail(
    store: &dyn SessionStore,
    session_id: Uuid,
    sample: Result<Vec<SessionEvent>>,
) -> Probe<SessionDetail> {
    let state = match store.state(session_id).await {
        Ok(state) => state,
        Err(error) => return Probe::failed(&error),
    };
    let events = match sample {
        Ok(events) => events,
        Err(error) => return Probe::failed(&error),
    };
    Probe::ok(SessionDetail {
        session_id,
        inherited_event_count: match store.inherited_event_count(session_id).await {
            Ok(count) => Probe::ok(count),
            Err(error) => Probe::failed(&error),
        },
        common: session_common(&state),
        context_generation: state.context_generation,
        active_process_count: state.active_processes.len(),
        status_detail_class: state
            .status_detail
            .as_deref()
            .map(|detail| class_of(&detail.to_lowercase())),
        event_sample: event_sample(&events),
        process_ownership: process_ownership(session_id),
    })
}

fn event_sample(events: &[SessionEvent]) -> EventSample {
    let mut kind_counts = BTreeMap::new();
    let mut recent = Vec::with_capacity(events.len());
    for event in events {
        let kind = event_kind_name(&event.kind);
        *kind_counts.entry(kind.clone()).or_insert(0) += 1;
        recent.push(EventReference {
            sequence: event.sequence,
            event_id: event.id,
            kind,
            created_at: event.created_at,
        });
    }
    EventSample {
        sampled: events.len(),
        from_sequence: events.first().map_or(0, |event| event.sequence),
        truncated: events.len() >= MAX_EVENT_SAMPLE,
        kind_counts,
        recent,
    }
}

/// The name of an event's variant, and nothing else it carries.
///
/// `SessionEventKind` is internally tagged, so the variant name is the `type`
/// field of its serialized form. The serialized value is read for that one
/// string and dropped: no payload -- no message text, no tool output, no file
/// path -- is copied out of it.
fn event_kind_name(kind: &SessionEventKind) -> String {
    serde_json::to_value(kind)
        .ok()
        .and_then(|value| {
            value
                .get("type")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| "unknown".to_string())
}

fn process_ownership(session_id: Uuid) -> Probe<ProcessOwnership> {
    let sessions_dir = default_host_config_path()
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("sessions");
    match local_session_owner_is_active(&sessions_dir, session_id) {
        Ok(owner_active) => Probe::ok(ProcessOwnership {
            owner_active,
            note: OWNERSHIP_NOTE,
        }),
        // Ownership is reported only when it can be established accurately.
        // An unreadable owner record means unknown, not "not running".
        Err(error) => Probe::failed(&error),
    }
}

fn build_transcript(session_id: Uuid, events: &[SessionEvent]) -> Transcript {
    let mut messages = Vec::new();
    let mut truncated = false;
    for event in events {
        if messages.len() >= MAX_TRANSCRIPT_MESSAGES {
            truncated = true;
            break;
        }
        let SessionEventKind::Message {
            actor: actor @ (EventActor::User | EventActor::Assistant),
            text,
            attachments,
            status: MessageStatus::Complete | MessageStatus::Failed,
            ..
        } = &event.kind
        else {
            continue;
        };
        let (text, text_truncated) = truncate(text);
        messages.push(TranscriptMessage {
            sequence: event.sequence,
            actor: *actor,
            created_at: event.created_at,
            text,
            text_truncated,
            attachment_count: attachments.len(),
        });
    }
    Transcript {
        session_id,
        warning: TRANSCRIPT_WARNING,
        // The sample itself is a tail, so a full window is a truncated
        // conversation even when no message cap was reached.
        truncated: truncated || events.len() >= MAX_EVENT_SAMPLE,
        messages,
    }
}

fn truncate(text: &str) -> (String, bool) {
    match text.char_indices().nth(MAX_TRANSCRIPT_MESSAGE_CHARS) {
        Some((cut, _)) => (text[..cut].to_string(), true),
        None => (text.to_string(), false),
    }
}

/// Write the bundle where only this user can read it.
///
/// Created with `create_new` and the mode set at open time, so the file is
/// never briefly world-readable between creation and a later `chmod`.
fn write_private(output: &Path, contents: &str, force: bool) -> Result<()> {
    use std::io::Write as _;

    if let Some(parent) = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    if force && output.exists() {
        std::fs::remove_file(output).with_context(|| format!("replace {}", output.display()))?;
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(output).with_context(|| {
        format!(
            "write {} (pass --force to replace an existing bundle)",
            output.display()
        )
    })?;
    file.write_all(contents.as_bytes())
        .with_context(|| format!("write {}", output.display()))?;
    Ok(())
}

fn print_summary(bundle: &BugBundle) {
    println!(
        "Borg {} on {}/{}",
        bundle.build.version, bundle.build.os, bundle.build.arch
    );
    match &bundle.session_store {
        Probe::Available { value, .. } => println!(
            "Durable session store: {} · {} sessions · {} events",
            if value.ready { "ready" } else { "degraded" },
            value.sessions,
            value.events
        ),
        Probe::Unavailable { reason, .. } => {
            println!(
                "Durable session store: unavailable ({})",
                reason.error_class
            );
        }
    }
    let ready = bundle
        .runtimes
        .iter()
        .filter(|runtime| runtime.ready)
        .count();
    println!("Runtimes ready: {ready}/{}", bundle.runtimes.len());
    println!(
        "Transcript: {}",
        if bundle.transcript.is_some() {
            "included (--include-transcript)"
        } else {
            "excluded"
        }
    );
}

/// Reduce a failure to a fixed class, discarding the text that produced it.
fn error_class(error: &anyhow::Error) -> &'static str {
    let mut text = String::new();
    for cause in error.chain() {
        text.push_str(&cause.to_string().to_lowercase());
        text.push(' ');
    }
    class_of(&text)
}

/// The classification table.
///
/// Ordered most specific first, and every arm returns a literal: there is no
/// path by which a fragment of the input becomes part of the result.
fn class_of(lowercased: &str) -> &'static str {
    const CLASSES: &[(&str, &str)] = &[
        ("permission denied", "permission_denied"),
        ("access denied", "permission_denied"),
        ("not permitted", "permission_denied"),
        ("connection refused", "connection_refused"),
        ("connection reset", "connection_reset"),
        ("connection closed", "connection_closed"),
        ("timed out", "timed_out"),
        ("timeout", "timed_out"),
        ("no such file", "not_found"),
        ("does not exist", "not_found"),
        ("not found", "not_found"),
        ("unauthorized", "authentication"),
        ("authentication", "authentication"),
        ("password", "authentication"),
        ("certificate", "tls"),
        ("tls", "tls"),
        ("out of memory", "resource_exhausted"),
        ("too many", "resource_exhausted"),
        ("cancel", "cancelled"),
        ("stopped", "stopped"),
        ("interrupt", "cancelled"),
        ("degraded", "degraded"),
        ("unsupported", "unsupported"),
        ("invalid", "invalid_input"),
        ("parse", "invalid_input"),
    ];
    CLASSES
        .iter()
        .find(|(needle, _)| lowercased.contains(needle))
        .map_or("unclassified", |(_, class)| class)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    /// These tests exist for one reason: every claim this module makes about
    /// what it does not collect is a claim a future edit can silently break,
    /// and the failure is invisible -- a bundle that leaks looks exactly like
    /// a bundle that does not. Each test below pins one of those claims to a
    /// serialized artifact, which is the only place the leak would appear.
    const SECRET: &str = "sk-live-do-not-ship-this";

    fn message_event(sequence: u64, text: &str) -> SessionEvent {
        SessionEvent {
            id: Uuid::new_v4(),
            session_id: Uuid::nil(),
            sequence,
            created_at: Utc::now(),
            kind: SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: text.to_string(),
                attachments: vec![PathBuf::from("/home/someone/private/notes.txt")],
                status: MessageStatus::Complete,
                delivery: None,
            },
        }
    }

    /// `SessionConfiguration` carries `cwd`, and a working directory names the
    /// user's filesystem and frequently their clients. Three of its fields are
    /// copied out by hand and the rest are left behind, so the only thing
    /// standing between a bundle and the user's project paths is that nobody
    /// widens that copy -- which compiles cleanly and produces a bundle that
    /// still looks exactly right.
    #[test]
    fn a_session_view_leaves_the_working_directory_behind() {
        use borg_remote::{CodingProvider, PermissionMode, ResponseLanguage, SessionConfiguration};

        let state = SessionState {
            configuration: Some(SessionConfiguration {
                cwd: PathBuf::from("/home/someone/clients/acme-acquisition"),
                provider: CodingProvider::Claude,
                model: Some("claude-opus-5".to_string()),
                effort: None,
                fast: false,
                response_language: ResponseLanguage::Auto,
                permission_mode: PermissionMode::Manual,
            }),
            ..SessionState::default()
        };
        let serialized = serde_json::to_string(&session_common(&state)).unwrap();
        assert!(
            !serialized.contains("acme-acquisition"),
            "a session view must not carry the user's working directory"
        );
        // The allowlisted fields are still collected; this is an exclusion,
        // not an empty report.
        assert!(serialized.contains("claude-opus-5"));
        assert!(serialized.contains("Claude"));
    }

    #[test]
    fn an_event_reference_names_the_event_and_carries_none_of_its_payload() {
        let events = vec![message_event(1, SECRET)];
        let serialized = serde_json::to_string(&event_sample(&events)).unwrap();
        assert!(
            !serialized.contains(SECRET),
            "event references must not carry message text"
        );
        assert!(
            !serialized.contains("notes.txt"),
            "event references must not carry attachment paths"
        );
        assert!(
            serialized.contains("message"),
            "the variant name is the point of the reference"
        );
    }

    #[test]
    fn an_included_transcript_counts_attachments_rather_than_naming_them() {
        let transcript = build_transcript(Uuid::nil(), &[message_event(1, SECRET)]);
        let serialized = serde_json::to_string(&transcript).unwrap();
        // The text is here because the user asked for it; the path is not,
        // because they did not.
        assert!(serialized.contains(SECRET));
        assert!(
            !serialized.contains("notes.txt"),
            "an opted-in transcript still must not name attachment paths"
        );
        assert_eq!(transcript.messages[0].attachment_count, 1);
    }

    #[test]
    fn a_long_message_is_truncated_rather_than_copied_whole() {
        let long = "x".repeat(MAX_TRANSCRIPT_MESSAGE_CHARS * 2);
        let (text, truncated) = truncate(&long);
        assert!(truncated);
        assert_eq!(text.chars().count(), MAX_TRANSCRIPT_MESSAGE_CHARS);
    }

    #[test]
    fn a_failure_is_reduced_to_a_class_and_its_text_is_never_copied() {
        // The realistic shape of the worst case: a store URL with a password
        // in it, inside a connection error.
        let error = anyhow::anyhow!("postgres://borg:{SECRET}@10.0.0.2/journal")
            .context("connection refused");
        let probe = Probe::<StoreReport>::failed(&error);
        let serialized = serde_json::to_string(&probe).unwrap();
        assert!(
            !serialized.contains(SECRET),
            "a classified failure must not carry the original message"
        );
        assert!(!serialized.contains("10.0.0.2"));
        assert!(serialized.contains("connection_refused"));
    }

    #[test]
    fn an_unrecognized_failure_classifies_rather_than_falling_through_to_its_text() {
        let error = anyhow::anyhow!("{SECRET}");
        let serialized = serde_json::to_string(&Probe::<StoreReport>::failed(&error)).unwrap();
        assert!(!serialized.contains(SECRET));
        assert!(serialized.contains("unclassified"));
    }

    #[cfg(unix)]
    #[test]
    fn a_written_bundle_is_readable_only_by_its_owner() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("nested").join("bundle.json");
        write_private(&output, "{}", false).unwrap();
        let mode = std::fs::metadata(&output).unwrap().permissions().mode();
        assert_eq!(
            mode & 0o777,
            0o600,
            "a bundle may contain a transcript; it must not be readable by other users"
        );
        // Without --force an existing bundle is never silently overwritten.
        assert!(write_private(&output, "{}", false).is_err());
        write_private(&output, "{}", true).unwrap();
    }
}
