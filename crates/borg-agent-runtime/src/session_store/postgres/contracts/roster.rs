//! What a parent session durably knows about its children.
//!
//! A parent mirrors each child's event stream so the UI can follow a child
//! live, which makes the parent's journal the single largest consumer of disk
//! in an orchestration session. The rules that trim it are all in
//! `SessionEventKind::persistence`, and they are easy to get wrong in the
//! direction that loses history rather than the direction that fails loudly:
//! drop one kind too many and a parent silently forgets what a child did.
//!
//! These contracts pin three things, and the third is the one that makes the
//! first two safe:
//!
//! * which mirrored child rows the parent journals and which stay live-only,
//! * that rows older builds already wrote still deserialise and still render,
//! * that the roster reconstructed from the durable journal is the same roster
//!   you would reconstruct from everything, dropped rows included.
//!
//! They run against a real store rather than against `persistence()` alone,
//! because the classification is only half the contract: the other half is that
//! `append` routes on it and that recovery's roster projection reads back what
//! survived. See `super::support`.

use chrono::Utc;
use uuid::Uuid;

use super::support;
use crate::session_store::RecoveryParts;
use crate::{
    CodingProvider, EventPersistence, MessageStatus, SessionEvent, SessionEventKind, SessionStore,
};

/// A session that exists and has started, ready to be appended to.
async fn started(store: &crate::session_store::postgres::PostgresSessionStore) -> Uuid {
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
    session_id
}

/// The parent mirrors a child's whole event stream so the UI can follow a child
/// live, but a child's provider audit trail is already durable in the child's
/// own journal and nothing renders or replays it from the parent. Measured on
/// one orchestration session, mirrored child `native_model_message` rows alone
/// were 424 MB of 1,132 MB. Ordered subagent replay of the child's transcript
/// events must be unaffected.
#[tokio::test]
async fn mirrored_child_provider_audit_events_are_live_only() {
    let (scratch, store) = support::store().await;
    let child_id = Uuid::new_v4();
    let snapshot = || crate::SubagentSnapshot {
        session_id: child_id,
        parent_session_id: Uuid::new_v4(),
        task_name: "worker".to_string(),
        status: crate::SubagentStatus::Running,
        provider: CodingProvider::Codex,
        model: None,
        effort: None,
        cwd: std::path::PathBuf::from("/tmp"),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        detail: None,
        final_text: None,
        usage: crate::SubagentUsage::default(),
        interrupted_by: None,
    };
    let mirrored = |kind: SessionEventKind| SessionEventKind::SubagentActivity {
        activity: crate::SubagentActivityKind::Updated,
        agent: snapshot(),
        event: Some(Box::new(SessionEvent::new(child_id, 0, kind))),
    };

    // Durable in the child, but provider audit records the parent never reads.
    for kind in [
        "native_model_message",
        "native_model_request",
        "native_model_usage",
        "native_tool_round_completed",
        "context_compaction",
    ] {
        let child = SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: kind.to_string(),
            payload: serde_json::json!({"content": "a full model message"}),
        };
        assert_eq!(
            child.persistence(),
            EventPersistence::Durable,
            "fixture must be durable in the child: {kind}"
        );
        assert_eq!(
            mirrored(child).persistence(),
            EventPersistence::Ephemeral,
            "the parent must not journal a child's provider audit trail: {kind}"
        );
    }

    // A child's own session metadata describes the child's session, not its
    // transcript. The parent renders and replays none of it.
    for kind in [
        SessionEventKind::ProviderCapabilitiesUpdated {
            providers: Vec::new(),
        },
        SessionEventKind::UserStopChanged { engaged: true },
    ] {
        assert_eq!(
            kind.persistence(),
            EventPersistence::Durable,
            "fixture must be durable in the child: {kind:?}"
        );
        assert_eq!(
            mirrored(kind.clone()).persistence(),
            EventPersistence::Ephemeral,
            "the parent must not journal a child's own session metadata: {kind:?}"
        );
    }

    // The child's transcript events stay durable in the parent: commit 84b03b9
    // requires them to remain replayable after the live projection disconnects.
    let replayable = [
        SessionEventKind::ToolStarted {
            tool_call_id: "call-1".to_string(),
            name: "exec".to_string(),
            input: serde_json::json!({"cmd": "cargo test"}),
            input_ref: None,
        },
        SessionEventKind::ToolCompleted {
            tool_call_id: "call-1".to_string(),
            output: "ok".to_string(),
            output_ref: None,
            is_error: false,
            input: None,
            input_ref: None,
        },
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: crate::EventActor::Assistant,
            text: "report".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
        SessionEventKind::StatusChanged {
            status: crate::SessionStatus::Ready,
            detail: None,
        },
    ];
    for kind in replayable.clone() {
        assert_eq!(
            mirrored(kind.clone()).persistence(),
            EventPersistence::Durable,
            "ordered subagent replay must keep this durable: {kind:?}"
        );
    }

    // A child's live stream stays live-only, and snapshot-only activity is
    // always durable.
    assert_eq!(
        mirrored(SessionEventKind::ReasoningDelta {
            text: "thinking".to_string()
        })
        .persistence(),
        EventPersistence::Ephemeral
    );
    assert_eq!(
        SessionEventKind::SubagentActivity {
            activity: crate::SubagentActivityKind::Completed,
            agent: snapshot(),
            event: None,
        }
        .persistence(),
        EventPersistence::Durable
    );

    // The classification is only half the rule. `append` routes on it, so the
    // parent's journal must end up holding exactly the transcript rows.
    let parent_id = started(&store).await;
    for kind in replayable {
        store
            .append(SessionEvent::new(parent_id, 0, mirrored(kind)))
            .await
            .expect("mirrored transcript row");
    }
    for kind in [
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "native_model_message".to_string(),
            payload: serde_json::json!({"content": "a full model message"}),
        },
        SessionEventKind::ProviderCapabilitiesUpdated {
            providers: Vec::new(),
        },
        SessionEventKind::ReasoningDelta {
            text: "thinking".to_string(),
        },
    ] {
        store
            .append(SessionEvent::new(parent_id, 0, mirrored(kind)))
            .await
            .expect("mirrored live-only row");
    }

    let journalled: Vec<SessionEventKind> = store
        .read(parent_id)
        .await
        .expect("read the parent journal")
        .into_iter()
        .filter_map(|event| match event.kind {
            SessionEventKind::SubagentActivity {
                event: Some(child), ..
            } => Some(child.kind),
            _ => None,
        })
        .collect();
    assert_eq!(
        journalled.len(),
        4,
        "only the four mirrored transcript rows may reach the journal, got {journalled:?}"
    );
    assert!(
        !journalled.iter().any(|kind| matches!(
            kind,
            SessionEventKind::ProviderEvent { .. }
                | SessionEventKind::ProviderCapabilitiesUpdated { .. }
                | SessionEventKind::ReasoningDelta { .. }
        )),
        "a live-only mirrored row reached the journal: {journalled:?}"
    );
    scratch.discard().await;
}

/// Journal rows written by older builds must still load. Two shapes matter and
/// both are taken verbatim from the live 59 GB journal:
///
/// * a July `subagent_activity` whose agent snapshot predates the `usage`
///   field entirely, and
/// * a row whose mirrored child event is a kind current code no longer
///   journals - dropping the *write* must never drop the *read*, or 3.4 GB of
///   existing history stops rendering.
#[tokio::test]
async fn historical_subagent_activity_rows_still_deserialise() {
    let (scratch, store) = support::store().await;
    // Verbatim from the journal: session 479bc6e5, 2026-07-26, before the agent
    // snapshot carried `usage`.
    let legacy_july = r#"{"id":"4d85aa4b-e17d-413b-a94c-eb050d98a783","session_id":"479bc6e5-9272-4efa-b2a3-641adf31c379","sequence":33,"created_at":"2026-07-26T01:01:45.889433205Z","kind":{"type":"subagent_activity","activity":"started","agent":{"session_id":"1e293f97-280c-40b8-bade-9dc7fc5da93c","parent_session_id":"479bc6e5-9272-4efa-b2a3-641adf31c379","task_name":"/root/smoke_child","status":"starting","provider":"codex","model":"gpt-5.6-sol","effort":"medium","cwd":"/home/shulgin/borg","created_at":"2026-07-26T01:01:45.861616732Z","updated_at":"2026-07-26T01:01:45.861616732Z","detail":null,"final_text":null},"event":null}}"#;

    let event: SessionEvent =
        serde_json::from_str(legacy_july).expect("a pre-`usage` agent snapshot must still load");
    let SessionEventKind::SubagentActivity {
        activity,
        agent,
        event: child,
    } = &event.kind
    else {
        panic!("expected subagent_activity, got {:?}", event.kind);
    };
    assert_eq!(*activity, crate::SubagentActivityKind::Started);
    assert_eq!(agent.task_name, "/root/smoke_child");
    assert_eq!(agent.status, crate::SubagentStatus::Starting);
    assert!(child.is_none());
    // The missing field defaults rather than failing the whole row.
    assert_eq!(agent.usage.total_tokens, 0);

    // A row whose child event is a kind we now keep live-only. Current code
    // will not write this again, but 3.4 GB of journal already contains it and
    // it must still load and still render the same agent state.
    let mirrored_provider_audit = r#"{"id":"0fe32fee-495d-4a9e-8271-6246f98ca113","session_id":"bd254d05-e129-4703-af86-68e9aafc3223","sequence":293527,"created_at":"2026-09-02T16:57:19.433426094Z","kind":{"type":"subagent_activity","activity":"updated","agent":{"session_id":"eee819c7-ae35-45c9-af95-cf3a3efc33b7","parent_session_id":"bd254d05-e129-4703-af86-68e9aafc3223","task_name":"/root/worker","status":"running","provider":"codex","model":"gpt-5.6-luna","effort":"max","cwd":"/home/shulgin/free-radicals","created_at":"2026-09-02T10:03:11.028415785Z","updated_at":"2026-09-02T16:57:19.423451668Z","detail":"turn phase: provider active","final_text":"a report","usage":{"input_tokens":1158104,"output_tokens":171134,"total_tokens":28811094,"context_tokens":124146,"cost_microusd":null,"cost_basis":"unavailable"}},"event":{"id":"42169e93-e4f8-4674-bb39-2e26ebdadb84","session_id":"eee819c7-ae35-45c9-af95-cf3a3efc33b7","sequence":0,"created_at":"2026-09-02T16:57:19.423451668Z","kind":{"type":"provider_event","provider":"codex","kind":"native_model_message","payload":{"content":"anything"}}}}}"#;

    // Kept for the round trip below, before the next row shadows `event`.
    let legacy_kind = event.kind.clone();

    let event: SessionEvent = serde_json::from_str(mirrored_provider_audit)
        .expect("an already-journaled provider-audit row must still load");
    let SessionEventKind::SubagentActivity {
        agent,
        event: Some(child),
        ..
    } = &event.kind
    else {
        panic!("expected a mirrored child event, got {:?}", event.kind);
    };
    assert_eq!(agent.usage.total_tokens, 28_811_094);
    assert_eq!(agent.detail.as_deref(), Some("turn phase: provider active"));
    assert!(matches!(
        child.kind,
        SessionEventKind::ProviderEvent { ref kind, .. } if kind == "native_model_message"
    ));
    // Reading it back is unaffected by the write rule; only new writes stop.
    assert_eq!(event.kind.persistence(), EventPersistence::Ephemeral);

    // Postgres stores the body rather than the wire text, so "still loads" has
    // to survive the store's own encode/decode round trip as well as serde.
    let parent_id = started(&store).await;
    store
        .append(SessionEvent::new(parent_id, 0, legacy_kind))
        .await
        .expect("a legacy snapshot must still be storable");
    let stored = store
        .read(parent_id)
        .await
        .expect("read the parent journal")
        .into_iter()
        .find_map(|stored| match stored.kind {
            SessionEventKind::SubagentActivity { agent, .. } => Some(agent),
            _ => None,
        })
        .expect("the legacy row must come back");
    assert_eq!(stored.task_name, "/root/smoke_child");
    assert_eq!(stored.status, crate::SubagentStatus::Starting);
    assert_eq!(
        stored.usage.total_tokens, 0,
        "the defaulted usage must survive the round trip rather than reappear as an error"
    );
    scratch.discard().await;
}

/// Keeping a mirrored child event out of the journal must not change what the
/// UI reconstructs from history. borg-ui's `rebuild_agents` walks the durable
/// history and lets the LAST `subagent_activity` per child win, so the risk is
/// that the dropped row was the last one and carried fresher agent state than
/// the row before it.
///
/// It does not, and the reason is structural: the agent snapshot only moves
/// when the child does transcript work, and those rows stay durable. Verified
/// against the live journal too - across the 25 most recent sessions, all 23
/// agents reconstruct a byte-identical final snapshot with and without the
/// dropped rows, even though 16 of them end on a row that is now dropped.
#[tokio::test]
async fn dropping_live_only_child_rows_does_not_change_the_reconstructed_roster() {
    let (scratch, store) = support::store().await;
    let parent_id = started(&store).await;
    let child_id = Uuid::new_v4();
    let snapshot = |detail: &str, total_tokens: u64| crate::SubagentSnapshot {
        session_id: child_id,
        parent_session_id: parent_id,
        task_name: "worker".to_string(),
        status: crate::SubagentStatus::Running,
        provider: CodingProvider::Codex,
        model: None,
        effort: None,
        cwd: std::path::PathBuf::from("/tmp"),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        detail: Some(detail.to_string()),
        final_text: None,
        usage: crate::SubagentUsage {
            total_tokens,
            ..Default::default()
        },
        interrupted_by: None,
    };
    let row = |agent: crate::SubagentSnapshot, child: Option<SessionEventKind>| {
        SessionEventKind::SubagentActivity {
            activity: crate::SubagentActivityKind::Updated,
            agent,
            event: child.map(|kind| Box::new(SessionEvent::new(child_id, 0, kind))),
        }
    };

    // The agent snapshot advances on transcript work, then the child emits
    // provider audit and capability rows that carry the SAME snapshot - which
    // is what the live journal actually looks like, and why the last row being
    // dropped is harmless.
    let settled = snapshot("ran a tool", 2_048);
    let history = vec![
        row(
            snapshot("starting", 0),
            Some(SessionEventKind::StatusChanged {
                status: crate::SessionStatus::Running,
                detail: None,
            }),
        ),
        row(
            settled.clone(),
            Some(SessionEventKind::ToolCompleted {
                tool_call_id: "call-1".to_string(),
                output: "ok".to_string(),
                output_ref: None,
                is_error: false,
                input: None,
                input_ref: None,
            }),
        ),
        // Both of these are now live-only, and both are LAST.
        row(
            settled.clone(),
            Some(SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "native_model_message".to_string(),
                payload: serde_json::json!({"content": "big"}),
            }),
        ),
        row(
            settled.clone(),
            Some(SessionEventKind::ProviderCapabilitiesUpdated {
                providers: Vec::new(),
            }),
        ),
    ];

    // Mirrors borg-ui rebuild_agents: last activity per child wins.
    let latest_agent = |events: &[SessionEventKind]| {
        events
            .iter()
            .filter_map(|kind| match kind {
                SessionEventKind::SubagentActivity { agent, .. } => Some(agent.clone()),
                _ => None,
            })
            .next_back()
            .expect("at least one activity")
    };

    let durable: Vec<SessionEventKind> = history
        .iter()
        .filter(|kind| kind.persistence() == EventPersistence::Durable)
        .cloned()
        .collect();

    assert_eq!(
        durable.len(),
        2,
        "the provider-audit and capability rows must be live-only"
    );
    let from_everything = latest_agent(&history);
    let from_journal = latest_agent(&durable);
    assert_eq!(from_journal.detail, from_everything.detail);
    assert_eq!(
        from_journal.usage.total_tokens,
        from_everything.usage.total_tokens
    );
    assert_eq!(from_journal.status, from_everything.status);
    assert_eq!(from_journal.session_id, from_everything.session_id);

    // The same claim against the store that actually rebuilds the roster: the
    // whole history goes in, the live-only rows are dropped on the way, and the
    // roster slice still reconstructs the snapshot the parent would have seen
    // with every row present.
    for kind in history.iter().cloned() {
        store
            .append(SessionEvent::new(parent_id, 0, kind))
            .await
            .expect("mirrored child row");
    }
    let recovered = store
        .recovery_parts(parent_id, RecoveryParts::SUBAGENTS)
        .await
        .expect("roster recovery");
    let recovered_kinds: Vec<SessionEventKind> = recovered
        .subagent_events
        .into_iter()
        .map(|event| event.kind)
        .collect();
    assert_eq!(
        recovered_kinds.len(),
        1,
        "the roster keeps one row per child, got {recovered_kinds:?}"
    );
    let from_store = latest_agent(&recovered_kinds);
    assert_eq!(from_store.session_id, child_id);
    assert_eq!(from_store.detail, from_everything.detail);
    assert_eq!(
        from_store.usage.total_tokens,
        from_everything.usage.total_tokens
    );
    assert_eq!(from_store.status, from_everything.status);
    scratch.discard().await;
}
