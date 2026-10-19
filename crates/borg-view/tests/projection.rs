//! Protect shared projection/restart and bounded resumption contracts; compiler checks cannot
//! catch semantic divergence, UTF-8 offsets or growing-history wire costs.
use borg_agent_runtime::{EventActor, MessageStatus, SessionEvent, SessionEventKind};
use borg_view::{
    ViewReducer,
    wire::{RowBody, SNAPSHOT_BYTES, SNAPSHOT_ROWS, ViewOp},
};
use std::collections::HashMap;
use uuid::Uuid;

fn message(
    session: Uuid,
    sequence: u64,
    message: Uuid,
    actor: EventActor,
    text: &str,
    status: MessageStatus,
) -> SessionEvent {
    SessionEvent::new(
        session,
        sequence,
        SessionEventKind::Message {
            message_id: message,
            actor,
            text: text.into(),
            status,
            attachments: Vec::new(),
            delivery: None,
        },
    )
}
#[test]
fn utf8_append_survives_preview_and_checkpoint_restore() {
    let session = Uuid::new_v4();
    let id = Uuid::new_v4();
    let mut reducer = ViewReducer::new(session, "epoch".into());
    let body = "λ".repeat(3000);
    reducer.apply(message(
        session,
        1,
        id,
        EventActor::Assistant,
        &body,
        MessageStatus::InProgress,
    ));
    let before = reducer.snapshot();
    assert!(before.rows[0].text_state.as_ref().unwrap().asset.is_some());
    assert_eq!(before.rows[0].text_state.as_ref().unwrap().bytes, "6000");
    let texts = reducer
        .checkpoint_texts()
        .into_iter()
        .map(|(r, t)| ((r.row_id, format!("{:?}", r.field)), t.to_string()))
        .collect::<HashMap<_, _>>();
    let checkpoint = reducer.checkpoint().unwrap();
    assert!(checkpoint.len() < 64 * 1024);
    assert!(!String::from_utf8_lossy(&checkpoint).contains(&body));
    let mut restored = ViewReducer::restore(&checkpoint, |r| {
        Ok(texts[&(r.row_id.clone(), format!("{:?}", r.field))].clone())
    })
    .unwrap();
    let event = SessionEvent::new(
        session,
        2,
        SessionEventKind::MessageDelta {
            message_id: id,
            delta: "🦀".into(),
        },
    );
    let live_ops = reducer.apply(event.clone());
    let restored_ops = restored.apply(event);
    assert_eq!(
        serde_json::to_value(&live_ops).unwrap(),
        serde_json::to_value(&restored_ops).unwrap()
    );
    assert!(
        live_ops
            .iter()
            .any(|op| matches!(op,ViewOp::RowAppend {offset:6000,text,..} if text=="🦀"))
    );
    assert_eq!(
        restored.snapshot().rows[0]
            .text_state
            .as_ref()
            .unwrap()
            .bytes,
        "6004"
    );
}
#[test]
fn hundred_thousand_events_keep_snapshot_and_active_state_bounded() {
    let session = Uuid::new_v4();
    let mut reducer = ViewReducer::new(session, "epoch".into());
    for sequence in 1..=100_000 {
        reducer.apply(message(
            session,
            sequence,
            Uuid::new_v4(),
            EventActor::User,
            "small prompt",
            MessageStatus::Complete,
        ));
    }
    let snapshot = reducer.snapshot();
    assert!(snapshot.rows.len() <= SNAPSHOT_ROWS);
    assert!(serde_json::to_vec(&snapshot).unwrap().len() <= SNAPSHOT_BYTES);
    assert!(snapshot.history_anchor.is_some());
    assert!(reducer.core.order.len() <= 128);
    assert!(reducer.core.messages.len() <= 128);
    assert!(reducer.checkpoint().unwrap().len() <= SNAPSHOT_BYTES);
}
#[test]
fn system_provider_input_is_hidden_and_unknown_protocol_rejected() {
    let session = Uuid::new_v4();
    let mut reducer = ViewReducer::new(session, "epoch".into());
    reducer.apply(message(
        session,
        1,
        Uuid::new_v4(),
        EventActor::System,
        "private provider input",
        MessageStatus::Complete,
    ));
    assert!(reducer.snapshot().rows.is_empty());
    assert!(
        serde_json::from_str::<borg_view::wire::ServerFrame>(
            r#"{"type":"unknown","cursor":{"epoch":"e","revision":"99"}}"#
        )
        .is_err()
    );
    let closed = borg_view::intent::parse_submission_intent(
        "/quit",
        borg_agent_runtime::CodingProvider::Codex,
        borg_agent_runtime::PromptDelivery::Steer,
        &[],
    )
    .unwrap();
    assert!(matches!(
        closed,
        borg_view::intent::SubmissionIntent::Client(borg_view::wire::ClientEffect::CloseView)
    ));
    assert!(
        !reducer
            .snapshot()
            .rows
            .iter()
            .any(|r| matches!(r.body, RowBody::Message { .. }))
    );
}

#[test]
fn parent_has_one_child_summary_and_source_scoped_approval_without_child_bodies() {
    use borg_agent_runtime::{
        CodingProvider, SubagentActivityKind, SubagentSnapshot, SubagentStatus, SubagentUsage,
    };
    let parent = Uuid::new_v4();
    let child = Uuid::new_v4();
    let mut reducer = ViewReducer::new(parent, "epoch".into());
    let now = chrono::Utc::now();
    let agent = SubagentSnapshot {
        lifecycle: None,
        source: None,
        desktop: false,
        session_id: child,
        parent_session_id: parent,
        task_name: "worker".into(),
        status: SubagentStatus::Running,
        provider: CodingProvider::Codex,
        model: None,
        effort: None,
        fast: false,
        ultrafast: false,
        cwd: ".".into(),
        created_at: now,
        updated_at: now,
        detail: None,
        final_text: Some("PRIVATE CHILD BODY".into()),
        usage: SubagentUsage::default(),
        interrupted_by: None,
    };
    reducer.apply(SessionEvent::new(
        parent,
        1,
        SessionEventKind::SubagentActivity {
            activity: SubagentActivityKind::Started,
            agent: agent.clone(),
            event: None,
        },
    ));
    let approval = SessionEvent::new(
        child,
        1,
        SessionEventKind::ApprovalRequested {
            approval_id: "approval".into(),
            title: "Review".into(),
            detail: "safe command detail".into(),
            command: None,
        },
    );
    let revision = approval.id.to_string();
    reducer.apply(SessionEvent::new(
        parent,
        2,
        SessionEventKind::SubagentActivity {
            activity: SubagentActivityKind::Updated,
            agent,
            event: Some(Box::new(approval)),
        },
    ));
    let snapshot = reducer.snapshot();
    assert_eq!(
        snapshot
            .rows
            .iter()
            .filter(|row| row.kind == borg_view::wire::RowKind::ActionGroup)
            .count(),
        1
    );
    assert_eq!(snapshot.rows[0].source_session_id, parent.to_string());
    assert!(
        matches!(&snapshot.rows[0].body, RowBody::ActionGroup { actions, .. }
        if matches!(&actions[0], borg_view::wire::ActionRow::Lifecycle { target_session_id: Some(target), .. } if target == &child.to_string()))
    );
    assert_eq!(
        snapshot.meta.approvals[0].source_session_id,
        child.to_string()
    );
    assert_eq!(snapshot.meta.approvals[0].target_revision, revision);
    assert!(
        !serde_json::to_string(&snapshot)
            .unwrap()
            .contains("PRIVATE CHILD BODY")
    );

    // Old child rows leave the active window; their metadata must still advance
    // without resurrecting a historical row or poisoning the parent cursor.
    reducer
        .apply_child_meta(
            child,
            borg_view::wire::Cursor {
                epoch: "child-epoch".into(),
                revision: "0".into(),
            },
            &borg_view::wire::Meta {
                status: Some(borg_agent_runtime::SessionStatus::Ready),
                ..Default::default()
            },
        )
        .unwrap();
    for sequence in 3..=132 {
        reducer.apply(message(
            parent,
            sequence,
            Uuid::new_v4(),
            EventActor::User,
            "",
            MessageStatus::Complete,
        ));
    }
    assert!(!reducer.core.subagent_entries.contains_key(&child));
    let checkpoint = reducer.checkpoint().unwrap();
    let mut reducer = ViewReducer::restore(&checkpoint, |_| Ok(String::new())).unwrap();
    let previous: u64 = reducer.cursor().revision.parse().unwrap();
    let meta = borg_view::wire::Meta {
        status: Some(borg_agent_runtime::SessionStatus::Failed),
        ..Default::default()
    };
    let cursor = borg_view::wire::Cursor {
        epoch: "child-epoch".into(),
        revision: "1".into(),
    };
    let ops = reducer
        .apply_child_meta(child, cursor.clone(), &meta)
        .unwrap();
    assert_eq!(reducer.cursor().revision, (previous + 1).to_string());
    assert!(ops.iter().all(|op| !matches!(op, ViewOp::RowUpsert { .. })));
    let snapshot = reducer.snapshot();
    assert_eq!(snapshot.meta.children.len(), 1);
    assert!(matches!(
        snapshot.meta.children[0].status,
        borg_view::wire::Lifecycle::Failed
    ));
    assert!(
        reducer
            .apply_child_meta(child, cursor, &meta)
            .unwrap()
            .is_empty()
    );
}

/// A child report must stay private even when the reducer no longer holds
/// the child's snapshot (bounded cache eviction or a cold replay that starts
/// after the child was spawned); only a genuine peer message becomes visible.
#[test]
fn recorded_child_ownership_hides_reports_from_uncached_children() {
    let parent = Uuid::new_v4();
    let mut reducer = ViewReducer::new(parent, "epoch".into());
    let receipt = |sequence, text: &str, from_child| {
        SessionEvent::new(
            parent,
            sequence,
            SessionEventKind::AgentMessageReceived {
                message_id: Uuid::new_v4(),
                sender_id: Uuid::new_v4(),
                sender_name: "sender".into(),
                text: text.into(),
                from_child: Some(from_child),
            },
        )
    };
    reducer.apply(receipt(1, "PRIVATE CHILD REPORT", true));
    reducer.apply(receipt(2, "visible peer note", false));
    let snapshot = serde_json::to_string(&reducer.snapshot()).unwrap();
    assert!(!snapshot.contains("PRIVATE CHILD REPORT"));
    assert!(snapshot.contains("visible peer note"));
}

// These exercise the public projection/replay contract: Rust's type system
// cannot catch a valid Complete card that contradicts a Running roster.
#[test]
fn worker_lifecycle_is_independent_of_reports_and_stale_deliveries() {
    use borg_agent_runtime::{
        CodingProvider, SubagentActivityKind, SubagentLifecycle, SubagentSnapshot,
        SubagentSourceCursor, SubagentStatus, SubagentUsage,
    };
    let parent = Uuid::new_v4();
    let child = Uuid::new_v4();
    let now = chrono::Utc::now();
    let mut agent = SubagentSnapshot {
        session_id: child,
        parent_session_id: parent,
        task_name: "worker".into(),
        status: SubagentStatus::Running,
        lifecycle: Some(SubagentLifecycle::Running),
        source: Some(SubagentSourceCursor {
            child_sequence: 10,
            revision: 1,
        }),
        provider: CodingProvider::Codex,
        model: None,
        effort: None,
        fast: false,
        ultrafast: false,
        desktop: false,
        cwd: ".".into(),
        created_at: now,
        updated_at: now,
        detail: None,
        final_text: None,
        usage: SubagentUsage::default(),
        interrupted_by: None,
    };
    let event = |sequence, agent: &SubagentSnapshot, child_event| {
        SessionEvent::new(
            parent,
            sequence,
            SessionEventKind::SubagentActivity {
                activity: SubagentActivityKind::Updated,
                agent: agent.clone(),
                event: child_event,
            },
        )
    };
    let mut live = ViewReducer::new(parent, "live".into());
    let mut history = vec![event(1, &agent, None)];
    live.apply(history[0].clone());
    agent.source = Some(SubagentSourceCursor {
        child_sequence: 11,
        revision: 2,
    });
    let commentary = message(
        child,
        11,
        Uuid::new_v4(),
        EventActor::Assistant,
        "I will run tests next.",
        MessageStatus::Complete,
    );
    history.push(event(2, &agent, Some(Box::new(commentary))));
    live.apply(history[1].clone());
    assert!(matches!(
        live.snapshot().meta.children[0].status,
        borg_view::wire::Lifecycle::Running
    ));
    assert!(matches!(
        &live.core.order[0],
        borg_view::entry::TranscriptEntry::Action {
            execution,
            ..
        } if execution.state() == borg_view::entry::ExecutionState::Running
    ));

    let mut stale = agent.clone();
    stale.status = SubagentStatus::Ready;
    stale.lifecycle = Some(SubagentLifecycle::Ready);
    stale.source.as_mut().unwrap().revision = 1;
    stale.updated_at = now + chrono::Duration::hours(1);
    history.push(event(3, &stale, None));
    live.apply(history[2].clone());
    assert_eq!(live.core.subagents[&child], SubagentStatus::Running);
    assert!(matches!(
        &live.core.order[0],
        borg_view::entry::TranscriptEntry::Action {
            execution,
            ..
        } if execution.state() == borg_view::entry::ExecutionState::Running
    ));

    let stale_approval = SessionEvent::new(
        child,
        10,
        SessionEventKind::ApprovalRequested {
            approval_id: "obsolete".into(),
            title: "obsolete action".into(),
            detail: String::new(),
            command: None,
        },
    );
    live.apply(event(4, &stale, Some(Box::new(stale_approval))));
    assert!(
        live.snapshot().meta.approvals.is_empty(),
        "stale snapshots cannot resurrect approvals"
    );

    agent.status = SubagentStatus::Ready;
    agent.lifecycle = Some(SubagentLifecycle::WaitingOnWatchers);
    agent.source = Some(SubagentSourceCursor {
        child_sequence: 12,
        revision: 3,
    });
    agent.detail = Some("Waiting on 1 watcher(s): tests".into());
    history.push(event(5, &agent, None));
    live.apply(history[3].clone());
    assert_eq!(live.core.waiting_subagent_count(), 1);
    assert!(matches!(
        live.snapshot().meta.children[0].status,
        borg_view::wire::Lifecycle::Waiting
    ));
    assert!(matches!(
        &live.core.order[0],
        borg_view::entry::TranscriptEntry::Action {
            execution,
            ..
        } if execution.state() == borg_view::entry::ExecutionState::Waiting
    ));
    let mut replay = ViewReducer::new(parent, "replay".into());
    for event in history {
        replay.apply(event);
    }
    assert_eq!(
        serde_json::to_value(&live.snapshot().meta.children).unwrap(),
        serde_json::to_value(&replay.snapshot().meta.children).unwrap()
    );
    assert_eq!(
        live.core.order.len(),
        1,
        "worker summary is not an operation receipt per update"
    );
    agent.status = SubagentStatus::Ready;
    agent.lifecycle = Some(SubagentLifecycle::Ready);
    agent.source = Some(SubagentSourceCursor {
        child_sequence: 13,
        revision: 4,
    });
    live.apply(event(6, &agent, None));
    let terminal = serde_json::to_value(&live.core.order[0]).unwrap();
    let terminal_wire = serde_json::to_value(&live.snapshot().rows[0]).unwrap();
    let identity = live.core.order.identities[0];
    agent.status = SubagentStatus::Running;
    agent.lifecycle = Some(SubagentLifecycle::Running);
    agent.source = Some(SubagentSourceCursor {
        child_sequence: 14,
        revision: 5,
    });
    live.apply(event(7, &agent, None));
    assert_eq!(
        live.core.order.len(),
        2,
        "waking a reusable child starts a fresh execution row"
    );
    assert_eq!(serde_json::to_value(&live.core.order[0]).unwrap(), terminal);
    assert_ne!(live.core.order.identities[1], identity);
    let snapshot = live.snapshot();
    assert_eq!(
        serde_json::to_value(&snapshot.rows[0]).unwrap(),
        terminal_wire,
        "old wire receipt must not change ID, body, state or revision when the entity wakes"
    );
    assert_ne!(snapshot.rows[0].id, snapshot.rows[1].id);
    assert!(
        snapshot
            .rows
            .iter()
            .all(|row| row.source_session_id == parent.to_string())
    );
    assert!(snapshot.rows.iter().all(|row| matches!(&row.body, RowBody::ActionGroup { actions, .. }
        if matches!(&actions[0], borg_view::wire::ActionRow::Lifecycle { target_session_id: Some(target), .. } if target == &child.to_string()))));
    assert_eq!(live.core.subagents[&child], SubagentStatus::Running);
}

#[test]
fn child_metadata_and_coordinator_state_share_a_child_journal_watermark() {
    use borg_agent_runtime::{
        CodingProvider, SessionStatus, SubagentActivityKind, SubagentLifecycle, SubagentSnapshot,
        SubagentSourceCursor, SubagentStatus, SubagentUsage,
    };
    use borg_view::wire::{Cursor, Lifecycle, Meta};
    let parent = Uuid::new_v4();
    let child = Uuid::new_v4();
    let now = chrono::Utc::now();
    let mut agent = SubagentSnapshot {
        session_id: child,
        parent_session_id: parent,
        task_name: "worker".into(),
        status: SubagentStatus::Ready,
        lifecycle: Some(SubagentLifecycle::Dormant),
        source: Some(SubagentSourceCursor {
            child_sequence: 20,
            revision: 1,
        }),
        provider: CodingProvider::Codex,
        model: None,
        effort: None,
        fast: false,
        ultrafast: false,
        desktop: false,
        cwd: ".".into(),
        created_at: now,
        updated_at: now,
        detail: None,
        final_text: None,
        usage: SubagentUsage::default(),
        interrupted_by: None,
    };
    let mut view = ViewReducer::new(parent, "parent".into());
    let publish = |sequence, agent: &SubagentSnapshot| {
        SessionEvent::new(
            parent,
            sequence,
            SessionEventKind::SubagentActivity {
                activity: SubagentActivityKind::Updated,
                agent: agent.clone(),
                event: None,
            },
        )
    };
    view.apply(publish(1, &agent));
    let mut meta = Meta {
        source_sequence: Some("20".into()),
        status: Some(SessionStatus::Running),
        status_lifecycle: Some(SubagentLifecycle::Running),
        ..Meta::default()
    };
    view.apply_child_meta(
        child,
        Cursor {
            epoch: "child".into(),
            revision: "100".into(),
        },
        &meta,
    )
    .unwrap();
    assert!(matches!(
        view.snapshot().meta.children[0].status,
        Lifecycle::Waiting
    ));
    meta.source_sequence = Some("21".into());
    assert!(
        !view
            .apply_child_meta(
                child,
                Cursor {
                    epoch: "child".into(),
                    revision: "101".into()
                },
                &meta
            )
            .unwrap()
            .is_empty()
    );
    assert_eq!(view.core.subagents[&child], SubagentStatus::Running);
    assert!(matches!(
        view.snapshot().meta.children[0].status,
        Lifecycle::Running
    ));
    let mut old_meta = meta.clone();
    old_meta.source_sequence = Some("20".into());
    old_meta.status = Some(SessionStatus::Ready);
    old_meta.status_lifecycle = Some(SubagentLifecycle::Ready);
    assert!(
        view.apply_child_meta(
            child,
            Cursor {
                epoch: "rebuilt-child-projector".into(),
                revision: "999".into()
            },
            &old_meta
        )
        .unwrap()
        .is_empty()
    );
    assert!(
        matches!(view.snapshot().meta.children[0].status, Lifecycle::Running),
        "a new projector epoch cannot regress child state"
    );
    agent.source = Some(SubagentSourceCursor {
        child_sequence: 22,
        revision: 2,
    });
    view.apply(publish(2, &agent));
    assert_eq!(
        view.core
            .effective_subagent_snapshot(child)
            .unwrap()
            .lifecycle(),
        SubagentLifecycle::Dormant
    );
    assert_eq!(view.core.subagents[&child], SubagentStatus::Ready);
    assert!(matches!(
        view.snapshot().meta.children[0].status,
        Lifecycle::Waiting
    ));
    let checkpoint = view.checkpoint().unwrap();
    let mut restored = ViewReducer::restore(&checkpoint, |_| Ok(String::new())).unwrap();
    assert!(matches!(
        restored.snapshot().meta.children[0].status,
        Lifecycle::Waiting
    ));
    restored
        .apply_child_meta(
            child,
            Cursor {
                epoch: "child".into(),
                revision: "102".into(),
            },
            &meta,
        )
        .unwrap();
    assert!(
        matches!(
            restored.snapshot().meta.children[0].status,
            Lifecycle::Waiting
        ),
        "late old metadata cannot undo pause after checkpoint replay"
    );
}

#[test]
fn stale_child_metadata_cannot_erase_a_canonical_approval() {
    use borg_view::wire::{Cursor, Meta};
    let parent = Uuid::new_v4();
    let child = Uuid::new_v4();
    let now = chrono::Utc::now();
    let agent = borg_agent_runtime::SubagentSnapshot {
        session_id: child,
        parent_session_id: parent,
        task_name: "worker".into(),
        status: borg_agent_runtime::SubagentStatus::WaitingForApproval,
        lifecycle: Some(borg_agent_runtime::SubagentLifecycle::WaitingForApproval),
        source: Some(borg_agent_runtime::SubagentSourceCursor {
            child_sequence: 100,
            revision: 1,
        }),
        provider: borg_agent_runtime::CodingProvider::Codex,
        model: None,
        effort: None,
        fast: false,
        ultrafast: false,
        desktop: false,
        cwd: ".".into(),
        created_at: now,
        updated_at: now,
        detail: None,
        final_text: None,
        usage: Default::default(),
        interrupted_by: None,
    };
    let mut view = ViewReducer::new(parent, "parent".into());
    view.apply(SessionEvent::new(
        parent,
        1,
        SessionEventKind::SubagentActivity {
            activity: borg_agent_runtime::SubagentActivityKind::Updated,
            agent: agent.clone(),
            event: Some(Box::new(SessionEvent::new(
                child,
                100,
                SessionEventKind::ApprovalRequested {
                    approval_id: "current".into(),
                    title: "current approval".into(),
                    detail: String::new(),
                    command: None,
                },
            ))),
        },
    ));
    assert_eq!(view.snapshot().meta.children[0].approvals.len(), 1);
    view.apply_child_meta(
        child,
        Cursor {
            epoch: "child".into(),
            revision: "1".into(),
        },
        &Meta {
            source_sequence: Some("99".into()),
            ..Meta::default()
        },
    )
    .unwrap();
    assert_eq!(view.snapshot().meta.approvals.len(), 1);
    assert_eq!(view.snapshot().meta.children[0].approvals[0].id, "current");
    let mut resolved = agent;
    resolved.status = borg_agent_runtime::SubagentStatus::Running;
    resolved.lifecycle = Some(borg_agent_runtime::SubagentLifecycle::Running);
    resolved.source = Some(borg_agent_runtime::SubagentSourceCursor {
        child_sequence: 101,
        revision: 2,
    });
    view.apply(SessionEvent::new(
        parent,
        2,
        SessionEventKind::SubagentActivity {
            activity: borg_agent_runtime::SubagentActivityKind::Updated,
            agent: resolved,
            event: None,
        },
    ));
    assert!(view.snapshot().meta.approvals.is_empty());
    assert!(view.snapshot().meta.children[0].approvals.is_empty());
}

// A background receipt admits work; only its terminal event settles nested actions.
// Replay must preserve that distinction rather than leaving completed children blue.
#[test]
fn nested_background_receipt_waits_for_terminal_completion_live_and_replay() {
    use borg_view::{core::TranscriptCore, entry::TranscriptEntry};
    let session = Uuid::new_v4();
    let watch = Uuid::new_v4();
    let mut events = Vec::new();
    for (id, parent) in [("outer", None), ("inner", Some("outer"))] {
        events.push(SessionEvent::new(
            session,
            events.len() as u64 + 1,
            SessionEventKind::ToolStarted {
                tool_call_id: id.into(),
                name: "runtime_exec".into(),
                input: serde_json::json!({"action":"check"}),
                input_ref: None,
                parent_tool_call_id: parent.map(str::to_string),
            },
        ));
    }
    events.push(SessionEvent::new(
        session,
        3,
        SessionEventKind::ToolCompleted {
            tool_call_id: "outer".into(),
            output: serde_json::json!({"running":true,"background":true,"watch_id":watch})
                .to_string(),
            output_ref: None,
            is_error: false,
            input: None,
            input_ref: None,
            parent_tool_call_id: None,
        },
    ));
    let mut live = TranscriptCore::default();
    for event in &events {
        live.apply(event);
    }
    assert!(matches!(
        &live.order[1],
        TranscriptEntry::Tool { execution, .. } if !execution.is_terminal()
    ));
    let settled_watch = SessionEvent::new(
        session,
        4,
        SessionEventKind::WatchesChanged {
            watches: vec![borg_agent_runtime::WatchSummary {
                watch_id: watch,
                label: "check".into(),
                command: "runtime_exec".into(),
                running: false,
                started_at: events[0].created_at,
                last_event_at: Some(events[2].created_at),
                event_count: 1,
            }],
        },
    );
    let mut watched = TranscriptCore::default();
    for event in &events {
        watched.apply_history(event);
    }
    watched.apply_history(&settled_watch);
    assert!(watched.order.iter().all(|row| matches!(
        row,
        TranscriptEntry::Tool { execution, .. } if execution.is_terminal() && !execution.is_failed()
    )));
    events.push(SessionEvent::new(
        session,
        4,
        SessionEventKind::ToolCompleted {
            tool_call_id: "outer".into(),
            output: "done".into(),
            output_ref: None,
            is_error: true,
            input: None,
            input_ref: None,
            parent_tool_call_id: None,
        },
    ));
    live.apply(events.last().unwrap());
    let mut replay = TranscriptCore::default();
    for event in &events {
        replay.apply_history(event);
    }
    for projection in [&live, &replay] {
        assert!(projection.order.iter().all(|row| matches!(
            row,
            TranscriptEntry::Tool { execution, .. } if execution.is_failed()
        )));
    }
}

// A compiler check cannot prove absorbing terminal transitions or immutable timing.
#[test]
fn execution_terminal_state_and_clock_are_absorbing() {
    use borg_view::entry::{Execution, ExecutionState};
    let start = chrono::Utc::now();
    for terminal in [
        ExecutionState::Complete,
        ExecutionState::Received,
        ExecutionState::Failed,
        ExecutionState::Stopped,
    ] {
        let mut execution = Execution::new(start, ExecutionState::Running);
        assert!(execution.transition(ExecutionState::Backgrounded, start));
        assert!(!execution.is_terminal());
        assert_eq!(execution.completed_at(), None);
        let end = start + chrono::Duration::seconds(3);
        assert!(execution.transition(terminal, end));
        let settled = execution.clone();
        for next in [
            ExecutionState::Running,
            ExecutionState::Waiting,
            ExecutionState::Backgrounded,
            ExecutionState::Complete,
            ExecutionState::Failed,
        ] {
            assert!(!execution.transition(next, end + chrono::Duration::seconds(8)));
            assert_eq!(execution, settled);
        }
        let serialized = serde_json::to_value(&execution).unwrap();
        assert_eq!(
            serde_json::from_value::<Execution>(serialized.clone()).unwrap(),
            execution
        );
        let mut invalid = serialized;
        invalid["completed_at"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<Execution>(invalid).is_err());
    }
}

// Late completion used to replace the result/timer; restarting reused the old identity.
#[test]
fn terminal_tool_ignores_late_updates_and_new_start_gets_fresh_identity() {
    use borg_view::{core::TranscriptCore, entry::TranscriptEntry};
    let session = Uuid::new_v4();
    let mut core = TranscriptCore::default();
    let start = |sequence| {
        SessionEvent::new(
            session,
            sequence,
            SessionEventKind::ToolStarted {
                tool_call_id: "call".into(),
                name: "exec".into(),
                input: serde_json::json!({"cmd":"true"}),
                input_ref: None,
                parent_tool_call_id: None,
            },
        )
    };
    let finish = |sequence, output: &str| {
        SessionEvent::new(
            session,
            sequence,
            SessionEventKind::ToolCompleted {
                tool_call_id: "call".into(),
                output: output.into(),
                is_error: false,
                output_ref: None,
                input: None,
                input_ref: None,
                parent_tool_call_id: None,
            },
        )
    };
    core.apply(&start(1));
    core.apply(&finish(2, "done"));
    let identity = core.order.identities[0];
    let settled = serde_json::to_value(&core.order[0]).unwrap();
    core.apply(&finish(3, "late replacement"));
    core.apply(&SessionEvent::new(
        session,
        4,
        SessionEventKind::ToolUpdated {
            tool_call_id: "call".into(),
            name: "exec".into(),
            input: serde_json::json!({"cmd":"false"}),
        },
    ));
    assert_eq!(serde_json::to_value(&core.order[0]).unwrap(), settled);
    core.apply(&start(5));
    assert_eq!(
        core.order.len(),
        1,
        "a repeated start for the same call is not a new execution"
    );
    let mut fresh = start(6);
    if let SessionEventKind::ToolStarted { tool_call_id, .. } = &mut fresh.kind {
        *tool_call_id = "fresh-call".into();
    }
    core.apply(&fresh);
    assert_eq!(core.order.len(), 2);
    assert_eq!(core.order.identities[0], identity);
    assert_ne!(core.order.identities[1], identity);
    assert!(
        matches!(&core.order[1], TranscriptEntry::Tool { execution, .. } if !execution.is_terminal())
    );
}

// Lifecycle resolution must settle the transcript as well as remove metadata badges.
#[test]
fn approval_resolution_and_broadcast_completion_are_terminal() {
    use borg_agent_runtime::ApprovalDecision;
    use borg_view::{core::TranscriptCore, entry::TranscriptEntry};
    let session = Uuid::new_v4();
    let mut core = TranscriptCore::default();
    core.apply(&SessionEvent::new(
        session,
        1,
        SessionEventKind::ApprovalRequested {
            approval_id: "approval".into(),
            title: "Run command".into(),
            detail: String::new(),
            command: None,
        },
    ));
    core.apply(&SessionEvent::new(
        session,
        2,
        SessionEventKind::ApprovalResolved {
            approval_id: "approval".into(),
            decision: ApprovalDecision::Deny,
        },
    ));
    assert!(core.meta.approvals.is_empty());
    assert!(
        matches!(&core.order[0], TranscriptEntry::Action { execution, .. } if execution.is_interrupted())
    );
    let id = Uuid::new_v4();
    let update = |sequence, acknowledged| {
        SessionEvent::new(
            session,
            sequence,
            SessionEventKind::TeamBroadcastUpdated {
                message_id: id,
                text: "work".into(),
                recipient_ids: vec![Uuid::nil()],
                acknowledged,
                steered: false,
            },
        )
    };
    core.apply(&update(3, 1));
    let settled = serde_json::to_value(&core.order[1]).unwrap();
    core.apply(&update(4, 0));
    assert_eq!(serde_json::to_value(&core.order[1]).unwrap(), settled);
}

// Failed partial provider snapshots are not terminal until a turn boundary or final answer.
#[test]
fn message_transport_failure_does_not_reopen_terminal_execution() {
    use borg_view::{core::TranscriptCore, entry::TranscriptEntry};
    let session = Uuid::new_v4();
    let id = Uuid::new_v4();
    let mut core = TranscriptCore::default();
    core.apply(&message(
        session,
        1,
        id,
        EventActor::Assistant,
        "partial",
        MessageStatus::Failed,
    ));
    assert!(
        matches!(&core.order[0], TranscriptEntry::Message { execution, .. } if !execution.is_terminal())
    );
    core.apply(&message(
        session,
        2,
        id,
        EventActor::Assistant,
        "final",
        MessageStatus::Complete,
    ));
    let terminal = serde_json::to_value(&core.order[0]).unwrap();
    core.apply(&message(
        session,
        3,
        id,
        EventActor::Assistant,
        "late",
        MessageStatus::Complete,
    ));
    core.apply(&message(
        session,
        4,
        id,
        EventActor::Assistant,
        "late failure",
        MessageStatus::Failed,
    ));
    assert_eq!(serde_json::to_value(&core.order[0]).unwrap(), terminal);
}

// The process lifetime, not tool transport or turn completion, owns terminal timing.
#[test]
fn active_process_survives_transport_return_and_interruption_until_exit() {
    use borg_agent_runtime::RuntimeProcessStatus;
    use borg_view::{core::TranscriptCore, entry::TranscriptEntry};
    let session = Uuid::new_v4();
    let process_id = Uuid::new_v4();
    let mut core = TranscriptCore::default();
    core.apply(&SessionEvent::new(
        session,
        1,
        SessionEventKind::ToolStarted {
            tool_call_id: "process".into(),
            name: "exec".into(),
            input: serde_json::json!({"cmd":"long-test"}),
            input_ref: None,
            parent_tool_call_id: None,
        },
    ));
    core.apply(&SessionEvent::new(
        session,
        2,
        SessionEventKind::RuntimeProcessStarted {
            process_id,
            pid: 42,
            command: "long-test".into(),
            cwd: ".".into(),
        },
    ));
    core.apply(&SessionEvent::new(
        session,
        3,
        SessionEventKind::ToolCompleted {
            tool_call_id: "process".into(),
            output: "transport returned without a background marker".into(),
            is_error: false,
            output_ref: None,
            input: None,
            input_ref: None,
            parent_tool_call_id: None,
        },
    ));
    core.mark_running_tools_user_interrupted(chrono::Utc::now());
    let TranscriptEntry::Tool { execution, .. } = &core.order[0] else {
        panic!("missing process tool")
    };
    assert!(execution.is_backgrounded());
    assert_eq!(execution.completed_at(), None);
    core.apply(&SessionEvent::new(
        session,
        4,
        SessionEventKind::RuntimeProcessCompleted {
            process_id,
            pid: 42,
            status: RuntimeProcessStatus::TimedOut,
            exit_code: None,
            timed_out: true,
            stdout: "partial output".into(),
            stderr: String::new(),
            stdout_omitted_bytes: 0,
            stderr_omitted_bytes: 0,
            error: None,
            changes: Vec::new(),
        },
    ));
    assert!(
        matches!(&core.order[0], TranscriptEntry::Tool { execution, .. } if execution.is_failed() && execution.completed_at().is_some())
    );
}

// A bounded live window must not forget admission when a terminal row is evicted.
#[test]
fn live_replay_cannot_resurrect_evicted_executions() {
    let session = Uuid::new_v4();
    let mut view = ViewReducer::new(session, "epoch".into());
    let started = SessionEvent::new(
        session,
        1,
        SessionEventKind::ToolStarted {
            tool_call_id: "old-call".into(),
            name: "exec".into(),
            input: serde_json::json!({"cmd":"true"}),
            input_ref: None,
            parent_tool_call_id: None,
        },
    );
    let completed = SessionEvent::new(
        session,
        2,
        SessionEventKind::ToolCompleted {
            tool_call_id: "old-call".into(),
            output: "done".into(),
            is_error: false,
            input: None,
            input_ref: None,
            output_ref: None,
            parent_tool_call_id: None,
        },
    );
    view.apply(started.clone());
    view.apply(completed.clone());
    for sequence in 3..140 {
        view.apply(message(
            session,
            sequence,
            Uuid::new_v4(),
            EventActor::Assistant,
            "prompt",
            MessageStatus::Complete,
        ));
    }
    assert!(!view.core.tools.contains_key("old-call"));
    let before = serde_json::to_value(&view.core.order).unwrap();
    view.apply(started);
    view.apply(completed);
    assert_eq!(serde_json::to_value(&view.core.order).unwrap(), before);
}

// Historical display ordering is not journal ordering, but subsequent live input is.
#[test]
fn reordered_history_preserves_earlier_output_and_fences_live_replay() {
    use borg_view::{core::TranscriptCore, entry::TranscriptEntry};
    let session = Uuid::new_v4();
    let mut core = TranscriptCore::default();
    core.apply_history(&message(
        session,
        10,
        Uuid::new_v4(),
        EventActor::User,
        "forked prompt",
        MessageStatus::Complete,
    ));
    let start = SessionEvent::new(
        session,
        2,
        SessionEventKind::ToolStarted {
            tool_call_id: "historical".into(),
            name: "exec".into(),
            input: serde_json::json!({"cmd":"true"}),
            input_ref: None,
            parent_tool_call_id: None,
        },
    );
    core.apply_history(&start);
    core.apply_history(&SessionEvent::new(
        session,
        3,
        SessionEventKind::ToolCompleted {
            tool_call_id: "historical".into(),
            output: "done".into(),
            is_error: false,
            input: None,
            input_ref: None,
            output_ref: None,
            parent_tool_call_id: None,
        },
    ));
    assert_eq!(core.order.len(), 2);
    assert!(
        matches!(&core.order[1], TranscriptEntry::Tool { execution, .. } if execution.is_terminal())
    );
    let before = serde_json::to_value(&core.order).unwrap();
    core.apply(&start);
    assert_eq!(serde_json::to_value(&core.order).unwrap(), before);
    core.apply(&message(
        session,
        11,
        Uuid::new_v4(),
        EventActor::Assistant,
        "new answer",
        MessageStatus::Complete,
    ));
    assert_eq!(core.order.len(), 3);
}

// Raw and keyed late reasoning cannot create an unfinishable spinner after a turn.
#[test]
fn closed_turn_rejects_new_reasoning_sources() {
    use borg_view::core::TranscriptCore;
    let session = Uuid::new_v4();
    let mut core = TranscriptCore::default();
    core.apply(&SessionEvent::new(
        session,
        1,
        SessionEventKind::TurnCompleted {
            message_id: Uuid::new_v4(),
            provider_session_id: None,
            final_text: String::new(),
            error: None,
        },
    ));
    core.apply(&SessionEvent::new(
        session,
        2,
        SessionEventKind::ReasoningDelta {
            text: "late".into(),
        },
    ));
    core.apply(&SessionEvent::new(
        session,
        3,
        SessionEventKind::ProviderEvent {
            provider: borg_agent_runtime::CodingProvider::Codex,
            kind: "reasoning_snapshot".into(),
            payload: serde_json::json!({"reasoning_id":"late", "text":"late", "complete":false}),
        },
    ));
    assert!(core.order.is_empty());
    assert_eq!(core.active_reasoning, None);
}

// Loading canonical archived text is not a new execution or a terminal transition.
#[test]
fn archived_reasoning_hydrates_payload_without_changing_terminal_receipt() {
    use borg_view::{core::TranscriptCore, entry::TranscriptEntry};
    let session = Uuid::new_v4();
    let mut core = TranscriptCore::default();
    let snapshot = |sequence, text, complete| {
        SessionEvent::new(
            session,
            sequence,
            SessionEventKind::ProviderEvent {
                provider: borg_agent_runtime::CodingProvider::Codex,
                kind: "reasoning_snapshot".into(),
                payload: serde_json::json!({"reasoning_id":"block", "text":text, "complete":complete}),
            },
        )
    };
    core.apply(&snapshot(1, "preview", false));
    core.apply(&SessionEvent::new(
        session,
        2,
        SessionEventKind::ReasoningCompleted,
    ));
    let identity = core.order.identities[0];
    let receipt = |entry: &TranscriptEntry| match entry {
        TranscriptEntry::Tool {
            execution,
            name,
            detail,
            time,
            outcome,
            ..
        } => (
            execution.clone(),
            name.clone(),
            detail.clone(),
            time.clone(),
            outcome.clone(),
        ),
        _ => panic!("missing reasoning"),
    };
    let sealed = receipt(&core.order[0]);
    core.apply(&snapshot(3, "canonical full reasoning text", true));
    assert_eq!(receipt(&core.order[0]), sealed);
    assert_eq!(core.order.identities[0], identity);
    assert_eq!(core.order.len(), 1);
    assert!(
        matches!(&core.order[0], TranscriptEntry::Tool { code_view: Some((_, text)), .. } if text == "canonical full reasoning text")
    );
}

// A progress frame is not authority to create a fresh execution in live input.
#[test]
fn unknown_tool_update_is_live_progress_only_but_history_can_reconstruct_it() {
    use borg_view::core::TranscriptCore;
    let event = SessionEvent::new(
        Uuid::new_v4(),
        1,
        SessionEventKind::ToolUpdated {
            tool_call_id: "unseen".into(),
            name: "exec".into(),
            input: serde_json::json!({"cmd":"true"}),
        },
    );
    let mut live = TranscriptCore::default();
    live.apply(&event);
    assert!(live.order.is_empty());
    let mut history = TranscriptCore::default();
    history.apply_history(&event);
    assert_eq!(history.order.len(), 1);
}

// Metadata and transcript receipts must admit against the same terminal execution.
#[test]
fn terminal_interaction_requests_cannot_resurrect_pending_metadata() {
    use borg_agent_runtime::ApprovalDecision;
    use borg_view::core::TranscriptCore;
    let session = Uuid::new_v4();
    let mut core = TranscriptCore::default();
    let approval = |sequence| {
        SessionEvent::new(
            session,
            sequence,
            SessionEventKind::ApprovalRequested {
                approval_id: "approval".into(),
                title: "Run".into(),
                detail: String::new(),
                command: None,
            },
        )
    };
    let interaction = |sequence| {
        SessionEvent::new(
            session,
            sequence,
            SessionEventKind::ProviderInteractionRequested {
                interaction_id: "question".into(),
                kind: "question".into(),
                title: "Choose".into(),
                detail: String::new(),
                payload: serde_json::json!({}),
            },
        )
    };
    core.apply(&approval(1));
    core.apply(&SessionEvent::new(
        session,
        2,
        SessionEventKind::ApprovalResolved {
            approval_id: "approval".into(),
            decision: ApprovalDecision::AllowOnce,
        },
    ));
    core.apply(&interaction(3));
    core.apply(&SessionEvent::new(
        session,
        4,
        SessionEventKind::ProviderInteractionResolved {
            interaction_id: "question".into(),
            response: serde_json::json!({}),
        },
    ));
    let rows = serde_json::to_value(&core.order).unwrap();
    let mut metadata = serde_json::to_value(&core.meta).unwrap();
    metadata.as_object_mut().unwrap().remove("source_sequence");
    core.apply(&approval(5));
    core.apply(&interaction(6));
    assert_eq!(serde_json::to_value(&core.order).unwrap(), rows);
    let mut after = serde_json::to_value(&core.meta).unwrap();
    after.as_object_mut().unwrap().remove("source_sequence");
    assert_eq!(
        after, metadata,
        "only the source admission cursor may advance"
    );
    assert_eq!(core.meta.source_sequence.as_deref(), Some("6"));
}

// Finishing unrelated work must not mark sealed receipts dirty or revise them on the wire.
#[test]
fn terminal_wire_receipts_do_not_churn_on_unrelated_completion_or_interrupt() {
    let session = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let mut view = ViewReducer::new(session, "epoch".into());
    view.apply(SessionEvent::new(
        session,
        1,
        SessionEventKind::ToolStarted {
            tool_call_id: "finished".into(),
            name: "exec".into(),
            input: serde_json::json!({"cmd":"true"}),
            input_ref: None,
            parent_tool_call_id: None,
        },
    ));
    view.apply(SessionEvent::new(
        session,
        2,
        SessionEventKind::ToolCompleted {
            tool_call_id: "finished".into(),
            output: "done".into(),
            is_error: false,
            input: None,
            input_ref: None,
            output_ref: None,
            parent_tool_call_id: None,
        },
    ));
    view.apply(message(
        session,
        3,
        message_id,
        EventActor::Assistant,
        "answer",
        MessageStatus::Complete,
    ));
    let sealed = view
        .snapshot()
        .rows
        .into_iter()
        .filter(|row| !row.id.starts_with("group:"))
        .map(|row| (row.id.clone(), serde_json::to_value(row).unwrap()))
        .collect::<HashMap<_, _>>();
    let events = [
        SessionEvent::new(
            session,
            4,
            SessionEventKind::TurnCompleted {
                message_id: Uuid::new_v4(),
                provider_session_id: None,
                final_text: String::new(),
                error: None,
            },
        ),
        SessionEvent::new(
            session,
            5,
            SessionEventKind::StatusChanged {
                status: borg_agent_runtime::SessionStatus::Stopped,
                lifecycle: None,
                detail: Some("interrupted".into()),
            },
        ),
    ];
    for event in events {
        view.apply(event);
        for row in view
            .snapshot()
            .rows
            .into_iter()
            .filter(|row| !row.id.starts_with("group:"))
        {
            assert_eq!(
                serde_json::to_value(&row).unwrap(),
                sealed[&row.id],
                "sealed row {} changed",
                row.id
            );
        }
    }
}

// Correlation, not arrival order, owns the reasoning execution being updated.
#[test]
fn late_reasoning_block_events_cannot_change_or_finish_another_block() {
    use borg_view::entry::TranscriptEntry;
    let session = Uuid::new_v4();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let mut view = ViewReducer::new(session, "epoch".into());
    for (sequence, kind) in [
        (
            1,
            SessionEventKind::ReasoningBlockStarted { reasoning_id: a },
        ),
        (
            2,
            SessionEventKind::ReasoningBlockDelta {
                reasoning_id: a,
                delta: "first".into(),
            },
        ),
        (
            3,
            SessionEventKind::ReasoningBlockCompleted { reasoning_id: a },
        ),
        (
            4,
            SessionEventKind::ReasoningBlockStarted { reasoning_id: b },
        ),
        (
            5,
            SessionEventKind::ReasoningBlockDelta {
                reasoning_id: b,
                delta: "second".into(),
            },
        ),
    ] {
        view.apply(SessionEvent::new(session, sequence, kind));
    }
    let sealed = serde_json::to_value(&view.core.order[0]).unwrap();
    let live = serde_json::to_value(&view.core.order[1]).unwrap();
    let texts = view
        .checkpoint_texts()
        .into_iter()
        .map(|(reference, text)| {
            (
                (reference.row_id, format!("{:?}", reference.field)),
                text.to_string(),
            )
        })
        .collect::<HashMap<_, _>>();
    let mut restored = ViewReducer::restore(&view.checkpoint().unwrap(), |reference| {
        Ok(texts[&(reference.row_id.clone(), format!("{:?}", reference.field))].clone())
    })
    .unwrap();
    let late = [
        SessionEventKind::ReasoningBlockCompleted { reasoning_id: a },
        SessionEventKind::ReasoningBlockDelta { reasoning_id: a, delta: "late".into() },
        SessionEventKind::ReasoningBlockSnapshot { reasoning_id: a, text: "late snapshot".into() },
        SessionEventKind::ReasoningBlockStarted { reasoning_id: a },
        SessionEventKind::ReasoningBlockDelta { reasoning_id: Uuid::new_v4(), delta: "unknown".into() },
        SessionEventKind::ReasoningCompleted,
        SessionEventKind::ReasoningDelta { text: "legacy late text".into() },
        SessionEventKind::ProviderEvent { provider: borg_agent_runtime::CodingProvider::Codex,
            kind: "item/completed:reasoning".into(), payload: serde_json::json!({"item":{"type":"reasoning","id":"old-provider-item"}}) },
        SessionEventKind::ProviderEvent { provider: borg_agent_runtime::CodingProvider::Codex,
            kind: "reasoning_snapshot".into(), payload: serde_json::json!({"reasoning_id":"old-provider-item","text":"legacy","complete":false}) },
    ].into_iter().enumerate().map(|(index, kind)| SessionEvent::new(session, index as u64 + 6, kind)).collect::<Vec<_>>();
    let completion = SessionEvent::new(
        session,
        15,
        SessionEventKind::ReasoningBlockCompleted { reasoning_id: b },
    );
    for projection in [&mut view, &mut restored] {
        for event in &late {
            projection.apply(event.clone());
        }
        assert_eq!(projection.core.order.len(), 2);
        assert_eq!(
            serde_json::to_value(&projection.core.order[0]).unwrap(),
            sealed
        );
        assert_eq!(
            serde_json::to_value(&projection.core.order[1]).unwrap(),
            live
        );
        assert_eq!(projection.core.active_reasoning, Some(1));
        assert_eq!(projection.core.order.identities, vec![(a, 0), (b, 0)]);
        projection.apply(completion.clone());
        assert!(
            matches!(&projection.core.order[1], TranscriptEntry::Tool { execution, .. }
            if execution.is_terminal() && execution.completed_at() == Some(completion.created_at))
        );
    }
    assert_eq!(
        serde_json::to_value(&view.core.order[1]).unwrap(),
        serde_json::to_value(&restored.core.order[1]).unwrap()
    );
}

// Recovery synthesizes admission for the same source block, not another UI execution.
#[test]
fn reasoning_recovery_preserves_block_identity_and_start_clock() {
    use borg_view::core::TranscriptCore;
    let session = Uuid::new_v4();
    let block = Uuid::new_v4();
    let mut started = SessionEvent::new(
        session,
        1,
        SessionEventKind::ReasoningBlockStarted {
            reasoning_id: block,
        },
    );
    let mut snapshot = SessionEvent::new(
        session,
        2,
        SessionEventKind::ReasoningBlockSnapshot {
            reasoning_id: block,
            text: "live text".into(),
        },
    );
    snapshot.created_at = started.created_at;
    let mut live = TranscriptCore::default();
    live.apply(&started);
    live.apply(&snapshot);
    started.id = block;
    started.sequence = 0;
    snapshot.sequence = 0;
    let mut recovered = TranscriptCore::default();
    recovered.apply(&started);
    recovered.apply(&snapshot);
    assert_eq!(recovered.order.identities, live.order.identities);
    assert_eq!(
        serde_json::to_value(&recovered.order[0]).unwrap(),
        serde_json::to_value(&live.order[0]).unwrap()
    );
    // Receiving the synthetic admission again is idempotent and cannot reset the clock.
    recovered.apply(&started);
    assert_eq!(recovered.order.len(), 1);
    assert_eq!(
        serde_json::to_value(&recovered.order[0]).unwrap(),
        serde_json::to_value(&live.order[0]).unwrap()
    );
}

// A truncated archived block may be shown as history, but must never become active.
#[test]
fn unknown_final_reasoning_archive_is_terminal_only_and_does_not_finish_live_block() {
    use borg_view::{core::TranscriptCore, entry::TranscriptEntry};
    let session = Uuid::new_v4();
    let current = Uuid::new_v4();
    let archived = Uuid::new_v4();
    let mut core = TranscriptCore::default();
    core.apply(&SessionEvent::new(
        session,
        1,
        SessionEventKind::ReasoningBlockStarted {
            reasoning_id: current,
        },
    ));
    core.apply(&SessionEvent::new(session, 2, SessionEventKind::ProviderEvent {
        provider: borg_agent_runtime::CodingProvider::Codex, kind: "reasoning_snapshot".into(),
        payload: serde_json::json!({"reasoning_block":true,"reasoning_id":archived,"text":"archived full text","complete":true}),
    }));
    assert_eq!(core.active_reasoning, Some(0));
    assert!(
        matches!(&core.order[0], TranscriptEntry::Tool { execution, .. } if !execution.is_terminal())
    );
    assert!(
        matches!(&core.order[1], TranscriptEntry::Tool { execution, .. } if execution.is_terminal())
    );
    assert_eq!(core.order.identities[1], (archived, 0));
    core.apply(&SessionEvent::new(session, 3, SessionEventKind::ProviderEvent {
        provider: borg_agent_runtime::CodingProvider::Codex, kind: "reasoning_snapshot".into(),
        payload: serde_json::json!({"reasoning_block":true,"reasoning_id":Uuid::new_v4(),"text":"unknown progress","complete":false}),
    }));
    assert_eq!(core.order.len(), 2);
    assert_eq!(core.active_reasoning, Some(0));
}

// Operation correlation must survive retries and interleaved reasoning or later compactions.
#[test]
fn compaction_operation_retries_preserve_clock_and_old_events_cannot_touch_current_work() {
    use borg_view::{core::TranscriptCore, entry::TranscriptEntry};
    let session = Uuid::new_v4();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let reasoning = Uuid::new_v4();
    let start = chrono::Utc::now();
    let event = |sequence: u64, operation, status: &str| {
        let mut event = SessionEvent::new(
            session,
            sequence,
            SessionEventKind::ProviderEvent {
                provider: borg_agent_runtime::CodingProvider::Codex,
                kind: if status == "failed" {
                    "context_compaction_failed"
                } else {
                    "context_compaction"
                }
                .into(),
                payload: serde_json::json!({"operation_id":operation,"status":status,"summary":"current progress","error":"failed"}),
            },
        );
        event.created_at = start + chrono::Duration::seconds(sequence as i64);
        event
    };
    let mut core = TranscriptCore::default();
    core.apply(&event(1, a, "started"));
    core.apply(&event(2, a, "progress"));
    core.apply(&event(3, a, "started"));
    assert_eq!(core.order.len(), 1);
    assert_eq!(
        core.order[0].execution().unwrap().started_at(),
        start + chrono::Duration::seconds(1)
    );
    core.apply(&event(4, b, "started"));
    assert!(core.order[0].execution().unwrap().is_interrupted());
    core.apply(&SessionEvent::new(
        session,
        5,
        SessionEventKind::ReasoningBlockStarted {
            reasoning_id: reasoning,
        },
    ));
    let old = serde_json::to_value(&core.order[0]).unwrap();
    let current = serde_json::to_value(&core.order[1]).unwrap();
    for (sequence, status) in [
        (6, "progress"),
        (7, "completed"),
        (8, "failed"),
        (9, "started"),
    ] {
        core.apply(&event(sequence, a, status));
    }
    core.apply(&event(10, Uuid::new_v4(), "progress"));
    assert_eq!(core.order.len(), 3);
    assert_eq!(serde_json::to_value(&core.order[0]).unwrap(), old);
    assert_eq!(serde_json::to_value(&core.order[1]).unwrap(), current);
    assert_eq!(core.active_reasoning, Some(2));
    core.apply(&SessionEvent::new(session, 11, SessionEventKind::ProviderEvent {
        provider: borg_agent_runtime::CodingProvider::Codex, kind: "context_compaction".into(),
        payload: serde_json::json!({"status":"progress", "summary":"uncorrelated late progress"}),
    }));
    assert_eq!(core.order.len(), 3);
    assert_eq!(core.active_reasoning, Some(2));
    core.apply(&event(12, b, "progress"));
    core.apply(&event(13, b, "completed"));
    assert!(
        matches!(&core.order[1], TranscriptEntry::Compaction { execution, .. } if execution.is_terminal() && execution.started_at() == start + chrono::Duration::seconds(4))
    );
    assert_eq!(core.active_reasoning, Some(2));
    core.apply(&event(14, b, "started"));
    let historical = Uuid::new_v4();
    core.apply(&event(15, historical, "completed"));
    assert_eq!(core.active_reasoning, Some(2));
    assert_eq!(core.order.identities[3], (historical, 0));
    assert!(core.order[3].execution().unwrap().is_terminal());
}

// The checkpoint is a fixed budget: a session that has run many commands must keep
// checkpointing, or its projection silently stops persisting.
#[test]
fn checkpoint_stays_within_budget_after_many_finished_processes() {
    use borg_agent_runtime::RuntimeProcessStatus;
    let session = Uuid::new_v4();
    let mut reducer = ViewReducer::new(session, "epoch".into());
    for index in 0..400_u64 {
        let process_id = Uuid::new_v4();
        reducer.apply(SessionEvent::new(
            session,
            index * 2 + 1,
            SessionEventKind::RuntimeProcessStarted {
                process_id,
                pid: 42,
                command: "x".repeat(14_000),
                cwd: ".".into(),
            },
        ));
        reducer.apply(SessionEvent::new(
            session,
            index * 2 + 2,
            SessionEventKind::RuntimeProcessCompleted {
                process_id,
                pid: 42,
                status: RuntimeProcessStatus::Exited,
                exit_code: Some(0),
                timed_out: false,
                stdout: String::new(),
                stderr: String::new(),
                stdout_omitted_bytes: 0,
                stderr_omitted_bytes: 0,
                error: None,
                changes: Vec::new(),
            },
        ));
    }
    assert!(reducer.checkpoint().unwrap().len() < SNAPSHOT_BYTES / 4);
}

// Blue means backgrounded. The text the model wrote before a tool call can be
// journaled after the process has already started; that is the same response, not
// the model moving on, so a command it is still waiting for must stay foreground.
#[test]
fn late_message_of_the_same_response_does_not_background_a_running_command() {
    use borg_view::{core::TranscriptCore, entry::TranscriptEntry};
    let session = Uuid::new_v4();
    let mut core = TranscriptCore::default();
    let tool_state = |core: &TranscriptCore| {
        (0..core.order.len())
            .find_map(|index| match &core.order[index] {
                TranscriptEntry::Tool { execution, .. } => Some(execution.clone()),
                _ => None,
            })
            .expect("tool row")
    };
    core.apply(&SessionEvent::new(
        session,
        1,
        SessionEventKind::ProviderEvent {
            provider: borg_agent_runtime::CodingProvider::Codex,
            kind: "action/preparing".into(),
            payload: serde_json::json!({"label": "command", "tool_call_id": "call-1"}),
        },
    ));
    core.apply(&SessionEvent::new(
        session,
        2,
        SessionEventKind::RuntimeProcessStarted {
            process_id: Uuid::new_v4(),
            pid: 42,
            command: "sleep 30".into(),
            cwd: ".".into(),
        },
    ));
    assert!(!tool_state(&core).is_backgrounded(), "after process start");
    core.apply(&message(
        session,
        3,
        Uuid::new_v4(),
        EventActor::Assistant,
        "Running it now.",
        MessageStatus::Complete,
    ));
    assert!(!tool_state(&core).is_backgrounded(), "after late message");
    core.apply(&SessionEvent::new(
        session,
        4,
        SessionEventKind::ToolStarted {
            tool_call_id: "call-1".into(),
            name: "exec".into(),
            input: serde_json::json!({"cmd": "sleep 30"}),
            input_ref: None,
            parent_tool_call_id: None,
        },
    ));
    let execution = tool_state(&core);
    assert!(!execution.is_backgrounded() && !execution.is_terminal());
}

// An image an agent shows needs no caption: an empty-text assistant message
// that carries attachments is content, not a dropped empty reply.
#[test]
fn captionless_assistant_image_stays_in_the_transcript() {
    let session = Uuid::new_v4();
    let mut reducer = ViewReducer::new(session, "epoch".into());
    reducer.apply(SessionEvent::new(
        session,
        1,
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::Assistant,
            text: String::new(),
            status: MessageStatus::Complete,
            attachments: vec!["/tmp/shown.png".into()],
            delivery: None,
        },
    ));
    let snapshot = reducer.snapshot();
    assert_eq!(snapshot.rows.len(), 1);
    assert!(matches!(
        &snapshot.rows[0].body,
        borg_view::wire::RowBody::Message { attachments, .. } if attachments.len() == 1
    ));
}
