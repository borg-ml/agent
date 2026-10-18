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
            .filter(|row| row.kind == borg_view::wire::RowKind::ChildSummary)
            .count(),
        1
    );
    assert_eq!(snapshot.rows[0].id, format!("child:{child}"));
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
        live.core.order[0],
        borg_view::entry::TranscriptEntry::Action {
            state: borg_view::entry::TranscriptActionState::Running,
            ..
        }
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
        live.core.order[0],
        borg_view::entry::TranscriptEntry::Action {
            state: borg_view::entry::TranscriptActionState::Running,
            ..
        }
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
    history.push(event(4, &agent, None));
    live.apply(history[3].clone());
    assert_eq!(live.core.waiting_subagent_count(), 1);
    assert!(matches!(
        live.snapshot().meta.children[0].status,
        borg_view::wire::Lifecycle::Waiting
    ));
    assert!(matches!(
        live.core.order[0],
        borg_view::entry::TranscriptEntry::Action {
            state: borg_view::entry::TranscriptActionState::Waiting,
            ..
        }
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
        TranscriptEntry::Tool {
            complete: false,
            ..
        }
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
        TranscriptEntry::Tool {
            complete: true,
            backgrounded: false,
            error: false,
            ..
        }
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
            TranscriptEntry::Tool {
                complete: true,
                backgrounded: false,
                error: true,
                ..
            }
        )));
    }
}
