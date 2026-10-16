//! Protect structural TUI/host live and replay parity after semantic extraction.
use super::*;

#[test]
fn view_authority_live_and_replay_match_terminal_semantic_rows() {
    let session = Uuid::new_v4();
    let user = Uuid::new_v4();
    let reply = Uuid::new_v4();
    let events = vec![
        SessionEventKind::Message {
            message_id: user,
            actor: EventActor::User,
            text: "inspect".into(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
        SessionEventKind::TurnStarted {
            message_id: user,
            provider: CodingProvider::Codex,
            model: Some("model".into()),
            effort: Some("high".into()),
            fast: false,
            ultrafast: false,
        },
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::System,
            text: "provider input must stay hidden".into(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
        SessionEventKind::ReasoningTextDelta {
            delta: "**Inspect the source**\n\nλ".into(),
        },
        SessionEventKind::ReasoningCompleted,
        SessionEventKind::ToolStarted {
            tool_call_id: "read".into(),
            name: "read_file".into(),
            input: serde_json::json!({"path":"source.rs"}),
            input_ref: None,
            parent_tool_call_id: None,
        },
        SessionEventKind::ToolCompleted {
            tool_call_id: "read".into(),
            output: "source content".into(),
            output_ref: None,
            is_error: false,
            input: None,
            input_ref: None,
            parent_tool_call_id: None,
        },
        SessionEventKind::AgentMessageReceived {
            message_id: Uuid::new_v4(),
            sender_id: Uuid::new_v4(),
            sender_name: "worker".into(),
            text: "verified report".into(),
            from_child: None,
        },
        SessionEventKind::Message {
            message_id: reply,
            actor: EventActor::Assistant,
            text: "Result λ".into(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        },
        SessionEventKind::MessageDelta {
            message_id: reply,
            delta: "🦀".into(),
        },
        SessionEventKind::Message {
            message_id: reply,
            actor: EventActor::Assistant,
            text: "Result λ🦀".into(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
    ];
    let mut terminal = Transcript::default();
    let mut replay = Transcript::default();
    let mut host = borg_view::ViewReducer::new(session, "epoch".into());
    for (index, kind) in events.into_iter().enumerate() {
        let event = SessionEvent::new(session, index as u64 + 1, kind);
        terminal.apply(&event);
        replay.apply_history(&event);
        host.apply(event);
        assert_eq!(
            serde_json::to_value(&terminal.core.order.entries).unwrap(),
            serde_json::to_value(&host.core.order.entries).unwrap()
        );
        assert_eq!(
            serde_json::to_value(&terminal.core.order.entries).unwrap(),
            serde_json::to_value(&replay.core.order.entries).unwrap()
        );
    }
    let snapshot = host.snapshot();
    assert!(
        snapshot
            .rows
            .iter()
            .any(|row| row.kind == borg_view::wire::RowKind::TeamReport)
    );
    assert!(
        !serde_json::to_string(&snapshot)
            .unwrap()
            .contains("provider input must stay hidden")
    );
}

#[test]
fn child_metadata_refs_lift_pending_approval_in_terminal_and_host_without_nested_events() {
    let parent = Uuid::new_v4();
    let child = Uuid::new_v4();
    let now = Utc::now();
    let snapshot = SubagentSnapshot {
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
        final_text: None,
        usage: borg_remote::SubagentUsage::default(),
        interrupted_by: None,
    };
    let event = SessionEvent::new(
        parent,
        1,
        SessionEventKind::SubagentActivity {
            activity: SubagentActivityKind::Started,
            agent: snapshot,
            event: None,
        },
    );
    let mut terminal = Transcript::default();
    let mut host = borg_view::ViewReducer::new(parent, "root".into());
    terminal.apply(&event);
    host.apply(event);
    let mut child_view = borg_view::ViewReducer::new(child, "child".into());
    child_view.apply(SessionEvent::new(
        child,
        1,
        SessionEventKind::ApprovalRequested {
            approval_id: "approve".into(),
            title: "Review".into(),
            detail: "safe input".into(),
            command: None,
        },
    ));
    let child_snapshot = child_view.snapshot();
    terminal
        .apply_child_meta(child, child_snapshot.cursor.clone(), &child_snapshot.meta)
        .unwrap();
    host.apply_child_meta(child, child_snapshot.cursor.clone(), &child_snapshot.meta)
        .unwrap();
    assert_eq!(
        terminal.meta.approvals[0].source_session_id,
        child.to_string()
    );
    assert_eq!(
        serde_json::to_value(&terminal.core.order.entries).unwrap(),
        serde_json::to_value(&host.core.order.entries).unwrap()
    );
    let cursor = host.cursor().clone();
    assert!(
        host.apply_child_meta(child, child_snapshot.cursor, &child_snapshot.meta)
            .unwrap()
            .is_empty()
    );
    assert_eq!(host.cursor(), &cursor);
}
