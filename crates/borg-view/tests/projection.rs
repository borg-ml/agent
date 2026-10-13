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
