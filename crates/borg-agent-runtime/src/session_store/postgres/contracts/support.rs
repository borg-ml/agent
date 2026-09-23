//! Fixtures shared by the trait-contract suites.
//!
//! These contracts were written against the journal's behaviour, not against
//! one engine's internals, so they are kept as their own modules rather than
//! folded into the query-level Postgres tests. Each one opens its own scratch
//! database through [`testing::session_store`], which requires a server and
//! fails with instructions when there is none.

use std::path::Path;

use chrono::Utc;
use uuid::Uuid;

use crate::session_store::postgres::PostgresSessionStore;
use crate::session_store::postgres::testing::{self, ScratchDatabase};
use crate::{
    CodingProvider, EventActor, MessageStatus, PermissionMode, PromptDelivery, ResponseLanguage,
    SessionEvent, SessionEventKind, SessionStore,
};

/// A scratch database and a store opened on it.
///
/// Named `store` so the ported contracts read as they were written; the tuple
/// shape differs from the original because a scratch database has to be dropped
/// explicitly where a temp directory did not.
pub(super) async fn store() -> (ScratchDatabase, PostgresSessionStore) {
    testing::session_store().await
}

pub(super) fn configured(directory: &Path) -> SessionEventKind {
    SessionEventKind::SessionConfigured {
        cwd: directory.to_path_buf(),
        provider: CodingProvider::Codex,
        model: Some("gpt-test".to_string()),
        effort: Some("high".to_string()),
        fast: false,
        response_language: ResponseLanguage::Auto,
        permission_mode: PermissionMode::FullAccess,
    }
}

pub(super) fn opencode_configured(model: &str) -> SessionEventKind {
    SessionEventKind::SessionConfigured {
        cwd: std::path::PathBuf::from("/tmp"),
        provider: CodingProvider::OpenCode,
        model: Some(model.to_string()),
        effort: None,
        fast: false,
        response_language: ResponseLanguage::default(),
        permission_mode: PermissionMode::Auto,
    }
}

pub(super) fn message(message_id: Uuid, text: &str) -> SessionEventKind {
    SessionEventKind::Message {
        message_id,
        actor: EventActor::User,
        text: text.to_string(),
        attachments: Vec::new(),
        status: MessageStatus::Complete,
        delivery: Some(PromptDelivery::Steer),
    }
}

pub(super) fn subagent_activity(
    child_id: Uuid,
    parent_id: Uuid,
    task: &str,
    status: crate::SubagentStatus,
) -> SessionEventKind {
    SessionEventKind::SubagentActivity {
        activity: crate::SubagentActivityKind::Updated,
        agent: crate::SubagentSnapshot {
            session_id: child_id,
            parent_session_id: parent_id,
            task_name: task.to_string(),
            status,
            provider: CodingProvider::Claude,
            model: None,
            effort: None,
            cwd: std::path::PathBuf::from("/tmp"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            detail: None,
            final_text: None,
            usage: crate::SubagentUsage::default(),
            interrupted_by: None,
        },
        event: None,
    }
}

pub(super) fn event_ids(events: &[SessionEvent]) -> Vec<Uuid> {
    events.iter().map(|event| event.id).collect()
}

pub(super) async fn seed_recovery_fixture(
    store: &PostgresSessionStore,
    session_id: Uuid,
    children: &[Uuid],
) {
    for (index, child) in children.iter().enumerate() {
        for status in [
            crate::SubagentStatus::Starting,
            crate::SubagentStatus::Running,
        ] {
            store
                .append(SessionEvent::new(
                    session_id,
                    0,
                    subagent_activity(*child, session_id, &format!("task_{index}"), status),
                ))
                .await
                .unwrap();
        }
    }
    store
        .append(SessionEvent::new(
            session_id,
            0,
            message(Uuid::new_v4(), "queued prompt"),
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::PromptRecalled {
                message_id: Uuid::new_v4(),
                text: "recalled prompt".into(),
                attachments: Vec::new(),
            },
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ToolStarted {
                tool_call_id: "tool-1".into(),
                name: "read_file".into(),
                input: serde_json::json!({"path": "/tmp/a"}),
                input_ref: None,
            },
        ))
        .await
        .unwrap();
    store
        .append(SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ToolCompleted {
                tool_call_id: "tool-1".into(),
                output: "ok".into(),
                output_ref: None,
                is_error: false,
                input: None,
                input_ref: None,
            },
        ))
        .await
        .unwrap();
}
