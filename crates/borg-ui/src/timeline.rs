use std::collections::HashMap;

use borg_remote::{EventActor, MessageStatus, SessionEvent, SessionEventKind, tool_call_summary};
use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimelineKind {
    User,
    Assistant,
    Reasoning,
    Tool,
    Subagent,
    Approval,
    Status,
    Error,
}

#[derive(Clone, Debug)]
pub struct TimelineEntry {
    pub id: String,
    pub created_at: DateTime<Utc>,
    pub kind: TimelineKind,
    pub title: String,
    pub detail: Option<String>,
    pub body: String,
    pub running: bool,
    pub failed: bool,
}

pub fn project_timeline(events: &[SessionEvent]) -> Vec<TimelineEntry> {
    let mut entries: Vec<TimelineEntry> = Vec::new();
    let mut messages: HashMap<Uuid, usize> = HashMap::new();
    let mut tools: HashMap<String, usize> = HashMap::new();
    let mut reasoning: Option<usize> = None;

    for event in events {
        match &event.kind {
            SessionEventKind::Message {
                message_id,
                actor,
                text,
                status,
                ..
            } => {
                let kind = match actor {
                    EventActor::User => TimelineKind::User,
                    EventActor::Assistant => TimelineKind::Assistant,
                    EventActor::Tool | EventActor::System => TimelineKind::Status,
                };
                let title = match actor {
                    EventActor::User => "you",
                    EventActor::Assistant => "borg",
                    EventActor::Tool => "tool",
                    EventActor::System => "system",
                };
                let entry = TimelineEntry {
                    id: format!("message:{message_id}"),
                    created_at: event.created_at,
                    kind,
                    title: title.into(),
                    detail: message_status_label(*status).map(str::to_string),
                    body: text.clone(),
                    running: matches!(status, MessageStatus::Queued | MessageStatus::InProgress),
                    failed: *status == MessageStatus::Failed,
                };
                if let Some(index) = messages.get(message_id).copied() {
                    entries[index] = entry;
                } else {
                    messages.insert(*message_id, entries.len());
                    entries.push(entry);
                }
            }
            SessionEventKind::ReasoningDelta { text } => {
                if let Some(index) = reasoning {
                    if !text.is_empty() {
                        entries[index].body.push_str(text);
                    }
                } else {
                    reasoning = Some(entries.len());
                    entries.push(TimelineEntry {
                        id: format!("reasoning:{}", event.sequence),
                        created_at: event.created_at,
                        kind: TimelineKind::Reasoning,
                        title: "Thinking".into(),
                        detail: None,
                        body: text.clone(),
                        running: true,
                        failed: false,
                    });
                }
            }
            SessionEventKind::ReasoningCompleted => {
                if let Some(index) = reasoning.take() {
                    entries[index].running = false;
                }
            }
            SessionEventKind::ToolStarted {
                tool_call_id,
                name,
                input,
                ..
            }
            | SessionEventKind::ToolUpdated {
                tool_call_id,
                name,
                input,
            } => {
                reasoning = None;
                let (title, detail) = tool_call_summary(name, input);
                let entry = TimelineEntry {
                    id: format!("tool:{tool_call_id}"),
                    created_at: event.created_at,
                    kind: TimelineKind::Tool,
                    title,
                    detail: (!detail.is_empty()).then_some(detail),
                    body: String::new(),
                    running: true,
                    failed: false,
                };
                if let Some(index) = tools.get(tool_call_id).copied() {
                    entries[index] = entry;
                } else {
                    tools.insert(tool_call_id.clone(), entries.len());
                    entries.push(entry);
                }
            }
            SessionEventKind::ToolCompleted {
                tool_call_id,
                output,
                is_error,
                input: _,
                ..
            } => {
                let index = tools.get(tool_call_id).copied();
                if let Some(index) = index {
                    let entry = &mut entries[index];
                    entry.running = false;
                    entry.failed = *is_error;
                    entry.body = output.clone();
                } else {
                    entries.push(TimelineEntry {
                        id: format!("tool:{tool_call_id}"),
                        created_at: event.created_at,
                        kind: TimelineKind::Tool,
                        title: "Completed tool".into(),
                        detail: None,
                        body: output.clone(),
                        running: false,
                        failed: *is_error,
                    });
                }
            }
            SessionEventKind::ApprovalRequested { title, detail, .. } => {
                entries.push(simple_entry(
                    event,
                    TimelineKind::Approval,
                    title,
                    detail,
                    true,
                    false,
                ));
            }
            SessionEventKind::SubagentActivity { agent, .. } => {
                entries.push(simple_entry(
                    event,
                    TimelineKind::Subagent,
                    &agent.task_name,
                    agent.detail.as_deref().unwrap_or_default(),
                    agent.final_text.is_none(),
                    false,
                ));
            }
            SessionEventKind::Error { message } => entries.push(simple_entry(
                event,
                TimelineKind::Error,
                "Error",
                message,
                false,
                true,
            )),
            _ => {}
        }
    }
    entries
}

fn simple_entry(
    event: &SessionEvent,
    kind: TimelineKind,
    title: &str,
    body: &str,
    running: bool,
    failed: bool,
) -> TimelineEntry {
    TimelineEntry {
        id: format!("event:{}", event.id),
        created_at: event.created_at,
        kind,
        title: title.into(),
        detail: None,
        body: body.into(),
        running,
        failed,
    }
}

fn message_status_label(status: MessageStatus) -> Option<&'static str> {
    match status {
        MessageStatus::Queued => Some("queued"),
        MessageStatus::InProgress => Some("sending"),
        MessageStatus::Complete => None,
        MessageStatus::Failed => Some("failed"),
    }
}
