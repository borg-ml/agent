use std::collections::HashMap;
use std::sync::Arc;

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

#[derive(Clone, Default)]
pub struct TimelineProjector {
    entries: Vec<Arc<TimelineEntry>>,
    messages: HashMap<Uuid, usize>,
    tools: HashMap<String, usize>,
    reasoning: Option<usize>,
}

impl TimelineProjector {
    pub fn from_events(events: &[SessionEvent]) -> Self {
        let mut projector = Self::default();
        projector.extend(events);
        projector
    }

    pub fn extend(&mut self, events: &[SessionEvent]) {
        for event in events {
            self.push(event);
        }
    }

    pub fn into_entries(self) -> Vec<TimelineEntry> {
        self.entries.into_iter().map(Arc::unwrap_or_clone).collect()
    }

    pub fn into_shared_entries(self) -> Vec<Arc<TimelineEntry>> {
        self.entries
    }

    pub fn push(&mut self, event: &SessionEvent) {
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
                if let Some(index) = self.messages.get(message_id).copied() {
                    self.entries[index] = Arc::new(entry);
                } else {
                    self.messages.insert(*message_id, self.entries.len());
                    self.entries.push(Arc::new(entry));
                }
            }
            SessionEventKind::ReasoningDelta { text } => {
                if let Some(index) = self.reasoning {
                    if !text.is_empty() {
                        let body = &mut Arc::make_mut(&mut self.entries[index]).body;
                        if text.starts_with(body.as_str()) {
                            body.clone_from(text);
                        } else if !body.starts_with(text) {
                            body.push_str(text);
                        }
                    }
                } else {
                    self.reasoning = Some(self.entries.len());
                    self.entries.push(Arc::new(TimelineEntry {
                        id: format!("reasoning:{}", event.sequence),
                        created_at: event.created_at,
                        kind: TimelineKind::Reasoning,
                        title: "Thinking".into(),
                        detail: None,
                        body: text.clone(),
                        running: true,
                        failed: false,
                    }));
                }
            }
            SessionEventKind::ReasoningCompleted => {
                if let Some(index) = self.reasoning.take() {
                    Arc::make_mut(&mut self.entries[index]).running = false;
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
                self.reasoning = None;
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
                if let Some(index) = self.tools.get(tool_call_id).copied() {
                    self.entries[index] = Arc::new(entry);
                } else {
                    self.tools.insert(tool_call_id.clone(), self.entries.len());
                    self.entries.push(Arc::new(entry));
                }
            }
            SessionEventKind::ToolCompleted {
                tool_call_id,
                output,
                is_error,
                input: _,
                ..
            } => {
                let index = self.tools.get(tool_call_id).copied();
                if let Some(index) = index {
                    let entry = Arc::make_mut(&mut self.entries[index]);
                    entry.running = false;
                    entry.failed = *is_error;
                    entry.body = output.clone();
                } else {
                    self.entries.push(Arc::new(TimelineEntry {
                        id: format!("tool:{tool_call_id}"),
                        created_at: event.created_at,
                        kind: TimelineKind::Tool,
                        title: "Completed tool".into(),
                        detail: None,
                        body: output.clone(),
                        running: false,
                        failed: *is_error,
                    }));
                }
            }
            SessionEventKind::ApprovalRequested { title, detail, .. } => {
                self.entries.push(Arc::new(simple_entry(
                    event,
                    TimelineKind::Approval,
                    title,
                    detail,
                    true,
                    false,
                )));
            }
            SessionEventKind::SubagentActivity { agent, .. } => {
                self.entries.push(Arc::new(simple_entry(
                    event,
                    TimelineKind::Subagent,
                    &agent.task_name,
                    agent.detail.as_deref().unwrap_or_default(),
                    agent.final_text.is_none(),
                    false,
                )));
            }
            SessionEventKind::Error { message } => self.entries.push(Arc::new(simple_entry(
                event,
                TimelineKind::Error,
                "Error",
                message,
                false,
                true,
            ))),
            _ => {}
        }
    }
}

pub fn project_timeline(events: &[SessionEvent]) -> Vec<TimelineEntry> {
    TimelineProjector::from_events(events).into_entries()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cumulative_live_reasoning_replaces_instead_of_duplicating() {
        let session_id = Uuid::new_v4();
        let mut projector = TimelineProjector::default();
        projector.push(&SessionEvent::new(
            session_id,
            1,
            SessionEventKind::ReasoningDelta {
                text: "checking".into(),
            },
        ));
        projector.push(&SessionEvent::new(
            session_id,
            0,
            SessionEventKind::ReasoningDelta {
                text: "checking the workspace".into(),
            },
        ));

        let entries = projector.into_entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].body, "checking the workspace");
    }
}
