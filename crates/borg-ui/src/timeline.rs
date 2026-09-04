use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use borg_remote::{
    EventActor, MessageStatus, SessionEvent, SessionEventKind, edit_is_awaiting_diff,
    tool_call_summary,
};
use chrono::{DateTime, Utc};
use uuid::Uuid;

pub fn tool_lifecycle_label(name: &str, complete: bool) -> Cow<'_, str> {
    if complete && (name == "Generate" || name.starts_with("Generate ")) {
        let label = name.strip_prefix("Generate ").unwrap_or("command");
        return Cow::Owned(format!("Stopped generating {label}"));
    }
    if name == "Git add" {
        return Cow::Borrowed(if complete {
            "Updated Git index"
        } else {
            "Updating Git index…"
        });
    }
    let (verb, rest) = name.split_once(' ').unwrap_or((name, ""));
    let forms = match verb {
        "Run" => Some(("Running", "Ran")),
        "Prepare" => Some(("Preparing", "Prepared")),
        "Consult" => Some(("Consulting", "Consulted")),
        "Inspect" => Some(("Inspecting", "Inspected")),
        "Read" => Some(("Reading", "Read")),
        "Edit" => Some(("Editing", "Edited")),
        "Update" => Some(("Updating", "Updated")),
        "Search" => Some(("Searching", "Searched")),
        "List" => Some(("Listing", "Listed")),
        "Check" => Some(("Checking", "Checked")),
        "Find" => Some(("Finding", "Found")),
        "Go" => Some(("Going", "Went")),
        "Generate" => Some(("Generating", "Stopped generating")),
        "View" => Some(("Viewing", "Viewed")),
        "Create" => Some(("Creating", "Created")),
        "Delete" => Some(("Deleting", "Deleted")),
        "Wait" => Some(("Waiting", "Finished waiting")),
        "Send" => Some(("Sending", "Sent")),
        "Follow" => Some(("Following", "Followed")),
        "Message" => Some(("Sending message to", "Sent message to")),
        "Use" => Some(("Using", "Used")),
        "Spawn" => Some(("Spawning", "Spawned")),
        "Interrupt" => Some(("Interrupting", "Interrupted")),
        "Stop" => Some(("Stopping", "Stopped")),
        "Compare" => Some(("Comparing", "Compared")),
        "Review" => Some(("Reviewing", "Reviewed")),
        "Show" => Some(("Showing", "Shown")),
        "Add" => Some(("Adding", "Added")),
        "Remove" => Some(("Removing", "Removed")),
        "Prune" => Some(("Pruning", "Pruned")),
        "Lock" => Some(("Locking", "Locked")),
        "Unlock" => Some(("Unlocking", "Unlocked")),
        "Switch" => Some(("Switching", "Switched")),
        "Commit" => Some(("Committing", "Committed")),
        "Fetch" => Some(("Fetching", "Fetched")),
        "Pull" => Some(("Pulling", "Pulled")),
        "Push" => Some(("Pushing", "Pushed")),
        "Merge" => Some(("Merging", "Merged")),
        "Rebase" => Some(("Rebasing", "Rebased")),
        _ => None,
    };
    if let Some((running, completed)) = forms {
        let form = if complete { completed } else { running };
        return Cow::Owned(format!(
            "{form}{}{}{}",
            if rest.is_empty() { "" } else { " " },
            rest,
            if complete { "" } else { "…" }
        ));
    }
    let phrase = match name {
        "Git status" | "Git diff" | "Git log" | "Git branch" | "Git tags" | "Git remotes" => {
            Some(if complete { "Inspected" } else { "Inspecting" })
        }
        "Repository info" => Some(if complete { "Inspected" } else { "Inspecting" }),
        "Language servers" | "Workspace diagnostics" => {
            Some(if complete { "Checked" } else { "Checking" })
        }
        _ => None,
    };
    phrase.map_or_else(
        || {
            if complete {
                Cow::Borrowed(name)
            } else {
                Cow::Owned(format!("{name}…"))
            }
        },
        |form| Cow::Owned(format!("{form} {name}{}", if complete { "" } else { "…" })),
    )
}

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
    pub rich_body: Option<Arc<crate::markdown::RichText>>,
    pub running: bool,
    pub failed: bool,
}

#[derive(Clone, Default)]
pub struct TimelineProjector {
    entries: Vec<Arc<TimelineEntry>>,
    messages: HashMap<Uuid, usize>,
    tools: HashMap<String, usize>,
    preparing_tools: HashMap<String, usize>,
    unkeyed_preparing_tools: Vec<usize>,
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

    fn has_preparing_tool(&self, tool_call_id: &str) -> bool {
        self.preparing_tools.contains_key(tool_call_id) || !self.unkeyed_preparing_tools.is_empty()
    }

    fn take_preparing_tool(&mut self, tool_call_id: &str) -> Option<usize> {
        self.preparing_tools.remove(tool_call_id).or_else(|| {
            (!self.unkeyed_preparing_tools.is_empty())
                .then(|| self.unkeyed_preparing_tools.remove(0))
        })
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
                    rich_body: matches!(actor, EventActor::Assistant)
                        .then(|| Arc::new(crate::markdown::project_markdown(text))),
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
                        id: format!("reasoning:{}", event.id),
                        created_at: event.created_at,
                        kind: TimelineKind::Reasoning,
                        title: "Thinking".into(),
                        detail: None,
                        body: text.clone(),
                        rich_body: None,
                        running: true,
                        failed: false,
                    }));
                }
            }
            SessionEventKind::TurnCompleted { .. } => {
                for index in self
                    .preparing_tools
                    .drain()
                    .map(|(_, index)| index)
                    .chain(self.unkeyed_preparing_tools.drain(..))
                {
                    Arc::make_mut(&mut self.entries[index]).running = false;
                }
            }
            SessionEventKind::ReasoningCompleted => {
                if let Some(index) = self.reasoning.take() {
                    Arc::make_mut(&mut self.entries[index]).running = false;
                }
            }
            SessionEventKind::ProviderEvent { kind, .. }
                if kind == "action/preparing_cancelled" =>
            {
                if let Some(index) = self.unkeyed_preparing_tools.pop() {
                    if index + 1 == self.entries.len() {
                        self.entries.pop();
                    } else if let Some(entry) = self.entries.get_mut(index) {
                        Arc::make_mut(entry).running = false;
                    }
                }
            }
            SessionEventKind::ProviderEvent { kind, payload, .. } if kind == "action/preparing" => {
                self.reasoning = None;
                let provider_tool_id = payload
                    .get("tool_call_id")
                    .and_then(serde_json::Value::as_str);
                let label = payload
                    .get("label")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("action");
                let existing = provider_tool_id
                    .and_then(|provider_tool_id| {
                        self.preparing_tools
                            .get(provider_tool_id)
                            .copied()
                            .or_else(|| {
                                if self.unkeyed_preparing_tools.is_empty() {
                                    return None;
                                }
                                let index = self.unkeyed_preparing_tools.remove(0);
                                self.preparing_tools
                                    .insert(provider_tool_id.to_string(), index);
                                Some(index)
                            })
                    })
                    .or_else(|| {
                        provider_tool_id
                            .is_none()
                            .then(|| {
                                self.unkeyed_preparing_tools
                                    .last()
                                    .copied()
                                    .filter(|index| {
                                        self.entries.get(*index).is_some_and(|entry| {
                                            entry.running && entry.title == "Generate"
                                        })
                                    })
                            })
                            .flatten()
                    });
                if provider_tool_id.is_none()
                    && !label.is_empty()
                    && existing.is_none()
                    && let Some(index) = self.unkeyed_preparing_tools.pop()
                {
                    Arc::make_mut(&mut self.entries[index]).running = false;
                }
                let input = serde_json::json!({"label": label});
                let (title, detail) = tool_call_summary("action_preparing", &input);
                if let Some(index) = existing {
                    let entry = Arc::make_mut(&mut self.entries[index]);
                    entry.title = title;
                    entry.detail = (!detail.is_empty()).then_some(detail);
                    entry.body.clear();
                    entry.rich_body = None;
                    entry.running = true;
                    entry.failed = false;
                    return;
                }
                let index = self.entries.len();
                self.entries.push(Arc::new(TimelineEntry {
                    id: format!("action:{}", event.id),
                    created_at: event.created_at,
                    kind: TimelineKind::Tool,
                    title,
                    detail: (!detail.is_empty()).then_some(detail),
                    body: String::new(),
                    rich_body: None,
                    running: true,
                    failed: false,
                }));
                if let Some(provider_tool_id) = provider_tool_id {
                    self.preparing_tools
                        .insert(provider_tool_id.to_string(), index);
                } else {
                    self.unkeyed_preparing_tools.push(index);
                }
            }
            SessionEventKind::ToolStarted {
                tool_call_id,
                name,
                input,
                ..
            } if !self.tools.contains_key(tool_call_id)
                && self.has_preparing_tool(tool_call_id)
                && edit_is_awaiting_diff(name, input) =>
            {
                self.reasoning = None;
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
                let known_tool = self.tools.get(tool_call_id).copied();
                let running_tool = known_tool.filter(|index| self.entries[*index].running);
                let matching_preparation = known_tool
                    .is_none()
                    .then(|| self.take_preparing_tool(tool_call_id))
                    .flatten();
                let index = if matches!(&event.kind, SessionEventKind::ToolStarted { .. }) {
                    matching_preparation.or(running_tool)
                } else {
                    running_tool.or_else(|| {
                        if known_tool.is_none() {
                            matching_preparation
                        } else {
                            None
                        }
                    })
                };
                let index = if let Some(index) = index {
                    let entry = Arc::make_mut(&mut self.entries[index]);
                    entry.title = title;
                    entry.detail = (!detail.is_empty()).then_some(detail);
                    entry.body.clear();
                    entry.rich_body = None;
                    entry.running = true;
                    entry.failed = false;
                    index
                } else {
                    let index = self.entries.len();
                    self.entries.push(Arc::new(TimelineEntry {
                        id: format!("tool:{tool_call_id}:{}", event.id),
                        created_at: event.created_at,
                        kind: TimelineKind::Tool,
                        title,
                        detail: (!detail.is_empty()).then_some(detail),
                        body: String::new(),
                        rich_body: None,
                        running: true,
                        failed: false,
                    }));
                    index
                };
                self.tools.insert(tool_call_id.clone(), index);
            }
            SessionEventKind::ToolCompleted {
                tool_call_id,
                output,
                is_error,
                ..
            } => {
                let known_index = self.tools.get(tool_call_id).copied();
                let index = known_index.filter(|index| self.entries[*index].running);
                if let Some(index) = index {
                    let entry = Arc::make_mut(&mut self.entries[index]);
                    entry.running = false;
                    entry.failed = *is_error;
                    entry.body = output.clone();
                    entry.rich_body = None;
                } else if known_index.is_none()
                    && let Some(index) = self.take_preparing_tool(tool_call_id)
                {
                    let entry = Arc::make_mut(&mut self.entries[index]);
                    let label = entry.detail.take().unwrap_or_else(|| {
                        entry
                            .title
                            .strip_prefix("Generate ")
                            .unwrap_or("command")
                            .to_string()
                    });
                    entry.title = format!("Run {label}");
                    entry.running = false;
                    entry.failed = *is_error;
                    entry.body = output.clone();
                    entry.rich_body = None;
                    self.tools.insert(tool_call_id.clone(), index);
                } else {
                    let index = self.entries.len();
                    self.entries.push(Arc::new(TimelineEntry {
                        id: format!("tool:{tool_call_id}:{}", event.id),
                        created_at: event.created_at,
                        kind: TimelineKind::Tool,
                        title: "Completed tool".into(),
                        detail: None,
                        body: output.clone(),
                        rich_body: None,
                        running: false,
                        failed: *is_error,
                    }));
                    self.tools.insert(tool_call_id.clone(), index);
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
        rich_body: None,
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
    use borg_remote::CodingProvider;

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

    #[test]
    fn reused_tool_call_id_starts_a_new_audit_entry() {
        let session_id = Uuid::new_v4();
        let mut projector = TimelineProjector::default();
        projector.push(&SessionEvent::new(
            session_id,
            1,
            SessionEventKind::ToolStarted {
                tool_call_id: "command-1".into(),
                name: "exec_command".into(),
                input: serde_json::json!({"cmd": "first"}),
                input_ref: None,
            },
        ));
        projector.push(&SessionEvent::new(
            session_id,
            2,
            SessionEventKind::ToolCompleted {
                tool_call_id: "command-1".into(),
                output: "first output".into(),
                output_ref: None,
                is_error: false,
                input: None,
                input_ref: None,
            },
        ));
        projector.push(&SessionEvent::new(
            session_id,
            3,
            SessionEventKind::ToolStarted {
                tool_call_id: "command-1".into(),
                name: "exec_command".into(),
                input: serde_json::json!({"cmd": "second"}),
                input_ref: None,
            },
        ));

        let entries = projector.into_entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].body, "first output");
        assert!(!entries[0].running);
        assert!(entries[1].running);
        assert_ne!(entries[0].id, entries[1].id);
    }

    #[test]
    fn late_tool_completion_does_not_claim_the_next_action() {
        let session_id = Uuid::new_v4();
        let mut projector = TimelineProjector::default();
        projector.push(&SessionEvent::new(
            session_id,
            1,
            SessionEventKind::ToolStarted {
                tool_call_id: "command-1".into(),
                name: "exec_command".into(),
                input: serde_json::json!({"cmd": "server"}),
                input_ref: None,
            },
        ));
        projector.push(&SessionEvent::new(
            session_id,
            2,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "action/preparing".into(),
                payload: serde_json::json!({"label": "inspect next target"}),
            },
        ));
        projector.push(&SessionEvent::new(
            session_id,
            3,
            SessionEventKind::ToolCompleted {
                tool_call_id: "command-1".into(),
                output: "server stopped".into(),
                output_ref: None,
                is_error: false,
                input: None,
                input_ref: None,
            },
        ));

        let entries = projector.into_entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].body, "server stopped");
        assert!(!entries[0].running);
        assert_eq!(entries[1].title, "Generate inspect next target");
        assert!(entries[1].running);
    }

    #[test]
    fn consecutive_unmatched_action_preparations_preserve_audit_entries() {
        let session_id = Uuid::new_v4();
        let mut projector = TimelineProjector::default();
        for (sequence, label) in [(1, "inspect first"), (2, "inspect second")] {
            projector.push(&SessionEvent::new(
                session_id,
                sequence,
                SessionEventKind::ProviderEvent {
                    provider: CodingProvider::Codex,
                    kind: "action/preparing".into(),
                    payload: serde_json::json!({"label": label}),
                },
            ));
        }

        let entries = projector.into_entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].title, "Generate inspect first");
        assert!(!entries[0].running);
        assert_eq!(
            tool_lifecycle_label(&entries[0].title, true),
            "Stopped generating inspect first"
        );
        assert_eq!(entries[1].title, "Generate inspect second");
        assert!(entries[1].running);
    }

    #[test]
    fn unkeyed_action_refinement_preserves_one_identity_and_start_time() {
        let session_id = Uuid::new_v4();
        let started_at = Utc::now();
        let mut projector = TimelineProjector::default();
        for (sequence, label) in [(1, ""), (2, "edit session retry policy")] {
            let mut event = SessionEvent::new(
                session_id,
                sequence,
                SessionEventKind::ProviderEvent {
                    provider: CodingProvider::Codex,
                    kind: "action/preparing".into(),
                    payload: serde_json::json!({"label": label}),
                },
            );
            event.created_at =
                started_at + chrono::Duration::seconds(sequence.saturating_sub(1) as i64);
            projector.push(&event);
        }

        let entries = projector.into_entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].title, "Generate edit session retry policy");
        assert_eq!(entries[0].created_at, started_at);
        assert!(entries[0].running);
    }

    #[test]
    fn parallel_action_preparations_promote_without_orphan_entries() {
        let session_id = Uuid::new_v4();
        let mut projector = TimelineProjector::default();
        for (sequence, tool_call_id, label) in [
            (1, "tool-a", ""),
            (2, "tool-b", ""),
            (3, "tool-a", "read"),
            (4, "tool-b", "run tests"),
        ] {
            projector.push(&SessionEvent::new(
                session_id,
                sequence,
                SessionEventKind::ProviderEvent {
                    provider: CodingProvider::Codex,
                    kind: "action/preparing".into(),
                    payload: serde_json::json!({
                        "label": label,
                        "tool_call_id": tool_call_id,
                    }),
                },
            ));
        }
        assert_eq!(projector.entries.len(), 2);
        assert!(projector.entries.iter().all(|entry| entry.running));

        for (sequence, tool_call_id, name, input) in [
            (
                5,
                "tool-a",
                "read_file",
                serde_json::json!({"path": "src/lib.rs"}),
            ),
            (
                6,
                "tool-b",
                "exec_command",
                serde_json::json!({"cmd": "cargo test"}),
            ),
        ] {
            projector.push(&SessionEvent::new(
                session_id,
                sequence,
                SessionEventKind::ToolStarted {
                    tool_call_id: tool_call_id.into(),
                    name: name.into(),
                    input,
                    input_ref: None,
                },
            ));
        }

        assert_eq!(projector.entries.len(), 2);
        assert_ne!(projector.tools["tool-a"], projector.tools["tool-b"]);
        assert!(projector.entries.iter().all(|entry| entry.running));
        assert!(
            projector
                .entries
                .iter()
                .all(|entry| !entry.title.starts_with("Generate"))
        );
    }
}
