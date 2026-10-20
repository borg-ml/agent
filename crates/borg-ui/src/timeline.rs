use std::collections::HashMap;
use std::sync::Arc;

use borg_remote::{EventActor, MessageStatus, SessionEvent, SubagentLifecycle, SubagentSnapshot};
use borg_view::core::{RenderInvalidation, TranscriptCore};
use borg_view::entry::{TranscriptActionKind, TranscriptEntry};
use borg_view::wire::Lifecycle;
use chrono::{DateTime, Utc};
use uuid::Uuid;

pub use borg_view::grouping::tool_lifecycle_label;
pub use borg_view::projection_helpers::subagent_lifecycle_detail;

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

#[derive(Clone, Debug, PartialEq, Eq)]
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
    /// Only current-state worker summaries navigate. Tool receipts still inspect output.
    pub target_session_id: Option<Uuid>,
}

#[derive(Clone)]
pub struct TimelineProjector {
    session_id: Option<Uuid>,
    core: TranscriptCore,
    entries: Vec<Arc<TimelineEntry>>,
}

impl Default for TimelineProjector {
    fn default() -> Self {
        let core = TranscriptCore {
            retain_tool_inspection: true,
            ..Default::default()
        };
        Self {
            session_id: None,
            core,
            entries: Vec::new(),
        }
    }
}

impl TimelineProjector {
    pub fn from_events(events: &[SessionEvent]) -> Self {
        let mut projector = Self {
            session_id: events.first().map(|event| event.session_id),
            ..Self::default()
        };
        for event in events {
            projector.core.apply_history(event);
        }
        projector.sync_entries();
        projector
    }

    pub fn extend(&mut self, events: &[SessionEvent]) {
        self.session_id = self
            .session_id
            .or_else(|| events.first().map(|event| event.session_id));
        for event in events {
            self.core.apply(event);
        }
        self.sync_entries();
    }

    pub fn into_entries(self) -> Vec<TimelineEntry> {
        self.entries.into_iter().map(Arc::unwrap_or_clone).collect()
    }

    pub fn into_shared_entries(self) -> Vec<Arc<TimelineEntry>> {
        self.entries
    }

    /// Recovery metadata covers more history than the bounded transcript window.
    /// Reconcile it through the same freshness gate, without resurrecting old rows.
    pub fn seed_subagents(&mut self, agents: &[SubagentSnapshot]) {
        for agent in agents {
            if Some(agent.parent_session_id) == self.session_id {
                self.core.reconcile_subagent_snapshot(agent, false);
            }
        }
        self.sync_entries();
    }

    pub fn push(&mut self, event: &SessionEvent) {
        self.session_id.get_or_insert(event.session_id);
        self.core.apply(event);
        self.sync_entries();
    }

    fn sync_entries(&mut self) {
        let changed_from = self.core.order.take_changed_from();
        let structural = self.entries.len() != self.core.order.len()
            || self
                .core
                .order
                .invalidations
                .borrow()
                .iter()
                .any(|change| !matches!(change, RenderInvalidation::Changed(_)));
        self.core.order.invalidations.borrow_mut().clear();
        self.core.prune_tool_inspection();
        self.core.order.removed.clear();
        let dirty = self
            .core
            .order
            .dirty
            .borrow_mut()
            .drain()
            .collect::<Vec<_>>();
        if structural {
            let start = changed_from
                .min(self.entries.len())
                .min(self.core.order.len());
            let old = self
                .entries
                .drain(start..)
                .map(|entry| (entry.id.clone(), entry))
                .collect::<HashMap<_, _>>();
            for index in start..self.core.order.len() {
                let id = self.row_id(index);
                let entry = self.project_entry(index, id.clone(), old.get(&id).map(Arc::as_ref));
                self.entries.push(match old.get(&id) {
                    Some(previous) if previous.as_ref() == &entry => Arc::clone(previous),
                    _ => Arc::new(entry),
                });
            }
        } else {
            for index in dirty {
                let Some(previous) = self.entries.get(index) else {
                    continue;
                };
                let entry = self.project_entry(index, self.row_id(index), Some(previous));
                if previous.as_ref() != &entry {
                    self.entries[index] = Arc::new(entry);
                }
            }
        }
    }

    fn row_id(&self, index: usize) -> String {
        if let Some((message, _)) = self.core.messages.iter().find(|(_, row)| **row == index) {
            return format!("message:{message}");
        }
        let (event, ordinal) = self.core.order.identities[index];
        format!("{event}:{ordinal}")
    }

    fn child_id(&self, index: usize) -> Option<Uuid> {
        match self.core.order.get(index)? {
            TranscriptEntry::Action {
                source_session_id, ..
            } => *source_session_id,
            _ => None,
        }
    }

    fn project_entry(
        &self,
        index: usize,
        id: String,
        previous: Option<&TimelineEntry>,
    ) -> TimelineEntry {
        let mut entry = TimelineEntry {
            id,
            created_at: DateTime::<Utc>::default(),
            kind: TimelineKind::Status,
            title: String::new(),
            detail: None,
            body: String::new(),
            rich_body: None,
            running: false,
            failed: false,
            target_session_id: self.child_id(index),
        };
        let time = match &self.core.order[index] {
            TranscriptEntry::Message {
                actor,
                text,
                effort,
                time,
                status,
                execution,
                ..
            } => {
                entry.kind = match actor {
                    EventActor::User => TimelineKind::User,
                    EventActor::Assistant => TimelineKind::Assistant,
                    EventActor::Tool | EventActor::System => TimelineKind::Status,
                };
                entry.title = match actor {
                    EventActor::User => "you",
                    EventActor::Assistant => "borg",
                    EventActor::Tool => "tool",
                    EventActor::System => "system",
                }
                .into();
                entry.detail = effort
                    .iter()
                    .map(String::as_str)
                    .chain(message_status_label(*status))
                    .collect::<Vec<_>>()
                    .join(" · ")
                    .into();
                entry.body.clone_from(text);
                entry.running = !execution.is_terminal();
                entry.failed = execution.is_failed();
                time
            }
            TranscriptEntry::Tool {
                name,
                detail,
                code_view,
                output_view,
                time,
                execution,
                ..
            } => {
                let reasoning = code_view
                    .as_ref()
                    .is_some_and(|(language, _)| language == "reasoning");
                entry.kind = if reasoning {
                    TimelineKind::Reasoning
                } else {
                    TimelineKind::Tool
                };
                entry.title.clone_from(name);
                entry.detail = (!detail.is_empty()).then(|| detail.clone());
                entry.body = if reasoning {
                    code_view
                        .as_ref()
                        .map(|(_, text)| text.clone())
                        .unwrap_or_default()
                } else {
                    self.core
                        .tool_inspection_text(index)
                        .map(str::to_owned)
                        .unwrap_or_else(|| {
                            output_view
                                .as_ref()
                                .or(code_view.as_ref())
                                .map(|(_, text)| text.clone())
                                .unwrap_or_default()
                        })
                };
                let lifecycle = execution.lifecycle();
                entry.running = matches!(
                    lifecycle,
                    Lifecycle::Running | Lifecycle::Backgrounded | Lifecycle::Waiting
                );
                entry.failed = lifecycle == Lifecycle::Failed;
                time
            }
            TranscriptEntry::Action {
                kind,
                label,
                detail,
                body,
                time,
                execution,
                ..
            } => {
                entry.kind = match kind {
                    TranscriptActionKind::Agent => TimelineKind::Subagent,
                    TranscriptActionKind::Approval | TranscriptActionKind::ProviderInteraction => {
                        TimelineKind::Approval
                    }
                    TranscriptActionKind::Error => TimelineKind::Error,
                    TranscriptActionKind::Watch => TimelineKind::Status,
                };
                entry.title.clone_from(label);
                entry.detail = (!detail.is_empty()).then(|| detail.clone());
                entry.body = body.clone().unwrap_or_default();
                entry.running = !execution.is_terminal();
                entry.failed = execution.is_failed();
                time
            }
            TranscriptEntry::Activity { text, time } => {
                entry.title = "Status".into();
                entry.body.clone_from(text);
                time
            }
            TranscriptEntry::Info { title, text, time } => {
                entry.title.clone_from(title);
                entry.body.clone_from(text);
                time
            }
            TranscriptEntry::Compaction {
                summary,
                time,
                execution,
                ..
            } => {
                entry.title = "Compaction".into();
                entry.body.clone_from(summary);
                entry.running = !execution.is_terminal();
                time
            }
            TranscriptEntry::Plan { items, time, .. } => {
                entry.title = "Plan".into();
                entry.body = items
                    .iter()
                    .map(|item| format!("{:?} · {}", item.status, item.content))
                    .collect::<Vec<_>>()
                    .join("\n");
                time
            }
            TranscriptEntry::Goal { goal, time } => {
                entry.title = "Goal".into();
                entry.body.clone_from(&goal.objective);
                time
            }
        };
        entry.created_at = DateTime::parse_from_rfc3339(time)
            .map(|time| time.with_timezone(&Utc))
            .unwrap_or_default();
        if let Some(agent) = entry
            .target_session_id
            .and_then(|id| self.core.effective_subagent_snapshot(id))
        {
            // A parked worker is waiting, not executing. The shared action
            // state also uses Waiting for approvals and dormant summaries.
            entry.running = matches!(
                agent.lifecycle(),
                SubagentLifecycle::Starting
                    | SubagentLifecycle::Running
                    | SubagentLifecycle::WaitingOnWatchers
                    | SubagentLifecycle::WaitingForApproval
            );
        }
        entry.detail = entry.detail.filter(|detail| !detail.is_empty());
        if entry.kind == TimelineKind::Assistant
            || (entry.kind == TimelineKind::Subagent
                && entry.target_session_id.is_none()
                && !entry.body.is_empty())
        {
            entry.rich_body = previous
                .filter(|previous| previous.body == entry.body)
                .and_then(|previous| previous.rich_body.clone())
                .or_else(|| Some(Arc::new(crate::markdown::project_markdown(&entry.body))));
        }
        entry
    }
}

pub fn project_timeline(events: &[SessionEvent]) -> Vec<TimelineEntry> {
    TimelineProjector::from_events(events).into_entries()
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
    use borg_remote::{CodingProvider, SessionEventKind};

    // Regression: valid snapshots used to append forever-live cards, while a
    // report boundary or stale delivery could contradict the roster. Type
    // checks cannot protect this live/replay and privacy contract.
    #[test]
    fn worker_status_stays_in_roster_until_attention_is_needed_live_and_replay() {
        use borg_remote::{
            SubagentActivityKind, SubagentLifecycle, SubagentSourceCursor, SubagentStatus,
            SubagentUsage,
        };
        let root = Uuid::new_v4();
        let child = Uuid::new_v4();
        let started = Utc::now();
        let mut snapshot = SubagentSnapshot {
            session_id: child,
            parent_session_id: root,
            task_name: "worker".into(),
            status: SubagentStatus::Running,
            lifecycle: Some(SubagentLifecycle::Running),
            source: None,
            provider: CodingProvider::Codex,
            model: None,
            effort: None,
            fast: false,
            ultrafast: false,
            desktop: false,
            cwd: ".".into(),
            created_at: started,
            updated_at: started,
            detail: None,
            final_text: None,
            usage: SubagentUsage::default(),
            interrupted_by: None,
        };
        let mut live = TimelineProjector::default();
        let mut events = Vec::new();
        for (revision, lifecycle, status, activity, running, failed) in [
            (
                1,
                SubagentLifecycle::Running,
                SubagentStatus::Running,
                SubagentActivityKind::Started,
                true,
                false,
            ),
            (
                2,
                SubagentLifecycle::Running,
                SubagentStatus::Running,
                SubagentActivityKind::Updated,
                true,
                false,
            ),
            (
                3,
                SubagentLifecycle::WaitingOnWatchers,
                SubagentStatus::Ready,
                SubagentActivityKind::Completed,
                true,
                false,
            ),
            (
                4,
                SubagentLifecycle::Dormant,
                SubagentStatus::Ready,
                SubagentActivityKind::Completed,
                false,
                false,
            ),
            (
                5,
                SubagentLifecycle::Failed,
                SubagentStatus::Failed,
                SubagentActivityKind::Failed,
                false,
                true,
            ),
        ] {
            snapshot.status = status;
            snapshot.lifecycle = Some(lifecycle);
            snapshot.source = Some(SubagentSourceCursor {
                child_sequence: revision,
                revision,
            });
            snapshot.updated_at = started + chrono::Duration::seconds(revision as i64);
            snapshot.final_text = Some("private child report".into());
            let child_event = (revision == 2).then(|| {
                Box::new(SessionEvent::new(
                    child,
                    revision,
                    SessionEventKind::Message {
                        message_id: Uuid::new_v4(),
                        actor: EventActor::Assistant,
                        text: "private commentary completed before tools".into(),
                        attachments: Vec::new(),
                        status: MessageStatus::Complete,
                        delivery: None,
                    },
                ))
            });
            let event = SessionEvent::new(
                root,
                revision,
                SessionEventKind::SubagentActivity {
                    activity,
                    agent: snapshot.clone(),
                    event: child_event,
                },
            );
            live.push(&event);
            events.push(event);
            let replay = project_timeline(&events);
            assert_eq!(live.entries.len(), usize::from(failed));
            assert_eq!(
                live.entries.iter().map(AsRef::as_ref).collect::<Vec<_>>(),
                replay.iter().collect::<Vec<_>>()
            );
            assert_eq!(
                live.core
                    .effective_subagent_snapshot(child)
                    .unwrap()
                    .lifecycle(),
                lifecycle
            );
            if let Some(card) = live.entries.first() {
                assert_eq!(card.id, format!("{}:0", events.last().unwrap().id));
                assert_eq!(card.target_session_id, Some(child));
                assert_eq!((card.running, card.failed), (running, failed));
                assert!(card.body.is_empty());
            }
        }
        let current = Arc::clone(&live.entries[0]);
        let mut stale = events[0].clone();
        stale.sequence = 6;
        stale.created_at = started + chrono::Duration::hours(1);
        live.push(&stale);
        events.push(stale);
        assert!(Arc::ptr_eq(&current, &live.entries[0]));

        for (from_child, text) in [
            (true, "visible child receipt"),
            (false, "**visible peer report**"),
        ] {
            let event = SessionEvent::new(
                root,
                events.len() as u64 + 1,
                SessionEventKind::AgentMessageReceived {
                    message_id: Uuid::new_v4(),
                    sender_id: child,
                    sender_name: "worker".into(),
                    text: text.into(),
                    from_child: Some(from_child),
                },
            );
            live.push(&event);
            events.push(event);
        }
        let replay = project_timeline(&events);
        assert_eq!(live.into_entries(), replay);
        assert_eq!(replay.len(), 3);
        assert_eq!(replay[1].body, "visible child receipt");
        assert_eq!(replay[2].body, "**visible peer report**");
        assert!(replay[2].rich_body.is_some());
        assert_eq!(replay[2].target_session_id, None);

        // The root roster is still shown when focusing a child, but siblings
        // are peers, not that child's private subagents (including old receipts).
        let mut focused = TimelineProjector::from_events(&[SessionEvent::new(
            child,
            1,
            SessionEventKind::SessionStarted,
        )]);
        snapshot.session_id = Uuid::new_v4();
        focused.seed_subagents(&[snapshot.clone()]);
        focused.push(&SessionEvent::new(
            child,
            2,
            SessionEventKind::AgentMessageReceived {
                message_id: Uuid::new_v4(),
                sender_id: snapshot.session_id,
                sender_name: "sibling".into(),
                text: "visible sibling report".into(),
                from_child: None,
            },
        ));
        assert_eq!(focused.entries.len(), 1);
        assert_eq!(focused.entries[0].body, "visible sibling report");
    }

    // Regression: a shared-core adapter must not replace full GUI inspection
    // with a formatted/preview body, or reallocate unrelated markdown on each
    // tool update. Neither condition is observable to the compiler.
    #[test]
    fn shared_adapter_preserves_full_inspection_markdown_and_incremental_identity() {
        let session = Uuid::new_v4();
        let message = Uuid::new_v4();
        let output = format!("{}\ninspection tail", "full output\n".repeat(2_000));
        let events = [
            SessionEventKind::Message {
                message_id: message,
                actor: EventActor::Assistant,
                text: "## Answer\n**kept markdown**".into(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
            SessionEventKind::ReasoningDelta {
                text: "checking".into(),
            },
            SessionEventKind::ReasoningDelta {
                text: "checking the workspace".into(),
            },
            SessionEventKind::ReasoningCompleted,
            SessionEventKind::ToolStarted {
                tool_call_id: "inspect".into(),
                name: "exec".into(),
                input: serde_json::json!({"cmd":"inspect"}),
                input_ref: None,
                parent_tool_call_id: None,
            },
            SessionEventKind::ToolCompleted {
                tool_call_id: "inspect".into(),
                output: output.clone(),
                output_ref: None,
                is_error: false,
                input: None,
                input_ref: None,
                parent_tool_call_id: None,
            },
        ]
        .into_iter()
        .enumerate()
        .map(|(index, kind)| SessionEvent::new(session, index as u64 + 1, kind))
        .collect::<Vec<_>>();
        let mut live = TimelineProjector::default();
        live.extend(&events[..5]);
        let assistant = Arc::clone(&live.entries[0]);
        let reasoning = Arc::clone(&live.entries[1]);
        let tool_id = live.entries[2].id.clone();
        live.push(&events[5]);
        assert!(Arc::ptr_eq(&assistant, &live.entries[0]));
        assert!(Arc::ptr_eq(&reasoning, &live.entries[1]));
        assert_eq!(live.entries[0].id, format!("message:{message}"));
        assert!(
            assistant
                .rich_body
                .as_ref()
                .unwrap()
                .spans
                .iter()
                .any(|span| span.style.strong)
        );
        assert_eq!(reasoning.body, "checking the workspace");
        assert!(!reasoning.running);
        let tool = &live.entries[2];
        assert_eq!(tool.id, tool_id);
        assert_eq!(tool.body, output);
        assert_eq!(tool.target_session_id, None);
        assert!(!tool.running);
        assert_eq!(live.into_entries(), project_timeline(&events));
        // A bounded reconnect may start at the result, without the call.
        let bounded = project_timeline(&events[5..]);
        assert_eq!(bounded.len(), 1);
        assert_eq!(bounded[0].body, output);
        assert!(!bounded[0].running);
        let mut cleared = TimelineProjector::from_events(&events[5..]);
        assert_eq!(cleared.core.tool_inspection_text(0), Some(output.as_str()));
        cleared.push(&SessionEvent::new(
            session,
            7,
            SessionEventKind::ContextCleared,
        ));
        assert!(cleared.entries.is_empty());
        assert_eq!(cleared.core.tool_inspection_text(0), None);
    }

    #[test]
    fn keyed_preparation_cancellation_allows_id_reuse_without_orphaning_parallel_rows() {
        let session_id = Uuid::new_v4();
        let mut projector = TimelineProjector::default();
        for (sequence, kind, id, label) in [
            (1, "action/preparing", "retry", "first attempt"),
            (2, "action/preparing", "parallel", "parallel work"),
            (3, "action/preparing_cancelled", "retry", ""),
            (4, "action/preparing", "retry", "new attempt"),
        ] {
            projector.push(&SessionEvent::new(
                session_id,
                sequence,
                SessionEventKind::ProviderEvent {
                    provider: CodingProvider::Codex,
                    kind: kind.into(),
                    payload: serde_json::json!({"tool_call_id": id, "label": label}),
                },
            ));
        }
        assert!(!projector.entries[0].running);
        assert!(
            projector.entries[projector.core.tools[&projector.core.preparing_tools["parallel"]]]
                .running
        );
        let retry =
            &projector.entries[projector.core.tools[&projector.core.preparing_tools["retry"]]];
        assert!(retry.running);
        assert!(retry.title.ends_with("new attempt"));
        assert_eq!(
            projector
                .entries
                .iter()
                .filter(|entry| entry.running)
                .count(),
            2
        );
    }

    #[test]
    fn replay_parent_cancellation_settles_nested_wait_not_parallel_tool() {
        let session_id = Uuid::new_v4();
        let mut projector = TimelineProjector::default();
        for (index, (id, parent, name)) in [
            ("runtime", None, "runtime_exec"),
            ("wait", Some("runtime"), "wait_agent"),
            ("parallel", None, "exec"),
        ]
        .into_iter()
        .enumerate()
        {
            projector.push(&SessionEvent::new(
                session_id,
                index as u64 + 1,
                SessionEventKind::ToolStarted {
                    tool_call_id: id.into(),
                    name: name.into(),
                    input: serde_json::json!({}),
                    input_ref: None,
                    parent_tool_call_id: parent.map(str::to_string),
                },
            ));
        }
        projector.push(&SessionEvent::new(
            session_id,
            4,
            SessionEventKind::ToolCompleted {
                tool_call_id: "runtime".into(),
                output: "persistent runtime was cancelled".into(),
                output_ref: None,
                is_error: true,
                input: None,
                input_ref: None,
                parent_tool_call_id: None,
            },
        ));
        assert!(!projector.entries[projector.core.tools["wait"]].running);
        assert!(projector.entries[projector.core.tools["wait"]].failed);
        assert!(projector.entries[projector.core.tools["parallel"]].running);
    }
    #[test]
    fn replay_preserves_ultrafast_badge_when_an_old_message_updates() {
        let session_id = Uuid::new_v4();
        let message_id = Uuid::new_v4();
        let mut projector = TimelineProjector::default();
        for (sequence, fast, ultrafast) in [(1, false, true), (3, true, false)] {
            projector.push(&SessionEvent::new(
                session_id,
                sequence,
                SessionEventKind::TurnStarted {
                    message_id: Uuid::new_v4(),
                    provider: borg_remote::CodingProvider::Codex,
                    model: None,
                    effort: None,
                    fast,
                    ultrafast,
                },
            ));
            projector.push(&SessionEvent::new(
                session_id,
                sequence + 1,
                SessionEventKind::Message {
                    message_id,
                    actor: EventActor::Assistant,
                    text: "answer".into(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ));
        }
        assert_eq!(
            projector.into_entries()[0].detail.as_deref(),
            Some("ultrafast")
        );
    }

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
    fn fresh_tool_call_id_starts_a_new_audit_entry() {
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
                parent_tool_call_id: None,
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
                parent_tool_call_id: None,
            },
        ));
        projector.push(&SessionEvent::new(
            session_id,
            3,
            SessionEventKind::ToolStarted {
                tool_call_id: "command-2".into(),
                name: "exec_command".into(),
                input: serde_json::json!({"cmd": "second"}),
                input_ref: None,
                parent_tool_call_id: None,
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
                parent_tool_call_id: None,
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
                parent_tool_call_id: None,
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
            "Stopped generating tool call · inspect first"
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
                    payload: if label.is_empty() {
                        serde_json::json!({})
                    } else {
                        serde_json::json!({"label": label})
                    },
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
                    parent_tool_call_id: None,
                },
            ));
        }

        assert_eq!(projector.entries.len(), 2);
        assert_ne!(
            projector.core.tools["tool-a"],
            projector.core.tools["tool-b"]
        );
        assert!(projector.entries.iter().all(|entry| entry.running));
        assert!(
            projector
                .entries
                .iter()
                .all(|entry| !entry.title.starts_with("Generate"))
        );
    }

    #[test]
    fn generation_status_waits_and_resumes_the_same_action() {
        let session_id = Uuid::new_v4();
        let started_at = Utc::now();
        let mut projector = TimelineProjector::default();
        let mut preparing = SessionEvent::new(
            session_id,
            1,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "action/preparing".into(),
                payload: serde_json::json!({
                    "label": "read configuration",
                    "tool_call_id": null,
                }),
            },
        );
        preparing.created_at = started_at;
        projector.push(&preparing);
        let original_id = projector.entries[0].id.clone();
        let original_detail = projector.entries[0].detail.clone();

        for (sequence, waiting) in [(2, true), (3, false)] {
            projector.push(&SessionEvent::new(
                session_id,
                sequence,
                SessionEventKind::ProviderEvent {
                    provider: CodingProvider::Codex,
                    kind: "action/generation_status".into(),
                    payload: serde_json::json!({
                        "tool_call_id": if waiting { None } else { Some("read-1") },
                        "waiting": waiting,
                        "label": "read configuration",
                    }),
                },
            ));
            assert_eq!(projector.entries.len(), 1);
            assert_eq!(projector.entries[0].id, original_id);
            assert_eq!(projector.entries[0].created_at, started_at);
            assert_eq!(projector.entries[0].detail, original_detail);
            if waiting {
                assert_eq!(
                    tool_lifecycle_label(&projector.entries[0].title, false),
                    "Awaiting tool-call arguments…"
                );
            }
        }
        assert_eq!(projector.entries[0].title, "Generate read configuration");
        assert!(projector.entries[0].running);
        projector.push(&SessionEvent::new(
            session_id,
            4,
            SessionEventKind::ToolStarted {
                tool_call_id: "read-1".into(),
                name: "read_file".into(),
                input: serde_json::json!({"path": "config.toml"}),
                input_ref: None,
                parent_tool_call_id: None,
            },
        ));
        assert_eq!(projector.entries.len(), 1);
        assert!(!projector.entries[0].title.starts_with("Generate"));
    }

    #[test]
    fn generation_status_respects_parallel_and_completed_actions() {
        let session_id = Uuid::new_v4();
        let mut projector = TimelineProjector::default();
        for (sequence, tool_call_id, label) in [(1, "tool-a", "read"), (2, "tool-b", "run tests")] {
            projector.push(&SessionEvent::new(
                session_id,
                sequence,
                SessionEventKind::ProviderEvent {
                    provider: CodingProvider::Codex,
                    kind: "action/preparing".into(),
                    payload: serde_json::json!({
                        "tool_call_id": tool_call_id,
                        "label": label,
                    }),
                },
            ));
        }
        projector.push(&SessionEvent::new(
            session_id,
            3,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "action/generation_status".into(),
                payload: serde_json::json!({
                    "tool_call_id": "tool-b",
                    "waiting": true,
                    "label": "run tests",
                }),
            },
        ));
        assert_eq!(projector.entries[0].title, "Generate read");
        assert_eq!(projector.entries[1].title, "Wait for provider");

        projector.push(&SessionEvent::new(
            session_id,
            4,
            SessionEventKind::ToolStarted {
                tool_call_id: "tool-b".into(),
                name: "exec_command".into(),
                input: serde_json::json!({"cmd": "cargo test"}),
                input_ref: None,
                parent_tool_call_id: None,
            },
        ));
        let promoted_title = projector.entries[1].title.clone();
        projector.push(&SessionEvent::new(
            session_id,
            5,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "action/generation_status".into(),
                payload: serde_json::json!({
                    "tool_call_id": "tool-b",
                    "waiting": true,
                    "label": "run tests",
                }),
            },
        ));
        assert_eq!(projector.entries[1].title, promoted_title);
        projector.push(&SessionEvent::new(
            session_id,
            6,
            SessionEventKind::ToolCompleted {
                tool_call_id: "tool-b".into(),
                output: "done".into(),
                output_ref: None,
                is_error: false,
                input: None,
                input_ref: None,
                parent_tool_call_id: None,
            },
        ));
        projector.push(&SessionEvent::new(
            session_id,
            7,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "action/generation_status".into(),
                payload: serde_json::json!({
                    "tool_call_id": "tool-b",
                    "waiting": false,
                    "label": "run tests",
                }),
            },
        ));
        assert!(!projector.entries[1].running);
        assert_eq!(projector.entries[1].title, promoted_title);

        projector.push(&SessionEvent::new(
            session_id,
            8,
            SessionEventKind::TurnCompleted {
                message_id: Uuid::new_v4(),
                provider_session_id: None,
                final_text: String::new(),
                error: None,
            },
        ));
        assert!(!projector.entries[0].running);
        assert_eq!(
            tool_lifecycle_label(&projector.entries[0].title, true),
            "Stopped generating tool call · read"
        );
        assert_eq!(projector.entries[1].title, promoted_title);
    }
}
