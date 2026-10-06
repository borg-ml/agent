//! Latest-state conflation for live session events.
//!
//! Streaming output reaches observers as replaceable live state: each
//! in-progress message snapshot carries the whole reply so far, reasoning
//! snapshots supersede one another, and a context-window update replaces the
//! last one. A consumer that falls behind should therefore jump to the newest
//! state instead of replaying every intermediate one. Ordered events (tool
//! boundaries, durable rows, status changes) are never merged, and a snapshot
//! is never merged across an ordered event of its own stream, so applying the
//! conflated queue is equivalent to applying every event in order.

use std::collections::{HashMap, VecDeque};

use uuid::Uuid;

use crate::{MessageStatus, SessionEvent, SessionEventKind};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SnapshotKey {
    Message(Uuid),
    Reasoning,
    ContextWindow,
}

/// The replaceable state this event kind describes, if any.
pub(crate) fn snapshot_key(kind: &SessionEventKind) -> Option<SnapshotKey> {
    match kind {
        SessionEventKind::Message {
            message_id,
            status: MessageStatus::InProgress,
            ..
        } => Some(SnapshotKey::Message(*message_id)),
        SessionEventKind::ReasoningDelta { .. } => Some(SnapshotKey::Reasoning),
        SessionEventKind::ContextWindowUpdated { .. } => Some(SnapshotKey::ContextWindow),
        _ => None,
    }
}

/// Fold `next` into `previous`; both must have the same [`snapshot_key`].
pub(crate) fn merge_snapshot(previous: &mut SessionEventKind, next: SessionEventKind) {
    match (previous, next) {
        (
            SessionEventKind::ReasoningDelta { text: previous },
            SessionEventKind::ReasoningDelta { text: next },
        ) => {
            // Owners send cumulative snapshots; tolerate a stale shorter one
            // and plain fragments.
            if next.starts_with(previous.as_str()) {
                *previous = next;
            } else if !previous.starts_with(next.as_str()) {
                previous.push_str(&next);
            }
        }
        (previous, next) => *previous = next,
    }
}

/// The stream an event belongs to (the root session or one child) and the
/// replaceable state it carries. Durable rows never conflate.
fn live_key(event: &SessionEvent) -> (Uuid, Option<SnapshotKey>) {
    let live = event.sequence == 0;
    match &event.kind {
        SessionEventKind::SubagentActivity { agent, event, .. } => (
            agent.session_id,
            event
                .as_deref()
                .filter(|_| live)
                .and_then(|child| snapshot_key(&child.kind)),
        ),
        kind => (event.session_id, live.then(|| snapshot_key(kind)).flatten()),
    }
}

/// Merge a same-key event into the pending one. The pending envelope keeps its
/// id and timestamp, so a reasoning block keeps its start time; a child's
/// agent snapshot advances to the newest.
fn merge_event(pending: &mut SessionEvent, next: SessionEvent) {
    match (&mut pending.kind, next.kind) {
        (
            SessionEventKind::SubagentActivity {
                activity,
                agent,
                event: Some(pending_child),
            },
            SessionEventKind::SubagentActivity {
                activity: next_activity,
                agent: next_agent,
                event: Some(next_child),
            },
        ) => {
            *activity = next_activity;
            *agent = next_agent;
            merge_snapshot(&mut pending_child.kind, next_child.kind);
        }
        (pending, next) => merge_snapshot(pending, next),
    }
}

/// Ordered queue of live session events that keeps only the newest state of
/// each pending snapshot.
#[derive(Debug, Default)]
pub struct LiveEventQueue {
    events: VecDeque<SessionEvent>,
    /// Absolute position of `events[0]`; positions only advance by popping.
    front: u64,
    /// Snapshots still mergeable, by stream and state, at absolute positions.
    pending: HashMap<(Uuid, SnapshotKey), u64>,
}

impl LiveEventQueue {
    /// Append an event, merging it into a queued snapshot of the same state
    /// when no ordered event of its stream has been queued since.
    pub fn push_back(&mut self, event: SessionEvent) {
        let (stream, key) = live_key(&event);
        let Some(key) = key else {
            self.pending
                .retain(|(pending_stream, _), _| *pending_stream != stream);
            self.events.push_back(event);
            return;
        };
        if let Some(&at) = self.pending.get(&(stream, key))
            && let Some(slot) = at
                .checked_sub(self.front)
                .and_then(|index| self.events.get_mut(index as usize))
        {
            merge_event(slot, event);
            return;
        }
        self.pending
            .insert((stream, key), self.front + self.events.len() as u64);
        self.events.push_back(event);
    }

    /// Put recovered events ahead of everything queued, in their order.
    /// They are never merged, and nothing queued merges across them.
    pub fn extend_front(&mut self, events: Vec<SessionEvent>) {
        if events.is_empty() {
            return;
        }
        self.pending.clear();
        for event in events.into_iter().rev() {
            self.events.push_front(event);
        }
    }

    pub fn pop_front(&mut self) -> Option<SessionEvent> {
        let event = self.events.pop_front()?;
        self.front += 1;
        if self.events.is_empty() {
            self.pending.clear();
        }
        Some(event)
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EventActor;

    fn event(session_id: Uuid, sequence: u64, kind: SessionEventKind) -> SessionEvent {
        SessionEvent::new(session_id, sequence, kind)
    }

    fn reply(message_id: Uuid, text: &str) -> SessionEventKind {
        SessionEventKind::Message {
            message_id,
            actor: EventActor::Assistant,
            text: text.into(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        }
    }

    fn reasoning(text: &str) -> SessionEventKind {
        SessionEventKind::ReasoningDelta { text: text.into() }
    }

    /// A lagging consumer sees only the newest state of each snapshot, but
    /// never one merged across an ordered event or into a durable row.
    #[test]
    fn backlog_jumps_to_latest_state_without_reordering() {
        let session = Uuid::new_v4();
        let message = Uuid::new_v4();
        let mut queue = LiveEventQueue::default();
        let first_reasoning = event(session, 0, reasoning("a"));
        let started_at = first_reasoning.created_at;
        queue.push_back(first_reasoning);
        for text in ["one", "one two", "one two three"] {
            queue.push_back(event(session, 0, reply(message, text)));
        }
        queue.push_back(event(session, 0, reasoning("ab")));
        queue.push_back(event(session, 7, SessionEventKind::ReasoningCompleted));
        queue.push_back(event(session, 0, reasoning("c")));
        queue.push_back(event(session, 8, reply(message, "durable")));
        queue.push_back(event(session, 9, reply(message, "durable again")));

        let drained = std::iter::from_fn(|| queue.pop_front()).collect::<Vec<_>>();
        let kinds = drained
            .iter()
            .map(|event| format!("{:?}", event.kind))
            .collect::<Vec<_>>();
        let expected = [
            reasoning("ab"),
            reply(message, "one two three"),
            SessionEventKind::ReasoningCompleted,
            reasoning("c"),
            reply(message, "durable"),
            reply(message, "durable again"),
        ]
        .map(|kind| format!("{kind:?}"));
        assert_eq!(kinds, expected);
        assert_eq!(
            drained[0].created_at, started_at,
            "a block keeps its start time"
        );
    }
}
