//! `wait_agent`: one blocking call per unit of child progress.
//!
//! A parent that waits on children should spend one tool call per real event,
//! not one per minute. The wait therefore blocks for up to half an hour and
//! returns as soon as something the parent has not yet been shown happens: a
//! child settles (finished, failed, stopped, or needs approval), a child
//! reports to the parent, or human/team input is waiting for the parent's
//! turn. What was reported is remembered per parent, so a child that finished
//! earlier does not end every later wait immediately.

use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::watch;

pub(crate) const DEFAULT_WAIT: Duration = Duration::from_secs(10 * 60);
pub(crate) const MAX_WAIT: Duration = Duration::from_secs(30 * 60);
const MIN_WAIT: Duration = Duration::from_millis(100);
/// A child's final message and its Ready status arrive as separate events a
/// moment apart; reporting them as one wake saves the parent a second call.
const COALESCE: Duration = Duration::from_millis(750);
/// With no child working, nothing is going to end the wait. Linger briefly for
/// a just-assigned child to start, then say so instead of blocking.
const IDLE_GRACE: Duration = Duration::from_secs(5);
/// Reports that bypass the activity stream (durable cross-process replays) are
/// still noticed within this bound.
const RECHECK: Duration = Duration::from_secs(5);
const TEXT_LIMIT: usize = 4_000;
const LINE_LIMIT: usize = 200;
const AGENT_LIMIT: usize = 40;

/// What one parent has already been shown by `wait_agent`.
#[derive(Default)]
pub(super) struct WaitCursor {
    settled: HashMap<Uuid, u64>,
    messages: HashSet<Uuid>,
}

/// External reasons for a wait to end early.
#[derive(Default)]
pub(crate) struct WaitSignals {
    /// Cancelled when the tool call is abandoned or a steer preempts it.
    pub cancel: Option<CancellationToken>,
    /// True while human or team input is queued for the waiting session.
    pub input_pending: Option<watch::Receiver<bool>>,
    /// One pending input should interrupt at most one wait until it clears.
    pub input_reported: Option<Arc<AtomicBool>>,
}

#[derive(Default)]
struct Unseen {
    changes: Vec<SubagentSnapshot>,
    messages: Vec<(Uuid, Uuid, String)>,
}

impl Unseen {
    fn is_empty(&self) -> bool {
        self.changes.is_empty() && self.messages.is_empty()
    }
}

fn settled(agent: &SubagentSnapshot) -> bool {
    match agent.status {
        SubagentStatus::Ready => agent.final_text.is_some(),
        SubagentStatus::Stopped | SubagentStatus::Failed | SubagentStatus::WaitingForApproval => {
            true
        }
        SubagentStatus::Starting | SubagentStatus::Running => false,
    }
}

/// Identifies one settled state of a child. Not `updated_at`: every child
/// event bumps that, including usage that lands after the child went idle,
/// which would report the same completion twice.
fn fingerprint(agent: &SubagentSnapshot) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    (agent.status as u8).hash(&mut hasher);
    if agent.status == SubagentStatus::Ready {
        agent.final_text.hash(&mut hasher);
    } else {
        agent.detail.hash(&mut hasher);
    }
    hasher.finish()
}

fn working(agent: &SubagentSnapshot) -> bool {
    matches!(
        agent.status,
        SubagentStatus::Starting | SubagentStatus::Running
    )
}

/// Child events that can change what the parent should be told. Streaming
/// deltas and tool chatter are not, and there can be many of them.
fn may_change_state(activity: &SubagentActivity) -> bool {
    match activity {
        SubagentActivity::SessionEvent { event, .. } => matches!(
            event.kind,
            SessionEventKind::StatusChanged { .. }
                | SessionEventKind::ApprovalRequested { .. }
                | SessionEventKind::Message {
                    status: MessageStatus::Complete,
                    ..
                }
        ),
        _ => true,
    }
}

fn bounded(text: &str, limit: usize) -> String {
    let text = text.trim();
    match text.char_indices().nth(limit) {
        Some((end, _)) => format!("{}…", &text[..end]),
        None => text.to_string(),
    }
}

fn last_line(agent: &SubagentSnapshot) -> Option<String> {
    agent
        .final_text
        .as_deref()
        .and_then(|text| text.lines().rev().find(|line| !line.trim().is_empty()))
        .or(agent.detail.as_deref())
        .map(|line| bounded(line, LINE_LIMIT))
}

impl SubagentCoordinator {
    /// Block until the parent has something new to act on, or `timeout`.
    pub(crate) async fn wait_for(
        &self,
        actor: Uuid,
        timeout: Duration,
        signals: WaitSignals,
    ) -> Result<Value> {
        let started = tokio::time::Instant::now();
        let deadline = started + timeout.clamp(MIN_WAIT, MAX_WAIT);
        let cancel = signals.cancel.unwrap_or_default();
        let mut input = signals.input_pending;
        let reported = signals.input_reported.unwrap_or_default();
        // Subscribe before the first look so nothing lands in between.
        let mut activity = self.subscribe();
        let mut wakes = self.subscribe_root_messages();
        let mut wakes_open = self.is_root_session(actor);
        let mut woken = Vec::new();

        let unseen = self.unseen(actor, &woken).await;
        if !unseen.is_empty() {
            return self.report(actor, "child_update", unseen, started).await;
        }
        if input_waiting(&input) && !reported.swap(true, Ordering::AcqRel) {
            return self.report(actor, "input_pending", unseen, started).await;
        }
        let idle_at_start = !self.children(actor).await.iter().any(working);
        let mut until = if idle_at_start {
            deadline.min(started + IDLE_GRACE)
        } else {
            deadline
        };
        let mut settle_at: Option<tokio::time::Instant> = None;
        let mut recheck = tokio::time::interval_at(started + RECHECK, RECHECK);
        loop {
            let wake_at = settle_at.unwrap_or(until);
            let mut check = false;
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    let reason = if input_waiting(&input) && !reported.swap(true, Ordering::AcqRel) { "input_pending" } else { "cancelled" };
                    let unseen = self.unseen(actor, &woken).await;
                    return self.report(actor, reason, unseen, started).await;
                }
                () = input_arrives(&mut input, &reported) => {
                    let unseen = self.unseen(actor, &woken).await;
                    return self.report(actor, "input_pending", unseen, started).await;
                }
                message = wakes.recv(), if wakes_open => match message {
                    Ok(message) => {
                        woken.push(message);
                        check = true;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => check = true,
                    Err(broadcast::error::RecvError::Closed) => wakes_open = false,
                },
                event = activity.recv() => match event {
                    Ok(event) => check = may_change_state(&event),
                    Err(broadcast::error::RecvError::Lagged(_)) => check = true,
                    Err(broadcast::error::RecvError::Closed) => {
                        bail!("subagent activity stream closed")
                    }
                },
                _ = recheck.tick() => check = true,
                () = tokio::time::sleep_until(wake_at) => {
                    if settle_at.take().is_some() {
                        // The change can vanish while coalescing: the child
                        // resumed work, or its message was delivered as input.
                        // An empty result reads as a dropped message.
                        let unseen = self.unseen(actor, &woken).await;
                        if !unseen.is_empty() {
                            return self.report(actor, "child_update", unseen, started).await;
                        }
                        continue;
                    }
                    if until < deadline && self.children(actor).await.iter().any(working) {
                        until = deadline;
                        continue;
                    }
                    let reason = if until < deadline { "no_active_children" } else { "timeout" };
                    let unseen = self.unseen(actor, &woken).await;
                    return self.report(actor, reason, unseen, started).await;
                }
            }
            if check && settle_at.is_none() && !self.unseen(actor, &woken).await.is_empty() {
                settle_at = Some(tokio::time::Instant::now() + COALESCE);
            }
        }
    }

    /// The parent is giving this child new work, so the state the child is
    /// resting in (say, idle after an interrupt) is not news to report back:
    /// the next wait should end on what the new work does.
    pub(super) async fn mark_seen(&self, actor: Uuid, agent: &SubagentSnapshot) {
        if settled(agent) {
            self.wait_cursors
                .lock()
                .await
                .entry(actor)
                .or_default()
                .settled
                .insert(agent.session_id, fingerprint(agent));
        }
    }

    pub(crate) async fn has_working_children(&self, actor: Uuid) -> bool {
        self.children(actor).await.iter().any(working)
    }

    async fn children(&self, actor: Uuid) -> Vec<SubagentSnapshot> {
        self.table
            .lock()
            .await
            .snapshots()
            .into_iter()
            .filter(|agent| agent.parent_session_id == actor)
            .collect()
    }

    async fn unseen(&self, actor: Uuid, woken: &[TeamInboxMessage]) -> Unseen {
        let children = self.children(actor).await;
        let inbox = if self.is_root_session(actor) {
            self.root_inbox.lock().await.clone()
        } else {
            Vec::new()
        };
        let cursors = self.wait_cursors.lock().await;
        let cursor = cursors.get(&actor);
        let changes = children
            .into_iter()
            .filter(|agent| {
                settled(agent)
                    && cursor.and_then(|cursor| cursor.settled.get(&agent.session_id))
                        != Some(&fingerprint(agent))
            })
            .collect();
        let mut messages: Vec<(Uuid, Uuid, String)> = Vec::new();
        for message in inbox.iter().chain(woken) {
            if message.sender_session_id != actor
                && !cursor.is_some_and(|cursor| cursor.messages.contains(&message.message_id))
                && !messages.iter().any(|(id, ..)| *id == message.message_id)
            {
                messages.push((
                    message.message_id,
                    message.sender_session_id,
                    message.report_text.clone(),
                ));
            }
        }
        Unseen { changes, messages }
    }

    /// Build the result and remember what it showed.
    async fn report(
        &self,
        actor: Uuid,
        reason: &str,
        unseen: Unseen,
        started: tokio::time::Instant,
    ) -> Result<Value> {
        {
            let mut cursors = self.wait_cursors.lock().await;
            let cursor = cursors.entry(actor).or_default();
            for agent in &unseen.changes {
                cursor.settled.insert(agent.session_id, fingerprint(agent));
            }
            cursor
                .messages
                .extend(unseen.messages.iter().map(|(id, ..)| *id));
        }
        if self.is_root_session(actor) && !unseen.messages.is_empty() {
            // Reports shown here are read: acknowledge them so
            // list_unread_team_messages does not return them again. Wake
            // messages are left to the steer that delivers them.
            let inbox = self
                .root_inbox
                .lock()
                .await
                .iter()
                .map(|message| message.message_id)
                .collect::<HashSet<_>>();
            let read = unseen
                .messages
                .iter()
                .map(|(id, ..)| *id)
                .filter(|id| inbox.contains(id))
                .collect::<Vec<_>>();
            if let Err(error) = self.acknowledge_messages_for_session(actor, &read).await {
                tracing::debug!(%error, "wait_agent could not acknowledge the reports it returned");
            }
        }
        let mut children = self.children(actor).await;
        children.sort_by_key(|agent| !working(agent));
        let now = Utc::now();
        let agents = children
            .iter()
            .take(AGENT_LIMIT)
            .map(|agent| {
                let mut row = json!({
                    "task_name": agent.task_name,
                    "session_id": agent.session_id,
                    "status": agent.status,
                    "last": last_line(agent),
                });
                if working(agent) {
                    row["quiet_s"] = json!((now - agent.updated_at).num_seconds().max(0));
                }
                row
            })
            .collect::<Vec<_>>();
        let mut senders = HashMap::new();
        for (_, sender, _) in &unseen.messages {
            if !senders.contains_key(sender) {
                let name = match self.get(*sender).await {
                    Some(agent) => agent.task_name,
                    None => sender.to_string(),
                };
                senders.insert(*sender, name);
            }
        }
        let reason = match (
            reason,
            unseen.changes.is_empty(),
            unseen.messages.is_empty(),
        ) {
            ("child_update", false, true) => "child_settled",
            ("child_update", true, false) => "child_message",
            (reason, ..) => reason,
        };
        let mut result = json!({
            "reason": reason,
            "waited_ms": u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "changes": unseen.changes.iter().map(|agent| json!({
                "task_name": agent.task_name,
                "session_id": agent.session_id,
                "status": agent.status,
                "detail": agent.detail,
                "final_text": agent.final_text.as_deref().map(|text| bounded(text, TEXT_LIMIT)),
            })).collect::<Vec<_>>(),
            "messages": unseen.messages.iter().map(|(id, sender, text)| json!({
                "message_id": id,
                "from": senders[sender],
                "text": bounded(text, TEXT_LIMIT),
            })).collect::<Vec<_>>(),
            "agents": agents,
        });
        if children.len() > AGENT_LIMIT {
            result["more_agents"] = json!(children.len() - AGENT_LIMIT);
        }
        let note = match reason {
            "input_pending" => Some(
                "Human or team input is waiting and is delivered right after this result. Answer it before waiting again.",
            ),
            "no_active_children" => Some(
                "No child is working, so nothing would end this wait. Assign work or continue yourself.",
            ),
            _ => None,
        };
        if let Some(note) = note {
            result["note"] = json!(note);
        }
        Ok(result)
    }
}

fn input_waiting(input: &Option<watch::Receiver<bool>>) -> bool {
    input.as_ref().is_some_and(|input| *input.borrow())
}

/// Resolves when input is waiting. A dropped sender can never signal again,
/// so that case pends instead of spinning.
async fn input_arrives(input: &mut Option<watch::Receiver<bool>>, reported: &AtomicBool) {
    if let Some(input) = input {
        while input.wait_for(|pending| !*pending).await.is_ok() {
            if input.wait_for(|pending| *pending).await.is_err() {
                break;
            }
            if !reported.swap(true, Ordering::AcqRel) {
                return;
            }
        }
    }
    std::future::pending().await
}
