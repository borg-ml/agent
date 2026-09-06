use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tokio::time::Instant;

use crate::{CodingProvider, SessionEventKind, edit_is_awaiting_diff};

const INPUT_IDLE_TIMEOUT: Duration = Duration::from_secs(5);

struct PendingGeneration {
    label: String,
    last_input: Instant,
    waiting: bool,
}

pub(crate) struct GenerationActivity {
    provider: CodingProvider,
    pending: HashMap<Option<String>, PendingGeneration>,
    executing: HashSet<String>,
}

impl GenerationActivity {
    pub(crate) fn new(provider: CodingProvider) -> Self {
        Self {
            provider,
            pending: HashMap::new(),
            executing: HashSet::new(),
        }
    }

    // Consume fragment pulses here, rather than persisting one timeline event per token.
    pub(crate) fn observe(
        &mut self,
        event: SessionEventKind,
        now: Instant,
    ) -> Option<SessionEventKind> {
        match &event {
            SessionEventKind::ProviderEvent { kind, payload, .. }
                if kind == "action/preparing" || kind == "action/input_delta" =>
            {
                let id = payload
                    .get("tool_call_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
                if id.as_ref().is_some_and(|id| self.executing.contains(id)) {
                    return None;
                }
                if id.is_some()
                    && !self.pending.contains_key(&id)
                    && let Some(pending) = self.pending.remove(&None)
                {
                    self.pending.insert(id.clone(), pending);
                }
                if kind == "action/input_delta" {
                    let pending = self.pending.get_mut(&id)?;
                    pending.last_input = now;
                    if !pending.waiting {
                        return None;
                    }
                    pending.waiting = false;
                    return Some(status(self.provider, &id, pending));
                }
                let label = payload
                    .get("label")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                self.pending.insert(
                    id,
                    PendingGeneration {
                        label: label.to_owned(),
                        last_input: now,
                        waiting: false,
                    },
                );
            }
            SessionEventKind::ProviderEvent { kind, .. }
                if kind == "action/preparing_cancelled" =>
            {
                self.pending.remove(&None);
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
            } if !edit_is_awaiting_diff(name, input) => {
                self.finish(tool_call_id);
            }
            SessionEventKind::ToolCompleted { tool_call_id, .. } => self.finish(tool_call_id),
            SessionEventKind::TurnCompleted { .. } => self.pending.clear(),
            _ => {}
        }
        Some(event)
    }

    fn finish(&mut self, id: &str) {
        if self.pending.remove(&Some(id.to_owned())).is_none() {
            self.pending.remove(&None);
        }
        self.executing.insert(id.to_owned());
    }

    pub(crate) async fn wait(&self) {
        match self
            .pending
            .values()
            .filter(|pending| !pending.waiting)
            .map(|pending| pending.last_input + INPUT_IDLE_TIMEOUT)
            .min()
        {
            Some(deadline) => tokio::time::sleep_until(deadline).await,
            None => std::future::pending().await,
        }
    }

    pub(crate) fn expire(&mut self, now: Instant) -> Vec<SessionEventKind> {
        let mut events = Vec::new();
        for (id, pending) in &mut self.pending {
            if !pending.waiting && now.duration_since(pending.last_input) >= INPUT_IDLE_TIMEOUT {
                pending.waiting = true;
                events.push(status(self.provider, id, pending));
            }
        }
        events
    }
}

fn status(
    provider: CodingProvider,
    id: &Option<String>,
    pending: &PendingGeneration,
) -> SessionEventKind {
    SessionEventKind::ProviderEvent {
        provider,
        kind: "action/generation_status".into(),
        payload: serde_json::json!({"tool_call_id": id, "label": pending.label, "waiting": pending.waiting}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn input(kind: &str, id: Option<&str>, label: &str) -> SessionEventKind {
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Claude,
            kind: kind.into(),
            payload: json!({"tool_call_id": id, "label": label}),
        }
    }

    #[test]
    fn silence_and_resumption_are_per_call_and_stop_at_execution() {
        let mut state = GenerationActivity::new(CodingProvider::Claude);
        let start = Instant::now();
        assert!(
            state
                .observe(input("action/preparing", Some("a"), "edit files"), start)
                .is_some()
        );
        state.observe(input("action/preparing", Some("b"), "read files"), start);
        assert!(state.expire(start + Duration::from_secs(4)).is_empty());
        assert!(
            state
                .observe(
                    input("action/input_delta", Some("b"), ""),
                    start + Duration::from_secs(4)
                )
                .is_none()
        );
        let waiting = state.expire(start + Duration::from_secs(5));
        assert!(
            matches!(waiting.as_slice(), [SessionEventKind::ProviderEvent { payload, .. }]
            if payload["tool_call_id"] == "a" && payload["waiting"] == true)
        );
        assert!(state.expire(start + Duration::from_secs(6)).is_empty());
        let resumed = state.observe(
            input("action/input_delta", Some("a"), ""),
            start + Duration::from_secs(6),
        );
        assert!(
            matches!(resumed, Some(SessionEventKind::ProviderEvent { payload, .. })
            if payload["waiting"] == false && payload["label"] == "edit files")
        );
        state.observe(
            SessionEventKind::ToolStarted {
                tool_call_id: "a".into(),
                name: "read_file".into(),
                input: json!({}),
                input_ref: None,
            },
            start + Duration::from_secs(7),
        );
        assert!(
            state
                .observe(
                    input("action/input_delta", Some("a"), ""),
                    start + Duration::from_secs(8)
                )
                .is_none()
        );
        assert!(
            state
                .observe(
                    input("action/preparing", Some("a"), "late"),
                    start + Duration::from_secs(8)
                )
                .is_none()
        );
        let waiting = state.expire(start + Duration::from_secs(20));
        assert!(
            matches!(waiting.as_slice(), [SessionEventKind::ProviderEvent { payload, .. }] if payload["tool_call_id"] == "b")
        );
    }

    #[test]
    fn late_identity_keeps_the_pending_action_and_unknown_pulses_do_not_start_one() {
        let mut state = GenerationActivity::new(CodingProvider::Codex);
        let start = Instant::now();
        assert!(
            state
                .observe(input("action/input_delta", Some("unknown"), ""), start)
                .is_none()
        );
        assert!(state.expire(start + INPUT_IDLE_TIMEOUT).is_empty());
        state.observe(input("action/preparing", None, ""), start);
        state.expire(start + INPUT_IDLE_TIMEOUT);
        let resumed = state.observe(
            input("action/input_delta", Some("known"), ""),
            start + INPUT_IDLE_TIMEOUT,
        );
        assert!(
            matches!(resumed, Some(SessionEventKind::ProviderEvent { payload, .. }) if payload["tool_call_id"] == "known" && payload["waiting"] == false)
        );
        state.observe(
            input("action/preparing", Some("known"), "edit files"),
            start + INPUT_IDLE_TIMEOUT,
        );
        assert_eq!(state.expire(start + INPUT_IDLE_TIMEOUT * 2).len(), 1);
    }
}
