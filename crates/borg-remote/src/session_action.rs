use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;
use uuid::Uuid;

/// Durable work categories accepted by a session actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionActionKind {
    Prompt,
    Steering,
    FollowUp,
    Command,
    Compaction,
    Revert,
    ProviderChange,
    AgentMessage,
    Workflow,
}

/// The persisted lifecycle is deliberately stricter than the in-memory
/// provider loop. A row cannot jump from queued to completed or be completed
/// twice after a crash/retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionActionState {
    Queued,
    Admitted,
    Delivered,
    Preparing,
    Committing,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl SessionActionState {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    pub const fn can_transition(self, next: Self) -> bool {
        use SessionActionState::*;
        matches!(
            (self, next),
            (Queued, Admitted | Cancelled)
                | (Admitted, Delivered | Preparing | Failed | Cancelled)
                | (Delivered, Preparing | Running | Failed | Cancelled)
                | (Preparing, Committing | Failed | Cancelled)
                | (Committing, Queued | Running | Failed | Cancelled)
                | (Running, Queued | Completed | Failed | Cancelled)
                | (Completed, Queued)
                | (Failed, Queued | Cancelled)
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionDeliveryPolicy {
    NextTurnBoundary,
    WhenRunIdle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionWakePolicy {
    Immediate,
    OnLowerBoundary,
    ExternalResume,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionAction {
    pub action_id: Uuid,
    pub session_id: Uuid,
    pub kind: SessionActionKind,
    pub state: SessionActionState,
    pub delivery: ActionDeliveryPolicy,
    pub wake: ActionWakePolicy,
    pub payload: Value,
    pub attempt: u32,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub accepted_at: Option<DateTime<Utc>>,
    pub delivered_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub lease_owner: Option<String>,
    pub lease_token: Option<Uuid>,
    pub lease_heartbeat_at: Option<DateTime<Utc>>,
    pub lease_expires_at: Option<DateTime<Utc>>,
}

/// Immutable audit record for one state transition. The current action row is
/// a fast recovery projection; this sequence is the proof that an action did
/// not skip or repeat lifecycle boundaries across retries or crashes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionActionTransition {
    pub action_id: Uuid,
    pub session_id: Uuid,
    pub transition_no: u64,
    pub from: Option<SessionActionState>,
    pub to: SessionActionState,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl SessionAction {
    pub fn new(
        action_id: Uuid,
        session_id: Uuid,
        kind: SessionActionKind,
        delivery: ActionDeliveryPolicy,
        wake: ActionWakePolicy,
        payload: Value,
    ) -> Self {
        let now = Utc::now();
        Self {
            action_id,
            session_id,
            kind,
            state: SessionActionState::Queued,
            delivery,
            wake,
            payload,
            attempt: 0,
            error: None,
            created_at: now,
            updated_at: now,
            accepted_at: None,
            delivered_at: None,
            completed_at: None,
            lease_owner: None,
            lease_token: None,
            lease_heartbeat_at: None,
            lease_expires_at: None,
        }
    }

    pub fn claim(
        &mut self,
        owner: impl Into<String>,
        token: Uuid,
        now: DateTime<Utc>,
        duration: Duration,
    ) -> anyhow::Result<()> {
        let owner = owner.into();
        anyhow::ensure!(!owner.trim().is_empty(), "action lease owner is empty");
        anyhow::ensure!(!self.state.is_terminal(), "cannot claim terminal action");
        anyhow::ensure!(!duration.is_zero(), "action lease duration is zero");
        self.lease_owner = Some(owner);
        self.lease_token = Some(token);
        self.lease_heartbeat_at = Some(now);
        self.lease_expires_at = Some(now + chrono::Duration::from_std(duration)?);
        self.updated_at = now;
        Ok(())
    }

    pub fn heartbeat(
        &mut self,
        owner: &str,
        token: Uuid,
        now: DateTime<Utc>,
        duration: Duration,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(!duration.is_zero(), "action lease duration is zero");
        anyhow::ensure!(
            self.lease_owner.as_deref() == Some(owner) && self.lease_token == Some(token),
            "action {} lease is not owned by {owner}",
            self.action_id
        );
        anyhow::ensure!(
            self.lease_expires_at.is_some_and(|expires| expires > now),
            "action {} lease has expired",
            self.action_id
        );
        self.lease_heartbeat_at = Some(now);
        self.lease_expires_at = Some(now + chrono::Duration::from_std(duration)?);
        self.updated_at = now;
        Ok(())
    }

    pub fn clear_lease(&mut self) {
        self.lease_owner = None;
        self.lease_token = None;
        self.lease_heartbeat_at = None;
        self.lease_expires_at = None;
    }

    pub fn lease_expired_at(&self, now: DateTime<Utc>) -> bool {
        self.lease_expires_at.is_none_or(|expires| expires <= now)
    }

    pub fn transition(
        &mut self,
        expected: Option<SessionActionState>,
        next: SessionActionState,
        error: Option<String>,
    ) -> anyhow::Result<()> {
        if let Some(expected) = expected {
            anyhow::ensure!(
                self.state == expected,
                "action {} is {}, expected {}",
                self.action_id,
                serde_json::to_string(&self.state)?,
                serde_json::to_string(&expected)?
            );
        }
        anyhow::ensure!(
            self.state.can_transition(next),
            "illegal action transition {:?} -> {:?}",
            self.state,
            next
        );
        let now = Utc::now();
        self.state = next;
        self.updated_at = now;
        self.error = error;
        if next == SessionActionState::Admitted {
            self.accepted_at = Some(now);
            self.attempt = self.attempt.saturating_add(1);
        }
        if next == SessionActionState::Delivered {
            self.delivered_at = Some(now);
        }
        if next.is_terminal() {
            self.completed_at = Some(now);
        }
        if next == SessionActionState::Queued {
            // A retry is a new execution attempt; retain the transition
            // history but do not expose the previous terminal timestamp as
            // if the requeued action were still complete.
            self.completed_at = None;
            self.clear_lease();
        } else if next.is_terminal() {
            self.clear_lease();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_rejects_skipped_states_and_records_timestamps() {
        let mut action = SessionAction::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            SessionActionKind::Prompt,
            ActionDeliveryPolicy::NextTurnBoundary,
            ActionWakePolicy::OnLowerBoundary,
            Value::Null,
        );
        assert!(
            action
                .transition(None, SessionActionState::Running, None)
                .is_err()
        );
        action
            .transition(
                Some(SessionActionState::Queued),
                SessionActionState::Admitted,
                None,
            )
            .unwrap();
        assert_eq!(action.attempt, 1);
        action
            .transition(
                Some(SessionActionState::Admitted),
                SessionActionState::Delivered,
                None,
            )
            .unwrap();
        action
            .transition(
                Some(SessionActionState::Delivered),
                SessionActionState::Preparing,
                None,
            )
            .unwrap();
        action
            .transition(
                Some(SessionActionState::Preparing),
                SessionActionState::Committing,
                None,
            )
            .unwrap();
        action
            .transition(
                Some(SessionActionState::Committing),
                SessionActionState::Running,
                None,
            )
            .unwrap();
        action
            .transition(
                Some(SessionActionState::Running),
                SessionActionState::Completed,
                None,
            )
            .unwrap();
        assert!(action.completed_at.is_some());
        assert!(
            action
                .transition(None, SessionActionState::Cancelled, None)
                .is_err()
        );
    }

    #[test]
    fn terminal_actions_can_be_explicitly_requeued_but_not_completed_directly() {
        assert!(SessionActionState::Failed.can_transition(SessionActionState::Queued));
        assert!(SessionActionState::Completed.can_transition(SessionActionState::Queued));
        assert!(!SessionActionState::Failed.can_transition(SessionActionState::Completed));
        assert!(!SessionActionState::Cancelled.can_transition(SessionActionState::Queued));
    }

    #[test]
    fn lease_claim_and_heartbeat_are_fenced_and_expiry_is_observable() {
        let mut action = SessionAction::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            SessionActionKind::Prompt,
            ActionDeliveryPolicy::NextTurnBoundary,
            ActionWakePolicy::Immediate,
            Value::Null,
        );
        let now = Utc::now();
        let token = Uuid::new_v4();
        action
            .claim("worker-a", token, now, Duration::from_secs(30))
            .unwrap();
        assert_eq!(action.lease_owner.as_deref(), Some("worker-a"));
        assert_eq!(action.lease_token, Some(token));
        assert!(!action.lease_expired_at(now));
        assert!(
            action
                .heartbeat("worker-b", token, now, Duration::from_secs(30))
                .is_err()
        );
        assert!(
            action
                .heartbeat("worker-a", Uuid::new_v4(), now, Duration::from_secs(30))
                .is_err()
        );
        let expired = now + chrono::Duration::seconds(31);
        assert!(action.lease_expired_at(expired));
        assert!(
            action
                .heartbeat("worker-a", token, expired, Duration::from_secs(30))
                .is_err()
        );
    }

    #[test]
    fn requeue_transition_clears_lease_and_allows_recovery() {
        let mut action = SessionAction::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            SessionActionKind::Prompt,
            ActionDeliveryPolicy::NextTurnBoundary,
            ActionWakePolicy::Immediate,
            Value::Null,
        );
        action
            .transition(None, SessionActionState::Admitted, None)
            .unwrap();
        action
            .transition(None, SessionActionState::Delivered, None)
            .unwrap();
        action
            .transition(None, SessionActionState::Preparing, None)
            .unwrap();
        action
            .transition(None, SessionActionState::Committing, None)
            .unwrap();
        action
            .claim(
                "worker-a",
                Uuid::new_v4(),
                Utc::now(),
                Duration::from_secs(1),
            )
            .unwrap();
        action
            .transition(
                Some(SessionActionState::Committing),
                SessionActionState::Queued,
                Some("lease expired".to_string()),
            )
            .unwrap();
        assert_eq!(action.state, SessionActionState::Queued);
        assert!(action.lease_owner.is_none());
        assert!(action.lease_token.is_none());
        assert!(SessionActionState::Queued.can_transition(SessionActionState::Admitted));
    }
}
