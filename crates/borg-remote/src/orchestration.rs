//! Provider-neutral policy and durable event payloads for autonomous teams.
//!
//! This module decides *what* a team may do. Hosts turn approved decisions
//! into processes, sessions, and provider calls.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Stable identifier for a provider adapter, not a provider-specific enum.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProviderId(pub String);

/// Provider configuration selected for one team role. Values remain opaque to
/// the policy kernel so adapters may interpret them independently.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelProfile {
    pub provider: ProviderId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamRole {
    Director,
    Worker,
    Specialist,
}

/// One canonical workspace participant in a team topology.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamMember {
    pub participant_id: Uuid,
    pub role: TeamRole,
    pub profile: ModelProfile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamTopology {
    pub team_id: Uuid,
    pub workspace_id: Uuid,
    pub members: Vec<TeamMember>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleConcurrencyLimit {
    pub role: TeamRole,
    pub max_concurrent_assignments: u32,
}

/// Optional hard limits. `cost_microusd` avoids non-deterministic floating point costs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamBudget {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_cost_microusd: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wall_time_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamBudgetUsage {
    pub tokens: u64,
    pub cost_microusd: u64,
    pub wall_time_ms: u64,
}

impl TeamBudget {
    fn exhausted_by(&self, usage: &TeamBudgetUsage) -> bool {
        self.max_tokens.is_some_and(|limit| usage.tokens >= limit)
            || self
                .max_cost_microusd
                .is_some_and(|limit| usage.cost_microusd >= limit)
            || self
                .max_wall_time_ms
                .is_some_and(|limit| usage.wall_time_ms >= limit)
    }

    fn reaches_percent(&self, usage: &TeamBudgetUsage, percent: u8) -> bool {
        let reaches = |used: u64, limit: Option<u64>| {
            limit.is_some_and(|limit| {
                u128::from(used) * 100 >= u128::from(limit) * u128::from(percent)
            })
        };
        reaches(usage.tokens, self.max_tokens)
            || reaches(usage.cost_microusd, self.max_cost_microusd)
            || reaches(usage.wall_time_ms, self.max_wall_time_ms)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamLimits {
    pub max_concurrent_assignments: u32,
    pub per_role_concurrency: Vec<RoleConcurrencyLimit>,
    pub max_assignments_per_member: u32,
    pub max_total_assignments: u32,
    pub max_total_reports: u32,
    pub max_total_escalations: u32,
    pub budget: TeamBudget,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentPolicy {
    pub director_assigns_work: bool,
    pub workers_may_assign_specialists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecialistPolicy {
    pub director_may_spawn: bool,
    pub workers_may_spawn: bool,
    pub max_specialists: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportPolicy {
    pub required_on_completion: bool,
    pub require_director_review: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EscalationReason {
    Blocked,
    NeedsDecision,
    BudgetRisk,
    RepeatedFailure,
    SafetyConcern,
    ScopeChange,
    Uncertainty,
    AmbiguousCriteria,
    MissingAuthority,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EscalationThresholds {
    pub repeated_failures: u32,
    /// Percentage of any configured budget limit at which `budget_risk` is allowed.
    pub budget_percent: u8,
    pub allowed_reasons: Vec<EscalationReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewCondition {
    OnWorkerReport,
    OnEscalation,
    BeforeStop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopCondition {
    ExplicitRequest,
    WorkComplete,
    BudgetExhausted,
    FailureThreshold,
    EscalationLimit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamPolicy {
    pub topology: TeamTopology,
    pub limits: TeamLimits,
    pub assignment: AssignmentPolicy,
    pub specialists: SpecialistPolicy,
    pub reporting: ReportPolicy,
    pub escalation: EscalationThresholds,
    pub review_conditions: Vec<ReviewCondition>,
    pub stop_conditions: Vec<StopCondition>,
}

/// Provider-neutral convenience topology. Effort labels are opaque strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamPreset {
    XhighDirectorLowWorkers,
}

impl TeamPreset {
    pub fn policy(
        self,
        team_id: Uuid,
        workspace_id: Uuid,
        director_id: Uuid,
        worker_ids: impl IntoIterator<Item = Uuid>,
        provider: ProviderId,
    ) -> TeamPolicy {
        let worker_ids: Vec<_> = worker_ids.into_iter().collect();
        let worker_count = u32::try_from(worker_ids.len()).unwrap_or(u32::MAX);
        let director = TeamMember {
            participant_id: director_id,
            role: TeamRole::Director,
            profile: ModelProfile {
                provider: provider.clone(),
                model: None,
                reasoning_effort: Some("xhigh".to_owned()),
            },
        };
        let workers = worker_ids.into_iter().map(|participant_id| TeamMember {
            participant_id,
            role: TeamRole::Worker,
            profile: ModelProfile {
                provider: provider.clone(),
                model: None,
                reasoning_effort: Some("low".to_owned()),
            },
        });
        TeamPolicy {
            topology: TeamTopology {
                team_id,
                workspace_id,
                members: std::iter::once(director).chain(workers).collect(),
            },
            limits: TeamLimits {
                max_concurrent_assignments: worker_count,
                per_role_concurrency: vec![RoleConcurrencyLimit {
                    role: TeamRole::Worker,
                    max_concurrent_assignments: worker_count,
                }],
                max_assignments_per_member: 1,
                max_total_assignments: worker_count,
                max_total_reports: worker_count,
                max_total_escalations: worker_count,
                budget: TeamBudget::default(),
            },
            assignment: AssignmentPolicy {
                director_assigns_work: true,
                workers_may_assign_specialists: false,
            },
            specialists: SpecialistPolicy {
                director_may_spawn: true,
                workers_may_spawn: false,
                max_specialists: worker_count,
            },
            reporting: ReportPolicy {
                required_on_completion: true,
                require_director_review: true,
            },
            escalation: EscalationThresholds {
                repeated_failures: 2,
                budget_percent: 90,
                allowed_reasons: vec![
                    EscalationReason::Blocked,
                    EscalationReason::NeedsDecision,
                    EscalationReason::BudgetRisk,
                    EscalationReason::RepeatedFailure,
                    EscalationReason::SafetyConcern,
                    EscalationReason::ScopeChange,
                    EscalationReason::Uncertainty,
                    EscalationReason::AmbiguousCriteria,
                    EscalationReason::MissingAuthority,
                ],
            },
            review_conditions: vec![
                ReviewCondition::OnWorkerReport,
                ReviewCondition::OnEscalation,
                ReviewCondition::BeforeStop,
            ],
            stop_conditions: vec![
                StopCondition::ExplicitRequest,
                StopCondition::WorkComplete,
                StopCondition::BudgetExhausted,
                StopCondition::FailureThreshold,
                StopCondition::EscalationLimit,
            ],
        }
    }
}

/// A durable cursor into the canonical workspace/session history for handoff.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextCursor {
    pub workspace_id: Uuid,
    pub after_sequence: u64,
    pub through_sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_event_id: Option<Uuid>,
}

/// Immutable, provider-neutral evidence reference retained with a handoff.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceRef {
    pub evidence_id: Uuid,
    pub kind: String,
    pub reference: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Handoff {
    pub context: ContextCursor,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<EvidenceRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<String>,
}

/// Durable payloads emitted after an orchestrator applies a policy decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TeamEvent {
    Assignment {
        assignment_id: Uuid,
        work_id: Uuid,
        assigned_by: Uuid,
        assigned_to: Uuid,
        handoff: Handoff,
    },
    SpecialistSpawnRequested {
        work_id: Uuid,
        requested_by: Uuid,
        specialist_participant_id: Uuid,
        profile: ModelProfile,
        handoff: Handoff,
    },
    Report {
        assignment_id: Uuid,
        work_id: Uuid,
        reported_by: Uuid,
        summary: String,
        completed: bool,
        handoff: Handoff,
    },
    Escalation {
        assignment_id: Option<Uuid>,
        work_id: Option<Uuid>,
        raised_by: Uuid,
        reason: EscalationReason,
        detail: String,
        handoff: Handoff,
    },
    ReviewRequested {
        assignment_id: Option<Uuid>,
        work_id: Option<Uuid>,
        requested_by: Uuid,
        reason: String,
        handoff: Handoff,
    },
    StopRequested {
        requested_by: Uuid,
        condition: StopCondition,
        detail: Option<String>,
        handoff: Handoff,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TeamPolicyState {
    pub active_assignments: u32,
    pub active_assignments_by_role: BTreeMap<TeamRole, u32>,
    pub total_assignments: u32,
    pub total_reports: u32,
    pub total_escalations: u32,
    pub member_assignments: u32,
    pub active_specialists: u32,
    pub repeated_failures: u32,
    pub budget_usage: TeamBudgetUsage,
    pub work_complete: bool,
    pub stop_requested: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyRequest {
    Assign {
        assigned_by: Uuid,
        assigned_to: Uuid,
    },
    SpawnSpecialist {
        requested_by: Uuid,
        specialist_participant_id: Uuid,
    },
    Report {
        reported_by: Uuid,
    },
    Escalate {
        raised_by: Uuid,
        reason: EscalationReason,
    },
    Stop {
        condition: StopCondition,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyDecision {
    Allow,
    AllowSpecialistSpawn,
    RequestReview(ReviewCondition),
    EscalateToDirector(EscalationReason),
    Stop(StopCondition),
    Deny(&'static str),
}

impl TeamPolicy {
    /// Deterministic and side-effect-free policy evaluation.
    pub fn decide(&self, state: &TeamPolicyState, request: PolicyRequest) -> PolicyDecision {
        if state.stop_requested {
            return PolicyDecision::Deny("team is already stopping");
        }
        if state.work_complete && self.stop_conditions.contains(&StopCondition::WorkComplete) {
            return PolicyDecision::Stop(StopCondition::WorkComplete);
        }
        if self.limits.budget.exhausted_by(&state.budget_usage)
            && self
                .stop_conditions
                .contains(&StopCondition::BudgetExhausted)
        {
            return PolicyDecision::Stop(StopCondition::BudgetExhausted);
        }
        if state.repeated_failures >= self.escalation.repeated_failures
            && self
                .stop_conditions
                .contains(&StopCondition::FailureThreshold)
        {
            return PolicyDecision::Stop(StopCondition::FailureThreshold);
        }
        if state.total_escalations >= self.limits.max_total_escalations
            && self
                .stop_conditions
                .contains(&StopCondition::EscalationLimit)
        {
            return PolicyDecision::Stop(StopCondition::EscalationLimit);
        }
        match request {
            PolicyRequest::Assign {
                assigned_by,
                assigned_to,
            } => {
                let Some(assigner) = self.member(assigned_by) else {
                    return PolicyDecision::Deny("unknown assigner");
                };
                let Some(assignee) = self.member(assigned_to) else {
                    return PolicyDecision::Deny("unknown assignee");
                };
                let permitted = matches!(assigner.role, TeamRole::Director)
                    && self.assignment.director_assigns_work
                    || matches!(assigner.role, TeamRole::Worker)
                        && matches!(assignee.role, TeamRole::Specialist)
                        && self.assignment.workers_may_assign_specialists;
                if !permitted {
                    PolicyDecision::Deny("role may not assign this member")
                } else {
                    self.assignment_capacity(state, assignee.role)
                }
            }
            PolicyRequest::SpawnSpecialist {
                requested_by,
                specialist_participant_id,
            } => {
                let Some(requester) = self.member(requested_by) else {
                    return PolicyDecision::Deny("unknown specialist requester");
                };
                let Some(specialist) = self.member(specialist_participant_id) else {
                    return PolicyDecision::Deny("unknown specialist");
                };
                if specialist.role != TeamRole::Specialist {
                    PolicyDecision::Deny("target is not a specialist")
                } else if state.active_specialists >= self.specialists.max_specialists {
                    PolicyDecision::Deny("specialist limit reached")
                } else if matches!(requester.role, TeamRole::Director)
                    && self.specialists.director_may_spawn
                {
                    PolicyDecision::AllowSpecialistSpawn
                } else if matches!(requester.role, TeamRole::Worker)
                    && self.specialists.workers_may_spawn
                {
                    PolicyDecision::AllowSpecialistSpawn
                } else {
                    PolicyDecision::EscalateToDirector(EscalationReason::MissingAuthority)
                }
            }
            PolicyRequest::Report { reported_by } => {
                let Some(member) = self.member(reported_by) else {
                    return PolicyDecision::Deny("unknown reporter");
                };
                if state.total_reports >= self.limits.max_total_reports {
                    PolicyDecision::Deny("report limit reached")
                } else if matches!(member.role, TeamRole::Worker | TeamRole::Specialist)
                    && self.reporting.require_director_review
                    && self
                        .review_conditions
                        .contains(&ReviewCondition::OnWorkerReport)
                {
                    PolicyDecision::RequestReview(ReviewCondition::OnWorkerReport)
                } else {
                    PolicyDecision::Allow
                }
            }
            PolicyRequest::Escalate { raised_by, reason } => {
                if self.member(raised_by).is_none() {
                    return PolicyDecision::Deny("unknown escalation requester");
                }
                if !self.escalation.allowed_reasons.contains(&reason) {
                    PolicyDecision::Deny("escalation reason is not allowed")
                } else if reason == EscalationReason::RepeatedFailure
                    && state.repeated_failures < self.escalation.repeated_failures
                {
                    PolicyDecision::Deny("failure threshold not reached")
                } else if reason == EscalationReason::BudgetRisk
                    && !self
                        .limits
                        .budget
                        .reaches_percent(&state.budget_usage, self.escalation.budget_percent)
                {
                    PolicyDecision::Deny("budget threshold not reached")
                } else if self
                    .review_conditions
                    .contains(&ReviewCondition::OnEscalation)
                {
                    PolicyDecision::RequestReview(ReviewCondition::OnEscalation)
                } else {
                    PolicyDecision::Allow
                }
            }
            PolicyRequest::Stop { condition } => {
                if !self.stop_conditions.contains(&condition) {
                    PolicyDecision::Deny("stop condition is not enabled")
                } else if self
                    .review_conditions
                    .contains(&ReviewCondition::BeforeStop)
                {
                    PolicyDecision::RequestReview(ReviewCondition::BeforeStop)
                } else {
                    PolicyDecision::Stop(condition)
                }
            }
        }
    }

    fn member(&self, participant_id: Uuid) -> Option<&TeamMember> {
        self.topology
            .members
            .iter()
            .find(|member| member.participant_id == participant_id)
    }

    fn assignment_capacity(&self, state: &TeamPolicyState, role: TeamRole) -> PolicyDecision {
        if state.active_assignments >= self.limits.max_concurrent_assignments {
            PolicyDecision::Deny("concurrency limit reached")
        } else if self.limits.per_role_concurrency.iter().any(|limit| {
            limit.role == role
                && state
                    .active_assignments_by_role
                    .get(&role)
                    .copied()
                    .unwrap_or_default()
                    >= limit.max_concurrent_assignments
        }) {
            PolicyDecision::Deny("role concurrency limit reached")
        } else if state.total_assignments >= self.limits.max_total_assignments {
            PolicyDecision::Deny("assignment budget reached")
        } else if state.member_assignments >= self.limits.max_assignments_per_member {
            PolicyDecision::Deny("member assignment limit reached")
        } else {
            PolicyDecision::Allow
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (TeamPolicy, Uuid, Uuid) {
        let director = Uuid::new_v4();
        let worker = Uuid::new_v4();
        (
            TeamPreset::XhighDirectorLowWorkers.policy(
                Uuid::new_v4(),
                Uuid::new_v4(),
                director,
                [worker],
                ProviderId("example".into()),
            ),
            director,
            worker,
        )
    }

    #[test]
    fn preset_uses_uuid_participants_and_effort_by_role() {
        let (policy, director, worker) = fixture();
        assert_eq!(policy.topology.members[0].participant_id, director);
        assert_eq!(policy.topology.members[1].participant_id, worker);
        assert_eq!(
            policy.topology.members[0]
                .profile
                .reasoning_effort
                .as_deref(),
            Some("xhigh")
        );
        assert_eq!(
            policy.topology.members[1]
                .profile
                .reasoning_effort
                .as_deref(),
            Some("low")
        );
    }

    #[test]
    fn decisions_enforce_per_role_concurrency_deterministically() {
        let (policy, director, worker) = fixture();
        let state = TeamPolicyState {
            active_assignments_by_role: BTreeMap::from([(TeamRole::Worker, 1)]),
            ..Default::default()
        };
        let request = PolicyRequest::Assign {
            assigned_by: director,
            assigned_to: worker,
        };
        assert_eq!(
            policy.decide(&state, request.clone()),
            PolicyDecision::Deny("role concurrency limit reached")
        );
        assert_eq!(
            policy.decide(&state, request),
            PolicyDecision::Deny("role concurrency limit reached")
        );
    }

    #[test]
    fn budgets_stop_and_budget_risk_uses_configured_limits() {
        let (mut policy, director, _) = fixture();
        policy.limits.budget.max_tokens = Some(100);
        policy
            .review_conditions
            .retain(|condition| *condition != ReviewCondition::OnEscalation);
        let risk = TeamPolicyState {
            budget_usage: TeamBudgetUsage {
                tokens: 90,
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(
            policy.decide(
                &risk,
                PolicyRequest::Escalate {
                    raised_by: director,
                    reason: EscalationReason::BudgetRisk
                }
            ),
            PolicyDecision::Allow
        );
        let exhausted = TeamPolicyState {
            budget_usage: TeamBudgetUsage {
                tokens: 100,
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(
            policy.decide(
                &exhausted,
                PolicyRequest::Report {
                    reported_by: director
                }
            ),
            PolicyDecision::Stop(StopCondition::BudgetExhausted)
        );
    }

    #[test]
    fn worker_specialist_request_escalates_without_authority() {
        let (mut policy, _, worker) = fixture();
        let specialist = Uuid::new_v4();
        policy.topology.members.push(TeamMember {
            participant_id: specialist,
            role: TeamRole::Specialist,
            profile: ModelProfile {
                provider: ProviderId("example".into()),
                model: None,
                reasoning_effort: None,
            },
        });
        assert_eq!(
            policy.decide(
                &TeamPolicyState::default(),
                PolicyRequest::SpawnSpecialist {
                    requested_by: worker,
                    specialist_participant_id: specialist
                }
            ),
            PolicyDecision::EscalateToDirector(EscalationReason::MissingAuthority)
        );
    }

    #[test]
    fn durable_events_round_trip_with_handoff_references() {
        let handoff = Handoff {
            context: ContextCursor {
                workspace_id: Uuid::new_v4(),
                after_sequence: 3,
                through_sequence: 5,
                session_id: Some(Uuid::new_v4()),
                session_event_id: Some(Uuid::new_v4()),
            },
            evidence: vec![EvidenceRef {
                evidence_id: Uuid::new_v4(),
                kind: "test_result".into(),
                reference: "artifact://result/1".into(),
                summary: Some("passed".into()),
            }],
            continuation: Some("review evidence".into()),
        };
        let event = TeamEvent::Escalation {
            assignment_id: Some(Uuid::new_v4()),
            work_id: Some(Uuid::new_v4()),
            raised_by: Uuid::new_v4(),
            reason: EscalationReason::AmbiguousCriteria,
            detail: "acceptance criteria conflict".into(),
            handoff,
        };
        assert_eq!(
            serde_json::from_str::<TeamEvent>(&serde_json::to_string(&event).unwrap()).unwrap(),
            event
        );
    }
}
