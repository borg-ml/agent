//! Provider-neutral policy and durable event payloads for autonomous teams.
//!
//! This module decides *what* a team may do. Hosts turn approved decisions
//! into processes, sessions, and provider calls.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, ensure};
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamPolicyState {
    pub active_assignments: u32,
    pub active_assignments_by_role: BTreeMap<TeamRole, u32>,
    pub total_assignments: u32,
    pub total_reports: u32,
    pub total_escalations: u32,
    pub member_assignments: u32,
    #[serde(default)]
    pub active_assignments_by_member: BTreeMap<Uuid, u32>,
    pub active_specialists: u32,
    pub repeated_failures: u32,
    pub budget_usage: TeamBudgetUsage,
    pub work_complete: bool,
    pub stop_requested: bool,
}

/// Serializable policy journal and projection. It is deliberately detached
/// from provider processes: a host may consume the approved event later.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamPolicyRuntime {
    pub policy: TeamPolicy,
    #[serde(default)]
    pub state: TeamPolicyState,
    #[serde(default)]
    pub events: Vec<TeamEvent>,
    #[serde(default)]
    active_assignments: BTreeMap<Uuid, (Uuid, TeamRole)>,
    #[serde(default)]
    active_specialists: BTreeSet<Uuid>,
}

/// Provider-neutral tool input. Applying one records the approved durable
/// events; process spawning is intentionally outside this module.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TeamPolicyToolRequest {
    Assign {
        assignment_id: Uuid,
        work_id: Uuid,
        assigned_by: Uuid,
        assigned_to: Uuid,
        handoff: Handoff,
    },
    RequestSpecialist {
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
    Escalate {
        assignment_id: Option<Uuid>,
        work_id: Option<Uuid>,
        raised_by: Uuid,
        reason: EscalationReason,
        detail: String,
        handoff: Handoff,
    },
    Stop {
        requested_by: Uuid,
        condition: StopCondition,
        detail: Option<String>,
        handoff: Handoff,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamPolicyToolOutcome {
    pub decision: PolicyDecision,
    pub events: Vec<TeamEvent>,
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
                    self.assignment_capacity(state, assignee.participant_id, assignee.role)
                }
            }
            PolicyRequest::SpawnSpecialist {
                requested_by,
                specialist_participant_id,
            } => {
                let Some(requester) = self.member(requested_by) else {
                    return PolicyDecision::Deny("unknown specialist requester");
                };
                if self.member(specialist_participant_id).is_some() {
                    PolicyDecision::Deny("specialist participant is already in the team")
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

    fn assignment_capacity(
        &self,
        state: &TeamPolicyState,
        participant_id: Uuid,
        role: TeamRole,
    ) -> PolicyDecision {
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
        } else if state
            .active_assignments_by_member
            .get(&participant_id)
            .copied()
            .unwrap_or(state.member_assignments)
            >= self.limits.max_assignments_per_member
        {
            PolicyDecision::Deny("member assignment limit reached")
        } else {
            PolicyDecision::Allow
        }
    }
}

impl TeamPolicyRuntime {
    pub fn new(policy: TeamPolicy) -> Self {
        Self {
            policy,
            state: TeamPolicyState::default(),
            events: Vec::new(),
            active_assignments: BTreeMap::new(),
            active_specialists: BTreeSet::new(),
        }
    }

    /// Rebuilds a projection from its durable event stream, validating every
    /// transition under the supplied policy.
    pub fn replay(policy: TeamPolicy, events: impl IntoIterator<Item = TeamEvent>) -> Result<Self> {
        let mut runtime = Self::new(policy);
        for event in events {
            runtime.apply_event(event)?;
        }
        Ok(runtime)
    }

    /// Evaluates a tool request and, if permitted, records its durable event.
    /// The returned event is the only hand-off a provider host needs.
    pub fn apply_tool_request(
        &mut self,
        request: TeamPolicyToolRequest,
    ) -> Result<TeamPolicyToolOutcome> {
        let (decision, event) = match &request {
            TeamPolicyToolRequest::Assign {
                assigned_by,
                assigned_to,
                assignment_id,
                work_id,
                handoff,
            } => (
                self.policy.decide(
                    &self.state,
                    PolicyRequest::Assign {
                        assigned_by: *assigned_by,
                        assigned_to: *assigned_to,
                    },
                ),
                TeamEvent::Assignment {
                    assignment_id: *assignment_id,
                    work_id: *work_id,
                    assigned_by: *assigned_by,
                    assigned_to: *assigned_to,
                    handoff: handoff.clone(),
                },
            ),
            TeamPolicyToolRequest::RequestSpecialist {
                requested_by,
                specialist_participant_id,
                work_id,
                profile,
                handoff,
            } => (
                self.policy.decide(
                    &self.state,
                    PolicyRequest::SpawnSpecialist {
                        requested_by: *requested_by,
                        specialist_participant_id: *specialist_participant_id,
                    },
                ),
                TeamEvent::SpecialistSpawnRequested {
                    work_id: *work_id,
                    requested_by: *requested_by,
                    specialist_participant_id: *specialist_participant_id,
                    profile: profile.clone(),
                    handoff: handoff.clone(),
                },
            ),
            TeamPolicyToolRequest::Report {
                reported_by,
                assignment_id,
                work_id,
                summary,
                completed,
                handoff,
            } => (
                self.policy.decide(
                    &self.state,
                    PolicyRequest::Report {
                        reported_by: *reported_by,
                    },
                ),
                TeamEvent::Report {
                    assignment_id: *assignment_id,
                    work_id: *work_id,
                    reported_by: *reported_by,
                    summary: summary.clone(),
                    completed: *completed,
                    handoff: handoff.clone(),
                },
            ),
            TeamPolicyToolRequest::Escalate {
                raised_by,
                reason,
                assignment_id,
                work_id,
                detail,
                handoff,
            } => (
                self.policy.decide(
                    &self.state,
                    PolicyRequest::Escalate {
                        raised_by: *raised_by,
                        reason: *reason,
                    },
                ),
                TeamEvent::Escalation {
                    assignment_id: *assignment_id,
                    work_id: *work_id,
                    raised_by: *raised_by,
                    reason: *reason,
                    detail: detail.clone(),
                    handoff: handoff.clone(),
                },
            ),
            TeamPolicyToolRequest::Stop {
                condition,
                requested_by,
                detail,
                handoff,
            } => (
                self.policy.decide(
                    &self.state,
                    PolicyRequest::Stop {
                        condition: *condition,
                    },
                ),
                TeamEvent::StopRequested {
                    requested_by: *requested_by,
                    condition: *condition,
                    detail: detail.clone(),
                    handoff: handoff.clone(),
                },
            ),
        };
        let review = |condition| TeamEvent::ReviewRequested {
            assignment_id: match &request {
                TeamPolicyToolRequest::Report { assignment_id, .. } => Some(*assignment_id),
                TeamPolicyToolRequest::Escalate { assignment_id, .. } => *assignment_id,
                _ => None,
            },
            work_id: match &request {
                TeamPolicyToolRequest::Report { work_id, .. } => Some(*work_id),
                TeamPolicyToolRequest::Escalate { work_id, .. } => *work_id,
                _ => None,
            },
            requested_by: request_actor(&request),
            reason: format!("policy review: {condition:?}"),
            handoff: request_handoff(&request).clone(),
        };
        let events = match decision {
            PolicyDecision::Allow | PolicyDecision::AllowSpecialistSpawn => vec![event],
            PolicyDecision::Stop(condition) => vec![TeamEvent::StopRequested {
                requested_by: request_actor(&request),
                condition,
                detail: Some("policy stop condition reached".to_string()),
                handoff: request_handoff(&request).clone(),
            }],
            // Reports and escalations remain part of the durable history while
            // the separate review event makes their required approval explicit.
            PolicyDecision::RequestReview(condition) => match event {
                TeamEvent::Report { .. } | TeamEvent::Escalation { .. } => {
                    vec![event, review(condition)]
                }
                _ => vec![review(condition)],
            },
            PolicyDecision::EscalateToDirector(reason) => vec![TeamEvent::Escalation {
                assignment_id: None,
                work_id: None,
                raised_by: request_actor(&request),
                reason,
                detail: "policy requires director authority".to_string(),
                handoff: request_handoff(&request).clone(),
            }],
            PolicyDecision::Deny(_) => Vec::new(),
        };
        for event in events.clone() {
            self.apply_event(event)?;
        }
        Ok(TeamPolicyToolOutcome { decision, events })
    }

    /// Replays a durable policy event. Validation prevents a corrupt or
    /// unauthorized event log from silently changing the projection.
    pub fn apply_event(&mut self, event: TeamEvent) -> Result<()> {
        ensure!(
            event_handoff(&event).context.workspace_id == self.policy.topology.workspace_id,
            "event handoff belongs to a different workspace"
        );
        match &event {
            TeamEvent::Assignment {
                assignment_id,
                assigned_by,
                assigned_to,
                ..
            } => {
                ensure!(
                    !self.active_assignments.contains_key(assignment_id),
                    "assignment already active"
                );
                ensure!(
                    matches!(
                        self.policy.decide(
                            &self.state,
                            PolicyRequest::Assign {
                                assigned_by: *assigned_by,
                                assigned_to: *assigned_to
                            }
                        ),
                        PolicyDecision::Allow
                    ),
                    "assignment violates policy"
                );
                let role = self
                    .policy
                    .member(*assigned_to)
                    .expect("validated assignee")
                    .role;
                self.active_assignments
                    .insert(*assignment_id, (*assigned_to, role));
                self.state.active_assignments += 1;
                *self
                    .state
                    .active_assignments_by_role
                    .entry(role)
                    .or_default() += 1;
                *self
                    .state
                    .active_assignments_by_member
                    .entry(*assigned_to)
                    .or_default() += 1;
                self.state.total_assignments += 1;
            }
            TeamEvent::SpecialistSpawnRequested {
                requested_by,
                specialist_participant_id,
                ..
            } => {
                ensure!(
                    matches!(
                        self.policy.decide(
                            &self.state,
                            PolicyRequest::SpawnSpecialist {
                                requested_by: *requested_by,
                                specialist_participant_id: *specialist_participant_id
                            }
                        ),
                        PolicyDecision::AllowSpecialistSpawn
                    ),
                    "specialist request violates policy"
                );
                ensure!(
                    self.active_specialists.insert(*specialist_participant_id),
                    "specialist already active"
                );
                self.state.active_specialists += 1;
            }
            TeamEvent::Report {
                assignment_id,
                reported_by,
                completed,
                ..
            } => {
                ensure!(
                    self.policy.member(*reported_by).is_some(),
                    "unknown reporter"
                );
                ensure!(
                    self.state.total_reports < self.policy.limits.max_total_reports,
                    "report limit reached"
                );
                self.state.total_reports += 1;
                if *completed {
                    let Some((assignee, role)) = self.active_assignments.remove(assignment_id)
                    else {
                        anyhow::bail!("completed report has no active assignment");
                    };
                    ensure!(assignee == *reported_by, "reporter does not own assignment");
                    self.state.active_assignments -= 1;
                    *self
                        .state
                        .active_assignments_by_role
                        .entry(role)
                        .or_default() -= 1;
                    *self
                        .state
                        .active_assignments_by_member
                        .entry(assignee)
                        .or_default() -= 1;
                }
            }
            TeamEvent::Escalation {
                raised_by, reason, ..
            } => {
                ensure!(
                    self.policy.member(*raised_by).is_some(),
                    "unknown escalation requester"
                );
                ensure!(
                    self.policy.escalation.allowed_reasons.contains(reason),
                    "escalation reason is not allowed"
                );
                ensure!(
                    self.state.total_escalations < self.policy.limits.max_total_escalations,
                    "escalation limit reached"
                );
                self.state.total_escalations += 1;
            }
            TeamEvent::ReviewRequested { requested_by, .. } => ensure!(
                self.policy.member(*requested_by).is_some(),
                "unknown review requester"
            ),
            TeamEvent::StopRequested { condition, .. } => {
                ensure!(
                    matches!(
                        self.policy.decide(
                            &self.state,
                            PolicyRequest::Stop {
                                condition: *condition
                            }
                        ),
                        PolicyDecision::Stop(_)
                    ),
                    "stop violates policy or requires review"
                );
                self.state.stop_requested = true;
            }
        }
        self.events.push(event);
        Ok(())
    }
}

fn request_handoff(request: &TeamPolicyToolRequest) -> &Handoff {
    match request {
        TeamPolicyToolRequest::Assign { handoff, .. }
        | TeamPolicyToolRequest::RequestSpecialist { handoff, .. }
        | TeamPolicyToolRequest::Report { handoff, .. }
        | TeamPolicyToolRequest::Escalate { handoff, .. }
        | TeamPolicyToolRequest::Stop { handoff, .. } => handoff,
    }
}

fn request_actor(request: &TeamPolicyToolRequest) -> Uuid {
    match request {
        TeamPolicyToolRequest::Assign { assigned_by, .. } => *assigned_by,
        TeamPolicyToolRequest::RequestSpecialist { requested_by, .. }
        | TeamPolicyToolRequest::Stop { requested_by, .. } => *requested_by,
        TeamPolicyToolRequest::Report { reported_by, .. } => *reported_by,
        TeamPolicyToolRequest::Escalate { raised_by, .. } => *raised_by,
    }
}

fn event_handoff(event: &TeamEvent) -> &Handoff {
    match event {
        TeamEvent::Assignment { handoff, .. }
        | TeamEvent::SpecialistSpawnRequested { handoff, .. }
        | TeamEvent::Report { handoff, .. }
        | TeamEvent::Escalation { handoff, .. }
        | TeamEvent::ReviewRequested { handoff, .. }
        | TeamEvent::StopRequested { handoff, .. } => handoff,
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
        let (policy, _, worker) = fixture();
        let specialist = Uuid::new_v4();
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
        let director = policy
            .topology
            .members
            .iter()
            .find(|member| member.role == TeamRole::Director)
            .unwrap()
            .participant_id;
        assert_eq!(
            policy.decide(
                &TeamPolicyState::default(),
                PolicyRequest::SpawnSpecialist {
                    requested_by: director,
                    specialist_participant_id: specialist
                }
            ),
            PolicyDecision::AllowSpecialistSpawn
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

    fn handoff(workspace_id: Uuid) -> Handoff {
        Handoff {
            context: ContextCursor {
                workspace_id,
                after_sequence: 0,
                through_sequence: 0,
                session_id: None,
                session_event_id: None,
            },
            evidence: Vec::new(),
            continuation: None,
        }
    }

    #[test]
    fn runtime_durably_applies_authorized_assignment_report_and_stop_rules() {
        let (mut policy, director, worker) = fixture();
        policy.review_conditions.clear();
        let workspace_id = policy.topology.workspace_id;
        let assignment_id = Uuid::new_v4();
        let mut runtime = TeamPolicyRuntime::new(policy);
        let assigned = runtime
            .apply_tool_request(TeamPolicyToolRequest::Assign {
                assignment_id,
                work_id: Uuid::new_v4(),
                assigned_by: director,
                assigned_to: worker,
                handoff: handoff(workspace_id),
            })
            .unwrap();
        assert_eq!(assigned.decision, PolicyDecision::Allow);
        assert_eq!(runtime.state.active_assignments, 1);
        assert!(matches!(
            runtime
                .apply_tool_request(TeamPolicyToolRequest::Assign {
                    assignment_id: Uuid::new_v4(),
                    work_id: Uuid::new_v4(),
                    assigned_by: worker,
                    assigned_to: worker,
                    handoff: handoff(workspace_id),
                })
                .unwrap()
                .decision,
            PolicyDecision::Deny("role may not assign this member")
        ));
        let report = runtime
            .apply_tool_request(TeamPolicyToolRequest::Report {
                assignment_id,
                work_id: Uuid::new_v4(),
                reported_by: worker,
                summary: "done".into(),
                completed: true,
                handoff: handoff(workspace_id),
            })
            .unwrap();
        assert_eq!(report.decision, PolicyDecision::Allow);
        assert_eq!(runtime.state.active_assignments, 0);
        assert!(matches!(
            runtime
                .apply_tool_request(TeamPolicyToolRequest::Stop {
                    requested_by: director,
                    condition: StopCondition::ExplicitRequest,
                    detail: None,
                    handoff: handoff(workspace_id),
                })
                .unwrap()
                .decision,
            PolicyDecision::Stop(StopCondition::ExplicitRequest)
        ));
        assert!(runtime.state.stop_requested);
        let restored: TeamPolicyRuntime =
            serde_json::from_str(&serde_json::to_string(&runtime).unwrap()).unwrap();
        assert_eq!(restored.events, runtime.events);
        let replayed =
            TeamPolicyRuntime::replay(runtime.policy.clone(), runtime.events.clone()).unwrap();
        assert_eq!(replayed.state, runtime.state);
    }

    #[test]
    fn runtime_specialist_and_escalation_decisions_remain_policy_bound() {
        let (mut policy, director, worker) = fixture();
        policy.review_conditions.clear();
        policy.limits.budget.max_tokens = Some(100);
        policy.limits.max_total_escalations = 2;
        let workspace_id = policy.topology.workspace_id;
        let mut runtime = TeamPolicyRuntime::new(policy);
        let denied = runtime
            .apply_tool_request(TeamPolicyToolRequest::RequestSpecialist {
                work_id: Uuid::new_v4(),
                requested_by: worker,
                specialist_participant_id: Uuid::new_v4(),
                profile: ModelProfile {
                    provider: ProviderId("example".into()),
                    model: None,
                    reasoning_effort: Some("low".into()),
                },
                handoff: handoff(workspace_id),
            })
            .unwrap();
        assert_eq!(
            denied.decision,
            PolicyDecision::EscalateToDirector(EscalationReason::MissingAuthority)
        );
        assert!(matches!(
            denied.events.as_slice(),
            [TeamEvent::Escalation { .. }]
        ));
        let allowed = runtime
            .apply_tool_request(TeamPolicyToolRequest::RequestSpecialist {
                work_id: Uuid::new_v4(),
                requested_by: director,
                specialist_participant_id: Uuid::new_v4(),
                profile: ModelProfile {
                    provider: ProviderId("example".into()),
                    model: None,
                    reasoning_effort: Some("low".into()),
                },
                handoff: handoff(workspace_id),
            })
            .unwrap();
        assert_eq!(allowed.decision, PolicyDecision::AllowSpecialistSpawn);
        assert_eq!(runtime.state.active_specialists, 1);
        runtime.state.budget_usage.tokens = 100;
        assert_eq!(
            runtime
                .apply_tool_request(TeamPolicyToolRequest::Escalate {
                    assignment_id: None,
                    work_id: None,
                    raised_by: director,
                    reason: EscalationReason::BudgetRisk,
                    detail: "budget".into(),
                    handoff: handoff(workspace_id),
                })
                .unwrap()
                .decision,
            PolicyDecision::Stop(StopCondition::BudgetExhausted)
        );
    }

    #[test]
    fn runtime_records_report_and_escalation_before_required_review() {
        let (policy, director, worker) = fixture();
        let workspace_id = policy.topology.workspace_id;
        let assignment_id = Uuid::new_v4();
        let mut runtime = TeamPolicyRuntime::new(policy);
        runtime
            .apply_tool_request(TeamPolicyToolRequest::Assign {
                assignment_id,
                work_id: Uuid::new_v4(),
                assigned_by: director,
                assigned_to: worker,
                handoff: handoff(workspace_id),
            })
            .unwrap();

        let report = runtime
            .apply_tool_request(TeamPolicyToolRequest::Report {
                assignment_id,
                work_id: Uuid::new_v4(),
                reported_by: worker,
                summary: "needs review".into(),
                completed: false,
                handoff: handoff(workspace_id),
            })
            .unwrap();
        assert_eq!(
            report.decision,
            PolicyDecision::RequestReview(ReviewCondition::OnWorkerReport)
        );
        assert!(matches!(
            report.events.as_slice(),
            [TeamEvent::Report { .. }, TeamEvent::ReviewRequested { .. }]
        ));
        assert_eq!(runtime.state.total_reports, 1);

        let escalation = runtime
            .apply_tool_request(TeamPolicyToolRequest::Escalate {
                assignment_id: Some(assignment_id),
                work_id: None,
                raised_by: worker,
                reason: EscalationReason::Blocked,
                detail: "blocked".into(),
                handoff: handoff(workspace_id),
            })
            .unwrap();
        assert_eq!(
            escalation.decision,
            PolicyDecision::RequestReview(ReviewCondition::OnEscalation)
        );
        assert!(matches!(
            escalation.events.as_slice(),
            [
                TeamEvent::Escalation { .. },
                TeamEvent::ReviewRequested { .. }
            ]
        ));
        assert_eq!(runtime.state.total_escalations, 1);
    }
}
